"""Collections never name a photo that no longer exists.

Static collections store their members as ``photo_ids`` lists inside the
rules JSON, so nothing in the schema drops a member when its photo row goes.
Two halves keep them honest:

* every ``photos`` row is deleted through ``repositories.photo_row_deletion``,
  which takes the row out of every workspace's collections (or moves its
  entries to the photo that absorbed it). The structural test below fails on
  a ``DELETE FROM photos`` anywhere else, and each deleting path has a
  regression test here;
* ``StartupTasks.prune_collection_ids_of_missing_photos`` removes, at every
  boot, entries a collection was saved with after their photo was gone.
"""

import ast
import json
import logging
import re
from pathlib import Path

import pytest
from repositories.photo_row_deletion import photo_row_deletion
from services.startup_tasks import StartupTasks

VIREO_ROOT = Path(__file__).resolve().parents[1]
CHOKEPOINT = "repositories/photo_row_deletion.py"
# A SQL statement that deletes rows of ``photos`` itself (not ``photo_*``
# tables): optional schema prefix and identifier quotes included.
_DELETE_PHOTOS = re.compile(
    r"""^\s*DELETE\s+FROM\s+(?:(?:main|"main")\s*\.\s*)?["`\[]?photos["`\]]?(?:\s|;|$)""",
    re.IGNORECASE,
)


def _sql_strings(tree):
    """Every string literal in ``tree``, f-strings by their literal head."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            yield node.lineno, node.value
        elif isinstance(node, ast.JoinedStr) and node.values:
            head = node.values[0]
            if isinstance(head, ast.Constant) and isinstance(head.value, str):
                yield node.lineno, head.value


def _photo_row_deletes():
    found = []
    for path in sorted(VIREO_ROOT.rglob("*.py")):
        rel = path.relative_to(VIREO_ROOT).as_posix()
        if rel.startswith("tests/"):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for lineno, text in _sql_strings(tree):
            if _DELETE_PHOTOS.match(text):
                found.append(f"{rel}:{lineno}")
    return found


def test_every_photo_row_delete_goes_through_photo_row_deletion():
    """A new path that deletes photos must use the chokepoint.

    Otherwise its deleted ids stay in static collections, and the next photo
    SQLite hands the freed id to joins them.
    """
    deletes = _photo_row_deletes()
    # The matcher must see the chokepoint's own statement, or it proves nothing.
    assert any(site.startswith(CHOKEPOINT + ":") for site in deletes)
    offenders = [site for site in deletes if not site.startswith(CHOKEPOINT + ":")]
    assert offenders == [], (
        "delete photos rows through repositories.photo_row_deletion so their "
        "collection entries go with them: " + ", ".join(offenders)
    )


@pytest.mark.parametrize("sql", [
    "DELETE FROM photos WHERE id = ?",
    "  delete from photos where id in (1)",
    'DELETE FROM "photos" WHERE id = 1',
    "DELETE FROM main.photos",
    "DELETE FROM photos",
])
def test_delete_matcher_catches_spellings(sql):
    assert _DELETE_PHOTOS.match(sql)


@pytest.mark.parametrize("sql", [
    "DELETE FROM photo_keywords WHERE photo_id = ?",
    "DELETE FROM photos_fts WHERE rowid = ?",
    "the ``DELETE FROM photos`` below",
])
def test_delete_matcher_ignores_other_tables_and_prose(sql):
    assert not _DELETE_PHOTOS.match(sql)


# -- helpers ---------------------------------------------------------------


def _photo(db, folder_id, filename, **kw):
    return db.add_photo(
        folder_id=folder_id, filename=filename,
        extension="." + filename.rsplit(".", 1)[1],
        file_size=kw.pop("file_size", 10), file_mtime=kw.pop("file_mtime", 1.0),
        **kw,
    )


def _static_collection(db, workspace_id, name, ids):
    cid = db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        (name, json.dumps([{"field": "photo_ids", "value": list(ids)}]), workspace_id),
    ).lastrowid
    db.conn.commit()
    return cid


def _ids(db, collection_id):
    rules = json.loads(db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (collection_id,),
    ).fetchone()[0])
    return rules[0]["value"]


def _dangling(db):
    existing = {row[0] for row in db.conn.execute("SELECT id FROM photos")}
    return db.photo_ids_named_by_collections() - existing


# -- the chokepoint --------------------------------------------------------


def test_photo_row_deletion_rewrites_every_workspace_once_on_exit(db, tmp_path, monkeypatch):
    import repositories.photo_row_deletion as module

    other_ws = db.create_workspace("Other")
    folder = db.add_folder(str(tmp_path), name="lib")
    gone, merged, survivor, kept = (
        _photo(db, folder, f"{n}.jpg") for n in ("gone", "merged", "survivor", "kept")
    )
    here = _static_collection(db, db._ws_id(), "here", [gone, merged, kept])
    there = _static_collection(db, other_ws, "there", [merged, survivor])

    calls = []
    real = module.remap_collection_photo_ids

    def counted(conn, mapping):
        calls.append(dict(mapping))
        return real(conn, mapping)

    monkeypatch.setattr(module, "remap_collection_photo_ids", counted)
    with photo_row_deletion(db.conn) as photo_rows:
        photo_rows.delete({gone: None})
        photo_rows.delete({merged: survivor})
        # Rows are gone at once; collections wait for the block to end.
        assert db.get_photo(gone) is None and db.get_photo(merged) is None
        assert calls == []
    db.conn.commit()

    assert calls == [{gone: None, merged: survivor}]
    assert _ids(db, here) == [survivor, kept]
    # The merged row follows its survivor, which the rule already listed once.
    assert _ids(db, there) == [survivor]
    assert db.get_photo(kept) is not None


def test_photo_row_deletion_rewrites_collections_when_the_block_raises(db, tmp_path):
    folder = db.add_folder(str(tmp_path), name="lib")
    gone = _photo(db, folder, "gone.jpg")
    kept = _photo(db, folder, "kept.jpg")
    coll = _static_collection(db, db._ws_id(), "c", [gone, kept])

    with pytest.raises(RuntimeError), photo_row_deletion(db.conn) as photo_rows:
        photo_rows.delete({gone: None})
        raise RuntimeError("later step failed")
    # A caller that keeps the rows it did delete keeps a clean collection.
    db.conn.commit()
    assert _ids(db, coll) == [kept]


def test_photo_row_deletion_rollback_restores_rows_and_collections(db, tmp_path):
    folder = db.add_folder(str(tmp_path), name="lib")
    gone = _photo(db, folder, "gone.jpg")
    coll = _static_collection(db, db._ws_id(), "c", [gone])

    with pytest.raises(RuntimeError):
        try:
            with photo_row_deletion(db.conn) as photo_rows:
                photo_rows.delete({gone: None})
                raise RuntimeError("abort")
        except RuntimeError:
            db.conn.rollback()
            raise
    assert db.get_photo(gone) is not None
    assert _ids(db, coll) == [gone]


# -- every deleting path ---------------------------------------------------


def test_batch_delete_prunes_collections(db, tmp_path):
    other_ws = db.create_workspace("Other")
    folder = db.add_folder(str(tmp_path), name="lib")
    gone = _photo(db, folder, "gone.jpg")
    kept = _photo(db, folder, "kept.jpg")
    coll = _static_collection(db, other_ws, "c", [gone, kept])

    db.delete_photos([gone])

    assert _ids(db, coll) == [kept]
    assert _dangling(db) == set()


def test_remove_orphans_prunes_collections(db):
    from audit import remove_orphans

    other_ws = db.create_workspace("Other")
    folder = db.add_folder("/gone", name="gone")
    orphan = _photo(db, folder, "missing.jpg")
    kept = _photo(db, folder, "kept.jpg")
    coll = _static_collection(db, other_ws, "c", [orphan, kept])

    remove_orphans(db, [orphan])

    assert db.get_photo(orphan) is None
    assert _ids(db, coll) == [kept]
    assert _dangling(db) == set()


def test_relocating_a_missing_folder_onto_an_existing_one_prunes_collections(db, tmp_path):
    other_ws = db.create_workspace("Other")
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    target = db.add_folder(str(target_dir), workspace_root=False)
    source = db.add_folder("/coll-src")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (source,))
    db.conn.commit()
    survivor = _photo(db, target, "dup.jpg")
    dup = _photo(db, source, "dup.jpg")
    phantom = _photo(db, source, "phantom.jpg")
    coll = _static_collection(db, other_ws, "c", [dup, phantom])

    db._merge_into_existing(source, target, str(target_dir))

    assert _ids(db, coll) == [survivor]
    assert _dangling(db) == set()


def _staged_tree(db, tmp_path, *, archived_on_disk):
    arch = tmp_path / "arch" / "USA"
    date_dir = arch / "2026-01-01"
    date_dir.mkdir(parents=True)
    if archived_on_disk:
        (date_dir / "dup.raf").write_bytes(b"archived")
    base_id = db.add_folder(str(arch), name="USA")
    date_id = db.add_folder(str(date_dir), name="2026-01-01", parent_id=base_id)
    archived = _photo(db, date_id, "dup.raf", file_size=8, file_hash="DUPHASH")
    db.add_workspace_folder(db._ws_id(), base_id, is_root=True)
    stage = tmp_path / "stage" / "USA"
    stage_root = db.add_folder(str(stage), name="USA", workspace_root=False)
    stage_leaf = db.add_folder(
        str(stage / "2026-01-01"), name="2026-01-01",
        parent_id=stage_root, workspace_root=False,
    )
    staged = _photo(
        db, stage_leaf, "dup.raf", file_size=8, file_mtime=2.0, file_hash="DUPHASH",
    )
    return str(arch), stage_root, archived, staged


def test_archive_merge_dropping_an_already_archived_photo_remaps_collections(db, tmp_path):
    other_ws = db.create_workspace("Other")
    arch, stage_root, archived, staged = _staged_tree(db, tmp_path, archived_on_disk=True)
    coll = _static_collection(db, other_ws, "c", [staged])

    counts = db.merge_staged_tree_into_archive(stage_root, arch)

    assert counts["already_present"] == 1
    assert db.get_photo(staged) is None
    assert _ids(db, coll) == [archived]
    assert _dangling(db) == set()


def test_archive_merge_replacing_a_phantom_row_remaps_collections(db, tmp_path):
    other_ws = db.create_workspace("Other")
    arch, stage_root, phantom, staged = _staged_tree(db, tmp_path, archived_on_disk=False)
    coll = _static_collection(db, other_ws, "c", [phantom])

    counts = db.merge_staged_tree_into_archive(stage_root, arch)

    assert phantom in counts["dropped_photo_ids"]
    assert db.get_photo(phantom) is None
    assert _ids(db, coll) == [staged]
    assert _dangling(db) == set()


def test_companion_merge_remaps_collections(db, tmp_path):
    from scanner import _pair_raw_jpeg_companions

    other_ws = db.create_workspace("Other")
    folder = db.add_folder(str(tmp_path), name="lib")
    jpeg = _photo(db, folder, "IMG_001.jpg", file_size=1000)
    raw = _photo(db, folder, "IMG_001.cr3", file_size=2000)
    coll = _static_collection(db, other_ws, "c", [jpeg])

    assert _pair_raw_jpeg_companions(db) == {jpeg: raw}

    assert _ids(db, coll) == [raw]
    assert _dangling(db) == set()


# -- the startup repair ----------------------------------------------------


def _repair(db):
    return StartupTasks(app=None, db_path=db._db_path, init_db=db)


def test_startup_repair_removes_only_entries_for_missing_photos(db, tmp_path, caplog):
    other_ws = db.create_workspace("Other")
    folder = db.add_folder(str(tmp_path), name="lib")
    kept_a = _photo(db, folder, "a.jpg")
    kept_b = _photo(db, folder, "b.jpg")
    gone_a = _photo(db, folder, "c.jpg")
    gone_b = _photo(db, folder, "d.jpg")
    # Deleted behind the chokepoint's back, as older builds did.
    db.conn.execute("DELETE FROM photos WHERE id IN (?, ?)", (gone_a, gone_b))
    db.conn.commit()

    flat = _static_collection(db, db._ws_id(), "flat", [kept_a, gone_a, kept_b, gone_a])
    nested_rules = [
        {"field": "rating", "op": ">=", "value": 3},
        {"mode": "any", "rules": [
            {"field": "photo_ids", "value": [str(gone_b), kept_b]},
            {"field": "keyword", "op": "contains", "value": "heron"},
        ]},
    ]
    nested = db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        ("nested", json.dumps(nested_rules), other_ws),
    ).lastrowid
    clean = _static_collection(db, other_ws, "clean", [kept_a])
    smart = db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        ("smart", json.dumps([{"field": "rating", "op": ">=", "value": 4}]), other_ws),
    ).lastrowid
    db.conn.commit()
    clean_text, smart_text = (
        db.conn.execute("SELECT rules FROM collections WHERE id = ?", (cid,)).fetchone()[0]
        for cid in (clean, smart)
    )
    photo_count = db.conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0]

    with caplog.at_level(logging.INFO, logger="services.startup_tasks"):
        pruned = _repair(db).prune_collection_ids_of_missing_photos()

    assert pruned == [
        {"collection_id": flat, "workspace_id": db._ws_id(), "name": "flat", "removed": 2},
        {"collection_id": nested, "workspace_id": other_ws, "name": "nested", "removed": 1},
    ]
    assert _ids(db, flat) == [kept_a, kept_b]
    nested_rules[1]["rules"][0]["value"] = [kept_b]
    assert json.loads(db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (nested,),
    ).fetchone()[0]) == nested_rules
    # Collections with nothing to remove are not rewritten at all.
    assert [
        db.conn.execute("SELECT rules FROM collections WHERE id = ?", (cid,)).fetchone()[0]
        for cid in (clean, smart)
    ] == [clean_text, smart_text]
    assert db.conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0] == photo_count
    assert not db.conn.in_transaction
    assert _dangling(db) == set()
    assert "Removed 2 deleted photo(s) from collection %d 'flat'" % flat in caplog.text
    assert "Removed 3 entries for deleted photos from 2 collection(s)" in caplog.text


def test_startup_repair_is_idempotent_and_writes_nothing_when_clean(db, tmp_path, caplog):
    folder = db.add_folder(str(tmp_path), name="lib")
    kept = _photo(db, folder, "a.jpg")
    gone = _photo(db, folder, "b.jpg")
    db.conn.execute("DELETE FROM photos WHERE id = ?", (gone,))
    coll = _static_collection(db, db._ws_id(), "c", [kept, gone])

    assert len(_repair(db).prune_collection_ids_of_missing_photos()) == 1
    changes = db.conn.total_changes
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="services.startup_tasks"):
        assert _repair(db).prune_collection_ids_of_missing_photos() == []
    assert db.conn.total_changes == changes
    assert caplog.text == ""
    assert _ids(db, coll) == [kept]


@pytest.mark.parametrize("top_id", [None, 5000], ids=["scan-all-ids", "look-up-each-id"])
def test_startup_repair_finds_missing_ids_either_way_it_reads_photos(
        db, tmp_path, top_id, monkeypatch):
    """Few ids next to a large catalog are looked up one by one; otherwise
    every photo id is read once. Both find the same missing entries."""
    import repositories.collections as collections_repo

    statements = []
    db.conn.set_trace_callback(statements.append)
    folder = db.add_folder(str(tmp_path), name="lib")
    kept = _photo(db, folder, "a.jpg")
    if top_id is not None:
        db.conn.execute("UPDATE photos SET id = ? WHERE id = ?", (top_id, kept))
        db.conn.commit()
        kept = top_id
    beyond_rowids = str(2 ** 70)
    coll = _static_collection(db, db._ws_id(), "c", [kept, kept + 1, beyond_rowids])
    statements.clear()

    pruned = collections_repo.prune_collection_ids_of_missing_photos(
        db.conn, commit=lambda conn: conn.commit(),
    )

    db.conn.set_trace_callback(None)
    looked_up = any("FROM photos WHERE id IN" in s for s in statements)
    assert looked_up == (top_id is not None)
    assert pruned[0]["removed"] == 2
    assert _ids(db, coll) == [kept]


def test_run_catalog_repairs_prunes_collections(db, tmp_path):
    folder = db.add_folder(str(tmp_path), name="lib")
    kept = _photo(db, folder, "a.jpg")
    gone = _photo(db, folder, "b.jpg")
    db.conn.execute("DELETE FROM photos WHERE id = ?", (gone,))
    coll = _static_collection(db, db._ws_id(), "c", [gone, kept])

    _repair(db).run_catalog_repairs()

    assert _ids(db, coll) == [kept]


def test_startup_repair_failure_does_not_stop_startup(db, monkeypatch, caplog):
    def boom():
        raise RuntimeError("disk I/O error")

    monkeypatch.setattr(db, "prune_collection_ids_of_missing_photos", boom)
    with caplog.at_level(logging.ERROR, logger="services.startup_tasks"):
        assert _repair(db).prune_collection_ids_of_missing_photos() == []
    assert "Could not check collections for deleted photos" in caplog.text
