"""Behavior pins for the photos domain of ``Database``.

The tests exercise the core photo-row methods only through the public
``Database`` façade, so they hold whether the SQL lives in ``db.py`` or in
``repositories/photos.py``: adding and reading photo rows, the id/path
lookups, the workspace-scoped listing, paging, position, count, calendar
and browse-summary reads, companion counts, deletion (with its commit,
rollback, cache-invalidation and pipeline-prune boundaries), and the
sharpness/quality writers.
"""

import ast
import contextlib
import inspect
import json
import sqlite3
import textwrap

import config as cfg
import pytest
from db import _SQLITE_PARAM_CHUNK_SIZE, Database
from repositories.photo_sql import PHOTO_COLS, PHOTO_DETAIL_COLS
from services import browse_queries, catalog_deletion, duplicate_resolution, photo_ingest, workspace_folders


@pytest.fixture
def isolated_config(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))


def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return contextlib.closing(conn)


def _photo(db, folder_id, filename, **kw):
    kw.setdefault("extension", "." + filename.rsplit(".", 1)[-1])
    kw.setdefault("file_size", 100)
    kw.setdefault("file_mtime", 1.0)
    return photo_ingest.add_photo(db, folder_id=folder_id, filename=filename, **kw)


def _trace(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    return statements


def _sql(statements, needle):
    return [s.strip() for s in statements if needle in s]


class _RecordingCache:
    def __init__(self):
        self.invalidated = []

    def invalidate_workspaces(self, db_path, workspace_ids):
        self.invalidated.append((db_path, set(workspace_ids)))


@pytest.fixture
def cache(db, monkeypatch):
    recorder = _RecordingCache()
    monkeypatch.setattr(db, "_new_images_cache", recorder)
    return recorder


@pytest.fixture
def lib(db, tmp_path):
    """A workspace with two visible folders and one foreign folder."""
    root = workspace_folders.add_folder(db, str(tmp_path / "lib"), name="lib")
    child = workspace_folders.add_folder(db, str(tmp_path / "lib" / "c"), name="c", parent_id=root)
    other_ws = db.create_workspace("Other")
    db.commit()
    foreign = workspace_folders.add_folder(
        db, str(tmp_path / "foreign"), name="foreign", link_to_workspace=False,
    )
    workspace_folders.add_workspace_folder(db, other_ws, foreign)
    a = _photo(db, root, "a.jpg", timestamp="2024-01-05T10:00:00")
    b = _photo(db, root, "b.jpg", timestamp="2024-03-07T10:00:00")
    c = _photo(db, child, "c.jpg", timestamp="2023-06-01T10:00:00")
    f = _photo(db, foreign, "f.jpg", timestamp="2022-01-01T10:00:00")
    return {
        "root": root, "child": child, "foreign": foreign, "other_ws": other_ws,
        "a": a, "b": b, "c": c, "f": f,
    }


# -- add_photo ---------------------------------------------------------------


def test_add_photo_commits_and_returns_new_id(db):
    fid = workspace_folders.add_folder(db, "/p", name="p")
    pid = _photo(db, fid, "x.jpg", timestamp="2024-01-01", width=10,
                 height=20, xmp_mtime=5.0, file_hash="h1")
    assert not db.conn.in_transaction
    with _reader(db) as r:
        row = r.execute(
            "SELECT folder_id, filename, extension, file_size, file_mtime, "
            "xmp_mtime, timestamp, width, height, file_hash FROM photos "
            "WHERE id = ?", (pid,),
        ).fetchone()
    assert tuple(row) == (fid, "x.jpg", ".jpg", 100, 1.0, 5.0,
                          "2024-01-01", 10, 20, "h1")


def test_add_photo_existing_row_returns_existing_id_without_update(db):
    fid = workspace_folders.add_folder(db, "/p", name="p")
    pid = _photo(db, fid, "x.jpg", file_size=1)
    again = _photo(db, fid, "x.jpg", file_size=999)
    assert again == pid
    assert db.conn.execute(
        "SELECT file_size FROM photos WHERE id = ?", (pid,)
    ).fetchone()[0] == 1


def test_add_photo_runs_duplicate_resolution_only_with_hash(db, monkeypatch):
    calls = []
    monkeypatch.setattr(
        duplicate_resolution, "check_and_resolve_duplicates_for_hash",
        lambda _db, file_hash: calls.append(file_hash),
    )
    fid = workspace_folders.add_folder(db, "/p", name="p")
    _photo(db, fid, "x.jpg")
    assert calls == []
    _photo(db, fid, "y.jpg", file_hash="abc")
    assert calls == ["abc"]
    # A re-add of an existing row still triggers the hook.
    _photo(db, fid, "y.jpg", file_hash="abc")
    assert calls == ["abc", "abc"]


def test_add_photo_duplicate_hash_rejects_loser(db):
    fid = workspace_folders.add_folder(db, "/p", name="p")
    first = _photo(db, fid, "x.jpg", file_hash="same")
    second = _photo(db, fid, "y.jpg", file_hash="same")
    flags = dict(db.conn.execute(
        "SELECT id, flag FROM photos WHERE id IN (?, ?)", (first, second),
    ).fetchall())
    assert sorted(v or "none" for v in flags.values()) == ["none", "rejected"]


def test_add_photo_inserts_with_retry_and_leaves_commit_to_its_unit(db, monkeypatch):
    import db as db_module

    seen = []
    real_exec = db_module.execute_with_retry
    real_commit = db_module.commit_with_retry

    def exec_spy(conn, sql, params=()):
        seen.append(("exec", sql.split()[0]))
        return real_exec(conn, sql, params)

    def commit_spy(conn, *a, **kw):
        seen.append(("commit",))
        return real_commit(conn, *a, **kw)

    fid = workspace_folders.add_folder(db, "/p", name="p")
    monkeypatch.setattr(db_module, "execute_with_retry", exec_spy)
    monkeypatch.setattr(db_module, "commit_with_retry", commit_spy)
    _photo(db, fid, "x.jpg")
    # The insert retries while busy; the repository never commits (the
    # ingest service's unit of work does, with its own busy retry).
    assert seen == [("exec", "INSERT")]


# -- get_photo -----------------------------------------------------------------


def test_get_photo_returns_detail_columns(db, lib):
    row = db.photos.get(lib["a"])
    assert set(row.keys()) == {
        c.strip() for c in PHOTO_DETAIL_COLS.split(",")
    }
    assert row["filename"] == "a.jpg"
    assert db.photos.get(999999) is None


def test_get_photo_sql_is_byte_identical(db, lib):
    statements = _trace(db)
    db.photos.get(lib["a"])
    db.photos.get(lib["a"], verify_workspace=True)
    db.conn.set_trace_callback(None)
    detail = PHOTO_DETAIL_COLS
    assert statements == [
        f"SELECT {detail} FROM photos WHERE id = {lib['a']}",
        f"""SELECT {detail} FROM photos
                    WHERE id = {lib['a']} AND id IN (
                        SELECT photo_id FROM photo_workspace_visibility
                        WHERE workspace_id = {db._active_workspace_id})""",
    ]


def test_get_photo_verify_workspace_scopes_to_active_workspace(db, lib):
    assert db.photos.get(lib["f"])["filename"] == "f.jpg"
    assert db.photos.get(lib["f"], verify_workspace=True) is None
    assert db.photos.get(lib["a"], verify_workspace=True)["filename"] == "a.jpg"


def test_get_photo_needs_workspace_only_when_verifying(db, lib):
    db.set_active_workspace(None)
    assert db.photos.get(lib["a"])["filename"] == "a.jpg"
    with pytest.raises(RuntimeError):
        db.photos.get(lib["a"], verify_workspace=True)


# -- id lookups ------------------------------------------------------------------


def test_get_photo_filenames(db, lib):
    assert db.photos.get_filenames([]) == {}
    assert db.photos.get_filenames(None) == {}
    got = db.photos.get_filenames({lib["a"], lib["c"], 999999})
    assert got == {lib["a"]: (lib["root"], "a.jpg"),
                   lib["c"]: (lib["child"], "c.jpg")}


def test_get_photo_filenames_chunks(db, lib):
    ids = [lib["a"]] + list(range(10**6, 10**6 + _SQLITE_PARAM_CHUNK_SIZE))
    statements = _trace(db)
    got = db.photos.get_filenames(ids)
    db.conn.set_trace_callback(None)
    assert got == {lib["a"]: (lib["root"], "a.jpg")}
    assert len(_sql(statements, "SELECT id, folder_id, filename")) == 2


def test_get_existing_photo_ids_spans_workspaces_and_chunks(db, lib):
    assert db.photos.existing_ids([]) == set()
    # Photo rows are global: the foreign workspace's photo exists too.
    assert db.photos.existing_ids([lib["a"], lib["f"], 999999]) == {lib["a"], lib["f"]}
    ids = [lib["a"]] + list(range(10**6, 10**6 + _SQLITE_PARAM_CHUNK_SIZE))
    statements = _trace(db)
    assert db.photos.existing_ids(ids) == {lib["a"]}
    db.conn.set_trace_callback(None)
    assert len(_sql(statements, "SELECT id FROM photos WHERE id IN")) == 2


def test_get_photos_by_ids(db, lib):
    assert db.photos.get_by_ids([]) == {}
    got = db.photos.get_by_ids([lib["a"], lib["b"], 999999])
    assert sorted(got) == sorted([lib["a"], lib["b"]])
    cols = {c.strip() for c in PHOTO_COLS.split(",")}
    assert set(got[lib["a"]].keys()) == cols
    with_exif = db.photos.get_by_ids([lib["a"]], include_exif=True)
    assert set(with_exif[lib["a"]].keys()) == cols | {"exif_data"}


def test_get_photos_by_ids_chunks(db, lib):
    ids = [lib["b"]] + list(range(10**6, 10**6 + _SQLITE_PARAM_CHUNK_SIZE))
    statements = _trace(db)
    got = db.photos.get_by_ids(ids)
    db.conn.set_trace_callback(None)
    assert list(got) == [lib["b"]]
    assert len(_sql(statements, "FROM photos WHERE id IN")) == 2


def test_get_photo_folder_statuses(db, lib):
    assert db.photos.get_folder_statuses([]) == {}
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (lib["child"],),
    )
    got = db.photos.get_folder_statuses(
        [lib["a"], lib["c"], lib["a"], 999999]
    )
    assert got == {lib["a"]: "ok", lib["c"]: "missing"}


def test_get_photo_folder_statuses_dedupes_before_chunking(db, lib):
    ids = [lib["a"]] * (_SQLITE_PARAM_CHUNK_SIZE + 5)
    statements = _trace(db)
    assert db.photos.get_folder_statuses(ids) == {lib["a"]: "ok"}
    db.conn.set_trace_callback(None)
    assert len(_sql(statements, "SELECT p.id, f.status")) == 1


def test_get_photo_with_folder_path(db, lib, tmp_path):
    row = db.photos.get_with_folder_path(lib["c"])
    assert row["filename"] == "c.jpg"
    assert row["folder_path"] == str(tmp_path / "lib" / "c")
    # The whole row, BLOB columns included, from any workspace.
    assert "dino_global_embedding" in set(row.keys())
    assert db.photos.get_with_folder_path(lib["f"])["filename"] == "f.jpg"
    assert db.photos.get_with_folder_path(999999) is None


def test_get_photo_file_in_workspace(db, lib, tmp_path):
    ws = db.active_workspace_id
    row = db.photos.file_in_workspace(lib["c"], ws)
    assert dict(row) == {
        "filename": "c.jpg", "folder_path": str(tmp_path / "lib" / "c"),
    }
    # The workspace is the argument, not the active one.
    assert db.photos.file_in_workspace(lib["f"], ws) is None
    assert db.photos.file_in_workspace(lib["f"], lib["other_ws"])["filename"] == "f.jpg"
    assert db.photos.file_in_workspace(999999, ws) is None
    # None matches nothing instead of raising, with or without an active one.
    assert db.photos.file_in_workspace(lib["a"], None) is None
    db.set_active_workspace(None)
    assert db.photos.file_in_workspace(lib["a"], ws)["filename"] == "a.jpg"


def test_get_best_batch_rows_by_ids(db, lib):
    db.conn.execute(
        "UPDATE photos SET rating = 4, flag = 'flagged', quality_score = 0.5, "
        "sharpness = 12.0 WHERE id = ?", (lib["a"],),
    )
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (lib["child"],))
    db.conn.commit()
    rows = db.photos.best_batch_rows_by_ids([lib["a"], lib["b"], lib["c"], lib["f"], 999999])
    assert {tuple(r.keys()) for r in rows} == {(
        "id", "folder_id", "filename", "extension", "timestamp",
        "flag", "rating", "quality_score", "sharpness",
    )}
    # c sits in a missing folder, f in another workspace's folder.
    by_id = {r["id"]: dict(r) for r in rows}
    assert set(by_id) == {lib["a"], lib["b"]}
    assert by_id[lib["a"]] == {
        "id": lib["a"], "folder_id": lib["root"], "filename": "a.jpg",
        "extension": ".jpg", "timestamp": "2024-01-05T10:00:00",
        "flag": "flagged", "rating": 4, "quality_score": 0.5, "sharpness": 12.0,
    }
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (lib["child"],))
    assert {r["id"] for r in db.photos.best_batch_rows_by_ids([lib["c"]])} == {lib["c"]}
    db.conn.rollback()
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.photos.best_batch_rows_by_ids([lib["a"]])


def test_get_photo_flags_ratings_and_eyes(db, lib):
    assert db.photos.flags_ratings_and_eyes([]) == {}
    db.photo_review.set_flag(lib["b"], "flagged")
    db.commit()
    db.conn.execute(
        "UPDATE photos SET rating = 3, eye_x = 0.25, eye_y = 0.5, eye_conf = 0.9,"
        " eye_tenengrad = 12.0 WHERE id = ?",
        (lib["a"],),
    )
    got = db.photos.flags_ratings_and_eyes([lib["a"], lib["b"], lib["f"], 999999])
    assert sorted(got) == sorted([lib["a"], lib["b"], lib["f"]])
    assert dict(got[lib["a"]]) == {
        "id": lib["a"], "flag": "none", "rating": 3,
        "eye_x": 0.25, "eye_y": 0.5, "eye_conf": 0.9, "eye_tenengrad": 12.0,
    }
    assert got[lib["b"]]["flag"] == "flagged"
    assert got[lib["b"]]["eye_x"] is None


def test_get_photo_flags_ratings_and_eyes_chunks(db, lib):
    ids = [lib["a"]] + list(range(10**6, 10**6 + _SQLITE_PARAM_CHUNK_SIZE))
    statements = _trace(db)
    got = db.photos.flags_ratings_and_eyes(ids)
    db.conn.set_trace_callback(None)
    assert list(got) == [lib["a"]]
    assert len(_sql(statements, "SELECT id, flag, rating")) == 2


def test_get_workspace_photos_in_folders(db, lib):
    assert db.photos.in_workspace_folders([], db.active_workspace_id) == []
    rows = db.photos.in_workspace_folders(
        [lib["root"], lib["child"], lib["foreign"]], db.active_workspace_id,
    )
    got = {(r["id"], r["folder_id"], r["filename"]) for r in rows}
    # The foreign folder's photo is in another workspace.
    assert got == {
        (lib["a"], lib["root"], "a.jpg"),
        (lib["b"], lib["root"], "b.jpg"),
        (lib["c"], lib["child"], "c.jpg"),
    }
    # A photo-only grant makes that one photo visible, not its siblings.
    db.conn.execute(
        "INSERT INTO workspace_photos (workspace_id, photo_id) VALUES (?, ?)",
        (db.active_workspace_id, lib["f"]),
    )
    rows = db.photos.in_workspace_folders([lib["foreign"]], db.active_workspace_id)
    assert [r["id"] for r in rows] == [lib["f"]]


def test_get_workspace_photos_in_folders_without_workspace_is_empty(db, lib):
    db.set_active_workspace(None)
    assert db.photos.in_workspace_folders([lib["root"]], db.active_workspace_id) == []


def test_get_workspace_photos_in_folders_chunks(db, lib):
    ids = [lib["root"]] + list(range(10**6, 10**6 + _SQLITE_PARAM_CHUNK_SIZE))
    statements = _trace(db)
    rows = db.photos.in_workspace_folders(ids, db.active_workspace_id)
    db.conn.set_trace_callback(None)
    assert sorted(r["id"] for r in rows) == sorted([lib["a"], lib["b"]])
    assert len(_sql(statements, "JOIN photo_workspace_visibility pv")) == 2


# -- counts ------------------------------------------------------------------


def test_count_photos_excludes_missing_folders_and_other_workspaces(db, lib):
    assert db.photos.count() == 3
    assert db.photos.count_in_workspace() == 3
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (lib["child"],),
    )
    assert db.photos.count() == 2
    assert db.photos.count_in_workspace() == 3
    db.conn.execute(
        "UPDATE folders SET status = 'partial' WHERE id = ?", (lib["child"],),
    )
    assert db.photos.count() == 3


def test_counts_require_active_workspace(db, lib):
    db.set_active_workspace(None)
    for fn in (db.photos.count, db.photos.count_in_workspace):
        with pytest.raises(RuntimeError):
            fn()


# -- photos_by_paths --------------------------------------------------------


def test_photos_by_paths(db, lib, tmp_path):
    assert db.photos.by_paths([]) == {}
    a_path = str(tmp_path / "lib" / "a.jpg")
    c_path = str(tmp_path / "lib" / "c" / "c.jpg")
    f_path = str(tmp_path / "foreign" / "f.jpg")
    new_path = str(tmp_path / "lib" / "new.jpg")
    got = db.photos.by_paths([a_path, c_path, f_path, new_path, a_path])
    # Photos are global, so the foreign-workspace photo is found too.
    assert got == {a_path: lib["a"], c_path: lib["c"], f_path: lib["f"]}


def test_photos_by_paths_batches_filenames_per_directory(db, tmp_path):
    fid = workspace_folders.add_folder(db, str(tmp_path / "big"), name="big")
    pid = _photo(db, fid, "keep.jpg")
    paths = [str(tmp_path / "big" / "keep.jpg")] + [
        str(tmp_path / "big" / f"n{i}.jpg") for i in range(805)
    ] + [str(tmp_path / "elsewhere" / "keep.jpg")]
    statements = _trace(db)
    got = db.photos.by_paths(paths)
    db.conn.set_trace_callback(None)
    assert got == {str(tmp_path / "big" / "keep.jpg"): pid}
    # 806 names in one directory -> 2 batches of <=800; one more directory.
    assert len(_sql(statements, "SELECT p.id, p.filename")) == 3


# -- photo-id reads by path and folder ---------------------------------------


def test_get_photo_ids_at_paths(db, lib, tmp_path):
    a_path = str(tmp_path / "lib" / "a.jpg")
    c_path = str(tmp_path / "lib" / "c" / "c.jpg")
    f_path = str(tmp_path / "foreign" / "f.jpg")
    new_path = str(tmp_path / "lib" / "new.jpg")
    # Photos are global; an unknown path matches nothing.
    got = db.photos.ids_at_paths([a_path, c_path, f_path, new_path])
    assert sorted(got) == sorted([lib["a"], lib["c"], lib["f"]])
    # Same filename in another directory does not match.
    assert db.photos.ids_at_paths([str(tmp_path / "foreign" / "a.jpg")]) == []
    # The temp table is reset per call and dropped afterwards.
    assert db.photos.ids_at_paths([c_path]) == [lib["c"]]
    assert db.conn.execute(
        "SELECT name FROM sqlite_temp_master WHERE name = '_imported_paths'"
    ).fetchone() is None


def test_get_photo_ids_at_paths_is_one_statement_for_many_paths(db, tmp_path):
    fid = workspace_folders.add_folder(db, str(tmp_path / "big"), name="big")
    pid = _photo(db, fid, "keep.jpg")
    paths = [str(tmp_path / "big" / "keep.jpg")] + [
        str(tmp_path / "big" / f"n{i}.jpg") for i in range(1200)
    ]
    statements = _trace(db)
    got = db.photos.ids_at_paths(paths)
    db.conn.set_trace_callback(None)
    assert got == [pid]
    assert len(_sql(statements, "JOIN _imported_paths")) == 1


def test_get_photo_ids_in_folders(db, lib):
    assert sorted(db.photos.ids_in_folders([lib["root"], lib["foreign"]])) == sorted(
        [lib["a"], lib["b"], lib["f"]])
    assert db.photos.ids_in_folders([]) == []


def test_get_photo_ids_in_folders_chunks(db, lib):
    folder_ids = [lib["child"]] + list(range(10_000, 10_000 + _SQLITE_PARAM_CHUNK_SIZE))
    statements = _trace(db)
    got = db.photos.ids_in_folders(folder_ids)
    db.conn.set_trace_callback(None)
    assert got == [lib["c"]]
    assert len(_sql(statements, "WHERE folder_id IN")) == 2


# -- filter_out_wildlife_excluded ---------------------------------------------


def test_filter_out_wildlife_excluded(db, lib):
    assert db.photos.filter_out_wildlife_excluded([]) == []
    assert db.photos.filter_out_wildlife_excluded(None) == []
    db.conn.execute(
        "UPDATE photos SET wildlife_excluded = 1 WHERE id = ?", (lib["b"],),
    )
    ids = [lib["c"], lib["b"], lib["a"], lib["c"]]
    assert db.photos.filter_out_wildlife_excluded(ids) == [lib["c"], lib["a"], lib["c"]]
    # Accepts any iterable; never needs an active workspace.
    db.set_active_workspace(None)
    assert db.photos.filter_out_wildlife_excluded(iter([lib["b"], lib["a"]])) == [lib["a"]]


def test_filter_out_wildlife_excluded_reads_instance_chunk_size(db, lib, monkeypatch):
    monkeypatch.setattr(db, "_FILTER_SUBJECT_CHUNK", 2)
    statements = _trace(db)
    db.photos.filter_out_wildlife_excluded([lib["a"], lib["b"], lib["c"]])
    db.conn.set_trace_callback(None)
    assert len(_sql(statements, "wildlife_excluded = 1")) == 2


# -- get_photos / get_photo_ids / count_filtered_photos ------------------------


def test_get_photos_pages_and_scopes(db, lib):
    rows = db.photos.list_page()
    assert [r["filename"] for r in rows] == ["c.jpg", "a.jpg", "b.jpg"]
    assert set(rows[0].keys()) == {
        c.strip() for c in PHOTO_COLS.split(",")
    }
    assert [r["filename"] for r in db.photos.list_page(page=2, per_page=2)] == ["b.jpg"]
    # page < 1 is clamped to the first page.
    assert [r["filename"] for r in db.photos.list_page(page=0, per_page=1)] == ["c.jpg"]
    assert [r["filename"] for r in db.photos.list_page(sort="name")] == [
        "a.jpg", "b.jpg", "c.jpg",
    ]
    assert [r["filename"] for r in db.photos.list_page(folder_id=lib["child"])] == ["c.jpg"]
    assert db.photos.get_ids() == [lib["c"], lib["a"], lib["b"]]
    assert db.photos.count_filtered() == 3


def test_listing_filters_agree(db, lib):
    db.photo_review.set_rating(lib["a"], 5)
    db.commit()
    db.photo_review.set_rating(lib["b"], 2)
    db.commit()
    db.photo_review.set_flag(lib["b"], "flagged")
    db.commit()
    db.photo_labels.set(lib["a"], "red")
    kid = db.add_keyword("Heron")
    db.tag_photo(lib["a"], kid)
    db.tag_photo(lib["b"], kid)
    db.commit()
    db.conn.execute(
        "UPDATE photos SET latitude = 1.0, longitude = 2.0 WHERE id = ?",
        (lib["c"],),
    )
    db.conn.commit()
    cases = [
        ({"rating_min": 3}, [lib["a"]]),
        ({"date_from": "2024-01-01"}, [lib["a"], lib["b"]]),
        ({"date_to": "2024-01-05"}, [lib["c"], lib["a"]]),
        ({"flag": "flagged"}, [lib["b"]]),
        ({"flag": "none"}, [lib["c"], lib["a"]]),
        ({"color_label": "red"}, [lib["a"]]),
        ({"keyword": "heron"}, [lib["a"], lib["b"]]),
        ({"keyword": "Heron", "keyword_match_case": True,
          "keyword_whole_word": True}, [lib["a"], lib["b"]]),
        ({"keyword": "   "}, [lib["c"], lib["a"], lib["b"]]),
        ({"location_status": "exif"}, [lib["c"]]),
        ({"folder_id": lib["root"]}, [lib["c"], lib["a"], lib["b"]]),
    ]
    for kwargs, expected in cases:
        assert [r["id"] for r in db.photos.list_page(**kwargs)] == expected, kwargs
        assert db.photos.get_ids(**kwargs) == expected, kwargs
        assert db.photos.count_filtered(**kwargs) == len(expected), kwargs


def test_listing_keyword_uses_distinct_and_color_join_param_order(db, lib):
    db.photo_labels.set(lib["a"], "red")
    statements = _trace(db)
    db.photos.list_page(keyword="x", color_label="red")
    db.photos.get_ids(keyword="x", color_label="red")
    db.photos.list_page(color_label="red")
    db.conn.set_trace_callback(None)
    selects = [s for s in statements if "FROM photos p" in s]
    assert "SELECT DISTINCT p.id" in selects[0]
    assert "SELECT DISTINCT p.id FROM photos p" in selects[1]
    assert "DISTINCT" not in selects[2].split("FROM")[0]
    ws = db._active_workspace_id
    assert f"pcl.workspace_id = {ws}" in selects[2]


def test_listing_collection_restriction(db, lib):
    cid = db.collections.add(
        "Just A", json.dumps([{"field": "photo_ids", "value": [lib["a"]]}]),
    )
    db.commit()
    assert [r["id"] for r in db.photos.list_page(collection_id=cid)] == [lib["a"]]
    assert db.photos.get_ids(collection_id=cid) == [lib["a"]]
    assert db.photos.count_filtered(collection_id=cid) == 1
    assert db.photos.get_position(lib["a"], collection_id=cid) == 0
    assert db.photos.get_position(lib["b"], collection_id=cid) is None
    summary = browse_queries.get_browse_summary(db, collection_id=cid)
    assert summary["total"] == 3
    assert summary["filtered_total"] == 1
    assert db.photos.get_calendar_data(2024, collection_id=cid)["days"] == {
        "2024-01-05": 1,
    }


def _listing(db, method):
    """``get_browse_summary`` lives in ``services/browse_queries.py``; the rest are on ``db.photos``."""
    if method == "get_browse_summary":
        return lambda *args, **kwargs: browse_queries.get_browse_summary(db, *args, **kwargs)
    return getattr(db.photos, method)


@pytest.mark.parametrize("method,args", [
    ("list_page", ()),
    ("get_ids", ()),
    ("count_filtered", ()),
    ("get_position", (1,)),
    ("get_browse_summary", ()),
    ("get_calendar_data", (2024,)),
])
def test_listing_missing_collection_raises(db, lib, method, args):
    with pytest.raises(ValueError, match="collection not found in active workspace"):
        _listing(db, method)(*args, collection_id=987654)


@pytest.mark.parametrize("method,args", [
    ("list_page", ()),
    ("get_ids", ()),
    ("count_filtered", ()),
    ("get_position", (1,)),
    ("get_browse_summary", ()),
    ("get_calendar_data", (2024,)),
    ("count_with_companions", ([1],)),
])
def test_listing_requires_active_workspace(db, lib, method, args):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        _listing(db, method)(*args)


def test_listing_routes_through_facade_helpers(db, lib, monkeypatch):
    """Folder subtrees come from the repository's own subtree read, and the
    collection queries and sort clauses from ``CollectionRepository``, so
    patches of those methods reach the listing reads."""
    calls = []
    from repositories.collections import CollectionRepository
    from repositories.photos import PhotoRepository

    real_subtree = PhotoRepository.get_folder_subtree_ids

    real_coll = CollectionRepository._build_collection_query
    real_sort = CollectionRepository._photo_sort_clause

    def subtree(self, fid):
        calls.append(("subtree", fid))
        return real_subtree(self, fid)

    def coll(self, cid):
        calls.append(("coll", cid))
        return real_coll(self, cid)

    def sort(self, key):
        calls.append(("sort", key))
        return real_sort(self, key)

    monkeypatch.setattr(PhotoRepository, "get_folder_subtree_ids", subtree)
    monkeypatch.setattr(CollectionRepository, "_build_collection_query", coll)
    monkeypatch.setattr(CollectionRepository, "_photo_sort_clause", sort)
    cid = db.collections.add("All", json.dumps([{"field": "all"}]))
    db.commit()
    db.photos.list_page(folder_id=lib["root"], collection_id=cid, sort="name")
    db.photos.get_ids(folder_id=lib["root"], collection_id=cid)
    db.photos.count_filtered(folder_id=lib["root"], collection_id=cid)
    db.photos.get_position(lib["a"], folder_id=lib["root"], collection_id=cid)
    browse_queries.get_browse_summary(db, folder_id=lib["root"], collection_id=cid)
    db.photos.get_calendar_data(2024, folder_id=lib["root"], collection_id=cid)
    root = lib["root"]
    assert calls == [
        ("subtree", root), ("coll", cid), ("sort", "name"),
        ("subtree", root), ("coll", cid), ("sort", "date"),
        ("subtree", root), ("coll", cid),
        ("subtree", root), ("coll", cid), ("sort", "date"),
        ("subtree", root), ("coll", cid),
        ("coll", cid), ("subtree", root),
    ]


# -- get_photo_position --------------------------------------------------------


def test_get_photo_position(db, lib):
    assert db.photos.get_position(lib["c"]) == 0
    assert db.photos.get_position(lib["b"]) == 2
    assert db.photos.get_position(lib["b"], sort="name") == 1
    assert db.photos.get_position(lib["b"], folder_id=lib["root"]) == 2
    assert db.photos.get_position(lib["f"]) is None
    assert db.photos.get_position(999999) is None


# -- get_calendar_data ---------------------------------------------------------


def test_get_calendar_data(db, lib):
    data = db.photos.get_calendar_data(2024)
    assert data == {
        "year": 2024,
        "days": {"2024-01-05": 1, "2024-03-07": 1},
        "min_year": 2023,
        "max_year": 2024,
    }
    assert db.photos.get_calendar_data(2023, folder_id=lib["child"])["days"] == {
        "2023-06-01": 1,
    }
    db.photo_review.set_rating(lib["b"], 4)
    db.commit()
    assert db.photos.get_calendar_data(
        2024, rules=[{"field": "rating", "op": ">=", "value": 4}],
    )["days"] == {"2024-03-07": 1}


def test_get_calendar_data_year_bounds_default_to_requested_year(db):
    data = db.photos.get_calendar_data(2031)
    assert data == {"year": 2031, "days": {}, "min_year": 2031, "max_year": 2031}


# -- get_browse_summary --------------------------------------------------------


def _detect(db, pid, conf, species, confidence=0.9, fp="fp"):
    det = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence, category) "
        "VALUES (?, ?, 'animal')", (pid, conf),
    ).lastrowid
    pred = db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) VALUES (?, 'm', ?, ?, ?)",
        (det, fp, species, confidence),
    ).lastrowid
    db.conn.commit()
    return pred


def test_get_browse_summary(db, lib, isolated_config):
    _detect(db, lib["a"], 0.9, "Heron")
    _detect(db, lib["b"], 0.9, "Heron")
    rejected = _detect(db, lib["c"], 0.9, "Egret")
    _detect(db, lib["f"], 0.9, "Crane")
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
        "VALUES (?, ?, 'rejected')", (rejected, db._active_workspace_id),
    )
    db.conn.commit()
    summary = browse_queries.get_browse_summary(db)
    assert summary == {
        "total": 3,
        "filtered_total": 3,
        "classified": 3,
        "unclassified": 0,
        "top_species": [{"species": "Heron", "count": 2}],
        "folder_counts": [
            {"folder_id": lib["root"], "name": "lib", "count": 2},
            {"folder_id": lib["child"], "name": "c", "count": 1},
        ],
        # Every visible photo has an animal box above the floor; no completed
        # detection run, quality, eye or misses pass has been recorded.
        "analysis": {
            "with_detection": 3, "with_detection_run": 0,
            "with_quality": 0, "with_eye": 0, "with_misses": 0,
        },
    }
    by_folder = browse_queries.get_browse_summary(db, folder_id=lib["child"])
    assert by_folder["filtered_total"] == 1
    assert by_folder["top_species"] == []
    db.photo_review.set_rating(lib["a"], 5)
    db.commit()
    ruled = browse_queries.get_browse_summary(
        db, rules=[{"field": "rating", "op": ">=", "value": 5}],
    )
    assert ruled["total"] == 3
    assert ruled["filtered_total"] == 1
    assert ruled["top_species"] == [{"species": "Heron", "count": 1}]


def test_get_browse_summary_reads_detector_floor_from_effective_config(
    db, lib, isolated_config, monkeypatch,
):
    _detect(db, lib["a"], 0.5, "Heron")
    seen = []
    real = db.get_effective_config

    def effective(config):
        seen.append(True)
        out = dict(real(config))
        out["detector_confidence"] = 0.6
        return out

    monkeypatch.setattr(db, "get_effective_config", effective)
    summary = browse_queries.get_browse_summary(db)
    assert seen == [True]
    assert summary["classified"] == 0
    assert summary["unclassified"] == 3
    assert summary["top_species"] == []


def test_get_browse_summary_rejects_malformed_rules(db, lib, isolated_config):
    with pytest.raises(ValueError):
        browse_queries.get_browse_summary(db, rules=[{"field": "no-such-field", "value": 1}])


# -- count_photos_with_companions ------------------------------------------------


def test_count_photos_with_companions(db, lib):
    db.conn.execute(
        "UPDATE photos SET companion_path = 'a.xmp' WHERE id IN (?, ?)",
        (lib["a"], lib["f"]),
    )
    db.conn.execute(
        "UPDATE photos SET companion_path = '' WHERE id = ?", (lib["b"],),
    )
    ids = [lib["a"], lib["a"], lib["b"], lib["c"], lib["f"]]
    assert db.photos.count_with_companions(ids) == 1
    assert db.photos.count_with_companions([]) == 0
    assert db.photos.count_with_companions(None) == 0


def test_count_photos_with_companions_chunks(db, lib):
    db.conn.execute(
        "UPDATE photos SET companion_path = 'a.xmp' WHERE id = ?", (lib["a"],),
    )
    ids = [lib["a"]] + list(range(10**6, 10**6 + _SQLITE_PARAM_CHUNK_SIZE))
    statements = _trace(db)
    assert db.photos.count_with_companions(ids) == 1
    db.conn.set_trace_callback(None)
    assert len(_sql(statements, "SELECT COUNT(*) AS n")) == 2


# -- resolve_photos_for_delete ---------------------------------------------------


def test_resolve_photos_for_delete(db, lib, tmp_path):
    assert db.photos.resolve_for_delete([]) == {"ids": [], "files": [], "_rows": []}
    assert db.photos.resolve_for_delete([999999]) == {
        "ids": [], "files": [], "_rows": [],
    }
    got = db.photos.resolve_for_delete([lib["a"], lib["a"]])
    assert got["ids"] == [lib["a"]]
    assert got["files"] == [{
        "photo_id": lib["a"],
        "folder_id": lib["root"],
        "folder_path": str(tmp_path / "lib"),
        "filename": "a.jpg",
        "companion_path": None,
    }]
    assert [r["id"] for r in got["_rows"]] == [lib["a"]]
    # Read-only: nothing is deleted and no transaction is left open.
    assert db.photos.get(lib["a"]) is not None
    assert not db.conn.in_transaction


def test_resolve_photos_for_delete_companions(db, lib):
    raw = _photo(db, lib["root"], "a.cr3")
    db.conn.execute(
        "UPDATE photos SET companion_path = 'a.cr3' WHERE id = ?", (lib["a"],),
    )
    db.conn.execute(
        "UPDATE photos SET companion_path = 'gone.cr3' WHERE id = ?", (lib["b"],),
    )
    db.conn.commit()
    without = db.photos.resolve_for_delete([lib["a"]])
    assert without["ids"] == [lib["a"]]
    got = db.photos.resolve_for_delete([lib["a"], lib["b"]], include_companions=True)
    assert sorted(got["ids"]) == sorted([lib["a"], lib["b"], raw])
    assert [f["photo_id"] for f in got["files"]] == [lib["a"], lib["b"], raw]
    # A companion the caller already selected is not added twice.
    both = db.photos.resolve_for_delete([lib["a"], raw], include_companions=True)
    assert [f["photo_id"] for f in both["files"]] == [lib["a"], raw]


# -- delete_photos ---------------------------------------------------------------


def test_delete_photos_removes_rows_and_dependents(db, lib, cache, monkeypatch):
    pruned = []
    monkeypatch.setattr(catalog_deletion, "prune_pipeline_cache_for_ids", lambda _db, ids: pruned.append(ids))
    kid = db.add_keyword("Heron")
    db.tag_photo(lib["a"], kid)
    db.commit()
    db.queue_change(lib["a"], "keyword_add", "Heron")
    db.commit()
    _detect(db, lib["a"], 0.9, "Heron")
    db.conn.execute(
        "UPDATE folders SET photo_count = 5 WHERE id = ?", (lib["root"],),
    )
    db.conn.commit()

    result = catalog_deletion.delete_photos(db, [lib["a"]])

    assert result["deleted"] == 1
    assert result["ids"] == [lib["a"]]
    assert [f["filename"] for f in result["files"]] == ["a.jpg"]
    assert not db.conn.in_transaction
    with _reader(db) as r:
        for table in ("photo_keywords", "pending_changes", "detections"):
            assert r.execute(
                f"SELECT COUNT(*) FROM {table} WHERE photo_id = ?", (lib["a"],),
            ).fetchone()[0] == 0, table
        assert r.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
        assert r.execute(
            "SELECT COUNT(*) FROM photos WHERE id = ?", (lib["a"],),
        ).fetchone()[0] == 0
        assert r.execute(
            "SELECT photo_count FROM folders WHERE id = ?", (lib["root"],),
        ).fetchone()[0] == 4
    assert pruned == [[lib["a"]]]
    assert len(cache.invalidated) == 1


def test_delete_photos_nothing_to_delete(db, lib, cache, monkeypatch):
    pruned = []
    monkeypatch.setattr(catalog_deletion, "prune_pipeline_cache_for_ids", lambda _db, ids: pruned.append(ids))
    assert catalog_deletion.delete_photos(db, []) == {"deleted": 0, "ids": [], "files": []}
    assert catalog_deletion.delete_photos(db, [999999]) == {"deleted": 0, "ids": [], "files": []}
    assert pruned == []
    assert cache.invalidated == []


def test_delete_photos_goes_through_facade_resolver(db, lib, monkeypatch):
    from repositories.photos import PhotoRepository

    seen = []
    real = PhotoRepository.resolve_for_delete

    def resolver(repo, photo_ids, include_companions=False):
        seen.append((list(photo_ids), include_companions))
        return real(repo, photo_ids, include_companions=include_companions)

    monkeypatch.setattr(PhotoRepository, "resolve_for_delete", resolver)
    catalog_deletion.delete_photos(db, [lib["b"]], include_companions=True)
    assert seen == [([lib["b"]], True)]


def test_delete_photos_cleans_collection_photo_id_rules(db, lib):
    flat = db.collections.add(
        "flat", json.dumps([{"field": "photo_ids", "value": [lib["a"], lib["b"]]}]),
    )
    db.commit()
    nested = db.collections.add("nested", json.dumps([
        {"match": "any", "rules": [
            {"field": "photo_ids", "value": [lib["a"]]},
            {"field": "rating", "op": ">=", "value": 3},
        ]},
    ]))
    db.commit()
    odd = json.dumps([
        {"field": "photo_ids", "value": "not-a-list"},
        {"field": "photo_ids"},
        "junk",
    ])
    untouched = db.collections.add("odd", odd)
    db.commit()
    other = db.collections.add(
        "other", json.dumps([{"field": "photo_ids", "value": [lib["c"]]}]),
    )
    db.commit()
    # Photos are global, so a collection in another workspace is cleaned too.
    foreign = db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        ("foreign", json.dumps([{"field": "photo_ids", "value": [lib["a"], lib["f"]]}]),
         lib["other_ws"]),
    ).lastrowid
    db.conn.commit()

    statements = _trace(db)
    catalog_deletion.delete_photos(db, [lib["a"]])
    db.conn.set_trace_callback(None)

    rules = dict(db.conn.execute("SELECT name, rules FROM collections").fetchall())
    assert json.loads(rules["flat"]) == [{"field": "photo_ids", "value": [lib["b"]]}]
    assert json.loads(rules["nested"])[0]["rules"][0] == {
        "field": "photo_ids", "value": [],
    }
    assert rules["odd"] == odd
    assert json.loads(rules["other"]) == [{"field": "photo_ids", "value": [lib["c"]]}]
    assert json.loads(rules["foreign"]) == [
        {"field": "photo_ids", "value": [lib["f"]]},
    ]
    updates = _sql(statements, "UPDATE collections SET rules")
    assert [s.rsplit("WHERE id = ", 1)[1] for s in updates] == [
        str(flat), str(nested), str(foreign),
    ]
    assert untouched not in (flat, nested) and other not in (flat, nested)


def test_deleted_photo_id_reused_by_sqlite_does_not_join_foreign_collection(db, lib):
    # ``photos.id`` has no AUTOINCREMENT, so SQLite hands the highest freed
    # id to the next insert. A static collection in another workspace that
    # still named the deleted id would silently gain the new photo.
    doomed = _photo(db, lib["root"], "doomed.jpg")
    workspace_folders.add_workspace_folder(db, lib["other_ws"], lib["root"])
    foreign = db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        ("foreign", json.dumps([{"field": "photo_ids", "value": [doomed]}]),
         lib["other_ws"]),
    ).lastrowid
    db.conn.commit()

    catalog_deletion.delete_photos(db, [doomed])
    reused = _photo(db, lib["root"], "unrelated.jpg")

    assert reused == doomed
    db.set_active_workspace(lib["other_ws"])
    assert db.collections.get_photo_ids(foreign) == []


def test_delete_photos_expires_drained_move_provenance(db, lib, tmp_path):
    lib_path = str(tmp_path / "lib")
    dest = workspace_folders.add_folder(db, str(tmp_path / "dest"), name="dest")
    moved_a = _photo(db, dest, "a.cr3")
    moved_b = _photo(db, dest, "b.cr3")
    db.conn.execute(
        "UPDATE photos SET last_move_source_folder_path = ? WHERE id IN (?, ?)",
        (lib_path, moved_a, moved_b),
    )
    # A same-stem sibling that remains in the source keeps b's proof alive.
    _photo(db, lib["root"], "b.cr3")
    db.conn.commit()

    catalog_deletion.delete_photos(db, [lib["a"], lib["b"]])

    got = dict(db.conn.execute(
        "SELECT id, last_move_source_folder_path FROM photos WHERE id IN (?, ?)",
        (moved_a, moved_b),
    ).fetchall())
    assert got == {moved_a: None, moved_b: lib_path}


def test_delete_photos_inside_a_unit_leaves_commit_and_prune_to_its_owner(db, lib, cache, monkeypatch):
    pruned = []
    monkeypatch.setattr(catalog_deletion, "prune_pipeline_cache_for_ids", lambda _db, ids: pruned.append(ids))
    with pytest.raises(RuntimeError, match="owner rolls back"), db.transaction():
        result = catalog_deletion.delete_photos(db, [lib["a"]])
        assert result["ids"] == [lib["a"]]
        assert db.conn.in_transaction
        with _reader(db) as r:
            assert r.execute(
                "SELECT COUNT(*) FROM photos WHERE id = ?", (lib["a"],),
            ).fetchone()[0] == 1
        raise RuntimeError("owner rolls back")
    assert pruned == []
    assert len(cache.invalidated) == 1
    assert db.photos.get(lib["a"]) is not None


def test_delete_photos_rolls_back_on_error_and_still_invalidates(db, lib, cache, monkeypatch):
    pruned = []
    monkeypatch.setattr(catalog_deletion, "prune_pipeline_cache_for_ids", lambda _db, ids: pruned.append(ids))
    kid = db.add_keyword("Heron")
    db.tag_photo(lib["a"], kid)
    db.commit()
    db.conn.execute(
        "CREATE TEMP TRIGGER block_photo_delete BEFORE DELETE ON photos "
        "BEGIN SELECT RAISE(ABORT, 'blocked'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="blocked"):
        catalog_deletion.delete_photos(db, [lib["a"]])
    assert not db.conn.in_transaction
    # The earlier keyword delete was rolled back with the rest.
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photo_keywords WHERE photo_id = ?", (lib["a"],),
    ).fetchone()[0] == 1
    assert len(cache.invalidated) == 1
    assert pruned == []


def test_delete_photos_error_inside_a_unit_rolls_back_only_its_savepoint(db, lib, cache):
    kid = db.add_keyword("Heron")
    db.tag_photo(lib["a"], kid)
    db.commit()
    db.conn.execute(
        "CREATE TEMP TRIGGER block_photo_delete BEFORE DELETE ON photos "
        "BEGIN SELECT RAISE(ABORT, 'blocked'); END"
    )
    with db.transaction():
        db.conn.execute("UPDATE photos SET rating = 4 WHERE id = ?", (lib["b"],))
        with pytest.raises(sqlite3.IntegrityError, match="blocked"):
            catalog_deletion.delete_photos(db, [lib["a"]])
        # The failed delete rolled back to its savepoint: the caller's own
        # write and the photo's keywords are both still there.
        assert db.conn.in_transaction
        assert db.conn.execute(
            "SELECT COUNT(*) FROM photo_keywords WHERE photo_id = ?", (lib["a"],),
        ).fetchone()[0] == 1
    assert db.photos.get(lib["b"])["rating"] == 4
    assert len(cache.invalidated) == 1


def test_delete_photos_resolves_workspace_mid_transaction(db, lib, cache):
    """The active workspace is read after the dependent-row deletes, so a
    missing workspace fails inside the transaction and rolls it back."""
    kid = db.add_keyword("Heron")
    db.tag_photo(lib["a"], kid)
    db.commit()
    db.set_active_workspace(None)
    statements = _trace(db)
    with pytest.raises(RuntimeError):
        catalog_deletion.delete_photos(db, [lib["a"]])
    db.conn.set_trace_callback(None)
    stmts = [s.strip() for s in statements]
    first_delete = next(
        i for i, s in enumerate(stmts) if s.startswith("DELETE FROM photo_keywords")
    )
    assert "ROLLBACK" in stmts[first_delete:]
    assert not any("FROM collections" in s for s in stmts)
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photo_keywords WHERE photo_id = ?", (lib["a"],),
    ).fetchone()[0] == 1
    assert len(cache.invalidated) == 1


def test_delete_photos_chunks_every_in_clause(db, lib, monkeypatch):
    monkeypatch.setattr(catalog_deletion, "prune_pipeline_cache_for_ids", lambda _db, ids: None)
    # The dependents chunk at ``db._chunks``' size; the photo rows go
    # through ``photo_row_deletion``, which chunks at ``sql_chunks``'. Just
    # past the larger of the two splits every statement in two.
    from sql_chunks import SQL_PARAM_CHUNK

    ids = [_photo(db, lib["root"], f"z{i}.jpg")
           for i in range(max(_SQLITE_PARAM_CHUNK_SIZE, SQL_PARAM_CHUNK) + 3)]
    statements = _trace(db)
    result = catalog_deletion.delete_photos(db, ids)
    db.conn.set_trace_callback(None)
    assert result["deleted"] == len(ids)
    statements = list(dict.fromkeys(s.strip() for s in statements))
    for prefix in ("DELETE FROM photo_keywords",
                   "DELETE FROM photo_embedded_keyword_offered",
                   "DELETE FROM pending_changes",
                   "DELETE FROM detections", "DELETE FROM photos"):
        assert len([s for s in statements if s.startswith(prefix)]) == 2, prefix


def test_delete_photos_sql_order(db, lib, monkeypatch, tmp_path):
    monkeypatch.setattr(catalog_deletion, "prune_pipeline_cache_for_ids", lambda _db, ids: None)
    statements = _trace(db)
    catalog_deletion.delete_photos(db, [lib["a"]])
    db.conn.set_trace_callback(None)
    a = lib["a"]
    root = lib["root"]
    # FK cascade sub-programs re-report their statement; dedupe keeps order.
    stmts = list(dict.fromkeys(s.strip() for s in statements))
    head = stmts.index(f"DELETE FROM photo_keywords WHERE photo_id IN ({a})")
    assert stmts[head:stmts.index("COMMIT") + 1] == [
        f"DELETE FROM photo_keywords WHERE photo_id IN ({a})",
        f"DELETE FROM photo_embedded_keyword_offered WHERE photo_id IN ({a})",
        f"DELETE FROM pending_changes WHERE photo_id IN ({a})",
        f"DELETE FROM detections WHERE photo_id IN ({a})",
        # ``photo_row_deletion``: the rows, then their collection entries.
        f"DELETE FROM photos WHERE id IN ({a})",
        "SELECT id, rules FROM collections",
        f"SELECT filename FROM photos WHERE folder_id = {root}",
        "SELECT id, filename, last_move_source_folder_path FROM photos "
        f"WHERE last_move_source_folder_path IN ('{tmp_path / 'lib'}')",
        f"UPDATE folders SET photo_count = photo_count - 1 WHERE id = {root}",
        "COMMIT",
    ]


# -- prune_pipeline_cache_for_ids ----------------------------------------------


def test_prune_pipeline_cache_for_ids(db, monkeypatch):
    from services import result_files

    calls = []
    monkeypatch.setattr(result_files, "prune_photo_results", lambda *a: calls.append(a))
    catalog_deletion.prune_pipeline_cache_for_ids(db, [])
    assert calls == []
    catalog_deletion.prune_pipeline_cache_for_ids(db, [1, 2])
    import os
    workspace_ids = [workspace["id"] for workspace in db.workspaces.list_all()]
    assert calls == [(os.path.dirname(db._db_path), [1, 2], workspace_ids)]
    db.set_active_workspace(None)
    catalog_deletion.prune_pipeline_cache_for_ids(db, [3])
    assert calls[-1] == (os.path.dirname(db._db_path), [3], workspace_ids)


def test_prune_pipeline_cache_for_ids_swallows_errors_but_not_interrupts(db, monkeypatch):
    from services import result_files

    def boom(*a):
        raise OSError("disk")

    monkeypatch.setattr(result_files, "prune_photo_results", boom)
    catalog_deletion.prune_pipeline_cache_for_ids(db, [1])

    def interrupt(*a):
        raise KeyboardInterrupt

    monkeypatch.setattr(result_files, "prune_photo_results", interrupt)
    with pytest.raises(KeyboardInterrupt):
        catalog_deletion.prune_pipeline_cache_for_ids(db, [1])


# -- quality writers -------------------------------------------------------------


def test_update_photo_sharpness_commits(db, lib):
    db.photos.update_sharpness(lib["a"], 12.5)
    db.commit()
    assert not db.conn.in_transaction
    with _reader(db) as r:
        assert r.execute(
            "SELECT sharpness FROM photos WHERE id = ?", (lib["a"],),
        ).fetchone()[0] == 12.5


def test_update_photo_quality_commits_and_overwrites_all_fields(db, lib):
    db.photos.update_quality(lib["a"], subject_sharpness=1.0, subject_size=2.0,
                            quality_score=3.0, sharpness=4.0)
    db.commit()
    with _reader(db) as r:
        row = r.execute(
            "SELECT subject_sharpness, subject_size, quality_score, sharpness "
            "FROM photos WHERE id = ?", (lib["a"],),
        ).fetchone()
    assert tuple(row) == (1.0, 2.0, 3.0, 4.0)
    db.photos.update_quality(lib["a"], quality_score=9.0)
    db.commit()
    row = db.conn.execute(
        "SELECT subject_sharpness, subject_size, quality_score, sharpness "
        "FROM photos WHERE id = ?", (lib["a"],),
    ).fetchone()
    assert tuple(row) == (None, None, 9.0, None)
    assert not db.conn.in_transaction


def test_quality_writers_never_commit(db, lib, monkeypatch):
    import db as db_module

    commits = []
    real = db_module.commit_with_retry
    monkeypatch.setattr(
        db_module, "commit_with_retry",
        lambda conn, *a, **kw: (commits.append(1), real(conn, *a, **kw))[1],
    )
    db.photos.update_sharpness(lib["a"], 1.0)
    db.photos.update_quality(lib["a"], sharpness=2.0)
    assert commits == []
    assert db.in_transaction
    db.commit()


# -- working copy and thumbnail columns ------------------------------------------


def _working_copy_state(db, pid):
    with _reader(db) as r:
        return tuple(r.execute(
            "SELECT working_copy_path, working_copy_evicted_mtime, width, "
            "height FROM photos WHERE id = ?", (pid,),
        ).fetchone())


def test_get_photo_working_copy_path(db, lib):
    assert db.photos.working_copy_path(lib["a"]) is None
    db.photos.record_generated_original(lib["a"], "working/1.jpg", tracked=True)
    db.commit()
    assert db.photos.working_copy_path(lib["a"]) == "working/1.jpg"
    assert db.photos.working_copy_path(99999) is None


def test_record_generated_original_tracked_stores_path_and_clears_eviction(db, lib):
    db.conn.execute(
        "UPDATE photos SET working_copy_evicted_mtime = 7, width = 5, "
        "height = 6 WHERE id = ?", (lib["a"],),
    )
    db.conn.commit()
    db.photos.record_generated_original(lib["a"], "working/a.jpg", tracked=True)
    db.commit()
    assert not db.conn.in_transaction
    assert _working_copy_state(db, lib["a"]) == ("working/a.jpg", None, 5, 6)


def test_record_generated_original_untracked_marks_evicted_at_source_mtime(db, lib):
    db.photos.record_generated_original(lib["a"], "working/a.jpg", tracked=True)
    db.photos.record_generated_original(lib["a"], "ignored", tracked=False,
                                 dimensions=(40, 30))
    db.commit()
    assert _working_copy_state(db, lib["a"]) == (None, 1.0, 40, 30)
    db.conn.execute("UPDATE photos SET file_mtime = NULL WHERE id = ?", (lib["b"],))
    db.conn.commit()
    db.photos.record_generated_original(lib["b"], None, tracked=False)
    db.commit()
    assert _working_copy_state(db, lib["b"])[:2] == (None, -1)


def test_record_generated_original_leaves_commit_to_caller(db, lib):
    # Neither ends a transaction the caller opened nor commits an open unit.
    db.begin_immediate()
    db.photos.record_generated_original(lib["a"], "working/a.jpg", tracked=True)
    assert db.in_transaction
    db.commit()
    with db.transaction():
        db.photos.record_generated_original(lib["b"], "working/b.jpg", tracked=True)
        assert db.in_transaction
    assert _working_copy_state(db, lib["b"])[0] == "working/b.jpg"


def test_set_photo_thumb_path_commits(db, lib):
    db.photos.set_thumb_path(lib["a"], f"{lib['a']}.jpg")
    db.commit()
    assert not db.conn.in_transaction
    with _reader(db) as r:
        assert r.execute(
            "SELECT thumb_path FROM photos WHERE id = ?", (lib["a"],),
        ).fetchone()[0] == f"{lib['a']}.jpg"


def _thumb_paths(db):
    with _reader(db) as r:
        return {
            row["id"]: row["thumb_path"]
            for row in r.execute("SELECT id, thumb_path FROM photos")
        }


def test_clear_all_photo_thumb_paths_commits(db, lib):
    for pid in (lib["a"], lib["f"]):
        db.photos.set_thumb_path(pid, f"{pid}.jpg")
        db.commit()
    db.photos.clear_all_thumb_paths()
    db.commit()
    assert not db.conn.in_transaction
    assert set(_thumb_paths(db).values()) == {None}


def test_clear_photo_thumb_paths_clears_only_named_photos_in_chunks_of_900(db, lib):
    for pid in (lib["a"], lib["b"], lib["f"]):
        db.photos.set_thumb_path(pid, f"{pid}.jpg")
        db.commit()
    statements = _trace(db)
    db.photos.clear_thumb_paths([lib["a"]] + list(range(100_000, 100_899)) + [lib["f"]])
    db.commit()
    db.conn.set_trace_callback(None)
    assert not db.conn.in_transaction
    assert len(_sql(statements, "SET thumb_path = NULL")) == 2
    paths = _thumb_paths(db)
    assert paths[lib["a"]] is None
    assert paths[lib["f"]] is None
    assert paths[lib["b"]] == f"{lib['b']}.jpg"


def test_count_catalog_photos_spans_every_workspace(db, lib):
    assert db.photos.count_all() == 4
    assert db.photos.count_in_workspace() == 3
    db.set_active_workspace(None)
    assert db.photos.count_all() == 4


def _failure_state(db, pid):
    with _reader(db) as r:
        return tuple(r.execute(
            "SELECT working_copy_evicted_mtime, working_copy_failed_at, "
            "working_copy_failed_mtime, working_copy_failed_source "
            "FROM photos WHERE id = ?", (pid,),
        ).fetchone())


def test_clear_working_copy_evictions_resets_evicted_rows(db, lib, monkeypatch):
    import db as db_module

    def mark(pid, *, evicted, source, companion):
        db.conn.execute(
            "UPDATE photos SET working_copy_evicted_mtime = ?, "
            "working_copy_failed_at = 'then', working_copy_failed_mtime = 3.0, "
            "working_copy_failed_source = ?, companion_path = ? WHERE id = ?",
            (evicted, source, companion, pid),
        )

    mark(lib["a"], evicted=5.0, source="source", companion="a.jpg")
    mark(lib["b"], evicted=5.0, source="source", companion=None)
    mark(lib["c"], evicted=5.0, source="working_copy", companion="c.jpg")
    mark(lib["f"], evicted=None, source="source", companion="f.jpg")
    db.conn.commit()
    commits = []
    real = db_module.commit_with_retry
    monkeypatch.setattr(
        db_module, "commit_with_retry",
        lambda conn, *a, **kw: (commits.append(conn), real(conn, *a, **kw))[1],
    )

    db.photos.clear_working_copy_evictions()
    assert commits == []  # the repository never commits
    db.commit()

    assert not db.in_transaction
    # Evicted RAW-source failure on a photo with a companion: all cleared.
    assert _failure_state(db, lib["a"]) == (None, None, None, None)
    # Without a companion, or for another source, the failure stays.
    assert _failure_state(db, lib["b"]) == (None, "then", 3.0, "source")
    assert _failure_state(db, lib["c"]) == (None, "then", 3.0, "working_copy")
    # A row that was never evicted is untouched, in any workspace.
    assert _failure_state(db, lib["f"]) == (None, "then", 3.0, "source")


def test_count_catalog_photos_counts_every_folder_status(db, lib):
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (lib["child"],),
    )
    db.conn.commit()
    assert db.photos.count_in_workspace() == 3
    assert db.photos.count_all() == 4


# -- structure -----------------------------------------------------------------

# The forwarding wrappers this domain used to have on ``Database``; callers
# reach the same methods through ``db.photos`` now.
REMOVED_PHOTO_WRAPPERS = [
    "filter_out_wildlife_excluded", "get_photo",
    "get_photo_filenames", "get_existing_photo_ids", "get_photos_by_ids",
    "get_photo_folder_statuses",
    "count_photos", "count_photos_in_workspace", "photos_by_paths",
    "get_calendar_data", "get_photos", "get_photo_ids", "get_photo_position",
    "count_filtered_photos",
    "count_photos_with_companions", "resolve_photos_for_delete",
    "update_photo_sharpness", "update_photo_quality",
    "get_photo_with_folder_path", "get_photo_flags_ratings_and_eyes",
    "get_photo_file_in_workspace",
    "get_workspace_photos_in_folders",
    "get_photo_working_copy_path", "record_generated_original",
    "set_photo_thumb_path", "clear_working_copy_evictions",
    "get_photo_ids_at_paths",
    "clear_all_photo_thumb_paths", "clear_photo_thumb_paths",
    "count_catalog_photos",
    "get_photo_ids_in_folders",
]


def _self_attrs(name_or_fn):
    fn_obj = getattr(Database, name_or_fn) if isinstance(name_or_fn, str) else name_or_fn
    source = textwrap.dedent(inspect.getsource(fn_obj))
    fn = ast.parse(source).body[0]
    return {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }


def test_delete_photos_lives_in_the_deletion_service():
    """``catalog_deletion.delete_photos`` deletes through ``db.photos``, never the connection."""
    assert not hasattr(Database, "delete_photos")
    source = textwrap.dedent(inspect.getsource(catalog_deletion.delete_photos))
    assert ".conn" not in source
    assert "db.photos.resolve_for_delete" in source and "db.photos.delete" in source


def test_photos_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.photos``; Database keeps no aliases."""
    for name in REMOVED_PHOTO_WRAPPERS:
        assert not hasattr(Database, name), f"Database.{name} came back; call db.photos"
    accessor = Database.__dict__["photos"]
    assert isinstance(accessor, property)
    attrs = _self_attrs(accessor.fget)
    assert "_photos_repository" in attrs
    assert "conn" not in attrs


def test_photos_is_a_fresh_repository_per_access(db):
    """A new repository each time, with the workspace passed uncalled.

    Catalog-wide reads work with no active workspace; scoped ones raise.
    """
    from repositories.photos import PhotoRepository

    first, second = db.photos, db.photos
    assert isinstance(first, PhotoRepository)
    assert first is not second
    assert first.conn is db.conn
    assert first._resolve_workspace_id == db._ws_id
    db.set_active_workspace(None)
    assert db.photos.count_all() == 0
    with pytest.raises(RuntimeError):
        db.photos.count()


def test_composition_stays_on_the_facade():
    """Cross-domain calls and side effects are made from the façade, and the
    helpers the listing reads consult are bound from it when the repository
    is built, so monkeypatches of these Database methods keep applying."""
    deletion = textwrap.dedent(inspect.getsource(catalog_deletion.delete_photos))
    for call in ("db.invalidate_new_images_cache_for_folders",
                 "prune_pipeline_cache_for_ids(db", "db.require_workspace_id"):
        assert call in deletion
    summary = textwrap.dedent(inspect.getsource(browse_queries.get_browse_summary))
    assert "db.get_effective_config" in summary and "db.photos.get_browse_summary" in summary
    assert not hasattr(Database, "get_browse_summary")
    assert {"_FILTER_SUBJECT_CHUNK", "_ws_id"} <= _self_attrs("_photos_repository")


def test_add_photo_inserts_through_the_repository_then_runs_the_hook():
    """``photo_ingest.add_photo`` composes the insert and the duplicate hook."""
    tree = ast.parse(inspect.getsource(photo_ingest))
    impl = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "add_photo"
        and not any(
            isinstance(d, ast.Name) and d.id == "overload" for d in node.decorator_list
        )
    ]
    assert len(impl) == 1
    attrs = {node.attr for node in ast.walk(impl[0]) if isinstance(node, ast.Attribute)}
    assert {"photos", "add", "check_and_resolve_duplicates_for_hash"} <= attrs
    assert "conn" not in attrs
    assert not hasattr(Database, "add_photo")


def test_photo_repository_takes_no_facade(db):
    import repositories.photos as module

    assert not hasattr(module, "FACADE_METHODS")
    assert "facade" not in inspect.signature(module.PhotoRepository).parameters
    repo = db._photos_repository()
    # The rules engine is a CollectionRepository on the same connection.
    assert repo._collections().conn is db.conn


def test_photo_repository_imports_no_db_code():
    import repositories.photos as module

    tree = ast.parse(inspect.getsource(module))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert not imported & {"db", "config"}
