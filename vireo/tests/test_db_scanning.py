"""Behavior and structure pins for the scanner's catalog domains.

``scanner`` reaches the catalog through four accessors instead of its own
connection: ``db.scanning`` (the per-file and per-folder catalog pass),
``db.companion_pairing`` (RAW+JPEG pairing), ``db.working_copies`` (the
working-copy extraction pass) and ``db.keyword_provenance`` (the companion
keyword copy, beside every other ``photo_keywords`` writer). The behavior
tests cover the guards those writes carry; the structural tests at the end
keep their SQL in ``vireo/repositories/`` and the scanner off the connection.
"""

import ast
import contextlib
import inspect
import sqlite3
import textwrap
from pathlib import Path

import pytest
from db import Database
from repositories.companion_pairing import CompanionPairingRepository
from repositories.keyword_provenance import KeywordProvenanceRepository
from repositories.scanning import ScanningRepository
from repositories.working_copies import WorkingCopyRepository

VIREO = Path(__file__).resolve().parents[1]


def _photo(db, folder_id, filename, **columns):
    pid = db.add_photo(
        folder_id=folder_id, filename=filename,
        extension="." + filename.rsplit(".", 1)[1].lower(),
        file_size=columns.pop("file_size", 100),
        file_mtime=columns.pop("file_mtime", 1.0),
    )
    if columns:
        db.scanning.update_photo(pid, [f"{name}=?" for name in columns], list(columns.values()))
    db.commit()
    return pid


def _committed(db, sql, params=()):
    with contextlib.closing(sqlite3.connect(db._db_path)) as other:
        return other.execute(sql, params).fetchall()


# -- db.scanning -----------------------------------------------------------------


def test_set_folder_status_guard_only_moves_partial_rows(db, tmp_path):
    a = db.add_folder(str(tmp_path / "a"), name="a")
    b = db.add_folder(str(tmp_path / "b"), name="b")
    db.scanning.set_folder_status("partial", folder_ids=[a])
    db.scanning.set_folder_status("missing", folder_ids=[b])
    db.scanning.set_folder_status(
        "ok", paths=[str(tmp_path / "a"), str(tmp_path / "b")], only_from_partial=True,
    )
    db.commit()
    statuses = dict(_committed(db, "SELECT id, status FROM folders WHERE id IN (?, ?)", (a, b)))
    assert statuses == {a: "ok", b: "missing"}


def test_set_xmp_mtime_records_and_clears(db, tmp_path):
    fid = db.add_folder(str(tmp_path), name="p")
    pid = _photo(db, fid, "a.jpg")
    db.scanning.set_xmp_mtime(pid, 12.5)
    assert db.conn.execute("SELECT xmp_mtime FROM photos WHERE id=?", (pid,)).fetchone()[0] == 12.5
    db.scanning.set_xmp_mtime(pid, None)
    assert db.conn.execute("SELECT xmp_mtime FROM photos WHERE id=?", (pid,)).fetchone()[0] is None


def test_incremental_baseline_rows_match_a_trailing_separator_folder(db, tmp_path):
    import os

    fid = db.add_folder(str(tmp_path) + os.sep, name="p")
    pid = _photo(db, fid, "a.jpg")
    rows = list(db.scanning.incremental_baseline_rows([(str(tmp_path), "a.jpg")]))
    assert [row["id"] for row in rows] == [pid]
    assert rows[0]["exif_extracted"] == 0
    assert rows[0]["summary_needs_extract"] == 1


def test_broken_metadata_rows_select_photos_a_rescan_should_repair(db, tmp_path):
    fid = db.add_folder(str(tmp_path / "b"), name="b")
    other = db.add_folder(str(tmp_path / "a"), name="a")
    no_time = _photo(db, fid, "z.jpg")
    small_raw = _photo(db, fid, "c.nef", timestamp="2024-01-01T00:00:00", width=640)
    big_raw = _photo(db, fid, "d.nef", timestamp="2024-01-01T00:00:00", width=6000)
    small_jpg = _photo(db, other, "e.jpg", timestamp="2024-01-01T00:00:00", width=640)
    has_exif = _photo(db, other, "f.jpg", exif_data="{}")
    other_no_time = _photo(db, other, "y.jpg")
    rows = db.scanning.broken_metadata_rows(
        [no_time, small_raw, big_raw, small_jpg, has_exif, other_no_time], (".nef",),
    )
    assert [(r["folder_path"], r["filename"]) for r in rows] == [
        (str(tmp_path / "a"), "y.jpg"),
        (str(tmp_path / "b"), "c.nef"),
        (str(tmp_path / "b"), "z.jpg"),
    ]


def test_collection_change_counter_counts_this_connections_writes(db):
    repo = db.scanning
    repo.install_collection_change_counter()
    before = repo.collection_revision()
    db.conn.execute(
        "INSERT INTO collections (workspace_id, name, rules) VALUES (?, 'c', '[]')",
        (db.active_workspace_id,),
    )
    db.rollback()
    # A rolled-back write still advanced the counter: a revision is never reused.
    assert db.scanning.collection_revision() == before + 1
    # Installing again keeps the counter (and does not duplicate the triggers).
    db.scanning.install_collection_change_counter()
    assert db.scanning.collection_revision() == before + 1


# -- db.companion_pairing --------------------------------------------------------


def test_claim_companion_refuses_a_changed_owner(db, tmp_path):
    fid = db.add_folder(str(tmp_path), name="p")
    raw = _photo(db, fid, "IMG.cr3")
    pairing = db.companion_pairing
    assert pairing.claim_companion(raw, fid, "IMG.cr3", "IMG.jpg")
    # Re-claiming the same file is idempotent; a different file is refused.
    assert pairing.claim_companion(raw, fid, "IMG.cr3", "IMG.jpg")
    assert not pairing.claim_companion(raw, fid, "IMG.cr3", "IMG.jpeg")
    assert not pairing.claim_companion(raw, fid, "OTHER.cr3", "IMG.jpg")
    assert pairing.lock_owner(raw, fid, "IMG.cr3", "IMG.jpg")
    assert not pairing.lock_owner(raw, fid, "IMG.cr3", "IMG.jpeg")
    db.rollback()


def test_rows_for_stem_reads_the_stem_range_and_paired_raws(db, tmp_path):
    fid = db.add_folder(str(tmp_path), name="p")
    raw = _photo(db, fid, "IMG.cr3", companion_path="IMG.jpg")
    jpeg = _photo(db, fid, "IMG.jpeg")
    _photo(db, fid, "IMG_2.jpg")
    _photo(db, fid, "IMH.jpg")
    ids = {row["id"] for row in db.companion_pairing.rows_for_stem(fid, "IMG")}
    assert ids == {raw, jpeg}


def test_owner_of_prefers_the_raw_with_a_stored_identity(db, tmp_path):
    fid = db.add_folder(str(tmp_path), name="p")
    _photo(db, fid, "IMG.cr3", companion_path="IMG.jpg")
    vouched = _photo(db, fid, "IMG.nef", companion_path="IMG.jpg")
    db.companion_pairing.record_identity(vouched, "IMG.jpg", 10, None, "h", 2.0)
    db.commit()
    assert db.companion_pairing.companion_names(fid) == {"IMG.jpg"}
    assert db.companion_pairing.owner_of(fid, "IMG.jpg")["owner_id"] == vouched


# -- db.working_copies -----------------------------------------------------------


def _wc_snapshot(db, pid):
    return db.conn.execute(
        "SELECT p.id, p.folder_id, p.filename, p.companion_path, p.file_size,"
        " p.file_mtime, f.path AS folder_path"
        " FROM photos p JOIN folders f ON f.id = p.folder_id WHERE p.id = ?",
        (pid,),
    ).fetchone()


def test_set_working_copy_requires_the_snapshot_identity(db, tmp_path):
    fid = db.add_folder(str(tmp_path), name="p")
    pid = _photo(db, fid, "a.nef")
    snapshot = _wc_snapshot(db, pid)
    db.companion_pairing.set_companion_path(pid, "a.jpg")
    cursor = db.working_copies.set_working_copy(
        snapshot, f"working/{pid}.jpg", raw_failed_then_companion=False,
    )
    assert cursor.rowcount == 0
    db.rollback()
    cursor = db.working_copies.set_working_copy(
        _wc_snapshot(db, pid), f"working/{pid}.jpg", raw_failed_then_companion=True,
    )
    assert cursor.rowcount == 1
    db.commit()
    row = _committed(
        db, "SELECT working_copy_path, working_copy_failed_source FROM photos WHERE id=?", (pid,),
    )[0]
    assert row == (f"working/{pid}.jpg", "source")
    assert db.working_copies.path_is_claimed(f"working/{pid}.jpg")
    assert db.working_copies.count_with_working_copy() == 1


def test_quota_eviction_writes_only_touch_rows_still_on_the_expected_path(db, tmp_path):
    fid = db.add_folder(str(tmp_path), name="p")
    stale = _photo(db, fid, "a.nef", working_copy_path="working/a.jpg")
    replaced = _photo(db, fid, "b.nef", working_copy_path="working/b2.jpg")
    evicted = _photo(db, fid, "c.nef", file_mtime=4.0, working_copy_path="working/c.jpg")
    untouched = _photo(db, fid, "d.nef", file_mtime=None)
    assert {r["id"]: r["working_copy_path"] for r in db.working_copies.eviction_rows()} == {
        stale: "working/a.jpg", replaced: "working/b2.jpg",
        evicted: "working/c.jpg", untouched: None,
    }
    db.working_copies.clear_missing_tracked(
        [(stale, "working/a.jpg"), (replaced, "working/b.jpg")],
    )
    db.working_copies.record_eviction(evicted, "working/c.jpg")
    db.working_copies.record_eviction(replaced, "working/b.jpg")
    db.working_copies.record_eviction(untouched, "working/d.jpg")
    assert db.in_transaction
    db.commit()
    rows = _committed(
        db, "SELECT id, working_copy_path, working_copy_evicted_mtime FROM photos ORDER BY id",
    )
    assert rows == [
        (stale, None, None),
        (replaced, "working/b2.jpg", None),
        (evicted, None, 4.0),
        (untouched, None, -1),
    ]


def test_defer_and_undo_deferral_round_trip(db, tmp_path):
    fid = db.add_folder(str(tmp_path), name="p")
    pid = _photo(db, fid, "a.nef", file_mtime=None)
    snapshot = _wc_snapshot(db, pid)
    db.working_copies.defer([snapshot])
    assert db.conn.execute(
        "SELECT working_copy_evicted_mtime FROM photos WHERE id=?", (pid,),
    ).fetchone()[0] == -1
    db.working_copies.undo_deferral([snapshot])
    assert db.conn.execute(
        "SELECT working_copy_evicted_mtime FROM photos WHERE id=?", (pid,),
    ).fetchone()[0] is None
    db.rollback()


def test_load_scope_commits_only_the_transaction_it_opened(db, tmp_path):
    clause = db.working_copies.load_scope([(str(tmp_path), "exact", None)])
    assert "working_copy_scope" in clause
    assert not db.in_transaction
    db.begin()
    db.working_copies.load_scope([(str(tmp_path), "exact", None)])
    assert db.in_transaction
    db.rollback()


def test_mark_raw_decode_failed_stamps_the_source_marker_uncommitted(db, tmp_path):
    fid = db.add_folder(str(tmp_path), name="p")
    pid = _photo(db, fid, "a.nef", file_mtime=7.5)
    db.working_copies.mark_raw_decode_failed(pid, 7.5)
    assert db.in_transaction
    assert _committed(
        db, "SELECT working_copy_failed_source FROM photos WHERE id=?", (pid,),
    )[0] == (None,)
    db.commit()
    row = _committed(
        db,
        "SELECT working_copy_failed_at IS NOT NULL, working_copy_failed_mtime,"
        " working_copy_failed_source FROM photos WHERE id=?",
        (pid,),
    )[0]
    assert row == (1, 7.5, "source")


# -- structure -------------------------------------------------------------------


_ACCESSORS = {
    "scanning": ("_scanning_repository", ScanningRepository),
    "companion_pairing": ("_companion_pairing_repository", CompanionPairingRepository),
    "working_copies": ("_working_copy_repository", WorkingCopyRepository),
    "keyword_provenance": ("_keyword_provenance_repository", KeywordProvenanceRepository),
}


@pytest.mark.parametrize("name", sorted(_ACCESSORS))
def test_accessor_is_a_fresh_repository_on_the_connection_per_access(db, name):
    """A cached repository could outlive the connection; a fresh one cannot."""
    _factory, cls = _ACCESSORS[name]
    first, second = getattr(db, name), getattr(db, name)
    assert isinstance(first, cls)
    assert first is not second
    assert first.conn is db.conn


@pytest.mark.parametrize("name", sorted(_ACCESSORS))
def test_accessor_only_builds_the_repository(name):
    factory, _cls = _ACCESSORS[name]
    accessor = Database.__dict__[name]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert attrs == {factory}


@pytest.mark.parametrize("module", ["scanning", "companion_pairing", "working_copies"])
def test_scanner_repositories_import_no_db_code(module):
    source = (VIREO / "repositories" / f"{module}.py").read_text(encoding="utf-8")
    imported = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module)
    assert "db" not in imported
    assert "scanner" not in imported


def _scanner_sources():
    package = VIREO / "scanner"
    if package.is_dir():
        return sorted(package.rglob("*.py"))
    return [VIREO / "scanner.py"]


def test_scanner_never_touches_the_connection():
    """Catalog reads, writes and transaction control go through ``Database``.

    The scanner reaches SQL through ``db.scanning``, ``db.companion_pairing``,
    ``db.working_copies``, ``db.caches``, ``db.photo_visibility`` and
    ``db.keyword_provenance``, and controls transactions with
    ``db.commit_with_retry()``, ``db.rollback()``, ``db.in_transaction`` and
    ``db.begin_immediate()``.
    """
    offenders = []
    for path in _scanner_sources():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "conn":
                offenders.append(f"{path.relative_to(VIREO)}:{node.lineno}")
    assert not offenders, offenders
