"""Behavior pins for the audit domain of ``Database``.

The behavior tests exercise the audit-run and hash-integrity methods only
through the public ``Database`` façade, so they hold regardless of whether
the SQL lives in ``db.py`` or in ``repositories/audit.py``; the structural
test at the end keeps it in the repository. They cover audit-run records,
the integrity photo/flagged/stats queries (workspace and folder-status
scoping, ordering, return shapes), and hash-check verdict writes (the three
update branches, argument validation, and commit boundaries).
"""

import ast
import inspect
import sqlite3
import textwrap
from datetime import datetime

import pytest
from db import Database


def _other_conn(db):
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _photo_row(db, photo_id):
    conn = _other_conn(db)
    try:
        return conn.execute(
            "SELECT hash_status, hash_checked_at, file_hash FROM photos "
            "WHERE id = ?",
            (photo_id,),
        ).fetchone()
    finally:
        conn.close()


@pytest.fixture
def library(db):
    """Active workspace with one ok folder, one missing folder, and a folder
    that belongs only to another workspace."""
    ws = db._active_workspace_id
    ok = db.add_folder("/lib/ok", name="ok")
    partial = db.add_folder("/lib/partial", name="partial")
    missing = db.add_folder("/lib/missing", name="missing")
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?",
                    (partial,))
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?",
                    (missing,))
    other_ws = db.create_workspace("Other")
    foreign = db.add_folder("/lib/foreign", name="foreign",
                            link_to_workspace=False)
    db.add_workspace_folder(other_ws, foreign)
    db.conn.commit()

    ids = {}
    ids["b_ok"] = db.add_photo(ok, "b.jpg", ".jpg", 100, 1.0,
                               file_hash="h-b")
    ids["a_ok"] = db.add_photo(ok, "a.jpg", ".jpg", 100, 2.0,
                               file_hash="h-a")
    ids["c_partial"] = db.add_photo(partial, "c.jpg", ".jpg", 100, 3.0)
    ids["d_missing"] = db.add_photo(missing, "d.jpg", ".jpg", 100, 4.0)
    ids["e_foreign"] = db.add_photo(foreign, "e.jpg", ".jpg", 100, 5.0)
    return {"ws": ws, "other_ws": other_ws, "ids": ids,
            "folders": {"ok": ok, "partial": partial, "missing": missing,
                        "foreign": foreign}}


# -- audit runs --------------------------------------------------------------


def test_get_audit_runs_empty(db):
    assert db.get_audit_runs() == {}


def test_record_audit_run_commits_and_overwrites(db):
    before = datetime.now()
    db.record_audit_run("verify", 3)
    assert not db.conn.in_transaction

    conn = _other_conn(db)
    try:
        rows = conn.execute(
            "SELECT workspace_id, check_name, ran_at, problem_count "
            "FROM audit_runs"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 1
    assert rows[0]["workspace_id"] == db._active_workspace_id
    assert rows[0]["check_name"] == "verify"
    assert rows[0]["problem_count"] == 3
    assert datetime.fromisoformat(rows[0]["ran_at"]) >= before

    db.record_audit_run("verify", 0)
    runs = db.get_audit_runs()
    assert list(runs) == ["verify"]
    assert runs["verify"]["problem_count"] == 0
    assert set(runs["verify"]) == {"ran_at", "problem_count"}


def test_record_audit_run_coerces_problem_count_to_int(db):
    db.record_audit_run("drift", "7")
    db.record_audit_run("orphans", 2.9)
    runs = db.get_audit_runs()
    assert runs["drift"]["problem_count"] == 7
    assert runs["orphans"]["problem_count"] == 2
    assert isinstance(runs["orphans"]["problem_count"], int)


def test_record_audit_run_rejects_non_numeric_count(db):
    with pytest.raises(ValueError):
        db.record_audit_run("drift", "many")
    assert db.get_audit_runs() == {}


def test_audit_runs_are_scoped_to_active_workspace(db):
    first = db._active_workspace_id
    db.record_audit_run("verify", 1)
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    assert db.get_audit_runs() == {}
    db.record_audit_run("verify", 5)
    assert db.get_audit_runs()["verify"]["problem_count"] == 5
    db.set_active_workspace(first)
    assert db.get_audit_runs()["verify"]["problem_count"] == 1


@pytest.mark.parametrize("call", [
    lambda db: db.record_audit_run("verify", 0),
    lambda db: db.get_audit_runs(),
    lambda db: db.get_integrity_photos(),
    lambda db: db.get_integrity_flagged(),
    lambda db: db.get_integrity_stats(),
])
def test_scoped_methods_require_active_workspace(db, call):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        call(db)


# -- integrity queries -------------------------------------------------------


def test_get_integrity_photos_scope_order_and_shape(db, library):
    ids = library["ids"]
    rows = db.get_integrity_photos()
    assert isinstance(rows, list)
    assert all(isinstance(r, dict) for r in rows)
    # ok + partial folders only; missing folder and foreign workspace excluded
    assert [r["id"] for r in rows] == sorted(
        [ids["b_ok"], ids["a_ok"], ids["c_partial"]]
    )
    assert set(rows[0]) == {
        "id", "filename", "file_hash", "file_mtime", "hash_status",
        "hash_checked_at", "folder_path",
    }
    by_id = {r["id"]: r for r in rows}
    assert by_id[ids["b_ok"]]["folder_path"] == "/lib/ok"
    assert by_id[ids["b_ok"]]["file_hash"] == "h-b"
    assert by_id[ids["b_ok"]]["file_mtime"] == 1.0
    assert by_id[ids["b_ok"]]["hash_status"] is None
    assert by_id[ids["b_ok"]]["hash_checked_at"] is None
    assert by_id[ids["c_partial"]]["folder_path"] == "/lib/partial"


def test_get_integrity_photos_in_other_workspace(db, library):
    db.set_active_workspace(library["other_ws"])
    rows = db.get_integrity_photos()
    assert [r["id"] for r in rows] == [library["ids"]["e_foreign"]]


def test_get_integrity_flagged_filters_and_orders(db, library):
    ids = library["ids"]
    assert db.get_integrity_flagged() == []
    db.update_photo_hash_check(ids["b_ok"], "modified")
    db.update_photo_hash_check(ids["a_ok"], "modified")
    db.update_photo_hash_check(ids["c_partial"], "corrupt")
    db.update_photo_hash_check(ids["d_missing"], "unreadable")  # missing folder
    db.update_photo_hash_check(ids["e_foreign"], "corrupt")  # other workspace
    extra = db.add_photo(library["folders"]["ok"], "z.jpg", ".jpg", 1, 9.0)
    db.update_photo_hash_check(extra, "unreadable")
    ok_photo = db.add_photo(library["folders"]["ok"], "y.jpg", ".jpg", 1, 9.0)
    db.update_photo_hash_check(ok_photo, "ok")

    rows = db.get_integrity_flagged()
    assert [(r["hash_status"], r["filename"]) for r in rows] == [
        ("corrupt", "c.jpg"),
        ("modified", "a.jpg"),
        ("modified", "b.jpg"),
        ("unreadable", "z.jpg"),
    ]
    assert all(isinstance(r, dict) for r in rows)
    assert set(rows[0]) == {
        "photo_id", "filename", "hash_status", "hash_checked_at",
        "folder_path",
    }
    assert rows[0]["photo_id"] == ids["c_partial"]
    assert rows[0]["folder_path"] == "/lib/partial"
    assert rows[0]["hash_checked_at"] is not None


def test_get_integrity_stats_empty_workspace(db):
    assert db.get_integrity_stats() == {
        "total": 0, "checked": 0, "unchecked": 0, "flagged": 0,
    }


def test_get_integrity_stats_counts(db, library):
    ids = library["ids"]
    assert db.get_integrity_stats() == {
        "total": 3, "checked": 0, "unchecked": 3, "flagged": 0,
    }
    db.update_photo_hash_check(ids["a_ok"], "ok")
    db.update_photo_hash_check(ids["b_ok"], "corrupt")
    db.update_photo_hash_check(ids["d_missing"], "modified")
    db.update_photo_hash_check(ids["e_foreign"], "modified")
    assert db.get_integrity_stats() == {
        "total": 3, "checked": 2, "unchecked": 1, "flagged": 1,
    }


# -- hash-check verdicts -----------------------------------------------------


def test_update_photo_hash_check_status_only_keeps_hash(db, library):
    pid = library["ids"]["a_ok"]
    before = datetime.now()
    db.update_photo_hash_check(pid, "ok")
    assert not db.conn.in_transaction
    row = _photo_row(db, pid)
    assert row["hash_status"] == "ok"
    assert row["file_hash"] == "h-a"
    assert datetime.fromisoformat(row["hash_checked_at"]) >= before


def test_update_photo_hash_check_replaces_hash(db, library):
    pid = library["ids"]["a_ok"]
    db.update_photo_hash_check(pid, "ok", file_hash="h-new")
    row = _photo_row(db, pid)
    assert row["hash_status"] == "ok"
    assert row["file_hash"] == "h-new"
    assert row["hash_checked_at"] is not None


def test_update_photo_hash_check_clears_hash(db, library):
    pid = library["ids"]["a_ok"]
    db.update_photo_hash_check(pid, "ok", clear_file_hash=True)
    row = _photo_row(db, pid)
    assert row["hash_status"] == "ok"
    assert row["file_hash"] is None
    assert row["hash_checked_at"] is not None


def test_update_photo_hash_check_rejects_conflicting_hash_args(db, library):
    pid = library["ids"]["a_ok"]
    with pytest.raises(
        ValueError,
        match="clear_file_hash and file_hash are mutually exclusive",
    ):
        db.update_photo_hash_check(pid, "ok", file_hash="h-x",
                                   clear_file_hash=True)
    assert not db.conn.in_transaction
    row = _photo_row(db, pid)
    assert row["hash_status"] is None
    assert row["file_hash"] == "h-a"


def test_update_photo_hash_check_commit_false_defers(db, library):
    pid = library["ids"]["a_ok"]
    db.update_photo_hash_check(pid, "modified", file_hash="h-z",
                               commit=False)
    assert db.conn.in_transaction
    assert _photo_row(db, pid)["hash_status"] is None
    db.conn.commit()
    row = _photo_row(db, pid)
    assert row["hash_status"] == "modified"
    assert row["file_hash"] == "h-z"


def test_update_photo_hash_check_is_catalog_wide(db, library):
    """The verdict write never consults the active workspace."""
    pid = library["ids"]["e_foreign"]
    db.set_active_workspace(None)
    db.update_photo_hash_check(pid, "corrupt")
    assert _photo_row(db, pid)["hash_status"] == "corrupt"


def test_update_photo_hash_check_unknown_photo_is_noop(db):
    db.update_photo_hash_check(999_999, "ok")
    assert not db.conn.in_transaction


def test_update_photo_hash_check_signature_defaults():
    params = inspect.signature(Database.update_photo_hash_check).parameters
    assert list(params) == [
        "self", "photo_id", "status", "file_hash", "commit",
        "clear_file_hash",
    ]
    assert params["file_hash"].default is None
    assert params["commit"].default is True
    assert params["clear_file_hash"].default is False


# -- structure ---------------------------------------------------------------


_DELEGATING_AUDIT_METHODS = (
    "record_audit_run",
    "get_audit_runs",
    "get_integrity_photos",
    "get_integrity_flagged",
    "get_integrity_stats",
    "update_photo_hash_check",
)


@pytest.mark.parametrize("name", _DELEGATING_AUDIT_METHODS)
def test_audit_method_delegates_to_repository(name):
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    fn = ast.parse(source).body[0]
    attrs = {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to AuditRepository"
    )
    assert "_audit_repository" in attrs, (
        f"Database.{name} no longer delegates to AuditRepository"
    )
