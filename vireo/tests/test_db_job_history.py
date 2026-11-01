"""Behavior pins for the job-history domain of ``Database``.

The table is part of the canonical schema; ``jobs.JobRunner`` keeps its own
writes. These tests
cover what the job routes do through the ``db.job_history`` accessor: reading
a job type's newest completed result back (the Duplicates page restoring its
last scan), the single-record lookup, and the result rewrite. The structural
test at the end keeps their SQL in ``repositories/job_history.py``.
"""

import ast
import contextlib
import inspect
import json
import sqlite3
import textwrap

import pytest
from db import Database
from jobs import JobRunner
from repositories.job_history import JobHistoryRepository


@pytest.fixture
def history(db):
    def add(job_id, job_type, status, finished_at, result):
        db.conn.execute(
            "INSERT INTO job_history (id, type, status, started_at, finished_at, result) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (job_id, job_type, status, "2026-01-01T00:00:00", finished_at,
             None if result is None else json.dumps(result)),
        )
        db.conn.commit()

    return add


def test_last_completed_with_result_picks_newest_completed_result(db, history):
    assert db.job_history.last_completed_with_result("duplicate-scan") is None
    history("old", "duplicate-scan", "completed", "2026-01-02T00:00:00", {"n": 1})
    history("new", "duplicate-scan", "completed", "2026-01-03T00:00:00", {"n": 2})
    history("failed", "duplicate-scan", "failed", "2026-01-09T00:00:00", {"n": 3})
    history("empty", "duplicate-scan", "completed", "2026-01-08T00:00:00", None)
    history("other", "scan", "completed", "2026-01-10T00:00:00", {"n": 4})
    row = db.job_history.last_completed_with_result("duplicate-scan")
    assert row.keys() == ["id", "started_at", "finished_at", "result"]
    assert row["id"] == "new"
    assert json.loads(row["result"]) == {"n": 2}
    assert db.job_history.last_completed_with_result("scan")["id"] == "other"
    assert db.job_history.last_completed_with_result("thumbnails") is None


def test_last_completed_with_result_is_catalog_wide(db, history):
    history("j", "duplicate-scan", "completed", "2026-01-02T00:00:00", {"n": 1})
    db.set_active_workspace(None)
    assert db.job_history.last_completed_with_result("duplicate-scan")["id"] == "j"


@pytest.fixture
def history_db(db):
    """``db`` with the ``job_history`` table the job runner creates at startup."""
    JobRunner(db)
    return db


def _insert(db, job_id, *, workspace_id=None, result=None, job_type="move-folder",
            status="completed"):
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, result, config, workspace_id) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (job_id, job_type, status, result, json.dumps({"k": 1}), workspace_id),
    )
    db.conn.commit()


def _committed_result(db, job_id):
    with contextlib.closing(sqlite3.connect(db._db_path)) as other:
        row = other.execute(
            "SELECT result FROM job_history WHERE id = ?", (job_id,)
        ).fetchone()
    return row[0] if row else None


def test_get_returns_every_column(history_db):
    db = history_db
    ws = db.active_workspace_id
    _insert(db, "job-a", workspace_id=ws, result='{"moved": 2}')
    row = db.job_history.get("job-a")
    assert isinstance(row, sqlite3.Row)
    columns = [r[1] for r in db.conn.execute("PRAGMA table_info(job_history)")]
    assert row.keys() == columns
    assert dict(row)["type"] == "move-folder"
    assert row["status"] == "completed"
    assert row["workspace_id"] == ws
    assert row["result"] == '{"moved": 2}'


def test_get_is_exact_and_unscoped(history_db):
    db = history_db
    other = db.create_workspace("History other")
    _insert(db, "job-b", workspace_id=other)
    db.set_active_workspace(None)
    # Any workspace's record: the caller compares ``workspace_id`` itself.
    assert db.job_history.get("job-b")["workspace_id"] == other
    assert db.job_history.get("job-") is None
    assert db.job_history.get("JOB-B") is None
    assert db.job_history.get("missing") is None


def test_set_result_rewrites_one_row_and_commits(history_db):
    db = history_db
    _insert(db, "job-c", result='{"moved": 1}')
    _insert(db, "job-d", result='{"moved": 9}')
    assert db.job_history.set_result("job-c", '{"moved": 1, "cleanup": true}') is None
    assert not db.in_transaction
    assert _committed_result(db, "job-c") == '{"moved": 1, "cleanup": true}'
    assert _committed_result(db, "job-d") == '{"moved": 9}'


def test_set_result_for_unknown_id_writes_nothing(history_db):
    db = history_db
    db.job_history.set_result("missing", "{}")
    assert db.job_history.get("missing") is None


def test_set_result_commit_is_held_with_other_commits(history_db):
    db = history_db
    _insert(db, "job-e", result="{}")
    with db.transaction():
        db.job_history.set_result("job-e", '{"x": 1}')
        assert db.in_transaction
        assert _committed_result(db, "job-e") == "{}"
    assert _committed_result(db, "job-e") == '{"x": 1}'


def test_catalog_file_is_the_main_database_path(db, tmp_path):
    import os

    assert os.path.realpath(db.job_history.catalog_file()) == os.path.realpath(db._db_path)


def test_unfinished_and_mark_interrupted_leave_the_commit_to_the_sweep(history_db):
    db = history_db
    _insert(db, "run", status="running", result='{"a": 1}')
    _insert(db, "queue", status="queued")
    _insert(db, "done", status="completed")
    rows = db.job_history.unfinished()
    assert sorted(r["id"] for r in rows) == ["queue", "run"]
    assert rows[0].keys() == ["id", "status", "result", "tree", "progress", "error_count"]

    db.job_history.mark_interrupted("run", "2026-01-02T00:00:00", '{"e": 1}', 3, "[]", "cut", None)
    assert db.in_transaction
    assert _committed_result(db, "run") == '{"a": 1}'
    db.commit()
    row = db.job_history.get("run")
    assert (row["status"], row["finished_at"], row["result"], row["error_count"],
            row["tree"], row["summary"], row["progress"]) == (
        "failed", "2026-01-02T00:00:00", '{"e": 1}', 3, "[]", "cut", None,
    )
    assert db.job_history.get("queue")["status"] == "queued"


def test_terminal_is_newest_first_and_scoped_only_when_asked(history_db):
    db = history_db
    ws = db.require_workspace_id()
    for job_id, status, started, workspace in (
        ("a", "completed", "2026-01-01", ws),
        ("b", "failed", "2026-01-03", ws),
        ("c", "running", "2026-01-04", ws),
        ("d", "cancelled", "2026-01-02", None),
    ):
        _insert(db, job_id, status=status, workspace_id=workspace)
        db.conn.execute("UPDATE job_history SET started_at = ? WHERE id = ?", (started, job_id))
    db.commit()
    terminal = ("completed", "failed", "cancelled")
    # A workspace's history includes the global rows (maintenance, downloads).
    assert [r["id"] for r in db.job_history.terminal(ws, terminal, 10)] == ["b", "d", "a"]
    assert [r["id"] for r in db.job_history.terminal(None, terminal, 10)] == ["b", "d", "a"]
    assert [r["id"] for r in db.job_history.terminal(None, terminal, 1)] == ["b"]


def test_terminal_limit_is_a_total_cap_and_origin_reads_one_window(history_db):
    """``limit`` caps the rows; ``origin`` reads one origin's own window.

    Ten newer Vireo outcomes fill a mixed window, so the Jobs page asks for
    ``origin='user'`` when "Started by you" is chosen and still reaches the
    older retained user rows, global rows included.
    """
    db = history_db
    ws = db.require_workspace_id()
    for i in range(10):
        _insert(db, f"sys{i}", status="completed", workspace_id=ws if i % 2 else None)
        db.conn.execute(
            "UPDATE job_history SET started_at = ?, origin = 'system' WHERE id = ?",
            (f"2026-02-{i + 1:02d}", f"sys{i}"),
        )
    for job_id, started, workspace in (
        ("user_old_1", "2026-01-10", ws), ("user_old_2", "2026-01-11", None),
    ):
        _insert(db, job_id, status="completed", workspace_id=workspace)
        db.conn.execute(
            "UPDATE job_history SET started_at = ?, origin = 'user' WHERE id = ?",
            (started, job_id),
        )
    db.commit()
    terminal = ("completed", "failed", "cancelled")
    assert [r["id"] for r in db.job_history.terminal(ws, terminal, 1)] == ["sys9"]
    assert [r["id"] for r in db.job_history.terminal(ws, terminal, 5)] == [
        "sys9", "sys8", "sys7", "sys6", "sys5",
    ]
    assert [r["id"] for r in db.job_history.terminal(ws, terminal, 5, "user")] == [
        "user_old_2", "user_old_1",
    ]
    assert [r["id"] for r in db.job_history.terminal(None, terminal, 1, "user")] == ["user_old_2"]
    assert len(db.job_history.terminal(ws, terminal, 3, "system")) == 3


# -- structure ------------------------------------------------------------------


def test_job_history_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.job_history`` builds a new repository each time, never a cached one.

    A cached repository could outlive the connection or, for a scoped domain,
    the active workspace it was built for; a fresh one resolves both at use,
    exactly as a forwarding wrapper called at that moment did.
    """
    first, second = db.job_history, db.job_history
    assert isinstance(first, JobHistoryRepository)
    assert first is not second
    assert first.conn is db.conn


def test_job_history_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.job_history``; Database keeps no aliases."""
    for name in ("get_last_completed_job", "get_job_history_row", "set_job_history_result"):
        assert not hasattr(Database, name), f"Database.{name} came back; call db.job_history"
    accessor = Database.__dict__["job_history"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_job_history_repository" in attrs
    assert "conn" not in attrs
