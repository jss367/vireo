"""Behavior pins for the job-history domain of ``Database``.

``jobs.JobRunner`` creates the table and keeps its own writes; these tests
cover what the job routes do through the public ``Database`` façade: reading
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


@pytest.fixture
def history(db):
    # The real table definition and migrations; the method reads no runner state.
    JobRunner._ensure_history_table(None, db)
    db.conn.commit()

    def add(job_id, job_type, status, finished_at, result):
        db.conn.execute(
            "INSERT INTO job_history (id, type, status, started_at, finished_at, result) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (job_id, job_type, status, "2026-01-01T00:00:00", finished_at,
             None if result is None else json.dumps(result)),
        )
        db.conn.commit()

    return add


def test_get_last_completed_job_picks_newest_completed_result(db, history):
    assert db.get_last_completed_job("duplicate-scan") is None
    history("old", "duplicate-scan", "completed", "2026-01-02T00:00:00", {"n": 1})
    history("new", "duplicate-scan", "completed", "2026-01-03T00:00:00", {"n": 2})
    history("failed", "duplicate-scan", "failed", "2026-01-09T00:00:00", {"n": 3})
    history("empty", "duplicate-scan", "completed", "2026-01-08T00:00:00", None)
    history("other", "scan", "completed", "2026-01-10T00:00:00", {"n": 4})
    row = db.get_last_completed_job("duplicate-scan")
    assert row.keys() == ["id", "started_at", "finished_at", "result"]
    assert row["id"] == "new"
    assert json.loads(row["result"]) == {"n": 2}
    assert db.get_last_completed_job("scan")["id"] == "other"
    assert db.get_last_completed_job("thumbnails") is None


def test_get_last_completed_job_is_catalog_wide(db, history):
    history("j", "duplicate-scan", "completed", "2026-01-02T00:00:00", {"n": 1})
    db.set_active_workspace(None)
    assert db.get_last_completed_job("duplicate-scan")["id"] == "j"


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


def test_get_job_history_row_returns_every_column(history_db):
    db = history_db
    ws = db.active_workspace_id
    _insert(db, "job-a", workspace_id=ws, result='{"moved": 2}')
    row = db.get_job_history_row("job-a")
    assert isinstance(row, sqlite3.Row)
    columns = [r[1] for r in db.conn.execute("PRAGMA table_info(job_history)")]
    assert row.keys() == columns
    assert dict(row)["type"] == "move-folder"
    assert row["status"] == "completed"
    assert row["workspace_id"] == ws
    assert row["result"] == '{"moved": 2}'


def test_get_job_history_row_is_exact_and_unscoped(history_db):
    db = history_db
    other = db.create_workspace("History other")
    _insert(db, "job-b", workspace_id=other)
    db.set_active_workspace(None)
    # Any workspace's record: the caller compares ``workspace_id`` itself.
    assert db.get_job_history_row("job-b")["workspace_id"] == other
    assert db.get_job_history_row("job-") is None
    assert db.get_job_history_row("JOB-B") is None
    assert db.get_job_history_row("missing") is None


def test_set_job_history_result_rewrites_one_row_and_commits(history_db):
    db = history_db
    _insert(db, "job-c", result='{"moved": 1}')
    _insert(db, "job-d", result='{"moved": 9}')
    assert db.set_job_history_result("job-c", '{"moved": 1, "cleanup": true}') is None
    assert not db.in_transaction
    assert _committed_result(db, "job-c") == '{"moved": 1, "cleanup": true}'
    assert _committed_result(db, "job-d") == '{"moved": 9}'


def test_set_job_history_result_for_unknown_id_writes_nothing(history_db):
    db = history_db
    db.set_job_history_result("missing", "{}")
    assert db.get_job_history_row("missing") is None


def test_set_job_history_result_commit_is_held_with_other_commits(history_db):
    db = history_db
    _insert(db, "job-e", result="{}")
    with db._commits_held():
        db.set_job_history_result("job-e", '{"x": 1}')
        assert db.in_transaction
        assert _committed_result(db, "job-e") == "{}"
    assert _committed_result(db, "job-e") == '{"x": 1}'


# -- structure ------------------------------------------------------------------

_DELEGATING_JOB_HISTORY_METHODS = (
    "get_last_completed_job", "get_job_history_row", "set_job_history_result",
)


@pytest.mark.parametrize("name", _DELEGATING_JOB_HISTORY_METHODS)
def test_job_history_method_delegates_to_repository(name):
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
        f"Database.{name} touches self.conn; move the SQL to JobHistoryRepository"
    )
    assert "_job_history_repository" in attrs, (
        f"Database.{name} no longer delegates to JobHistoryRepository"
    )
