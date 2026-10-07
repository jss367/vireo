"""Behavior pins for the read-only job-history domain of ``Database``.

``JobRunner`` creates and writes ``job_history`` on its own connection; the
façade only reads finished jobs back (the Duplicates page restoring its last
scan). The structural test at the end keeps that SQL in
``repositories/job_history.py``.
"""

import ast
import inspect
import json
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


_DELEGATING_JOB_HISTORY_METHODS = ("get_last_completed_job",)


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
