"""A job-history read that fails must reach the Jobs page as an error.

Before, ``JobHistory.get_history`` caught the failure and returned ``[]``, so
``/api/jobs/history`` answered 200 with an empty list and the Jobs page said
"No job history" while the catalog simply could not be read.
"""

import json
import sqlite3

import pytest
from repositories.job_history import JobHistoryRepository


@pytest.fixture
def locked_history(monkeypatch):
    def fail(self, *args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(JobHistoryRepository, "terminal", fail)


def test_history_endpoint_reports_an_unreadable_history(app_and_db, locked_history):
    app, _ = app_and_db
    resp = app.test_client().get("/api/jobs/history?limit=100")

    assert resp.status_code == 503
    body = resp.get_json()
    assert body["code"] == "job_history_unavailable"
    assert body["error"] == (
        "Vireo could not read the job history from its catalog "
        "(database is locked)."
    )


def test_jobs_list_keeps_active_jobs_and_names_the_history_failure(app_and_db, locked_history):
    app, _ = app_and_db
    resp = app.test_client().get("/api/jobs")

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["active"] == []
    assert body["history"] == []
    assert body["history_error"] == (
        "Vireo could not read the job history from its catalog "
        "(database is locked)."
    )


def test_jobs_list_has_no_history_error_when_the_read_succeeds(app_and_db):
    app, _ = app_and_db
    body = app.test_client().get("/api/jobs").get_json()

    assert "history_error" not in body


def test_runner_raises_instead_of_returning_an_empty_history(app_and_db, locked_history):
    from job_runner.history import JobHistoryUnavailable

    app, db = app_and_db
    with pytest.raises(JobHistoryUnavailable) as caught:
        app._job_runner.get_history(db, limit=10)
    assert "database is locked" in str(caught.value)


def _insert_terminal_row(db, *, job_id, tree_json):
    """Insert a terminal history row with the given stored ``tree`` JSON."""
    db.conn.execute(
        "INSERT INTO job_history "
        "(id, type, status, started_at, finished_at, duration, "
        " result, error_count, config, workspace_id, tree, summary) "
        "VALUES (?, 'scan', 'completed', '2026-01-01T00:00:00', "
        " '2026-01-01T00:00:01', 1.0, ?, 0, '{}', ?, ?, '')",
        (job_id, json.dumps({"ok": True}), db.active_workspace_id, tree_json),
    )
    db.commit()


def test_a_malformed_tree_row_does_not_poison_the_whole_history(app_and_db):
    """A stored ``tree`` with an unexpected shape stays contained to its row.

    Before the per-row guard, a non-list ``tree`` (e.g. ``123`` after JSON
    decode) made ``attach_result_text`` raise ``TypeError`` while iterating
    the steps. The whole ``/api/jobs/history`` and ``/api/jobs`` responses
    then failed, swallowing the active-job list too.
    """
    app, db = app_and_db
    _insert_terminal_row(db, job_id="bad-row", tree_json="123")
    _insert_terminal_row(db, job_id="good-row", tree_json="[]")

    resp = app.test_client().get("/api/jobs/history?limit=100")
    assert resp.status_code == 200
    ids = [row["id"] for row in resp.get_json()]
    assert "bad-row" in ids
    assert "good-row" in ids

    jobs = app.test_client().get("/api/jobs").get_json()
    assert "history_error" not in jobs
    assert {row["id"] for row in jobs["history"]} >= {"bad-row", "good-row"}
