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


def test_a_dict_shaped_tree_is_dropped_before_the_frontend_sees_it(app_and_db):
    """A stored ``tree`` that decoded to an object is replaced with ``[]``.

    Iterating a dict visits its keys without raising, so the per-row
    ``attach_result_text`` guard does not fire on this shape. On the
    Jobs page, ``steps.length > 0`` is truthy for a payload like
    ``{"length": 1}`` (plain property access), and ``steps.forEach``
    then throws, aborting rendering of the whole history pane.
    """
    app, db = app_and_db
    _insert_terminal_row(db, job_id="dict-tree", tree_json='{"length": 1}')
    _insert_terminal_row(db, job_id="good-row", tree_json='[]')

    resp = app.test_client().get("/api/jobs/history?limit=100")
    assert resp.status_code == 200
    rows = {row["id"]: row for row in resp.get_json()}
    assert rows["dict-tree"]["tree"] == []
    assert rows["good-row"]["tree"] == []

    jobs = app.test_client().get("/api/jobs").get_json()
    assert "history_error" not in jobs
    history_rows = {row["id"]: row for row in jobs["history"]}
    assert history_rows["dict-tree"]["tree"] == []


def test_status_read_failure_is_distinct_from_unknown_id(app_and_db, locked_history):
    app, _ = app_and_db
    resp = app.test_client().get("/api/jobs/missing")
    assert resp.status_code == 503
    assert resp.get_json()["code"] == "job_history_unavailable"


@pytest.mark.parametrize("restart", [False, True], ids=["eviction", "restart"])
def test_status_recovers_chained_jobs(app_and_db, restart, monkeypatch):
    from job_runner.lineage import JobLineage
    from jobs import JobRunner
    from wait import wait_for_job_via_runner

    app, db = app_and_db
    client = app.test_client()
    runner = app._job_runner
    ws = db.active_workspace_id

    def process_work(job):
        job["steps"] = [{"id": "scan", "status": "completed", "summary": "Scanned 3 photos"}]
        job["errors"].extend(["preview failed", "thumbnail failed"])
        job["progress"].update(current=3, total=3)
        return {"scanned": 3}

    def import_work(job):
        process_id = runner.start("pipeline", process_work, workspace_id=ws,
                                  lineage=JobLineage.following(job["id"]))
        return {"imported": 3, "process_job_id": process_id}

    import_id = runner.start("import", import_work, workspace_id=ws)
    imported = wait_for_job_via_runner(runner, import_id, wait_for_history=True)
    process_id = imported["result"]["process_job_id"]
    processed = wait_for_job_via_runner(runner, process_id, wait_for_history=True)
    if restart:
        assert runner.shutdown(timeout=5)
        app._job_runner = JobRunner(db=db)
    else:
        monkeypatch.setattr("jobs._JOB_RETENTION_SECS", 0)
        with runner._lock:
            runner._prune_finished_jobs()
    assert app._job_runner.get(import_id) is None
    assert app._job_runner.get(process_id) is None

    parent = client.get(f"/api/jobs/{import_id}").get_json()
    assert parent["historical"] and parent["status"] == "completed"
    assert parent["_persisted"] is True
    assert parent["result"] == imported["result"]
    url = f"/api/jobs/{parent['result']['process_job_id']}"
    child = client.get(url).get_json()
    assert child["parent_job_id"] == import_id
    assert child["historical"] and child["status"] == "failed"
    assert child["result"]["scanned"] == 3
    assert child["steps"] == processed["steps"]
    assert child["progress"] == processed["progress"]
    assert child["errors"] == processed["errors"]
    assert child["error_count"] == 2
    assert child["pausable"] is False and child["cancellable"] is False
    for action in ("pause", "resume", "cancel"):
        assert client.post(f"{url}/{action}").status_code == 404


def test_historical_detail_limits_lookup_to_visible_terminal_rows(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    _insert_terminal_row(db, job_id="other-workspace", tree_json="[]")
    _insert_terminal_row(db, job_id="unfinished", tree_json="[]")
    other = db.create_workspace("Other workspace")
    db.conn.execute("UPDATE job_history SET workspace_id=? WHERE id='other-workspace'", (other,))
    db.conn.execute("UPDATE job_history SET status='running' WHERE id='unfinished'")
    db.commit()
    for job_id in ("other-workspace", "unfinished", "unknown"):
        assert client.get(f"/api/jobs/{job_id}").status_code == 404


def test_historical_detail_recovers_legacy_error_details(app_and_db):
    app, db = app_and_db
    _insert_terminal_row(db, job_id="old-job", tree_json="[]")
    db.conn.execute(
        "UPDATE job_history SET status='failed', result=?, error_count=3 WHERE id='old-job'",
        (json.dumps({"error": "fatal", "errors": ["decoder error"]}),),
    )
    db.commit()
    row = app.test_client().get("/api/jobs/old-job").get_json()
    assert row["errors"] == ["decoder error", "fatal"]
    assert row["error_count"] == 3
    assert row["progress"] is None
