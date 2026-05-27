# vireo/tests/test_pipeline_queue.py
"""Tests for the server-side pipeline queue.

The queue persists pending pipeline runs in ``job_history`` with
``status='queued'`` and promotes them to ``status='running'`` as slot
capacity opens. ``SLOT_CAP`` is 1 in this PR — the second queued run
waits for the first to reach a terminal state before promotion.
"""

import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.dirname(__file__))

from db import Database
from wait import wait_for_job_via_runner


def _make_runner_with_db(tmp_path):
    """Build a JobRunner + Database pair for a single test.

    The caller doesn't have to close the Database explicitly: pytest's
    tmp_path teardown removes the file, and Python's GC eventually
    finalises the Database object. For tests that open a second
    Database against the same file (the startup-sweep tests), close
    explicitly inside the test to avoid SQLite holding shared file
    locks across handles when GC order is non-deterministic.
    """
    db = Database(str(tmp_path / "test.db"))
    from jobs import JobRunner
    return JobRunner(db=db), db


def test_enqueue_pipeline_returns_pipeline_prefixed_job_id(tmp_path):
    """enqueue_pipeline must return a job id of the form ``pipeline-<ms>``."""
    runner, _ = _make_runner_with_db(tmp_path)

    job_id = runner.enqueue_pipeline(
        work_fn=lambda job: None,
        config={"sources": []},
        workspace_id=1,
    )
    assert job_id.startswith("pipeline-"), job_id


def test_enqueue_pipeline_persists_queued_row(tmp_path):
    """A row with status='queued' must be inserted into job_history."""
    runner, db = _make_runner_with_db(tmp_path)

    job_id = runner.enqueue_pipeline(
        work_fn=lambda job: None,
        config={"sources": ["/a"]},
        workspace_id=42,
    )

    # New connection so we don't see the runner's write through any
    # transactional cache; this confirms the row is committed.
    row = db.conn.execute(
        "SELECT id, type, status, workspace_id, config "
        "FROM job_history WHERE id = ?",
        (job_id,),
    ).fetchone()
    assert row is not None, "enqueue did not persist a job_history row"
    assert row["type"] == "pipeline"
    assert row["status"] in ("queued", "running")  # may have promoted already
    assert row["workspace_id"] == 42


def test_enqueue_pipeline_promotes_immediately_when_slot_free(tmp_path):
    """With no other pipelines active and SLOT_CAP>=1, an enqueued job
    promotes and runs to completion without further calls.
    """
    runner, _ = _make_runner_with_db(tmp_path)
    completed = threading.Event()

    def work(job):
        completed.set()
        return {"ok": True}

    job_id = runner.enqueue_pipeline(
        work_fn=work, config={}, workspace_id=1,
    )
    assert completed.wait(timeout=2.0), "work_fn was never invoked"
    job = wait_for_job_via_runner(runner, job_id)
    assert job["status"] == "completed"
    assert job["result"] == {"ok": True}


def test_second_enqueue_while_first_running_stays_queued(tmp_path):
    """A second enqueue while the slot is occupied must NOT start
    running. SLOT_CAP is 1 in this PR; the second waits.
    """
    runner, _ = _make_runner_with_db(tmp_path)
    first_started = threading.Event()
    let_first_finish = threading.Event()
    second_started = threading.Event()

    def first_work(job):
        first_started.set()
        let_first_finish.wait(timeout=3.0)
        return {"first": True}

    def second_work(job):
        second_started.set()
        return {"second": True}

    first_id = runner.enqueue_pipeline(
        work_fn=first_work, config={}, workspace_id=1,
    )
    assert first_started.wait(timeout=2.0), "first work_fn never started"

    second_id = runner.enqueue_pipeline(
        work_fn=second_work, config={}, workspace_id=1,
    )
    # Second must NOT have started yet — slot is occupied.
    assert not second_started.wait(timeout=0.2), (
        "second pipeline started while slot was occupied"
    )
    # Verify status via the runner's accessor.
    second_view = runner.get(second_id)
    assert second_view is not None
    assert second_view["status"] == "queued"

    # Release the first; second must now promote and run.
    let_first_finish.set()
    wait_for_job_via_runner(runner, first_id)
    assert second_started.wait(timeout=2.0), (
        "second pipeline did not promote after first completed"
    )
    second_final = wait_for_job_via_runner(runner, second_id)
    assert second_final["status"] == "completed"
    assert second_final["result"] == {"second": True}


def test_startup_sweep_marks_orphan_running_rows_as_failed(tmp_path):
    """Rows with status='running' from a prior process have no live
    thread to finish them. A fresh JobRunner must mark them 'failed'
    so the user (and /api/jobs/history) doesn't see them as alive forever.
    """
    db_path = str(tmp_path / "test.db")

    # First runner: simulate a process that crashed mid-run.
    first_db = Database(db_path)
    from jobs import JobRunner
    JobRunner(db=first_db)  # ensures schema
    first_db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, error_count) "
        "VALUES ('scan-old', 'scan', 'running', '2026-05-26T00:00:00', 0)"
    )
    first_db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, error_count) "
        "VALUES ('pipeline-old', 'pipeline', 'queued', '2026-05-26T00:00:00', 0)"
    )
    first_db.conn.commit()
    first_db.close()

    # Second runner: opens the same DB. Startup sweep must clean both up.
    second_db = Database(db_path)
    try:
        JobRunner(db=second_db)
        rows = {
            r["id"]: r["status"]
            for r in second_db.conn.execute(
                "SELECT id, status FROM job_history "
                "WHERE id IN ('scan-old', 'pipeline-old')"
            )
        }
        assert rows == {"scan-old": "failed", "pipeline-old": "failed"}, (
            f"startup sweep should have failed both orphans; got {rows}"
        )
    finally:
        second_db.close()


def test_startup_sweep_does_not_touch_terminal_rows(tmp_path):
    """Rows with status='completed', 'failed', or 'cancelled' from a
    prior process are already terminal — the sweep must leave them alone.
    """
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    from jobs import JobRunner
    JobRunner(db=db)
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, error_count) "
        "VALUES ('scan-done', 'scan', 'completed', '2026-05-26T00:00:00', 0)"
    )
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, error_count) "
        "VALUES ('scan-bad', 'scan', 'failed', '2026-05-26T00:00:00', 1)"
    )
    db.conn.commit()
    db.close()

    db2 = Database(db_path)
    try:
        JobRunner(db=db2)
        rows = {
            r["id"]: r["status"]
            for r in db2.conn.execute(
                "SELECT id, status FROM job_history "
                "WHERE id IN ('scan-done', 'scan-bad')"
            )
        }
        assert rows == {"scan-done": "completed", "scan-bad": "failed"}
    finally:
        db2.close()


def test_queued_pipeline_is_visible_via_get(tmp_path):
    """``runner.get(job_id)`` must return a queued-shaped job dict for
    pipelines that haven't promoted yet, not None. The UI polls this
    endpoint to render queue state.
    """
    runner, _ = _make_runner_with_db(tmp_path)
    blocker = threading.Event()

    def blocking_work(job):
        blocker.wait(timeout=3.0)
        return {}

    first_id = runner.enqueue_pipeline(
        work_fn=blocking_work, config={}, workspace_id=1,
    )
    second_id = runner.enqueue_pipeline(
        work_fn=lambda job: {"ok": True}, config={"x": 1}, workspace_id=2,
    )

    view = runner.get(second_id)
    assert view is not None
    assert view["status"] == "queued"
    assert view["type"] == "pipeline"
    assert view["config"] == {"x": 1}
    assert view["workspace_id"] == 2

    blocker.set()
    wait_for_job_via_runner(runner, first_id)
    wait_for_job_via_runner(runner, second_id)


def test_cancel_queued_pipeline_transitions_row_to_cancelled(tmp_path):
    """Cancelling a queued pipeline must atomically flip job_history to
    'cancelled' (without ever running the work_fn) and drop the
    in-memory context so promotion never picks it up.
    """
    runner, db = _make_runner_with_db(tmp_path)
    blocker = threading.Event()
    second_started = threading.Event()

    def first_work(job):
        blocker.wait(timeout=3.0)
        return {}

    def second_work(job):
        second_started.set()
        return {"should-not-run": True}

    first_id = runner.enqueue_pipeline(work_fn=first_work, config={}, workspace_id=1)
    second_id = runner.enqueue_pipeline(work_fn=second_work, config={}, workspace_id=1)
    assert runner.get(second_id)["status"] == "queued"

    # Cancel while still queued.
    ok = runner.cancel_job(second_id)
    assert ok is True
    row = db.conn.execute(
        "SELECT status FROM job_history WHERE id = ?", (second_id,),
    ).fetchone()
    assert row["status"] == "cancelled"

    # Releasing the first must NOT promote the cancelled one.
    blocker.set()
    wait_for_job_via_runner(runner, first_id)
    # Give the scheduler a beat — the cancelled row must stay cancelled.
    time.sleep(0.05)
    assert not second_started.is_set(), (
        "cancelled queued job must not be promoted"
    )
    final_row = db.conn.execute(
        "SELECT status FROM job_history WHERE id = ?", (second_id,),
    ).fetchone()
    assert final_row["status"] == "cancelled"


def test_promotion_loses_race_to_cancel_leaves_row_cancelled(tmp_path):
    """If a Cancel lands between the scheduler picking a queued job and
    its conditional UPDATE, the UPDATE matches zero rows and the
    scheduler bails out. The row stays 'cancelled' in job_history.
    """
    runner, db = _make_runner_with_db(tmp_path)

    # Pre-write a queued row directly, mimicking a row whose in-memory
    # context might race with cancel. We simulate the race by issuing
    # the cancel UPDATE before calling _try_promote_queued.
    import sqlite3
    job_id = "pipeline-1700000000000"
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, error_count) "
        "VALUES (?, 'pipeline', 'queued', '2026-05-26T00:00:00', 0)",
        (job_id,),
    )
    conn.execute(
        "UPDATE job_history SET status='cancelled' WHERE id = ?", (job_id,),
    )
    conn.commit()
    conn.close()

    # Inject an in-memory context for this id so _try_promote_queued
    # finds something to look at.
    with runner._lock:
        runner._queued_pipelines[job_id] = {
            "work_fn": lambda job: pytest_fail("should not have run"),
            "config": {},
            "workspace_id": 1,
            "runtime_warning": None,
            "started_at": "2026-05-26T00:00:00",
        }

    runner._try_promote_queued()

    final = db.conn.execute(
        "SELECT status FROM job_history WHERE id = ?", (job_id,),
    ).fetchone()
    assert final["status"] == "cancelled", (
        "conditional UPDATE must not overwrite a cancelled row"
    )
    # And the in-memory context is cleaned up.
    assert job_id not in runner._queued_pipelines


def pytest_fail(msg):
    raise AssertionError(msg)


def test_sse_subscribers_attached_while_queued_receive_post_promotion_events(tmp_path):
    """A client connected to /api/jobs/<id>/stream while the job is
    queued must keep receiving events once the job is promoted to
    running. Otherwise the UI sees ``status: queued`` forever even
    after the pipeline actually starts.
    """
    runner, _ = _make_runner_with_db(tmp_path)
    blocker = threading.Event()
    let_second_finish = threading.Event()

    def first_work(job):
        blocker.wait(timeout=3.0)
        return {}

    def second_work(job):
        runner.push_event(job["id"], "progress", {"phase": "started"})
        let_second_finish.wait(timeout=3.0)
        return {"ok": True}

    first_id = runner.enqueue_pipeline(first_work, config={}, workspace_id=1)
    second_id = runner.enqueue_pipeline(second_work, config={}, workspace_id=1)
    assert runner.get(second_id)["status"] == "queued"

    # Subscribe BEFORE promotion.
    q = runner.subscribe(second_id)

    # Release the first; second is now promoted and emits a progress event.
    blocker.set()
    wait_for_job_via_runner(runner, first_id)

    # The event the second's work_fn pushed must reach this subscriber.
    evt = q.get(timeout=2.0)
    assert evt["type"] == "progress"
    assert evt["data"]["phase"] == "started"

    let_second_finish.set()
    wait_for_job_via_runner(runner, second_id)
