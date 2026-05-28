# vireo/tests/test_pipeline_queue.py
"""Tests for the server-side pipeline queue.

The queue persists pending pipeline runs in ``job_history`` with
``status='queued'`` and promotes them to ``status='running'`` as slot
capacity opens. ``SLOT_CAP`` is 1 in this PR — the second queued run
waits for the first to reach a terminal state before promotion.
"""

import threading

from db import Database
from wait import wait_for_job_via_runner


def _wait_for_event(event, label, timeout=2.0):
    assert event.wait(timeout=timeout), f"{label} did not happen"


def _make_runner_with_db(tmp_path):
    """Build a JobRunner + Database pair for a single test.

    Also redirects ``config.CONFIG_PATH`` to ``tmp_path / "config.json"``
    so tests can't write through to ``~/.vireo/config.json`` even if a
    code path we exercise calls ``config.save()``. The conftest's
    ``_restore_global_config_paths`` autouse fixture restores it after
    the test.

    The caller doesn't have to close the Database explicitly: pytest's
    tmp_path teardown removes the file, and Python's GC eventually
    finalises the Database object. For tests that open a second
    Database against the same file (the startup-sweep tests), close
    explicitly inside the test to avoid SQLite holding shared file
    locks across handles when GC order is non-deterministic.
    """
    import config as cfg
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
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
    wait_for_job_via_runner(runner, job_id)


def test_enqueue_pipeline_persists_queued_row(tmp_path):
    """A row with status='queued' must be inserted into job_history
    BEFORE the worker thread runs to completion, and must be visible
    cross-connection (different sqlite handle).
    """
    runner, _ = _make_runner_with_db(tmp_path)
    # Block the first pipeline so the read below sees a non-terminal
    # state; without it the trivial lambda runs to completion before
    # the assertion fires and the status would be 'completed'.
    first_started = threading.Event()
    blocker = threading.Event()

    def first_work(job):
        first_started.set()
        blocker.wait(timeout=3.0)
        return {}

    first_id = runner.enqueue_pipeline(
        work_fn=first_work,
        config={}, workspace_id=1,
    )
    _wait_for_event(first_started, "first pipeline start")
    job_id = runner.enqueue_pipeline(
        work_fn=lambda job: None,
        config={"sources": ["/a"]},
        workspace_id=42,
    )

    # Open a fresh Database against the same file to verify the row is
    # actually committed and visible cross-connection — the test fixture's
    # Database isn't the same connection the runner used for the INSERT
    # (enqueue_pipeline opens its own sqlite3 connection), but using a
    # genuinely separate Database removes any ambiguity.
    verify_db = Database(str(tmp_path / "test.db"))
    try:
        row = verify_db.conn.execute(
            "SELECT id, type, status, workspace_id, config "
            "FROM job_history WHERE id = ?",
            (job_id,),
        ).fetchone()
    finally:
        verify_db.close()
    assert row is not None, "enqueue did not persist a job_history row"
    assert row["type"] == "pipeline"
    assert row["status"] == "queued"
    assert row["workspace_id"] == 42

    blocker.set()
    wait_for_job_via_runner(runner, first_id)
    wait_for_job_via_runner(runner, job_id)


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
    first_started = threading.Event()
    blocker = threading.Event()

    def blocking_work(job):
        first_started.set()
        blocker.wait(timeout=3.0)
        return {}

    first_id = runner.enqueue_pipeline(
        work_fn=blocking_work, config={}, workspace_id=1,
    )
    _wait_for_event(first_started, "first pipeline start")
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
    first_started = threading.Event()
    second_started = threading.Event()

    def first_work(job):
        first_started.set()
        blocker.wait(timeout=3.0)
        return {}

    def second_work(job):
        second_started.set()
        return {"should-not-run": True}

    first_id = runner.enqueue_pipeline(work_fn=first_work, config={}, workspace_id=1)
    _wait_for_event(first_started, "first pipeline start")
    second_id = runner.enqueue_pipeline(work_fn=second_work, config={}, workspace_id=1)
    assert runner.get(second_id)["status"] == "queued"

    # Cancel while still queued.
    ok = runner.cancel_job(second_id)
    assert ok is True
    cancelled_view = runner.get(second_id)
    assert cancelled_view is not None
    assert cancelled_view["status"] == "cancelled"
    events = runner.get_events(second_id)
    assert events
    assert events[-1]["type"] == "complete"
    assert events[-1]["data"]["status"] == "cancelled"
    row = db.conn.execute(
        "SELECT status FROM job_history WHERE id = ?", (second_id,),
    ).fetchone()
    assert row["status"] == "cancelled"

    # Releasing the first must NOT promote the cancelled one.
    blocker.set()
    wait_for_job_via_runner(runner, first_id)
    # Wait briefly; the cancelled row must never be promoted.
    assert not second_started.wait(timeout=0.3), (
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
    view = runner.get(job_id)
    assert view is not None
    assert view["status"] == "cancelled"
    events = runner.get_events(job_id)
    assert events
    assert events[-1]["type"] == "complete"
    assert events[-1]["data"]["status"] == "cancelled"


def test_promoting_pipeline_remains_visible_and_cancellable(tmp_path):
    """A queued job in the promotion DB window must not disappear from APIs."""
    runner, db = _make_runner_with_db(tmp_path)
    job_id = "pipeline-1700000000001"
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, error_count) "
        "VALUES (?, 'pipeline', 'queued', '2026-05-26T00:00:00', 0)",
        (job_id,),
    )
    db.conn.commit()
    with runner._lock:
        runner._queued_pipelines[job_id] = {
            "work_fn": lambda job: pytest_fail("should not have run"),
            "config": {"x": 1},
            "workspace_id": 1,
            "runtime_warning": None,
            "started_at": "2026-05-26T00:00:00",
            "_promoting": True,
        }

    view = runner.get(job_id)
    assert view is not None
    assert view["status"] == "queued"
    assert any(j["id"] == job_id for j in runner.list_jobs())

    assert runner.cancel_job(job_id, expected_status="queued") is True
    final = runner.get(job_id)
    assert final is not None
    assert final["status"] == "cancelled"


def test_cancel_promoting_pipeline_after_db_running_preserves_request(tmp_path):
    """Cancelling during the DB-running promotion window must still win."""
    runner, db = _make_runner_with_db(tmp_path)
    job_id = "pipeline-1700000000001"
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, error_count) "
        "VALUES (?, 'pipeline', 'running', '2026-05-26T00:00:00', 0)",
        (job_id,),
    )
    db.conn.commit()
    with runner._lock:
        runner._queued_pipelines[job_id] = {
            "work_fn": lambda job: pytest_fail("should not have run"),
            "config": {"x": 1},
            "workspace_id": 1,
            "runtime_warning": None,
            "started_at": "2026-05-26T00:00:00",
            "_promoting": True,
        }

    assert runner.cancel_job(job_id, expected_status="queued") is True
    assert runner.is_cancelled(job_id)
    with runner._lock:
        assert job_id in runner._queued_pipelines
        assert runner._queued_pipelines[job_id]["_promoting"] is True


def test_cancel_queued_pipeline_after_promotion_finishes_marks_running(tmp_path, monkeypatch):
    """A per-job queued cancel still wins if promotion finishes mid-cancel."""
    import sqlite3

    runner, db = _make_runner_with_db(tmp_path)
    job_id = "pipeline-1700000000001"
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, error_count) "
        "VALUES (?, 'pipeline', 'running', '2026-05-26T00:00:00', 0)",
        (job_id,),
    )
    db.conn.commit()
    with runner._lock:
        runner._queued_pipelines[job_id] = {
            "work_fn": lambda job: pytest_fail("should not have run"),
            "config": {"x": 1},
            "workspace_id": 1,
            "runtime_warning": None,
            "started_at": "2026-05-26T00:00:00",
            "_promoting": True,
        }

    class FakeCursor:
        rowcount = 0

    class FakeConnection:
        def execute(self, *args, **kwargs):
            with runner._lock:
                ctx = runner._queued_pipelines.pop(job_id)
                ctx.pop("_promoting", None)
                runner._jobs[job_id] = {
                    "id": job_id,
                    "type": "pipeline",
                    "status": "running",
                    "started_at": ctx["started_at"],
                    "finished_at": None,
                    "progress": {"current": 0, "total": 0, "current_file": ""},
                    "result": None,
                    "errors": [],
                    "config": ctx["config"],
                    "workspace_id": ctx["workspace_id"],
                    "steps": [],
                    "ephemeral": False,
                    "runtime_warning": ctx["runtime_warning"],
                }
            return FakeCursor()

        def commit(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(sqlite3, "connect", lambda *args, **kwargs: FakeConnection())

    assert runner.cancel_job(job_id) is True
    assert runner.is_cancelled(job_id)


def test_promoted_pipeline_with_pending_cancel_skips_work_fn(tmp_path):
    """A queued cancel recorded during promotion suppresses pipeline work."""
    runner, db = _make_runner_with_db(tmp_path)
    job_id = "pipeline-1700000000001"
    work_called = threading.Event()
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, error_count) "
        "VALUES (?, 'pipeline', 'queued', '2026-05-26T00:00:00', 0)",
        (job_id,),
    )
    db.conn.commit()
    with runner._lock:
        runner._queued_pipelines[job_id] = {
            "work_fn": lambda job: work_called.set(),
            "config": {"x": 1},
            "workspace_id": 1,
            "runtime_warning": None,
            "started_at": "2026-05-26T00:00:00",
        }
        runner._cancelled.add(job_id)

    runner._try_promote_queued()
    final = wait_for_job_via_runner(runner, job_id)
    assert final["status"] == "cancelled"
    assert not work_called.is_set()


def test_promotion_db_error_clears_promoting_marker(tmp_path, monkeypatch):
    """A SQLite promotion failure must not permanently consume SLOT_CAP."""
    import sqlite3

    runner, db = _make_runner_with_db(tmp_path)
    job_id = "pipeline-1700000000001"
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, error_count) "
        "VALUES (?, 'pipeline', 'queued', '2026-05-26T00:00:00', 0)",
        (job_id,),
    )
    db.conn.commit()
    with runner._lock:
        runner._queued_pipelines[job_id] = {
            "work_fn": lambda job: pytest_fail("should not have run"),
            "config": {"x": 1},
            "workspace_id": 1,
            "runtime_warning": None,
            "started_at": "2026-05-26T00:00:00",
        }

    def fail_connect(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(sqlite3, "connect", fail_connect)
    runner._try_promote_queued()

    with runner._lock:
        assert not runner._queued_pipelines[job_id].get("_promoting")
        assert job_id in runner._queued_pipelines


def test_in_flight_promotion_counts_against_slot_cap_not_global_guard(tmp_path, monkeypatch):
    """A promotion in progress should consume one slot, not block all slots.

    This protects the planned ``SLOT_CAP > 1`` case: if one queued context is
    already between the in-memory pick and SQLite UPDATE, another queued
    context should still promote when capacity remains.
    """
    import sqlite3

    import jobs as jobs_module

    monkeypatch.setattr(jobs_module, "SLOT_CAP", 2)
    runner, db = _make_runner_with_db(tmp_path)
    first_id = "pipeline-1700000000001"
    second_id = "pipeline-1700000000002"
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    conn.executemany(
        "INSERT INTO job_history (id, type, status, started_at, error_count) "
        "VALUES (?, 'pipeline', 'queued', ?, 0)",
        [
            (first_id, "2026-05-26T00:00:00"),
            (second_id, "2026-05-26T00:00:01"),
        ],
    )
    conn.commit()
    conn.close()

    with runner._lock:
        runner._queued_pipelines[first_id] = {
            "work_fn": lambda job: None,
            "config": {},
            "workspace_id": 1,
            "runtime_warning": None,
            "started_at": "2026-05-26T00:00:00",
            "_promoting": True,
        }
        runner._queued_pipelines[second_id] = {
            "work_fn": lambda job: None,
            "config": {},
            "workspace_id": 1,
            "runtime_warning": None,
            "started_at": "2026-05-26T00:00:01",
        }

    runner._try_promote_queued()
    promoted = wait_for_job_via_runner(runner, second_id, wait_for_history=True)

    assert promoted["status"] == "completed"
    row = db.conn.execute(
        "SELECT status FROM job_history WHERE id = ?", (second_id,),
    ).fetchone()
    assert row["status"] == "completed"
    with runner._lock:
        assert runner._queued_pipelines[first_id]["_promoting"] is True


def test_cancelled_promotion_candidate_retries_next_queued_pipeline(tmp_path):
    """A cancelled front-of-queue row should not stall later queued work."""
    import sqlite3

    runner, db = _make_runner_with_db(tmp_path)
    cancelled_id = "pipeline-1700000000003"
    next_id = "pipeline-1700000000004"
    conn = sqlite3.connect(str(tmp_path / "test.db"))
    conn.executemany(
        "INSERT INTO job_history (id, type, status, started_at, error_count) "
        "VALUES (?, 'pipeline', ?, ?, 0)",
        [
            (cancelled_id, "cancelled", "2026-05-26T00:00:00"),
            (next_id, "queued", "2026-05-26T00:00:01"),
        ],
    )
    conn.commit()
    conn.close()

    with runner._lock:
        runner._queued_pipelines[cancelled_id] = {
            "work_fn": lambda job: pytest_fail("cancelled job must not run"),
            "config": {},
            "workspace_id": 1,
            "runtime_warning": None,
            "started_at": "2026-05-26T00:00:00",
        }
        runner._queued_pipelines[next_id] = {
            "work_fn": lambda job: None,
            "config": {},
            "workspace_id": 1,
            "runtime_warning": None,
            "started_at": "2026-05-26T00:00:01",
        }

    runner._try_promote_queued()
    promoted = wait_for_job_via_runner(runner, next_id, wait_for_history=True)

    assert promoted["status"] == "completed"
    with runner._lock:
        assert cancelled_id not in runner._queued_pipelines
    row = db.conn.execute(
        "SELECT status FROM job_history WHERE id = ?", (next_id,),
    ).fetchone()
    assert row["status"] == "completed"


def pytest_fail(msg):
    raise AssertionError(msg)


def test_retention_does_not_prune_queued_row_under_load(tmp_path):
    """Codex P2 regression: ``_persist_job`` retains the 100 most-recent
    rows per workspace. If a queued pipeline sits behind a busy slot
    while >100 other jobs complete in the same workspace, the old
    retention DELETE (which keyed only on started_at) would prune the
    queued row. The subsequent promotion conditional UPDATE then sees
    rowcount==0 and silently drops the queued context.

    The fixed retention DELETE filters to terminal statuses so
    non-terminal rows survive regardless of count.
    """
    runner, db = _make_runner_with_db(tmp_path)
    try:
        ws = db._active_workspace_id
        first_started = threading.Event()
        blocker = threading.Event()

        def first_work(job):
            first_started.set()
            blocker.wait(timeout=10.0)
            return {}

        # Long-running first pipeline holds the slot.
        first_id = runner.enqueue_pipeline(
            work_fn=first_work,
            config={}, workspace_id=ws,
        )
        _wait_for_event(first_started, "first pipeline start")
        # Queued pipeline.
        queued_id = runner.enqueue_pipeline(
            work_fn=lambda job: {"ran": True},
            config={}, workspace_id=ws,
        )

        # Simulate 105 unrelated jobs completing in this workspace by
        # writing terminal rows directly. They share the workspace_id
        # so they're candidates for the retention DELETE.
        #
        # Timestamps MUST be newer than the queued row's runtime
        # started_at (set by enqueue_pipeline via datetime.now()).
        # Otherwise the OLD buggy retention DELETE — which orders by
        # started_at DESC and keeps the top 100 — would naturally keep
        # the queued row (newer timestamp wins) and the test would
        # pass even against the broken code, defeating its purpose.
        # Use a far-future date so the order is unambiguous regardless
        # of when the test runs.
        import sqlite3
        conn = sqlite3.connect(str(tmp_path / "test.db"))
        try:
            future_ts = "2099-01-01T00:00:00"
            for i in range(105):
                conn.execute(
                    "INSERT OR REPLACE INTO job_history "
                    "(id, type, status, started_at, finished_at, "
                    " duration, error_count, workspace_id) "
                    "VALUES (?, 'scan', 'completed', ?, ?, 0.1, 0, ?)",
                    (f"scan-fill-{i:03d}", future_ts, future_ts, ws),
                )
            conn.commit()
        finally:
            conn.close()

        # Trigger _persist_job's retention by completing one more job
        # via the runner. Use a non-pipeline type so it goes through
        # runner.start (which persists immediately on finish).
        fill_id = runner.start(
            "scan", lambda job: None, workspace_id=ws,
        )
        wait_for_job_via_runner(runner, fill_id)

        # The queued row MUST still exist after retention ran.
        row = db.conn.execute(
            "SELECT id, status FROM job_history WHERE id = ?", (queued_id,),
        ).fetchone()
        assert row is not None, (
            "queued row was pruned by retention DELETE — promotion would "
            "now see rowcount==0 and silently drop the run"
        )
        assert row["status"] == "queued"

        # And promotion still works when the slot opens.
        blocker.set()
        wait_for_job_via_runner(runner, first_id)
        second_final = wait_for_job_via_runner(runner, queued_id)
        assert second_final["status"] == "completed"
        assert second_final["result"] == {"ran": True}
    finally:
        db.close()


def test_get_history_excludes_queued_rows(tmp_path):
    """Codex P2 regression: queued pipeline rows live in job_history
    immediately on enqueue, but they're LIVE state, not history. The
    /jobs page and bottom-panel render history as completed runs with
    no cancel affordance — a queued row showing up there is confusing
    UX. ``get_history`` must filter to terminal statuses.
    """
    runner, db = _make_runner_with_db(tmp_path)
    try:
        ws = db._active_workspace_id

        first_started = threading.Event()
        blocker = threading.Event()

        def first_work(job):
            first_started.set()
            blocker.wait(timeout=3.0)
            return {}

        first_id = runner.enqueue_pipeline(
            work_fn=first_work,
            config={}, workspace_id=ws,
        )
        _wait_for_event(first_started, "first pipeline start")
        queued_id = runner.enqueue_pipeline(
            work_fn=lambda job: None, config={}, workspace_id=ws,
        )

        # Sanity: both rows exist with non-terminal status.
        assert runner.get(queued_id)["status"] == "queued"

        # History must NOT include either live row.
        history_ids = [j["id"] for j in runner.get_history(db, limit=50)]
        assert first_id not in history_ids, (
            "running rows belong to live state, not history"
        )
        assert queued_id not in history_ids, (
            "queued rows belong to live state, not history"
        )

        blocker.set()
        wait_for_job_via_runner(runner, first_id)
        wait_for_job_via_runner(runner, queued_id)

        # After completion both DO appear in history.
        history_ids = [j["id"] for j in runner.get_history(db, limit=50)]
        assert first_id in history_ids
        assert queued_id in history_ids
    finally:
        db.close()


def test_list_jobs_includes_queued_pipelines(tmp_path):
    """Codex P2 regression: the navbar and /jobs page build their
    active-jobs list from ``runner.list_jobs()``. Queued pipelines live
    only in ``_queued_pipelines`` until promotion — if they're not
    surfaced through ``list_jobs`` they disappear from the app-wide UI
    and can't be cancelled from /jobs.
    """
    runner, _ = _make_runner_with_db(tmp_path)
    first_started = threading.Event()
    blocker = threading.Event()

    def first_work(job):
        first_started.set()
        blocker.wait(timeout=3.0)
        return {}

    first_id = runner.enqueue_pipeline(first_work, config={}, workspace_id=1)
    _wait_for_event(first_started, "first pipeline start")
    second_id = runner.enqueue_pipeline(
        lambda job: None, config={"x": 7}, workspace_id=2,
    )

    all_jobs = runner.list_jobs()
    ids = {j["id"]: j for j in all_jobs}
    assert first_id in ids
    assert second_id in ids, (
        "queued pipeline must be visible through list_jobs() so the "
        "navbar/jobs page can render and cancel it"
    )
    assert ids[second_id]["status"] == "queued"
    assert ids[second_id]["type"] == "pipeline"
    assert ids[second_id]["config"] == {"x": 7}
    assert ids[second_id]["workspace_id"] == 2

    blocker.set()
    wait_for_job_via_runner(runner, first_id)
    wait_for_job_via_runner(runner, second_id)


def test_cancelling_queued_pipeline_emits_complete_event_to_sse(tmp_path):
    """Codex P2 regression: when a queued pipeline is cancelled while a
    client is subscribed to its SSE stream, the client must receive a
    terminal 'complete' event with status='cancelled' so it closes the
    stream cleanly. Without this the runner.get() lookup turns None
    and the SSE loop reports the job as 'expired'.
    """
    runner, _ = _make_runner_with_db(tmp_path)
    first_started = threading.Event()
    blocker = threading.Event()

    def first_work(job):
        first_started.set()
        blocker.wait(timeout=3.0)
        return {}

    first_id = runner.enqueue_pipeline(first_work, config={}, workspace_id=1)
    _wait_for_event(first_started, "first pipeline start")
    second_id = runner.enqueue_pipeline(
        lambda job: pytest_fail("cancelled queued must not run"),
        config={}, workspace_id=1,
    )
    assert runner.get(second_id)["status"] == "queued"

    # Subscribe BEFORE cancel — mirrors a UI tab that hit /api/jobs/<id>/stream
    # while the run was still waiting in the queue.
    q = runner.subscribe(second_id)

    assert runner.cancel_job(second_id) is True

    evt = q.get(timeout=1.0)
    assert evt["type"] == "complete"
    assert evt["data"]["status"] == "cancelled"

    blocker.set()
    wait_for_job_via_runner(runner, first_id)


def test_sse_subscribers_attached_while_queued_receive_post_promotion_events(tmp_path):
    """A client connected to /api/jobs/<id>/stream while the job is
    queued must keep receiving events once the job is promoted to
    running. Otherwise the UI sees ``status: queued`` forever even
    after the pipeline actually starts.
    """
    runner, _ = _make_runner_with_db(tmp_path)
    first_started = threading.Event()
    blocker = threading.Event()
    let_second_finish = threading.Event()

    def first_work(job):
        first_started.set()
        blocker.wait(timeout=3.0)
        return {}

    def second_work(job):
        runner.push_event(job["id"], "progress", {"phase": "started"})
        let_second_finish.wait(timeout=3.0)
        return {"ok": True}

    first_id = runner.enqueue_pipeline(first_work, config={}, workspace_id=1)
    _wait_for_event(first_started, "first pipeline start")
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
