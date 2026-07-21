# vireo/tests/test_jobs.py
import logging
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.dirname(__file__))

from wait import wait_for_job_via_runner


def test_job_runner_starts_and_completes(tmp_path):
    """JobRunner runs a function in a background thread and tracks completion."""
    from jobs import JobRunner

    runner = JobRunner()

    def work(job):
        job['progress']['total'] = 3
        for i in range(3):
            job['progress']['current'] = i + 1
            time.sleep(0.01)
        return {'items': 3}

    job_id = runner.start('test', work, config={'note': 'hello'})
    assert job_id is not None

    job = wait_for_job_via_runner(runner, job_id)
    assert job['status'] == 'completed'
    assert job['result'] == {'items': 3}
    assert job['progress']['current'] == 3


def _wait_for_status(runner, job_id, status, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = runner.get(job_id)
        if job and job.get("status") == status:
            return job
        time.sleep(0.01)
    raise AssertionError(
        f"job {job_id} did not reach {status}; last={runner.get(job_id)!r}"
    )


def test_pausable_job_stops_at_checkpoint_and_resumes():
    """Paused work retains its local state and continues after Resume."""
    from jobs import JobRunner

    runner = JobRunner()
    progress = {"count": 0}
    finish = threading.Event()

    def work(job):
        while not finish.is_set():
            if runner.is_cancelled(job["id"]):
                break
            progress["count"] += 1
            time.sleep(0.01)
        return {"count": progress["count"]}

    job_id = runner.start("scan", work, pausable=True)
    deadline = time.monotonic() + 2
    while progress["count"] < 3 and time.monotonic() < deadline:
        time.sleep(0.01)

    assert runner.pause_job(job_id) is True
    _wait_for_status(runner, job_id, "paused")
    paused_count = progress["count"]
    time.sleep(0.1)
    assert progress["count"] == paused_count

    assert runner.resume_job(job_id) is True
    finish.set()
    job = wait_for_job_via_runner(runner, job_id)
    assert job["status"] == "completed"
    assert job["result"]["count"] >= paused_count
    status_events = [
        event["data"]["status"]
        for event in runner.get_events(job_id)
        if event["type"] == "status"
    ]
    assert status_events == ["pausing", "paused", "running"]


def test_pipeline_pause_gate_waits_for_every_active_worker():
    """A pipeline is not reported paused while one worker is still in-flight."""
    from jobs import JobRunner
    from pipeline_job import _PipelinePauseGate

    runner = JobRunner()
    release_slow_worker = threading.Event()
    slow_worker_started = threading.Event()
    stop = threading.Event()
    counts = {"fast": 0, "slow": 0}

    def work(job):
        gate = _PipelinePauseGate(runner, job["id"])
        gate.register_many(("fast", "slow"))

        def fast_worker():
            try:
                while True:
                    if gate.checkpoint("fast") or stop.is_set():
                        return
                    counts["fast"] += 1
                    time.sleep(0.005)
            finally:
                gate.unregister("fast")

        def slow_worker():
            try:
                # Simulate a model/GPU batch that cannot be interrupted until
                # it reaches its next safe boundary.
                slow_worker_started.set()
                assert release_slow_worker.wait(timeout=3)
                while True:
                    if gate.checkpoint("slow") or stop.is_set():
                        return
                    counts["slow"] += 1
                    time.sleep(0.005)
            finally:
                gate.unregister("slow")

        threads = [
            threading.Thread(target=fast_worker),
            threading.Thread(target=slow_worker),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
            assert not thread.is_alive()
        return dict(counts)

    job_id = runner.start("pipeline", work, pausable=True)
    assert slow_worker_started.wait(timeout=2)
    deadline = time.monotonic() + 2
    while counts["fast"] < 3 and time.monotonic() < deadline:
        time.sleep(0.01)

    assert runner.pause_job(job_id) is True
    time.sleep(0.05)
    assert runner.get(job_id)["status"] == "pausing"

    release_slow_worker.set()
    _wait_for_status(runner, job_id, "paused")
    paused_counts = dict(counts)
    time.sleep(0.05)
    assert counts == paused_counts

    assert runner.resume_job(job_id) is True
    stop.set()
    job = wait_for_job_via_runner(runner, job_id)
    assert job["status"] == "completed"


def test_pause_status_events_stay_ordered_through_completion():
    """Competing pause and worker transitions cannot publish stale states."""
    from jobs import JobRunner

    runner = JobRunner()
    work_started = threading.Event()
    enter_checkpoint = threading.Event()
    checkpoint_reached = threading.Event()
    pausing_published = threading.Event()
    release_pausing = threading.Event()
    original_publish = runner._publish_status_locked

    def controlled_publish(job, status):
        original_publish(job, status)
        if status == "pausing":
            pausing_published.set()
            assert release_pausing.wait(timeout=2)

    runner._publish_status_locked = controlled_publish

    def work(job):
        work_started.set()
        assert enter_checkpoint.wait(timeout=2)
        checkpoint_reached.set()
        runner.is_cancelled(job["id"])
        return {}

    job_id = runner.start("scan", work, pausable=True)
    assert work_started.wait(timeout=2)

    pause_result = []
    pause_thread = threading.Thread(
        target=lambda: pause_result.append(runner.pause_job(job_id))
    )
    pause_thread.start()
    assert pausing_published.wait(timeout=2)

    # Let the worker race for the same lock while the pausing transition is
    # still publishing. It must not overtake that event with "paused".
    enter_checkpoint.set()
    assert checkpoint_reached.wait(timeout=2)
    release_pausing.set()
    pause_thread.join(timeout=2)
    assert not pause_thread.is_alive()
    assert pause_result == [True]

    _wait_for_status(runner, job_id, "paused")
    assert runner.resume_job(job_id) is True
    assert wait_for_job_via_runner(runner, job_id)["status"] == "completed"

    transitions = [
        (event["type"], event["data"]["status"])
        for event in runner.get_events(job_id)
        if event["type"] in ("status", "complete")
    ]
    assert transitions == [
        ("status", "pausing"),
        ("status", "paused"),
        ("status", "running"),
        ("complete", "completed"),
    ]


def test_cancel_paused_job_wakes_worker_and_marks_cancelled():
    """Cancel must not leave a paused worker sleeping forever."""
    from jobs import JobRunner

    runner = JobRunner()

    def work(job):
        while True:
            if runner.is_cancelled(job["id"]):
                return {"stopped": True}
            time.sleep(0.01)

    job_id = runner.start("import", work, pausable=True)
    assert runner.pause_job(job_id) is True
    _wait_for_status(runner, job_id, "paused")
    assert runner.cancel_job(job_id) is True

    job = wait_for_job_via_runner(runner, job_id)
    assert job["status"] == "cancelled"
    assert job["result"] == {"stopped": True}


def test_non_pausable_job_rejects_pause():
    """The UI capability flag is backed by runner enforcement."""
    from jobs import JobRunner

    runner = JobRunner()
    release = threading.Event()

    def work(_job):
        release.wait(timeout=2)
        return {}

    job_id = runner.start("test", work)
    assert runner.pause_job(job_id) is False
    assert runner.get(job_id)["pausable"] is False
    release.set()
    assert wait_for_job_via_runner(runner, job_id)["status"] == "completed"


def test_job_runner_tracks_failure(tmp_path):
    """JobRunner marks job as failed when work function raises."""
    from jobs import JobRunner

    runner = JobRunner()

    def failing_work(job):
        raise ValueError("something broke")

    job_id = runner.start('test', failing_work)

    job = wait_for_job_via_runner(runner, job_id)
    assert job['status'] == 'failed'
    assert len(job['errors']) >= 1
    assert 'something broke' in job['errors'][0]


def test_job_runner_does_not_duplicate_preexisting_errors():
    """When work_fn records its own errors into job['errors'] and then raises
    with the same message, the failure handler must not double-count it.

    Pipelines do exactly this: stages append to job['errors'] directly, and
    run_pipeline_job re-raises with errors[0]. Without the dedupe, the error
    shows up twice and inflates error_count in job_history.
    """
    from jobs import JobRunner

    runner = JobRunner()

    def failing_work(job):
        job['errors'].append("[model_loader] Fatal: model_path must not be empty")
        raise RuntimeError("[model_loader] Fatal: model_path must not be empty")

    job_id = runner.start('test', failing_work)

    wait_for_job_via_runner(runner, job_id)

    job = runner.get(job_id)
    assert job['status'] == 'failed'
    # Exactly one error entry — the one the work function already recorded.
    assert job['errors'] == [
        "[model_loader] Fatal: model_path must not be empty"
    ], f"Expected single error entry, got: {job['errors']}"


def test_job_runner_still_records_novel_exception_text():
    """If the exception from work_fn is *different* from any pre-recorded
    error, it should still be appended (the dedupe is targeted, not blanket).
    """
    from jobs import JobRunner

    runner = JobRunner()

    def failing_work(job):
        job['errors'].append("stage warning: something odd")
        raise RuntimeError("orchestrator failure: unexpected state")

    job_id = runner.start('test', failing_work)

    wait_for_job_via_runner(runner, job_id)

    job = runner.get(job_id)
    assert job['status'] == 'failed'
    assert len(job['errors']) == 2
    assert "stage warning: something odd" in job['errors']
    assert "orchestrator failure: unexpected state" in job['errors']


def test_job_result_ok_false_marks_failed(tmp_path):
    """A work function that returns normally but signals failure via
    {"ok": False, "errors": [...]} must be recorded as 'failed', not
    'completed' — and its result errors must be folded into error_count.

    This is the move-folder case: rsync times out, move_folder returns
    {"moved": 0, "errors": [...]} (no exception), and the run used to read
    as "completed, 0 errors" in history.
    """
    from jobs import JobRunner

    runner = JobRunner()

    def work(job):
        return {"moved": 0, "errors": ["rsync timed out"], "ok": False}

    job_id = runner.start('move-folder', work)

    job = wait_for_job_via_runner(runner, job_id)
    assert job['status'] == 'failed'
    assert "rsync timed out" in job['errors']


def test_job_result_ok_true_with_warnings_stays_completed(tmp_path):
    """A work function returning {"ok": True, "errors": [...]} represents a
    partial success: the job completes, but the result errors are still
    folded into the job's error tally so the count is honest."""
    from jobs import JobRunner

    runner = JobRunner()

    def work(job):
        return {"moved": 5, "errors": ["one file skipped"], "ok": True}

    job_id = runner.start('move-folder', work)

    job = wait_for_job_via_runner(runner, job_id)
    assert job['status'] == 'completed'
    assert "one file skipped" in job['errors']


def test_job_result_without_ok_key_unaffected(tmp_path):
    """The ok/errors folding is opt-in: a result dict with no "ok" key keeps
    today's behavior (completed, runner-level error list untouched)."""
    from jobs import JobRunner

    runner = JobRunner()

    def work(job):
        return {"moved": 0, "errors": ["informational note"]}

    job_id = runner.start('test', work)

    job = wait_for_job_via_runner(runner, job_id)
    assert job['status'] == 'completed'
    assert job['errors'] == []


def test_job_result_ok_false_persists_failed_with_error_count(tmp_path):
    """End-to-end: an ok=False result is persisted to job_history with
    status='failed' and error_count reflecting the result errors."""
    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    runner = JobRunner(db=db)

    def work(job):
        return {
            "moved": 0,
            "errors": ["rsync timed out", "renameat: Operation timed out"],
            "ok": False,
            "summary": "Move failed — rsync timed out",
        }

    job_id = runner.start('move-folder', work)

    row = None
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        row = db.conn.execute(
            "SELECT status, error_count, summary FROM job_history WHERE id = ?",
            (job_id,),
        ).fetchone()
        if row is not None:
            break
        time.sleep(0.05)
    assert row is not None
    assert row["status"] == "failed"
    assert row["error_count"] == 2, f"expected error_count=2, got {row['error_count']}"
    assert row["summary"] == "Move failed — rsync timed out"


def test_failed_job_history_preserves_structured_result(tmp_path):
    """When a work function stashes a structured result on job['result']
    before raising, _persist_job must preserve that structure in history
    (merging the error into it) rather than replacing it with {"error": ...}.

    This is what lets the pipeline UI render per-stage details on a failed
    run — it reads result.result.stages and result.result.errors.
    """
    import json as _json

    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    runner = JobRunner(db=db)

    def failing_pipeline_like(job):
        # Simulate pipeline_job's behavior: build a result dict, attach it to
        # the job, and raise with the first error message.
        job['result'] = {
            "stages": {
                "scan": {"status": "completed", "count": 10},
                "model_loader": {"status": "failed"},
            },
            "errors": ["[model_loader] Fatal: model_path must not be empty"],
            "duration": 1.2,
        }
        job['errors'].append("[model_loader] Fatal: model_path must not be empty")
        raise RuntimeError("[model_loader] Fatal: model_path must not be empty")

    job_id = runner.start('pipeline', failing_pipeline_like)

    # Wait until _persist_job has flushed the row to SQLite. The prior
    # manual 5s poll raced Windows CI, where WAL contention pushed the
    # write past the deadline; wait_for_job_via_runner blocks on the
    # runner's own _persisted flag with a 30s default budget.
    wait_for_job_via_runner(runner, job_id, wait_for_history=True)
    row = db.conn.execute(
        "SELECT result, error_count FROM job_history WHERE id = ?", (job_id,)
    ).fetchone()
    assert row is not None

    stored = _json.loads(row["result"])
    # Structured result must survive — the stages dict is what the UI needs.
    assert "stages" in stored, f"expected stages in stored result, got: {stored}"
    assert stored["stages"]["model_loader"]["status"] == "failed"
    # The error must be merged in, not replacing the structure.
    assert stored.get("error") == "[model_loader] Fatal: model_path must not be empty"
    # Exactly one error entry (dedupe is working) → error_count == 1.
    assert row["error_count"] == 1, f"expected error_count=1, got {row['error_count']}"


def test_failed_job_history_falls_back_when_no_structured_result(tmp_path):
    """When work_fn raises without stashing a result, persist the minimal
    {"error": ...} payload as before — the fallback path still works."""
    import json as _json

    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    runner = JobRunner(db=db)

    def failing_work(job):
        raise RuntimeError("boom")

    job_id = runner.start('test', failing_work)

    row = None
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        row = db.conn.execute(
            "SELECT result FROM job_history WHERE id = ?", (job_id,)
        ).fetchone()
        if row is not None:
            break
        time.sleep(0.05)
    assert row is not None
    stored = _json.loads(row["result"])
    assert stored == {"error": "boom"}


def test_job_runner_list_jobs():
    """JobRunner.list_jobs returns all jobs."""
    from jobs import JobRunner

    runner = JobRunner()

    def quick(job):
        return {'ok': True}

    runner.start('scan', quick)
    runner.start('thumbnails', quick)
    time.sleep(0.2)

    jobs = runner.list_jobs()
    assert len(jobs) >= 2


def test_job_progress_events():
    """Job progress updates are captured in the events queue."""
    from jobs import JobRunner

    runner = JobRunner()

    def work(job):
        job['progress']['total'] = 2
        for i in range(2):
            job['progress']['current'] = i + 1
            job['progress']['current_file'] = f'file_{i}.jpg'
            runner.push_event(job['id'], 'progress', dict(job['progress']))
            time.sleep(0.01)
        return {'done': True}

    job_id = runner.start('scan', work)

    wait_for_job_via_runner(runner, job_id)

    events = runner.get_events(job_id)
    assert len(events) >= 2
    assert events[0]['type'] == 'progress'


def test_log_broadcaster_captures_logs():
    """LogBroadcaster captures log records into a ring buffer."""
    from jobs import LogBroadcaster

    broadcaster = LogBroadcaster(buffer_size=50)
    broadcaster.install()

    logger = logging.getLogger('test.broadcaster')
    logger.setLevel(logging.DEBUG)
    logger.warning("test warning message")
    logger.info("test info message")

    recent = broadcaster.get_recent(10)
    assert len(recent) >= 2

    messages = [r['message'] for r in recent]
    assert 'test warning message' in messages
    assert 'test info message' in messages

    broadcaster.uninstall()


def test_log_broadcaster_subscriber():
    """LogBroadcaster pushes records to subscriber queues."""

    from jobs import LogBroadcaster

    broadcaster = LogBroadcaster(buffer_size=50)
    broadcaster.install()

    q = broadcaster.subscribe()

    logger = logging.getLogger('test.subscriber')
    logger.setLevel(logging.DEBUG)
    logger.info("subscriber test")

    # Should be in the queue
    try:
        record = q.get(timeout=1)
        assert record['message'] == 'subscriber test'
    finally:
        broadcaster.unsubscribe(q)
        broadcaster.uninstall()


def test_job_history_persistence(tmp_path):
    """JobRunner saves completed jobs to job_history table."""
    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    runner = JobRunner(db=db)

    def work(job):
        return {'photos': 42}

    job_id = runner.start('scan', work, config={'root': '/photos'})

    # wait_for_history=True blocks until the worker thread has flushed the
    # row to SQLite (job["_persisted"]). The previous fixed time.sleep(0.1)
    # was too short on slow Windows CI runners, where the worker thread had
    # not yet committed by the time db.conn read the row.
    wait_for_job_via_runner(runner, job_id, wait_for_history=True)

    rows = db.conn.execute("SELECT * FROM job_history WHERE id = ?", (job_id,)).fetchall()
    assert len(rows) == 1
    assert rows[0]['type'] == 'scan'
    assert rows[0]['status'] == 'completed'


def test_ephemeral_job_skips_history_persistence(tmp_path):
    """Ephemeral jobs run normally but never write a row to job_history."""
    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    runner = JobRunner(db=db)

    def work(job):
        return {"new_count": 7}

    job_id = runner.start("new_images_walk", work, ephemeral=True)

    wait_for_job_via_runner(runner, job_id)
    time.sleep(0.1)

    job = runner.get(job_id)
    assert job["status"] == "completed"
    assert job["result"] == {"new_count": 7}

    rows = db.conn.execute(
        "SELECT * FROM job_history WHERE id = ?", (job_id,)
    ).fetchall()
    assert rows == [], "ephemeral jobs must not be persisted to job_history"


def test_jobs_count_for_badge_by_default():
    """Jobs opt into attention badges unless explicitly marked ambient."""
    from jobs import JobRunner

    runner = JobRunner()
    release = threading.Event()

    def work(job):
        release.wait(timeout=2)

    job_id = runner.start("scan", work)
    try:
        job = runner.get(job_id)
        assert job["counts_for_badge"] is True
    finally:
        release.set()
        wait_for_job_via_runner(runner, job_id)


def test_job_can_opt_out_of_badge_counting():
    """Ambient jobs stay listed but do not contribute to app badges."""
    from jobs import JobRunner

    runner = JobRunner()
    release = threading.Event()

    def work(job):
        release.wait(timeout=2)

    job_id = runner.start("new_images_walk", work, counts_for_badge=False)
    try:
        job = runner.get(job_id)
        assert job["counts_for_badge"] is False
    finally:
        release.set()
        wait_for_job_via_runner(runner, job_id)


def test_ephemeral_failed_job_skips_history_persistence(tmp_path):
    """Ephemeral jobs that fail still must not be persisted."""
    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    runner = JobRunner(db=db)

    def work(job):
        raise RuntimeError("boom")

    job_id = runner.start("new_images_walk", work, ephemeral=True)

    wait_for_job_via_runner(runner, job_id)
    time.sleep(0.1)

    rows = db.conn.execute(
        "SELECT * FROM job_history WHERE id = ?", (job_id,)
    ).fetchall()
    assert rows == []


def test_job_history_stores_tree_and_summary(tmp_path):
    """Job history persists tree JSON and summary string."""
    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    runner = JobRunner(db=db)

    cols = [row[1] for row in db.conn.execute("PRAGMA table_info(job_history)").fetchall()]
    assert "tree" in cols
    assert "summary" in cols


def test_job_steps_tracking(tmp_path):
    """Jobs can define and update execution steps."""

    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    runner = JobRunner(db=db)

    def work(job):
        runner.set_steps(job["id"], [
            {"id": "scan", "label": "Scan folders"},
            {"id": "index", "label": "Index photos"},
            {"id": "thumbs", "label": "Generate thumbnails"},
        ])
        runner.update_step(job["id"], "scan", status="running")
        runner.update_step(job["id"], "scan", status="completed", summary="142 folders")
        runner.update_step(job["id"], "index", status="running",
                           progress={"current": 50, "total": 100})
        return {"photos_indexed": 100}

    job_id = runner.start("scan", work, workspace_id=1)

    wait_for_job_via_runner(runner, job_id)

    j = runner.get(job_id)
    assert j["status"] == "completed"
    assert "steps" in j
    assert len(j["steps"]) == 3
    assert j["steps"][0]["status"] == "completed"
    assert j["steps"][0]["summary"] == "142 folders"
    assert j["steps"][1]["progress"]["current"] == 50
    assert j["steps"][2]["status"] == "pending"


def test_job_history_persists_steps_tree(tmp_path):
    """Completed jobs persist their step tree to job_history."""
    import json

    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    runner = JobRunner(db=db)

    def work(job):
        runner.set_steps(job["id"], [
            {"id": "scan", "label": "Scan folders"},
            {"id": "index", "label": "Index photos"},
        ])
        runner.update_step(job["id"], "scan", status="running")
        runner.update_step(job["id"], "scan", status="completed", summary="50 folders")
        runner.update_step(job["id"], "index", status="running")
        runner.update_step(job["id"], "index", status="completed", summary="200 photos")
        return {"photos_indexed": 200}

    job_id = runner.start("scan", work, workspace_id=ws_id)

    # wait_for_history=True blocks until the worker thread has flushed the
    # job_history row; the previous fixed time.sleep(0.5) raced the worker
    # on slower Windows I/O and left get_history returning [].
    wait_for_job_via_runner(runner, job_id, wait_for_history=True)

    history = runner.get_history(db, limit=1)
    assert len(history) > 0
    row = history[0]
    assert row["tree"] is not None
    tree = json.loads(row["tree"]) if isinstance(row["tree"], str) else row["tree"]
    assert len(tree) == 2
    assert tree[0]["id"] == "scan"
    assert tree[0]["status"] == "completed"
    assert row["summary"] != ""


def test_job_history_prunes_to_100(tmp_path):
    """Job history prunes entries beyond 100 per workspace."""
    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    runner = JobRunner(db=db)

    for i in range(101):
        db.conn.execute(
            """INSERT INTO job_history
               (id, type, status, started_at, finished_at, duration,
                result, error_count, config, workspace_id, tree, summary)
               VALUES (?, 'test', 'completed', ?, ?, 1.0, '{}', 0, '{}', ?, '[]', 'test')""",
            (f"test-{i}", f"2026-01-01T00:{i:02d}:00", f"2026-01-01T00:{i:02d}:01", ws_id),
        )
    db.conn.commit()

    def work(job):
        return {}

    # Pruning happens inside _persist_job (INSERT + retention DELETE), and
    # _persisted flips true only after both commit. A fixed sleep raced the
    # worker thread on slower Windows I/O; wait_for_history is the exact
    # sync point.
    job_id = runner.start("test", work, workspace_id=ws_id)
    wait_for_job_via_runner(runner, job_id, wait_for_history=True)

    count = db.conn.execute(
        "SELECT COUNT(*) FROM job_history WHERE workspace_id = ?", (ws_id,)
    ).fetchone()[0]
    assert count <= 100


def test_progress_events_include_steps(tmp_path):
    """Progress events include the steps array when steps are defined."""

    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    runner = JobRunner(db=db)

    def work(job):
        runner.set_steps(job["id"], [
            {"id": "step1", "label": "Step One"},
            {"id": "step2", "label": "Step Two"},
        ])
        runner.update_step(job["id"], "step1", status="running")
        runner.push_event(job["id"], "progress", {
            "phase": "Step One",
            "current": 5,
            "total": 10,
        })
        return {}

    job_id = runner.start("test", work, workspace_id=1)

    wait_for_job_via_runner(runner, job_id)

    events = runner.get_events(job_id)
    progress_events = [e for e in events if e["type"] == "progress"]
    assert len(progress_events) > 0
    last_progress = progress_events[-1]
    assert "steps" in last_progress["data"]
    assert len(last_progress["data"]["steps"]) == 2
    assert last_progress["data"]["steps"][0]["status"] == "running"


def test_push_event_mirrors_progress_onto_job(tmp_path):
    """push_event('progress', ...) merges fields onto job['progress'] so
    polling clients (which don't subscribe to SSE) see the latest phase
    and current_file."""
    import threading
    import time

    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    runner = JobRunner(db=db)

    gate = threading.Event()

    def work(job):
        runner.push_event(job["id"], "progress", {
            "phase": "Step 3/5: Computing embeddings",
            "current": 150,
            "total": 843,
            "current_file": "Computing label embeddings (150/843)...",
        })
        gate.wait(timeout=2)
        return {}

    job_id = runner.start("test", work, workspace_id=1)

    # Read progress while the job is still running — this is what the UI does.
    deadline = time.time() + 2
    while time.time() < deadline:
        j = runner.get(job_id)
        if j and j["progress"].get("phase"):
            break
        time.sleep(0.02)

    j = runner.get(job_id)
    assert j["progress"]["phase"] == "Step 3/5: Computing embeddings"
    assert j["progress"]["current"] == 150
    assert j["progress"]["total"] == 843
    assert j["progress"]["current_file"] == "Computing label embeddings (150/843)..."
    # 'steps' must not leak into the stored progress (it is injected only
    # onto the outbound event payload).
    assert "steps" not in j["progress"]

    gate.set()
