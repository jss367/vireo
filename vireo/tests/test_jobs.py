# vireo/tests/test_jobs.py
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


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

    # Wait for completion
    for _ in range(50):
        job = runner.get(job_id)
        if job['status'] == 'completed':
            break
        time.sleep(0.05)

    job = runner.get(job_id)
    assert job['status'] == 'completed'
    assert job['result'] == {'items': 3}
    assert job['progress']['current'] == 3


def test_job_runner_tracks_failure(tmp_path):
    """JobRunner marks job as failed when work function raises."""
    from jobs import JobRunner

    runner = JobRunner()

    def failing_work(job):
        raise ValueError("something broke")

    job_id = runner.start('test', failing_work)

    for _ in range(50):
        job = runner.get(job_id)
        if job['status'] == 'failed':
            break
        time.sleep(0.05)

    job = runner.get(job_id)
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

    for _ in range(50):
        job = runner.get(job_id)
        if job['status'] == 'failed':
            break
        time.sleep(0.05)

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

    for _ in range(50):
        job = runner.get(job_id)
        if job['status'] == 'failed':
            break
        time.sleep(0.05)

    job = runner.get(job_id)
    assert job['status'] == 'failed'
    assert len(job['errors']) == 2
    assert "stage warning: something odd" in job['errors']
    assert "orchestrator failure: unexpected state" in job['errors']


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

    # Poll for the persisted row rather than sleeping a fixed interval —
    # the worker thread sets status='failed' before the finally block runs
    # _persist_job, so a fixed sleep races on slow runners.
    row = None
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        row = db.conn.execute(
            "SELECT result, error_count FROM job_history WHERE id = ?", (job_id,)
        ).fetchone()
        if row is not None:
            break
        time.sleep(0.05)
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

    for _ in range(50):
        job = runner.get(job_id)
        if job['status'] == 'completed':
            break
        time.sleep(0.05)

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

    for _ in range(50):
        job = runner.get(job_id)
        if job['status'] == 'completed':
            break
        time.sleep(0.05)

    # Give it a moment to persist
    time.sleep(0.1)

    rows = db.conn.execute("SELECT * FROM job_history WHERE id = ?", (job_id,)).fetchall()
    assert len(rows) == 1
    assert rows[0]['type'] == 'scan'
    assert rows[0]['status'] == 'completed'


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
    import time

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

    for _ in range(50):
        j = runner.get(job_id)
        if j and j["status"] != "running":
            break
        time.sleep(0.1)

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
    import time

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

    for _ in range(50):
        j = runner.get(job_id)
        if j and j["status"] != "running":
            break
        time.sleep(0.1)

    time.sleep(0.5)

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
    import time

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

    job_id = runner.start("test", work, workspace_id=ws_id)
    for _ in range(50):
        j = runner.get(job_id)
        if j and j["status"] != "running":
            break
        time.sleep(0.1)
    time.sleep(0.5)

    count = db.conn.execute(
        "SELECT COUNT(*) FROM job_history WHERE workspace_id = ?", (ws_id,)
    ).fetchone()[0]
    assert count <= 100


def test_progress_events_include_steps(tmp_path):
    """Progress events include the steps array when steps are defined."""
    import time

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

    for _ in range(50):
        j = runner.get(job_id)
        if j and j["status"] != "running":
            break
        time.sleep(0.1)

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
