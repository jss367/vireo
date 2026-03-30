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
