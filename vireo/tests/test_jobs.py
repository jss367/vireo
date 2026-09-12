# vireo/tests/test_jobs.py
import logging
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.dirname(__file__))

from wait import wait_for_job, wait_for_job_via_runner


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


@pytest.mark.parametrize("admission", ["start", "singleton", "pipeline"])
def test_workspace_transfer_reservation_blocks_job_admission(admission):
    from jobs import JobRunner, WorkspaceBusyError

    runner = JobRunner()
    release = threading.Event()
    called = threading.Event()
    transfer, _, _ = runner.start_singleton(
        "send-to-nas", lambda job: release.wait(10), singleton_key="archive",
        workspace_id=1, exclusive_workspace=True,
    )

    def admit(workspace_id):
        if admission == "start":
            return runner.start("export", lambda job: called.set(), workspace_id=workspace_id)
        if admission == "singleton":
            return runner.start_singleton("export", lambda job: called.set(), workspace_id=workspace_id,
                                          singleton_key="export")[0]
        return runner.enqueue_pipeline(lambda job: called.set(), workspace_id=workspace_id)

    try:
        with pytest.raises(WorkspaceBusyError, match="NAS transfer"):
            admit(1)
        assert not called.is_set()
        other = admit(2)
        assert wait_for_job_via_runner(runner, other)["status"] == "completed"
        assert called.is_set()
    finally:
        release.set()
        wait_for_job_via_runner(runner, transfer)
    assert wait_for_job_via_runner(runner, admit(1))["status"] == "completed"
    runner.shutdown()


def test_workspace_transfer_reservation_covers_pipeline_persistence(monkeypatch):
    from jobs import JobRunner, WorkspaceBusyError

    runner = JobRunner()
    entered, release = threading.Event(), threading.Event()
    errors = []

    def persist(*args, **kwargs):
        entered.set()
        assert release.wait(10)
        raise OSError("database unavailable")

    monkeypatch.setattr(runner, "_enqueue_pipeline_admitted", persist)

    def enqueue():
        try:
            runner.enqueue_pipeline(lambda job: None, workspace_id=1)
        except OSError as e:
            errors.append(str(e))

    thread = threading.Thread(target=enqueue)
    thread.start()
    try:
        assert entered.wait(10)
        with pytest.raises(WorkspaceBusyError, match="running jobs"):
            runner.start_singleton("send-to-nas", lambda job: None, singleton_key="archive",
                                   workspace_id=1, exclusive_workspace=True)
    finally:
        release.set()
        thread.join(timeout=10)
    assert errors == ["database unavailable"]
    assert not runner._pipeline_admissions
    transfer, _, _ = runner.start_singleton("send-to-nas", lambda job: None, singleton_key="archive",
                                            workspace_id=1, exclusive_workspace=True)
    assert wait_for_job_via_runner(runner, transfer)["status"] == "completed"
    runner.shutdown()


def test_job_runner_shutdown_cancels_and_joins_workers():
    """Teardown owns worker lifetime and refuses work after it begins."""
    from jobs import JobRunner

    runner = JobRunner()
    started = threading.Event()
    stopped = threading.Event()

    def work(job):
        started.set()
        while not runner.cancellation_requested(job["id"]):
            time.sleep(0.01)
        stopped.set()
        return {}

    job_id = runner.start("test", work)
    assert started.wait(timeout=2)
    with runner._lock:
        runner._schedule_promotion_retry_locked()

    assert runner.shutdown(timeout=2) is True
    assert stopped.is_set()
    assert runner.get(job_id)["status"] == "cancelled"
    assert not runner._worker_threads

    with pytest.raises(RuntimeError, match="shut down"):
        runner.start("late", lambda _job: {})


def test_job_runner_shutdown_joins_after_queued_cancel_failure(monkeypatch):
    """A locked queued row cannot bypass the bounded worker join."""
    from jobs import JobRunner

    runner = JobRunner()
    stopped = threading.Event()

    def work(job):
        while not runner.cancellation_requested(job["id"]):
            time.sleep(0.01)
        stopped.set()

    runner.start("test", work)
    runner._queued_pipelines["queued"] = {}
    observed_timeouts = []

    def fail_cancel(_job_id, *, promote_after_cancel, db_timeout):
        observed_timeouts.append(db_timeout)
        raise RuntimeError("database is locked")

    monkeypatch.setattr(runner, "cancel_job", fail_cancel)

    assert runner.shutdown(timeout=2) is False
    assert stopped.is_set()
    assert observed_timeouts and 0 < observed_timeouts[0] <= 2
    assert not runner._worker_threads
    with runner._lock:
        runner._queued_pipelines.clear()


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


@pytest.mark.parametrize("action", ["resume", "cancel"])
def test_pause_during_final_work_is_honored_before_completion(action):
    from jobs import JobRunner

    runner = JobRunner()
    started = threading.Event()
    release = threading.Event()
    resources_released = threading.Event()

    def work(job):
        try:
            started.set()
            assert release.wait(3)
            return {"items": 1}
        finally:
            resources_released.set()

    job_id = runner.start("test", work, pausable=True)
    try:
        assert started.wait(3)
        assert runner.pause_job(job_id)
        release.set()
        _wait_for_status(runner, job_id, "paused")
        assert resources_released.is_set()
        assert not any(e["type"] == "complete" for e in runner.get_events(job_id))
        assert getattr(runner, f"{action}_job")(job_id)
        job = wait_for_job_via_runner(runner, job_id)
        assert job["status"] == ("completed" if action == "resume" else "cancelled")
        assert job["result"] == {"items": 1}
    finally:
        release.set()
        runner.shutdown(timeout=3)


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

    # The row exists as ``running`` from job start; wait for the final
    # write rather than for the row to appear.
    wait_for_job_via_runner(runner, job_id, wait_for_history=True)
    row = db.conn.execute(
        "SELECT status, error_count, summary FROM job_history WHERE id = ?",
        (job_id,),
    ).fetchone()
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

    # The row exists as ``running`` from job start; wait for the final
    # write rather than for the row to appear.
    wait_for_job_via_runner(runner, job_id, wait_for_history=True)
    row = db.conn.execute(
        "SELECT result FROM job_history WHERE id = ?", (job_id,)
    ).fetchone()
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


def test_start_singleton_returns_existing_when_active():
    """A second start_singleton for the same (type, key) while the first
    is running must NOT create a second worker — that is the whole point
    of the guard."""
    from jobs import JobRunner

    runner = JobRunner()
    release = threading.Event()
    workers = []

    def work(job):
        workers.append(job['id'])
        assert release.wait(timeout=5), "worker never released"
        return {'ok': True}

    first_id, first_joined, first_snap = runner.start_singleton(
        'download-x', work, singleton_key='k',
        config={'note': 'first'},
    )
    assert first_joined is False
    assert first_snap is None

    second_id, second_joined, second_snap = runner.start_singleton(
        'download-x', work, singleton_key='k',
        config={'note': 'second-should-be-ignored'},
    )
    assert second_joined is True
    assert second_id == first_id
    # The snapshot must reflect the ORIGINAL registration's config,
    # not the caller's — otherwise the artifact-identity check the
    # endpoint layers on top of this would see whichever caller ran
    # last and could not detect a mismatched join.
    assert second_snap['config'] == {'note': 'first'}

    release.set()
    wait_for_job_via_runner(runner, first_id)
    assert len(workers) == 1, (
        f"exactly one worker must run, got {len(workers)}"
    )


def test_start_singleton_starts_a_fresh_job_once_the_first_finishes():
    """The singleton is per-active-run, not forever: a completed job
    releases the slot so retry from Settings can start a new download."""
    from jobs import JobRunner

    runner = JobRunner()

    def quick(job):
        return {'ok': True}

    first_id, first_joined, _ = runner.start_singleton(
        'download-x', quick, singleton_key='k',
    )
    assert first_joined is False
    wait_for_job_via_runner(runner, first_id)

    second_id, second_joined, second_snap = runner.start_singleton(
        'download-x', quick, singleton_key='k',
    )
    assert second_joined is False, (
        "a completed job must not pin the singleton — otherwise Retry stays broken"
    )
    assert second_snap is None
    assert second_id != first_id
    wait_for_job_via_runner(runner, second_id)


def test_start_singleton_check_and_start_are_atomic_under_thread_pressure():
    """Fire many concurrent start_singleton calls at once and confirm only
    ONE worker was ever created. This is the specific race the earlier
    list_jobs()+start() pattern had: two callers could both see "no
    existing" and both start a worker."""
    from jobs import JobRunner

    runner = JobRunner()
    release = threading.Event()
    workers = []
    workers_lock = threading.Lock()

    def work(job):
        with workers_lock:
            workers.append(job['id'])
        assert release.wait(timeout=10), "worker never released"
        return {'ok': True}

    ids = []
    ids_lock = threading.Lock()
    barrier = threading.Barrier(16)

    def fire():
        barrier.wait()
        jid, _joined, _snap = runner.start_singleton(
            'download-x', work, singleton_key='k',
        )
        with ids_lock:
            ids.append(jid)

    threads = [threading.Thread(target=fire) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(set(ids)) == 1, (
        f"all callers must converge on one id, got {set(ids)}"
    )
    release.set()
    wait_for_job_via_runner(runner, ids[0])
    assert len(workers) == 1, (
        f"exactly one worker may run for a singleton, got {len(workers)}"
    )


def test_start_singleton_isolates_different_keys():
    """Two singletons with different keys must coexist — the guard is
    per-(type, key), not per-type. Otherwise unrelated singleton flows
    would starve each other."""
    from jobs import JobRunner

    runner = JobRunner()
    release = threading.Event()

    def hold(job):
        assert release.wait(timeout=5)
        return {'ok': True}

    a_id, a_joined, _ = runner.start_singleton(
        'download-x', hold, singleton_key='alpha',
    )
    b_id, b_joined, _ = runner.start_singleton(
        'download-x', hold, singleton_key='beta',
    )
    assert a_joined is False
    assert b_joined is False
    assert a_id != b_id

    release.set()
    wait_for_job_via_runner(runner, a_id)
    wait_for_job_via_runner(runner, b_id)


def test_start_singleton_does_not_match_plain_start_jobs_without_a_key():
    """A regular start() call stores singleton_key=None; a start_singleton
    lookup for the SAME job_type with any real key must NOT match it —
    otherwise the guard would incorrectly join arbitrary jobs of the
    same type that were never opting into singleton behavior."""
    from jobs import JobRunner

    runner = JobRunner()
    release = threading.Event()

    def hold(job):
        assert release.wait(timeout=5)
        return {'ok': True}

    plain_id = runner.start('download-x', hold)
    sing_id, joined, _ = runner.start_singleton(
        'download-x', hold, singleton_key='k',
    )
    assert joined is False, (
        "start_singleton must not join a plain start() job with key=None"
    )
    assert sing_id != plain_id

    release.set()
    wait_for_job_via_runner(runner, plain_id)
    wait_for_job_via_runner(runner, sing_id)


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


def test_job_records_resource_wait_timing_in_snapshot_and_history(tmp_path):
    import resource_ledger
    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    workspace_id = db.ensure_default_workspace()
    db.set_active_workspace(workspace_id)
    runner = JobRunner(db=db)
    ledger = resource_ledger.ResourceLedger(cpu_capacity=1)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    holder = ledger.acquire(resource_ledger.ResourceRequest(
        cpu=resource_ledger.CpuRequest(1, 1, 1),
    ))
    waiting = threading.Event()

    def work(_job):
        with ledger.acquire(
            resource_ledger.ResourceRequest(
                cpu=resource_ledger.CpuRequest(1, 1, 1),
            ),
            on_wait=lambda _request: waiting.set(),
        ):
            return {"ok": True}

    try:
        job_id = runner.start("test", work, workspace_id=workspace_id)
        active_job = wait_for_job(
            lambda: runner.get(job_id) if waiting.is_set() else None,
            terminal=("running",),
            description=f"job {job_id} resource wait",
        )
        assert active_job["resource_wait_count"] == 1
        assert active_job["resource_wait_seconds"] >= 0
        holder.release()
        job = wait_for_job_via_runner(
            runner, job_id, wait_for_history=True,
        )
    finally:
        holder.release()
        resource_ledger._set_resource_ledger_for_tests(previous)

    assert job["resource_wait_count"] == 1
    assert job["resource_wait_seconds"] >= 0
    row = db.conn.execute(
        "SELECT resource_wait_seconds, resource_wait_count "
        "FROM job_history WHERE id = ?",
        (job_id,),
    ).fetchone()
    assert row["resource_wait_count"] == 1
    assert row["resource_wait_seconds"] >= 0


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


def test_discovery_history_does_not_evict_user_jobs(tmp_path):
    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    runner = JobRunner(db=db)
    for job_type in ("pipeline", "new_images_walk", "missing_originals_scan"):
        for i in range(101):
            db.conn.execute(
                """INSERT INTO job_history
                   (id, type, status, started_at, workspace_id)
                   VALUES (?, ?, 'completed', ?, ?)""",
                (f"{job_type}-{i}", job_type, f"2026-01-01T00:00:{i:03d}", ws_id),
            )
    db.conn.execute(
        """INSERT INTO job_history (id, type, status, started_at, workspace_id)
           VALUES ('queued-pipeline', 'pipeline', 'queued', '2025-01-01', ?)""",
        (ws_id,),
    )
    db.conn.commit()
    job_id = runner.start("new_images_walk", lambda _: {}, workspace_id=ws_id)
    wait_for_job_via_runner(runner, job_id, wait_for_history=True)
    counts = dict(db.conn.execute(
        """SELECT type, COUNT(*) FROM job_history
           WHERE workspace_id = ? AND status = 'completed' GROUP BY type""", (ws_id,),
    ).fetchall())
    assert counts["pipeline"] == 100
    assert counts.get("new_images_walk", 0) + counts.get("missing_originals_scan", 0) == 100
    assert db.conn.execute(
        "SELECT status FROM job_history WHERE id = 'queued-pipeline'",
    ).fetchone()[0] == "queued"
    assert db.conn.execute(
        "SELECT id FROM job_history WHERE id = ?", (job_id,),
    ).fetchone() is not None


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


def _history_row(db, job_id):
    return db.conn.execute(
        "SELECT * FROM job_history WHERE id = ?", (job_id,),
    ).fetchone()


def test_running_job_has_history_row_and_checkpoint_records_work(tmp_path):
    """A live job gets a ``running`` row the moment it starts, and
    ``checkpoint_live_jobs`` writes its step tree, progress and elapsed
    time onto that row. This is what the startup sweep has to work with
    after a crash, so it must reflect what the job actually got done.
    """
    import json

    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    runner = JobRunner(db=db)
    started = threading.Event()
    release = threading.Event()

    def work(job):
        runner.set_steps(job["id"], [
            {"id": "scan", "label": "Scan"},
            {"id": "classify", "label": "Classify"},
        ])
        runner.update_step(job["id"], "scan", status="running")
        runner.update_step(
            job["id"], "scan", status="completed", summary="Scanned 120 files",
        )
        runner.update_step(
            job["id"], "classify", status="running",
            progress={"current": 40, "total": 120},
        )
        runner.push_event(job["id"], "progress", {
            "current": 40, "total": 120, "current_file": "IMG_0040.jpg",
        })
        started.set()
        release.wait(timeout=5)
        return {"classified": 120}

    job_id = runner.start("pipeline", work, config={"root": "/photos"})
    assert started.wait(timeout=5)

    row = _history_row(db, job_id)
    assert row is not None and row["status"] == "running", (
        "row must exist from job start, not only at completion"
    )

    assert runner.checkpoint_live_jobs() == 1
    row = _history_row(db, job_id)
    assert row["status"] == "running"
    tree = json.loads(row["tree"])
    assert [s["status"] for s in tree] == ["completed", "running"]
    assert tree[0]["summary"] == "Scanned 120 files"
    assert tree[1]["progress"] == {"current": 40, "total": 120}
    progress = json.loads(row["progress"])
    assert progress["current"] == 40 and progress["total"] == 120
    assert progress["current_file"] == "IMG_0040.jpg"
    assert progress["checkpoint_at"]
    assert row["duration"] is not None and row["duration"] >= 0
    assert row["summary"] == "Scanned 120 files"

    # A checkpointed-but-live row must not leak into history listings.
    assert all(h["id"] != job_id for h in runner.get_history(db, limit=10))

    release.set()
    wait_for_job_via_runner(runner, job_id, wait_for_history=True)
    row = _history_row(db, job_id)
    assert row["status"] == "completed"
    assert json.loads(row["result"]) == {"classified": 120}
    assert row["progress"] is None, "final persist replaces the checkpoint row"

    # A late checkpoint racing the final write must not resurrect the row.
    snapshot = runner.get(job_id)
    snapshot["status"] = "running"
    assert runner._write_checkpoints([snapshot]) == 0
    assert _history_row(db, job_id)["status"] == "completed"
    assert runner.shutdown(timeout=5)


def test_ephemeral_job_is_never_checkpointed(tmp_path):
    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    runner = JobRunner(db=db)
    release = threading.Event()
    started = threading.Event()

    def work(job):
        started.set()
        release.wait(timeout=5)
        return {}

    job_id = runner.start("walk", work, ephemeral=True)
    assert started.wait(timeout=5)
    assert runner.checkpoint_live_jobs() == 0
    assert _history_row(db, job_id) is None
    release.set()
    wait_for_job_via_runner(runner, job_id, wait_for_history=True)
    assert _history_row(db, job_id) is None


def test_checkpoint_thread_runs_on_timer_and_exits_when_idle(tmp_path, monkeypatch):
    import json

    import jobs as jobs_module
    from db import Database
    from jobs import JobRunner

    monkeypatch.setattr(jobs_module, "CHECKPOINT_INTERVAL_SECS", 0.05)
    db = Database(str(tmp_path / "test.db"))
    runner = JobRunner(db=db)
    assert runner._checkpoint_thread is None
    release = threading.Event()

    def work(job):
        runner.push_event(job["id"], "progress", {"current": 7, "total": 9})
        release.wait(timeout=5)
        return {}

    job_id = runner.start("scan", work)
    deadline = time.time() + 5
    while time.time() < deadline:
        row = _history_row(db, job_id)
        # The first tick can land before the worker pushed progress;
        # keep polling until a checkpoint carries the pushed count.
        if row is not None and row["progress"] and (
            json.loads(row["progress"]).get("current") == 7
        ):
            break
        time.sleep(0.02)
    else:
        pytest.fail("checkpoint thread never wrote progress")
    thread = runner._checkpoint_thread
    assert thread is not None and thread.is_alive()

    release.set()
    wait_for_job_via_runner(runner, job_id, wait_for_history=True)
    deadline = time.time() + 5
    while runner._checkpoint_thread is not None and time.time() < deadline:
        time.sleep(0.02)
    assert runner._checkpoint_thread is None, (
        "checkpoint thread should exit once no persisted job is live"
    )


def test_startup_sweep_keeps_checkpointed_work_on_interrupted_rows(tmp_path):
    """Rows the restart orphaned must say what got done, not just that
    they were interrupted: completed steps keep their summaries, the
    running step is failed with the interruption as its error, the
    checkpoint time becomes finished_at / result.last_progress_at, and
    the one-line summary counts the finished work.
    """
    import json

    from db import Database
    from jobs import INTERRUPTED_BY_RESTART, JobRunner

    db_path = str(tmp_path / "test.db")
    first_db = Database(db_path)
    JobRunner(db=first_db)  # ensures schema
    tree = [
        {"id": "scan", "label": "Scan", "status": "completed",
         "summary": "Scanned 120 files", "error": None, "error_count": 0,
         "progress": {"current": 120, "total": 120}},
        {"id": "classify", "label": "Classify", "status": "running",
         "summary": None, "error": None, "error_count": 0,
         "progress": {"current": 40, "total": 120}},
        {"id": "embed", "label": "Embed", "status": "pending",
         "summary": None, "error": None, "error_count": 0,
         "progress": {"current": 0, "total": 0}},
    ]
    first_db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, duration, "
        " error_count, tree, progress, result, workspace_id) "
        "VALUES ('pipeline-crashed', 'pipeline', 'running', "
        " '2026-09-08T10:00:00', 95.5, 0, ?, ?, NULL, 1)",
        (
            json.dumps(tree),
            json.dumps({
                "current": 40, "total": 120, "current_file": "IMG_0040.jpg",
                "checkpoint_at": "2026-09-08T10:01:35",
            }),
        ),
    )
    first_db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, error_count, "
        " progress, workspace_id) "
        "VALUES ('scan-crashed', 'scan', 'running', '2026-09-08T10:00:00', 0, "
        " ?, 1)",
        (json.dumps({"current": 1234, "total": 5000,
                     "checkpoint_at": "2026-09-08T10:02:00"}),),
    )
    first_db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, error_count, "
        " workspace_id) "
        "VALUES ('scan-blind', 'scan', 'running', '2026-09-08T10:00:00', 0, 1)"
    )
    first_db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at, error_count, "
        " workspace_id) "
        "VALUES ('pipeline-queued', 'pipeline', 'queued', "
        " '2026-09-08T10:00:00', 0, 1)"
    )
    first_db.conn.commit()
    first_db.close()

    db = Database(db_path)
    try:
        runner = JobRunner(db=db)
        rows = {
            r["id"]: dict(r) for r in db.conn.execute(
                "SELECT * FROM job_history"
            )
        }
        assert {r["status"] for r in rows.values()} == {"failed"}

        crashed = rows["pipeline-crashed"]
        result = json.loads(crashed["result"])
        assert result["error"] == INTERRUPTED_BY_RESTART
        assert result["interrupted"] is True
        assert result["last_progress_at"] == "2026-09-08T10:01:35"
        assert crashed["finished_at"] == "2026-09-08T10:01:35"
        assert crashed["duration"] == 95.5
        assert crashed["error_count"] == 1
        swept_tree = json.loads(crashed["tree"])
        assert swept_tree[0]["status"] == "completed"
        assert swept_tree[0]["summary"] == "Scanned 120 files"
        assert swept_tree[1]["status"] == "failed"
        assert swept_tree[1]["error"] == INTERRUPTED_BY_RESTART
        assert swept_tree[1]["progress"] == {"current": 40, "total": 120}
        assert swept_tree[2]["status"] == "pending"
        assert crashed["summary"] == (
            f"{INTERRUPTED_BY_RESTART} after 1 of 3 steps"
        )
        progress = json.loads(crashed["progress"])
        assert progress["current"] == 40
        assert "checkpoint_at" not in progress

        scan = rows["scan-crashed"]
        assert scan["summary"] == f"{INTERRUPTED_BY_RESTART} at 1,234 of 5,000"
        assert scan["finished_at"] == "2026-09-08T10:02:00"

        blind = rows["scan-blind"]
        assert blind["summary"] == INTERRUPTED_BY_RESTART
        assert json.loads(blind["result"])["last_progress_at"] is None
        assert blind["finished_at"] > "2026-09-08T10:02:00"

        queued = rows["pipeline-queued"]
        assert queued["summary"] == f"{INTERRUPTED_BY_RESTART} before it started"
        assert queued["tree"] is None

        # The history API hands the parsed progress and tree to the page.
        db.set_active_workspace(1)
        history = {h["id"]: h for h in runner.get_history(db, limit=10)}
        assert history["pipeline-crashed"]["progress"]["current"] == 40
        assert history["pipeline-crashed"]["tree"][0]["status"] == "completed"
        assert history["pipeline-crashed"]["result"]["interrupted"] is True
    finally:
        db.close()
