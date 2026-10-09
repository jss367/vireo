"""JobRunner holds an idle-sleep assertion while long jobs run.

Issue #1397: a 2h16m import was suspended by idle sleep twelve minutes in
and died when the network share did not survive the sleep/DarkWake cycling.
"""

import os
import sys
import threading

from testing.waits import synchronization_timeout

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.dirname(__file__))

import pytest
from wait import wait_for_job_via_runner


def wait_until(predicate, *, timeout=10.0, poll=0.01, what="condition"):
    """Poll until ``predicate()`` is true.

    A job reaches terminal status inside ``_run_job``'s try/except, but the
    sleep assertion is released in the ``finally`` that runs afterwards. So
    ``wait_for_job_via_runner`` returning does NOT mean cleanup has
    happened — these assertions have to wait for it explicitly rather than
    race it.
    """
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(poll)
    raise AssertionError(f"timed out waiting for {what}")


class RecordingInhibitor:
    def __init__(self):
        self.started = 0
        self.stopped = 0

    def start(self, reason):
        self.started += 1
        return self

    def stop(self):
        self.stopped += 1


@pytest.fixture
def runner_with_recorder(monkeypatch):
    import power
    from jobs import JobRunner

    recorder = RecordingInhibitor()
    monkeypatch.setattr(power, "start_platform_inhibitor", recorder.start)
    runner = JobRunner()
    return runner, recorder


def test_long_job_holds_sleep_assertion_while_running(runner_with_recorder):
    runner, recorder = runner_with_recorder
    observed = {}

    def work(job):
        observed["active_during_work"] = runner.sleep_blocker.active
        return {"ok": True}

    job_id = runner.start("import", work)
    wait_for_job_via_runner(runner, job_id)

    assert observed["active_during_work"] is True, (
        "the assertion must be held while the work function runs, "
        "not merely acquired at some point"
    )
    wait_until(lambda: recorder.stopped == 1, what="inhibitor release")
    assert recorder.started == 1
    assert runner.sleep_blocker.active is False


def test_ephemeral_job_does_not_hold_sleep_assertion(runner_with_recorder):
    """Ephemeral jobs are transient background work surfaced for
    transparency (the new-images walk). They have no business overriding
    the user's power settings."""
    runner, recorder = runner_with_recorder

    job_id = runner.start(
        "new_images_walk", lambda job: {"ok": True}, ephemeral=True,
    )
    wait_for_job_via_runner(runner, job_id)

    assert recorder.started == 0


def test_unknown_job_type_is_protected_by_default(runner_with_recorder):
    """Fail safe. A job type nobody remembered to classify should keep the
    machine awake — over-protecting costs a little battery, under-
    protecting costs a dead multi-hour job."""
    runner, recorder = runner_with_recorder

    job_id = runner.start("some-future-job", lambda job: {"ok": True})
    wait_for_job_via_runner(runner, job_id)

    wait_until(lambda: recorder.stopped == 1, what="inhibitor release")
    assert recorder.started == 1


def test_assertion_released_when_work_raises(runner_with_recorder):
    """A crashing job must not strand the inhibitor — that would keep the
    machine awake indefinitely."""
    runner, recorder = runner_with_recorder

    def boom(job):
        raise RuntimeError("kaboom")

    job_id = runner.start("import", boom)
    job = wait_for_job_via_runner(runner, job_id)

    assert job["status"] == "failed"
    wait_until(lambda: recorder.stopped == 1, what="inhibitor release")
    assert runner.sleep_blocker.active is False


def test_overlapping_long_jobs_share_one_assertion(runner_with_recorder):
    """Two imports at once must not start two inhibitors, and the first to
    finish must not drop the assertion the second still needs."""
    runner, recorder = runner_with_recorder
    first_running = threading.Event()
    let_first_finish = threading.Event()
    second_done = threading.Event()

    def slow(job):
        first_running.set()
        let_first_finish.wait(timeout=synchronization_timeout(5))
        return {"ok": True}

    def quick(job):
        assert runner.sleep_blocker.active is True
        second_done.set()
        return {"ok": True}

    first = runner.start("import", slow)
    assert first_running.wait(timeout=synchronization_timeout(5))

    second = runner.start("scan", quick)
    wait_for_job_via_runner(runner, second)
    assert second_done.is_set()

    # Second job finished; first still running, so the assertion holds.
    assert runner.sleep_blocker.active is True
    assert recorder.started == 1
    assert recorder.stopped == 0

    let_first_finish.set()
    wait_for_job_via_runner(runner, first)

    wait_until(
        lambda: not runner.sleep_blocker.active, what="inhibitor release",
    )
    assert recorder.stopped == 1


def test_jobs_api_reports_keeping_awake(app_and_db, monkeypatch):
    """CORE_PHILOSOPHY: no black boxes. Vireo is overriding a system
    power setting, so it has to say so rather than do it silently.

    Substitute the platform inhibitor so the test asserts on the API's
    reporting rather than on whether the current runner has a working
    systemd (containers, CI's Windows leg, etc. don't) — a real
    ``start_platform_inhibitor`` on those hosts now raises during startup,
    which is the correct best-effort behavior but not what this test is
    about."""
    import power

    class _FakeHandle:
        def stop(self):
            pass

    monkeypatch.setattr(
        power, "start_platform_inhibitor", lambda reason: _FakeHandle(),
    )

    app, _ = app_and_db
    client = app.test_client()

    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert resp.get_json()["keeping_awake"] is False

    app._job_runner.sleep_blocker.acquire()
    try:
        assert client.get("/api/jobs").get_json()["keeping_awake"] is True
    finally:
        app._job_runner.sleep_blocker.release()
