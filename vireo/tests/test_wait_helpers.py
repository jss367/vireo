"""The Windows CI budget changes only bounded synchronization waits."""

import pytest
import testing.waits as waits
import wait as job_wait


@pytest.mark.parametrize(
    "platform,ci,seconds,expected",
    [
        ("win32", "true", 2, 30),
        ("win32", "true", 20, 60),
        ("win32", "true", 0.1, 0.1),
        ("win32", "true", 0, 0),
        ("win32", "false", 2, 2),
        ("win32", "", 2, 2),
        ("darwin", "true", 2, 2),
        ("linux", "true", 2, 2),
    ],
)
def test_synchronization_budget(monkeypatch, platform, ci, seconds, expected):
    monkeypatch.setattr(waits.sys, "platform", platform)
    monkeypatch.setenv("CI", ci)
    assert waits.synchronization_timeout(seconds) == expected


def test_job_wait_returns_immediately_when_done(monkeypatch):
    monkeypatch.setattr(waits.sys, "platform", "win32")
    monkeypatch.setenv("CI", "true")
    monkeypatch.setattr(job_wait.time, "sleep", lambda _: pytest.fail("slept after completion"))
    job = {"status": "completed"}
    assert job_wait.wait_for_job(lambda: job, timeout=2) is job


def test_slow_windows_job_can_complete_after_original_budget(monkeypatch):
    monkeypatch.setattr(waits.sys, "platform", "win32")
    monkeypatch.setenv("CI", "true")
    clock = iter([0, 3])
    monkeypatch.setattr(job_wait.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(job_wait.time, "sleep", lambda _: None)
    states = iter([{"status": "running"}, {"status": "completed"}])
    assert job_wait.wait_for_job(lambda: next(states), timeout=2)["status"] == "completed"


def test_job_wait_remains_bounded_and_reports_last_state(monkeypatch):
    monkeypatch.setattr(waits.sys, "platform", "win32")
    monkeypatch.setenv("CI", "true")
    clock = iter([0, 29, 30])
    monkeypatch.setattr(job_wait.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(job_wait.time, "sleep", lambda _: None)
    with pytest.raises(pytest.fail.Exception, match="within 30.0s; last=.*running"):
        job_wait.wait_for_job(lambda: {"status": "running"}, timeout=2)
