"""Selected-source callback regressions with fake locks, timer and history.

No JobRunner startup, worker thread, job execution or catalog is imported.
"""
import __future__

import ast
import copy
from pathlib import Path
from types import SimpleNamespace

import pytest

VIREO = Path(__file__).resolve().parents[1]


def _methods(path, owner):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == owner)
    return {n.name: n for n in cls.body if isinstance(n, ast.FunctionDef)}


def _compile(body, namespace):
    module = ast.fix_missing_locations(ast.Module(body=body, type_ignores=[]))
    exec(compile(module, "<inert checkpoint regression>", "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace


def _runner():
    events = []

    class Lock:
        def __init__(self, name):
            self.name = name

        def __enter__(self):
            events.append(("enter", self.name))

        def __exit__(self, *args):
            events.append(("exit", self.name))

    methods = _methods(VIREO / "job_runner/checkpoints.py", "JobCheckpointer")
    cls = ast.ClassDef(name="Checkpointer", bases=[], keywords=[],
                       body=[copy.deepcopy(n) for n in methods.values()], decorator_list=[])
    ns = _compile([cls], {
        "threading": SimpleNamespace(Event=lambda: SimpleNamespace(), Lock=lambda: Lock("writer")),
        "time": SimpleNamespace(sleep=lambda delay: events.append(("sleep", delay))),
        "snapshot_job": lambda job: dict(job), "_LIVE_STATUSES": ("running", "pausing", "paused"),
        "CHECKPOINT_INTERVAL_SECS": 2.0,
        "log": SimpleNamespace(exception=lambda *args: events.append("logged")),
    })
    runner_methods = _methods(VIREO / "jobs.py", "JobRunner")
    assignment = next(n for n in runner_methods["__init__"].body if isinstance(n, ast.Assign)
                      and any(ast.unparse(t) == "self._checkpoints" for t in n.targets))
    wire = ast.FunctionDef(name="wire", args=ast.arguments(posonlyargs=[], args=[ast.arg(arg="self")],
                           kwonlyargs=[], kw_defaults=[], defaults=[]), body=[copy.deepcopy(assignment)],
                           decorator_list=[])
    shell = ast.ClassDef(name="Runner", bases=[], keywords=[], decorator_list=[],
                        body=[wire, *[copy.deepcopy(runner_methods[n]) for n in
                                      ["checkpoint_live_jobs", "flush_partial_result"]]])
    Runner = _compile([shell], {"JobCheckpointer": ns["Checkpointer"]})["Runner"]
    runner = Runner()
    runner._lock = Lock("runner")
    runner._workers = SimpleNamespace(shutting_down=False)
    partial = {"invented": 1}
    job = {"id": "invented-live", "status": "running", "partial_result": partial}
    runner._jobs = {job["id"]: job}
    runner._history = SimpleNamespace(db_path="/invented/never-opened",
                                     write_checkpoints=lambda rows: events.append(("write", rows)) or len(rows))
    runner.wire()
    return runner, job, events


def test_registered_job_flush_resolves_runner_override_between_attempts():
    runner, job, events = _runner()
    calls, polls = [], []
    runner.checkpoint_live_jobs = lambda: calls.append("first") or 0

    def cancel():
        polls.append(True)
        if len(polls) == 2:
            runner.checkpoint_live_jobs = lambda: calls.append("replacement") or 0
        return len(polls) > 2

    assert runner.flush_partial_result(job, cancel_check=cancel) is False
    assert calls == ["first", "replacement"]
    assert len(polls) == 3
    assert not any(e[0] == "write" for e in events)
    assert "_checkpointed_partial" not in job


@pytest.mark.parametrize("raises", [False, True])
def test_timer_resolves_runner_override_and_exits_without_real_thread(raises):
    runner, _job, events = _runner()
    calls, waits = [], []

    def checkpoint():
        calls.append(True)
        if raises:
            raise ValueError("invented checkpoint failure")
        return 0

    def wait(interval):
        waits.append(interval)
        if len(waits) == 2:
            runner._workers.shutting_down = True

    runner.checkpoint_live_jobs = checkpoint
    runner._checkpoints.wakeup.wait = wait
    runner._checkpoints.thread = object()
    runner._checkpoints._loop()
    assert calls == [True]
    assert waits == [2.0, 2.0]
    assert runner._checkpoints.thread is None
    assert not any(e[0] == "write" for e in events)
    assert ("logged" in events) is raises


def test_default_runner_delegator_reaches_implementation_without_recursion():
    runner, job, events = _runner()
    assert runner.flush_partial_result(job) is True
    assert job["_checkpointed_partial"] is job["partial_result"]
    writes = [e for e in events if e[0] == "write"]
    assert len(writes) == 1
    assert writes[0][1][0]["result"] is job["partial_result"]
    assert events[:2] == [("enter", "writer"), ("enter", "runner")]
    assert not any(e[0] == "sleep" for e in events)


@pytest.mark.parametrize("history,ephemeral", [(False, False), (True, True)])
def test_unpersisted_flush_never_invokes_checkpoint(history, ephemeral):
    runner, job, _events = _runner()
    runner._history.db_path = "/invented/never-opened" if history else None
    job["ephemeral"] = ephemeral

    def forbidden():
        raise AssertionError("checkpoint should not run")

    runner.checkpoint_live_jobs = forbidden
    assert runner.flush_partial_result(job) is True
