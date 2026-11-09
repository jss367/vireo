"""Path guard repairs process-wide leaks before pytest can report a failure."""

import importlib.util
import os
from pathlib import Path

import pytest


@pytest.fixture
def guard():
    spec = importlib.util.spec_from_file_location("host_path_guard_probe", Path(__file__).parents[1] / "conftest.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("function", ["join", "exists", "abspath"])
def test_guard_restores_failing_path_function_before_reporting(guard, monkeypatch, function):
    real = getattr(os.path, function)
    hook = guard.pytest_runtest_call(None)
    next(hook)

    def broken(*args, **kwargs):
        raise AssertionError("leaked path function")

    with monkeypatch.context() as patch:
        patch.setattr(os.path, function, broken)
        with pytest.raises(pytest.fail.Exception, match="Path handling was still broken"):
            hook.send(None)
        assert getattr(os.path, function) is real


def test_guard_preserves_original_failure_after_repair(guard, monkeypatch):
    real = os.path.join
    hook = guard.pytest_runtest_call(None)
    next(hook)

    def broken(*args, **kwargs):
        raise AssertionError("leaked join")

    original = ValueError("test body's original failure")
    with monkeypatch.context() as patch:
        patch.setattr(os.path, "join", broken)
        with pytest.raises(ValueError) as caught:
            hook.throw(original)
        assert caught.value is original
        assert os.path.join is real


def test_guard_keeps_working_delegating_wrapper(guard, monkeypatch):
    real = os.path.join
    hook = guard.pytest_runtest_call(None)
    next(hook)

    def wrapped(*args):
        return real(*args)

    with monkeypatch.context() as patch:
        patch.setattr(os.path, "join", wrapped)
        with pytest.raises(StopIteration):
            hook.send(None)
        assert os.path.join is wrapped
