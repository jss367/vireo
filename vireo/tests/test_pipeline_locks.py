# vireo/tests/test_pipeline_locks.py
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.dirname(__file__))

from pipeline_locks import (
    acquire_gpu,
    acquire_workspace_regroup,
)


def test_gpu_lock_serialises_two_threads():
    """Only one thread holds the GPU lock at a time."""
    held = []
    release_first = threading.Event()
    second_started = threading.Event()

    def first():
        with acquire_gpu():
            held.append("first-in")
            second_started.wait(timeout=2.0)
            time.sleep(0.05)  # ensure second is blocked, not racing
            held.append("first-out")
        # released
        time.sleep(0.05)

    def second():
        second_started.set()
        with acquire_gpu():
            held.append("second-in")

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    # wait for first to actually grab the lock
    while "first-in" not in held:
        time.sleep(0.005)
    t2.start()
    t1.join(timeout=3.0)
    t2.join(timeout=3.0)

    assert held == ["first-in", "first-out", "second-in"], (
        f"second must wait for first to release; got {held}"
    )


def test_gpu_lock_released_after_with_block():
    """Sequential `with acquire_gpu()` calls don't deadlock — release works."""
    for _ in range(3):
        with acquire_gpu():
            pass
    # If release was broken, the second iteration would deadlock and the
    # test timeout would fire. Reaching here is the assertion.


def test_workspace_regroup_lock_serialises_same_workspace():
    """Two threads regrouping the same workspace take turns."""
    held = []
    second_started = threading.Event()

    def first():
        with acquire_workspace_regroup(42):
            held.append("first-in")
            second_started.wait(timeout=2.0)
            time.sleep(0.05)
            held.append("first-out")

    def second():
        second_started.set()
        with acquire_workspace_regroup(42):
            held.append("second-in")

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    while "first-in" not in held:
        time.sleep(0.005)
    t2.start()
    t1.join(timeout=3.0)
    t2.join(timeout=3.0)

    assert held == ["first-in", "first-out", "second-in"], (
        f"second on same workspace must wait; got {held}"
    )


def test_workspace_regroup_lock_does_not_block_other_workspaces():
    """Different workspace IDs use independent locks; second runs immediately."""
    held = []
    first_holding = threading.Event()
    let_first_go = threading.Event()

    def first():
        with acquire_workspace_regroup(1):
            held.append("first-in")
            first_holding.set()
            let_first_go.wait(timeout=2.0)
            held.append("first-out")

    def second():
        with acquire_workspace_regroup(2):
            held.append("second-in")

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    assert first_holding.wait(timeout=1.0)
    t2.start()
    t2.join(timeout=1.0)
    assert "second-in" in held, "different workspace must not be blocked"
    let_first_go.set()
    t1.join(timeout=2.0)


def test_workspace_regroup_lock_reentrant_keys_share_one_lock():
    """The lock object for a given workspace_id is stable across calls."""
    from pipeline_locks import _workspace_regroup_lock_for_tests
    lock1 = _workspace_regroup_lock_for_tests(7)
    lock2 = _workspace_regroup_lock_for_tests(7)
    assert lock1 is lock2
