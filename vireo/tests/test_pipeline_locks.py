# vireo/tests/test_pipeline_locks.py
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.dirname(__file__))

from pipeline_locks import (
    _GPU_SEMAPHORE,
    acquire_gpu,
    acquire_gpu_if_session_uses_it,
    acquire_photo_mask,
    acquire_workspace_regroup,
)


class _FakeSession:
    def __init__(self, providers):
        self._providers = list(providers)

    def get_providers(self):
        return list(self._providers)


def test_acquire_gpu_if_session_uses_it_takes_lock_for_cuda_session():
    sess = _FakeSession(["CUDAExecutionProvider", "CPUExecutionProvider"])
    before = _GPU_SEMAPHORE._value
    with acquire_gpu_if_session_uses_it(sess):
        held = _GPU_SEMAPHORE._value
    after = _GPU_SEMAPHORE._value
    assert before == 1
    assert held == 0, "semaphore should be acquired for GPU sessions"
    assert after == 1


def test_acquire_gpu_if_session_uses_it_takes_lock_for_coreml_session():
    sess = _FakeSession(["CoreMLExecutionProvider", "CPUExecutionProvider"])
    with acquire_gpu_if_session_uses_it(sess):
        assert _GPU_SEMAPHORE._value == 0


def test_acquire_gpu_if_session_uses_it_skips_lock_for_cpu_only_session():
    sess = _FakeSession(["CPUExecutionProvider"])
    before = _GPU_SEMAPHORE._value
    with acquire_gpu_if_session_uses_it(sess):
        held = _GPU_SEMAPHORE._value
    after = _GPU_SEMAPHORE._value
    assert before == 1
    assert held == 1, "CPU-only session must not take the GPU semaphore"
    assert after == 1


def test_acquire_gpu_if_session_uses_it_defaults_to_lock_when_providers_missing():
    """A session that doesn't expose get_providers (or raises) must
    conservatively take the lock — same behavior as before this check existed.
    """
    class _NoProviders:
        pass

    with acquire_gpu_if_session_uses_it(_NoProviders()):
        assert _GPU_SEMAPHORE._value == 0


def _wait_until(predicate, timeout=1.0, interval=0.005):
    """Poll ``predicate`` until true or timeout; assert on timeout.

    Replaces unbounded ``while not <cond>: time.sleep(...)`` loops that
    would otherwise hang the suite if a thread stalls.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")


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
    _wait_until(lambda: "first-in" in held)
    t2.start()
    t1.join(timeout=3.0)
    t2.join(timeout=3.0)
    assert not t1.is_alive(), "first thread did not finish"
    assert not t2.is_alive(), "second thread did not finish"

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
    _wait_until(lambda: "first-in" in held)
    t2.start()
    t1.join(timeout=3.0)
    t2.join(timeout=3.0)
    assert not t1.is_alive(), "first thread did not finish"
    assert not t2.is_alive(), "second thread did not finish"

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
    assert not t2.is_alive(), "second thread should not be blocked by a different workspace"
    assert "second-in" in held, "different workspace must not be blocked"
    let_first_go.set()
    t1.join(timeout=2.0)
    assert not t1.is_alive(), "first thread did not finish"


def test_workspace_regroup_lock_reentrant_keys_share_one_lock():
    """The lock object for a given workspace_id is stable across calls."""
    from pipeline_locks import _workspace_regroup_lock_for_tests
    lock1 = _workspace_regroup_lock_for_tests(7)
    lock2 = _workspace_regroup_lock_for_tests(7)
    assert lock1 is lock2


def test_photo_mask_lock_serialises_same_photo_and_variant():
    """Two threads writing the same (photo, variant) take turns."""
    held = []

    def first():
        with acquire_photo_mask(42, "sam2-small"):
            held.append("first-in")
            time.sleep(0.05)
            held.append("first-out")

    def second():
        with acquire_photo_mask(42, "sam2-small"):
            held.append("second-in")

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    _wait_until(lambda: "first-in" in held)
    t2.start()
    t1.join(timeout=3.0)
    t2.join(timeout=3.0)
    assert not t1.is_alive() and not t2.is_alive()
    assert held == ["first-in", "first-out", "second-in"], held


def test_photo_mask_lock_does_not_block_different_photo():
    """Same variant, different photo IDs don't contend."""
    held = []
    first_holding = threading.Event()
    let_first_go = threading.Event()

    def first():
        with acquire_photo_mask(1, "sam2-small"):
            held.append("first-in")
            first_holding.set()
            let_first_go.wait(timeout=2.0)

    def second():
        with acquire_photo_mask(2, "sam2-small"):
            held.append("second-in")

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    assert first_holding.wait(timeout=1.0)
    t2.start()
    t2.join(timeout=1.0)
    assert not t2.is_alive(), "different photo must not be blocked"
    assert "second-in" in held
    let_first_go.set()
    t1.join(timeout=2.0)


def test_photo_mask_lock_does_not_block_different_variant():
    """Same photo, different variants don't contend — each variant has
    its own deterministic mask path, so they don't conflict.
    """
    held = []
    first_holding = threading.Event()
    let_first_go = threading.Event()

    def first():
        with acquire_photo_mask(42, "sam2-small"):
            held.append("first-in")
            first_holding.set()
            let_first_go.wait(timeout=2.0)

    def second():
        with acquire_photo_mask(42, "sam2-large"):
            held.append("second-in")

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    assert first_holding.wait(timeout=1.0)
    t2.start()
    t2.join(timeout=1.0)
    assert not t2.is_alive(), "different variant must not be blocked"
    assert "second-in" in held
    let_first_go.set()
    t1.join(timeout=2.0)


def test_photo_mask_lock_reentrant_keys_share_one_lock():
    """The lock object for a given (photo_id, variant) is stable."""
    from pipeline_locks import _photo_mask_lock_for_tests
    lock1 = _photo_mask_lock_for_tests(7, "sam2-small")
    lock2 = _photo_mask_lock_for_tests(7, "sam2-small")
    lock3 = _photo_mask_lock_for_tests(7, "sam2-large")
    assert lock1 is lock2
    assert lock1 is not lock3
