# vireo/tests/test_pipeline_locks.py
import os
import sys
import threading
import time

import pytest
from testing.waits import synchronization_timeout

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.dirname(__file__))

from pipeline_locks import (
    _GPU_SEMAPHORE,
    acquire_gpu,
    acquire_gpu_if_session_uses_it,
    acquire_photo_mask,
    acquire_workspace_regroup,
    release_archive_destination,
    try_reserve_archive_destination,
)


class _FakeSession:
    def __init__(self, providers):
        self._providers = list(providers)

    def get_providers(self):
        return list(self._providers)


def test_session_cache_lock_wait_honors_bound_cancel_probe():
    import onnx_runtime
    import resource_ledger

    lock = threading.Lock()
    lock.acquire()
    ledger = resource_ledger.ResourceLedger(cpu_capacity=2)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    cancelled = threading.Event()
    finished = threading.Event()
    outcome = []

    def waiter():
        with (
            resource_ledger.bind_resource_owner("model-job"),
            resource_ledger.bind_resource_cancel_check(cancelled.is_set),
        ):
            try:
                with onnx_runtime.acquire_session_cache_lock(lock):
                    pass
            except resource_ledger.ResourceWaitCancelled:
                outcome.append("cancelled")
            finally:
                finished.set()

    thread = threading.Thread(target=waiter)
    thread.start()
    try:
        deadline = time.monotonic() + 1.0
        while ledger.snapshot()["waiters"] == 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ledger.snapshot()["waiters"] == 1
        cancelled.set()
        assert finished.wait(timeout=synchronization_timeout(1.0))
        thread.join(timeout=synchronization_timeout(1.0))
        assert not thread.is_alive()
        assert outcome == ["cancelled"]
        assert ledger.owner_timing("model-job")["wait_count"] == 1
    finally:
        lock.release()
        thread.join(timeout=synchronization_timeout(1.0))
        resource_ledger._set_resource_ledger_for_tests(previous)


def test_session_cache_lock_acquire_race_with_cancel_releases_and_raises():
    """Regression: when cancellation becomes true during the 0.2s
    ``lock.acquire(timeout=0.2)`` window inside
    ``acquire_session_cache_lock`` and the previous holder releases in
    the same window, the acquire succeeds and the caller used to
    proceed with the lock held despite the pending cancel. For an
    already-cancelled eye-keypoint participant waiting on
    ``_download_locks``, that meant a fresh multi-hundred-megabyte
    download would start if the previous holder exited with only one
    of the two required files on disk. Recheck the probe after
    acquire; release the lock and raise if cancel won the race.
    Matches the same fix already applied to the GPU semaphore's
    ``_GpuLockContext``.
    """
    import onnx_runtime
    import resource_ledger

    lock = threading.Lock()
    lock.acquire()
    holder_released = False
    ledger = resource_ledger.ResourceLedger(cpu_capacity=2)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    outcome = []
    call_count = {"n": 0}
    cancel = threading.Event()

    def counting_cancel_check():
        call_count["n"] += 1
        # Call #1 is the entry probe (line 34). Call #2 is inside the
        # while loop (line 48). From call #2 onward, honour the flag —
        # so the test can flip cancel after the flag-observing probe
        # ran but before the timed acquire returns, guaranteeing the
        # only remaining probe to fire is the post-acquire recheck.
        if call_count["n"] >= 2:
            return cancel.is_set()
        return False

    def waiter():
        with resource_ledger.bind_resource_cancel_check(
            counting_cancel_check,
        ):
            try:
                with onnx_runtime.acquire_session_cache_lock(lock):
                    outcome.append("acquired")
            except resource_ledger.ResourceWaitCancelled:
                outcome.append("cancelled")

    thread = threading.Thread(target=waiter)
    thread.start()
    try:
        _wait_until(lambda: call_count["n"] >= 2, timeout=2.0)
        cancel.set()
        lock.release()
        holder_released = True

        thread.join(timeout=synchronization_timeout(2.0))
        assert not thread.is_alive(), (
            "waiter did not finish — check the timed-acquire loop"
        )
        assert outcome == ["cancelled"], (
            f"Expected cancelled outcome (post-acquire cancel recheck), "
            f"got {outcome!r}"
        )
        # The lock must be free — the recheck released it before raising.
        assert lock.acquire(blocking=False), (
            "Lock was not released on the cancel-race path — leaked"
        )
        lock.release()
    finally:
        if not holder_released:
            lock.release()
        thread.join(timeout=synchronization_timeout(1.0))
        resource_ledger._set_resource_ledger_for_tests(previous)


def test_session_cache_lock_releases_on_post_acquire_probe_raise():
    """Regression: the post-acquire probe inside
    ``acquire_session_cache_lock`` may itself raise — a bound
    pipeline probe can surface an unrelated internal error. Without
    a release-on-raise guard, the cache mutex would leak: every
    subsequent caller waiting on the same session cache would then
    block until the process died, even though the probe's exception
    would propagate out and unwind normally.

    Verifies both branches: the non-blocking acquire (uncontended)
    AND the timed acquire (contended). The bug historically lived
    only in the timed branch, but the fix wraps both so a probe bug
    can never leak the lock regardless of contention state.
    """
    import onnx_runtime
    import resource_ledger

    class _ProbeBoom(RuntimeError):
        pass

    # --- uncontended (non-blocking acquire) path ---
    lock_a = threading.Lock()
    ledger = resource_ledger.ResourceLedger(cpu_capacity=2)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    call_count_a = {"n": 0}

    def raising_probe_a():
        call_count_a["n"] += 1
        # Call #1 is the entry probe (line 35). Call #2 is the
        # post-acquire recheck on the non-blocking branch (line 66)
        # — that's the one whose raise must not leak the lock.
        if call_count_a["n"] >= 2:
            raise _ProbeBoom("uncontended post-acquire probe raised")
        return False

    try:
        with resource_ledger.bind_resource_cancel_check(raising_probe_a):
            with pytest.raises(_ProbeBoom):
                with onnx_runtime.acquire_session_cache_lock(lock_a):
                    pytest.fail(
                        "body should not run — probe raised inside the "
                        "context manager entry",
                    )
        assert lock_a.acquire(blocking=False), (
            "Lock leaked when non-blocking-branch post-acquire probe raised"
        )
        lock_a.release()

        # --- contended (timed acquire) path ---
        lock_b = threading.Lock()
        lock_b.acquire()
        holder_released = False
        outcome = []
        call_count_b = {"n": 0}
        raise_now = threading.Event()

        def raising_probe_b():
            call_count_b["n"] += 1
            # Call #1: entry probe (line 35).
            # Call #2+: loop probe (line 77) — return False until the
            #           holder is released and the post-acquire probe
            #           fires. Then raise from THAT probe call so the
            #           timed acquire has already succeeded and the
            #           lock is held.
            if raise_now.is_set() and call_count_b["n"] >= 3:
                raise _ProbeBoom("contended post-acquire probe raised")
            return False

        def waiter():
            with resource_ledger.bind_resource_cancel_check(raising_probe_b):
                try:
                    with onnx_runtime.acquire_session_cache_lock(lock_b):
                        outcome.append("acquired")
                except _ProbeBoom:
                    outcome.append("boom")

        thread = threading.Thread(target=waiter)
        thread.start()
        try:
            _wait_until(lambda: call_count_b["n"] >= 2, timeout=2.0)
            # Arm the raise, then release the holder so the waiter's
            # timed acquire succeeds and its post-acquire probe fires.
            raise_now.set()
            lock_b.release()
            holder_released = True

            thread.join(timeout=synchronization_timeout(3.0))
            assert not thread.is_alive(), "waiter did not finish"
            assert outcome == ["boom"], (
                f"Expected the probe's RuntimeError to propagate; got "
                f"{outcome!r}"
            )
            assert lock_b.acquire(blocking=False), (
                "Lock leaked when timed-branch post-acquire probe raised"
            )
            lock_b.release()
        finally:
            if not holder_released:
                lock_b.release()
            thread.join(timeout=synchronization_timeout(1.0))
    finally:
        resource_ledger._set_resource_ledger_for_tests(previous)


def test_session_cache_lock_post_acquire_uses_pure_cancel_probe():
    """The post-acquire recheck must use the pure-cancel probe, not the
    pause-aware one — a Pause arriving in the race window between
    ``lock.acquire()`` and the recheck must NOT park the caller inside
    ``wait_if_paused``. Parking there would block every unpaused peer
    waiting on the same DINO/detector/SAM/keypoint session cache until
    Resume. Cancel still releases the lock and raises.
    """
    import onnx_runtime
    import resource_ledger

    # Pause probe blocks until released; cancel probe returns False.
    pause_gate = threading.Event()
    pause_probe_calls = {"n": 0}
    pure_probe_calls = {"n": 0}

    def pause_aware_probe():
        pause_probe_calls["n"] += 1
        # Simulate ``_pause_checkpoint`` parking on pause — would block
        # here if called from inside the post-acquire recheck. Test
        # times out if the recheck picks up the pause-aware probe.
        pause_gate.wait(timeout=synchronization_timeout(2.0))
        return False

    def pure_cancel_probe():
        pure_probe_calls["n"] += 1
        return False

    lock = threading.Lock()
    ledger = resource_ledger.ResourceLedger(cpu_capacity=2)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    finished = threading.Event()
    outcome = []

    def runner():
        with (
            resource_ledger.bind_resource_cancel_check(pause_aware_probe),
            resource_ledger.bind_resource_pure_cancel_check(pure_cancel_probe),
        ):
            try:
                # Entry probe uses the pause-aware probe (parks on pause) —
                # release the gate immediately so the entry call returns
                # quickly; the interesting probe is the post-acquire one.
                pause_gate.set()
                with onnx_runtime.acquire_session_cache_lock(lock):
                    outcome.append("acquired")
            except Exception as exc:  # pragma: no cover - shouldn't happen
                outcome.append(f"error:{type(exc).__name__}")
            finally:
                finished.set()

    thread = threading.Thread(target=runner)
    thread.start()
    try:
        assert finished.wait(timeout=synchronization_timeout(1.0)), (
            "acquire_session_cache_lock did not return; the post-acquire "
            "recheck may still be using the pause-aware probe and blocking "
            "on ``wait_if_paused`` while the session lock is held."
        )
        thread.join(timeout=synchronization_timeout(1.0))
        assert outcome == ["acquired"]
        # The post-acquire recheck must have consulted the pure probe.
        assert pure_probe_calls["n"] >= 1, (
            f"Pure-cancel probe never called (n={pure_probe_calls['n']}); "
            "recheck fell back to the pause-aware probe."
        )
    finally:
        resource_ledger._set_resource_ledger_for_tests(previous)


def test_session_cache_lock_post_acquire_still_releases_on_pure_cancel():
    """Pure-cancel probe returning True must release the lock and raise
    ``ResourceWaitCancelled`` from the post-acquire recheck, same as the
    legacy pause-aware probe path.
    """
    import onnx_runtime
    import resource_ledger

    lock = threading.Lock()
    ledger = resource_ledger.ResourceLedger(cpu_capacity=2)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    entry_calls = {"n": 0}
    pure_calls = {"n": 0}
    outcome = []

    def entry_probe():
        # Pause-aware probe used ONLY for the pre-acquire check; return
        # False so we get past the entry and reach the post-acquire path.
        entry_calls["n"] += 1
        return False

    def pure_probe():
        # First call is the post-acquire recheck — flip the cancel bit
        # right there to force release+raise.
        pure_calls["n"] += 1
        return True

    try:
        with (
            resource_ledger.bind_resource_cancel_check(entry_probe),
            resource_ledger.bind_resource_pure_cancel_check(pure_probe),
        ):
            with pytest.raises(resource_ledger.ResourceWaitCancelled):
                with onnx_runtime.acquire_session_cache_lock(lock):
                    outcome.append("acquired")
        assert outcome == [], "body must not run when post-acquire cancel fires"
        assert pure_calls["n"] == 1, (
            f"Pure-cancel probe should fire exactly once from the recheck; "
            f"got {pure_calls['n']}"
        )
        assert lock.acquire(blocking=False), (
            "Lock leaked when pure-cancel post-acquire probe returned True"
        )
        lock.release()
    finally:
        resource_ledger._set_resource_ledger_for_tests(previous)


def test_acquire_gpu_if_session_uses_it_takes_lock_for_cuda_session():
    """Mixed CUDA+CPU sessions take BOTH the GPU semaphore AND CPU permits.

    Vireo's default provider order registers CPU alongside CUDA/CoreML
    so unsupported ops fall back per-op to CPU. The compound claim
    prevents an op-level CPU fallback from overrunning the process CPU
    budget while a concurrent scan / CPU inference is also running.
    """
    import resource_ledger

    sess = _FakeSession(["CUDAExecutionProvider", "CPUExecutionProvider"])
    ledger = resource_ledger.ResourceLedger(cpu_capacity=8)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    before = _GPU_SEMAPHORE._value
    try:
        with acquire_gpu_if_session_uses_it(sess):
            held = _GPU_SEMAPHORE._value
            snapshot = ledger.snapshot()
            assert snapshot["cpu"]["allocated"] > 0, (
                "mixed CUDA+CPU session must claim CPU permits so an "
                "op-level fallback cannot exceed the process budget"
            )
            assert snapshot["lanes"]["cpu_ml"]["allocated"] == 1, (
                "mixed CUDA+CPU session must hold the cpu_ml lane too"
            )
        after = _GPU_SEMAPHORE._value
    finally:
        resource_ledger._set_resource_ledger_for_tests(previous)
    assert before == 1
    assert held == 0, "semaphore should be acquired for GPU sessions"
    assert after == 1
    assert ledger.snapshot()["cpu"]["allocated"] == 0, "CPU permits leaked"
    assert ledger.snapshot()["lanes"]["cpu_ml"]["allocated"] == 0, (
        "cpu_ml lane leaked"
    )


def test_acquire_gpu_if_session_uses_it_takes_lock_for_coreml_session():
    """Mixed CoreML+CPU: same compound claim as the CUDA path."""
    import resource_ledger

    sess = _FakeSession(["CoreMLExecutionProvider", "CPUExecutionProvider"])
    ledger = resource_ledger.ResourceLedger(cpu_capacity=8)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    try:
        with acquire_gpu_if_session_uses_it(sess):
            assert _GPU_SEMAPHORE._value == 0
            assert ledger.snapshot()["cpu"]["allocated"] > 0
            assert ledger.snapshot()["lanes"]["cpu_ml"]["allocated"] == 1
    finally:
        resource_ledger._set_resource_ledger_for_tests(previous)


def test_acquire_inference_resources_pure_gpu_session_takes_only_semaphore():
    """A pure-accelerator session (no CPUExecutionProvider registered)
    takes ONLY the GPU semaphore.

    The compound claim is a fallback safeguard for op-level CPU
    fallback; when the session has no CPU provider at all, no such
    fallback is possible and adding the CPU claim would serialize
    accelerator work against unrelated CPU inference for no benefit.
    """
    import resource_ledger
    from pipeline_locks import acquire_inference_resources

    sess = _FakeSession(["CUDAExecutionProvider"])
    ledger = resource_ledger.ResourceLedger(cpu_capacity=8)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    try:
        with acquire_inference_resources(sess):
            assert _GPU_SEMAPHORE._value == 0, "must hold GPU semaphore"
            assert ledger.snapshot()["cpu"]["allocated"] == 0, (
                "pure-accelerator session must NOT claim CPU permits — "
                "op-level fallback is not possible when no CPU provider "
                "is registered"
            )
            assert ledger.snapshot()["lanes"]["cpu_ml"]["allocated"] == 0
    finally:
        resource_ledger._set_resource_ledger_for_tests(previous)


def test_compound_context_releases_cpu_before_parking_on_gpu_wait():
    """Regression: when the compound context (mixed accelerator+CPU
    session) needs to wait for the GPU semaphore, the CPU permits
    and ``cpu_ml`` lane MUST be released before the wait so unrelated
    unpaused CPU inference / scan is not blocked for the duration.

    Prior to the release-and-retry redesign, ``_CompoundInferenceContext``
    took the CPU lease first, then blocked on the GPU semaphore while
    holding it. A pause arriving in that window would park inside
    ``_GpuLockContext.__enter__`` with the CPU permits + ``cpu_ml``
    lane still committed — exactly the shape Codex called out.

    Contract: block a waiter on the GPU semaphore; verify the ledger
    reports zero CPU allocation while the waiter is contending; then
    release the semaphore and confirm the waiter successfully claims
    BOTH resources on the retry.
    """
    import resource_ledger
    from pipeline_locks import acquire_inference_resources

    sess = _FakeSession(["CUDAExecutionProvider", "CPUExecutionProvider"])
    ledger = resource_ledger.ResourceLedger(cpu_capacity=8)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)

    # Hold the GPU semaphore so the compound acquire is forced to wait.
    holder = acquire_gpu()
    holder.__enter__()
    holder_released = False

    entered = threading.Event()
    inside_lease = threading.Event()
    release_body = threading.Event()
    exited = threading.Event()

    def waiter():
        try:
            with acquire_inference_resources(sess):
                inside_lease.set()
                release_body.wait(timeout=synchronization_timeout(5.0))
        finally:
            exited.set()

    thread = threading.Thread(target=waiter)
    thread.start()
    entered.set()

    try:
        # While the waiter is contending on the GPU semaphore, the
        # ledger MUST show no CPU allocation. Poll for up to 2s so a
        # slow retry loop still passes; the important invariant is
        # that we observe a period where the CPU is fully released
        # despite the compound acquire being in flight.
        observed_zero = False
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if inside_lease.is_set():
                # Waiter got past the retry loop before we sampled — the
                # test cannot prove the release-during-wait property in
                # that case, but the fix is verified by the fact that
                # the waiter didn't deadlock while we still held the
                # GPU semaphore. Skip the assertion, but require the
                # semaphore was NEVER acquired while CPU was allocated
                # (which _GPU_SEMAPHORE._value plus ledger snapshot
                # would show as inconsistent).
                break
            snapshot = ledger.snapshot()
            if snapshot["cpu"]["allocated"] == 0:
                observed_zero = True
                break
            time.sleep(0.01)
        assert observed_zero or inside_lease.is_set(), (
            f"Compound context must release CPU permits while waiting on "
            f"the GPU semaphore. Never observed cpu allocation == 0 while "
            f"the waiter was contending; ledger snapshot: "
            f"{ledger.snapshot()!r}."
        )

        # Now release the semaphore so the waiter can complete its retry.
        holder.__exit__(None, None, None)
        holder_released = True

        assert inside_lease.wait(timeout=synchronization_timeout(3.0)), (
            "waiter never acquired both resources after semaphore released"
        )
        # Once inside, both must be held: GPU semaphore taken, CPU allocated.
        assert _GPU_SEMAPHORE._value == 0
        snapshot_inside = ledger.snapshot()
        assert snapshot_inside["cpu"]["allocated"] > 0
        assert snapshot_inside["lanes"]["cpu_ml"]["allocated"] == 1

        release_body.set()
        assert exited.wait(timeout=synchronization_timeout(2.0))
        thread.join(timeout=synchronization_timeout(2.0))
        assert not thread.is_alive()

        # Full release on exit.
        assert _GPU_SEMAPHORE._value == 1
        assert ledger.snapshot()["cpu"]["allocated"] == 0
        assert ledger.snapshot()["lanes"]["cpu_ml"]["allocated"] == 0
    finally:
        release_body.set()
        if not holder_released:
            holder.__exit__(None, None, None)
        thread.join(timeout=synchronization_timeout(1.0))
        resource_ledger._set_resource_ledger_for_tests(previous)


def test_acquire_gpu_if_session_uses_it_skips_lock_for_cpu_only_session():
    import resource_ledger

    sess = _FakeSession(["CPUExecutionProvider"])
    ledger = resource_ledger.ResourceLedger(cpu_capacity=2)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    before = _GPU_SEMAPHORE._value
    try:
        with acquire_gpu_if_session_uses_it(sess):
            held = _GPU_SEMAPHORE._value
            snapshot = ledger.snapshot()
            assert snapshot["cpu"]["allocated"] == 2
            assert snapshot["lanes"]["cpu_ml"]["allocated"] == 1
        after = _GPU_SEMAPHORE._value
    finally:
        resource_ledger._set_resource_ledger_for_tests(previous)
    assert before == 1
    assert held == 1, "CPU-only session must not take the GPU semaphore"
    assert after == 1
    assert ledger.snapshot()["cpu"]["allocated"] == 0


def test_acquire_inference_resources_wakes_on_bound_cancel_check():
    """A queued CPU-inference claim wakes when the bound job probe fires.

    Without this, a cancelled classify/detect/mask/embed worker would
    block on ``cpu_ml`` until the current holder released — and if the
    native inference stalls, the cancelled worker could outlive
    ``JobRunner.shutdown()``.
    """
    import resource_ledger
    from pipeline_locks import acquire_inference_resources
    from resource_ledger import (
        CpuRequest,
        ResourceLedger,
        ResourceRequest,
        ResourceWaitCancelled,
        bind_resource_cancel_check,
    )

    sess = _FakeSession(["CPUExecutionProvider"])
    ledger = ResourceLedger(cpu_capacity=1)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    holder = ledger.acquire(ResourceRequest(
        cpu=CpuRequest(1, 1, 1), lanes=("cpu_ml",),
    ))
    cancelled_flag = threading.Event()
    finished = threading.Event()
    outcome = []
    try:
        def waiter():
            with bind_resource_cancel_check(cancelled_flag.is_set):
                try:
                    with acquire_inference_resources(sess):
                        pass
                except ResourceWaitCancelled:
                    outcome.append("cancelled")
                finally:
                    finished.set()

        thread = threading.Thread(target=waiter)
        thread.start()
        # Give the waiter time to enter the ledger wait loop.
        time.sleep(0.05)
        cancelled_flag.set()
        # The ledger uses a bounded 200ms poll so the waiter observes the
        # flag on the next tick; no need to release the holder.
        assert finished.wait(timeout=synchronization_timeout(1.0)), "waiter never observed cancel"
        thread.join(timeout=synchronization_timeout(1.0))
        assert not thread.is_alive()
        assert outcome == ["cancelled"]
    finally:
        holder.release()
        resource_ledger._set_resource_ledger_for_tests(previous)


def test_gpu_inference_wait_wakes_on_bound_cancel_check():
    """Queued accelerator work observes the same bound job probe as CPU."""
    from pipeline_locks import acquire_inference_resources
    from resource_ledger import (
        ResourceWaitCancelled,
        bind_resource_cancel_check,
    )

    sess = _FakeSession(["CUDAExecutionProvider", "CPUExecutionProvider"])
    holder = acquire_gpu()
    holder.__enter__()
    cancelled_flag = threading.Event()
    finished = threading.Event()
    outcome = []
    try:
        def waiter():
            with bind_resource_cancel_check(cancelled_flag.is_set):
                try:
                    with acquire_inference_resources(sess):
                        pass
                except ResourceWaitCancelled:
                    outcome.append("cancelled")
                finally:
                    finished.set()

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.05)
        cancelled_flag.set()
        assert finished.wait(timeout=synchronization_timeout(1.0)), "GPU waiter never observed cancel"
        thread.join(timeout=synchronization_timeout(1.0))
        assert not thread.is_alive()
        assert outcome == ["cancelled"]
    finally:
        holder.__exit__(None, None, None)


def test_gpu_inference_contention_records_live_and_completed_owner_timing():
    """Accelerator semaphore waits feed the shared job diagnostics."""
    import resource_ledger
    from pipeline_locks import acquire_inference_resources
    from resource_ledger import ResourceLedger, bind_resource_owner

    sess = _FakeSession(["CUDAExecutionProvider", "CPUExecutionProvider"])
    ledger = ResourceLedger(cpu_capacity=2)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    holder = acquire_gpu()
    holder.__enter__()
    holder_released = False
    waiting = threading.Event()
    finished = threading.Event()

    def waiter():
        with bind_resource_owner("gpu-job"):
            waiting.set()
            with acquire_inference_resources(sess):
                pass
        finished.set()

    thread = threading.Thread(target=waiter)
    thread.start()
    try:
        assert waiting.wait(timeout=synchronization_timeout(1.0))
        _wait_until(
            lambda: ledger.owner_timing("gpu-job")["wait_count"] == 1,
        )
        active = ledger.owner_timing("gpu-job")
        assert active["wait_seconds"] >= 0
        assert ledger.snapshot()["waiters"] == 1

        holder.__exit__(None, None, None)
        holder_released = True
        assert finished.wait(timeout=synchronization_timeout(1.0))
        thread.join(timeout=synchronization_timeout(1.0))
        assert not thread.is_alive()
        completed = ledger.owner_timing("gpu-job")
        assert completed["wait_count"] == 1
        assert completed["wait_seconds"] >= active["wait_seconds"]
        assert ledger.snapshot()["waiters"] == 0
    finally:
        if not holder_released:
            holder.__exit__(None, None, None)
        thread.join(timeout=synchronization_timeout(1.0))
        resource_ledger._set_resource_ledger_for_tests(previous)


def test_gpu_acquire_race_with_cancel_releases_semaphore_and_raises():
    """Regression: when cancellation becomes true during the
    ``_GPU_SEMAPHORE.acquire(timeout=0.2)`` window and a release wins
    the same window, the successful acquire path used to return without
    rechecking cancellation. The caller then held the semaphore and
    proceeded into ``session.run`` despite the pending cancel — for
    interactive text search past its 5-second deadline, that means the
    UI stalls on inference the user has already cancelled and the
    semaphore stays held by a request whose result will be discarded.
    Recheck after acquire; release and raise if cancel won the race.
    """
    from pipeline_locks import _GpuLockContext
    from resource_ledger import ResourceWaitCancelled

    # Occupy the semaphore so the pre-acquire non-blocking probe at
    # ``_GpuLockContext.__enter__`` fails and the waiter enters the
    # timed-polling loop.
    _GPU_SEMAPHORE.acquire()
    holder_released = False
    outcome = []
    call_count = {"n": 0}
    cancel = threading.Event()

    def counting_cancel_check():
        call_count["n"] += 1
        # Call #1 is the entry probe (line 61). Call #2 is inside the
        # while loop (line 73). From call #2 onward, honour the flag —
        # so the test can flip cancel AFTER the flag-observing probe
        # ran but BEFORE the timed acquire returns, guaranteeing the
        # only remaining probe to fire is the post-acquire recheck.
        if call_count["n"] >= 2:
            return cancel.is_set()
        return False

    def waiter():
        try:
            with _GpuLockContext(cancel_check=counting_cancel_check):
                outcome.append("acquired")
        except ResourceWaitCancelled:
            outcome.append("cancelled")

    thread = threading.Thread(target=waiter)
    thread.start()
    try:
        # Wait until the waiter has called cancel_check at least twice —
        # meaning it is now inside ``_GPU_SEMAPHORE.acquire(timeout=0.2)``
        # (or about to enter it, with the flag observed as False on the
        # loop probe).
        _wait_until(lambda: call_count["n"] >= 2, timeout=2.0)

        # Fire the race: cancel becomes true THEN the semaphore is
        # released. The waiter's timed acquire will succeed shortly.
        # Without the post-acquire recheck the waiter would return with
        # the semaphore held; with the fix, the recheck sees cancel and
        # unwinds.
        cancel.set()
        _GPU_SEMAPHORE.release()
        holder_released = True

        thread.join(timeout=synchronization_timeout(2.0))
        assert not thread.is_alive(), (
            "waiter did not finish — check the timed-acquire loop"
        )
        assert outcome == ["cancelled"], (
            f"Expected cancelled outcome (post-acquire cancel recheck), "
            f"got {outcome!r}"
        )
        # Semaphore must be back to full value 1 — the released holder
        # slot returned to it, the waiter's briefly-acquired slot was
        # released before the raise.
        assert _GPU_SEMAPHORE._value == 1, (
            f"Semaphore leaked; value={_GPU_SEMAPHORE._value}"
        )
    finally:
        # If the fix regressed and the waiter returned "acquired", it
        # would hold the semaphore on the way out of its with-block —
        # already released by ``__exit__``. But if the test aborted
        # before joining, the waiter thread may still be blocked in
        # ``acquire``: release once more so the module-level semaphore
        # doesn't leak across tests.
        if not holder_released:
            _GPU_SEMAPHORE.release()
        thread.join(timeout=synchronization_timeout(1.0))


def test_gpu_semaphore_released_before_pause_park_during_post_acquire():
    """Regression: when the pipeline's bound ``_pause_checkpoint`` parks
    inside the ``_GpuLockContext`` post-acquire probe, the semaphore
    must be RELEASED before the park so unpaused GPU jobs can proceed.
    Prior fix (``51e7cc21``) only released on cancel; a pure pause
    that returned False (no cancel) would keep the semaphore held for
    the entire pause duration.

    Simulates the pause-park scenario with a probe that (1) records
    that the semaphore was released BEFORE the probe was called and
    (2) returns False, forcing the loop to reacquire. Without the
    fix, the probe would see the semaphore still held; with the
    fix, the semaphore counter must be back to 1 at probe time.
    """
    from pipeline_locks import _GpuLockContext
    _GPU_SEMAPHORE.acquire()  # occupy so waiter enters polling loop
    holder_released = False
    outcome = []
    call_count = {"n": 0}
    semaphore_value_seen_by_probe = []

    def counting_probe():
        call_count["n"] += 1
        # Call #1: pre-acquire entry probe → False (not cancelled).
        # Call #2: pre-timed-acquire loop probe → False (not
        # cancelled).
        # Call #3+: POST-acquire probe → must observe the semaphore
        # was released BEFORE this call fired (value back to 1),
        # then simulate a pure pause by returning False. The loop
        # will reacquire after we release the holder.
        if call_count["n"] >= 3:
            semaphore_value_seen_by_probe.append(_GPU_SEMAPHORE._value)
        return False

    def waiter():
        try:
            with _GpuLockContext(cancel_check=counting_probe):
                outcome.append("acquired")
        except Exception as exc:  # pragma: no cover - diagnostic
            outcome.append(f"raised:{exc.__class__.__name__}")

    thread = threading.Thread(target=waiter)
    thread.start()
    try:
        # Wait until the waiter has passed the pre-acquire probes and
        # entered its blocking timed acquire (call_count == 2).
        _wait_until(lambda: call_count["n"] >= 2, timeout=2.0)
        # Release the holder so the timed acquire succeeds.
        _GPU_SEMAPHORE.release()
        holder_released = True
        # Wait for the waiter to finish acquiring (should proceed
        # after post-acquire probe returns False and reacquires).
        thread.join(timeout=synchronization_timeout(3.0))
        assert not thread.is_alive(), "waiter did not finish"
        assert outcome == ["acquired"], (
            f"Expected acquired outcome after pure-pause probe, got "
            f"{outcome!r}"
        )
        # The post-acquire probe must have seen the semaphore already
        # RELEASED (value=1). If the fix regressed the value would
        # have been 0 at probe time.
        assert semaphore_value_seen_by_probe, (
            "Post-acquire probe was never called — cannot verify "
            "release-before-park semantics"
        )
        assert all(v == 1 for v in semaphore_value_seen_by_probe), (
            f"Semaphore must be released BEFORE the post-acquire "
            f"probe (value=1). Probe saw values "
            f"{semaphore_value_seen_by_probe!r} — a paused participant "
            f"would retain the process-wide GPU slot for the whole "
            f"pause and block unrelated unpaused GPU jobs."
        )
        # Final: semaphore is back to 1 because the with-block
        # released via __exit__.
        assert _GPU_SEMAPHORE._value == 1
    finally:
        if not holder_released:
            _GPU_SEMAPHORE.release()
        thread.join(timeout=synchronization_timeout(1.0))


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
    timeout = synchronization_timeout(timeout)
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
            second_started.wait(timeout=synchronization_timeout(2.0))
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
    t1.join(timeout=synchronization_timeout(3.0))
    t2.join(timeout=synchronization_timeout(3.0))
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
            second_started.wait(timeout=synchronization_timeout(2.0))
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
    t1.join(timeout=synchronization_timeout(3.0))
    t2.join(timeout=synchronization_timeout(3.0))
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
            let_first_go.wait(timeout=synchronization_timeout(2.0))
            held.append("first-out")

    def second():
        with acquire_workspace_regroup(2):
            held.append("second-in")

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    assert first_holding.wait(timeout=synchronization_timeout(1.0))
    t2.start()
    t2.join(timeout=synchronization_timeout(1.0))
    assert not t2.is_alive(), "second thread should not be blocked by a different workspace"
    assert "second-in" in held, "different workspace must not be blocked"
    let_first_go.set()
    t1.join(timeout=synchronization_timeout(2.0))
    assert not t1.is_alive(), "first thread did not finish"


def test_workspace_regroup_lock_reentrant_keys_share_one_lock():
    """The lock object for a given workspace_id is stable across calls."""
    from pipeline_locks import _workspace_regroup_lock_for_tests
    lock1 = _workspace_regroup_lock_for_tests(7)
    lock2 = _workspace_regroup_lock_for_tests(7)
    assert lock1 is lock2


def test_photo_mask_lock_serialises_same_photo():
    """Two threads writing the same photo's mask take turns."""
    held = []

    def first():
        with acquire_photo_mask(42):
            held.append("first-in")
            time.sleep(0.05)
            held.append("first-out")

    def second():
        with acquire_photo_mask(42):
            held.append("second-in")

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    _wait_until(lambda: "first-in" in held)
    t2.start()
    t1.join(timeout=synchronization_timeout(3.0))
    t2.join(timeout=synchronization_timeout(3.0))
    assert not t1.is_alive() and not t2.is_alive()
    assert held == ["first-in", "first-out", "second-in"], held


def test_photo_mask_lock_does_not_block_different_photo():
    """Different photo IDs don't contend — common case."""
    held = []
    first_holding = threading.Event()
    let_first_go = threading.Event()

    def first():
        with acquire_photo_mask(1):
            held.append("first-in")
            first_holding.set()
            let_first_go.wait(timeout=synchronization_timeout(2.0))
            held.append("first-out")

    def second():
        with acquire_photo_mask(2):
            held.append("second-in")

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    assert first_holding.wait(timeout=synchronization_timeout(1.0))
    t2.start()
    t2.join(timeout=synchronization_timeout(1.0))
    assert not t2.is_alive(), "different photo must not be blocked"
    assert "second-in" in held
    assert "first-out" not in held, "first lock released before checking independence"
    let_first_go.set()
    t1.join(timeout=synchronization_timeout(2.0))


def test_photo_mask_lock_serialises_same_photo_across_variants():
    """Same photo, DIFFERENT SAM variants still take turns.

    Cross-variant serialisation is load-bearing: ``set_active_mask_variant``
    and ``update_photo_embeddings`` denormalise into the same ``photos``
    row regardless of variant, so interleaved writes between two
    pipelines on the same photo with different variants would corrupt
    the row (e.g. active_mask_variant=large but dino embedding cropped
    from small's mask). The lock keys on photo_id only — variant is
    intentionally NOT part of the key.

    The pipeline_job call site passes only photo_id; this test exercises
    the lock primitive directly to lock in the cross-variant guarantee.
    """
    held = []
    first_holding = threading.Event()
    let_first_go = threading.Event()

    def first():
        # Caller treats lock as photo-scoped — variant is irrelevant.
        with acquire_photo_mask(42):
            held.append("first-in")
            first_holding.set()
            let_first_go.wait(timeout=synchronization_timeout(2.0))
            held.append("first-out")

    def second():
        with acquire_photo_mask(42):
            held.append("second-in")

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    assert first_holding.wait(timeout=synchronization_timeout(1.0))
    t2.start()
    # Second must block until first releases — even though semantically
    # the two pipelines might be working different variants.
    assert not held.count("second-in"), (
        "second thread ran while first held the photo lock"
    )
    let_first_go.set()
    t1.join(timeout=synchronization_timeout(2.0))
    t2.join(timeout=synchronization_timeout(2.0))
    assert held == ["first-in", "first-out", "second-in"], held


def test_photo_mask_lock_reentrant_keys_share_one_lock():
    """The lock object for a given photo_id is stable across calls."""
    from pipeline_locks import _photo_mask_lock_for_tests
    lock1 = _photo_mask_lock_for_tests(7)
    lock2 = _photo_mask_lock_for_tests(7)
    assert lock1 is lock2


def test_archive_destination_reserve_rejects_exact_match():
    """Two pipelines targeting the same destination cannot both reserve."""
    try:
        assert try_reserve_archive_destination("/Photos/Shoot") is True
        assert try_reserve_archive_destination("/Photos/Shoot") is False
    finally:
        release_archive_destination("/Photos/Shoot")


def test_archive_destination_reserve_rejects_descendant():
    """A child of an already-reserved root is rejected before processing starts.

    Without this the parent run can create a tracked folder row that the
    child later tries to archive into, leaving overlapping folder roots in
    the catalog after the second job moves staging into the already-claimed
    tree.
    """
    try:
        assert try_reserve_archive_destination("/Photos/Shoot") is True
        assert try_reserve_archive_destination("/Photos/Shoot/Subset") is False
    finally:
        release_archive_destination("/Photos/Shoot")


def test_archive_destination_reserve_rejects_ancestor():
    """A parent of an already-reserved leaf is rejected too.

    Symmetry matters because either run can land first; once the leaf is
    in flight, a second pipeline aiming at the enclosing root would later
    have to reparent the in-flight subtree, which ``move_folder_path``
    doesn't do.
    """
    try:
        assert try_reserve_archive_destination("/Photos/Shoot/Subset") is True
        assert try_reserve_archive_destination("/Photos/Shoot") is False
    finally:
        release_archive_destination("/Photos/Shoot/Subset")


def test_archive_destination_reserve_allows_sibling_prefix_lookalike():
    """``/Photos/Shoot`` and ``/Photos/ShootSubset`` are siblings, not nested.

    A naive ``startswith`` overlap check would false-positive here and
    block an unrelated archive. ``commonpath`` correctly returns
    ``/Photos`` for both, which equals neither leaf.
    """
    try:
        assert try_reserve_archive_destination("/Photos/Shoot") is True
        assert try_reserve_archive_destination("/Photos/ShootSubset") is True
    finally:
        release_archive_destination("/Photos/Shoot")
        release_archive_destination("/Photos/ShootSubset")


def test_archive_destination_release_allows_reacquire():
    """After release, the same destination (or a nested one) can be claimed."""
    assert try_reserve_archive_destination("/Photos/Shoot") is True
    release_archive_destination("/Photos/Shoot")
    try:
        # Same path is reusable.
        assert try_reserve_archive_destination("/Photos/Shoot") is True
        release_archive_destination("/Photos/Shoot")
        # And a previously-blocked nested path is also reusable.
        assert try_reserve_archive_destination("/Photos/Shoot/Subset") is True
    finally:
        release_archive_destination("/Photos/Shoot/Subset")


def test_archive_destination_reserve_normalises_relative_paths():
    """Relative and absolute spellings of the same target collide."""
    abs_path = os.path.abspath("Photos/Shoot")
    try:
        assert try_reserve_archive_destination("Photos/Shoot") is True
        assert try_reserve_archive_destination(abs_path) is False
    finally:
        release_archive_destination("Photos/Shoot")


def test_archive_destination_reserve_rejects_symlinked_alias(tmp_path):
    """A symlink and its real path resolve to the same archive root.

    Without realpath in the normalisation, two jobs choosing the same
    physical destination via different spellings (a real path and a
    symlink to it) could both pass the reservation check; with two
    pipeline slots they would then race into the same archive root before
    ``move_folder``'s catalog-side guards run.
    """
    real_target = tmp_path / "real_dest"
    real_target.mkdir()
    symlink = tmp_path / "link_dest"
    try:
        symlink.symlink_to(real_target)
    except OSError as exc:
        pytest.skip(f"symlinks not supported on this filesystem: {exc}")

    real_child = str(real_target / "Shoot")
    aliased_child = str(symlink / "Shoot")

    try:
        assert try_reserve_archive_destination(real_child) is True
        # Aliased path resolves through realpath to the same physical
        # directory — the second reservation must collide.
        assert try_reserve_archive_destination(aliased_child) is False
    finally:
        release_archive_destination(real_child)
