# vireo/tests/test_model_cache.py
import logging
import os
import sys
import threading
import time

import pytest
from testing.waits import synchronization_timeout

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.dirname(__file__))

from model_cache import ModelCache


def test_acquire_calls_factory_once_per_miss():
    cache = ModelCache(idle_secs=60)
    calls = []

    def factory():
        calls.append(1)
        return object()

    with cache.acquire("k", factory) as v1:
        assert v1 is not None
    assert len(calls) == 1


def test_concurrent_acquire_reuses_loaded_value():
    cache = ModelCache(idle_secs=60)
    calls = []
    obj = object()

    def factory():
        calls.append(1)
        return obj

    # Two sequential acquires while previous is still held -> factory runs once
    h1 = cache.acquire("k", factory)
    v1 = h1.__enter__()
    h2 = cache.acquire("k", factory)
    v2 = h2.__enter__()
    assert v1 is obj
    assert v2 is obj
    assert len(calls) == 1
    h1.__exit__(None, None, None)
    h2.__exit__(None, None, None)


def test_release_arms_idle_timer_and_evicts():
    cache = ModelCache(idle_secs=0.1)
    loads = []

    def factory():
        loads.append(1)
        return object()

    with cache.acquire("k", factory):
        pass
    # Timer should fire shortly; wait for eviction.
    deadline = time.time() + 2.0
    while time.time() < deadline:
        if not cache._has_entry("k"):
            break
        time.sleep(0.02)
    assert not cache._has_entry("k"), "entry should be evicted after idle window"

    # Re-acquire after eviction triggers a fresh load.
    with cache.acquire("k", factory):
        pass
    assert len(loads) == 2


def test_reacquire_before_idle_cancels_eviction():
    cache = ModelCache(idle_secs=1.0)
    loads = []

    def factory():
        loads.append(1)
        return object()

    with cache.acquire("k", factory):
        pass
    # Immediately re-acquire; idle timer should be cancelled.
    with cache.acquire("k", factory):
        pass
    # Wait past where the first timer would have fired.
    time.sleep(0.2)
    # The factory must NOT have been re-invoked because the second acquire
    # cancelled the pending eviction.
    assert len(loads) == 1


def test_active_refs_prevent_eviction():
    cache = ModelCache(idle_secs=0.05)

    def factory():
        return object()

    h_outer = cache.acquire("k", factory)
    h_outer.__enter__()
    with cache.acquire("k", factory):
        pass  # release inner; refcount still 1
    time.sleep(0.15)
    assert cache._has_entry("k"), "entry must survive while refcount > 0"
    h_outer.__exit__(None, None, None)


def test_factory_exception_does_not_corrupt_cache():
    cache = ModelCache(idle_secs=60)

    def bad_factory():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        with cache.acquire("k", bad_factory):
            pass
    # Failure must not leave a phantom entry or leaked refcount.
    assert not cache._has_entry("k")

    # A second acquire on the same key, with a working factory, must succeed.
    def good_factory():
        return "ok"

    with cache.acquire("k", good_factory) as v:
        assert v == "ok"


def test_independent_keys_do_not_interfere():
    cache = ModelCache(idle_secs=60)
    calls_a = []
    calls_b = []

    with cache.acquire("a", lambda: (calls_a.append(1), "A")[1]):
        with cache.acquire("b", lambda: (calls_b.append(1), "B")[1]) as vb:
            assert vb == "B"
    assert len(calls_a) == 1
    assert len(calls_b) == 1


def test_concurrent_acquire_from_multiple_threads_only_loads_once():
    cache = ModelCache(idle_secs=60)
    load_calls = []
    load_started = threading.Event()
    release_load = threading.Event()

    def slow_factory():
        load_calls.append(1)
        load_started.set()
        release_load.wait(timeout=synchronization_timeout(2.0))
        return "loaded"

    values = []

    def worker():
        with cache.acquire("k", slow_factory) as v:
            values.append(v)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    assert load_started.wait(timeout=synchronization_timeout(1.0))
    # Second worker should now be queued waiting for the first load to finish.
    t2.start()
    time.sleep(0.05)
    release_load.set()
    t1.join(timeout=synchronization_timeout(2.0))
    t2.join(timeout=synchronization_timeout(2.0))
    assert not t1.is_alive(), "t1 did not terminate after join"
    assert not t2.is_alive(), "t2 did not terminate after join"
    assert load_calls == [1], "factory must run exactly once across threads"
    assert values == ["loaded", "loaded"]


def test_contended_cold_load_wait_records_owner_timing():
    """Regression: when two jobs cold-load the same key concurrently,
    the second thread waits on the producer's ``load_lock`` — which can
    take minutes for a BioCLIP / SAM2 / DINOv2 cold construction, and
    even longer if the producer is itself blocked on the ONNX
    construction lease. Prior to this fix that wait bypassed the
    resource ledger entirely: the waiter's ``owner_timing`` still
    reported zero waits even though the job was resource-blocked for
    the whole load, so the per-job live and persisted resource-wait
    diagnostics under-reported reality. This asserts the contended
    wait shows up as an external wait in ``owner_timing``.

    Uncontended fast path is covered by
    ``test_uncontended_load_does_not_record_owner_timing`` — an
    unconditional wrap would also inflate wait_count on every cache
    hit (per-photo detection/masking runs), so the uncontended branch
    must NOT touch the ledger.
    """
    import resource_ledger

    # Deterministic clock injected into the ledger so the recorded
    # elapsed for the contended wait is bounded from below by a real,
    # known interval — independent of OS scheduling. Windows CI has
    # produced spurious wait_seconds=0.0 under this assertion when the
    # test relied on a fixed real-time sleep to guarantee the waiter
    # was blocked long enough for time.monotonic() to advance.
    clock_value = [0.0]

    def fake_clock():
        return clock_value[0]

    ledger = resource_ledger.ResourceLedger(cpu_capacity=2, clock=fake_clock)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    try:
        cache = ModelCache(idle_secs=60)
        load_started = threading.Event()
        release_load = threading.Event()

        def slow_factory():
            load_started.set()
            release_load.wait(timeout=synchronization_timeout(2.0))
            return "loaded"

        producer_done = threading.Event()

        def producer():
            try:
                with cache.acquire("k", slow_factory):
                    pass
            finally:
                producer_done.set()

        waiter_done = threading.Event()

        def waiter():
            with resource_ledger.bind_resource_owner("waiter-job"):
                try:
                    with cache.acquire(
                        "k",
                        lambda: pytest.fail(
                            "waiter must not run the factory — producer's "
                            "value is what it should receive",
                        ),
                    ) as value:
                        assert value == "loaded"
                finally:
                    waiter_done.set()

        tp = threading.Thread(target=producer)
        tp.start()
        assert load_started.wait(timeout=synchronization_timeout(1.0))

        tw = threading.Thread(target=waiter)
        tw.start()
        # Poll owner_timing until the waiter has entered the tracked-wait
        # branch. A fixed sleep here would race on a slow Windows runner:
        # either release_load fires before the waiter blocks (uncontended
        # acquire, no ledger entry) or the waiter's wait_timing.started_at
        # is captured after we would advance the clock, both of which turn
        # into a spurious zero-wait recording.
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if ledger.owner_timing("waiter-job")["wait_count"] >= 1:
                break
            time.sleep(0.01)
        else:
            pytest.fail(
                "waiter never entered the tracked-wait branch within 2s",
            )
        # Advance the injected clock now that started_at has been
        # captured. When _record_wait_locked fires on the waiter's context
        # exit, elapsed() sees 0.25s regardless of scheduling latency.
        clock_value[0] += 0.25
        release_load.set()

        assert producer_done.wait(timeout=synchronization_timeout(2.0))
        assert waiter_done.wait(timeout=synchronization_timeout(2.0))
        tp.join(timeout=synchronization_timeout(1.0))
        tw.join(timeout=synchronization_timeout(1.0))
        assert not tp.is_alive()
        assert not tw.is_alive()

        # The waiter's contended wait must show up in owner_timing.
        # Under the pre-fix behavior wait_count is 0 and wait_seconds is
        # 0.0 because the load_lock.acquire() call bypassed the ledger.
        waiter_timing = ledger.owner_timing("waiter-job")
        assert waiter_timing["wait_count"] >= 1, (
            f"Contended ModelCache wait must record at least one wait; got "
            f"wait_count={waiter_timing['wait_count']}. Without this the "
            f"per-job resource-wait diagnostics under-report the time the "
            f"waiter spent blocked on the producer's factory."
        )
        assert waiter_timing["wait_seconds"] > 0.0, (
            f"Contended ModelCache wait must record positive wait_seconds; "
            f"got {waiter_timing['wait_seconds']}. A minute-long BioCLIP "
            f"cold load would otherwise show zero blocked time on the "
            f"waiter's job."
        )
    finally:
        resource_ledger._set_resource_ledger_for_tests(previous)


def test_uncontended_load_does_not_record_owner_timing():
    """The uncontended fast path (nobody else holds ``load_lock``) must
    NOT touch the resource ledger: an unconditional wrap would inflate
    ``wait_count`` on every cache HIT — per-photo detection/masking
    runs would then show hundreds of zero-duration waits per job, which
    is meaningless noise in the diagnostics panel.

    The pattern mirrors ``onnx_runtime.acquire_session_cache_lock``,
    which also gates its ledger interaction on non-blocking acquire
    failing first.
    """
    import resource_ledger

    ledger = resource_ledger.ResourceLedger(cpu_capacity=2)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    try:
        cache = ModelCache(idle_secs=60)

        with resource_ledger.bind_resource_owner("owner-job"):
            # First (miss) acquire is uncontended.
            with cache.acquire("k", lambda: "v"):
                pass
            # Subsequent hits: still uncontended (nobody else is inside).
            for _ in range(3):
                with cache.acquire("k", lambda: "v"):
                    pass

        timing = ledger.owner_timing("owner-job")
        assert timing["wait_count"] == 0, (
            f"Uncontended ModelCache acquires must NOT record any wait; "
            f"got wait_count={timing['wait_count']}. An unconditional "
            f"track_external_wait wrap would flood diagnostics with "
            f"zero-duration entries on every cache hit."
        )
    finally:
        resource_ledger._set_resource_ledger_for_tests(previous)


def _cancellation_exception_classes():
    """Return every exception class ModelCache must treat as caller-local.

    Both a ``ClassificationCancelled`` (job-level abort) and a
    ``ResourceWaitCancelled`` (ledger probe fired for the producer only)
    must NOT poison a sibling waiter — that waiter should retry against a
    fresh entry. Parameterizing over both classes keeps one test body
    and prevents the two identical shapes from drifting.
    """
    from classifier import ClassificationCancelled, ClassifierLoadPaused
    from resource_ledger import ResourceWaitCancelled

    return [ClassificationCancelled, ResourceWaitCancelled, ClassifierLoadPaused]


@pytest.mark.parametrize("exc_class", _cancellation_exception_classes())
def test_cancelled_load_waiter_retries_instead_of_inheriting_cancel(exc_class):
    """A caller-local cancel from the producer must not poison an
    uncancelled waiter: the waiter retries against a fresh entry and runs
    its own factory rather than rethrowing the producer's exception.

    Covers both cancellation shapes ModelCache must translate to a
    caller-local retry — job-level ``ClassificationCancelled`` and the
    ledger-level ``ResourceWaitCancelled`` raised when the producer's
    ONNX construction lease was cancelled while a sibling job was
    already waiting on the same cache entry.
    """
    cache = ModelCache(idle_secs=60)
    load_started = threading.Event()
    release_cancel = threading.Event()
    good_factory_called = threading.Event()
    values = []

    def cancelled_factory():
        load_started.set()
        release_cancel.wait(timeout=synchronization_timeout(2.0))
        raise exc_class("producer cancelled")

    def good_factory():
        good_factory_called.set()
        return "ok"

    def cancelled_loader():
        with pytest.raises(exc_class):
            with cache.acquire("k", cancelled_factory):
                pass

    def waiting_loader():
        with cache.acquire("k", good_factory) as value:
            values.append(value)

    ta = threading.Thread(target=cancelled_loader)
    ta.start()
    assert load_started.wait(timeout=synchronization_timeout(1.0))

    tb = threading.Thread(target=waiting_loader)
    tb.start()
    time.sleep(0.05)

    release_cancel.set()
    ta.join(timeout=synchronization_timeout(2.0))
    tb.join(timeout=synchronization_timeout(2.0))

    assert not ta.is_alive()
    assert not tb.is_alive()
    assert good_factory_called.is_set(), (
        "The healthy waiter must retry against a fresh entry — its factory "
        f"should have run after the producer's {exc_class.__name__} surfaced."
    )
    assert values == ["ok"], (
        "The healthy waiter must have received the value from its own retry "
        f"factory, not inherited the producer's {exc_class.__name__}."
    )


def test_waiter_cancel_check_unblocks_while_producer_still_loading():
    """Regression: without a waiter-local cancellation check, a waiter blocked
    on another caller's load_lock only observes its own cancellation after
    the producing factory finishes. For a BioCLIP embedding job that can be
    minutes long, and one that also blocks JobRunner.shutdown, that delay
    is unacceptable. cancel_check must let the waiter unwind promptly while
    the producer keeps loading for its own callers."""
    from classifier import ClassificationCancelled

    cache = ModelCache(idle_secs=60)
    producer_started = threading.Event()
    release_producer = threading.Event()
    cancel_waiter = threading.Event()
    values = []

    def slow_factory():
        producer_started.set()
        release_producer.wait(timeout=synchronization_timeout(2.0))
        return "loaded"

    def producer():
        with cache.acquire("k", slow_factory) as v:
            values.append(v)

    waiter_error = []

    def waiter():
        try:
            with cache.acquire(
                "k",
                lambda: pytest.fail("waiter must not run the factory"),
                cancel_check=cancel_waiter.is_set,
            ):
                pytest.fail("waiter must have raised")
        except ClassificationCancelled as e:
            waiter_error.append(e)

    tp = threading.Thread(target=producer)
    tp.start()
    assert producer_started.wait(timeout=synchronization_timeout(1.0))

    tw = threading.Thread(target=waiter)
    tw.start()
    time.sleep(0.05)  # let waiter enter its poll loop

    cancel_waiter.set()
    tw.join(timeout=synchronization_timeout(2.0))
    assert not tw.is_alive(), "waiter did not unwind after cancel"
    assert len(waiter_error) == 1

    # The producer must be untouched — cancellation belonged to the waiter
    # alone, and the shared load must finish for its own caller.
    assert tp.is_alive(), "producer must still be loading"
    release_producer.set()
    tp.join(timeout=synchronization_timeout(2.0))
    assert not tp.is_alive()
    assert values == ["loaded"]

    # Waiter's cancellation must not have leaked a refcount on the entry
    # that would prevent idle eviction later.
    entry = cache._entries.get("k")
    assert entry is not None
    assert entry.refcount == 0


def test_failed_load_waiter_does_not_evict_recreated_entry():
    """Regression: when factory fails while another thread is queued on the
    same key, the waiter must not decrement the refcount of a fresh entry
    created later by a third acquirer. Without identity-checked release,
    the waiter's _release decrements the wrong entry and arms a spurious
    idle timer on a model still in use."""
    cache = ModelCache(idle_secs=60)

    # Gate B's _release_entry until thread C has installed a fresh entry
    # under the same key, so the race is deterministic rather than
    # timing-luck.
    c_installed = threading.Event()
    original_release = cache._release_entry
    release_count = [0]

    def gated_release(*args, **kwargs):
        release_count[0] += 1
        if release_count[0] == 1:
            # First release is B's failed-load release. Wait for C.
            assert c_installed.wait(timeout=synchronization_timeout(2.0)), "C never installed entry"
        return original_release(*args, **kwargs)

    cache._release_entry = gated_release

    bad_started = threading.Event()
    release_bad = threading.Event()

    def bad_factory():
        bad_started.set()
        release_bad.wait(timeout=synchronization_timeout(2.0))
        raise RuntimeError("boom")

    a_exc = []

    def thread_a():
        try:
            with cache.acquire("k", bad_factory):
                pass
        except RuntimeError as e:
            a_exc.append(e)

    b_exc = []
    b_started = threading.Event()

    def b_factory():
        b_started.set()
        return "should-not-be-called"

    def thread_b():
        try:
            with cache.acquire("k", b_factory):
                pass
        except RuntimeError as e:
            b_exc.append(e)

    ta = threading.Thread(target=thread_a)
    ta.start()
    assert bad_started.wait(timeout=synchronization_timeout(1.0))

    tb = threading.Thread(target=thread_b)
    tb.start()
    # Give B time to enter the global lock, increment refcount on the
    # original entry, and queue on load_lock.
    time.sleep(0.05)

    # Let A's factory raise and abandon the entry.
    release_bad.set()
    ta.join(timeout=synchronization_timeout(2.0))
    assert not ta.is_alive()
    assert len(a_exc) == 1

    # B has now woken from load_lock and is parked inside gated_release.
    # Install C's fresh entry under the same key.
    good_handle = cache.acquire("k", lambda: "good")
    good_handle.__enter__()
    new_entry = cache._entries["k"]
    assert new_entry.refcount == 1

    # Release B; with the bug it decrements new_entry.refcount; with the
    # fix it's a no-op because new_entry is not the entry B acquired.
    c_installed.set()
    tb.join(timeout=synchronization_timeout(2.0))
    assert not tb.is_alive()
    assert len(b_exc) == 1
    assert not b_started.is_set()

    # Critical assertion: C's entry must be untouched.
    assert cache._entries["k"] is new_entry
    assert new_entry.refcount == 1, (
        f"waiter's release corrupted recreated entry's refcount "
        f"(got {new_entry.refcount}, want 1)"
    )
    assert new_entry.idle_timer is None, (
        "waiter's release armed idle eviction on a model still in use"
    )

    good_handle.__exit__(None, None, None)


def test_stale_idle_timer_does_not_evict_renewed_entry():
    """Regression: when an idle timer's callback races past cancel() and an
    acquire+release cycle arms a fresh timer before the stale callback gets
    the lock, the stale callback must not evict the entry that the fresh
    timer is supposed to own. Without the evict_token guard, the stale
    callback sees refcount==0 and deletes immediately, throwing away a
    model whose true idle window has barely started."""
    cache = ModelCache(idle_secs=60)

    # Load and release so an entry + idle timer exist.
    h1 = cache.acquire("k", lambda: "loaded-once")
    h1.__enter__()
    h1.__exit__(None, None, None)
    entry = cache._entries["k"]
    assert entry.refcount == 0
    first_token = entry.evict_token
    first_timer = entry.idle_timer
    assert first_token is not None
    assert first_timer is not None

    # Simulate a timer callback that has already started running (so cancel
    # below cannot stop it): grab the args the real callback would receive.
    stale_args = (entry, first_token)

    # An acquire happens after the stale callback fired but before it took
    # the lock. This cancels the (already-firing) timer, bumps refcount,
    # and invalidates the token.
    h2 = cache.acquire("k", lambda: "must-not-rerun")
    h2.__enter__()
    assert cache._entries["k"] is entry, "acquire must reuse the existing entry"
    assert entry.refcount == 1
    assert entry.evict_token is None, "acquire must invalidate the in-flight token"

    # Release. This arms a fresh timer with a NEW token.
    h2.__exit__(None, None, None)
    fresh_token = entry.evict_token
    fresh_timer = entry.idle_timer
    assert fresh_token is not None and fresh_token is not first_token
    assert fresh_timer is not None and fresh_timer is not first_timer

    # Now the stale callback finally grabs the lock and runs. With the
    # guard it sees its old token mismatched against entry.evict_token and
    # bails. Without the guard it deletes the entry — wiping out a model
    # whose real idle window has hardly begun.
    cache._evict(*stale_args)

    assert cache._has_entry("k"), (
        "stale timer evicted entry that a fresh release re-armed"
    )
    assert cache._entries["k"] is entry
    assert entry.evict_token is fresh_token

    # Cancel the fresh timer so it doesn't outlive the test.
    fresh_timer.cancel()


def test_handle_release_is_idempotent():
    cache = ModelCache(idle_secs=60)
    h = cache.acquire("k", lambda: "v")
    h.__enter__()
    h.__exit__(None, None, None)
    # Second exit must be a no-op, not a double-decrement.
    h.__exit__(None, None, None)
    # Acquire again — refcount must be 0, not negative.
    with cache.acquire("k", lambda: "v2") as v:
        assert v in ("v", "v2")  # cached or re-loaded — either is fine,
        # what matters is no exception


def test_post_load_key_rekeys_when_factory_mutates_fingerprint():
    """Regression for the self-heal redownload case: the factory replaces
    on-disk weights, so the fingerprint baked into the original cache key
    is stale by the time the value is stored. A second pipeline computing
    the post-load fingerprint must hit the existing entry instead of
    loading a duplicate model."""
    cache = ModelCache(idle_secs=60)

    # The factory bumps the fingerprint (simulating self-heal rewriting
    # the on-disk weights, which changes mtime/size).
    fp = ["pre"]

    def healing_factory():
        # The on-disk state mutates here.
        fp[0] = "post"
        return "session"

    def post_load_key(_value):
        return ("model", fp[0])

    h1 = cache.acquire(("model", "pre"), healing_factory,
                       post_load_key=post_load_key)
    v1 = h1.__enter__()
    assert v1 == "session"
    # Entry must now live under the post-load key, not the pre-load one.
    assert cache._has_entry(("model", "post"))
    assert not cache._has_entry(("model", "pre"))

    # A second pipeline computes its key from the post-self-heal files
    # and looks up directly — must hit the existing entry, not reload.
    factory_calls = [0]

    def fresh_factory():
        factory_calls[0] += 1
        return "another-session"

    h2 = cache.acquire(("model", "post"), fresh_factory,
                       post_load_key=lambda _v: ("model", "post"))
    v2 = h2.__enter__()
    assert v2 == "session", "second acquire must reuse the healed session"
    assert factory_calls == [0], "factory must not run a second time"

    # Release both — the entry should still be the single one we created.
    h1.__exit__(None, None, None)
    h2.__exit__(None, None, None)


def test_post_load_key_unchanged_does_not_rekey():
    """When the factory does not mutate on-disk state, the post-load key
    matches the pre-load key and the entry stays under its original key."""
    cache = ModelCache(idle_secs=60)

    h = cache.acquire("k", lambda: "v", post_load_key=lambda _v: "k")
    h.__enter__()
    assert cache._has_entry("k")
    h.__exit__(None, None, None)
    assert cache._has_entry("k")


def test_post_load_key_collision_leaves_entry_under_original_key(caplog):
    """If a concurrent acquirer has already installed an entry under the
    post-load key, rekey is a no-op rather than overwriting the sibling
    entry. The freshly loaded session lives out its life under the
    pre-load key and idles out (brief duplicate VRAM, no corruption)."""
    cache = ModelCache(idle_secs=60)

    # Pre-populate "post" with an unrelated entry.
    h_existing = cache.acquire("post", lambda: "other-session")
    h_existing.__enter__()

    with caplog.at_level(logging.WARNING, logger="model_cache"):
        h = cache.acquire("pre", lambda: "healed",
                          post_load_key=lambda _v: "post")
        h.__enter__()

    # Rekey was refused; original entry stays at "pre", and the sibling
    # at "post" is untouched.
    assert cache._has_entry("pre")
    assert cache._has_entry("post")
    assert cache._entries["pre"].value == "healed"
    assert cache._entries["post"].value == "other-session"
    assert any("cannot rekey" in r.message for r in caplog.records)

    h.__exit__(None, None, None)
    h_existing.__exit__(None, None, None)


def test_post_load_key_exception_keeps_entry_under_original_key(caplog):
    """If the post_load_key callback itself raises, the entry stays under
    its original key and the load result is still returned. The exception
    is logged rather than propagated — a rekey failure is a degraded
    cache-hit rate, not a load failure."""
    cache = ModelCache(idle_secs=60)

    def bad_post_load(_v):
        raise RuntimeError("fingerprint failed")

    with caplog.at_level(logging.ERROR, logger="model_cache"):
        with cache.acquire("k", lambda: "v",
                           post_load_key=bad_post_load) as v:
            assert v == "v"
    assert cache._has_entry("k")
    assert any("post_load_key callback raised" in r.message
               for r in caplog.records)


def test_factory_uses_pure_cancel_probe_when_bound():
    """Factory must see the pure-cancel probe, not the pause-parking one.

    Regression for Codex discussion_r3790941020 — sibling of the
    session-cache-lock-across-pause fix in commit e3693467. Previously
    ``factory()`` ran under the pipeline's pause-aware cancel probe;
    that probe parks on pause via ``_pause_checkpoint`` while
    ``entry.load_lock`` is still owned by this thread, blocking any
    unpaused peer waiting on the same cache key for the entire pause
    window. ModelCache.acquire now rebinds the resource cancel check to
    the pure-cancel probe (never parks) around the factory call,
    matching what ``acquire_session_cache_lock`` does for its post-
    acquire recheck.
    """
    from resource_ledger import (
        bind_resource_cancel_check,
        bind_resource_pure_cancel_check,
        resolve_resource_cancel_check,
    )

    seen = {}

    def parking_probe():  # pragma: no cover - must NEVER run under factory
        seen["parking_called"] = True
        return False

    def pure_probe():
        seen["pure_called"] = True
        return False

    cache = ModelCache(idle_secs=60)

    def factory():
        # Whatever the factory (or ``create_session`` inside it) resolves
        # as the current probe MUST be the pure one — parking here would
        # keep entry.load_lock held across pause.
        seen["probe_in_factory"] = resolve_resource_cancel_check()
        return "value"

    with (
        bind_resource_cancel_check(parking_probe),
        bind_resource_pure_cancel_check(pure_probe),
    ):
        with cache.acquire("k", factory) as v:
            assert v == "value"

    assert seen["probe_in_factory"] is pure_probe
    assert "parking_called" not in seen


def test_factory_falls_back_to_pause_aware_probe_when_no_pure_bound():
    """Callers without a bound pure probe keep the existing behavior.

    Not every entry point sets up a pipeline participant (tests, standalone
    scripts, one-shot CLIs). Those must keep whatever probe the caller
    established rather than losing cancel semantics entirely.
    """
    from resource_ledger import (
        bind_resource_cancel_check,
        resolve_resource_cancel_check,
    )

    def parking_probe():
        return False

    cache = ModelCache(idle_secs=60)
    seen = {}

    def factory():
        seen["probe_in_factory"] = resolve_resource_cancel_check()
        return "value"

    with bind_resource_cancel_check(parking_probe):
        with cache.acquire("k", factory) as v:
            assert v == "value"

    # No pure probe bound → resolve_resource_pure_cancel_check falls back
    # to the pause-aware probe, so the rebind is a no-op preserving it.
    assert seen["probe_in_factory"] is parking_probe
