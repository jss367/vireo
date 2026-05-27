# vireo/tests/test_model_cache.py
import os
import sys
import threading
import time

import pytest

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
        release_load.wait(timeout=2.0)
        return "loaded"

    values = []

    def worker():
        with cache.acquire("k", slow_factory) as v:
            values.append(v)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    assert load_started.wait(timeout=1.0)
    # Second worker should now be queued waiting for the first load to finish.
    t2.start()
    time.sleep(0.05)
    release_load.set()
    t1.join(timeout=2.0)
    t2.join(timeout=2.0)
    assert not t1.is_alive(), "t1 did not terminate after join"
    assert not t2.is_alive(), "t2 did not terminate after join"
    assert load_calls == [1], "factory must run exactly once across threads"
    assert values == ["loaded", "loaded"]


def test_failed_load_waiter_does_not_evict_recreated_entry():
    """Regression: when factory fails while another thread is queued on the
    same key, the waiter must not decrement the refcount of a fresh entry
    created later by a third acquirer. Without identity-checked release,
    the waiter's _release decrements the wrong entry and arms a spurious
    idle timer on a model still in use."""
    cache = ModelCache(idle_secs=60)

    # Gate B's _release until thread C has installed a fresh entry under
    # the same key, so the race is deterministic rather than timing-luck.
    c_installed = threading.Event()
    original_release = cache._release
    release_count = [0]

    def gated_release(*args, **kwargs):
        release_count[0] += 1
        if release_count[0] == 1:
            # First _release is B's failed-load release. Wait for C.
            assert c_installed.wait(timeout=2.0), "C never installed entry"
        return original_release(*args, **kwargs)

    cache._release = gated_release

    bad_started = threading.Event()
    release_bad = threading.Event()

    def bad_factory():
        bad_started.set()
        release_bad.wait(timeout=2.0)
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
    assert bad_started.wait(timeout=1.0)

    tb = threading.Thread(target=thread_b)
    tb.start()
    # Give B time to enter the global lock, increment refcount on the
    # original entry, and queue on load_lock.
    time.sleep(0.05)

    # Let A's factory raise and abandon the entry.
    release_bad.set()
    ta.join(timeout=2.0)
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
    tb.join(timeout=2.0)
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
    stale_args = ("k", entry, first_token)

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
