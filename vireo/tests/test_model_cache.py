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
    assert load_calls == [1], "factory must run exactly once across threads"
    assert values == ["loaded", "loaded"]


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
