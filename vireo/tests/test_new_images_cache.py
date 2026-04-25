import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


from new_images import NewImagesCache

# Sentinel db_path for the unit tests — the cache keys by
# (db_path, workspace_id), so every call site must pass one. The exact string
# doesn't matter here since nothing opens it; what matters is that the same
# key is used for set and get.
DB = "/tmp/unit-test.db"


def test_cache_returns_cached_value_within_ttl():
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(DB, workspace_id=1, result={"new_count": 5})
    assert cache.get(DB, 1) == {"new_count": 5}


def test_cache_expires_after_ttl(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr("new_images.time.monotonic", lambda: clock[0])
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(DB, workspace_id=1, result={"new_count": 5})
    clock[0] += 61
    assert cache.get(DB, 1) is None


def test_cache_invalidate_by_folder_ids_clears_all_workspaces_linking_those_folders():
    """When folder F is scanned, every workspace linked to F must have its cache cleared."""
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(DB, workspace_id=1, result={"new_count": 5})
    cache.set(DB, workspace_id=2, result={"new_count": 7})

    # Caller supplies the mapping: folder_id -> list of workspace_ids linked to it.
    cache.invalidate_workspaces(DB, [1, 2])

    assert cache.get(DB, 1) is None
    assert cache.get(DB, 2) is None


def test_cache_invalidate_workspace_does_not_clear_others():
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(DB, workspace_id=1, result={"new_count": 5})
    cache.set(DB, workspace_id=2, result={"new_count": 7})
    cache.invalidate_workspaces(DB, [1])
    assert cache.get(DB, 1) is None
    assert cache.get(DB, 2) == {"new_count": 7}


def test_cache_set_with_stale_generation_is_dropped():
    cache = NewImagesCache(ttl_seconds=60)
    gen_before = cache.get_generation(DB, workspace_id=1)
    cache.invalidate_workspaces(DB, [1])
    # Simulate: compute started before invalidate, tries to write with stale gen
    cache.set(DB, workspace_id=1, result={"new_count": 5}, generation=gen_before)
    assert cache.get(DB, 1) is None, "Stale set must not repopulate after invalidate"


def test_cache_set_with_current_generation_stores():
    cache = NewImagesCache(ttl_seconds=60)
    gen = cache.get_generation(DB, workspace_id=1)
    cache.set(DB, workspace_id=1, result={"new_count": 5}, generation=gen)
    assert cache.get(DB, 1) == {"new_count": 5}


def test_cache_set_without_generation_stores_unconditionally():
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(DB, workspace_id=1, result={"new_count": 5})
    assert cache.get(DB, 1) == {"new_count": 5}


def test_cache_invalidate_then_set_with_stale_gen_is_dropped_then_new_set_works():
    cache = NewImagesCache(ttl_seconds=60)
    gen1 = cache.get_generation(DB, 1)
    cache.invalidate_workspaces(DB, [1])
    cache.set(DB, workspace_id=1, result={"new_count": 5}, generation=gen1)  # dropped
    assert cache.get(DB, 1) is None
    # Fresh compute after invalidation gets the new generation and stores fine.
    gen2 = cache.get_generation(DB, 1)
    assert gen2 != gen1
    cache.set(DB, workspace_id=1, result={"new_count": 7}, generation=gen2)
    assert cache.get(DB, 1) == {"new_count": 7}


def test_cache_keys_by_db_path_isolates_identical_workspace_ids():
    """Two databases with identical workspace_ids (typically 1 for Default)
    must not read each other's cached results. Otherwise switching between two
    open Vireo instances — or running tests that reuse a shared process-wide
    cache — would cross-contaminate."""
    cache = NewImagesCache(ttl_seconds=60)
    cache.set("/path/a.db", workspace_id=1, result={"new_count": 5, "tag": "A"})
    cache.set("/path/b.db", workspace_id=1, result={"new_count": 99, "tag": "B"})

    assert cache.get("/path/a.db", 1) == {"new_count": 5, "tag": "A"}
    assert cache.get("/path/b.db", 1) == {"new_count": 99, "tag": "B"}

    # Invalidating one db must not clear the other.
    cache.invalidate_workspaces("/path/a.db", [1])
    assert cache.get("/path/a.db", 1) is None
    assert cache.get("/path/b.db", 1) == {"new_count": 99, "tag": "B"}


def test_kickoff_compute_drops_stale_error_when_generation_changes():
    """If ``invalidate_workspaces`` runs while a background compute is in
    flight, a subsequent failure for that stale generation must not be
    recorded. Mirrors the stale-write guard in :meth:`set` — without it,
    the next request goes into the 30s backoff window for a key that has
    already moved on, suppressing a fresh recompute after workspace/folder
    changes."""
    cache = NewImagesCache(ttl_seconds=60)

    # Compute simulates the race: it bumps the generation (as a finishing
    # scan or workspace switch would) and *then* fails. The error reflects
    # the old generation and must be dropped.
    def compute():
        cache.invalidate_workspaces(DB, [1])
        raise RuntimeError("disk unreachable")

    event = cache.kickoff_compute(DB, 1, compute)
    assert event.wait(timeout=2.0), "background compute did not finish"

    assert cache.get_recent_error(DB, 1) is None, (
        "stale error must be dropped when the generation moved during compute"
    )


def test_kickoff_compute_records_error_when_generation_unchanged():
    """The generation guard must not regress the normal failure path: if no
    invalidation happens during compute, the error is recorded so the next
    request hits the backoff window."""
    cache = NewImagesCache(ttl_seconds=60)

    def compute():
        raise RuntimeError("disk unreachable")

    event = cache.kickoff_compute(DB, 1, compute)
    assert event.wait(timeout=2.0)

    err = cache.get_recent_error(DB, 1)
    assert err is not None and "disk unreachable" in err


def test_kickoff_compute_clears_prior_error_on_success():
    """A successful compute must clear any prior failure so a transient error
    doesn't keep suppressing retries after recovery."""
    cache = NewImagesCache(ttl_seconds=60)

    def boom():
        raise RuntimeError("transient")

    event = cache.kickoff_compute(DB, 1, boom)
    assert event.wait(timeout=2.0)
    assert cache.get_recent_error(DB, 1) is not None

    # The error sits in the backoff window and would normally suppress the
    # next kickoff. Bypass that by calling set() directly to simulate a
    # later successful compute (the worker calls set() then clears errors).
    # Easier: poke the internal state to drop the error and re-run via the
    # public API.
    cache._errors.clear()  # simulate window expiry

    def ok():
        return {"new_count": 3}

    event = cache.kickoff_compute(DB, 1, ok)
    assert event.wait(timeout=2.0)
    assert cache.get(DB, 1) == {"new_count": 3}
    assert cache.get_recent_error(DB, 1) is None


def test_invalidate_workspaces_clears_recent_error():
    """A recorded failure must not survive an invalidation: when a scan or
    workspace/folder change advances the generation, the old error reflects
    state that no longer applies and would otherwise force the next request
    into the 30s backoff window even though the key has moved on. The next
    ``kickoff_compute`` after invalidation should run fresh, not be gated."""
    cache = NewImagesCache(ttl_seconds=60)

    # Record a failure the normal way (via kickoff) so the backoff entry
    # exists and ``get_recent_error`` would surface it.
    def boom():
        raise RuntimeError("disk unreachable")

    event = cache.kickoff_compute(DB, 1, boom)
    assert event.wait(timeout=2.0)
    assert cache.get_recent_error(DB, 1) is not None

    # Invalidation simulates a finished scan or workspace folder change.
    cache.invalidate_workspaces(DB, [1])
    assert cache.get_recent_error(DB, 1) is None, (
        "invalidate_workspaces must drop stale failures so a fresh recompute "
        "isn't suppressed by the prior error's 30s backoff"
    )

    # And the next kickoff actually runs (not short-circuited by the error
    # gate), letting a successful compute repopulate the cache.
    def ok():
        return {"new_count": 4}

    event = cache.kickoff_compute(DB, 1, ok)
    assert event.wait(timeout=2.0)
    assert cache.get(DB, 1) == {"new_count": 4}


def test_invalidate_workspaces_clears_error_only_for_targeted_keys():
    """Invalidation is scoped: a failure on workspace 2 must survive when
    workspace 1's cache is invalidated."""
    cache = NewImagesCache(ttl_seconds=60)

    def boom():
        raise RuntimeError("disk unreachable")

    cache.kickoff_compute(DB, 1, boom).wait(timeout=2.0)
    cache.kickoff_compute(DB, 2, boom).wait(timeout=2.0)
    assert cache.get_recent_error(DB, 1) is not None
    assert cache.get_recent_error(DB, 2) is not None

    cache.invalidate_workspaces(DB, [1])

    assert cache.get_recent_error(DB, 1) is None
    assert cache.get_recent_error(DB, 2) is not None


def test_kickoff_compute_reuses_in_flight_for_current_generation():
    """A second kickoff that arrives while a compute is in flight for the same
    generation must reuse the existing thread — not spawn a duplicate walk."""
    cache = NewImagesCache(ttl_seconds=60)
    started = threading.Event()
    proceed = threading.Event()
    call_count = [0]

    def slow_compute():
        call_count[0] += 1
        started.set()
        proceed.wait(timeout=5.0)
        return {"new_count": 3}

    e1 = cache.kickoff_compute(DB, 1, slow_compute)
    assert started.wait(timeout=2.0)
    e2 = cache.kickoff_compute(DB, 1, slow_compute)
    assert e1 is e2, "concurrent kickoff for same generation must reuse in-flight"
    proceed.set()
    assert e1.wait(timeout=2.0)
    assert call_count[0] == 1


def test_kickoff_compute_coalesces_stale_generation_into_deferred_rerun():
    """If ``invalidate_workspaces`` runs while a compute is in flight, the next
    kickoff must NOT spawn a parallel walk. Generations advance per discovered
    folder during a scan, and the navbar re-polls pending every 3s; without
    coalescing, both effects fan out concurrent ``os.walk`` jobs that thrash
    disk/CPU on large libraries.

    The contract: at most one walk per key in flight; the latest
    ``compute_fn`` is queued as a deferred rerun and fires once the current
    walk finishes."""
    import time as _t
    cache = NewImagesCache(ttl_seconds=60)
    stale_started = threading.Event()
    stale_proceed = threading.Event()

    def stale_compute():
        stale_started.set()
        stale_proceed.wait(timeout=5.0)
        return {"new_count": 999}  # stale value — must be dropped

    e1 = cache.kickoff_compute(DB, 1, stale_compute)
    assert stale_started.wait(timeout=2.0)

    # Invalidation advances the generation while the compute is still running.
    cache.invalidate_workspaces(DB, [1])

    fresh_called = threading.Event()

    def fresh_compute():
        fresh_called.set()
        return {"new_count": 7}

    e2 = cache.kickoff_compute(DB, 1, fresh_compute)
    # Caller waits on the in-flight (stale) event; no parallel walk spawned.
    assert e1 is e2, "stale-generation kickoff must reuse in-flight, not fork"
    assert not fresh_called.is_set(), (
        "fresh compute must not run in parallel with stale in-flight thread"
    )

    # A burst of polls during the scan-time invalidation storm must collapse
    # to a single rerun token, not queue a backlog of walks.
    e3 = cache.kickoff_compute(DB, 1, fresh_compute)
    e4 = cache.kickoff_compute(DB, 1, fresh_compute)
    assert e3 is e1 and e4 is e1
    assert not fresh_called.is_set()

    # Release the stale thread; the deferred rerun fires asynchronously after
    # its finally block runs.
    stale_proceed.set()
    assert e1.wait(timeout=2.0)
    assert fresh_called.wait(timeout=2.0), (
        "deferred rerun must spawn fresh compute after stale finishes"
    )

    # Wait for the rerun's result to land in the cache.
    deadline = _t.monotonic() + 2.0
    while _t.monotonic() < deadline:
        if cache.get(DB, 1) == {"new_count": 7}:
            break
        _t.sleep(0.01)
    assert cache.get(DB, 1) == {"new_count": 7}, (
        "stale result must be dropped by set()'s generation guard, and the "
        "deferred rerun's result must populate the cache"
    )


def test_kickoff_compute_coalesces_repeated_invalidations_into_single_rerun():
    """Multiple stale-generation kickoffs during a single in-flight walk must
    all collapse onto one rerun token (last writer wins). Otherwise a long
    walk plus repeated scan-time invalidations could backlog walks."""
    cache = NewImagesCache(ttl_seconds=60)
    stale_started = threading.Event()
    stale_proceed = threading.Event()
    stale_calls = [0]

    def stale_compute():
        stale_calls[0] += 1
        stale_started.set()
        stale_proceed.wait(timeout=5.0)
        return {"new_count": 0}

    cache.kickoff_compute(DB, 1, stale_compute)
    assert stale_started.wait(timeout=2.0)

    # Several rounds of invalidation + kickoff with different compute_fns;
    # only the last one's result should ultimately land in the cache.
    rerun_calls = {"a": 0, "b": 0, "c": 0}

    def make_rerun(tag, value):
        def fn():
            rerun_calls[tag] += 1
            return {"new_count": value, "tag": tag}
        return fn

    cache.invalidate_workspaces(DB, [1])
    cache.kickoff_compute(DB, 1, make_rerun("a", 1))
    cache.invalidate_workspaces(DB, [1])
    cache.kickoff_compute(DB, 1, make_rerun("b", 2))
    cache.invalidate_workspaces(DB, [1])
    cache.kickoff_compute(DB, 1, make_rerun("c", 3))

    stale_proceed.set()

    # Wait for the deferred rerun to finish populating the cache.
    import time as _t
    deadline = _t.monotonic() + 2.0
    while _t.monotonic() < deadline:
        cached = cache.get(DB, 1)
        if cached is not None and cached.get("tag") == "c":
            break
        _t.sleep(0.01)

    assert cache.get(DB, 1) == {"new_count": 3, "tag": "c"}, (
        "last queued compute_fn must win — earlier ones are overwritten "
        "in the rerun slot, not run as parallel walks"
    )
    assert stale_calls[0] == 1
    assert rerun_calls["a"] == 0 and rerun_calls["b"] == 0, (
        "earlier rerun candidates must not run — they were superseded "
        f"in the rerun slot before the stale walk finished (got {rerun_calls})"
    )
    assert rerun_calls["c"] == 1


def test_kickoff_compute_stale_thread_clears_inflight_slot_for_rerun():
    """The stale worker's finally block must clear ``_inflight[key]`` so the
    deferred rerun spawned from that finally can take ownership of the slot.
    Otherwise the rerun's ``kickoff_compute`` would see a stale in-flight
    entry and queue itself as another rerun, deadlocking progress."""
    cache = NewImagesCache(ttl_seconds=60)
    stale_started = threading.Event()
    stale_proceed = threading.Event()

    def stale_compute():
        stale_started.set()
        stale_proceed.wait(timeout=5.0)
        return {"new_count": 1}

    cache.kickoff_compute(DB, 1, stale_compute)
    assert stale_started.wait(timeout=2.0)
    cache.invalidate_workspaces(DB, [1])

    rerun_done = threading.Event()

    def rerun_compute():
        rerun_done.set()
        return {"new_count": 2}

    cache.kickoff_compute(DB, 1, rerun_compute)
    stale_proceed.set()

    assert rerun_done.wait(timeout=2.0), (
        "rerun must run after stale thread finishes and frees the in-flight slot"
    )

    # Cache eventually reflects the rerun's result.
    import time as _t
    deadline = _t.monotonic() + 2.0
    while _t.monotonic() < deadline:
        if cache.get(DB, 1) == {"new_count": 2}:
            break
        _t.sleep(0.01)
    assert cache.get(DB, 1) == {"new_count": 2}


def test_cache_generation_is_scoped_by_db_path():
    """A bump to one db's generation must not race-drop a concurrent write for
    a different db with the same workspace_id."""
    cache = NewImagesCache(ttl_seconds=60)
    gen_a = cache.get_generation("/path/a.db", 1)
    gen_b = cache.get_generation("/path/b.db", 1)
    # Advance only A's generation.
    cache.invalidate_workspaces("/path/a.db", [1])
    # B's stale-check passes because its generation is still gen_b — the
    # invalidation on A must not have bumped it.
    cache.set("/path/b.db", workspace_id=1,
              result={"new_count": 7}, generation=gen_b)
    assert cache.get("/path/b.db", 1) == {"new_count": 7}
    # A's attempted write with the old gen is still dropped.
    cache.set("/path/a.db", workspace_id=1,
              result={"new_count": 99}, generation=gen_a)
    assert cache.get("/path/a.db", 1) is None
