import os
import sys

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


def test_invalidate_workspaces_starts_fresh_compute_after_in_flight_walk():
    """A long walk that's still running when ``invalidate_workspaces`` fires
    must not block a fresh recompute. The next ``kickoff_compute`` should
    start a new worker rather than handing back the in-flight event for the
    obsolete walk — otherwise ``/api/workspaces/active/new-images`` keeps
    waiting on stale work for the full duration of the original walk and
    the UI stays in pending while a folder/workspace change is unreflected.
    """
    import threading

    cache = NewImagesCache(ttl_seconds=60)

    release_stale = threading.Event()
    stale_started = threading.Event()
    fresh_call_count = {"n": 0}

    def stale_compute():
        stale_started.set()
        release_stale.wait(timeout=5)
        return {"new_count": 999}

    def fresh_compute():
        fresh_call_count["n"] += 1
        return {"new_count": 4}

    try:
        # Kick off a long-running walk and wait until the worker has actually
        # entered ``stale_compute`` so the in-flight entry exists.
        cache.kickoff_compute(DB, 1, stale_compute)
        assert stale_started.wait(timeout=2.0), "stale worker never started"

        # Folder/workspace state changes mid-walk.
        cache.invalidate_workspaces(DB, [1])

        # The next request must run a fresh compute, not silently wait on
        # the obsolete in-flight event for the rest of the stale walk.
        fresh_event = cache.kickoff_compute(DB, 1, fresh_compute)
        assert fresh_event.wait(timeout=2.0), (
            "fresh compute after invalidation never finished"
        )
        assert fresh_call_count["n"] == 1, (
            f"expected fresh_compute to run after invalidation; "
            f"called {fresh_call_count['n']} times"
        )
        assert cache.get(DB, 1) == {"new_count": 4}
    finally:
        # Let the stale worker finish before the test exits.
        release_stale.set()


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
