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
