import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


from new_images import NewImagesCache


def test_cache_returns_cached_value_within_ttl():
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(workspace_id=1, result={"new_count": 5})
    assert cache.get(1) == {"new_count": 5}


def test_cache_expires_after_ttl(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr("new_images.time.monotonic", lambda: clock[0])
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(workspace_id=1, result={"new_count": 5})
    clock[0] += 61
    assert cache.get(1) is None


def test_cache_invalidate_by_folder_ids_clears_all_workspaces_linking_those_folders():
    """When folder F is scanned, every workspace linked to F must have its cache cleared."""
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(workspace_id=1, result={"new_count": 5})
    cache.set(workspace_id=2, result={"new_count": 7})

    # Caller supplies the mapping: folder_id -> list of workspace_ids linked to it.
    cache.invalidate_workspaces([1, 2])

    assert cache.get(1) is None
    assert cache.get(2) is None


def test_cache_invalidate_workspace_does_not_clear_others():
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(workspace_id=1, result={"new_count": 5})
    cache.set(workspace_id=2, result={"new_count": 7})
    cache.invalidate_workspaces([1])
    assert cache.get(1) is None
    assert cache.get(2) == {"new_count": 7}


def test_cache_set_with_stale_generation_is_dropped():
    cache = NewImagesCache(ttl_seconds=60)
    gen_before = cache.get_generation(workspace_id=1)
    cache.invalidate_workspaces([1])
    # Simulate: compute started before invalidate, tries to write with stale gen
    cache.set(workspace_id=1, result={"new_count": 5}, generation=gen_before)
    assert cache.get(1) is None, "Stale set must not repopulate after invalidate"


def test_cache_set_with_current_generation_stores():
    cache = NewImagesCache(ttl_seconds=60)
    gen = cache.get_generation(workspace_id=1)
    cache.set(workspace_id=1, result={"new_count": 5}, generation=gen)
    assert cache.get(1) == {"new_count": 5}


def test_cache_set_without_generation_stores_unconditionally():
    cache = NewImagesCache(ttl_seconds=60)
    cache.set(workspace_id=1, result={"new_count": 5})
    assert cache.get(1) == {"new_count": 5}


def test_cache_invalidate_then_set_with_stale_gen_is_dropped_then_new_set_works():
    cache = NewImagesCache(ttl_seconds=60)
    gen1 = cache.get_generation(1)
    cache.invalidate_workspaces([1])
    cache.set(workspace_id=1, result={"new_count": 5}, generation=gen1)  # dropped
    assert cache.get(1) is None
    # Fresh compute after invalidation gets the new generation and stores fine.
    gen2 = cache.get_generation(1)
    assert gen2 != gen1
    cache.set(workspace_id=1, result={"new_count": 7}, generation=gen2)
    assert cache.get(1) == {"new_count": 7}
