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
