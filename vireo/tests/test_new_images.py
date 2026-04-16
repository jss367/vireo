import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from db import Database
from PIL import Image


def _touch_image(path):
    """Create a real 1x1 JPEG at path."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", (1, 1), "white").save(path, "JPEG")


@pytest.fixture(autouse=True)
def _clear_shared_new_images_cache():
    from new_images import get_shared_cache
    get_shared_cache().clear()
    yield
    get_shared_cache().clear()


@pytest.fixture
def db_with_workspace(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    return db, ws_id, tmp_path


def test_count_new_images_detects_unscanned_files(db_with_workspace):
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "USA2026"
    _touch_image(str(root / "IMG_0001.JPG"))
    _touch_image(str(root / "IMG_0002.JPG"))
    db.add_folder(str(root), name="USA2026")

    from new_images import count_new_images_for_workspace
    result = count_new_images_for_workspace(db, ws_id)

    assert result["new_count"] == 2
    assert len(result["per_root"]) == 1
    assert result["per_root"][0]["new_count"] == 2
    assert len(result["sample"]) == 2


def test_count_new_images_no_double_counting_with_nested_linked_folders(db_with_workspace):
    """Nested subfolders auto-linked to workspace_folders must not cause double-counting."""
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "USA2026"
    nested = root / "day1"
    deep = nested / "raw"
    _touch_image(str(deep / "IMG_0001.JPG"))  # one unscanned file, three levels deep

    # Register root AND the intermediate dirs as workspace_folders (mirrors what
    # the scanner's Database.add_folder does for every discovered subdirectory).
    root_id = db.add_folder(str(root), name="USA2026")
    nested_id = db.add_folder(str(nested), name="day1", parent_id=root_id)
    db.add_folder(str(deep), name="raw", parent_id=nested_id)

    from new_images import count_new_images_for_workspace
    result = count_new_images_for_workspace(db, ws_id)

    assert result["new_count"] == 1, (
        f"Expected 1 new image, got {result['new_count']}. "
        f"per_root={result['per_root']}"
    )
    # Only the top-level root should appear in per_root.
    assert len(result["per_root"]) == 1
    assert result["per_root"][0]["path"] == str(root)


def test_count_new_images_basename_collision_across_subdirs(db_with_workspace):
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "shoot"
    _touch_image(str(root / "day1" / "IMG_0001.JPG"))
    _touch_image(str(root / "day2" / "IMG_0001.JPG"))
    root_id = db.add_folder(str(root), name="shoot")

    # Ingest only day1's IMG_0001.JPG.
    day1_id = db.add_folder(str(root / "day1"), name="day1", parent_id=root_id)
    db.add_photo(
        folder_id=day1_id, filename="IMG_0001.JPG", extension=".JPG",
        file_size=1, file_mtime=0.0,
    )

    from new_images import count_new_images_for_workspace
    result = count_new_images_for_workspace(db, ws_id)

    assert result["new_count"] == 1  # day2's IMG_0001.JPG is the only new one
    assert any("day2" in s for s in result["sample"])


def test_db_get_new_images_for_workspace_caches_result(db_with_workspace, monkeypatch):
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG_0001.JPG"))
    db.add_folder(str(root), name="shoot")

    calls = [0]
    import new_images
    real = new_images.count_new_images_for_workspace

    def counting_wrapper(*args, **kwargs):
        calls[0] += 1
        return real(*args, **kwargs)

    monkeypatch.setattr(new_images, "count_new_images_for_workspace", counting_wrapper)

    r1 = db.get_new_images_for_workspace(ws_id)
    r2 = db.get_new_images_for_workspace(ws_id)
    assert r1 == r2
    assert calls[0] == 1  # second call served from cache


def test_invalidate_cache_for_shared_folder_across_workspaces(tmp_path):
    """If workspaces A and B both link folder F, a scan of F must clear both caches."""
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.ensure_default_workspace()
    ws_b = db.create_workspace("B")

    # Link the same folder into both workspaces.
    db.set_active_workspace(ws_a)
    root = tmp_path / "shared"
    _touch_image(str(root / "IMG.JPG"))
    root_id = db.add_folder(str(root), name="shared")
    db.set_active_workspace(ws_b)
    db.add_workspace_folder(ws_b, root_id)

    # Prime both caches.
    db.set_active_workspace(ws_a)
    db.get_new_images_for_workspace(ws_a)
    db.get_new_images_for_workspace(ws_b)
    assert db._new_images_cache.get(ws_a) is not None
    assert db._new_images_cache.get(ws_b) is not None

    # Scan completes for folder root_id.
    db.invalidate_new_images_cache_for_folders([root_id])

    assert db._new_images_cache.get(ws_a) is None
    assert db._new_images_cache.get(ws_b) is None


def test_scan_job_invalidates_cache(db_with_workspace):
    """After a successful scan, the cached new_count must be re-computed on next read."""
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "shoot"
    _touch_image(str(root / "a.JPG"))
    root_id = db.add_folder(str(root), name="shoot")

    # Prime cache with current state (1 new).
    r1 = db.get_new_images_for_workspace(ws_id)
    assert r1["new_count"] == 1

    # Simulate a scan by inserting the photo row and invalidating.
    db.add_photo(folder_id=root_id, filename="a.JPG", extension=".JPG",
                 file_size=1, file_mtime=0.0)
    db.invalidate_new_images_cache_for_folders([root_id])

    r2 = db.get_new_images_for_workspace(ws_id)
    assert r2["new_count"] == 0


def test_two_database_instances_share_cache(tmp_path):
    db_a = Database(str(tmp_path / "test.db"))
    ws_id = db_a.ensure_default_workspace()
    db_a.set_active_workspace(ws_id)
    root = tmp_path / "shoot"
    os.makedirs(root, exist_ok=True)
    Image.new("RGB", (1, 1), "white").save(str(root / "a.JPG"), "JPEG")
    db_a.add_folder(str(root), name="shoot")
    db_a.get_new_images_for_workspace(ws_id)  # populates shared cache

    # A second Database instance (simulating a different thread / request)
    db_b = Database(str(tmp_path / "test.db"))
    assert db_b._new_images_cache.get(ws_id) is not None, (
        "Second Database should see cache populated by the first"
    )


def test_count_new_images_skips_dotfiles(db_with_workspace):
    """Dotfiles (e.g. macOS AppleDouble ``._IMG_0001.JPG`` sidecars) must not be
    counted as new, since the scanner never ingests them."""
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG_0001.JPG"))
    # AppleDouble sidecar next to the real file. Use _touch_image so the
    # suffix matches SUPPORTED_EXTENSIONS; the filter must still reject it
    # because of the leading dot.
    _touch_image(str(root / "._IMG_0001.JPG"))
    db.add_folder(str(root), name="shoot")

    from new_images import count_new_images_for_workspace
    result = count_new_images_for_workspace(db, ws_id)

    assert result["new_count"] == 1, (
        f"Expected 1 new image (dotfile skipped), got {result['new_count']}. "
        f"sample={result['sample']}"
    )
    assert all(not os.path.basename(s).startswith(".") for s in result["sample"])


def test_invalidate_new_images_after_scan_normalizes_trailing_slash(tmp_path):
    """Caller-supplied root with a trailing slash must still match folders stored
    by the scanner as ``str(Path(...))`` (no trailing slash)."""
    from app import _invalidate_new_images_after_scan

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG_0001.JPG"))
    # Folder stored in canonical form (no trailing slash), matching what the
    # scanner writes via str(Path(...)).
    db.add_folder(str(root), name="shoot")

    # Prime the cache.
    db.get_new_images_for_workspace(ws_id)
    assert db._new_images_cache.get(ws_id) is not None

    # Invalidate with a trailing slash. Must still clear the cache.
    _invalidate_new_images_after_scan(db, str(root) + "/")

    assert db._new_images_cache.get(ws_id) is None, (
        "Trailing-slash root must be normalized so path = ? matches the "
        "canonical form stored in the folders table"
    )


def test_invalidate_new_images_after_scan_clears_shared_cache_across_instances(tmp_path):
    """End-to-end coverage of _invalidate_new_images_after_scan:

    - Includes a descendant folder auto-registered with parent_id so the LIKE
      query must pick it up to invalidate the workspace that only references
      the descendant.
    - Uses two Database instances against the same DB file: populate via
      db_a, invalidate via db_b, assert db_a sees the cleared cache. This
      locks in the shared-cache contract.
    """
    from app import _invalidate_new_images_after_scan

    # db_a populates the cache for a workspace whose only linked folder is a
    # descendant of `root`.
    db_a = Database(str(tmp_path / "test.db"))
    ws_id = db_a.ensure_default_workspace()
    db_a.set_active_workspace(ws_id)

    root = tmp_path / "shoot"
    descendant = root / "day1"
    _touch_image(str(descendant / "IMG_0001.JPG"))

    root_id = db_a.add_folder(str(root), name="shoot")
    descendant_id = db_a.add_folder(
        str(descendant), name="day1", parent_id=root_id
    )

    # Create a second workspace that only links the descendant folder, so
    # invalidation via the LIKE query (on the root path) must reach it.
    ws_b = db_a.create_workspace("B")
    db_a.add_workspace_folder(ws_b, descendant_id)

    # Prime caches for both workspaces via db_a.
    db_a.get_new_images_for_workspace(ws_id)
    db_a.get_new_images_for_workspace(ws_b)
    assert db_a._new_images_cache.get(ws_id) is not None
    assert db_a._new_images_cache.get(ws_b) is not None

    # Invalidate from a DIFFERENT Database instance to exercise the shared
    # cache contract end-to-end.
    db_b = Database(str(tmp_path / "test.db"))
    _invalidate_new_images_after_scan(db_b, str(root))

    # db_a must observe the cleared caches for both workspaces, including the
    # one that only references the descendant folder (proves the LIKE picked
    # it up).
    assert db_a._new_images_cache.get(ws_id) is None, (
        "Cache for workspace linked to root should be cleared"
    )
    assert db_a._new_images_cache.get(ws_b) is None, (
        "Cache for workspace linked only to descendant folder should be cleared "
        "(LIKE query must match auto-registered subfolders)"
    )


class _CapturingConn:
    """Proxy that forwards every attr to the real sqlite3 connection but
    captures parameters of ``folders``/``LIKE`` queries for assertions.

    Needed because ``sqlite3.Connection.execute`` is a read-only built-in
    attribute and cannot be monkeypatched directly.
    """

    def __init__(self, real_conn, captured):
        self._real = real_conn
        self._captured = captured

    def execute(self, sql, params=()):
        if "folders" in sql and "LIKE" in sql:
            self._captured["params"] = params
        return self._real.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_invalidate_new_images_after_scan_uses_os_sep(db_with_workspace):
    """The descendant LIKE pattern must end in ``os.sep + '%'`` so it matches
    folder paths stored by the scanner via ``str(Path(...))`` — which uses
    ``\\`` on Windows and ``/`` on POSIX."""
    from app import _invalidate_new_images_after_scan

    db, ws_id, tmp_path = db_with_workspace
    captured = {}
    db.conn = _CapturingConn(db.conn, captured)

    _invalidate_new_images_after_scan(db, str(tmp_path / "root"))

    assert "params" in captured, "Expected LIKE query on folders to fire"
    assert captured["params"][1].endswith(os.sep + "%"), (
        f"Descendant LIKE pattern must end with os.sep + %, "
        f"got {captured['params'][1]!r}"
    )


def test_invalidate_new_images_after_scan_windows_separator(db_with_workspace, monkeypatch):
    """Simulate Windows: the scanner stores folder paths with backslashes via
    ``str(Path(...))``, so the LIKE descendant pattern must also use
    backslashes. Verified here by monkeypatching ``os.sep`` to ``\\``."""
    import app as app_module
    from app import _invalidate_new_images_after_scan

    db, ws_id, tmp_path = db_with_workspace
    captured = {}
    db.conn = _CapturingConn(db.conn, captured)

    # Patch os.sep on the ``os`` module the helper resolves through so it sees
    # the Windows separator during this call.
    monkeypatch.setattr(app_module.os, "sep", "\\")
    # os.path.normpath on POSIX won't rewrite ``C:\shoot``; pass a path that
    # normpath leaves alone so we can assert on the trailing separator.
    _invalidate_new_images_after_scan(db, "C:\\shoot")

    assert "params" in captured, "Expected LIKE query on folders to fire"
    assert captured["params"][1].endswith("\\%"), (
        f"Windows pattern must end with \\%, got {captured['params'][1]!r}"
    )


def test_get_new_images_for_workspace_race_does_not_repopulate_stale(db_with_workspace, monkeypatch):
    """If invalidation fires during compute, the stale result must not be cached."""
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    root_id = db.add_folder(str(root), name="shoot")

    import new_images

    def racing_compute(db_arg, ws_id_arg, **kwargs):
        # Simulate an invalidation that fires while we're "walking."
        db_arg.invalidate_new_images_cache_for_folders([root_id])
        return {"new_count": 999, "per_root": [], "sample": []}  # stale value

    monkeypatch.setattr(new_images, "count_new_images_for_workspace", racing_compute)

    db.get_new_images_for_workspace(ws_id)
    # Cache must NOT hold the stale value.
    assert db._new_images_cache.get(ws_id) is None, "Stale compute leaked into cache"
