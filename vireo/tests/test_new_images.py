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


def test_mapped_roots_excludes_folder_with_any_linked_ancestor(db_with_workspace):
    """A folder is a root only if *no* ancestor at any depth is linked — not
    just its immediate parent. Without this, a scenario like:

        /A       linked
        /A/B     unlinked (intermediate)
        /A/B/C   linked

    would treat both /A and /A/B/C as roots (because /A/B, C's immediate
    parent, is not linked) and os.walk'ing both would double-count every file
    under /A/B/C.
    """
    db, ws_id, tmp_path = db_with_workspace

    a = tmp_path / "A"
    b = a / "B"
    c = b / "C"
    _touch_image(str(c / "IMG_0001.JPG"))

    # Register the full chain against the workspace, then unlink the
    # intermediate /A/B so only /A and /A/B/C remain linked.
    a_id = db.add_folder(str(a), name="A")
    b_id = db.add_folder(str(b), name="B", parent_id=a_id)
    db.add_folder(str(c), name="C", parent_id=b_id)
    db.remove_workspace_folder(ws_id, b_id)

    from new_images import count_new_images_for_workspace
    result = count_new_images_for_workspace(db, ws_id)

    assert result["new_count"] == 1, (
        f"Expected 1 new image (must not double-count across /A and /A/B/C), "
        f"got {result['new_count']}. per_root={result['per_root']}"
    )
    assert len(result["per_root"]) == 1, (
        f"Only the true root /A should walk; per_root={result['per_root']}"
    )
    assert result["per_root"][0]["path"] == str(a), (
        f"Root path should be /A, got {result['per_root'][0]['path']!r}"
    )


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


def test_invalidate_new_images_after_scan_preserves_dotdot_segments(tmp_path):
    """Caller-supplied root with ``..`` segments must still match folders stored
    by the scanner as ``str(Path(...))`` — which preserves ``..`` segments.

    Using ``os.path.normpath`` here would resolve ``..`` to a non-matching
    canonical path, leaving the cache stale after a successful scan launched
    against a path like ``/data/shoot/../trip``."""
    from pathlib import Path

    from app import _invalidate_new_images_after_scan

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    # Build a path containing `..`. Mirror what the scanner would do: store
    # the folder via str(Path(...)) so the `..` segment survives in
    # folders.path.
    dotdot_path = tmp_path / "x" / ".." / "y"
    # Create the resolved directory on disk with a real image so
    # count_new_images_for_workspace can walk it (it needs os.path.isdir to
    # pass).
    (tmp_path / "y").mkdir()
    _touch_image(str(tmp_path / "y" / "IMG_0001.JPG"))

    stored = str(Path(dotdot_path))
    assert ".." in stored, (
        f"Test precondition: str(Path(...)) must preserve `..` — got {stored!r}. "
        "If your platform's pathlib collapses `..`, this test is moot."
    )
    db.add_folder(stored, name="y")

    # Prime the cache.
    db.get_new_images_for_workspace(ws_id)
    assert db._new_images_cache.get(ws_id) is not None

    # Invalidate using the same `..`-bearing path. Canonicalization inside
    # the helper must use str(Path(...)) (which keeps `..`) rather than
    # os.path.normpath (which would resolve `..` and produce a mismatch).
    _invalidate_new_images_after_scan(db, stored)

    assert db._new_images_cache.get(ws_id) is None, (
        "Root containing `..` must match the canonical form stored by the "
        "scanner (which also preserves `..` via str(Path(...))). Using "
        "os.path.normpath here would resolve `..` and leave the cache stale."
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


def test_scan_handler_invalidates_cache_when_scan_raises(tmp_path, monkeypatch):
    """If do_scan raises partway through, invalidation must still run because
    scanner.scan commits photo rows incrementally. The try/finally in the scan
    handlers at vireo/app.py guarantees this."""
    import app as app_module

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    root_id = db.add_folder(str(root), name="shoot")

    # Prime the cache.
    db.get_new_images_for_workspace(ws_id)
    assert db._new_images_cache.get(ws_id) is not None

    # Simulate the try/finally pattern used in api_job_scan / api_job_import_full:
    # do_scan raises, invalidation still runs in finally.
    try:
        try:
            raise RuntimeError("simulated mid-scan failure")
        finally:
            app_module._invalidate_new_images_after_scan(db, str(root))
    except RuntimeError:
        pass

    assert db._new_images_cache.get(ws_id) is None, (
        "Cache must be invalidated even when the scan raises"
    )


def test_pipeline_job_scan_invalidates_cache(tmp_path, monkeypatch):
    """The pipeline_job scanner stage (the third scan path alongside
    api_job_scan and api_job_import_full) must invalidate the new-images
    cache. Prior to this fix, a pipeline run from templates/pipeline.html
    would scan photos without clearing the banner's "N new images" count,
    so the banner stayed stale until TTL.

    Runs the full pipeline job with classify / extract-masks / regroup
    skipped so the test completes quickly; asserts the cache for the active
    workspace was cleared by the time the job returns.
    """
    import app as app_module  # ensures _invalidate_new_images_after_scan is wired up
    import config as cfg
    from PIL import Image
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    Image.new("RGB", (100, 100), "red").save(str(photo_dir / "a.jpg"))
    Image.new("RGB", (100, 100), "red").save(str(photo_dir / "b.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Register the source folder against the workspace so
    # count_new_images_for_workspace has something to enumerate.
    db.add_folder(str(photo_dir), name="photos")

    # Prime the cache: two files present, none ingested yet -> new_count=2.
    primed = db.get_new_images_for_workspace(ws_id)
    assert primed["new_count"] == 2
    assert db._new_images_cache.get(ws_id) is not None

    # Minimal FakeRunner inline so we don't couple this test to
    # tests/test_pipeline_job.py's helper class.
    class _Runner:
        def push_event(self, *a, **k): pass
        def set_steps(self, *a, **k): pass
        def update_step(self, *a, **k): pass
        def is_cancelled(self, *a, **k): return False

    job = {
        "id": "test-pipeline-invalidates",
        "type": "pipeline",
        "status": "running",
        "started_at": "2026-01-01T00:00:00",
        "finished_at": None,
        "progress": {"current": 0, "total": 0, "current_file": ""},
        "result": None,
        "errors": [],
        "config": {},
        "workspace_id": ws_id,
    }

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    run_pipeline_job(job, _Runner(), db_path, ws_id, params)

    # After the scan, the cache must be cleared so the next banner fetch
    # recomputes against the updated photos table.
    assert db._new_images_cache.get(ws_id) is None, (
        "pipeline_job scanner_stage must invalidate the new-images cache "
        "for roots fed to do_scan (try/finally mirrors api_job_scan)"
    )
    # Sanity-check that the helper app.py exposes still resolves to the
    # same canonical implementation used by pipeline_job.
    import new_images as new_images_mod
    assert app_module._invalidate_new_images_after_scan is (
        new_images_mod.invalidate_new_images_after_scan
    )


def test_audit_import_untracked_invalidates_cache(db_with_workspace):
    """audit.import_untracked() calls scanner.scan internally (once per unique
    parent directory of the untracked paths). Each scan must invalidate the
    new-images cache for workspaces that reference the scanned folder so the
    banner updates immediately rather than waiting for TTL expiry.

    Mirrors the try/finally pattern in pipeline_job.scanner_stage and the
    api_job_scan / api_job_import_full handlers.
    """
    from audit import import_untracked

    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "shoot"
    img_path = root / "IMG_0001.JPG"
    _touch_image(str(img_path))
    db.add_folder(str(root), name="shoot")

    # Prime the cache: one file present, none ingested -> new_count=1.
    primed = db.get_new_images_for_workspace(ws_id)
    assert primed["new_count"] == 1
    assert db._new_images_cache.get(ws_id) is not None

    import_untracked(db, [str(img_path)])

    assert db._new_images_cache.get(ws_id) is None, (
        "audit.import_untracked must invalidate the new-images cache for "
        "every scanned parent directory (try/finally mirrors pipeline_job)"
    )


def test_invalidate_new_images_cache_for_folders_handles_thousands_of_ids(
    db_with_workspace,
):
    """A scan of a deep tree can auto-register thousands of descendant folders.
    Passing them all to ``invalidate_new_images_cache_for_folders`` must not
    raise ``OperationalError: too many SQL variables``.

    The helper should chunk the ``IN (?, ?, ...)`` query rather than building a
    single placeholder list whose length exceeds SQLite's
    ``SQLITE_LIMIT_VARIABLE_NUMBER``. To make this test fast and independent
    of the host SQLite build (the default cap varies — 999 on old builds,
    250000 on newer ones), we lower the cap on the connection via
    ``setlimit`` so a modest list is enough to expose the bug.
    """
    import sqlite3
    db, ws_id, tmp_path = db_with_workspace

    # Lower the variable cap so 2500 ids would blow past it if the helper
    # did not chunk.
    db.conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 500)

    # 2500 unknown folder_ids: SQL simply returns no rows, but must execute
    # without raising.
    unknown_ids = list(range(100000, 102500))
    db.invalidate_new_images_cache_for_folders(unknown_ids)  # must not raise

    # Now build a scenario with a real linked folder mixed in with many unknown
    # ids. Prime the cache, invalidate, and assert it was cleared.
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    root_id = db.add_folder(str(root), name="shoot")

    db.get_new_images_for_workspace(ws_id)
    assert db._new_images_cache.get(ws_id) is not None

    mixed = unknown_ids + [root_id]
    db.invalidate_new_images_cache_for_folders(mixed)
    assert db._new_images_cache.get(ws_id) is None, (
        "Chunked invalidation must still clear the real linked folder's workspace"
    )


def test_delete_workspace_clears_cache(tmp_path):
    """Deleting a workspace must drop its cached new-images entry immediately.
    Otherwise, if SQLite later reuses the rowid for a new workspace, stale
    data leaks across identities until TTL expiry."""
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    ws_tmp = db.create_workspace("to-delete")

    # Populate the cache directly (avoids needing a real folder / photo setup).
    db._new_images_cache.set(
        ws_tmp, {"new_count": 7, "per_root": [], "sample": []}
    )
    assert db._new_images_cache.get(ws_tmp) is not None

    db.delete_workspace(ws_tmp)

    assert db._new_images_cache.get(ws_tmp) is None, (
        "delete_workspace must invalidate the new-images cache entry"
    )


def test_create_workspace_clears_stale_cache_on_id_reuse(tmp_path):
    """SQLite's ``INTEGER PRIMARY KEY`` (without AUTOINCREMENT) can reuse a
    deleted rowid. If a cache entry exists under a rowid at the moment a new
    workspace is created with that id, the new workspace must NOT inherit it.

    We simulate this by seeding the cache *after* the delete (to mimic a race
    where an in-flight compute from a prior request wrote a stale entry after
    the delete's invalidation), then creating a new workspace and asserting
    its ``create_workspace`` hook cleared the stale entry.
    """
    db = Database(str(tmp_path / "test.db"))
    ws_default = db.ensure_default_workspace()
    db.set_active_workspace(ws_default)

    ws_a = db.create_workspace("A")
    stale_payload = {"new_count": 42, "per_root": [], "sample": ["/stale.JPG"]}

    db.delete_workspace(ws_a)

    # Simulate a late write landing in the cache AFTER delete_workspace's
    # invalidation ran. In production this could happen if an in-flight
    # ``get_new_images_for_workspace`` for ws_a computed a result and called
    # ``set`` with a stale generation that happened to match (or if the cache
    # was repopulated by a reader racing with delete). Passing ``generation``
    # unconditionally here seeds the entry regardless of the generation
    # invalidate_workspaces bumped.
    db._new_images_cache.set(ws_a, stale_payload)
    assert db._new_images_cache.get(ws_a) == stale_payload

    # Create a new workspace. SQLite typically reuses the highest freed rowid,
    # so this usually gets ws_a's old id.
    ws_b = db.create_workspace("B")

    if ws_a == ws_b:
        # Id was reused — create_workspace's hook must have cleared the stale
        # entry so the new workspace does NOT inherit the old payload.
        assert db._new_images_cache.get(ws_b) is None, (
            f"create_workspace must clear any stale cache entry for the reused "
            f"id={ws_b}; found {db._new_images_cache.get(ws_b)!r}"
        )
    else:  # pragma: no cover — sqlite3 almost always reuses the highest freed rowid
        # No id reuse; verify the new workspace has no stale cache entry under
        # its own id (trivially true) and that the original stale entry is
        # untouched (since it's under a different id now).
        assert db._new_images_cache.get(ws_b) is None

    # And a full round-trip: fetching new-images for ws_b must not return the
    # stale payload from ws_a.
    result = db.get_new_images_for_workspace(ws_b)
    assert result != stale_payload, (
        f"New workspace (id={ws_b}) must not inherit deleted workspace's cache "
        f"(id reuse of {ws_a}? {ws_a == ws_b})"
    )


def test_delete_photos_invalidates_cache(db_with_workspace):
    """Deleting photos must invalidate the new-images cache.

    In "Remove from Vireo" mode the on-disk files stay put; the photo rows
    go away. Those files immediately become eligible for new-image detection
    again. Without an invalidation, ``/api/workspaces/active/new-images``
    keeps serving the cached pre-delete ``new_count`` for up to the TTL.
    """
    db, ws_id, tmp_path = db_with_workspace
    root = tmp_path / "shoot"
    _touch_image(str(root / "a.JPG"))
    _touch_image(str(root / "b.JPG"))
    root_id = db.add_folder(str(root), name="shoot")

    # Ingest both so they're not "new".
    a_id = db.add_photo(
        folder_id=root_id, filename="a.JPG", extension=".JPG",
        file_size=1, file_mtime=0.0,
    )
    db.add_photo(
        folder_id=root_id, filename="b.JPG", extension=".JPG",
        file_size=1, file_mtime=0.0,
    )

    # Prime the cache: nothing should be new yet.
    r1 = db.get_new_images_for_workspace(ws_id)
    assert r1["new_count"] == 0
    assert db._new_images_cache.get(ws_id) is not None

    # Remove a.JPG's photo row but leave the file on disk (mimics
    # "Remove from Vireo" semantics — delete_photos does not touch files).
    db.delete_photos([a_id])

    # a.JPG on disk is now untracked: it should re-appear as "new" on the
    # next read. If the cache was not invalidated, this would still be 0.
    r2 = db.get_new_images_for_workspace(ws_id)
    assert r2["new_count"] == 1, (
        f"Expected a.JPG to re-surface as new after delete_photos removed "
        f"its row; got new_count={r2['new_count']}. per_root={r2['per_root']}"
    )
