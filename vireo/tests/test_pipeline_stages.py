"""Boundaries and shared-state behavior of the extracted pipeline stages."""

import ast
import inspect
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import config as cfg
import pipeline_job
import pipeline_stages
from db import Database
from PIL import Image
from pipeline_stages import media
from test_pipeline_job import FakeRunner, _make_job


def test_stages_do_not_import_the_orchestrator():
    for path in Path(pipeline_stages.__file__).parent.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.append(node.module or "")
        assert "pipeline_job" not in imports, path.name


def test_orchestrator_does_not_define_processing_stages():
    tree = ast.parse(inspect.getsource(pipeline_job.run_pipeline_job))
    stages = {
        node.name for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name.endswith("_stage")
    }
    # This guard only rejects the retired import/archive mode; it does no work.
    assert stages <= {"archive_stage"}


def test_concurrent_runs_keep_their_created_collections(tmp_path, monkeypatch):
    """Later stages must read their own collection after both scans finish."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    db_path = str(tmp_path / "catalog.db")
    db = Database(db_path)
    workspaces = [db._active_workspace_id, db.create_workspace("Second run")]
    folders = []
    for index in range(2):
        folder = tmp_path / f"photos-{index}"
        folder.mkdir()
        Image.new("RGB", (32, 32), "black").save(folder / f"photo-{index}.jpg")
        folders.append(folder)

    previews_ready = threading.Barrier(2, timeout=20)
    previews = media.previews_stage
    observed = {}

    def synchronized_previews(run, **kwargs):
        # Force both collection stages to publish before either consumer reads.
        previews_ready.wait()
        observed[run.workspace_id] = run.collection_id
        return previews(run, **kwargs)

    monkeypatch.setattr(media, "previews_stage", synchronized_previews)

    def process(index):
        job = _make_job()
        job["id"] = f"pipeline-{index}"
        job["workspace_id"] = workspaces[index]
        params = pipeline_job.PipelineParams(
            source=str(folders[index]),
            skip_classify=True,
            skip_extract_masks=True,
            skip_regroup=True,
        )
        return pipeline_job.run_pipeline_job(
            job, FakeRunner(), db_path, workspaces[index], params,
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(process, range(2)))
        assert results[0]["collection_id"] != results[1]["collection_id"]
        for index, result in enumerate(results):
            workspace_id = workspaces[index]
            assert observed[workspace_id] == result["collection_id"]
            assert result["errors"] == []
            db.set_active_workspace(workspace_id)
            ids = db.get_collection_photo_ids(result["collection_id"])
            assert [db.get_photo(photo_id)["filename"] for photo_id in ids] == [
                f"photo-{index}.jpg",
            ]
    finally:
        db.close()


def test_on_scanned_photo_dedupes_companion_from_raw():
    """A RAW/JPEG pair that lives as one photo (the RAW's row plus the
    companion JPEG under the same owner id, reported by
    ``scanner._credit_known_companion``) must land in
    ``collected_photo_ids`` and the scan→thumb queue only once. Otherwise
    the thumbnail stage processes it twice (whichever path sorts first
    creates ``{owner_id}.jpg``, so the JPEG can bypass the normal
    RAW-first/fallback selection) and the generated pipeline collection
    carries duplicate ids."""
    from types import SimpleNamespace

    from pipeline_stages.scanning import _ScanPass

    enqueued = []
    collected = []
    scan_step = {"count": 0}
    updates = []

    run = SimpleNamespace(
        stages={"scan": scan_step},
        job={"id": "pipeline-1"},
        runner=SimpleNamespace(
            update_step=lambda job_id, step, **kw: updates.append(
                (job_id, step, kw),
            ),
        ),
    )

    scan = _ScanPass(
        run,
        sentinel=object(),
        filter_excluded=lambda *_a, **_k: None,
        find_broken_metadata_folders=lambda *_a, **_k: [],
        missing_archive_mount_root=lambda *_a, **_k: None,
        put_scan_item=enqueued.append,
        collected_photo_ids=collected,
        effective_thumb_cache_dir=None,
        effective_vireo_dir=None,
        final_destination=None,
        missing_originals_invalidator=None,
        remote_archive=None,
        skip_scan=False,
        snapshot_paths=None,
    )

    scan._on_scanned_photo(42, "/photos/IMG_001.cr3")
    scan._on_scanned_photo(42, "/photos/IMG_001.jpg")  # companion of RAW 42
    scan._on_scanned_photo(43, "/photos/IMG_002.jpg")

    assert collected == [42, 43]
    assert enqueued == [
        (42, "/photos/IMG_001.cr3"),
        (43, "/photos/IMG_002.jpg"),
    ]
    assert scan_step["count"] == 2


def test_on_scanned_photo_normalizes_companion_first_callback_to_raw_path(tmp_path):
    """When the companion JPEG's callback arrives *before* its RAW's own
    callback, the queued path still has to be the canonical RAW path.
    Dedup alone would queue the JPEG (first-callback-wins), which lets
    the thumbnail stage render the shared ``{owner_id}.jpg`` from the
    companion instead of the RAW, bypassing the normal RAW-first/fallback
    selection. Look the catalog's own filename up and reconstruct the
    RAW's path when the given callback path's basename differs."""
    from types import SimpleNamespace

    from pipeline_stages.scanning import _ScanPass

    enqueued = []
    collected = []
    scan_step = {"count": 0}

    run = SimpleNamespace(
        stages={"scan": scan_step},
        job={"id": "pipeline-1"},
        runner=SimpleNamespace(update_step=lambda *a, **k: None),
    )

    scan = _ScanPass(
        run,
        sentinel=object(),
        filter_excluded=lambda *_a, **_k: None,
        find_broken_metadata_folders=lambda *_a, **_k: [],
        missing_archive_mount_root=lambda *_a, **_k: None,
        put_scan_item=enqueued.append,
        collected_photo_ids=collected,
        effective_thumb_cache_dir=None,
        effective_vireo_dir=None,
        final_destination=None,
        missing_originals_invalidator=None,
        remote_archive=None,
        skip_scan=False,
        snapshot_paths=None,
    )

    # Give the pair real paths so the canonical-RAW existence check
    # (added for the "RAW missing, companion present" fallback below)
    # passes and the normalization runs.
    folder = tmp_path / "photos"
    folder.mkdir()
    (folder / "IMG_001.cr3").write_bytes(b"")
    (folder / "IMG_001.jpg").write_bytes(b"")

    scan.thread_db = SimpleNamespace(
        get_photo_filenames=lambda ids: {42: (7, "IMG_001.cr3")},
        get_folder=lambda folder_id: {"path": str(folder)},
    )

    # Companion JPEG arrives first (file ordering).
    scan._on_scanned_photo(42, str(folder / "IMG_001.jpg"))
    # RAW's own callback arrives second — dedupes.
    scan._on_scanned_photo(42, str(folder / "IMG_001.cr3"))

    assert collected == [42]
    # Must be the canonical RAW path even though the JPEG came first.
    # ``_canonical_photo_path`` reconstructs it via ``os.path.join``, so the
    # separator matches the host (``/`` on POSIX, ``\\`` on Windows); use
    # the same join rather than a hardcoded slash so the assertion is not
    # OS-specific.
    assert enqueued == [(42, os.path.join(str(folder), "IMG_001.cr3"))]
    assert scan_step["count"] == 1


def test_on_scanned_photo_keeps_companion_path_when_canonical_raw_missing(tmp_path):
    """When the catalog's RAW is unavailable (deleted or unreadable) but
    the companion JPEG is still on disk, the scanner's companion-credit
    callback is the pair's only report. ``_canonical_photo_path`` must
    keep the given companion path instead of rewriting it to the missing
    RAW: the thumbnail stage never loads ``detail_photo`` for a photo
    without an edit recipe (``_thumbnail_scanned_photo``), so a queued
    missing-RAW path has no path back to the companion from there and
    would be reported as a failed thumbnail despite the available JPEG."""
    from types import SimpleNamespace

    from pipeline_stages.scanning import _ScanPass

    enqueued = []
    collected = []
    scan_step = {"count": 0}

    run = SimpleNamespace(
        stages={"scan": scan_step},
        job={"id": "pipeline-1"},
        runner=SimpleNamespace(update_step=lambda *a, **k: None),
    )

    scan = _ScanPass(
        run,
        sentinel=object(),
        filter_excluded=lambda *_a, **_k: None,
        find_broken_metadata_folders=lambda *_a, **_k: [],
        missing_archive_mount_root=lambda *_a, **_k: None,
        put_scan_item=enqueued.append,
        collected_photo_ids=collected,
        effective_thumb_cache_dir=None,
        effective_vireo_dir=None,
        final_destination=None,
        missing_originals_invalidator=None,
        remote_archive=None,
        skip_scan=False,
        snapshot_paths=None,
    )

    # The companion JPEG is on disk, the catalog's RAW is not.
    folder = tmp_path / "photos"
    folder.mkdir()
    (folder / "IMG_001.jpg").write_bytes(b"")

    scan.thread_db = SimpleNamespace(
        get_photo_filenames=lambda ids: {42: (7, "IMG_001.cr3")},
        get_folder=lambda folder_id: {"path": str(folder)},
    )

    scan._on_scanned_photo(42, str(folder / "IMG_001.jpg"))

    assert collected == [42]
    # Companion path is kept: the thumbnail stage has an available file
    # to render, instead of a canonical RAW path that would fail to
    # decode with no fallback for a recipe-less photo.
    assert enqueued == [(42, str(folder / "IMG_001.jpg"))]
    assert scan_step["count"] == 1


def test_on_scanned_photo_leaves_matching_basename_alone():
    """When the given path's basename matches the catalog's filename, use
    it directly. Avoids a folder lookup on the common same-filename case
    (every non-companion scan callback) and keeps paths with trailing
    slashes or OS-specific separators untouched."""
    from types import SimpleNamespace

    from pipeline_stages.scanning import _ScanPass

    enqueued = []
    collected = []
    scan_step = {"count": 0}
    folder_lookups: list = []

    run = SimpleNamespace(
        stages={"scan": scan_step},
        job={"id": "pipeline-1"},
        runner=SimpleNamespace(update_step=lambda *a, **k: None),
    )

    scan = _ScanPass(
        run,
        sentinel=object(),
        filter_excluded=lambda *_a, **_k: None,
        find_broken_metadata_folders=lambda *_a, **_k: [],
        missing_archive_mount_root=lambda *_a, **_k: None,
        put_scan_item=enqueued.append,
        collected_photo_ids=collected,
        effective_thumb_cache_dir=None,
        effective_vireo_dir=None,
        final_destination=None,
        missing_originals_invalidator=None,
        remote_archive=None,
        skip_scan=False,
        snapshot_paths=None,
    )

    def record_folder(folder_id):
        folder_lookups.append(folder_id)
        return {"path": "/photos"}

    scan.thread_db = SimpleNamespace(
        get_photo_filenames=lambda ids: {42: (7, "IMG_001.cr3")},
        get_folder=record_folder,
    )

    scan._on_scanned_photo(42, "/root/elsewhere/IMG_001.cr3")

    assert collected == [42]
    assert enqueued == [(42, "/root/elsewhere/IMG_001.cr3")]
    # Basename matched; folder lookup is skipped.
    assert folder_lookups == []


def test_scan_resets_dedup_set_between_invocations():
    """SQLite reuses the ids of rows deleted by RAW/JPEG pairing (the
    transient JPEG row of a new RAW/JPEG pair whose RAW extension sorts
    before ``.jpg`` becomes the maximum rowid, then pairing deletes it
    after the JPEG's callback fired). ``_scan_in_place`` iterates sources
    with one ``do_scan`` call per source, so if the ``_reported_photo_ids``
    dedup set persisted across invocations, the next source's genuinely
    new photo would be dropped by dedup when SQLite reused the same id —
    it would appear in neither ``collected_photo_ids`` nor the thumbnail
    queue. ``_scan`` resets the dedup set per scanner invocation."""
    from types import SimpleNamespace

    from pipeline_stages.scanning import _ScanPass

    enqueued = []
    collected = []
    scan_step = {"count": 0}

    run = SimpleNamespace(
        stages={"scan": scan_step},
        job={"id": "pipeline-1"},
        runner=SimpleNamespace(update_step=lambda *a, **k: None),
        control=SimpleNamespace(
            should_abort=lambda _a: False,
            cancellation_requested=lambda: False,
        ),
        abort=SimpleNamespace(set=lambda: None, is_set=lambda: False),
    )

    scan = _ScanPass(
        run,
        sentinel=object(),
        filter_excluded=lambda *_a, **_k: None,
        find_broken_metadata_folders=lambda *_a, **_k: [],
        missing_archive_mount_root=lambda *_a, **_k: None,
        put_scan_item=enqueued.append,
        collected_photo_ids=collected,
        effective_thumb_cache_dir=None,
        effective_vireo_dir=None,
        final_destination=None,
        missing_originals_invalidator=None,
        remote_archive=None,
        skip_scan=False,
        snapshot_paths=None,
    )
    scan.pipeline_cfg = {}
    scan.thread_db = SimpleNamespace(
        get_photo_filenames=lambda ids: {},
        get_folder=lambda folder_id: None,
    )

    def do_scan_a(root, db, **kwargs):
        kwargs["photo_callback"](42, "/A/IMG_001.jpg")
    def do_scan_b(root, db, **kwargs):
        # SQLite reuses id 42 for a new photo in the next source.
        kwargs["photo_callback"](42, "/B/IMG_002.jpg")

    scan.do_scan = do_scan_a
    scan._scan("/A", photo_callback=scan._on_scanned_photo)
    scan.do_scan = do_scan_b
    scan._scan("/B", photo_callback=scan._on_scanned_photo)

    assert collected == [42, 42]
    assert enqueued == [
        (42, "/A/IMG_001.jpg"),
        (42, "/B/IMG_002.jpg"),
    ]
    assert scan_step["count"] == 2


def _make_thumb_pass(tmp_path):
    """Build a minimally-wired _ThumbPass for direct method unit tests.

    Returns the pass with its thread_db / cache_dir / generate_thumbnail
    unassigned — each test sets what it needs.
    """
    from types import SimpleNamespace

    from pipeline_stages.media import _ThumbPass

    run = SimpleNamespace(
        stages={"thumbnails": {}},
        job={"id": "pipeline-1", "_start_time": 0.0},
        runner=SimpleNamespace(update_step=lambda *a, **k: None),
        control=SimpleNamespace(
            should_abort=lambda _a: False,
            cancellation_requested=lambda: False,
            pause_checkpoint=lambda: None,
        ),
        abort=SimpleNamespace(set=lambda: None, is_set=lambda: False),
        emit_progress=lambda *a, **k: None,
    )
    thumbs = _ThumbPass(
        run,
        raw_extensions={".cr3", ".nef", ".arw"},
        sentinel=object(),
        filter_excluded=lambda *_a, **_k: None,
        recipe_render_source=None,
        retry_thumbnail_with_companion=lambda *_a, **_k: None,
        retry_thumbnail_with_working_copy=lambda *_a, **_k: None,
        thumb_min_source_size_kwargs=lambda *_a, **_k: {},
        thumb_raw_decode_kwargs=lambda *_a, **_k: {},
        effective_thumb_cache_dir=str(tmp_path),
        effective_vireo_dir=str(tmp_path),
        scan_to_thumb=None,
    )
    thumbs.thumb_size = 300
    thumbs.cache_dir = str(tmp_path)
    return thumbs


class _RowLike:
    """Stand-in for ``sqlite3.Row``: subscript access only, no ``dict.get``.

    ``Database.get_folder`` returns a real ``sqlite3.Row``, so test
    mocks that return plain ``dict`` instances hide a production-only
    ``AttributeError`` on ``.get()``. Using this fake in every
    ``_still_owns`` test shape makes the mocks fail the same way the
    real DB would if the production code ever reaches for ``.get``.
    """

    def __init__(self, **cols):
        self._cols = cols

    def __getitem__(self, key):
        return self._cols[key]

    def keys(self):
        return self._cols.keys()


def test_thumbnail_skips_stale_queue_entry_after_id_reuse(tmp_path):
    """A transient JPEG row inserted in one scanner invocation is deleted
    by RAW/JPEG pairing at the end of that pass, but its ``(id,
    companion_path)`` queue entry can still be waiting for the thumbnail
    worker. If the next ``_scan_in_place`` iteration inserts an unrelated
    photo under that reused SQLite id before the drain reaches the stale
    entry, caching ``{id}.jpg`` from the companion's bytes would pin the
    deleted companion's pixels under the new photo's id — the new row's
    own queue entry would then see the cache file already present and
    skip. ``_thumbnail_scanned_photo`` must re-resolve the catalog's
    canonical ownership for the id at drain time and skip a queue entry
    whose path no longer names the current row.
    """
    from types import SimpleNamespace

    from pipeline_stages.media import _ThumbPhoto

    thumbs = _make_thumb_pass(tmp_path)

    def _generate_must_not_run(*_a, **_k):
        raise AssertionError(
            "generate_thumbnail must not run for a stale queue entry"
        )

    # The catalog's row at id 42 now names a different file in a
    # different folder: the transient JPEG that was queued under id 42
    # has been deleted and the id has been reused for a new photo.
    thumbs.thread_db = SimpleNamespace(
        get_photos_by_ids=lambda ids: (
            {42: _RowLike(folder_id=99, filename="IMG_002.jpg", companion_path=None)}
            if 42 in ids else {}
        ),
        get_folder=lambda fid: _RowLike(id=fid, path="/B") if fid == 99 else None,
        get_photo_edit_recipe=lambda _id: None,
    )
    thumbs.generate_thumbnail = _generate_must_not_run

    stale = _ThumbPhoto(photo_id=42, photo_path="/A/IMG_001.jpg")
    assert thumbs._thumbnail_scanned_photo(stale) is False
    # No cache file was created under the reused id.
    assert not os.path.exists(os.path.join(str(tmp_path), "42.jpg"))
    # Neither the generated nor skipped counter advanced; the stale
    # entry was discarded without touching the cache.
    assert thumbs.generated == 0
    assert thumbs.skipped == 0
    assert thumbs.failed == 0

    # Row entirely absent from the catalog (deleted but not yet reused)
    # is also skipped.
    thumbs.thread_db = SimpleNamespace(
        get_photos_by_ids=lambda ids: {},
        get_folder=lambda _fid: None,
        get_photo_edit_recipe=lambda _id: None,
    )
    orphan = _ThumbPhoto(photo_id=99, photo_path="/anywhere/anything.jpg")
    assert thumbs._thumbnail_scanned_photo(orphan) is False
    assert thumbs.generated == 0 and thumbs.skipped == 0 and thumbs.failed == 0

    # The entry whose ownership DOES match the current catalog row is
    # not short-circuited by the guard: with no cache file on disk,
    # no edit recipe, and generate_thumbnail returning a real path,
    # the function proceeds and increments the generated tally.
    generated_path = os.path.join(str(tmp_path), "42.jpg")
    thumbs.thread_db = SimpleNamespace(
        get_photos_by_ids=lambda ids: (
            {42: _RowLike(folder_id=99, filename="IMG_002.jpg", companion_path=None)}
            if 42 in ids else {}
        ),
        get_folder=lambda fid: _RowLike(id=fid, path="/B") if fid == 99 else None,
        get_photo_edit_recipe=lambda _id: None,
    )
    thumbs.generate_thumbnail = lambda *_a, **_k: generated_path
    live = _ThumbPhoto(photo_id=42, photo_path="/B/IMG_002.jpg")
    assert thumbs._thumbnail_scanned_photo(live) is True
    assert thumbs.generated == 1
    assert thumbs.skipped == 0
    assert thumbs.failed == 0


def test_thumbnail_skips_queue_entry_when_folder_differs(tmp_path):
    """Basename equality alone does not identify the owner: the catalog
    row at a reused id can happen to share the queued entry's basename
    while living in a different folder (two photos coincidentally named
    IMG_001.jpg in different directories). ``_thumbnail_scanned_photo``
    must compare folder + filename, not basename alone — otherwise the
    stale ``/A/IMG_001.jpg`` entry would be treated as valid for a row
    the catalog now locates at ``/B/IMG_001.jpg``.
    """
    from types import SimpleNamespace

    from pipeline_stages.media import _ThumbPhoto

    thumbs = _make_thumb_pass(tmp_path)

    def _generate_must_not_run(*_a, **_k):
        raise AssertionError(
            "generate_thumbnail must not run when the folder differs"
        )

    thumbs.generate_thumbnail = _generate_must_not_run
    # Catalog says id 42 is /B/IMG_001.jpg (folder B, same filename).
    thumbs.thread_db = SimpleNamespace(
        get_photos_by_ids=lambda ids: (
            {42: _RowLike(folder_id=7, filename="IMG_001.jpg", companion_path=None)}
            if 42 in ids else {}
        ),
        get_folder=lambda fid: _RowLike(id=fid, path="/B") if fid == 7 else None,
        get_photo_edit_recipe=lambda _id: None,
    )
    # Queue entry is /A/IMG_001.jpg (same basename, folder A).
    stale = _ThumbPhoto(photo_id=42, photo_path="/A/IMG_001.jpg")
    assert thumbs._thumbnail_scanned_photo(stale) is False
    assert not os.path.exists(os.path.join(str(tmp_path), "42.jpg"))
    assert thumbs.generated == 0 and thumbs.skipped == 0 and thumbs.failed == 0


def test_thumbnail_discards_cache_when_ownership_changes_during_generate(tmp_path):
    """Ownership can change DURING ``generate_thumbnail``: the pre-check
    passes while the row is still the transient JPEG's, then pairing
    deletes that row and the next scanner invocation inserts an
    unrelated photo under the same reused id — all before generate
    publishes ``{id}.jpg``. The published cache file now holds the stale
    companion's pixels under the new photo's id. The post-generation
    re-check must detect this and delete the written cache file so the
    new row's own queue entry regenerates from the correct source.
    """
    from types import SimpleNamespace

    from pipeline_stages.media import _ThumbPhoto

    thumbs = _make_thumb_pass(tmp_path)

    # The ownership query flips between calls: pre-check sees the
    # original transient row (owns the stale path), generate_thumbnail
    # runs and writes the cache file, then the post-check sees the row
    # replaced under the reused id by an unrelated photo in another
    # folder with a different filename.
    ownership_states = iter([
        # Pre-check: the row still names the queued path.
        {42: _RowLike(folder_id=7, filename="IMG_001.jpg", companion_path=None)},
        # Post-check: the row now names a different photo in a
        # different folder.
        {42: _RowLike(folder_id=99, filename="DIFFERENT.jpg", companion_path=None)},
    ])

    def get_photos_by_ids(ids):
        return next(ownership_states) if 42 in ids else {}

    def get_folder(fid):
        if fid == 7:
            return _RowLike(id=7, path="/A")
        if fid == 99:
            return _RowLike(id=99, path="/B")
        return None

    thumbs.thread_db = SimpleNamespace(
        get_photos_by_ids=get_photos_by_ids,
        get_folder=get_folder,
        get_photo_edit_recipe=lambda _id: None,
    )

    # generate_thumbnail writes the (now-wrong) cache file and returns
    # its path, mimicking the real published location.
    published_path = os.path.join(str(tmp_path), "42.jpg")

    def fake_generate(*_a, **_k):
        Path(published_path).write_bytes(b"stale-companion-bytes")
        return published_path

    thumbs.generate_thumbnail = fake_generate

    entry = _ThumbPhoto(photo_id=42, photo_path="/A/IMG_001.jpg")
    assert thumbs._thumbnail_scanned_photo(entry) is False
    # The published cache file was removed — the reused id does not
    # carry the stale companion's pixels forward.
    assert not os.path.exists(published_path)
    # No tally advanced; the new row's own queue entry will regenerate.
    assert thumbs.generated == 0 and thumbs.skipped == 0 and thumbs.failed == 0


def test_thumbnail_still_owns_works_with_real_sqlite_row_folders(tmp_path):
    """``Database.get_folder`` returns a real ``sqlite3.Row``, not a dict.
    ``_still_owns`` must use subscript access rather than ``dict.get``
    — reaching for ``folder.get("path")`` would raise AttributeError
    on every live thumbnail and fail the entire scanned queue. Drive
    the guard with an on-disk SQLite database so the row behaves like
    the production one, not the mocked dicts the other tests use.
    """
    import sqlite3
    from types import SimpleNamespace

    from pipeline_stages.media import _ThumbPhoto

    conn = sqlite3.connect(str(tmp_path / "catalog.db"))
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE folders (id INTEGER PRIMARY KEY, path TEXT)")
    conn.execute("INSERT INTO folders (id, path) VALUES (?, ?)", (99, "/B"))
    conn.commit()

    def get_folder(fid):
        return conn.execute(
            "SELECT id, path FROM folders WHERE id = ?", (fid,),
        ).fetchone()

    thumbs = _make_thumb_pass(tmp_path)
    thumbs.thread_db = SimpleNamespace(
        get_photos_by_ids=lambda ids: (
            {42: _RowLike(folder_id=99, filename="IMG_002.jpg", companion_path=None)}
            if 42 in ids else {}
        ),
        get_folder=get_folder,
        get_photo_edit_recipe=lambda _id: None,
    )
    generated_path = os.path.join(str(tmp_path), "42.jpg")
    thumbs.generate_thumbnail = lambda *_a, **_k: generated_path

    live = _ThumbPhoto(photo_id=42, photo_path="/B/IMG_002.jpg")
    # The guard must traverse the real sqlite3.Row without raising:
    # a bare ``folder.get("path")`` call would blow up here.
    assert thumbs._thumbnail_scanned_photo(live) is True
    assert thumbs.generated == 1


def test_thumbnail_post_check_uses_canonical_queued_path_not_mutated_render_source(tmp_path):
    """The retry paths (``_resolve_recipe_source``,
    ``_retry_after_working_copy_eviction``) rewrite ``thumb.photo_path``
    to a companion or working-copy render source. The post-generation
    ownership re-check must still compare against the ORIGINAL queued
    (owner) path — validating against the mutated render source
    (e.g. a RAW's companion JPEG that lives next to the RAW but has
    its own basename) would delete every valid thumbnail whenever a
    retry path produced the pixels.
    """
    from types import SimpleNamespace

    from pipeline_stages.media import _ThumbPhoto

    thumbs = _make_thumb_pass(tmp_path)

    # Catalog says id 42 is /B/IMG_001.cr3 (a RAW) with no companion.
    # The queued path is the RAW's canonical path; a retry path will
    # rewrite ``thumb.photo_path`` to a working-copy JPEG that lives
    # under a completely different name (``/working-copy/42.jpg``).
    thumbs.thread_db = SimpleNamespace(
        get_photos_by_ids=lambda ids: (
            {42: _RowLike(folder_id=99, filename="IMG_001.cr3", companion_path=None)}
            if 42 in ids else {}
        ),
        get_folder=lambda fid: (
            _RowLike(id=99, path="/B") if fid == 99 else None
        ),
        get_photo_edit_recipe=lambda _id: None,
    )
    generated_path = os.path.join(str(tmp_path), "42.jpg")

    def generate_and_mutate(*_a, **_k):
        # Simulate a retry-path mutation: the working-copy is used as
        # the actual render source. If the post-check compared
        # against thumb.photo_path, ``/working-copy/42.jpg`` would
        # not match the catalog's ``/B/IMG_001.cr3`` (different
        # folder, different basename, not the companion either) and
        # the valid thumbnail would be deleted.
        entry.photo_path = "/working-copy/42.jpg"
        return generated_path

    thumbs.generate_thumbnail = generate_and_mutate

    entry = _ThumbPhoto(photo_id=42, photo_path="/B/IMG_001.cr3")
    assert thumbs._thumbnail_scanned_photo(entry) is True
    # The published cache file survived — the post-check validated
    # against the ORIGINAL /B/IMG_001.cr3, not the mutated
    # /working-copy/42.jpg.
    assert thumbs.generated == 1
    assert thumbs.failed == 0


def test_thumbnail_accepts_queued_companion_path_when_canonical_raw_is_missing(tmp_path):
    """``_canonical_photo_path`` keeps the companion JPEG path (rather
    than rewriting to the owner's canonical RAW path) when the RAW is
    not on disk, so ``_ThumbPass`` can still generate a thumbnail from
    the available file. ``_still_owns`` must therefore accept EITHER
    the owner's canonical path OR the owner's ``companion_path`` as a
    valid queued path — a strict owner-filename check would silently
    discard every missing-RAW/companion-present pair, exactly the
    regression ``dc37f0ac`` was meant to prevent.
    """
    from types import SimpleNamespace

    from pipeline_stages.media import _ThumbPhoto

    thumbs = _make_thumb_pass(tmp_path)
    # Owner at id 42 is a RAW with a paired JPEG companion. The queue
    # entry carries the companion's full path (RAW is missing on disk).
    thumbs.thread_db = SimpleNamespace(
        get_photos_by_ids=lambda ids: (
            {42: _RowLike(
                folder_id=99,
                filename="IMG_001.cr3",
                companion_path="IMG_001.jpg",
            )}
            if 42 in ids else {}
        ),
        get_folder=lambda fid: _RowLike(id=fid, path="/B") if fid == 99 else None,
        get_photo_edit_recipe=lambda _id: None,
    )
    generated_path = os.path.join(str(tmp_path), "42.jpg")
    thumbs.generate_thumbnail = lambda *_a, **_k: generated_path

    missing_raw = _ThumbPhoto(photo_id=42, photo_path="/B/IMG_001.jpg")
    assert thumbs._thumbnail_scanned_photo(missing_raw) is True
    assert thumbs.generated == 1
    assert thumbs.failed == 0

    # A path that is neither the owner's filename nor its
    # companion_path is still rejected — the acceptance stays scoped
    # to what the catalog actually ties to this id.
    thumbs = _make_thumb_pass(tmp_path)
    thumbs.thread_db = SimpleNamespace(
        get_photos_by_ids=lambda ids: (
            {42: _RowLike(
                folder_id=99,
                filename="IMG_001.cr3",
                companion_path="IMG_001.jpg",
            )}
            if 42 in ids else {}
        ),
        get_folder=lambda fid: _RowLike(id=fid, path="/B") if fid == 99 else None,
        get_photo_edit_recipe=lambda _id: None,
    )

    def _must_not_run(*_a, **_k):
        raise AssertionError(
            "generate_thumbnail must not run for a third-party path"
        )

    thumbs.generate_thumbnail = _must_not_run
    bogus = _ThumbPhoto(photo_id=42, photo_path="/B/UNRELATED.jpg")
    assert thumbs._thumbnail_scanned_photo(bogus) is False
    assert thumbs.generated == 0 and thumbs.failed == 0
