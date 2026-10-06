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


def test_on_scanned_photo_normalizes_companion_first_callback_to_raw_path():
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

    # Mock thread_db to resolve photo 42 as /photos/IMG_001.cr3.
    scan.thread_db = SimpleNamespace(
        get_photo_filenames=lambda ids: {42: (7, "IMG_001.cr3")},
        get_folder=lambda folder_id: {"path": "/photos"},
    )

    # Companion JPEG arrives first (file ordering).
    scan._on_scanned_photo(42, "/photos/IMG_001.jpg")
    # RAW's own callback arrives second — dedupes.
    scan._on_scanned_photo(42, "/photos/IMG_001.cr3")

    assert collected == [42]
    # Must be the canonical RAW path even though the JPEG came first.
    # ``_canonical_photo_path`` reconstructs it via ``os.path.join``, so the
    # separator matches the host (``/`` on POSIX, ``\\`` on Windows); use
    # the same join rather than a hardcoded slash so the assertion is not
    # OS-specific.
    assert enqueued == [(42, os.path.join("/photos", "IMG_001.cr3"))]
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
    canonical filename for the id at drain time and skip a queue entry
    whose basename no longer names the current row.
    """
    from types import SimpleNamespace

    from pipeline_stages.media import _ThumbPass, _ThumbPhoto

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

    def _generate_must_not_run(*_a, **_k):
        raise AssertionError(
            "generate_thumbnail must not run for a stale queue entry"
        )

    # The catalog's row at id 42 now names a different file in a
    # different folder: the transient JPEG that was queued under id 42
    # has been deleted and the id has been reused for a new photo.
    thumbs.thread_db = SimpleNamespace(
        get_photo_filenames=lambda ids: (
            {42: (99, "IMG_002.jpg")} if 42 in ids else {}
        ),
        get_photo_edit_recipe=lambda _id: None,
    )
    thumbs.cache_dir = str(tmp_path)
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

    # The entry whose basename DOES match the current catalog row is
    # not short-circuited by the stale-entry guard: with no cache file
    # on disk, no edit recipe, and generate_thumbnail returning a real
    # path, the function proceeds and increments the generated tally.
    generated_path = os.path.join(str(tmp_path), "42.jpg")
    thumbs.generate_thumbnail = lambda *_a, **_k: generated_path
    live = _ThumbPhoto(photo_id=42, photo_path="/B/IMG_002.jpg")
    assert thumbs._thumbnail_scanned_photo(live) is True
    assert thumbs.generated == 1
    assert thumbs.skipped == 0
    assert thumbs.failed == 0
