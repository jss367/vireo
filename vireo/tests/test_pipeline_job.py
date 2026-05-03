"""Tests for the streaming pipeline job orchestrator."""

import contextlib
import json
import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pipeline_job import (
    STAGE_WEIGHTS,
    PipelineParams,
    _stage_fraction,
    _weighted_progress,
    run_pipeline_job,
)


def _drop_jpeg(folder_path, filename):
    """Write a tiny valid JPEG at folder_path/filename so previews/thumbnails
    can load it. Tests that use db.add_photo need a matching file on disk now
    that missing files count as stage failures."""
    from PIL import Image
    path = os.path.join(folder_path, filename)
    Image.new("RGB", (16, 16), "black").save(path)
    return path


def _make_job():
    return {
        "id": "pipeline-test-1",
        "type": "pipeline",
        "status": "running",
        "started_at": "2026-01-01T00:00:00",
        "finished_at": None,
        "progress": {"current": 0, "total": 0, "current_file": ""},
        "result": None,
        "errors": [],
        "config": {},
        "workspace_id": 1,
    }


class FakeRunner:
    def __init__(self):
        self.events = []
        self.step_updates = []
        self.cancelled_ids = set()

    def push_event(self, job_id, event_type, data):
        self.events.append((job_id, event_type, data))

    def set_steps(self, job_id, steps):
        self.steps_defined = list(steps)

    def update_step(self, job_id, step_id, **kwargs):
        self.step_updates.append((job_id, step_id, kwargs))

    def is_cancelled(self, job_id):
        return job_id in self.cancelled_ids


def test_pipeline_params_has_skip_classify():
    """PipelineParams should support skip_classify flag."""
    params = PipelineParams(collection_id=1, skip_classify=True)
    assert params.skip_classify is True


def test_pipeline_params_skip_classify_defaults_false():
    params = PipelineParams(collection_id=1)
    assert params.skip_classify is False


def test_pipeline_params_has_skip_eye_keypoints():
    """PipelineParams should support skip_eye_keypoints flag."""
    params = PipelineParams(collection_id=1, skip_eye_keypoints=True)
    assert params.skip_eye_keypoints is True


def test_pipeline_params_skip_eye_keypoints_defaults_false():
    params = PipelineParams(collection_id=1)
    assert params.skip_eye_keypoints is False


def test_pipeline_params_has_preview_max_size():
    """PipelineParams should support preview_max_size."""
    params = PipelineParams(collection_id=1, preview_max_size=2560)
    assert params.preview_max_size == 2560


def test_pipeline_params_preview_max_size_defaults_1920():
    params = PipelineParams(collection_id=1)
    assert params.preview_max_size == 1920


def test_pipeline_params_sources_list():
    """PipelineParams should accept a list of source folders."""
    params = PipelineParams(sources=["/photos/card1", "/photos/card2"])
    assert params.sources == ["/photos/card1", "/photos/card2"]


def test_pipeline_params_sources_defaults_none():
    params = PipelineParams(collection_id=1)
    assert params.sources is None


def test_pipeline_params_defaults():
    """PipelineParams should have sensible defaults."""
    params = PipelineParams(collection_id=42)
    assert params.collection_id == 42
    assert params.source is None
    assert params.destination is None
    assert params.file_types == "both"
    assert params.folder_template == "%Y/%Y-%m-%d"
    assert params.skip_duplicates is True
    assert params.labels_file is None
    assert params.labels_files is None
    assert params.model_id is None
    assert params.reclassify is False
    assert params.skip_extract_masks is False
    assert params.skip_regroup is False
    assert params.sources is None
    assert params.skip_classify is False
    assert params.preview_max_size == 1920


def test_pipeline_params_all_fields():
    """PipelineParams should accept all fields."""
    params = PipelineParams(
        collection_id=1,
        source="/src",
        sources=["/src1", "/src2"],
        destination="/dst",
        file_types="raw",
        folder_template="%Y",
        skip_duplicates=False,
        labels_file="/labels.txt",
        labels_files=["/a.txt", "/b.txt"],
        model_id="bioclip-2",
        model_ids=["bioclip-2", "timm-inat21-eva02-l"],
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
        skip_classify=True,
        preview_max_size=2560,
    )
    assert params.source == "/src"
    assert params.sources == ["/src1", "/src2"]
    assert params.destination == "/dst"
    assert params.file_types == "raw"
    assert params.model_ids == ["bioclip-2", "timm-inat21-eva02-l"]
    assert params.reclassify is True
    assert params.skip_extract_masks is True
    assert params.skip_regroup is True
    assert params.skip_classify is True
    assert params.preview_max_size == 2560


def test_pipeline_params_model_ids_defaults_none():
    """model_ids defaults to None (single-model / back-compat path)."""
    params = PipelineParams(collection_id=1)
    assert params.model_ids is None


def test_pipeline_job_with_collection_skips_scan(tmp_path, monkeypatch):
    """When collection_id is provided, pipeline should skip scan entirely."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Create an empty collection so classify has something to query
    col_id = db.add_collection("Test", "[]")

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert isinstance(result, dict)
    # Should have stage results
    assert "stages" in result
    # Duration should be tracked
    assert "duration" in result
    assert result["duration"] >= 0
    # Scan should not have run (collection_id was provided)
    # Check that no scan events were emitted with phase "Scanning photos"
    scan_events = [
        e for e in runner.events
        if e[1] == "progress" and e[2].get("phase") == "Scanning photos"
    ]
    assert len(scan_events) == 0


def test_pipeline_abort_event_stops_stages():
    """Setting pipeline_abort should cause _should_abort to return True."""
    from pipeline_job import _should_abort

    abort = threading.Event()
    assert not _should_abort(abort)
    abort.set()
    assert _should_abort(abort)


def test_pipeline_cancel_via_runner_skips_remaining_stages(tmp_path, monkeypatch):
    """When runner.is_cancelled returns True, the pipeline watcher should set
    the local abort event, and remaining stages should bail without raising."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ["a.jpg", "b.jpg", "c.jpg"]:
        Image.new("RGB", (50, 50), "red").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    # Pre-cancel the job: the watcher thread should pick this up almost
    # immediately and set abort.
    runner.cancelled_ids.add(job["id"])

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert isinstance(result, dict)
    assert "duration" in result
    # The pipeline should return without raising. It may still have run scan
    # (no interruption hook in scanner), but classify/extract_masks/regroup
    # were skip_* anyway, so this just verifies graceful completion under
    # cancellation.


def test_pipeline_abort_on_nonexistent_source(tmp_path, monkeypatch):
    """Pipeline with nonexistent source should complete gracefully.

    The scanner silently returns for nonexistent dirs (no photos found).
    With skip_classify=True we bypass model_loader (no model in test env),
    so the pipeline finishes without raising. If any stage regresses into
    a real failure, the fail-propagation path in run_pipeline_job now
    raises, which also fails the test.
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(tmp_path / "nonexistent_dir"),
        destination=str(tmp_path / "dest"),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert isinstance(result, dict)
    assert "duration" in result
    # With skip_classify set, the scanner should handle the missing source
    # gracefully and end without error. If this regresses — i.e. a real stage
    # failure creeps in — the pipeline now raises, which also fails the test.


def test_pipeline_scan_thumbnail_collection_stages(tmp_path, monkeypatch):
    """Pipeline should scan photos, generate thumbnails, and create collection."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Create test images
    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ["a.jpg", "b.jpg", "c.jpg"]:
        img = Image.new("RGB", (100, 100), "red")
        img.save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert isinstance(result, dict)
    assert "stages" in result

    # Thumbnails should have been generated
    thumb_dir = os.path.join(os.path.dirname(db_path), "thumbnails")
    assert os.path.isdir(thumb_dir)
    thumb_result = result["stages"].get("thumbnails", {})
    assert thumb_result.get("generated", 0) == 3

    # A collection should have been created
    assert "collection_id" in result

    # Verify collection exists in DB
    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    photos = db2.get_collection_photos(result["collection_id"], per_page=999999)
    assert len(photos) == 3


def test_pipeline_stages_dict_in_progress_events(tmp_path, monkeypatch):
    """Progress events should include a 'stages' dict showing all stage statuses."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    img = Image.new("RGB", (100, 100), "red")
    img.save(str(photo_dir / "test.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Find progress events with stages dict
    stage_events = [
        e for e in runner.events
        if e[1] == "progress" and "stages" in e[2]
    ]
    assert len(stage_events) > 0

    # Each stages dict should have all expected stage keys
    expected_keys = {"ingest", "scan", "thumbnails", "previews", "model_loader", "classify", "extract_masks", "regroup"}
    for _, _, data in stage_events:
        assert expected_keys.issubset(data["stages"].keys())


# ---------------------------------------------------------------------------
# Integration tests — full pipeline end-to-end
# ---------------------------------------------------------------------------


def test_pipeline_scan_and_thumbnail_overlap(tmp_path, monkeypatch):
    """Scan and thumbnail stages should both process photos from a real dir."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Create 5 test images
    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for i in range(5):
        img = Image.new("RGB", (100, 100), "blue")
        img.save(str(photo_dir / f"photo_{i}.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # Result should have stages dict
    assert result is not None
    assert isinstance(result["stages"], dict)

    # Scan should have found photos — check via progress events or thumbnail count
    scan_events = [
        e for e in runner.events
        if isinstance(e[2], dict) and e[2].get("phase", "").startswith("Scanning")
    ]
    assert (
        len(scan_events) > 0
        or result["stages"].get("thumbnails", {}).get("generated", 0) > 0
    )

    # Thumbnails should have been generated on the filesystem
    thumb_dir = os.path.join(os.path.dirname(db_path), "thumbnails")
    assert os.path.isdir(thumb_dir)
    thumb_files = [f for f in os.listdir(thumb_dir) if not f.startswith(".")]
    assert len(thumb_files) == 5


def test_pipeline_skips_scan_with_collection_id(tmp_path, monkeypatch):
    """When collection_id is given, no scan-phase events should be emitted."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", json.dumps([]))

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert result is not None
    # No scan-phase events should have been emitted
    scan_events = [
        e for e in runner.events
        if isinstance(e[2], dict) and "Scanning" in e[2].get("phase", "")
    ]
    assert len(scan_events) == 0


def test_pipeline_nonexistent_source_scans_nothing(tmp_path, monkeypatch):
    """Pipeline with a nonexistent source should complete with 0 photos scanned."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source="/nonexistent/path/that/does/not/exist",
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert result is not None
    # No collection created since no photos were found
    assert result.get("collection_id") is None


def test_pipeline_result_has_duration(tmp_path, monkeypatch):
    """Pipeline result dict should always contain a positive duration."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Empty", json.dumps([]))

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert "duration" in result
    assert isinstance(result["duration"], float)
    assert result["duration"] >= 0


def test_pipeline_collection_created_after_scan(tmp_path, monkeypatch):
    """Pipeline should create a collection from scanned photos."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Create test images
    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ["bird1.jpg", "bird2.jpg", "bird3.jpg"]:
        img = Image.new("RGB", (80, 80), "green")
        img.save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # collection_id should be in the result
    assert "collection_id" in result
    assert isinstance(result["collection_id"], int)

    # Verify the collection exists in the DB and has the right photos
    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    photos = db2.get_collection_photos(result["collection_id"], per_page=999999)
    assert len(photos) == 3


def test_pipeline_previews_stage_runs(tmp_path, monkeypatch):
    """Pipeline should run a previews stage after thumbnails."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Create an empty collection so classify has something to query
    col_id = db.add_collection("Test", "[]")

    # Patch scanner, thumbnails, etc. to be no-ops — we just need to verify
    # the previews stage appears in the result
    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
        preview_max_size=1920,
    )

    runner = FakeRunner()
    job = _make_job()
    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # The stages dict in progress events should include "previews"
    stage_events = [e[2]["stages"] for e in runner.events
                    if e[1] == "progress" and "stages" in e[2]]
    assert any("previews" in s for s in stage_events), \
        "Expected 'previews' stage in progress events"


def test_pipeline_params_sources_used_over_source():
    """When sources is provided, it should take precedence over source."""
    params = PipelineParams(source="/single", sources=["/a", "/b"])
    assert params.sources == ["/a", "/b"]


def test_pipeline_skip_classify_skips_model_loader(tmp_path, monkeypatch):
    """When skip_classify=True, model_loader and classify should be skipped."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        collection_id=1,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # Check that classify was skipped in the last stages event
    last_stages = None
    for _, evt_type, data in reversed(runner.events):
        if evt_type == "progress" and "stages" in data:
            last_stages = data["stages"]
            break

    assert last_stages is not None
    assert last_stages["classify"]["status"] == "skipped"
    assert last_stages["model_loader"]["status"] == "skipped"


def test_pipeline_passes_recursive_false_to_scan(tmp_path, monkeypatch):
    """Pipeline forwards recursive=False to scanner.scan()."""
    import config as cfg
    from db import Database
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    src = tmp_path / "photos"
    src.mkdir()
    (src / "img.jpg").write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)

    scan_kwargs = {}

    def fake_scan(root, db_arg, **kwargs):
        scan_kwargs.update(kwargs)

    monkeypatch.setattr("scanner.scan", fake_scan)

    params = PipelineParams(
        source=str(src),
        recursive=False,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert scan_kwargs.get("recursive") is False


def test_pipeline_passes_vireo_dir_to_scan(tmp_path, monkeypatch):
    """Pipeline must forward vireo_dir to scanner.scan() so the
    content-change cache invalidation (thumbnail/working-copy/preview)
    actually fires for pipeline-triggered rescans.

    Without this, _invalidate_derived_caches short-circuits (guard:
    ``if not vireo_dir: return``) and the bird/squirrel divergence this
    PR fixes still occurs for anyone using the pipeline to scan.
    """
    import config as cfg
    from db import Database
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "vireo.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    src = tmp_path / "photos"
    src.mkdir()
    (src / "img.jpg").write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)

    scan_kwargs = {}

    def fake_scan(root, db_arg, **kwargs):
        scan_kwargs.update(kwargs)

    monkeypatch.setattr("scanner.scan", fake_scan)

    params = PipelineParams(
        source=str(src),
        recursive=False,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert scan_kwargs.get("vireo_dir") == os.path.dirname(db_path), (
        "Pipeline must pass vireo_dir (the DB's parent dir) to scan() so "
        "derived-cache invalidation is reachable on pipeline rescans."
    )


def test_pipeline_forwards_thumb_cache_dir_to_scan(tmp_path, monkeypatch):
    """Pipeline must forward the configured thumb_cache_dir to scanner.scan().

    ``--thumb-dir`` can point outside ``vireo_dir/thumbnails`` — scanner's
    invalidation now accepts a ``thumb_cache_dir`` override for exactly
    that reason. If pipeline scans drop it, the default fallback
    (``vireo_dir/thumbnails``) targets the wrong directory on custom
    layouts and stale thumbnails survive.
    """
    import config as cfg
    from db import Database
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "vireo.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    src = tmp_path / "photos"
    src.mkdir()
    (src / "img.jpg").write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)

    scan_kwargs = {}

    def fake_scan(root, db_arg, **kwargs):
        scan_kwargs.update(kwargs)

    monkeypatch.setattr("scanner.scan", fake_scan)

    custom_thumb_dir = str(tmp_path / "custom-thumbs")

    params = PipelineParams(
        source=str(src),
        recursive=False,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(
        job, runner, db_path, ws_id, params,
        thumb_cache_dir=custom_thumb_dir,
    )

    assert scan_kwargs.get("thumb_cache_dir") == custom_thumb_dir, (
        "Pipeline must thread the configured thumb_cache_dir to scan() "
        "so invalidation targets the real cache on custom --thumb-dir "
        "layouts."
    )


def test_pipeline_vireo_dir_aligns_with_thumb_cache_dir_parent(tmp_path, monkeypatch):
    """On custom --thumb-dir layouts, the Flask serve path computes
    ``vireo_dir = os.path.dirname(THUMB_CACHE_DIR)`` — that's where it
    reads previews/ and working/. Pipeline scans must align with that
    convention, or invalidation runs against one tree while the app
    serves from another (so stale previews/working_copies survive).
    """
    import config as cfg
    from db import Database
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # DB and thumb cache on *different* roots (simulates
    # --db ~/.vireo/vireo.db --thumb-dir /data/thumbs).
    db_dir = tmp_path / "dbstore"
    db_dir.mkdir()
    db_path = str(db_dir / "vireo.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    custom_thumb_dir = tmp_path / "cache" / "thumbs"
    custom_thumb_dir.mkdir(parents=True)

    src = tmp_path / "photos"
    src.mkdir()
    (src / "img.jpg").write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)

    scan_kwargs = {}

    def fake_scan(root, db_arg, **kwargs):
        scan_kwargs.update(kwargs)

    monkeypatch.setattr("scanner.scan", fake_scan)

    params = PipelineParams(
        source=str(src),
        recursive=False,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(
        job, runner, db_path, ws_id, params,
        thumb_cache_dir=str(custom_thumb_dir),
    )

    expected_vireo_dir = os.path.dirname(str(custom_thumb_dir))
    assert scan_kwargs.get("vireo_dir") == expected_vireo_dir, (
        f"Pipeline should derive vireo_dir from thumb_cache_dir's parent "
        f"({expected_vireo_dir}); got {scan_kwargs.get('vireo_dir')!r}. "
        "Otherwise scan's previews/working paths diverge from the "
        "Flask serve paths."
    )


def test_pipeline_scan_progress_includes_rate_and_eta(tmp_path, monkeypatch):
    """Scan progress events should include rate and eta_seconds fields."""
    import time

    import config as cfg
    from db import Database
    from jobs import JobRunner
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    from PIL import Image
    src = tmp_path / "photos"
    src.mkdir()
    for i in range(12):
        Image.new("RGB", (40, 40), "blue").save(str(src / f"img{i:02d}.jpg"))

    runner = JobRunner()
    progress_events = []
    orig_push = runner.push_event

    def capture_push(job_id, event_type, data):
        if event_type == "progress" and data.get("phase") == "Scanning photos":
            progress_events.append(data)
        orig_push(job_id, event_type, data)

    monkeypatch.setattr(runner, "push_event", capture_push)

    params = PipelineParams(
        source=str(src),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    job = {
        "id": "test-scan-rate",
        "type": "pipeline",
        "status": "running",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "finished_at": None,
        "progress": {"current": 0, "total": 0, "current_file": ""},
        "result": None,
        "errors": [],
        "config": {},
        "workspace_id": ws_id,
        "steps": [],
    }

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert len(progress_events) > 0, "Expected at least one scan progress event"
    last = progress_events[-1]
    assert "rate" in last, "Progress event should include rate"
    assert "eta_seconds" in last, "Progress event should include eta_seconds"
    assert isinstance(last["rate"], (int, float))
    assert isinstance(last["eta_seconds"], (int, float))


def test_pipeline_multi_folder_scan_progress_is_monotonic(tmp_path, monkeypatch):
    """Scan progress must not move backward at folder boundaries.

    When sources is a list of folders, pipeline_job loops calling scan()
    once per folder. Each scan() reports progress as local (current, total).
    The weighted overall bar reads stages["scan"]["count"]/.total, so if
    those get overwritten rather than accumulated, the UI progress jumps
    backward when folder N+1 starts.
    """
    import time

    import config as cfg
    from db import Database
    from jobs import JobRunner
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    from PIL import Image
    folder_a = tmp_path / "folderA"
    folder_a.mkdir()
    for i in range(6):
        Image.new("RGB", (40, 40), "blue").save(str(folder_a / f"a{i:02d}.jpg"))
    folder_b = tmp_path / "folderB"
    folder_b.mkdir()
    for i in range(6):
        Image.new("RGB", (40, 40), "red").save(str(folder_b / f"b{i:02d}.jpg"))

    runner = JobRunner()
    scan_counts = []
    scan_totals = []
    orig_push = runner.push_event

    def capture_push(job_id, event_type, data):
        if event_type == "progress":
            stages = data.get("stages") or {}
            scan_info = stages.get("scan") or {}
            if scan_info.get("status") == "running":
                scan_counts.append(scan_info.get("count") or 0)
                scan_totals.append(scan_info.get("total") or 0)
        orig_push(job_id, event_type, data)

    monkeypatch.setattr(runner, "push_event", capture_push)

    params = PipelineParams(
        sources=[str(folder_a), str(folder_b)],
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    job = {
        "id": "test-multi-scan-mono",
        "type": "pipeline",
        "status": "running",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "finished_at": None,
        "progress": {"current": 0, "total": 0, "current_file": ""},
        "result": None,
        "errors": [],
        "config": {},
        "workspace_id": ws_id,
        "steps": [],
    }

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert len(scan_counts) > 0, "Expected at least one running scan progress event"
    for i in range(1, len(scan_counts)):
        assert scan_counts[i] >= scan_counts[i - 1], (
            f"scan count moved backward: {scan_counts[i - 1]} -> "
            f"{scan_counts[i]} at event {i}; full sequence={scan_counts}"
        )
    for i in range(1, len(scan_totals)):
        assert scan_totals[i] >= scan_totals[i - 1], (
            f"scan total moved backward: {scan_totals[i - 1]} -> "
            f"{scan_totals[i]} at event {i}; full sequence={scan_totals}"
        )
    assert scan_totals[-1] >= 12, (
        f"final scan total should cover both folders (>=12), got {scan_totals[-1]}"
    )


def test_pipeline_multi_source_ingest_progress_is_monotonic(tmp_path, monkeypatch):
    """Ingest progress must not move backward at source folder boundaries.

    Copy mode with sources=[folderA, folderB] calls do_ingest() once per
    folder. Each call reports (current, total) local to that folder. The
    weighted overall bar reads stages["ingest"]["count"]/.total, so if
    those get overwritten rather than accumulated, overall progress
    rewinds each time a new source starts — the exact regression the
    scan accumulator already prevents. Same treatment needed for ingest.
    """
    import time

    import config as cfg
    from db import Database
    from jobs import JobRunner
    from pipeline_job import PipelineParams, run_pipeline_job

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    from PIL import Image
    src_a = tmp_path / "srcA"
    src_a.mkdir()
    for i in range(5):
        Image.new("RGB", (40, 40), "blue").save(str(src_a / f"a{i:02d}.jpg"))
    src_b = tmp_path / "srcB"
    src_b.mkdir()
    for i in range(5):
        Image.new("RGB", (40, 40), "red").save(str(src_b / f"b{i:02d}.jpg"))
    dest = tmp_path / "dest"
    dest.mkdir()

    runner = JobRunner()
    ingest_counts = []
    ingest_totals = []
    orig_push = runner.push_event

    def capture_push(job_id, event_type, data):
        if event_type == "progress":
            stages = data.get("stages") or {}
            ingest_info = stages.get("ingest") or {}
            if ingest_info.get("status") == "running":
                ingest_counts.append(ingest_info.get("count") or 0)
                ingest_totals.append(ingest_info.get("total") or 0)
        orig_push(job_id, event_type, data)

    monkeypatch.setattr(runner, "push_event", capture_push)

    params = PipelineParams(
        sources=[str(src_a), str(src_b)],
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    job = {
        "id": "test-multi-ingest-mono",
        "type": "pipeline",
        "status": "running",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "finished_at": None,
        "progress": {"current": 0, "total": 0, "current_file": ""},
        "result": None,
        "errors": [],
        "config": {},
        "workspace_id": ws_id,
        "steps": [],
    }

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert len(ingest_counts) > 0, "Expected at least one running ingest progress event"
    for i in range(1, len(ingest_counts)):
        assert ingest_counts[i] >= ingest_counts[i - 1], (
            f"ingest count moved backward: {ingest_counts[i - 1]} -> "
            f"{ingest_counts[i]} at event {i}; full sequence={ingest_counts}"
        )
    for i in range(1, len(ingest_totals)):
        assert ingest_totals[i] >= ingest_totals[i - 1], (
            f"ingest total moved backward: {ingest_totals[i - 1]} -> "
            f"{ingest_totals[i]} at event {i}; full sequence={ingest_totals}"
        )
    assert ingest_totals[-1] >= 10, (
        f"final ingest total should cover both sources (>=10), got {ingest_totals[-1]}"
    )


def test_progress_lock_held_during_update_stages_push():
    """`_update_stages` must call `push_event` while holding `_progress_lock`.

    The lock makes (snapshot stages, append event) atomic across pipeline
    threads. Without it, a thread can build the stages snapshot, get
    preempted between the dict comprehension and the push_event call, and
    finally land its stale snapshot after another thread has already
    pushed events with newer counts — producing a non-monotonic captured
    sequence (the trailing stale `5` after `10` that flaked
    test_pipeline_multi_source_ingest_progress_is_monotonic on CI).
    """
    import pipeline_job as pj

    seen = []

    class TrackingRunner:
        def push_event(self, job_id, event_type, data):
            seen.append(pj._progress_lock.locked())

    stages = {
        "ingest": {
            "count": 0, "status": "running", "weight": 1.0, "label": "Ingest",
        },
    }
    pj._update_stages(TrackingRunner(), "job-1", stages)

    assert seen == [True], (
        "_update_stages must call push_event while holding _progress_lock; "
        f"got lock-held sequence {seen}"
    )


def test_emit_progress_lock_held_during_push():
    """The `_emit_progress` helper (used by per-stage cb pushes) must take
    the same lock so its snapshot+push is atomic with `_update_stages`."""
    import pipeline_job as pj

    seen = []

    class TrackingRunner:
        def push_event(self, job_id, event_type, data):
            seen.append(pj._progress_lock.locked())

    stages = {
        "ingest": {
            "count": 5, "status": "running", "weight": 1.0, "label": "Ingest",
        },
    }
    pj._emit_progress(TrackingRunner(), "job-1", stages, "ingest", "Importing")

    assert seen == [True], (
        "_emit_progress must call push_event while holding _progress_lock; "
        f"got lock-held sequence {seen}"
    )


def test_pipeline_ingest_updates_step_progress(tmp_path, monkeypatch):
    """Ingest (import) phase should call update_step so the jobs page shows progress."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Create source images
    src = tmp_path / "source"
    src.mkdir()
    for name in ["a.jpg", "b.jpg"]:
        img = Image.new("RGB", (100, 100), "red")
        img.save(str(src / name))

    dest = tmp_path / "dest"
    dest.mkdir()

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(src),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # The ingest step should have received update_step calls with progress
    ingest_progress_updates = [
        (step_id, kwargs) for _, step_id, kwargs in runner.step_updates
        if step_id == "ingest" and "progress" in kwargs
        and kwargs["progress"].get("total", 0) > 0
    ]
    assert len(ingest_progress_updates) > 0, \
        "Ingest phase should call update_step with progress for the ingest step"

    # Ingest step should have been marked completed
    ingest_completed = [
        kwargs for _, step_id, kwargs in runner.step_updates
        if step_id == "ingest" and kwargs.get("status") == "completed"
    ]
    assert len(ingest_completed) > 0, \
        "Ingest step should be marked completed after import finishes"


def test_pipeline_scan_step_gets_status_updates(tmp_path, monkeypatch):
    """Scanner should report status messages (e.g. 'Discovering files...')
    via update_step current_file during blocking phases."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    img = Image.new("RGB", (100, 100), "red")
    img.save(str(photo_dir / "test.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Scanner should have sent a "Discovering files..." status via update_step
    scan_status_messages = [
        kwargs.get("current_file", "")
        for _, step_id, kwargs in runner.step_updates
        if step_id == "scan" and "current_file" in kwargs
    ]
    assert any("Discovering" in msg for msg in scan_status_messages), \
        f"Expected 'Discovering files...' status update, got: {scan_status_messages[:5]}"

    # Status updates should also emit SSE progress events for real-time subscribers
    status_sse_events = [
        e[2] for e in runner.events
        if e[1] == "progress" and "Discovering" in e[2].get("phase", "")
    ]
    assert len(status_sse_events) > 0, \
        "Status updates should also push SSE progress events"


def test_pipeline_ingest_step_present_only_with_destination(tmp_path, monkeypatch):
    """The 'ingest' step should only appear in step_defs when destination is set."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    img = Image.new("RGB", (100, 100), "red")
    img.save(str(photo_dir / "test.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Without destination — no ingest step
    runner_no_dest = FakeRunner()
    job = _make_job()
    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    run_pipeline_job(job, runner_no_dest, db_path, ws_id, params)
    step_ids = [s["id"] for s in runner_no_dest.steps_defined]
    assert "ingest" not in step_ids, "ingest step should not appear without destination"

    # With destination — ingest step present
    dest = tmp_path / "dest"
    dest.mkdir()
    runner_dest = FakeRunner()
    job2 = _make_job()
    params2 = PipelineParams(
        source=str(photo_dir),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    run_pipeline_job(job2, runner_dest, db_path, ws_id, params2)
    step_ids2 = [s["id"] for s in runner_dest.steps_defined]
    assert "ingest" in step_ids2, "ingest step should appear when destination is set"
    assert step_ids2.index("ingest") < step_ids2.index("scan"), \
        "ingest step should come before scan"


def test_pipeline_all_duplicates_restricts_scan_to_existing_folders(tmp_path, monkeypatch):
    """When every source file is a duplicate of an existing photo in the DB,
    the scan phase must be restricted to just the folders that hold those
    existing duplicates — not left with restrict_dirs=None, which makes the
    scanner walk the entire destination tree.

    Regression test: user selects N photos from an SD card that have already
    been imported, clicks pipeline, and expects those photos to become linked
    to their current workspace. With restrict_dirs=None the scan either takes
    far too long (17+ minutes for a 50k-file library) or skips folder linking
    entirely for the workspaces the user cares about.
    """
    import shutil

    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Destination tree with two populated date folders plus an unrelated
    # folder that should NOT be walked by the restricted scan.
    dest = tmp_path / "dest"
    duplicate_home = dest / "2024" / "2024-06-15"
    duplicate_home.mkdir(parents=True)
    unrelated = dest / "2023" / "2023-01-01"
    unrelated.mkdir(parents=True)
    for i in range(2):
        Image.new("RGB", (100, 100), (i * 80, 50, 50)).save(
            str(duplicate_home / f"dup_{i}.jpg")
        )
    Image.new("RGB", (100, 100), "blue").save(str(unrelated / "unrelated.jpg"))

    # Scan so the existing photos land in the DB with their hashes.
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    from scanner import scan as do_scan
    do_scan(str(dest), db)

    # Source is a fresh directory containing byte-identical copies of the
    # duplicates (same hashes), simulating an SD card that still has the
    # already-imported photos on it.
    src = tmp_path / "source"
    src.mkdir()
    for i in range(2):
        shutil.copy2(
            str(duplicate_home / f"dup_{i}.jpg"),
            str(src / f"dup_{i}.jpg"),
        )

    params = PipelineParams(
        source=str(src),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()

    scan_calls = []
    from unittest.mock import patch

    import scanner as scanner_mod
    original_scan = scanner_mod.scan

    def tracking_scan(root, *args, **kwargs):
        scan_calls.append({
            "root": str(root),
            "restrict_dirs": kwargs.get("restrict_dirs"),
        })
        return original_scan(root, *args, **kwargs)

    with patch.object(scanner_mod, "scan", tracking_scan):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # Find the pipeline's scan-stage call (root == destination).
    pipeline_scans = [c for c in scan_calls if c["root"] == str(dest)]
    assert pipeline_scans, \
        f"Pipeline did not call scan on destination; calls={scan_calls}"
    call = pipeline_scans[-1]

    restrict = call["restrict_dirs"]
    assert restrict is not None, (
        "When every file is a duplicate, pipeline should restrict the scan "
        "to the existing-duplicates' folders instead of walking the entire "
        "destination tree (restrict_dirs=None)."
    )
    restrict_set = set(restrict)
    assert str(duplicate_home) in restrict_set, (
        f"Expected {duplicate_home!r} in restrict_dirs; got {restrict_set!r}"
    )
    assert str(unrelated) not in restrict_set, (
        f"Unrelated folder {unrelated!r} must not be in restrict_dirs; "
        f"got {restrict_set!r}"
    )


def test_pipeline_all_duplicates_links_existing_folders_to_workspace(tmp_path, monkeypatch):
    """When every source file is a duplicate, the folders holding those
    existing duplicates should end up linked to the active workspace after
    the pipeline runs — even if the workspace had no folders linked before.
    """
    import shutil

    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    dest = tmp_path / "dest"
    dup_folder = dest / "2024" / "2024-06-15"
    dup_folder.mkdir(parents=True)
    for i in range(2):
        Image.new("RGB", (100, 100), (i * 80, 40, 40)).save(
            str(dup_folder / f"dup_{i}.jpg")
        )

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    default_ws = db._active_workspace_id

    from scanner import scan as do_scan
    do_scan(str(dest), db)

    # Switch to a fresh workspace that has no folders.
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    assert db.get_folder_tree() == []

    src = tmp_path / "source"
    src.mkdir()
    for i in range(2):
        shutil.copy2(
            str(dup_folder / f"dup_{i}.jpg"),
            str(src / f"dup_{i}.jpg"),
        )

    params = PipelineParams(
        source=str(src),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, other_ws, params)

    # Re-open DB to pick up writes made on the worker thread's own connection.
    db2 = Database(db_path)
    db2.set_active_workspace(other_ws)
    other_folders = {f["path"] for f in db2.get_folder_tree()}
    assert str(dup_folder) in other_folders, (
        f"Expected {dup_folder!r} to be linked to Other workspace after "
        f"pipeline dedupped all source files; got {other_folders!r}"
    )


def test_pipeline_copy_mode_scans_subfolders(tmp_path, monkeypatch):
    """After ingest, scan should use restrict_dirs to target only subfolders
    that received files, while keeping the destination as root for folder hierarchy."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Create source images
    src = tmp_path / "source"
    src.mkdir()
    for name in ["a.jpg", "b.jpg"]:
        img = Image.new("RGB", (100, 100), "red")
        img.save(str(src / name))

    # Create destination with existing files in a different subfolder
    dest = tmp_path / "dest"
    dest.mkdir()
    existing_folder = dest / "2025" / "01-01"
    existing_folder.mkdir(parents=True)
    for i in range(5):
        img = Image.new("RGB", (100, 100), "blue")
        img.save(str(existing_folder / f"existing_{i}.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(src),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    # Track scan() calls
    scan_calls = []
    from unittest.mock import patch

    import scanner as scanner_mod
    original_scan = scanner_mod.scan

    def tracking_scan(root, *args, **kwargs):
        scan_calls.append({"root": str(root), "restrict_dirs": kwargs.get("restrict_dirs")})
        return original_scan(root, *args, **kwargs)

    with patch.object(scanner_mod, "scan", tracking_scan):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # Scan should be called with the destination as root (for folder hierarchy)
    assert len(scan_calls) > 0, "Scan should have been called"
    assert scan_calls[-1]["root"] == str(dest), \
        f"Scan root should be the destination, got: {scan_calls[-1]['root']}"
    # restrict_dirs should be set to only the subfolders that received files
    restrict = scan_calls[-1]["restrict_dirs"]
    assert restrict is not None, "restrict_dirs should be set when files were copied"
    # The restrict dirs should NOT include the existing subfolder
    for d in restrict:
        assert str(existing_folder) != d, \
            f"restrict_dirs should not include pre-existing folder {existing_folder}"


def test_pipeline_progress_events_carry_stage_id(tmp_path, monkeypatch):
    """Each per-stage progress event should carry a stage_id so the
    Pipeline UI can route concurrent stages (scan + thumbnails) to their
    own progress bars instead of colliding."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for i in range(3):
        img = Image.new("RGB", (100, 100), "green")
        img.save(str(photo_dir / f"p_{i}.jpg"))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Gather all progress events that include a stage_id
    stage_ids_seen = {
        e[2]["stage_id"]
        for e in runner.events
        if e[1] == "progress" and "stage_id" in e[2]
    }
    # Scan and thumbnails are the minimum we expect for a scan-in-place
    # run with classify/extract/regroup skipped.
    assert "scan" in stage_ids_seen, \
        f"Expected scan stage_id in events; saw: {stage_ids_seen}"
    assert "thumbnails" in stage_ids_seen, \
        f"Expected thumbnails stage_id in events; saw: {stage_ids_seen}"


def test_pipeline_scan_not_running_during_ingest(tmp_path, monkeypatch):
    """In copy mode, stages.scan should stay 'pending' while ingest runs,
    so the Scan card doesn't pulse during the import sub-phase."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    src = tmp_path / "source"
    src.mkdir()
    for name in ["a.jpg", "b.jpg"]:
        img = Image.new("RGB", (100, 100), "red")
        img.save(str(src / name))

    dest = tmp_path / "dest"
    dest.mkdir()

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(src),
        destination=str(dest),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Find events emitted while ingest was running
    ingest_running_events = [
        e[2] for e in runner.events
        if e[1] == "progress"
        and e[2].get("stages", {}).get("ingest", {}).get("status") == "running"
    ]
    assert len(ingest_running_events) > 0, \
        "Expected some events emitted while ingest was running"
    # During ingest, scan should still be pending (not running)
    for ev in ingest_running_events:
        scan_status = ev.get("stages", {}).get("scan", {}).get("status")
        assert scan_status == "pending", \
            f"scan should be 'pending' while ingest is running, got: {scan_status}"


def test_pipeline_collection_mode_marks_scan_skipped(tmp_path, monkeypatch):
    """In collection mode, stages.scan should be 'skipped' (not stuck
    on 'pending') so the Scan card renders as resolved."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Create an empty collection to reference
    coll_id = db.add_collection("test collection", json.dumps([]))

    params = PipelineParams(
        collection_id=coll_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Check that at least one event shows scan as 'skipped'
    scan_statuses = {
        e[2]["stages"]["scan"]["status"]
        for e in runner.events
        if e[1] == "progress" and "stages" in e[2] and "scan" in e[2]["stages"]
    }
    assert "skipped" in scan_statuses, \
        f"scan should be 'skipped' in collection mode, saw: {scan_statuses}"


def test_pipeline_collection_mode_generates_missing_thumbnails(tmp_path, monkeypatch):
    """In collection mode the thumbnail stage must still process the collection's
    photos. Previously it drained an empty queue (only fed by the scanner) and
    completed with '0 thumbnails' even when photos were missing thumbs."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ["a.jpg", "b.jpg", "c.jpg"]:
        Image.new("RGB", (100, 100), "red").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # First pipeline run: scan + build collection + generate thumbnails.
    runner = FakeRunner()
    job = _make_job()
    result = run_pipeline_job(
        job, runner, db_path, ws_id,
        PipelineParams(
            source=str(photo_dir),
            skip_classify=True, skip_extract_masks=True, skip_regroup=True,
        ),
    )
    coll_id = result["collection_id"]

    # Wipe the thumbnail cache to simulate thumbs that were lost or never built.
    thumb_dir = os.path.join(os.path.dirname(db_path), "thumbnails")
    for f in os.listdir(thumb_dir):
        os.remove(os.path.join(thumb_dir, f))

    # Second run: replay the pipeline against the existing collection
    # (skip_scan path). Thumbnails must be regenerated for all 3 photos.
    runner2 = FakeRunner()
    job2 = _make_job()
    result2 = run_pipeline_job(
        job2, runner2, db_path, ws_id,
        PipelineParams(
            collection_id=coll_id,
            skip_classify=True, skip_extract_masks=True, skip_regroup=True,
        ),
    )

    thumb_result = result2["stages"].get("thumbnails", {})
    assert thumb_result.get("generated", 0) == 3, (
        f"Expected 3 thumbnails regenerated in collection mode, "
        f"got {thumb_result}"
    )
    thumb_files = [f for f in os.listdir(thumb_dir) if not f.startswith(".")]
    assert len(thumb_files) == 3


# ---------------------------------------------------------------------------
# Stage failure propagation (fixes the silent model-loader failure incident)
# ---------------------------------------------------------------------------


def _make_stage_failer(monkeypatch, stage_name, err_message):
    """Monkeypatch a specific pipeline stage to raise when invoked."""
    import pipeline_job

    real_run = pipeline_job.run_pipeline_job

    def wrapped(job, runner, db_path, ws_id, params):
        # Replace the stage function inside run_pipeline_job by patching the
        # classifier module that model_loader_stage imports lazily. We use a
        # targeted env toggle instead so the test stays hermetic.
        raise NotImplementedError(
            "Use direct classifier monkeypatch in the test instead."
        )

    return real_run


def _write_fake_model_files(model_dir, extra_files=()):
    """Materialize a fake model directory that passes _classify_model_state."""
    import models
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "image_encoder.onnx").write_bytes(b"stub")
    with open(model_dir / "image_encoder.onnx.data", "wb") as f:
        f.truncate(models._MIN_BINARY_MODEL_BYTES + 1024)
    (model_dir / "text_encoder.onnx").write_bytes(b"stub")
    with open(model_dir / "text_encoder.onnx.data", "wb") as f:
        f.truncate(models._MIN_BINARY_MODEL_BYTES + 1024)
    (model_dir / "tokenizer.json").write_text("{}")
    (model_dir / "config.json").write_text("{}")
    for extra in extra_files:
        (model_dir / extra).write_text("{}")


def _setup_fake_downloaded_model(tmp_path, monkeypatch):
    """Put a validation-passing fake model on disk so model_loader_stage can
    get past the model-lookup / labels / taxonomy steps and into Classifier().
    Returns the model id that was set active.
    """
    import classify_job
    import model_verify
    import models
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))
    _write_fake_model_files(tmp_path / "models" / "bioclip-vit-b-16")
    models.set_active_model("bioclip-vit-b-16")
    # Short-circuit taxonomy and label loading so the test stays focused on
    # model-loading behavior.
    monkeypatch.setattr(classify_job, "_load_taxonomy", lambda *a, **k: {})
    monkeypatch.setattr(
        classify_job, "_load_labels", lambda *a, **k: (["test-label"], False)
    )
    # Short-circuit hash verification — these tests use stub files that
    # would never match any real HF hash, and the verification path has
    # its own dedicated unit tests.
    monkeypatch.setattr(
        model_verify,
        "verify_if_needed",
        lambda model_id, model_dir, hf_subdir: None,
    )
    return "bioclip-vit-b-16"


def _setup_two_fake_downloaded_models(tmp_path, monkeypatch):
    """Install two bioclip models side-by-side; both reported as downloaded."""
    import classify_job
    import models
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))
    _write_fake_model_files(tmp_path / "models" / "bioclip-vit-b-16")
    _write_fake_model_files(
        tmp_path / "models" / "bioclip-2",
        extra_files=("tol_embeddings.npy", "tol_classes.json"),
    )
    models.set_active_model("bioclip-vit-b-16")
    monkeypatch.setattr(classify_job, "_load_taxonomy", lambda *a, **k: {})
    monkeypatch.setattr(
        classify_job, "_load_labels", lambda *a, **k: (["test-label"], False)
    )
    return ["bioclip-vit-b-16", "bioclip-2"]


def test_pipeline_raises_when_stage_fails(tmp_path, monkeypatch):
    """If any pipeline stage ends in 'failed', run_pipeline_job must raise.

    This is the fix for the silent model-loader crash incident: a stage
    caught its own exception and returned normally, so jobs.py recorded the
    run as 'completed' despite the failure. Now stage failures propagate.
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def boom(*args, **kwargs):
        raise RuntimeError("model_path must not be empty")

    monkeypatch.setattr(classifier_mod, "Classifier", boom)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The pipeline must stash its structured result on the job before raising,
    # so the pipeline UI's _onPipelineComplete handler can still read
    # result.result.errors and map the "[model_loader] Fatal: ..." prefix to
    # the right stage card. Without this, users on a failed run lose the
    # actionable "Failed: <stage error>" label on the card that broke.
    assert isinstance(job["result"], dict), \
        "Failed pipeline must leave a dict result on the job for UI rendering"
    assert "errors" in job["result"]
    assert any(
        "model_loader" in e for e in job["result"]["errors"]
    ), f"Expected a [model_loader]-prefixed error, got: {job['result']['errors']}"
    assert "duration" in job["result"]
    assert "stages" in job["result"]


def test_pipeline_translates_verify_failure_to_repair_message(tmp_path, monkeypatch):
    """When model_verify.verify_if_needed raises ModelCorruptError during the
    model_loader preflight, the pipeline should fail with the same Repair
    message used for other incomplete-model errors.

    This is the lazy-verification path: a silent bit-rot or unfinished
    download is surfaced right before the model is handed to ONNXRuntime,
    with an actionable recovery hint for the user.
    """
    import classifier as classifier_mod
    import config as cfg
    import model_verify
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def raise_corrupt(model_id, model_dir, hf_subdir):
        raise model_verify.ModelCorruptError(
            model_id,
            model_verify.VerifyResult(
                ok=False, mismatches=["image_encoder.onnx.data"]
            ),
        )

    monkeypatch.setattr(model_verify, "verify_if_needed", raise_corrupt)

    # Classifier should never be reached because verify_if_needed fails first.
    def classifier_should_not_be_called(*args, **kwargs):
        raise AssertionError(
            "Classifier was constructed despite verify_if_needed raising"
        )

    monkeypatch.setattr(classifier_mod, "Classifier", classifier_should_not_be_called)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    model_loader_errors = [
        kwargs.get("error", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "model_loader" and "error" in kwargs
    ]
    assert any("Repair" in e for e in model_loader_errors), \
        f"Expected a Repair hint in model_loader errors, saw: {model_loader_errors}"


def test_pipeline_preflight_accepts_unverified_model(tmp_path, monkeypatch):
    """Models in 'unverified' state (files present, SHA256 check skipped
    because HF was unreachable) must pass preflight. get_models() already
    reports downloaded=True for them, so rejecting them at pipeline start
    would turn a transient outage into a hard pipeline failure with no way
    to clear it short of deleting and redownloading the model.
    """
    import classifier as classifier_mod
    import config as cfg
    import model_verify
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # Write the verify-skipped sentinel so _classify_model_state returns
    # "unverified" rather than "ok".
    model_dir = tmp_path / "models" / "bioclip-vit-b-16"
    (model_dir / model_verify.VERIFY_SKIPPED_SENTINEL).write_text(
        "Transient HF outage (test fixture)"
    )

    # Stub Classifier so the preflight check is the only thing that could
    # fail on this path. If preflight rejects the unverified state, we'll
    # see a "Repair" error in model_loader step updates before the stub
    # is ever invoked.
    class _StubClassifier:
        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(classifier_mod, "Classifier", _StubClassifier)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    model_loader_errors = [
        kwargs.get("error", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "model_loader" and "error" in kwargs
    ]
    assert not any("Repair" in e for e in model_loader_errors), (
        "Preflight must accept 'unverified' models, but saw Repair error: "
        f"{model_loader_errors}"
    )


def test_pipeline_translates_incomplete_model_error(tmp_path, monkeypatch):
    """Model loader failures from missing external-data get a friendly message.

    Users should see "open Settings → Models and click Repair" rather than
    the raw ONNXRuntime stack.
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def boom(*args, **kwargs):
        raise RuntimeError(
            "[ONNXRuntimeError] model_path must not be empty. Ensure that "
            "a path is provided when the model is created or loaded."
        )

    monkeypatch.setattr(classifier_mod, "Classifier", boom)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError) as exc:
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # Either the model_loader stage raised with the friendly message directly,
    # or the pipeline raised its own failure wrapping the original; in either
    # case the errors list (accessible via the model_loader step update) should
    # contain the actionable "Repair" hint.
    model_loader_errors = [
        kwargs.get("error", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "model_loader" and "error" in kwargs
    ]
    assert any("Repair" in e for e in model_loader_errors), \
        f"Expected a Repair hint in model_loader errors, saw: {model_loader_errors}"


def test_pipeline_cancellation_takes_precedence_over_failure(tmp_path, monkeypatch):
    """A cancelled pipeline must not be recorded as 'failed' even if a stage
    crashed on the way down. Cancellation intent beats failure.
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def boom(*args, **kwargs):
        raise RuntimeError("model_path must not be empty")

    monkeypatch.setattr(classifier_mod, "Classifier", boom)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    # Mark cancelled BEFORE the pipeline starts so the post-stage check sees it.
    runner.cancelled_ids.add(job["id"])

    # Should NOT raise — cancellation wins over stage failure.
    result = run_pipeline_job(job, runner, db_path, ws_id, params)
    assert isinstance(result, dict)


def test_pipeline_loops_over_multiple_models(tmp_path, monkeypatch):
    """When model_ids contains multiple models, each one must be loaded and
    run through classify. This is the fix for the UI-dropped-multi-select bug:
    the pipeline page collects multiple checked models but only the first one
    was forwarded to the backend. The backend now honors model_ids."""
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    construction_calls = []

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            construction_calls.append(kwargs)

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert len(construction_calls) == len(model_ids), (
        f"Expected Classifier() to be constructed {len(model_ids)} times "
        f"(one per model_id), got {len(construction_calls)}"
    )
    # model_loader must record completion with a summary naming each model.
    model_loader_summaries = [
        kwargs.get("summary", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "model_loader" and kwargs.get("status") == "completed"
    ]
    joined = " ".join(model_loader_summaries)
    assert "BioCLIP" in joined and "BioCLIP-2" in joined, (
        f"model_loader summary should mention both models, saw: {model_loader_summaries}"
    )


def test_pipeline_model_ids_back_compat_with_model_id(tmp_path, monkeypatch):
    """A job with only the legacy `model_id` field (no `model_ids`) must still
    load exactly that one model — preserving back-compat with older callers."""
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    construction_calls = []

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            construction_calls.append(kwargs)

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_id="bioclip-vit-b-16",
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert len(construction_calls) == 1, (
        f"Legacy model_id should load exactly one classifier, "
        f"got {len(construction_calls)}"
    )


def test_pipeline_reclassify_multimodel_ignores_stale_detection_ids(
    tmp_path, monkeypatch
):
    """On reclassify with multiple models, already_detected must be cleared
    before model 1's batch loop so model 2+ only reuse detections produced in
    this run, not stale rows from a prior pipeline pass.

    Regression: before the fix, already_detected was pre-seeded from
    get_existing_detection_photo_ids() before the model loop.  When model 1
    ran with reclassify=True but did NOT produce a detection for a photo that
    already had a prior-run detection row, model 2 (reclassify=False) still
    found that photo in already_detected and called db.get_detections(),
    binding its predictions to outdated detection_ids.
    """
    import json

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Create a folder + photo and insert a prior-run detection row so that
    # get_existing_detection_photo_ids() returns this photo's id.
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "test.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "test.jpg")
    db.save_detections(
        photo_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    # Static collection containing exactly that one photo.
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # Capture the already_detected_ids and cached_detections passed to each
    # _detect_batch call so we can verify model 2 gets fresh cache entries
    # from model 1 rather than stale DB rows.
    detect_calls = []

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        detect_calls.append({
            "already_detected_ids": frozenset(already_detected_ids or set()),
            "cached_detections": dict(cached_detections) if cached_detections else {},
            "reclassify": reclassify,
        })
        # Model 1 "detects" nothing in this run — empty det_map, but every
        # photo in the batch completed its iteration.
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # With the detect pre-pass, _detect_batch runs ONCE across the whole
    # collection regardless of how many models are classifying downstream.
    # Every subsequent classify stage reads from the shared cache rather
    # than invoking the detector again.
    assert len(detect_calls) == 1, (
        f"Expected exactly 1 _detect_batch call (shared pre-pass), got "
        f"{len(detect_calls)}"
    )

    # Reclassify: the shared pre-pass must start with an empty
    # already_detected so every photo's detection is recomputed — no stale
    # prior-run IDs should leak in.
    assert photo_id not in detect_calls[0]["already_detected_ids"], (
        f"Prior-run photo_id {photo_id} leaked into already_detected_ids on "
        "the reclassify pre-pass. already_detected must start empty."
    )
    assert detect_calls[0]["reclassify"] is True, (
        "Detect pre-pass should be called with reclassify=True on a "
        "reclassify run so MegaDetector re-runs instead of short-circuiting "
        "against existing DB rows."
    )


def test_detect_batch_prefers_cached_detections_over_db(monkeypatch):
    """_detect_batch must use cached_detections when provided instead of
    calling db.get_detections(), so model 2+ in a multi-model reclassify run
    bind predictions to the detection rows model 1 produced in *this* run
    rather than stale rows from a prior pipeline pass.

    Regression test for the second Codex P1 comment on #506 ('Restrict model
    2+ reuse to detections created in this run').
    """
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import classify_job

    photo = {"id": 42, "folder_id": 1, "filename": "bird.jpg"}

    cached_det = [{"id": 99, "box_x": 0.1, "box_y": 0.1,
                   "box_w": 0.5, "box_h": 0.5,
                   "confidence": 0.95, "category": "animal"}]

    db_called = {"n": 0}

    class FakeDB:
        def get_detections(self, photo_id):
            db_called["n"] += 1
            return []

    det_map, count, processed = classify_job._detect_batch(
        photos=[photo],
        folders={1: "/fake"},
        runner=None,
        job=None,
        reclassify=False,
        db=FakeDB(),
        already_detected_ids={42},
        cached_detections={42: cached_det},
    )

    assert db_called["n"] == 0, (
        "db.get_detections() must NOT be called when cached_detections "
        "already has an entry for the photo."
    )
    assert det_map.get(42) == cached_det, (
        "detection_map must contain the cached detection list, not a DB result."
    )
    assert count == 1
    assert 42 in processed


def test_pipeline_classify_passes_primary_detection_to_prepare_image(
    tmp_path, monkeypatch
):
    """classify_stage must pass the primary detection dict (with box_x/y/w/h
    keys) to _prepare_image, not the raw {photo_id: [dets]} det_map.

    Regression: classify_stage called
        _prepare_image(photo, folders, det_map)
    where det_map is {photo_id: [detection, ...]}.  Because det_map is truthy
    once any photo in a batch has a detection, _prepare_image entered its
    crop branch and evaluated det_map["box_w"] -> KeyError: 'box_w', aborting
    classify the moment the first detection came back.  The fix is to look
    up the highest-confidence detection for this specific photo and pass
    that (or None) to _prepare_image.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "bird.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "bird.jpg")
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    primary_det = {
        "id": 77,
        "box_x": 0.1, "box_y": 0.1, "box_w": 0.5, "box_h": 0.5,
        "confidence": 0.95, "category": "animal",
    }

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {p["id"]: [primary_det] for p in batch}
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    captured = []

    def capturing_prepare_image(photo, folders, detection, vireo_dir=None):
        captured.append(detection)
        # Returning (None, ...) tells the caller this photo failed to load,
        # which short-circuits _flush_batch.  We only care about the
        # arguments passed in.
        return None, "", ""

    monkeypatch.setattr(classify_job, "_prepare_image", capturing_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    # The fake _prepare_image returns None by design to short-circuit before
    # the classifier runs — we only care about what argument it received.
    # That now counts as a classify failure, which propagates to a pipeline
    # RuntimeError; swallow it so we can still inspect `captured`.
    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    assert captured, (
        "_prepare_image was never called — test setup no longer exercises "
        "the classify crop path."
    )
    # Every call must receive either None (no detection) or a detection dict
    # with a 'box_w' key — never the raw {photo_id: [dets]} map.
    for det in captured:
        assert det is None or (isinstance(det, dict) and "box_w" in det), (
            f"_prepare_image received {det!r}; expected a detection dict "
            "with 'box_w' (or None), not the {photo_id: [dets]} map."
        )
    # The fix should pass this photo's primary detection through.
    assert any(
        isinstance(d, dict) and d.get("box_w") == 0.5 for d in captured
    ), (
        f"Expected _prepare_image to receive the primary detection for "
        f"photo {photo_id}, got {captured!r}."
    )
    # And the KeyError must not have leaked into job errors.
    assert not any("'box_w'" in e for e in job["errors"]), (
        f"KeyError 'box_w' leaked into job errors: {job['errors']}"
    )


def test_pipeline_reclassify_purges_stale_detection_rows(tmp_path, monkeypatch):
    """On reclassify, prior-run detection rows must be deleted after model 1
    re-runs MegaDetector so that subsequent non-reclassify runs don't reuse
    stale bounding boxes via get_existing_detection_photo_ids + get_detections.

    Scenario: a photo had a prior detection (potential false positive). The
    reclassify run finds NO animals this time (fake_detect_batch returns {}).
    After reclassify the old detection row must be gone so future runs
    actually call MegaDetector rather than short-circuiting to the stale box.

    Regression for Codex P1 review on #511 line 848.
    """
    import json

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "test.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "test.jpg")

    # Prior-run detection row (e.g. a prior false positive).
    prior_det_ids = db.save_detections(
        photo_id,
        [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
             "confidence": 0.9, "category": "animal"},
        ],
        detector_model="MegaDetector",
    )
    assert prior_det_ids, "setup sanity: prior detection was inserted"

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # _detect_batch stub: reclassify finds no animals this time (false pos fixed).
    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids[:1],
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # The stale prior-run detection must be gone after reclassify so that
    # future non-reclassify runs don't reuse it via the already-detected path.
    verify_db = Database(db_path)
    verify_db.set_active_workspace(ws_id)
    remaining = verify_db.get_detections(photo_id)
    assert remaining == [], (
        f"Stale prior-run detection rows must be purged during reclassify but "
        f"db.get_detections({photo_id}) returned {remaining!r}. "
        "Without this cleanup, future non-reclassify runs short-circuit to "
        "stale boxes via get_existing_detection_photo_ids + get_detections, "
        "causing false-positive detections to persist indefinitely. "
        "Regression for Codex P1 review on #511 line 848."
    )


def test_detect_batch_skips_empty_photo_on_rerun(tmp_path, monkeypatch):
    """A photo with no animals, recorded in detector_runs, must not be
    re-detected on a subsequent non-reclassify pipeline run.

    Mirrors test_classify_job.test_detect_batch_skips_empty_photo_on_rerun
    but drives through run_pipeline_job's detect_stage so we exercise the
    pipeline's own already_detected seeding (which must use
    get_detector_run_photo_ids, not the legacy get_existing_detection_photo_ids
    shim that misses empty-scene photos).
    """
    import json

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "empty.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "empty.jpg")

    # Simulate a prior run where MegaDetector scanned the photo and found
    # NOTHING — there are no detection rows, but detector_runs records the
    # scan so the next pipeline pass can skip re-invoking the detector.
    db.save_detections(photo_id, [], detector_model="megadetector-v6")
    db.record_detector_run(photo_id, "megadetector-v6", box_count=0)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # Remove the legacy shim so the pipeline must call
    # get_detector_run_photo_ids directly. If pipeline still uses the
    # legacy name (via getattr) it will fall through to the default
    # `lambda: set()` and miss our empty-scene photo.
    monkeypatch.delattr(Database, "get_existing_detection_photo_ids")

    # Capture what already_detected_ids the pipeline passes to _detect_batch.
    # If the pipeline seeds correctly from get_detector_run_photo_ids, our
    # empty-scene photo will appear in already_detected_ids — meaning the
    # real _detect_batch would skip re-invoking MegaDetector for it.
    detect_calls = []

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        detect_calls.append({
            "already_detected_ids": frozenset(already_detected_ids or set()),
            "batch_ids": [p["id"] for p in batch],
        })
        # Simulate the real _detect_batch's skip behaviour: if the photo
        # is already in already_detected_ids, don't re-"detect" it.
        processed = {p["id"] for p in batch}
        return {}, 0, processed

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    # Patch classify_stage's prediction-photo lookup so classify_stage
    # doesn't trip on pre-existing schema issues in other migrations.
    # (This test focuses narrowly on detect_stage's already_detected seed.)
    monkeypatch.setattr(
        Database, "get_existing_prediction_photo_ids",
        lambda self, model_name: set(),
    )

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        reclassify=False,  # non-reclassify: must honour detector_runs skip
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    # classify_stage may still fail against pre-migration schema bits that
    # this task doesn't own. We only need detect_stage to have run.
    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    assert detect_calls, "expected detect_stage to call _detect_batch"
    first_call = detect_calls[0]
    assert photo_id in first_call["already_detected_ids"], (
        f"Empty-scene photo {photo_id} was recorded in detector_runs but "
        f"pipeline did not seed it into already_detected_ids "
        f"(got {set(first_call['already_detected_ids'])!r}). "
        f"detect_stage must seed from get_detector_run_photo_ids "
        f"('megadetector-v6'), not the legacy "
        f"get_existing_detection_photo_ids shim which misses empty-scene photos."
    )


def test_pipeline_reclassify_partial_abort_preserves_unprocessed_detections(
    tmp_path, monkeypatch
):
    """A reclassify aborted before any model finishes classifying must NOT
    delete pre-run detection rows. The purge is gated on a successful
    model run (see Codex P1 on #566) — otherwise a cancel mid-detect would
    destroy prior detections with no replacement predictions.

    Scenario: 2 photos each have a prior detection row. Batch size is
    patched to 1 so each photo is its own batch in detect_stage. After
    the first detect batch completes, _should_abort returns True so the
    rest of the pipeline short-circuits before classify_stage writes any
    predictions.

    Expected outcome: BOTH photos' prior detection rows are preserved,
    because `models_succeeded` never reaches 1.
    """
    import json

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo1_id = db.add_photo(folder_id, "photo1.jpg", ".jpg", 11111, 1_000_000.0)
    photo2_id = db.add_photo(folder_id, "photo2.jpg", ".jpg", 22222, 1_000_000.0)
    _drop_jpeg(folder_path, "photo1.jpg")
    _drop_jpeg(folder_path, "photo2.jpg")

    # Give each photo a prior-run detection row.
    prior_det1 = db.save_detections(
        photo1_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    prior_det2 = db.save_detections(
        photo2_id,
        [{"box": {"x": 0.2, "y": 0.2, "w": 0.3, "h": 0.3},
          "confidence": 0.8, "category": "animal"}],
        detector_model="MegaDetector",
    )
    assert prior_det1 and prior_det2, "setup sanity: prior detections inserted"

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo1_id, photo2_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # Process one photo per batch so we can abort between them.
    monkeypatch.setattr(classify_job, "_BATCH_SIZE", 1)

    # After the first _detect_batch call, all subsequent _should_abort checks
    # return True, preventing the second batch from being processed.
    detect_call_count = [0]
    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if detect_call_count[0] >= 1:
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        detect_call_count[0] += 1
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids[:1],
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert detect_call_count[0] == 1, (
        "Expected exactly one _detect_batch call before abort; "
        f"got {detect_call_count[0]}"
    )

    verify_db = Database(db_path)
    verify_db.set_active_workspace(ws_id)

    remaining1 = verify_db.get_detections(photo1_id)
    remaining2 = verify_db.get_detections(photo2_id)

    # No model ran to completion (abort fired before classify could store
    # predictions), so neither photo's prior row may be purged — otherwise
    # cancelling a reclassify would destroy prior detections and their
    # cascaded predictions with no replacement data.
    assert remaining1, (
        f"photo1's prior detection row must be preserved on an aborted "
        f"reclassify — no classifier ran to completion, so the stale purge "
        f"must not fire. get_detections returned {remaining1!r}."
    )
    assert remaining2, (
        "photo2's prior detection row must be preserved on an aborted "
        "reclassify — no classifier ran to completion."
    )


def test_pipeline_reclassify_partial_batch_exception_preserves_detections(
    tmp_path, monkeypatch
):
    """A reclassify where _detect_batch exits mid-batch on an exception must
    NOT delete detection rows for the photos that were never actually
    reached inside that batch.

    Scenario: two photos share a single batch.  _detect_batch only
    completes the per-photo iteration for the first photo and returns early
    (simulating detect_animals raising while processing photo2 — the real
    _detect_batch catches the exception at function level and returns the
    accumulated detection_map with only the already-processed photos).

    Expected outcome:
    - photo1 (whose iteration completed) has its stale prior-run row purged.
    - photo2 (whose iteration never ran) keeps its stale prior-run row.

    Regression for Codex P1 review on #513 line 981 — the purge must be
    keyed to per-photo processing completion, not the full submitted batch.
    """
    import json

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo1_id = db.add_photo(folder_id, "photo1.jpg", ".jpg", 11111, 1_000_000.0)
    photo2_id = db.add_photo(folder_id, "photo2.jpg", ".jpg", 22222, 1_000_000.0)
    _drop_jpeg(folder_path, "photo1.jpg")
    _drop_jpeg(folder_path, "photo2.jpg")

    prior_det1 = db.save_detections(
        photo1_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    prior_det2 = db.save_detections(
        photo2_id,
        [{"box": {"x": 0.2, "y": 0.2, "w": 0.3, "h": 0.3},
          "confidence": 0.8, "category": "animal"}],
        detector_model="MegaDetector",
    )
    assert prior_det1 and prior_det2, "setup sanity: prior detections inserted"

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo1_id, photo2_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # Both photos land in a single batch.  The stub returns a processed_ids
    # set containing ONLY photo1, mirroring what _detect_batch does when
    # detect_animals raises while processing photo2: the try/except at the
    # function level returns the accumulated results and photo2 never makes
    # it into processed_ids.
    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        return {}, 0, {photo1_id}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids[:1],
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    # Classification may fail (no real JPEGs on disk, stub classifier
    # misses methods) — we only care about the purge scope here, not the
    # pipeline exit code. Any RuntimeError gets swallowed.
    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    verify_db = Database(db_path)
    verify_db.set_active_workspace(ws_id)

    remaining1 = verify_db.get_detections(photo1_id)
    remaining2 = verify_db.get_detections(photo2_id)

    assert remaining1 == [], (
        f"photo1's per-photo iteration completed in _detect_batch; its stale "
        f"prior-run row must be purged, but get_detections returned "
        f"{remaining1!r}. Regression for Codex P1 review on #513 line 981."
    )
    assert remaining2 != [], (
        "photo2's iteration never ran (simulated mid-batch exception in "
        "_detect_batch).  Its stale prior-run detection row must be "
        "preserved — purging it would cascade-delete predictions for a "
        "photo that was never re-detected.  "
        "Regression for Codex P1 review on #513 line 981."
    )


def test_pipeline_classify_mid_batch_cancel_skips_storage(tmp_path, monkeypatch):
    """A mid-classify cancel must take effect within roughly one photo's
    worth of work (not at the next 32-photo batch boundary), and must skip
    _store_grouped_predictions, which can take a minute on large
    collections.  The per-model step is finalized with a 'Cancelled'
    summary so the user sees the partial state in the job tree.
    """
    import threading

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 1000 + i, 1_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # detect_stage stub: surface the prior-run detection rows we inserted
    # above as if MegaDetector just produced them, so classify_stage's
    # cached_detections lookup hits with real DB ids (record_classifier_run
    # has a FK to detections.id).
    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            existing = db_.get_detections(p["id"])
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in existing if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    # _prepare_image opens the real image and crops it. Stub it to a fake
    # PIL image so the per-photo loop progresses to _flush_batch regardless
    # of the dummy 16x16 black JPEGs on disk.
    from PIL import Image as _PILImage

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])
        return _PILImage.new("RGB", (16, 16), "black"), folder_path, image_path

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    # Spy on _flush_batch: count calls, populate raw_results with a fake
    # prediction so the per-model step has something to report, and trigger
    # abort after the FIRST call so the inner-loop abort check is exercised
    # on the second photo.
    abort_after_classify = threading.Event()
    flush_calls = [0]

    def spy_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                        top_k=1):
        flush_calls[0] += 1
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        abort_after_classify.set()
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", spy_flush_batch)

    # Spy on _store_grouped_predictions to verify the cancel path skips it.
    store_calls = [0]

    def spy_store(*args, **kwargs):
        store_calls[0] += 1
        return {"predictions_stored": 0, "burst_groups": 0,
                "already_labeled": 0}

    monkeypatch.setattr(classify_job, "_store_grouped_predictions", spy_store)

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_after_classify.is_set():
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Without the fix, all 3 photos are classified before the next outer
    # batch boundary fires (batch_size=32 > 3 photos). With the fix, the
    # inner-loop check catches abort on photo 2.
    assert 1 <= flush_calls[0] <= 2, (
        f"Expected classify to stop within ~1 photo of abort; got "
        f"{flush_calls[0]} _flush_batch calls. Without the inner-loop "
        f"abort check this would be 3."
    )

    # _store_grouped_predictions is the slow tail that the user reported
    # as 'still going' after cancel.  The cancel path must skip it.
    assert store_calls[0] == 0, (
        f"_store_grouped_predictions must NOT run on a mid-batch cancel; "
        f"got {store_calls[0]} calls."
    )

    classify_step_id = f"classify:{model_id}"
    cancelled_updates = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == classify_step_id and "Cancelled" in (kw.get("summary") or "")
    ]
    assert cancelled_updates, (
        f"Expected at least one update on {classify_step_id!r} with a "
        f"'Cancelled' summary; got step_updates={runner.step_updates!r}"
    )

    # Progress on the cancelled step must reflect what was *actually*
    # classified (1 photo), not what the per-batch progress event claimed
    # (the entire batch).  Without the corrected progress emit, the step
    # would show 3/3 even though only 1 photo was inferred — Codex P2.
    cancelled_kw = cancelled_updates[-1]
    assert cancelled_kw.get("progress") == {"current": 1, "total": 3}, (
        f"Cancelled step must show actual processed count (1/3), not "
        f"the pre-emptive batch claim (3/3). Got progress="
        f"{cancelled_kw.get('progress')!r}"
    )
    assert "1 of 3 processed" in (cancelled_kw.get("summary") or ""), (
        f"Cancelled summary should report actual processed count; got "
        f"{cancelled_kw.get('summary')!r}"
    )


def test_pipeline_reclassify_cancel_preserves_existing_predictions(
    tmp_path, monkeypatch,
):
    """A reclassify cancelled mid-classify must NOT erase the user's prior
    predictions. The reclassify clear runs only once we're committed to
    storing fresh results — Codex P1 review on #710. Without this guard,
    `clear_predictions` had already wiped the predictions table when the
    cancel guard skipped `_store_grouped_predictions`, leaving the model
    with no predictions for the entire collection.
    """
    import threading

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    detection_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 3000 + i, 3_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        det_ids = db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)
        detection_ids.append(det_ids[0])

    # Insert pre-existing predictions for each detection under the model
    # name and fingerprint the pipeline will use ('BioCLIP' / 'legacy' for
    # the test stubs).  These are what must survive the cancelled reclassify.
    for det_id in detection_ids:
        db.add_prediction(
            detection_id=det_id,
            species="Pre-existing Sparrow",
            confidence=0.95,
            model="BioCLIP",
            labels_fingerprint="legacy",
        )

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # Force the bundle's labels_fingerprint to 'legacy' so it matches the
    # add_prediction calls above.
    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "legacy")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            existing = db_.get_detections(p["id"])
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in existing if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])
        return _PILImage.new("RGB", (16, 16), "black"), folder_path, image_path

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    abort_after_classify = threading.Event()

    def spy_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                        top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        abort_after_classify.set()
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", spy_flush_batch)

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_after_classify.is_set():
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    verify_db = Database(db_path)
    verify_db.set_active_workspace(ws_id)

    # All three pre-existing predictions must still be in the table.
    # Before the fix, clear_predictions ran at the top of the per-spec body
    # and wiped them; the cancel guard skipped _store_grouped_predictions,
    # leaving the predictions table empty for this model.
    surviving = verify_db.conn.execute(
        "SELECT COUNT(*) FROM predictions "
        "WHERE classifier_model = ? AND labels_fingerprint = ? "
        "AND species = ?",
        ("BioCLIP", "legacy", "Pre-existing Sparrow"),
    ).fetchone()[0]
    assert surviving == 3, (
        f"A cancelled reclassify must NOT wipe the user's prior "
        f"predictions. Expected all 3 'Pre-existing Sparrow' rows to "
        f"survive; found {surviving}."
    )


def test_pipeline_reclassify_success_preserves_classifier_run_keys(
    tmp_path, monkeypatch,
):
    """A successful reclassify must leave fresh classifier_runs rows in
    place for the processed detections so the next non-reclassify pass
    hits the skip gate.  The deferred reclassify clear runs AFTER the
    per-photo ``record_classifier_run`` calls; without ``clear_run_keys=False``
    it wipes the just-written run keys, forcing the next normal classify to
    re-infer the entire collection.  Codex P1 review on #710.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    detection_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 4000 + i, 4_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        det_ids = db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)
        detection_ids.append(det_ids[0])

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    import labels_fingerprint as lfp
    monkeypatch.setattr(lfp, "compute_fingerprint", lambda *a, **k: "legacy")

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            existing = db_.get_detections(p["id"])
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in existing if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])
        return _PILImage.new("RGB", (16, 16), "black"), folder_path, image_path

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    # Wrap clear_predictions to verify the deferred reclassify clear runs
    # with clear_run_keys=False AND that the just-written classifier_runs
    # rows survive that clear.  We can't assert via the final post-pipeline
    # state because the reclassify stale-detection purge runs after
    # _store_grouped_predictions and FK-cascades the run keys away — but
    # that's pre-existing reclassify behavior, unrelated to the deferred
    # clear's scope.  The fix is about the clear itself, so test the clear.
    from db import Database as _Db
    original_clear = _Db.clear_predictions
    clear_calls = []

    def wrapped_clear(self, model=None, collection_photo_ids=None,
                     labels_fingerprint=None, clear_run_keys=True):
        before = self.conn.execute(
            "SELECT COUNT(*) FROM classifier_runs"
        ).fetchone()[0]
        result = original_clear(
            self, model=model,
            collection_photo_ids=collection_photo_ids,
            labels_fingerprint=labels_fingerprint,
            clear_run_keys=clear_run_keys,
        )
        after = self.conn.execute(
            "SELECT COUNT(*) FROM classifier_runs"
        ).fetchone()[0]
        clear_calls.append({
            "model": model,
            "fp": labels_fingerprint,
            "clear_run_keys": clear_run_keys,
            "runs_before": before,
            "runs_after": after,
        })
        return result

    monkeypatch.setattr(_Db, "clear_predictions", wrapped_clear)

    def fake_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                        top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", fake_flush_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Find the deferred reclassify clear (the only call inside classify_stage
    # that scopes by model+fingerprint+photos).  It must have been invoked
    # with clear_run_keys=False so the per-photo record_classifier_run
    # writes survive into _store_grouped_predictions.
    deferred = [c for c in clear_calls if c["model"] == "BioCLIP"
                and c["fp"] == "legacy"]
    assert len(deferred) == 1, (
        f"Expected exactly one deferred reclassify clear; got "
        f"{len(deferred)}: {clear_calls!r}"
    )
    call = deferred[0]
    assert call["clear_run_keys"] is False, (
        f"Deferred reclassify clear must run with clear_run_keys=False so "
        f"the just-written classifier_runs survive into the next "
        f"non-reclassify pass.  Got clear_run_keys={call['clear_run_keys']!r}."
    )
    assert call["runs_before"] >= 3 and call["runs_after"] == call["runs_before"], (
        f"clear_predictions(clear_run_keys=False) must NOT touch "
        f"classifier_runs. Got runs_before={call['runs_before']}, "
        f"runs_after={call['runs_after']}."
    )


def test_pipeline_classify_cancel_does_not_raise_when_earlier_model_load_failed(
    tmp_path, monkeypatch,
):
    """If model 0 failed to load (populating skipped_model_names) and model 1
    is then cancelled mid-classify, the post-loop
    `if models_succeeded == 0 and skipped_model_names: raise` check must
    NOT fire — cancellation takes precedence over the all-models-failed
    signal. Without this guard, a user cancel gets misclassified as a
    fatal load failure (Codex P2 review).
    """
    import threading

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(folder_id, name, ".jpg", 2000 + i, 2_000_000.0 + i)
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            existing = db_.get_detections(p["id"])
            det_map[p["id"]] = [{
                "id": d["id"],
                "box_x": d["box_x"], "box_y": d["box_y"],
                "box_w": d["box_w"], "box_h": d["box_h"],
                "confidence": d["detector_confidence"],
                "category": d["category"],
            } for d in existing if d["detector_model"] != "full-image"]
        return det_map, len(batch), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    from PIL import Image as _PILImage

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        folder_path = folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])
        return _PILImage.new("RGB", (16, 16), "black"), folder_path, image_path

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    abort_after_classify = threading.Event()

    def spy_flush_batch(batch, clf, model_type, model_name, db_, raw_results,
                        top_k=1):
        for entry in batch:
            raw_results.append({
                "photo": entry["photo"],
                "detection_id": entry.get("detection_id"),
                "folder_path": entry["folder_path"],
                "image_path": entry["image_path"],
                "prediction": "Robin",
                "confidence": 0.9,
                "timestamp": None,
                "filename": entry["photo"]["filename"],
                "embedding": None,
                "taxonomy": None,
            })
        abort_after_classify.set()
        return 0

    monkeypatch.setattr(classify_job, "_flush_batch", spy_flush_batch)
    monkeypatch.setattr(
        classify_job, "_store_grouped_predictions",
        lambda *a, **k: {"predictions_stored": 0, "burst_groups": 0,
                         "already_labeled": 0},
    )

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_after_classify.is_set():
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    # Make model 0 fail to load (so skipped_model_names gets populated) and
    # model 1 succeed. The pipeline tries model 0 twice — once in
    # model_loader_stage's preload, once in classify_stage's spec_idx==0
    # branch — so the first 2 Classifier() calls fail; call 3 (model 1)
    # succeeds.
    classifier_calls = [0]

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            classifier_calls[0] += 1
            if classifier_calls[0] <= 2:
                raise RuntimeError(
                    "simulated load failure for model 0"
                )

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    # Order matters: model 0 fails, model 1 must come second so it actually
    # runs classify and is the one we cancel. Pass them explicitly to lock
    # the order against any reordering inside the pipeline.
    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    # Pre-fix, this would raise RuntimeError("All 1 model(s) failed to load: ...")
    # because models_succeeded=0 and skipped_model_names=[model 0 name].
    # With the guard, the cancel takes precedence and the call returns cleanly.
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Sanity: the cancelled model 1 step has a 'Cancelled' summary.
    classify_step_id = f"classify:{model_ids[1]}"
    cancelled_updates = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == classify_step_id and "Cancelled" in (kw.get("summary") or "")
    ]
    assert cancelled_updates, (
        f"Expected {classify_step_id!r} to finalize as 'Cancelled'; "
        f"got step_updates={runner.step_updates!r}"
    )


# ---------------------------------------------------------------------------
# Sentinel written on ONNX load failure
# ---------------------------------------------------------------------------


def test_onnx_load_failure_writes_verify_failed_sentinel(tmp_path, monkeypatch):
    """When ONNXRuntime fails with a missing-external-data error, the
    .verify_failed sentinel must be written so that _classify_model_state
    reports 'incomplete' and the Settings UI shows a Repair button.

    This is the fix for the bug where the pipeline tells the user
    "Open Settings -> Models and click Repair" but Settings shows the
    model as healthy because no sentinel was written.
    """
    import classifier as classifier_mod
    import config as cfg
    import model_verify
    import models
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)
    model_dir = tmp_path / "models" / model_id

    # Simulate ONNXRuntime raising a missing-external-data error.
    def boom(*args, **kwargs):
        raise RuntimeError(
            "[ONNXRuntimeError] model_path must not be empty. Ensure that "
            "a path is provided when the model is created or loaded."
        )

    monkeypatch.setattr(classifier_mod, "Classifier", boom)
    # Force hash check to report the files as bad, so the ONNX failure
    # handler commits the sentinel write (the "real corruption" path).
    monkeypatch.setattr(
        model_verify,
        "verify_model",
        lambda *a, **k: model_verify.VerifyResult(
            ok=False, mismatches=["image_encoder.onnx.data"]
        ),
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # 1. The .verify_failed sentinel must have been written.
    sentinel = model_dir / model_verify.VERIFY_FAILED_SENTINEL
    assert sentinel.exists(), (
        ".verify_failed sentinel must be written when ONNXRuntime fails "
        "with a missing-external-data error, otherwise Settings shows the "
        "model as healthy and no Repair button appears."
    )
    assert "onnx-load-failure" in sentinel.read_text()

    # 2. After the sentinel is written, _classify_model_state must return
    #    'incomplete' so the Settings UI surfaces the Repair button.
    known = [m for m in models.KNOWN_MODELS if m["id"] == model_id]
    assert known, f"Expected to find {model_id} in KNOWN_MODELS"
    files = known[0].get("files", [])
    state = models._classify_model_state(str(model_dir), files)
    assert state == "incomplete", (
        f"_classify_model_state should return 'incomplete' after the "
        f"sentinel is written, but got '{state}'"
    )


def test_onnx_load_failure_skips_sentinel_when_files_verify_ok(
    tmp_path, monkeypatch
):
    """If ONNX Runtime fails to load but SHA256 verification reports the
    files are intact, the .verify_failed sentinel must NOT be written.

    This is the guard against a transient ONNXRuntime hiccup (memory
    pressure, mmap race, a pytest monkeypatch from a worktree running
    against the same $HOME) permanently marking a healthy model as
    'Incomplete — repair available'. Left unchecked, every subsequent
    pipeline run told the user to click Repair, and Repair succeeded
    but the sentinel came back on the next transient failure.
    """
    import classifier as classifier_mod
    import config as cfg
    import model_verify
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)
    model_dir = tmp_path / "models" / model_id

    def boom(*args, **kwargs):
        raise RuntimeError(
            "[ONNXRuntimeError] model_path must not be empty. Ensure that "
            "a path is provided when the model is created or loaded."
        )

    monkeypatch.setattr(classifier_mod, "Classifier", boom)
    # Files hash-check clean — the ONNX error is transient, not corruption.
    monkeypatch.setattr(
        model_verify,
        "verify_model",
        lambda *a, **k: model_verify.VerifyResult(ok=True),
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError) as exc:
        run_pipeline_job(job, runner, db_path, ws_id, params)

    sentinel = model_dir / model_verify.VERIFY_FAILED_SENTINEL
    assert not sentinel.exists(), (
        "verified-clean files must not get a .verify_failed sentinel "
        "from a transient ONNXRuntime error — that's what traps users "
        "in the 'Repair never sticks' loop."
    )
    # And the user-facing error should NOT say "click Repair" since the
    # files are fine; it should suggest a retry instead.
    assert "Repair" not in str(exc.value), (
        f"Expected transient-failure message, got: {exc.value}"
    )


# ---------------------------------------------------------------------------
# Multi-model pipeline resilience to individual model failures
# ---------------------------------------------------------------------------


def test_pipeline_continues_when_first_model_fails(tmp_path, monkeypatch):
    """When the first model in a multi-model run fails to load, the pipeline
    must NOT abort.  The second model should still classify photos and the
    pipeline should complete successfully.

    This is the fix for the multi-model pipeline abort bug: previously
    model_loader_stage set abort on ANY preload failure, which killed the
    entire pipeline even when other models were available.
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # The first model ("bioclip-vit-b-16") always fails; the second
    # ("bioclip-2") always succeeds. Use the pretrained_str kwarg to
    # distinguish which model is being loaded.
    construction_calls = []

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pretrained = kwargs.get("pretrained_str", "") or ""
            if "bioclip-vit-b-16" in pretrained:
                raise RuntimeError("simulated first model failure")
            construction_calls.append(kwargs)

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    # Should NOT raise — second model should succeed.
    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # The second model must have been constructed successfully.
    assert len(construction_calls) >= 1, (
        "Expected at least one successful Classifier construction (second model), "
        f"got {len(construction_calls)}"
    )

    # model_loader summary should note the preload failure.
    model_loader_summaries = [
        kwargs.get("summary", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "model_loader" and kwargs.get("status") == "completed"
    ]
    assert model_loader_summaries, "model_loader should complete (not fail)"
    assert "failed to preload" in " ".join(model_loader_summaries)

    # The failing model's per-model classify row should be 'failed'; the
    # surviving model's row should be 'completed'.
    bad_id, good_id = model_ids
    bad_statuses = [
        kwargs.get("status")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == f"classify:{bad_id}" and "status" in kwargs
    ]
    assert "failed" in bad_statuses, (
        f"Failing model's row should be marked failed, got {bad_statuses}"
    )
    good_statuses = [
        kwargs.get("status")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == f"classify:{good_id}" and "status" in kwargs
    ]
    assert "completed" in good_statuses, (
        f"Surviving model's row should be completed, got {good_statuses}"
    )

    # The returned result must record the skipped model info.
    assert isinstance(result, dict)
    classify_result = result.get("stages", {}).get("classify", {})
    assert classify_result.get("models_skipped", 0) >= 1
    assert classify_result.get("models_succeeded", 0) >= 1


def test_pipeline_continues_when_secondary_model_fails(tmp_path, monkeypatch):
    """When the second model in a multi-model run fails to load, the first
    model's results are kept and the pipeline completes with a partial success.
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # The second model ("bioclip-2") always fails; the first succeeds.
    construction_calls = []

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pretrained = kwargs.get("pretrained_str", "") or ""
            if "bioclip-2" in pretrained:
                raise RuntimeError("simulated second model failure")
            construction_calls.append(kwargs)

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    # Should NOT raise — first model succeeded.
    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # The first model must have been constructed.
    assert len(construction_calls) >= 1, (
        f"Expected at least 1 construction call, got {len(construction_calls)}"
    )

    good_id, bad_id = model_ids
    good_statuses = [
        kwargs.get("status")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == f"classify:{good_id}" and "status" in kwargs
    ]
    assert "completed" in good_statuses, (
        f"First (good) model row should be completed, got {good_statuses}"
    )
    bad_statuses = [
        kwargs.get("status")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == f"classify:{bad_id}" and "status" in kwargs
    ]
    assert "failed" in bad_statuses, (
        f"Second (bad) model row should be failed, got {bad_statuses}"
    )

    assert isinstance(result, dict)
    classify_result = result.get("stages", {}).get("classify", {})
    assert classify_result.get("models_skipped", 0) >= 1
    assert classify_result.get("models_succeeded", 0) >= 1


def test_pipeline_single_model_still_aborts_on_failure(tmp_path, monkeypatch):
    """When there is only one model and it fails to load, the pipeline must
    still abort — the resilience logic should NOT swallow single-model errors.
    This preserves the existing behavior tested by
    test_pipeline_raises_when_stage_fails.
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def boom(*args, **kwargs):
        raise RuntimeError("simulated single model failure")

    monkeypatch.setattr(classifier_mod, "Classifier", boom)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=["bioclip-vit-b-16"],
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # model_loader should be marked as failed.
    model_loader_failures = [
        kwargs
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "model_loader" and kwargs.get("status") == "failed"
    ]
    assert model_loader_failures, (
        "Single-model pipeline must mark model_loader as failed"
    )
    assert isinstance(job["result"], dict)
    assert any(
        "model_loader" in e for e in job["result"]["errors"]
    ), f"Expected a model_loader error, got: {job['result']['errors']}"


def test_pipeline_classify_stores_predictions_with_detection_id(
    tmp_path, monkeypatch
):
    """Predictions written by the pipeline classify stage MUST carry a valid
    detection_id. Without it, predictions are orphaned — the workspace-scoped
    skip query (get_existing_prediction_photo_ids) inner-joins on
    detection_id, so every subsequent run re-classifies the same photos
    instead of reusing the stored predictions.

    Regression: pipeline_job built img_batch entries without a detection_id
    key, so _flush_batch stored detection_id=None for every pipeline-written
    prediction.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_with_det = db.add_photo(
        folder_id, "hawk.jpg", ".jpg", 12345, 1_000_000.0
    )
    photo_without_det = db.add_photo(
        folder_id, "empty.jpg", ".jpg", 12346, 1_000_100.0
    )
    _drop_jpeg(folder_path, "hawk.jpg")
    _drop_jpeg(folder_path, "empty.jpg")
    col_id = db.add_collection(
        "Test",
        json.dumps([
            {"field": "photo_ids", "value": [photo_with_det, photo_without_det]}
        ]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # Persist a real detection row for photo_with_det so det_map carries a
    # valid DB id the pipeline can bind predictions to.
    real_det_ids = db.save_detections(
        photo_with_det,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.95, "category": "animal"}],
        detector_model="MegaDetector",
    )
    real_det_id = real_det_ids[0]

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        for p in batch:
            if p["id"] == photo_with_det:
                det_map[p["id"]] = [{
                    "id": real_det_id,
                    "box_x": 0.1, "box_y": 0.1, "box_w": 0.5, "box_h": 0.5,
                    "confidence": 0.95, "category": "animal",
                }]
        return det_map, len(det_map), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        return (
            Image.new("RGB", (32, 32), "white"),
            folders.get(photo["folder_id"], ""),
            os.path.join(folders.get(photo["folder_id"], ""), photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            emb = np.zeros(512, dtype=np.float32)
            return [
                ([{"species": "Red-tailed Hawk", "score": 0.99}], emb)
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # At least one prediction should have been stored.
    preds = db.conn.execute(
        "SELECT id, detection_id, species, classifier_model AS model FROM predictions"
    ).fetchall()
    assert preds, (
        "Pipeline classify stage produced no predictions — test setup did "
        "not exercise the write path."
    )

    # None of those predictions should have a NULL detection_id.
    orphans = [dict(p) for p in preds if p["detection_id"] is None]
    assert not orphans, (
        f"Pipeline wrote predictions with NULL detection_id: {orphans}. "
        "get_existing_prediction_photo_ids filters these out, so the skip "
        "logic never matches and every pipeline run re-classifies everything."
    )

    # Every prediction's detection_id must resolve to a real detection row.
    # Detections are global post-refactor — workspace scoping is through
    # workspace_folders, so we verify that join resolves instead of reading
    # the dropped workspace_id column on detections.
    for p in preds:
        det = db.conn.execute(
            """SELECT d.id, d.photo_id, wf.workspace_id
               FROM detections d
               JOIN photos ph ON ph.id = d.photo_id
               JOIN workspace_folders wf
                 ON wf.folder_id = ph.folder_id
               WHERE d.id = ? AND wf.workspace_id = ?""",
            (p["detection_id"], ws_id),
        ).fetchone()
        assert det is not None, (
            f"Prediction {dict(p)} references detection_id "
            f"{p['detection_id']} which doesn't resolve to a detection in "
            f"workspace {ws_id} via workspace_folders."
        )

    # photo_with_det must be in the skip set so the second run reuses its
    # prediction instead of re-classifying.
    skip_set = db.get_existing_prediction_photo_ids(preds[0]["model"])
    assert photo_with_det in skip_set, (
        f"photo_with_det ({photo_with_det}) missing from skip set {skip_set} — "
        "its prediction is not reachable via the predictions→detections join."
    )

    # photo_without_det must NOT have a prediction or a detection row. The
    # pipeline classify stage is detection-driven: photos without a real
    # MegaDetector hit are skipped, not classified against a synthetic
    # full-image box (earlier versions did that, but it broke
    # extract_masks_stage and caused multi-model duplicate inserts).
    no_det_preds = db.conn.execute(
        """SELECT pr.id FROM predictions pr
           LEFT JOIN detections d ON d.id = pr.detection_id
           WHERE d.photo_id = ? OR pr.detection_id IS NULL""",
        (photo_without_det,),
    ).fetchall()
    assert not no_det_preds, (
        f"Pipeline wrote predictions for photo_without_det: {[dict(r) for r in no_det_preds]}. "
        "Photos without a MegaDetector detection must be skipped by the "
        "pipeline classify stage."
    )
    leftover_dets = db.conn.execute(
        "SELECT id, detector_model FROM detections WHERE photo_id = ?",
        (photo_without_det,),
    ).fetchall()
    assert not leftover_dets, (
        f"Pipeline created spurious detection rows for photo_without_det: "
        f"{[dict(r) for r in leftover_dets]}. No synthetic detections should "
        "be inserted — the photo should just be skipped."
    )


def test_extract_masks_stage_ignores_synthetic_full_image_detections(
    tmp_path, monkeypatch
):
    """extract_masks_stage must treat detector_model='full-image' rows as
    non-detections: they are classify-anchor rows (created by the
    standalone classify path in classify_job.py for photos where
    MegaDetector found nothing), not real subject boxes. Counting them
    toward photos_with_detections hides the "weights missing / no
    detections" diagnostic and drives mask extraction on useless full-frame
    boxes.

    The pipeline classify stage no longer creates synthetic full-image
    detections — but classify_job.py still does, and this filter is the
    last line of defense regardless of the source.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "empty.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "empty.jpg")
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    # Simulate a prior standalone classify run that inserted a full-image
    # detection anchor for this photo.
    db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
          "confidence": 0, "category": "animal"}],
        detector_model="full-image",
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # No real MegaDetector hits in this pipeline pass, either.
    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            emb = np.zeros(512, dtype=np.float32)
            return [
                ([{"species": "Unknown", "score": 0.5}], emb)
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        skip_extract_masks=False,  # exercise extract_masks_stage
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Confirm the pre-existing full-image detection is still the only row
    # for this photo — the pipeline run did not create additional rows.
    all_dets = db.conn.execute(
        "SELECT detector_model FROM detections WHERE photo_id = ?",
        (photo_id,),
    ).fetchall()
    assert all_dets and all(
        d["detector_model"] == "full-image" for d in all_dets
    ), (
        f"Expected only full-image anchor detections, got: "
        f"{[dict(d) for d in all_dets]}"
    )

    # extract_masks_stage should have reported the no-detections diagnostic
    # rather than silently completing with masked=0 masked photos.
    extract_summaries = [
        kwargs.get("summary", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "extract_masks" and kwargs.get("status") in (
            "completed", "failed", "skipped",
        )
    ]
    joined = " ".join(extract_summaries).lower()
    assert "no detections" in joined or "megadetector" in joined, (
        f"extract_masks_stage should surface the no-detections diagnostic "
        f"when every detection row is synthetic, got summaries: "
        f"{extract_summaries}"
    )


def test_extract_masks_stage_warns_when_all_detections_below_threshold(
    tmp_path, monkeypatch
):
    """If a photo has detections but every one is below detector_confidence,
    extract_masks silently completes with masked=0 and the user has no way to
    discover why their unmasked photos were skipped — get_detections returns
    [] at the workspace threshold, so the photo never enters photo_det_map.

    Regression observed in production: 727 of 5054 photos had only
    sub-threshold detections, so extract_masks finished in 0.5s with
    "0 masked, 0 skipped" and no error. Surface a clear diagnostic that
    names the threshold and points at the workaround.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "lowconf.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "lowconf.jpg")
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    # Real MegaDetector hit, but at confidence 0.05 — well below the default
    # 0.2 detector_confidence threshold. The detection row is real, but
    # get_detections() filters it out at read time.
    db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 100, "h": 100},
          "confidence": 0.05, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    # Mark the photo as already detected so the detect stage reuses the
    # cached row instead of re-running MegaDetector against the stub jpeg.
    db.record_detector_run(photo_id, "megadetector-v6", box_count=1)

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            emb = np.zeros(512, dtype=np.float32)
            return [
                ([{"species": "Unknown", "score": 0.5}], emb)
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        skip_extract_masks=False,  # exercise extract_masks_stage
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    extract_summaries = [
        kwargs.get("summary", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "extract_masks" and kwargs.get("status") in (
            "completed", "failed", "skipped",
        )
    ]
    joined = " ".join(extract_summaries).lower()
    assert "below" in joined and (
        "threshold" in joined or "detector_confidence" in joined
    ), (
        f"extract_masks_stage should explain why nothing was masked when all "
        f"detections are sub-threshold; got summaries: {extract_summaries}"
    )


def test_extract_masks_stage_warns_on_mixed_already_masked_and_subthreshold(
    tmp_path, monkeypatch
):
    """Production hit: 4166/5054 photos already had masks (photos_with_detections
    > 0), and the remaining 727 had only sub-threshold detections. The
    existing "no detections" guard requires photos_with_detections == 0, so
    it didn't fire — extract_masks completed silently with "0 masked, 0
    skipped" while 727 unmasked photos sat untouched. The mixed-state guard
    must fire instead.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    # Photo A: already has a mask AND a qualifying detection — drives
    # photos_with_detections > 0 so the existing no-detections guard does
    # NOT fire.
    masked_id = db.add_photo(folder_id, "masked.jpg", ".jpg", 12345, 1_000_000.0)
    _drop_jpeg(folder_path, "masked.jpg")
    db.save_detections(
        masked_id,
        [{"box": {"x": 0, "y": 0, "w": 100, "h": 100},
          "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    db.record_detector_run(masked_id, "megadetector-v6", box_count=1)
    db.update_photo_pipeline_features(
        masked_id, mask_path=str(tmp_path / "mask_a.png"),
    )

    # Photo B: no mask, only sub-threshold detection → should be flagged.
    lowconf_id = db.add_photo(folder_id, "lowconf.jpg", ".jpg", 23456, 1_000_001.0)
    _drop_jpeg(folder_path, "lowconf.jpg")
    db.save_detections(
        lowconf_id,
        [{"box": {"x": 0, "y": 0, "w": 50, "h": 50},
          "confidence": 0.05, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    db.record_detector_run(lowconf_id, "megadetector-v6", box_count=1)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [masked_id, lowconf_id]}]),
    )

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            emb = np.zeros(512, dtype=np.float32)
            return [
                ([{"species": "Unknown", "score": 0.5}], emb)
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        skip_extract_masks=False,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    extract_summaries = [
        kwargs.get("summary", "")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == "extract_masks" and kwargs.get("status") in (
            "completed", "failed", "skipped",
        )
    ]
    joined = " ".join(extract_summaries).lower()
    assert "below" in joined and (
        "threshold" in joined or "detector_confidence" in joined
    ), (
        f"extract_masks_stage should flag the sub-threshold photo even when "
        f"another photo already has a mask; got summaries: {extract_summaries}"
    )


def test_pipeline_rerun_with_existing_prediction_and_bursts_does_not_crash(
    tmp_path, monkeypatch
):
    """Regression: after the detection_id fix made pipeline predictions
    eligible for get_existing_prediction_photo_ids, a second non-reclassify
    run routes skipped photos through the _existing raw_results branch. If
    those raw_results lack a detection_id key, _store_grouped_predictions
    crashes the first time burst grouping kicks in — it calls
    update_prediction_group_info(detection_id=item["detection_id"], ...)
    for every _existing item in a multi-item group.

    This test runs two photos through a pipeline pass that groups them into
    a single burst, then runs the pipeline again: the second run must
    complete without a KeyError on detection_id.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    # Two photos one second apart — burst grouping will fuse them.
    p1 = db.add_photo(folder_id, "a.jpg", ".jpg", 1, 1_000_000.0,
                      timestamp="2026-01-01T12:00:00")
    p2 = db.add_photo(folder_id, "b.jpg", ".jpg", 2, 1_000_001.0,
                      timestamp="2026-01-01T12:00:01")
    _drop_jpeg(folder_path, "a.jpg")
    _drop_jpeg(folder_path, "b.jpg")
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [p1, p2]}]),
    )

    det_p1 = db.save_detections(
        p1, [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )[0]
    det_p2 = db.save_detections(
        p2, [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )[0]

    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    det_rows = {
        p1: [{"id": det_p1, "box_x": 0.1, "box_y": 0.1,
              "box_w": 0.5, "box_h": 0.5, "confidence": 0.9,
              "category": "animal"}],
        p2: [{"id": det_p2, "box_x": 0.1, "box_y": 0.1,
              "box_w": 0.5, "box_h": 0.5, "confidence": 0.9,
              "category": "animal"}],
    }

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {p["id"]: det_rows[p["id"]] for p in batch}
        return det_map, len(det_map), {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        return (
            Image.new("RGB", (32, 32), "white"),
            folders.get(photo["folder_id"], ""),
            os.path.join(folders.get(photo["folder_id"], ""), photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            emb = np.zeros(512, dtype=np.float32)
            return [
                ([{"species": "Red-tailed Hawk", "score": 0.99}], emb)
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=[model_id],
        skip_extract_masks=True,
        skip_regroup=True,
    )

    # First run: stores predictions.
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    preds_after_first = db.conn.execute(
        "SELECT COUNT(*) AS c FROM predictions WHERE detection_id IS NOT NULL"
    ).fetchone()["c"]
    assert preds_after_first >= 2, (
        f"First run should have stored predictions for both photos, got "
        f"{preds_after_first} prediction rows"
    )

    # Second run: every photo hits the _existing branch, and burst grouping
    # calls _store_grouped_predictions with multi-item groups of _existing
    # items. Before the fix, this raised KeyError: 'detection_id'.
    job2 = _make_job()
    run_pipeline_job(job2, FakeRunner(), db_path, ws_id, params)

    assert not job2["errors"], (
        f"Second pipeline run raised errors: {job2['errors']}. "
        "The _existing raw_results dict must carry a detection_id so "
        "_store_grouped_predictions does not crash on bursts of reused "
        "predictions."
    )
    assert job2["status"] != "failed", (
        f"Second pipeline run status is {job2['status']} (expected not "
        "'failed')"
    )


def test_pipeline_step_defs_include_detect_and_per_model_classify(
    tmp_path, monkeypatch
):
    """With multiple models, step_defs should contain one 'detect' row and
    one 'classify:<model_id>' row per model. The detect row must come before
    every classify row so users see detection progress as its own phase."""
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    step_ids = [s["id"] for s in runner.steps_defined]

    assert "detect" in step_ids, (
        f"Expected a standalone 'detect' step in step_defs, got {step_ids}"
    )
    per_model_ids = [
        sid for sid in step_ids if sid.startswith("classify:")
    ]
    assert len(per_model_ids) == len(model_ids), (
        f"Expected one 'classify:<model_id>' step per model (got "
        f"{per_model_ids} for models {model_ids})"
    )
    for mid in model_ids:
        assert f"classify:{mid}" in step_ids, (
            f"Missing classify step for model {mid!r}: {step_ids}"
        )
    # Legacy single 'classify' row must not coexist with per-model rows.
    assert "classify" not in step_ids, (
        f"Legacy 'classify' step should be replaced by per-model rows: {step_ids}"
    )

    detect_idx = step_ids.index("detect")
    for pid in per_model_ids:
        assert step_ids.index(pid) > detect_idx, (
            f"'detect' step must come before classify rows (detect={detect_idx}, "
            f"{pid}={step_ids.index(pid)})"
        )


def test_pipeline_step_defs_cover_every_requested_id_on_partial_resolution(
    tmp_path, monkeypatch
):
    """When only a prefix of requested model ids resolves (e.g. a later id
    isn't downloaded), step_defs must still emit one 'classify:<mid>' row per
    REQUESTED id. Driving row creation off a partial resolved_specs hides the
    later failed ids — their later 'failed' update_step calls then no-op
    silently and the user can't see which model broke.

    Regression test for Codex P2 on PR #566 (step_defs at line 203).
    """
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    # Install the first model as downloaded; second is requested but not
    # downloaded, so resolution raises partway through.
    import models
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))
    _write_fake_model_files(tmp_path / "models" / "bioclip-vit-b-16")
    # "bioclip-2" deliberately NOT installed.
    models.set_active_model("bioclip-vit-b-16")

    params = PipelineParams(
        collection_id=col_id,
        model_ids=["bioclip-vit-b-16", "bioclip-2"],
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    # Resolution failure propagates as a model_loader stage failure, so the
    # pipeline raises. We only care about what was registered in step_defs.
    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    step_ids = [s["id"] for s in runner.steps_defined]
    assert "classify:bioclip-vit-b-16" in step_ids, (
        f"Resolved id should have its own row, got {step_ids}"
    )
    assert "classify:bioclip-2" in step_ids, (
        f"Unresolved-but-requested id must still have a row so its failure "
        f"is visible to the user, got {step_ids}"
    )


def test_pipeline_single_model_gets_per_model_classify_row(tmp_path, monkeypatch):
    """Even a single-model run uses one 'classify:<model_id>' row — labeled
    with the model's display name — for consistency with multi-model runs."""
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_id="bioclip-vit-b-16",
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    step_ids = [s["id"] for s in runner.steps_defined]
    assert "classify:bioclip-vit-b-16" in step_ids, (
        f"Single-model run should still produce a per-model classify row, "
        f"got {step_ids}"
    )
    step_by_id = {s["id"]: s for s in runner.steps_defined}
    label = step_by_id["classify:bioclip-vit-b-16"]["label"]
    assert "bioclip" in label.lower() or "BioCLIP" in label, (
        f"Per-model classify row should be labeled with the model's display "
        f"name, got {label!r}"
    )


def test_pipeline_detect_runs_once_before_any_classifier_loads(
    tmp_path, monkeypatch
):
    """Detection should run as its own pre-pass across all photos BEFORE any
    classifier is constructed, so users see detection as a distinct phase
    rather than interleaved with model 1's classify loop."""
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # Create real photos so collection has something to iterate.
    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_id = db.add_folder(str(photo_dir))
    photo_ids = []
    import json
    for i in range(3):
        img_path = photo_dir / f"p{i}.jpg"
        Image.new("RGB", (64, 64), "red").save(str(img_path))
        photo_ids.append(
            db.add_photo(folder_id, f"p{i}.jpg", ".jpg", 1000 + i, 1_000_000.0)
        )
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    events = []

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        events.append(("detect", [p["id"] for p in batch]))
        return {}, 0, {p["id"] for p in batch}

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            events.append(("classifier_init", kwargs.get("pretrained_str")))

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Every detect event should come strictly before any classifier_init for a
    # *second* model. (Model 1's classifier is allowed to load in parallel
    # via the model_loader stage, but NO model should classify before detect
    # has finished running on all photos.)
    kinds = [e[0] for e in events]
    assert kinds, "expected detect / classifier events to be recorded"
    # At least one detect must have fired before any call into encode_image
    # (which is the actual classification work).  We check: the LAST detect
    # event must be before any classifier is "used" for classification; since
    # encode_image isn't tracked here, we verify that all detect events occur
    # before any classifier_init that corresponds to model 2+.
    classifier_inits = [i for i, k in enumerate(kinds) if k == "classifier_init"]
    detect_events = [i for i, k in enumerate(kinds) if k == "detect"]
    assert detect_events, "expected detect to run"
    last_detect = max(detect_events)
    # All detects should happen before classify actually starts — i.e. before
    # classifier_init for model 2 (model 1 may preload earlier).
    if len(classifier_inits) > 1:
        second_init = classifier_inits[1]
        assert last_detect < second_init, (
            f"Detection pre-pass should complete before model 2 is loaded, "
            f"but saw event order: {kinds}"
        )


def test_pipeline_one_model_fails_to_load_other_model_still_runs(
    tmp_path, monkeypatch
):
    """If the FIRST of two models fails to load, the second must still run
    and its per-model classify row must complete with predictions. The failed
    model's row must be marked 'failed' so users see exactly which model
    broke, not a buried note inside an aggregate summary."""
    import classifier as classifier_mod
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_id = db.add_folder(str(photo_dir))
    import json
    Image.new("RGB", (64, 64), "red").save(str(photo_dir / "x.jpg"))
    photo_id = db.add_photo(folder_id, "x.jpg", ".jpg", 1000, 1_000_000.0)
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)
    bad_id = model_ids[0]
    good_id = model_ids[1]

    class SelectiveClassifier:
        def __init__(self, *args, **kwargs):
            # Fail whenever we're asked to build the BAD model; succeed for
            # the other one. Keyed off the pretrained path so the behavior
            # is stable across however many construction attempts
            # model_loader + classify_stage make.
            pretrained = kwargs.get("pretrained_str", "")
            if bad_id in str(pretrained):
                raise RuntimeError("simulated bad weights for model 1")

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", SelectiveClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Model 1 row should be in failed state.
    bad_statuses = [
        kwargs.get("status")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == f"classify:{bad_id}" and "status" in kwargs
    ]
    assert "failed" in bad_statuses, (
        f"Failed model's classify row should be marked 'failed', got "
        f"status history {bad_statuses}"
    )

    # Good model row should be in completed state.
    good_statuses = [
        kwargs.get("status")
        for (_, step_id, kwargs) in runner.step_updates
        if step_id == f"classify:{good_id}" and "status" in kwargs
    ]
    assert "completed" in good_statuses, (
        f"Surviving model's classify row should complete, got "
        f"status history {good_statuses}"
    )


def test_pipeline_per_model_step_summary_includes_prediction_count(
    tmp_path, monkeypatch
):
    """Each per-model classify row's completion summary should report
    counts (predictions stored, detections reused, etc.) so users can see
    which model found what without reading the aggregate."""
    import classifier as classifier_mod
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_id = db.add_folder(str(photo_dir))
    import json
    Image.new("RGB", (64, 64), "red").save(str(photo_dir / "p.jpg"))
    photo_id = db.add_photo(folder_id, "p.jpg", ".jpg", 1000, 1_000_000.0)
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    for mid in model_ids:
        completed_summaries = [
            kwargs.get("summary", "")
            for (_, step_id, kwargs) in runner.step_updates
            if step_id == f"classify:{mid}"
            and kwargs.get("status") == "completed"
            and "summary" in kwargs
        ]
        assert completed_summaries, (
            f"classify:{mid} row must record a summary on completion"
        )
        summary = completed_summaries[-1]
        assert "prediction" in summary.lower(), (
            f"per-model summary for {mid} should mention prediction counts, "
            f"got {summary!r}"
        )


def test_pipeline_reclassify_purge_deferred_until_a_model_succeeds(
    tmp_path, monkeypatch
):
    """On a reclassify run where every model fails to load, the pre-run
    detection rows MUST NOT be deleted. Deleting them ahead of a
    successful classify would cascade through the predictions FK and
    destroy prior results even though no new predictions were written.

    Regression test for Codex P1 on PR #566.
    """
    import json

    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Seed a prior-run detection so the reclassify purge has something to
    # potentially delete.
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    photo_id = db.add_photo(folder_id, "test.jpg", ".jpg", 12345, 1_000_000.0)
    db.save_detections(
        photo_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # Every Classifier construction raises — simulating the "all models
    # fail to load" case the purge must defend against.
    class AlwaysFailClassifier:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("simulated catastrophic load failure")

    monkeypatch.setattr(classifier_mod, "Classifier", AlwaysFailClassifier)

    # Snapshot pre-run detection row count so we can assert it survived.
    pre_count = db.conn.execute(
        "SELECT COUNT(*) AS c FROM detections"
    ).fetchone()["c"]
    assert pre_count >= 1, "fixture should have inserted at least 1 row"

    params = PipelineParams(
        collection_id=col_id,
        model_ids=["bioclip-vit-b-16", "bioclip-2"],
        reclassify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The original detection row MUST still exist: no model succeeded, so
    # the purge must not have fired.
    post = db.conn.execute(
        "SELECT COUNT(*) AS c FROM detections WHERE id = ?",
        (db.conn.execute(
            "SELECT id FROM detections LIMIT 1"
        ).fetchone()["id"],),
    ).fetchone()
    # Simpler: just confirm some prior detections survived.
    survivors = db.conn.execute(
        "SELECT COUNT(*) AS c FROM detections WHERE detector_model != 'full-image'"
    ).fetchone()["c"]
    assert survivors >= 1, (
        "reclassify must not purge pre-run detection rows when every model "
        "failed to load — it would cascade-destroy prior predictions "
        f"(survivors={survivors})"
    )


def test_pipeline_fatal_error_does_not_overwrite_completed_model_rows(
    tmp_path, monkeypatch
):
    """When classify_stage hits a fatal exception AFTER one model has
    already finished, the completed model's `classify:<id>` row must stay
    `completed` — not be rewritten to `failed` by the catch-all error
    handler. Otherwise per-model status is misreported.

    Regression test for Codex P2 on PR #566.
    """
    import json

    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    folder_id = db.add_folder(str(photo_dir))
    Image.new("RGB", (64, 64), "red").save(str(photo_dir / "p.jpg"))
    photo_id = db.add_photo(folder_id, "p.jpg", ".jpg", 1000, 1_000_000.0)
    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)
    first_id, second_id = model_ids

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def encode_image(self, *args, **kwargs):
            import numpy as np
            return np.zeros(512, dtype=np.float32)

        def classify_with_embedding(self, img, threshold=0):
            import numpy as np
            return [{"species": "Robin", "score": 0.9}], np.zeros(
                512, dtype=np.float32,
            )

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            zero = np.zeros(512, dtype=np.float32)
            return [(
                [{"species": "Robin", "score": 0.9}],
                zero,
            ) for _ in images]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    # Let the first model's grouping/storage succeed normally, then blow
    # up when the SECOND model asks _store_grouped_predictions to run.
    call_count = {"n": 0}
    original_store = classify_job._store_grouped_predictions

    def maybe_explode(raw_results, job_id, model_name, *args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] >= 2:
            raise RuntimeError("simulated mid-loop fatal after model 1")
        return original_store(raw_results, job_id, model_name, *args, **kwargs)

    monkeypatch.setattr(
        classify_job, "_store_grouped_predictions", maybe_explode,
    )

    params = PipelineParams(
        collection_id=col_id,
        model_ids=model_ids,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # First model's row must end in 'completed' and stay that way; the
    # fatal handler must NOT have overwritten it with 'failed'.
    first_final = None
    for (_, step_id, kwargs) in runner.step_updates:
        if step_id == f"classify:{first_id}" and "status" in kwargs:
            first_final = kwargs["status"]
    assert first_final == "completed", (
        f"First model's row should remain 'completed' after a later fatal "
        f"error, got final status {first_final!r}"
    )


def test_pipeline_loader_abort_finalizes_detect_and_classify_rows(
    tmp_path, monkeypatch
):
    """When model_loader_stage sets abort (single-model preload failure),
    the phase dispatcher must still invoke detect_stage and classify_stage
    so their step rows reach a terminal status. Without this, the newly
    added `detect` and `classify:<id>` rows stay `pending` forever on a
    loader-triggered failure, which is exactly the scenario these rows
    were added to clarify.

    Regression test for Codex P2 on PR #566 (line 1781).
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # Single-model run where construction always fails — this triggers
    # model_loader_stage's fatal path and sets abort before detect_stage.
    def boom(*args, **kwargs):
        raise RuntimeError("simulated single-model preload failure")

    monkeypatch.setattr(classifier_mod, "Classifier", boom)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # The `detect` and every `classify:<id>` row must reach a terminal
    # state. A pending status means the jobs view would display an
    # indeterminate spinner for a run that has already finished — the
    # exact bug this test guards against.
    terminal = {"completed", "failed", "skipped"}
    steps_of_interest = [
        s["id"] for s in runner.steps_defined
        if s["id"] == "detect" or s["id"].startswith("classify:")
    ]
    assert steps_of_interest, (
        "test precondition: expected detect + classify rows in step_defs"
    )
    for sid in steps_of_interest:
        statuses = [
            kw.get("status")
            for (_, s, kw) in runner.step_updates
            if s == sid and "status" in kw
        ]
        final = statuses[-1] if statuses else None
        assert final in terminal, (
            f"Step {sid!r} must reach a terminal status on loader-triggered "
            f"abort, got {final!r} (history={statuses})"
        )


def test_pipeline_loader_failure_marks_classify_rows_failed_not_skipped(
    tmp_path, monkeypatch
):
    """When model_loader_stage fails (e.g. single-model preload failure or
    id resolution failure), classify_stage's early-skip branch must finalize
    the per-model rows as 'failed' — NOT 'completed' with summary='Skipped'.

    Otherwise the failed model is misreported as a clean skip, which hides
    the per-model failure context the row split is meant to surface.

    Regression test for Codex P2 on PR #566 (pipeline_job.py:1173).
    """
    import classifier as classifier_mod
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    col_id = db.add_collection("Test", "[]")

    _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # Single-model run where construction always fails → model_loader_stage
    # catches the error, sets abort, and marks itself failed.
    def boom(*args, **kwargs):
        raise RuntimeError("simulated single-model preload failure")

    monkeypatch.setattr(classifier_mod, "Classifier", boom)

    params = PipelineParams(
        collection_id=col_id,
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    import pytest
    with pytest.raises(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    classify_rows = [
        (step_id, kwargs)
        for (_, step_id, kwargs) in runner.step_updates
        if step_id.startswith("classify:") and "status" in kwargs
    ]
    assert classify_rows, (
        "test precondition: expected at least one classify:<id> update"
    )
    for step_id, kwargs in classify_rows:
        # The final status on a loader-failure abort must be 'failed', not
        # 'completed' (which would render as a clean skipped row).
        assert kwargs["status"] == "failed", (
            f"Row {step_id!r} should be 'failed' after loader aborted the "
            f"pipeline, got status={kwargs['status']!r}, "
            f"summary={kwargs.get('summary')!r}"
        )


# ---------------------------------------------------------------------------
# Failure rollup — per-file failures surface at the stage/job level
# ---------------------------------------------------------------------------


def _make_photo_dir(tmp_path, n):
    """Drop n tiny JPEGs in a fresh dir and return it."""
    from PIL import Image
    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for i in range(n):
        img = Image.new("RGB", (40, 40), "red")
        img.save(str(photo_dir / f"p_{i}.jpg"))
    return photo_dir


def test_thumbnail_failures_flip_stage_status_to_failed(tmp_path, monkeypatch):
    """If any thumbnail fails, the stage status must be 'failed' (not 'completed'),
    even when some thumbnails succeeded. Mixed-outcome rollups are 'failed'."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    photo_dir = _make_photo_dir(tmp_path, 4)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Make every second thumbnail "fail" by returning None.
    import thumbnails as thumbnails_mod
    real_gen = thumbnails_mod.generate_thumbnail
    call_count = {"n": 0}

    def flaky_gen(photo_id, photo_path, cache_dir, size=300):
        call_count["n"] += 1
        if call_count["n"] % 2 == 0:
            return None
        return real_gen(photo_id, photo_path, cache_dir, size=size)

    monkeypatch.setattr(thumbnails_mod, "generate_thumbnail", flaky_gen)

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    job = _make_job()
    runner = FakeRunner()

    pipeline_result = None
    try:
        pipeline_result = run_pipeline_job(job, runner, db_path, ws_id, params)
    except RuntimeError:
        # Expected: pipeline raises when a stage ends up in 'failed'.
        pipeline_result = job.get("result")

    thumb_result = pipeline_result["stages"]["thumbnails"]
    assert thumb_result["failed"] > 0, (
        f"Test setup bug: expected thumbnail failures. Result: {thumb_result}"
    )
    assert thumb_result["generated"] > 0, (
        f"Test setup bug: expected some thumbnail successes. Result: {thumb_result}"
    )

    # Inspect the final stages status as updated on the job runner.
    final_thumb_updates = [
        kwargs for (_, step, kwargs) in runner.step_updates
        if step == "thumbnails" and kwargs.get("status")
    ]
    final_status = final_thumb_updates[-1]["status"]
    assert final_status == "failed", (
        f"Mixed-outcome rollup must report 'failed', got {final_status!r}. "
        f"Result: {thumb_result}"
    )


def test_thumbnail_failures_append_rollup_error(tmp_path, monkeypatch):
    """Per-file thumbnail failures must surface as exactly one rollup entry
    in the pipeline errors list — not N per-file entries."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    photo_dir = _make_photo_dir(tmp_path, 3)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # All thumbnails fail.
    import thumbnails as thumbnails_mod
    monkeypatch.setattr(
        thumbnails_mod, "generate_thumbnail",
        lambda photo_id, photo_path, cache_dir, size=300: None,
    )

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    job = _make_job()

    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, FakeRunner(), db_path, ws_id, params)

    errors = job["errors"]
    thumb_errors = [e for e in errors if "thumbnail" in e.lower()]
    assert len(thumb_errors) == 1, (
        f"Expected exactly one rollup entry for thumbnail failures, got "
        f"{len(thumb_errors)}: {thumb_errors}"
    )
    assert "3" in thumb_errors[0], (
        f"Rollup should mention the failure count (3), got: {thumb_errors[0]!r}"
    )


def test_thumbnail_progress_counter_includes_failed(tmp_path, monkeypatch):
    """stages['thumbnails']['count'] must include failed items so the UI
    progress bar reflects work actually attempted, not just successes."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    photo_dir = _make_photo_dir(tmp_path, 2)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    import thumbnails as thumbnails_mod
    monkeypatch.setattr(
        thumbnails_mod, "generate_thumbnail",
        lambda photo_id, photo_path, cache_dir, size=300: None,
    )

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    job = _make_job()
    runner = FakeRunner()

    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # stages dict's own counter (shown on the dashboard) must include failed.
    # Apr 5 bug: scan reported "1472 photos" but stages['thumbnails']['count']
    # sat at 0 (generated + skipped = 0 + 0) despite all 1472 files processed.
    thumb_progress_events = [
        data for (_, evt, data) in runner.events
        if evt == "progress" and data.get("stage_id") == "thumbnails"
    ]
    assert thumb_progress_events, "No thumbnails progress events emitted"
    last = thumb_progress_events[-1]
    thumb_stage_count = last["stages"]["thumbnails"].get("count", 0)
    assert thumb_stage_count > 0, (
        f"stages['thumbnails']['count'] must include failed items (was {thumb_stage_count}). "
        f"Last event stages: {last['stages']}"
    )


def test_pipeline_with_snapshot_scans_only_snapshot_folders(tmp_path, monkeypatch):
    """When source_snapshot_id is provided, the scan stage must walk only the
    parent directories of the snapshot's files — sibling folders registered
    with the workspace but not in the snapshot must NOT be scanned."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Two sibling folders each with one JPEG. Only folder A is in the snapshot.
    folder_a = tmp_path / "folderA"
    folder_b = tmp_path / "folderB"
    folder_a.mkdir()
    folder_b.mkdir()
    folder_a_id = db.add_folder(str(folder_a))
    folder_b_id = db.add_folder(str(folder_b))
    _drop_jpeg(str(folder_a), "IMG_001.JPG")
    _drop_jpeg(str(folder_b), "IMG_002.JPG")

    snap_id = db.create_new_images_snapshot([str(folder_a / "IMG_001.JPG")])

    params = PipelineParams(
        source_snapshot_id=snap_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()

    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Verify via DB state: folder A has its photo ingested, folder B does not.
    verify_db = Database(db_path)
    verify_db.set_active_workspace(ws_id)
    a_photos = verify_db.conn.execute(
        "SELECT filename FROM photos WHERE folder_id = ?", (folder_a_id,),
    ).fetchall()
    b_photos = verify_db.conn.execute(
        "SELECT filename FROM photos WHERE folder_id = ?", (folder_b_id,),
    ).fetchall()
    assert [r["filename"] for r in a_photos] == ["IMG_001.JPG"], (
        f"folder A should have its snapshot file ingested, got {list(a_photos)}"
    )
    assert list(b_photos) == [], (
        f"folder B must NOT be scanned (not in snapshot), got {list(b_photos)}"
    )


def test_pipeline_snapshot_excludes_late_arriving_files(tmp_path, monkeypatch):
    """Files that land in a registered folder AFTER a snapshot is captured
    must still be scanned (we walk the folder), but downstream stages
    (classify, extract_masks, regroup) must be constrained to the snapshot's
    photo-id set. Verified via DB state: only the early (snapshot) photo
    should have a predictions row after the pipeline completes.
    """
    import classifier as classifier_mod
    import classify_job
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder = tmp_path / "photos"
    folder.mkdir()
    db.add_folder(str(folder))

    # "Early" file — exists at snapshot time, goes into the snapshot.
    # Use distinct pixel content so the scanner's hash-based duplicate
    # resolver doesn't collapse the two files into one photo row (same
    # 16x16 black rectangle hashes to the same bytes).
    Image.new("RGB", (16, 16), (10, 10, 10)).save(
        str(folder / "IMG_early.JPG")
    )
    snap_id = db.create_new_images_snapshot([str(folder / "IMG_early.JPG")])

    # "Late" file — arrives after the snapshot but before the pipeline runs.
    # The scanner will ingest it (same folder), but downstream stages must
    # skip it.
    Image.new("RGB", (16, 16), (200, 50, 50)).save(
        str(folder / "IMG_late.JPG")
    )

    # Wire up fake classifier + detect_batch so classify_stage actually runs
    # and writes a predictions row for whatever photo it sees.
    model_id = _setup_fake_downloaded_model(tmp_path, monkeypatch)

    # detect_stage calls ensure_megadetector_weights() whenever any photo
    # lacks a cached detection — which is every fresh-scan run. Short-circuit
    # to avoid a real network download in the test.
    import detector as detector_mod
    monkeypatch.setattr(
        detector_mod, "ensure_megadetector_weights",
        lambda progress_callback=None: "/tmp/fake-md-weights.onnx",
    )

    # Map filename → synthetic detection_id; we need a real detection row per
    # photo fed to classify so _flush_batch has a valid FK to bind to.
    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        det_map = {}
        processed = set()
        for p in batch:
            det_ids = db_.save_detections(
                p["id"],
                [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
                  "confidence": 0.95, "category": "animal"}],
                detector_model="MegaDetector",
            )
            det_map[p["id"]] = [{
                "id": det_ids[0],
                "box_x": 0.1, "box_y": 0.1, "box_w": 0.5, "box_h": 0.5,
                "confidence": 0.95, "category": "animal",
            }]
            processed.add(p["id"])
        return det_map, len(det_map), processed

    monkeypatch.setattr(classify_job, "_detect_batch", fake_detect_batch)

    def fake_prepare_image(photo, folders, detection, vireo_dir=None):
        return (
            Image.new("RGB", (32, 32), "white"),
            folders.get(photo["folder_id"], ""),
            os.path.join(folders.get(photo["folder_id"], ""), photo["filename"]),
        )

    monkeypatch.setattr(classify_job, "_prepare_image", fake_prepare_image)

    class FakeClassifier:
        def __init__(self, *args, **kwargs):
            pass

        def classify_batch_with_embedding(self, images, threshold=0):
            import numpy as np
            emb = np.zeros(512, dtype=np.float32)
            return [
                ([{"species": "Red-tailed Hawk", "score": 0.99}], emb)
                for _ in images
            ]

    monkeypatch.setattr(classifier_mod, "Classifier", FakeClassifier)

    params = PipelineParams(
        source_snapshot_id=snap_id,
        model_ids=[model_id],
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Verify via DB: both files were scanned (scan walks the folder), but
    # only the early one should have a prediction row.
    verify_db = Database(db_path)
    verify_db.set_active_workspace(ws_id)

    scanned = {
        r["filename"] for r in verify_db.conn.execute(
            "SELECT filename FROM photos"
        ).fetchall()
    }
    assert scanned == {"IMG_early.JPG", "IMG_late.JPG"}, (
        f"scan should ingest both files in the folder, got {scanned}"
    )

    classified_names = {
        r["filename"] for r in verify_db.conn.execute(
            """SELECT p.filename
                 FROM predictions pr
                 JOIN detections d ON d.id = pr.detection_id
                 JOIN photos p ON p.id = d.photo_id"""
        ).fetchall()
    }
    assert "IMG_early.JPG" in classified_names, (
        f"early (snapshot) file should be classified, got {classified_names}"
    )
    assert "IMG_late.JPG" not in classified_names, (
        f"late (post-snapshot) file must NOT be classified, got "
        f"{classified_names}"
    )


def test_pipeline_snapshot_collapses_overlapping_scan_roots(tmp_path, monkeypatch):
    """When the snapshot contains files at both a folder and a nested subfolder
    (e.g. /root/a.jpg and /root/sub/b.jpg), deriving scan roots naively would
    produce overlapping paths (/root and /root/sub). The scanner would then
    walk the subtree twice. params.sources must be collapsed to the minimal
    non-overlapping ancestor set."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    root = tmp_path / "root"
    sub = root / "sub"
    sub.mkdir(parents=True)
    db.add_folder(str(root))
    db.add_folder(str(sub))

    top_path = root / "a.jpg"
    sub_path = sub / "b.jpg"
    _drop_jpeg(str(root), "a.jpg")
    _drop_jpeg(str(sub), "b.jpg")

    snap_id = db.create_new_images_snapshot([str(top_path), str(sub_path)])

    # Spy on scanner.scan to count how many distinct roots it walks.
    import scanner as scanner_mod
    scan_calls = []
    original_scan = scanner_mod.scan

    def spy_scan(root_path, db_, **kwargs):
        scan_calls.append(root_path)
        return original_scan(root_path, db_, **kwargs)

    monkeypatch.setattr(scanner_mod, "scan", spy_scan)

    params = PipelineParams(
        source_snapshot_id=snap_id,
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # The nested path is a descendant of the top path; the scanner walks root
    # recursively, so sub must NOT be re-scanned as a separate root.
    assert str(root) in scan_calls, f"top root must be scanned, got {scan_calls}"
    assert str(sub) not in scan_calls, (
        f"sub is a descendant of root and must not be scanned separately, "
        f"got {scan_calls}"
    )


def test_collapse_scan_roots_handles_filesystem_root():
    """Unit test for the collapse helper's edge case where a kept root IS
    the filesystem root ('/' on POSIX, 'C:\\' on Windows). The naive
    `kept + os.sep` prefix becomes '//' for '/' and fails to match child
    paths like '/sub'. Descendants of the filesystem root must still be
    collapsed away."""
    from pipeline_job import _collapse_scan_roots

    collapsed = _collapse_scan_roots([os.sep, os.path.join(os.sep, "sub")])
    assert collapsed == [os.sep], (
        f"descendants of filesystem root must collapse, got {collapsed}"
    )

    # Non-overlapping peers are preserved.
    a = os.path.join(os.sep, "a")
    b = os.path.join(os.sep, "b")
    collapsed = _collapse_scan_roots([a, b])
    assert collapsed == sorted([a, b]), (
        f"peers must both be kept, got {collapsed}"
    )

    # Prefix-but-not-descendant isn't collapsed (/foo vs /foobar).
    foo = os.path.join(os.sep, "foo")
    foobar = os.path.join(os.sep, "foobar")
    collapsed = _collapse_scan_roots([foo, foobar])
    assert collapsed == sorted([foo, foobar]), (
        f"/foo and /foobar are peers, got {collapsed}"
    )


def test_pipeline_miss_stage_skipped_when_regroup_fails(tmp_path, monkeypatch):
    """miss_stage depends on burst_id written by regroup. If regroup_stage
    throws, running miss_stage would overwrite miss_* flags with stale
    context during an already-failing job. The gate must check the
    stage's failed status, not just the global abort flag (regroup_stage
    marks itself failed without setting abort)."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ("a.jpg", "b.jpg"):
        Image.new("RGB", (16, 16), "black").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Force regroup to fail before it finishes. pipeline_job imports
    # run_full_pipeline lazily inside regroup_stage; patch at module level.
    import pipeline as pipeline_mod

    def _boom(*args, **kwargs):
        raise RuntimeError("synthetic regroup failure")

    monkeypatch.setattr(pipeline_mod, "run_full_pipeline", _boom)

    # Also mark the single photo with an arbitrary miss_computed_at so we
    # can detect mutation by miss_stage.
    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        # Intentionally NOT skip_regroup — regroup must be attempted and fail.
    )

    runner = FakeRunner()
    job = _make_job()

    import contextlib
    with contextlib.suppress(Exception):
        result = run_pipeline_job(job, runner, db_path, ws_id, params)

    # Inspect the stages dict from the last progress event — if miss_stage
    # ran, it would transition out of "pending" to "running"/"completed"/
    # "failed"/"skipped". The fix must leave it "pending" (never entered).
    progress_events = [
        data for (_, evt, data) in runner.events
        if evt == "progress" and "stages" in data
    ]
    assert progress_events, "pipeline emitted no progress events"
    last_stages = progress_events[-1]["stages"]
    assert last_stages["regroup"]["status"] == "failed"
    # Miss stage must not have mutated any miss_* row. Verify by reading
    # miss_computed_at on the scanned photos — all should still be NULL.
    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    rows = db2.conn.execute(
        "SELECT miss_computed_at FROM photos"
    ).fetchall()
    assert rows, "scan produced no photo rows"
    for r in rows:
        assert r["miss_computed_at"] is None, (
            "miss_stage ran after regroup failure and overwrote miss state"
        )


def test_pipeline_regroup_stamps_workspace_group_fingerprint(tmp_path, monkeypatch):
    """When regroup_stage completes successfully, last_grouped_at and
    last_group_fingerprint must be written on the active workspace so the
    pipeline page can render "fresh" instead of "Outdated"."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ("a.jpg", "b.jpg"):
        Image.new("RGB", (16, 16), "black").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Stub run_full_pipeline + save_results so regroup completes deterministically.
    import pipeline as pipeline_mod

    def _ok_run(photos, config=None):
        return {"summary": {"groups": 1}, "photos": photos}

    def _no_save(results, cache_dir, workspace_id):
        return None

    monkeypatch.setattr(pipeline_mod, "run_full_pipeline", _ok_run)
    monkeypatch.setattr(pipeline_mod, "save_results", _no_save)

    # Regroup uses load_photo_features to decide whether to skip on empty.
    # Return a single fake photo so the success branch (which stamps the
    # fingerprint) is exercised rather than the "no photos to group" branch.
    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda thread_db, collection_id=None, config=None: [{"id": 1}],
    )

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        # Regroup must be attempted and succeed.
    )

    runner = FakeRunner()
    job = _make_job()

    with contextlib.suppress(Exception):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # Verify the workspace row now has last_grouped_at + last_group_fingerprint
    # set to the values that compute_group_fingerprint() yields for this config.
    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    row = db2.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint FROM workspaces WHERE id=?",
        (ws_id,),
    ).fetchone()
    assert row["last_grouped_at"] is not None, (
        "regroup completed but workspace fingerprint timestamp was not stamped"
    )
    from pipeline import compute_group_fingerprint
    effective = db2.get_effective_config(cfg.load())
    assert row["last_group_fingerprint"] == compute_group_fingerprint(effective)



def test_pipeline_regroup_does_not_stamp_for_partial_run(tmp_path, monkeypatch):
    """A regroup run that filtered out workspace photos via exclude_photo_ids
    must NOT stamp last_group_fingerprint — those excluded photos are still
    ungrouped under the current settings, so claiming workspace-level
    freshness would let the pipeline page hide a real stale state."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ("a.jpg", "b.jpg"):
        Image.new("RGB", (16, 16), "black").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    import pipeline as pipeline_mod

    def _ok_run(photos, config=None):
        return {"summary": {"groups": 1}, "photos": photos}

    monkeypatch.setattr(pipeline_mod, "run_full_pipeline", _ok_run)
    monkeypatch.setattr(pipeline_mod, "save_results",
                        lambda results, cache_dir, workspace_id: None)
    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda thread_db, collection_id=None, config=None: [{"id": 1}],
    )

    # Pass an arbitrary exclude_photo_ids — its mere presence signals that
    # the regroup ran on a filtered subset.
    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        exclude_photo_ids={999},
    )

    runner = FakeRunner()
    job = _make_job()

    with contextlib.suppress(Exception):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    db2 = Database(db_path)
    db2.set_active_workspace(ws_id)
    row = db2.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint FROM workspaces WHERE id=?",
        (ws_id,),
    ).fetchone()
    assert row["last_grouped_at"] is None, (
        "partial regroup (exclude_photo_ids set) wrongly stamped "
        "last_grouped_at — pipeline page will falsely report 'fresh'"
    )
    assert row["last_group_fingerprint"] is None


def test_pipeline_regroup_invalidates_stamp_on_partial_run(tmp_path, monkeypatch):
    """A partial regroup overwrites the workspace's pipeline_results_ws*.json
    cache with subset output via save_results. Any pre-existing
    last_group_fingerprint would now point at a cache that no longer
    reflects the full workspace, so the pipeline page would falsely report
    Group as 'done-prior'. The stamp must be invalidated (NULL'd) on
    partial runs so pipeline_plan treats the resulting state as outdated."""
    import config as cfg
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()
    for name in ("a.jpg", "b.jpg"):
        Image.new("RGB", (16, 16), "black").save(str(photo_dir / name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Pre-stamp a fingerprint as if a prior FULL workspace regroup had
    # completed cleanly. The partial run we're about to do must wipe
    # this, since save_results will overwrite the cache with subset output.
    db.set_workspace_group_state(
        ws_id, fingerprint="pre-existing-from-full-run", when_ts=1714579200,
    )

    import pipeline as pipeline_mod
    monkeypatch.setattr(
        pipeline_mod, "run_full_pipeline",
        lambda photos, config=None: {"summary": {"groups": 1}, "photos": photos},
    )
    monkeypatch.setattr(pipeline_mod, "save_results",
                        lambda results, cache_dir, workspace_id: None)
    monkeypatch.setattr(
        pipeline_mod, "load_photo_features",
        lambda thread_db, collection_id=None, config=None: [{"id": 1}],
    )

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        exclude_photo_ids={999},
    )

    runner = FakeRunner()
    job = _make_job()

    with contextlib.suppress(Exception):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    db2 = Database(db_path)
    row = db2.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint FROM workspaces WHERE id=?",
        (ws_id,),
    ).fetchone()
    assert row["last_group_fingerprint"] is None, (
        "partial regroup left stale fingerprint behind — pipeline page would "
        "falsely report Group as 'done-prior' against a subset-only cache"
    )
    assert row["last_grouped_at"] is None



# --- Weighted overall progress ---------------------------------------------

def _empty_stages():
    return {name: {"status": "pending", "count": 0} for name in STAGE_WEIGHTS}


def test_stage_fraction_pending_is_zero():
    assert _stage_fraction({"status": "pending", "count": 0}) == 0.0


def test_stage_fraction_completed_is_one():
    assert _stage_fraction({"status": "completed", "count": 5, "total": 10}) == 1.0


def test_stage_fraction_skipped_is_one():
    """Skipped stages are "done" for overall-progress purposes — their
    weight has been paid out, so don't stall the bar at the last skip."""
    assert _stage_fraction({"status": "skipped"}) == 1.0


def test_stage_fraction_running_uses_count_over_total():
    assert _stage_fraction({"status": "running", "count": 25, "total": 100}) == 0.25


def test_stage_fraction_running_without_total_is_zero():
    """A running stage that hasn't yet reported a total can't compute a
    fraction; report 0 rather than dividing by zero or claiming completion."""
    assert _stage_fraction({"status": "running", "count": 5}) == 0.0


def test_stage_fraction_clamps_to_one():
    """Stage counters sometimes overshoot total (last batch rounding)."""
    assert _stage_fraction({"status": "running", "count": 105, "total": 100}) == 1.0


def test_stage_fraction_failed_counts_partial_work():
    """Stages like classify can process most items and then mark themselves
    'failed' due to per-item errors. Their partial completion must still
    count toward the weighted overall — otherwise the bar drops sharply
    when a near-done heavy stage fails."""
    assert _stage_fraction({"status": "failed", "count": 80, "total": 100}) == 0.8


def test_stage_fraction_failed_without_progress_is_zero():
    """A failed stage with no count/total contributes nothing, same as
    pending/unknown."""
    assert _stage_fraction({"status": "failed"}) == 0.0


def test_stage_fraction_failed_clamps_to_one():
    assert _stage_fraction({"status": "failed", "count": 105, "total": 100}) == 1.0


def test_weighted_progress_all_pending_is_zero():
    current, total = _weighted_progress(_empty_stages())
    assert current == 0
    assert total == sum(STAGE_WEIGHTS.values())


def test_weighted_progress_all_completed_is_full():
    stages = {name: {"status": "completed"} for name in STAGE_WEIGHTS}
    current, total = _weighted_progress(stages)
    assert current == total
    assert total == sum(STAGE_WEIGHTS.values())


def test_weighted_progress_fast_stage_done_heavy_pending():
    """After a fast stage finishes and a heavy one hasn't started, the bar
    should reflect the fast stage's small weight — NOT 100%. This is the
    bug the helper fixes: previously the last-pushed stage-local current/total
    dominated the overall bar."""
    stages = _empty_stages()
    stages["ingest"]["status"] = "completed"  # weight 2
    stages["scan"]["status"] = "completed"    # weight 8
    # classify (weight 30) still pending
    current, total = _weighted_progress(stages)
    pct = current / total * 100
    assert pct < 15, f"Expected <15% with only ingest+scan done, got {pct:.1f}%"


def test_weighted_progress_running_stage_partial():
    stages = _empty_stages()
    stages["ingest"]["status"] = "completed"
    stages["scan"]["status"] = "completed"
    stages["thumbnails"]["status"] = "completed"
    stages["previews"]["status"] = "completed"
    stages["model_loader"]["status"] = "completed"
    stages["detect"]["status"] = "completed"
    stages["classify"].update(status="running", count=50, total=100)
    current, total = _weighted_progress(stages)
    # ingest+scan+thumbs+previews+model_loader+detect = 2+8+6+6+2+15 = 39
    # classify half-done = 15
    # total weight sum via STAGE_WEIGHTS
    expected_done = 39 + 15
    assert current == expected_done
    assert total == sum(STAGE_WEIGHTS.values())


def test_weighted_progress_does_not_round_up_to_full():
    """Overall must not report `current == total` before every stage is
    actually complete. int(round(done)) would report 100/100 when done is
    99.5+, falsely showing 100% while a stage is still running."""
    stages = _empty_stages()
    for name in STAGE_WEIGHTS:
        stages[name]["status"] = "completed"
    # Override the last stage to running at 99/100. Contribution = 5.94
    # (weight 6 * 0.99); others fully completed = 94. Total done = 99.94.
    # A naive round(99.94) = 100 would hit total and falsely signal done.
    stages["regroup"].update(status="running", count=99, total=100)
    current, total = _weighted_progress(stages)
    assert current < total, (
        f"overall hit total ({current}/{total}) before last stage completed"
    )


def test_weighted_progress_does_not_round_up_with_failed_stage():
    """Same premature-100 guard, but via a failed stage that finished
    processing most items. If failed now counts partial work, the weighted
    sum can land at 99.x when only one stage hasn't fully completed."""
    stages = _empty_stages()
    for name in STAGE_WEIGHTS:
        stages[name]["status"] = "completed"
    stages["regroup"].update(status="failed", count=99, total=100)
    current, total = _weighted_progress(stages)
    assert current < total, (
        f"overall hit total ({current}/{total}) with a non-complete stage"
    )


def test_weighted_progress_monotonic_through_pipeline():
    """Completing stages in order should produce a monotonically increasing
    overall percentage — no drops between phases."""
    stages = _empty_stages()
    order = ["ingest", "scan", "thumbnails", "previews", "model_loader",
             "detect", "classify", "extract_masks", "eye_keypoints", "regroup",
             "misses"]
    last_pct = -1.0
    for name in order:
        stages[name]["status"] = "completed"
        current, total = _weighted_progress(stages)
        pct = current / total * 100
        assert pct > last_pct, f"Progress went backwards at {name}: {last_pct} -> {pct}"
        last_pct = pct
    assert last_pct == 100.0


def test_pipeline_thumbnail_stage_records_thumb_path_in_db(tmp_path, monkeypatch):
    """Each successful generate_thumbnail in the pipeline thumbnail_stage must
    set photos.thumb_path so the dashboard's coverage query reflects it.
    Without this, scanning produces JPEGs on disk but the column stays NULL
    and "0 of N thumbnails made" is reported forever."""
    import config as cfg
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    photo_dir = _make_photo_dir(tmp_path, 3)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    params = PipelineParams(
        source=str(photo_dir),
        skip_classify=True,
        skip_extract_masks=True,
        skip_regroup=True,
    )
    job = _make_job()
    runner = FakeRunner()

    with contextlib.suppress(RuntimeError):
        run_pipeline_job(job, runner, db_path, ws_id, params)

    # Re-open the DB connection — the pipeline runs on a thread with its
    # own connection and we want the committed view.
    db2 = Database(db_path)
    rows = db2.conn.execute(
        "SELECT id, thumb_path FROM photos ORDER BY id"
    ).fetchall()
    assert len(rows) == 3
    for r in rows:
        assert r["thumb_path"] is not None, (
            f"photo {r['id']} has thumb_path=NULL after pipeline ran"
        )
        assert r["thumb_path"] == f"{r['id']}.jpg", (
            f"thumb_path should be the bare filename '{r['id']}.jpg', "
            f"got {r['thumb_path']!r}"
        )


def test_update_stages_emits_weighted_current_total():
    """_update_stages must send the weighted overall to push_event instead
    of hardcoded 0/0. This is what makes the 'Overall %' visible in the UI."""
    from pipeline_job import _update_stages

    stages = _empty_stages()
    stages["ingest"]["status"] = "completed"
    stages["scan"]["status"] = "running"
    stages["scan"]["count"] = 50
    stages["scan"]["total"] = 100

    runner = FakeRunner()
    _update_stages(runner, "job-x", stages)
    assert runner.events, "no events pushed"
    _, evt, data = runner.events[-1]
    assert evt == "progress"
    assert data["total"] == sum(STAGE_WEIGHTS.values())
    # ingest (2) + scan half (4) = 6
    assert data["current"] == 6


# ---------------------------------------------------------------------------
# Cancel responsiveness in extract_masks / eye_keypoints
#
# PR #710 fixed mid-batch cancel for the classify stage. The same hang shape
# (cancel takes minutes, stage finalizes as plain "completed") still affected
# extract_masks and eye_keypoints. These tests pin the corrected behavior:
#   - The per-photo loop in extract_masks breaks promptly on abort.
#   - extract_masks finalizes with a "Cancelled (X of N processed)" summary.
#   - eye_keypoints finalizes with a "Cancelled" summary, not the
#     misleading default "X of N photos processed".
#   - detect_eye_keypoints_stage in pipeline.py honors an abort_check
#     callable so a stuck mid-stage cancel can take effect within one
#     keypoint inference, not at end of stage.
# ---------------------------------------------------------------------------


def _stub_extract_masks_heavy_ops(monkeypatch):
    """Stub the SAM2 + DINOv2 helpers extract_masks_stage imports so the loop
    body runs in microseconds. Returns a dict with the proxy-call counter so
    the test can assert how many photos the loop touched.
    """
    import dino_embed
    import masking
    import numpy as np
    import quality
    from db import Database

    state = {"proxy_calls": 0}

    def fake_render_proxy(image_path, longest_edge=None):
        state["proxy_calls"] += 1
        return np.zeros((16, 16, 3), dtype=np.uint8)

    monkeypatch.setattr(masking, "render_proxy", fake_render_proxy)
    monkeypatch.setattr(
        masking, "generate_mask",
        lambda *a, **k: np.ones((16, 16), dtype=np.uint8),
    )
    monkeypatch.setattr(
        masking, "save_mask",
        lambda mask, dir_, pid_, variant: os.path.join(
            dir_, f"{pid_}.{variant}.png",
        ),
    )
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 1.0)
    monkeypatch.setattr(masking, "crop_subject", lambda p, m, margin=0.15: None)
    monkeypatch.setattr(masking, "ensure_sam2_weights", lambda **k: None)
    monkeypatch.setattr(quality, "compute_all_quality_features", lambda p, m: {})
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")
    monkeypatch.setattr(dino_embed, "ensure_dinov2_weights", lambda **k: None)
    monkeypatch.setattr(
        Database, "update_photo_pipeline_features",
        lambda self, *a, **k: None,
    )
    monkeypatch.setattr(
        Database, "update_photo_embeddings",
        lambda self, *a, **k: None,
    )
    return state


def test_pipeline_extract_masks_cancel_marks_stage_cancelled(
    tmp_path, monkeypatch,
):
    """An abort triggered during extract_masks must finalize the stage with
    a 'Cancelled (X of N processed)' summary, not as plain 'completed' (or
    'failed') as if the full set was processed.

    Pre-fix shape: stages["extract_masks"]["status"] was unconditionally set
    to "completed" or "failed" based only on em_failed, regardless of
    abort. The user saw a green "completed" summary on a stage that had
    only processed 173 of 11,285 photos.
    """
    import config as cfg
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    photo_ids = []
    for i in range(3):
        name = f"photo{i}.jpg"
        pid = db.add_photo(
            folder_id, name, ".jpg", 1000 + i, 1_000_000.0 + i,
        )
        _drop_jpeg(folder_path, name)
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
              "confidence": 0.9, "category": "animal"}],
            detector_model="MegaDetector",
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    state = _stub_extract_masks_heavy_ops(monkeypatch)

    # Trigger abort once the first photo's render_proxy fires. The next
    # iteration's top-of-loop abort check (and the new intra-photo checks)
    # must catch it before any further photo-level work runs.
    abort_after_first = threading.Event()
    real_render = state.get("real_render")  # placeholder; we override below

    import masking
    import numpy as np

    def render_then_abort(image_path, longest_edge=None):
        state["proxy_calls"] += 1
        if state["proxy_calls"] == 1:
            abort_after_first.set()
        return np.zeros((16, 16, 3), dtype=np.uint8)

    state["proxy_calls"] = 0  # reset for the override
    monkeypatch.setattr(masking, "render_proxy", render_then_abort)

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_after_first.is_set():
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    # Without the intra-photo / top-of-loop abort honoring, all 3 photos
    # would render their proxies. With the fix, the loop bails after photo 1
    # (and at most one more iteration if abort lands between sub-steps).
    assert 1 <= state["proxy_calls"] <= 2, (
        f"Expected extract_masks to stop within ~1 photo of abort; got "
        f"{state['proxy_calls']} render_proxy calls."
    )

    # The final extract_masks step update must carry a 'Cancelled' summary,
    # not the default 'X masked, Y skipped'.
    em_finals = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == "extract_masks" and kw.get("status") in (
            "completed", "failed",
        ) and "summary" in kw
    ]
    assert em_finals, (
        f"Expected at least one final extract_masks update; got "
        f"step_updates={runner.step_updates!r}"
    )
    final_kw = em_finals[-1]
    final_summary = final_kw.get("summary") or ""
    assert "Cancelled" in final_summary, (
        f"extract_masks final summary must reflect cancellation; got "
        f"{final_summary!r}"
    )
    # The status must NOT be 'failed' on a clean cancel — failure status
    # would inflate the job rollup's error count.
    assert final_kw.get("status") == "completed", (
        f"Cancelled extract_masks should finalize as 'completed'; got "
        f"status={final_kw.get('status')!r}"
    )


# ---------------------------------------------------------------------------
# extract_masks per-variant cache (photo_masks)
#
# Phase 2 of the SAM mask history plan stops the masking stage from re-running
# SAM when a row already exists in `photo_masks` for (photo, configured
# variant) AND its stored prompt + detector still matches the photo's current
# primary detection.  Three regression tests pin the contract:
#
#   - cached_with_same_prompt → generate_mask NOT called, photo_masks
#     unchanged, masked counter still increments (cache hit is a successful
#     outcome, not a "skipped" SAM failure).
#   - variant_differs → switching `pipeline.sam2_variant` re-runs SAM, leaves
#     the previous variant's row in place, adds a new row.
#   - prompt_changed → if the detection bbox shifts (e.g. YOLO re-run with a
#     different threshold), the cached row is replaced with the new prompt.
# ---------------------------------------------------------------------------


def _run_extract_masks_for_test(
    tmp_path, monkeypatch, sam2_variant, photo_specs,
):
    """Drive a single pipeline run with extract_masks enabled and the heavy
    SAM2/DINOv2 calls stubbed.  Returns (db, runner, generate_mask_calls)
    where generate_mask_calls is a list of (photo_id, variant) tuples
    capturing every call the patched generate_mask saw.

    `photo_specs` is a list of dicts:
        [{"filename": "a.jpg", "box": (x, y, w, h), "model": "MegaDetector"}]

    Each photo gets a unique 1x1 mask whose pixel pattern depends on
    photo_id, so different photos cannot accidentally collide.
    """
    import config as cfg
    import dino_embed
    import masking
    import numpy as np
    import quality
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    cfg.save({
        "pipeline": {"sam2_variant": sam2_variant, "dinov2_variant": "vit-b14"},
    })

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)

    photo_ids = []
    for spec in photo_specs:
        pid = db.add_photo(folder_id, spec["filename"], ".jpg", 1000, 1.0)
        _drop_jpeg(folder_path, spec["filename"])
        x, y, w, h = spec["box"]
        db.save_detections(
            pid,
            [{"box": {"x": x, "y": y, "w": w, "h": h},
              "confidence": 0.9, "category": "animal"}],
            detector_model=spec["model"],
        )
        photo_ids.append(pid)

    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": photo_ids}]),
    )

    generate_mask_calls = []

    def fake_render_proxy(image_path, longest_edge=None):
        return np.zeros((4, 4, 3), dtype=np.uint8)

    def fake_generate_mask(proxy, det_box, variant=None):
        # Track every (variant, det_box) we get asked about — the cache
        # short-circuit must skip past this entirely on a hit.
        generate_mask_calls.append((variant, tuple(sorted(det_box.items()))))
        return np.ones((4, 4), dtype=bool)

    monkeypatch.setattr(masking, "render_proxy", fake_render_proxy)
    monkeypatch.setattr(masking, "generate_mask", fake_generate_mask)
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 0.9)
    monkeypatch.setattr(masking, "crop_subject", lambda p, m, margin=0.15: None)
    monkeypatch.setattr(masking, "ensure_sam2_weights", lambda **k: None)
    monkeypatch.setattr(
        quality, "compute_all_quality_features",
        lambda p, m: {
            "subject_tenengrad": 1.5,
            "bg_tenengrad": 0.3,
            "subject_clip_high": 0.01,
            "subject_clip_low": 0.01,
            "subject_y_median": 100.0,
            "bg_separation": 50.0,
            "phash_crop": "deadbeef",
            "noise_estimate": 5.0,
        },
    )
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")
    monkeypatch.setattr(dino_embed, "ensure_dinov2_weights", lambda **k: None)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    return db, runner, generate_mask_calls, photo_ids


def test_extract_masks_skips_sam_when_cached_with_same_prompt(
    tmp_path, monkeypatch,
):
    """Second pipeline pass over the same photo + same detection must NOT
    call generate_mask: the photo_masks row is already there for the
    configured variant and the cached prompt still matches.  The
    photo_masks row remains intact (one row, same path)."""
    spec = {"filename": "a.jpg", "box": (10, 20, 100, 200),
            "model": "MegaDetector"}

    db, _, calls_first, photo_ids = _run_extract_masks_for_test(
        tmp_path, monkeypatch, "sam2-small", [spec],
    )
    pid = photo_ids[0]
    assert len(calls_first) == 1, (
        f"first run should call generate_mask once; got {calls_first}"
    )
    rows_first = db.list_masks_for_photo(pid)
    assert len(rows_first) == 1
    assert rows_first[0]["variant"] == "sam2-small"
    first_path = rows_first[0]["path"]

    # Re-run the stage in the same workspace with the same DB.  Reuse the
    # helper to drive a *second* pass — but we want the same DB, so call
    # run_pipeline_job again directly.
    import config as cfg
    import dino_embed
    import masking
    import numpy as np
    import quality

    calls_second = []

    def fake_generate_mask_2(proxy, det_box, variant=None):
        calls_second.append((variant, tuple(sorted(det_box.items()))))
        return np.ones((4, 4), dtype=bool)

    monkeypatch.setattr(
        masking, "render_proxy",
        lambda *a, **k: np.zeros((4, 4, 3), dtype=np.uint8),
    )
    monkeypatch.setattr(masking, "generate_mask", fake_generate_mask_2)
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 0.9)
    monkeypatch.setattr(masking, "crop_subject", lambda p, m, margin=0.15: None)
    monkeypatch.setattr(masking, "ensure_sam2_weights", lambda **k: None)
    monkeypatch.setattr(
        quality, "compute_all_quality_features",
        lambda p, m: {
            "subject_tenengrad": 1.5, "bg_tenengrad": 0.3,
            "subject_clip_high": 0.01, "subject_clip_low": 0.01,
            "subject_y_median": 100.0, "bg_separation": 50.0,
            "phash_crop": "deadbeef", "noise_estimate": 5.0,
        },
    )
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")
    monkeypatch.setattr(dino_embed, "ensure_dinov2_weights", lambda **k: None)

    cfg.save({
        "pipeline": {"sam2_variant": "sam2-small", "dinov2_variant": "vit-b14"},
    })
    # The mask file must exist on disk for the cache check to fire — the
    # first run wrote it, but ensure it's still there.
    assert os.path.isfile(first_path)

    col_id = db.conn.execute(
        "SELECT id FROM collections ORDER BY id LIMIT 1"
    ).fetchone()[0]
    params = PipelineParams(
        collection_id=col_id, skip_classify=True,
        skip_extract_masks=False, skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(
        job, runner, str(tmp_path / "test.db"), db._active_workspace_id, params,
    )

    assert calls_second == [], (
        f"cache hit must skip generate_mask entirely; got {calls_second}"
    )
    rows_after = db.list_masks_for_photo(pid)
    assert len(rows_after) == 1
    assert rows_after[0]["path"] == first_path


def test_extract_masks_skips_weight_download_when_all_cached(
    tmp_path, monkeypatch,
):
    """Fully-cached rerun must NOT call ensure_sam2_weights /
    ensure_dinov2_weights. Before the fix, the worklist included every
    photo with a detection (cache hits filtered inside the loop, not by
    a `mask_path IS NULL` prefilter), so total > 0 unconditionally and
    the gate fired the multi-hundred-MB weight downloads on every
    rerun — fatal in offline / fresh-checkout environments where the
    only thing the user wanted was to denormalize the active variant."""
    spec = {"filename": "a.jpg", "box": (10, 20, 100, 200),
            "model": "MegaDetector"}

    # First run: populates photo_masks (this run *is* allowed to fire the
    # download; mocked to no-op).
    db, _, _, photo_ids = _run_extract_masks_for_test(
        tmp_path, monkeypatch, "sam2-small", [spec],
    )
    pid = photo_ids[0]
    rows = db.list_masks_for_photo(pid)
    assert rows and rows[0]["variant"] == "sam2-small"
    assert os.path.isfile(rows[0]["path"])

    # Second run: every photo is a cache hit. Track ensure_*_weights
    # invocations and assert they are zero.
    import config as cfg
    import dino_embed
    import masking
    import numpy as np
    import quality

    sam_calls = []
    dino_calls = []

    def fake_ensure_sam(**k):
        sam_calls.append(k)

    def fake_ensure_dino(**k):
        dino_calls.append(k)

    monkeypatch.setattr(masking, "ensure_sam2_weights", fake_ensure_sam)
    monkeypatch.setattr(dino_embed, "ensure_dinov2_weights", fake_ensure_dino)
    monkeypatch.setattr(
        masking, "render_proxy",
        lambda *a, **k: np.zeros((4, 4, 3), dtype=np.uint8),
    )
    # generate_mask should never be invoked on a cache hit.
    monkeypatch.setattr(
        masking, "generate_mask",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("generate_mask called on a cache hit")
        ),
    )
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 0.9)
    monkeypatch.setattr(masking, "crop_subject", lambda p, m, margin=0.15: None)
    monkeypatch.setattr(
        quality, "compute_all_quality_features",
        lambda p, m: {},
    )
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")

    cfg.save({
        "pipeline": {"sam2_variant": "sam2-small", "dinov2_variant": "vit-b14"},
    })
    col_id = db.conn.execute(
        "SELECT id FROM collections ORDER BY id LIMIT 1"
    ).fetchone()[0]
    params = PipelineParams(
        collection_id=col_id, skip_classify=True,
        skip_extract_masks=False, skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(
        job, runner, str(tmp_path / "test.db"), db._active_workspace_id, params,
    )

    assert sam_calls == [], (
        f"ensure_sam2_weights must not be called on a fully-cached rerun; "
        f"got {sam_calls}"
    )
    assert dino_calls == [], (
        f"ensure_dinov2_weights must not be called on a fully-cached rerun; "
        f"got {dino_calls}"
    )


def test_extract_masks_runs_for_new_variant_keeps_old(tmp_path, monkeypatch):
    """A first pass with sam2-small writes one row; switching the
    configured variant to sam2-large adds a second row — both
    variants for the photo are listed in photo_masks.  Active variant
    on the photos row tracks the most recent run."""
    spec = {"filename": "a.jpg", "box": (10, 20, 100, 200),
            "model": "MegaDetector"}

    db, _, calls_first, photo_ids = _run_extract_masks_for_test(
        tmp_path, monkeypatch, "sam2-small", [spec],
    )
    pid = photo_ids[0]
    assert len(calls_first) == 1
    assert calls_first[0][0] == "sam2-small"

    rows = db.list_masks_for_photo(pid)
    assert {r["variant"] for r in rows} == {"sam2-small"}

    # Switch the configured variant.  Re-run.
    import config as cfg
    import dino_embed
    import masking
    import numpy as np
    import quality

    cfg.save({
        "pipeline": {"sam2_variant": "sam2-large", "dinov2_variant": "vit-b14"},
    })

    calls_second = []

    def fake_generate_mask_2(proxy, det_box, variant=None):
        calls_second.append((variant, tuple(sorted(det_box.items()))))
        return np.ones((4, 4), dtype=bool)

    monkeypatch.setattr(
        masking, "render_proxy",
        lambda *a, **k: np.zeros((4, 4, 3), dtype=np.uint8),
    )
    monkeypatch.setattr(masking, "generate_mask", fake_generate_mask_2)
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 0.9)
    monkeypatch.setattr(masking, "crop_subject", lambda p, m, margin=0.15: None)
    monkeypatch.setattr(masking, "ensure_sam2_weights", lambda **k: None)
    monkeypatch.setattr(
        quality, "compute_all_quality_features",
        lambda p, m: {
            "subject_tenengrad": 2.0, "bg_tenengrad": 0.4,
            "subject_clip_high": 0.0, "subject_clip_low": 0.0,
            "subject_y_median": 110.0, "bg_separation": 60.0,
            "phash_crop": "cafef00d", "noise_estimate": 5.0,
        },
    )
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")
    monkeypatch.setattr(dino_embed, "ensure_dinov2_weights", lambda **k: None)

    col_id = db.conn.execute(
        "SELECT id FROM collections ORDER BY id LIMIT 1"
    ).fetchone()[0]
    params = PipelineParams(
        collection_id=col_id, skip_classify=True,
        skip_extract_masks=False, skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(
        job, runner, str(tmp_path / "test.db"), db._active_workspace_id, params,
    )

    assert len(calls_second) == 1 and calls_second[0][0] == "sam2-large", (
        f"new variant must trigger generate_mask once for sam2-large; "
        f"got {calls_second}"
    )
    rows = db.list_masks_for_photo(pid)
    assert {r["variant"] for r in rows} == {"sam2-small", "sam2-large"}, (
        f"expected both variants present after re-run; got {rows}"
    )
    active = db.conn.execute(
        "SELECT active_mask_variant FROM photos WHERE id=?", (pid,),
    ).fetchone()[0]
    assert active == "sam2-large"


def test_extract_masks_re_runs_when_prompt_changed(tmp_path, monkeypatch):
    """If the photo's primary detection's bbox changes between runs, the
    cached photo_masks row's prompt no longer matches and SAM has to
    re-run.  The row is replaced with the new prompt + path; the
    photo_masks set still has exactly one row for that variant."""
    spec = {"filename": "a.jpg", "box": (10, 20, 100, 200),
            "model": "MegaDetector"}

    db, _, calls_first, photo_ids = _run_extract_masks_for_test(
        tmp_path, monkeypatch, "sam2-small", [spec],
    )
    pid = photo_ids[0]
    assert len(calls_first) == 1
    rows = db.list_masks_for_photo(pid)
    assert len(rows) == 1
    assert (rows[0]["prompt_x"], rows[0]["prompt_w"]) == (10, 100)

    # Mutate the detection so it carries a new bbox (mimics YOLO re-run
    # with a different confidence threshold producing a slightly
    # different box).
    db.conn.execute(
        "UPDATE detections SET box_x = 99 WHERE photo_id=?", (pid,),
    )
    db.conn.commit()

    # Re-run — generate_mask must be called and the row must be replaced.
    import config as cfg
    import dino_embed
    import masking
    import numpy as np
    import quality

    cfg.save({
        "pipeline": {"sam2_variant": "sam2-small", "dinov2_variant": "vit-b14"},
    })

    calls_second = []

    def fake_generate_mask_2(proxy, det_box, variant=None):
        calls_second.append((variant, tuple(sorted(det_box.items()))))
        return np.ones((4, 4), dtype=bool)

    monkeypatch.setattr(
        masking, "render_proxy",
        lambda *a, **k: np.zeros((4, 4, 3), dtype=np.uint8),
    )
    monkeypatch.setattr(masking, "generate_mask", fake_generate_mask_2)
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 0.95)
    monkeypatch.setattr(masking, "crop_subject", lambda p, m, margin=0.15: None)
    monkeypatch.setattr(masking, "ensure_sam2_weights", lambda **k: None)
    monkeypatch.setattr(
        quality, "compute_all_quality_features",
        lambda p, m: {
            "subject_tenengrad": 1.5, "bg_tenengrad": 0.3,
            "subject_clip_high": 0.01, "subject_clip_low": 0.01,
            "subject_y_median": 100.0, "bg_separation": 50.0,
            "phash_crop": "deadbeef", "noise_estimate": 5.0,
        },
    )
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")
    monkeypatch.setattr(dino_embed, "ensure_dinov2_weights", lambda **k: None)

    col_id = db.conn.execute(
        "SELECT id FROM collections ORDER BY id LIMIT 1"
    ).fetchone()[0]
    params = PipelineParams(
        collection_id=col_id, skip_classify=True,
        skip_extract_masks=False, skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(
        job, runner, str(tmp_path / "test.db"), db._active_workspace_id, params,
    )

    assert len(calls_second) == 1, (
        f"prompt change must re-run generate_mask; got {calls_second}"
    )
    rows_after = db.list_masks_for_photo(pid)
    assert len(rows_after) == 1, (
        f"row should be replaced (upsert), not duplicated; got {rows_after}"
    )
    assert rows_after[0]["prompt_x"] == 99


def test_pipeline_eye_keypoints_cancel_marks_stage_cancelled(
    tmp_path, monkeypatch,
):
    """An abort during eye_keypoints must finalize the stage with a
    'Cancelled' summary, not the default 'X of N photos processed' which
    looks indistinguishable from a clean run that happened to process X.
    """
    import config as cfg
    import pipeline as pipeline_mod
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    # Stub extract_masks heavies so it sails through (eye_keypoints is
    # gated on extract_masks running). We do NOT trigger abort during
    # extract_masks — only during eye_keypoints.
    _stub_extract_masks_heavy_ops(monkeypatch)

    # Make eye_keypoints reachable: preflight returns None.
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # The wrapping stage uses list_photos_for_eye_keypoint_stage to compute
    # `total`. Force a simple list of one photo.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [{"id": pid}],
    )
    # Stub ensure_keypoint_weights so the auto-download path doesn't try to
    # hit HuggingFace from a unit test. Pretend weights are already there.
    import keypoints as _kp_mod
    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights",
        lambda name, progress_callback=None: "/fake/model.onnx",
    )

    abort_now = [False]

    def fake_detect_eye_keypoints_stage(
        db_, config, progress_callback=None,
        collection_id=None, exclude_photo_ids=None,
        abort_check=None,
    ):
        # Mid-stage: emit a progress event then trigger abort. The wrapping
        # stage must notice and finalize with a "Cancelled" summary.
        if progress_callback:
            progress_callback("Eye keypoints", 0, 1)
        abort_now[0] = True

    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        fake_detect_eye_keypoints_stage,
    )

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_now[0]:
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    ek_finals = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == "eye_keypoints" and kw.get("status") in (
            "completed", "failed",
        ) and "summary" in kw
    ]
    assert ek_finals, (
        f"Expected eye_keypoints final update; got "
        f"step_updates={runner.step_updates!r}"
    )
    summary = ek_finals[-1].get("summary") or ""
    assert "Cancelled" in summary, (
        f"eye_keypoints final summary must reflect cancellation; got "
        f"{summary!r}"
    )


def test_pipeline_eye_keypoints_stage_auto_downloads_superanimal_weights(
    tmp_path, monkeypatch,
):
    """The eye_keypoints stage must call ensure_keypoint_weights for both
    SuperAnimal models before iterating photos when the eligible set
    contains both bird and quadruped subjects. Without this auto-download,
    a fresh install would silently produce zero eye keypoints because the
    per-photo Gate 2 check would skip every photo.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # Provide one bird and one quadruped row so the routing-aware download
    # picks both SuperAnimal variants. species_conf is above the default
    # eye_classifier_conf_gate (0.5) so the conf-gate filter doesn't drop
    # them before _resolve_keypoint_model runs.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Mammalia", "species_conf": 0.9},
            {"id": pid + 1000, "taxonomy_class": "Aves", "species_conf": 0.9},
        ],
    )
    # detect_eye_keypoints_stage stub — we're testing the wrapping stage's
    # download orchestration, not per-photo inference.
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    downloaded = []
    import keypoints as _kp_mod

    def _spy_ensure(name, progress_callback=None):
        downloaded.append(name)
        return "/fake/model.onnx"

    monkeypatch.setattr(_kp_mod, "ensure_keypoint_weights", _spy_ensure)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    assert downloaded == ["superanimal-quadruped", "superanimal-bird"], (
        f"Expected stage to auto-download both SuperAnimal variants in order; "
        f"got {downloaded!r}"
    )


def test_pipeline_eye_keypoints_stage_only_downloads_routable_variants(
    tmp_path, monkeypatch,
):
    """When every eligible photo routes to bird, the stage must skip the
    quadruped variant download (and vice versa). Otherwise a bird-only
    collection pays the full quadruped download cost for weights it would
    never use.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # Bird-only eligible set; species_conf above eye_classifier_conf_gate
    # (default 0.5) so the conf-gate filter doesn't drop the row before
    # routing.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Aves", "species_conf": 0.9},
        ],
    )
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    downloaded = []
    import keypoints as _kp_mod
    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights",
        lambda name, progress_callback=None: downloaded.append(name) or "/fake",
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert downloaded == ["superanimal-bird"], (
        f"Expected only superanimal-bird to download for a bird-only "
        f"eligible set; got {downloaded!r}"
    )


def test_pipeline_eye_keypoints_stage_skips_download_for_out_of_scope_only(
    tmp_path, monkeypatch,
):
    """A collection of only out-of-scope subjects (fish/reptiles/inverts)
    must not trigger any SuperAnimal download. Without routing-awareness,
    the prior `if total > 0` guard would still pull both ~hundreds-of-MB
    variants even though `_resolve_keypoint_model` skips every photo.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # Eligible photos exist (total > 0) but every row carries an
    # out-of-scope taxonomy_class — the per-photo router will return None
    # for each, so no SuperAnimal model is needed. species_conf is above
    # the conf-gate threshold to isolate the routing skip from the
    # confidence skip.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Reptilia", "species_conf": 0.9},
            {"id": pid + 1, "taxonomy_class": "Actinopterygii",
             "species_conf": 0.9},
        ],
    )
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    downloaded = []
    import keypoints as _kp_mod
    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights",
        lambda name, progress_callback=None: downloaded.append(name) or "/fake",
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert downloaded == [], (
        f"Expected zero downloads for an out-of-scope-only eligible set; "
        f"got {downloaded!r}"
    )


def test_pipeline_eye_keypoints_stage_download_progress_isolated_from_photo_count(
    tmp_path, monkeypatch,
):
    """The download progress callback must NOT advance the photo
    `processed` counter. Otherwise a cancel during/just after weight
    download surfaces e.g. "Cancelled (1 of N processed)" before any
    photo has actually been touched, misreporting stage outcomes.
    """
    import config as cfg
    import pipeline as pipeline_mod
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # Three quadruped rows so total=3 — large enough to make the bug
    # observable if the download callback bumps the photo counter.
    # species_conf above the conf-gate so they reach the download
    # planner.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Mammalia", "species_conf": 0.9},
            {"id": pid + 1, "taxonomy_class": "Mammalia", "species_conf": 0.9},
            {"id": pid + 2, "taxonomy_class": "Mammalia", "species_conf": 0.9},
        ],
    )

    abort_now = [False]

    # Stub ensure_keypoint_weights to (a) invoke the progress callback the
    # way the real implementation does — phase, current=0/total=1 then
    # current=1/total=1 — and (b) trigger an abort, so detect_*_stage
    # exits before any real photo work happens.
    import keypoints as _kp_mod

    def _ensure_with_progress(name, progress_callback=None):
        if progress_callback is not None:
            progress_callback(f"Downloading {name}...", 0, 1)
            progress_callback(f"{name} ready", 1, 1)
        abort_now[0] = True
        return "/fake/model.onnx"

    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights", _ensure_with_progress,
    )
    # Make detect_eye_keypoints_stage a no-op — the abort fires before it
    # would touch a photo and we want to assert what the wrapping stage
    # reports for processed['count'].
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_now[0]:
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    run_pipeline_job(job, runner, db_path, ws_id, params)

    ek_finals = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == "eye_keypoints" and kw.get("status") in (
            "completed", "failed",
        ) and "summary" in kw
    ]
    assert ek_finals, (
        f"Expected eye_keypoints final update; got "
        f"step_updates={runner.step_updates!r}"
    )
    summary = ek_finals[-1].get("summary") or ""
    # Cancel summary must report 0 processed (no photo ran), not 1 — the
    # download callback must not bleed into the photo counter.
    assert "Cancelled (0 of 3 processed)" in summary, (
        f"Download progress callback leaked into the photo counter; "
        f"expected 'Cancelled (0 of 3 processed)', got {summary!r}"
    )


def test_pipeline_eye_keypoints_stage_skips_download_when_no_eligible_photos(
    tmp_path, monkeypatch,
):
    """When no photos are eligible (total == 0), the stage must NOT trigger
    a multi-hundred-MB download — gate matches the SAM2/DINOv2 pattern."""
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # Force "no eligible photos" — preflight passes but the eligibility
    # query returns nothing.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [],
    )
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    downloaded = []
    import keypoints as _kp_mod
    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights",
        lambda name, progress_callback=None: downloaded.append(name) or "/fake",
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert downloaded == [], (
        f"Expected no auto-download when 0 photos are eligible; got {downloaded!r}"
    )


def test_pipeline_eye_keypoints_stage_skips_download_when_all_below_conf_gate(
    tmp_path, monkeypatch,
):
    """When every eligible row has species_conf below
    eye_classifier_conf_gate, _process_photo_for_eye skips it at Gate 1
    before any keypoint inference. The download planner must mirror that
    threshold so an all-low-confidence collection doesn't pull
    multi-hundred-MB SuperAnimal weights that no photo can use.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # Default eye_classifier_conf_gate is 0.5; both rows sit below it.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Mammalia", "species_conf": 0.2},
            {"id": pid + 1, "taxonomy_class": "Aves", "species_conf": 0.4},
        ],
    )
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    downloaded = []
    import keypoints as _kp_mod
    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights",
        lambda name, progress_callback=None: downloaded.append(name) or "/fake",
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert downloaded == [], (
        f"Expected no downloads for an all-below-conf-gate set; "
        f"got {downloaded!r}"
    )


def test_pipeline_eye_keypoints_stage_aborts_between_keypoint_downloads(
    tmp_path, monkeypatch,
):
    """A cancel that arrives after the first SuperAnimal weights download
    must short-circuit the second. Without the abort check between models
    the user waits through tens-to-hundreds of MB of unwanted bandwidth
    before the stage exits.
    """
    import config as cfg
    import pipeline as pipeline_mod
    import pipeline_job as pj
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # One mammal + one bird so the planner queues both variants.
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Mammalia", "species_conf": 0.9},
            {"id": pid + 1, "taxonomy_class": "Aves", "species_conf": 0.9},
        ],
    )
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    abort_now = [False]
    downloaded = []
    import keypoints as _kp_mod

    def _ensure_then_abort(name, progress_callback=None):
        downloaded.append(name)
        # Trigger abort the moment the first download "finishes" so the
        # next iteration's abort check fires.
        abort_now[0] = True
        return "/fake/model.onnx"

    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights", _ensure_then_abort,
    )

    original_should_abort = pj._should_abort

    def patched_should_abort(event):
        if abort_now[0]:
            return True
        return original_should_abort(event)

    monkeypatch.setattr(pj, "_should_abort", patched_should_abort)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert downloaded == ["superanimal-quadruped"], (
        f"Cancel after the first weights download must short-circuit the "
        f"second; expected ['superanimal-quadruped'], got {downloaded!r}"
    )


def test_pipeline_eye_keypoints_stage_download_failure_skips_stage_not_pipeline(
    tmp_path, monkeypatch,
):
    """A transient HuggingFace/network failure inside
    ensure_keypoint_weights must degrade Eye Keypoints to a skipped stage
    rather than failing the whole pipeline run. Without this, first-run /
    offline users who never asked to opt out of eye keypoints get a hard
    RuntimeError out of run_pipeline_job for an optional stage.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Mammalia", "species_conf": 0.9},
        ],
    )

    detect_called = [False]

    def fake_detect_eye_keypoints_stage(*a, **k):
        detect_called[0] = True

    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        fake_detect_eye_keypoints_stage,
    )

    import keypoints as _kp_mod

    def _ensure_raises(name, progress_callback=None):
        raise RuntimeError(
            f"Failed to download {name} weights: connection reset. "
            "Check your network connection and retry."
        )

    monkeypatch.setattr(_kp_mod, "ensure_keypoint_weights", _ensure_raises)

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()
    # Must NOT raise: an optional stage's download failure cannot tank
    # the whole pipeline run.
    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert detect_called[0] is False, (
        "detect_eye_keypoints_stage must be skipped when weight download "
        "fails; running it would crash on missing weights."
    )

    ek_finals = [
        kw for (_, sid, kw) in runner.step_updates
        if sid == "eye_keypoints" and "summary" in kw
    ]
    assert ek_finals, (
        f"Expected eye_keypoints final update; got "
        f"step_updates={runner.step_updates!r}"
    )
    final = ek_finals[-1]
    assert final.get("status") == "completed", (
        f"eye_keypoints must finalize as completed (skipped variant), not "
        f"failed; got {final!r}"
    )
    summary = final.get("summary") or ""
    assert "Skipped" in summary and "download" in summary.lower(), (
        f"eye_keypoints summary must explain the download was skipped; "
        f"got {summary!r}"
    )

    ek_result = result.get("stages", {}).get("eye_keypoints", {})
    assert ek_result.get("skipped") == "weight_download_failed", (
        f"result.stages.eye_keypoints must record the skip reason; "
        f"got {ek_result!r}"
    )


def test_pipeline_eye_keypoints_stage_excluded_photos_do_not_influence_downloads(
    tmp_path, monkeypatch,
):
    """Photos in params.exclude_photo_ids must not influence which
    SuperAnimal variants are downloaded. detect_eye_keypoints_stage already
    skips them per-photo, so pulling weights to satisfy a deselected row
    wastes bandwidth on a variant that no included photo will use.
    """
    import config as cfg
    import pipeline as pipeline_mod
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, "p.jpg", ".jpg", 100, 1_000_000.0)
    _drop_jpeg(folder_path, "p.jpg")
    db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": 0.9, "category": "animal"}],
        detector_model="MegaDetector",
    )
    col_id = db.add_collection(
        "Test", json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    _stub_extract_masks_heavy_ops(monkeypatch)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    # One bird (kept) + one mammal (excluded). After exclusion, only the
    # bird variant should be downloaded — the mammal row would route to
    # quadruped, but it's deselected so that variant is wasted bandwidth.
    excluded_id = pid + 7
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": pid, "taxonomy_class": "Aves", "species_conf": 0.9},
            {"id": excluded_id, "taxonomy_class": "Mammalia",
             "species_conf": 0.9},
        ],
    )
    monkeypatch.setattr(
        pipeline_mod, "detect_eye_keypoints_stage",
        lambda *a, **k: None,
    )

    downloaded = []
    import keypoints as _kp_mod
    monkeypatch.setattr(
        _kp_mod, "ensure_keypoint_weights",
        lambda name, progress_callback=None: downloaded.append(name) or "/fake",
    )

    params = PipelineParams(
        collection_id=col_id,
        skip_classify=True,
        skip_extract_masks=False,
        skip_regroup=True,
        exclude_photo_ids={excluded_id},
    )
    run_pipeline_job(_make_job(), FakeRunner(), db_path, ws_id, params)

    assert downloaded == ["superanimal-bird"], (
        f"Expected only superanimal-bird to download — the mammal row was "
        f"excluded and shouldn't influence the download planner; "
        f"got {downloaded!r}"
    )


def test_detect_eye_keypoints_stage_honors_abort_check(tmp_path, monkeypatch):
    """detect_eye_keypoints_stage must accept an `abort_check` callable and
    break the per-photo loop the first time it returns True. Without this
    hook, a long eye_keypoints run swallows the user's cancel for many
    inferences.
    """
    import pipeline as pipeline_mod
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)

    # Drive the loop with a controlled photos list. Bypass eligibility and
    # the route check by stubbing helpers; we only care that the abort_check
    # parameter is honored before per-photo work fires.
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [
            {"id": 1}, {"id": 2}, {"id": 3},
        ],
    )
    monkeypatch.setattr(
        Database, "get_folder_tree",
        lambda self: [],
    )

    process_calls = [0]

    def spy_process(*args, **kwargs):
        process_calls[0] += 1

    monkeypatch.setattr(pipeline_mod, "_process_photo_for_eye", spy_process)

    abort_after_first = [False]

    def abort_check():
        # Returns False on first poll, True on every subsequent poll. The
        # loop polls once per photo at the top of each iteration. So the
        # first photo runs, the second iteration's check breaks.
        result = abort_after_first[0]
        abort_after_first[0] = True
        return result

    pipeline_mod.detect_eye_keypoints_stage(
        db, config={}, abort_check=abort_check,
    )

    assert process_calls[0] == 1, (
        f"detect_eye_keypoints_stage must break on abort_check; "
        f"got {process_calls[0]} _process_photo_for_eye calls (expected 1)."
    )


def test_detect_eye_keypoints_stage_skips_synthetic_100pct_on_abort(
    tmp_path, monkeypatch,
):
    """When abort fires mid-loop, detect_eye_keypoints_stage must NOT emit
    the unconditional final progress(total, total) callback. That synthetic
    100% signal corrupts the wrapping eye_keypoints_stage's processed['count']
    and surfaces "Cancelled (N of N processed)" — indistinguishable from a
    clean run that processed N photos.
    """
    import pipeline as pipeline_mod
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [{"id": 1}, {"id": 2}, {"id": 3}],
    )
    monkeypatch.setattr(
        Database, "get_folder_tree",
        lambda self: [],
    )
    monkeypatch.setattr(
        pipeline_mod, "_process_photo_for_eye", lambda *a, **kw: None,
    )

    progress_events = []

    def progress_callback(phase, current, total):
        progress_events.append((current, total))

    abort_after_first = [False]

    def abort_check():
        # First poll returns False (photo 1 runs), subsequent polls True.
        result = abort_after_first[0]
        abort_after_first[0] = True
        return result

    pipeline_mod.detect_eye_keypoints_stage(
        db, config={},
        progress_callback=progress_callback,
        abort_check=abort_check,
    )

    # Stage processed exactly one photo before aborting; the wrapper must see
    # current < total on the final emit, not current == total.
    assert progress_events, "Expected at least one progress event"
    last_current, last_total = progress_events[-1]
    assert last_total == 3, f"Unexpected total in last emit: {progress_events!r}"
    assert last_current < last_total, (
        f"detect_eye_keypoints_stage must not emit progress({last_current}, "
        f"{last_total}) after abort — that 100% signal would mask the cancel "
        f"in the wrapper. Events: {progress_events!r}"
    )
    # And the count should reflect the actual photo processed (1), not 0.
    assert last_current == 1, (
        f"Expected 1 photo to be reported processed before abort; "
        f"got events={progress_events!r}"
    )


def test_detect_eye_keypoints_stage_emits_final_100pct_on_clean_run(
    tmp_path, monkeypatch,
):
    """On a clean (non-aborted) run, detect_eye_keypoints_stage must finish
    by reporting current == total so the wrapping stage's processed['count']
    matches reality.
    """
    import pipeline as pipeline_mod
    from db import Database

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)

    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    monkeypatch.setattr(
        Database, "list_photos_for_eye_keypoint_stage",
        lambda self, **k: [{"id": 1}, {"id": 2}],
    )
    monkeypatch.setattr(
        Database, "get_folder_tree",
        lambda self: [],
    )
    monkeypatch.setattr(
        pipeline_mod, "_process_photo_for_eye", lambda *a, **kw: None,
    )

    progress_events = []

    def progress_callback(phase, current, total):
        progress_events.append((current, total))

    pipeline_mod.detect_eye_keypoints_stage(
        db, config={},
        progress_callback=progress_callback,
    )

    assert progress_events, "Expected at least one progress event"
    last_current, last_total = progress_events[-1]
    assert (last_current, last_total) == (2, 2), (
        f"Expected final emit (2, 2) on clean run; got {progress_events!r}"
    )
