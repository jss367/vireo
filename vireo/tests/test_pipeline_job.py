"""Tests for the streaming pipeline job orchestrator."""

import contextlib
import json
import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pipeline_job import PipelineParams, run_pipeline_job


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
        "SELECT id, detection_id, species, model FROM predictions"
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

    # Every prediction's detection_id must resolve to a real detection row in
    # the active workspace (so the workspace-scoped skip query picks it up).
    for p in preds:
        det = db.conn.execute(
            "SELECT photo_id, workspace_id FROM detections WHERE id = ?",
            (p["detection_id"],),
        ).fetchone()
        assert det is not None, (
            f"Prediction {dict(p)} references detection_id "
            f"{p['detection_id']} which does not exist in detections table."
        )
        assert det["workspace_id"] == ws_id, (
            f"Prediction bound to detection in workspace {det['workspace_id']}, "
            f"expected {ws_id}."
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
