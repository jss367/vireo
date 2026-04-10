"""Tests for the streaming pipeline job orchestrator."""

import json
import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pipeline_job import PipelineParams, run_pipeline_job


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

    def push_event(self, job_id, event_type, data):
        self.events.append((job_id, event_type, data))

    def set_steps(self, job_id, steps):
        self.steps_defined = list(steps)

    def update_step(self, job_id, step_id, **kwargs):
        self.step_updates.append((job_id, step_id, kwargs))


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
    assert params.reclassify is True
    assert params.skip_extract_masks is True
    assert params.skip_regroup is True
    assert params.skip_classify is True
    assert params.preview_max_size == 2560


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


def test_pipeline_abort_on_nonexistent_source(tmp_path, monkeypatch):
    """Pipeline with nonexistent source still returns result with errors.

    The scanner silently returns for nonexistent dirs (no photos found).
    The model_loader will abort because no model is available in test env.
    Either way, the pipeline should return a valid result dict with errors.
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
        skip_extract_masks=True,
        skip_regroup=True,
    )

    runner = FakeRunner()
    job = _make_job()

    result = run_pipeline_job(job, runner, db_path, ws_id, params)

    assert isinstance(result, dict)
    assert "duration" in result
    # Should have errors (model_loader will fail in test env without models)
    assert len(result["errors"]) > 0


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

    src = tmp_path / "photos"
    src.mkdir()
    for i in range(12):
        (src / f"img{i:02d}.jpg").write_bytes(b'\xff\xd8\xff\xe0' + b'\x00' * 100)

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
