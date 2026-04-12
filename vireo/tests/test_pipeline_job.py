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
        f.truncate(models._ONNX_DATA_MIN_BYTES + 1024)
    (model_dir / "text_encoder.onnx").write_bytes(b"stub")
    with open(model_dir / "text_encoder.onnx.data", "wb") as f:
        f.truncate(models._ONNX_DATA_MIN_BYTES + 1024)
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

    # Capture the already_detected_ids set passed to each _detect_batch call.
    detect_call_ids = []

    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        detect_call_ids.append(frozenset(already_detected_ids or set()))
        # Model 1 "detects" nothing in this run — empty det_map.
        return {}, 0

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

    # _detect_batch should have been called at least once (one batch per model).
    assert len(detect_call_ids) >= 1, (
        "Expected _detect_batch to be called at least once but it was not."
    )

    # The stale prior-run photo_id must NOT appear in already_detected_ids for
    # any _detect_batch invocation.  With the fix, already_detected is wiped
    # before model 1's loop; model 1 finds nothing → already_detected stays
    # empty → model 2 receives an empty set, not the pre-seeded stale ID.
    for call_ids in detect_call_ids:
        assert photo_id not in call_ids, (
            f"Prior-run photo_id {photo_id} leaked into already_detected_ids "
            f"{call_ids!r}. already_detected must be cleared before the first "
            "model's batch loop so later models do not use stale detection rows "
            "from prior pipeline passes."
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

    det_map, count = classify_job._detect_batch(
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


def test_pipeline_reclassify_clears_prior_detection_rows(tmp_path, monkeypatch):
    """On reclassify, prior-run detection rows must be DELETED from the DB
    before any model runs. Just clearing the in-memory already_detected set
    isn't sufficient: model 2+ hit _detect_batch's cached path, which calls
    db.get_detections(photo_id) and returns the union of old + newly-inserted
    rows when old rows are still present — so model 2's predictions bind to
    stale detection_ids from a prior pipeline pass.

    Regression for Codex review on #506: the earlier fix only wiped the
    in-memory set; this test pins the stronger "DB rows are gone" invariant.
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

    # Prior-run detection rows we expect to be wiped by reclassify.
    prior_det_ids = db.save_detections(
        photo_id,
        [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
             "confidence": 0.9, "category": "animal"},
            {"box": {"x": 0.2, "y": 0.2, "w": 0.3, "h": 0.3},
             "confidence": 0.8, "category": "animal"},
        ],
        detector_model="MegaDetector",
    )
    assert prior_det_ids, "setup sanity: prior detections were inserted"

    col_id = db.add_collection(
        "Test",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    model_ids = _setup_two_fake_downloaded_models(tmp_path, monkeypatch)

    # _detect_batch stub that writes NOTHING — mimics a reclassify pass that
    # produces no new detections. If the fix is working, the DB should end
    # up with zero rows for this photo; if not, prior rows will linger.
    def fake_detect_batch(batch, folders, runner, job, reclassify, db_,
                          det_conf_threshold=None, already_detected_ids=None,
                          cached_detections=None):
        return {}, 0

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

    # Reopen the DB to see the committed state from the pipeline's thread-db.
    verify_db = Database(db_path)
    verify_db.set_active_workspace(ws_id)
    remaining = verify_db.get_detections(photo_id)
    assert remaining == [], (
        f"Prior-run detection rows must be cleared on reclassify but "
        f"db.get_detections({photo_id}) still returned {remaining!r}. "
        "This is exactly the cross-model stale-id leak Codex flagged on #506: "
        "model 2+ would see these rows via _detect_batch's cached path and "
        "bind predictions to outdated detection_ids."
    )
