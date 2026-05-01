import json
import os
import time

from PIL import Image
from wait import wait_for_job_via_client, wait_for_job_via_runner


def test_job_scan_returns_job_id(app_and_db, tmp_path):
    """POST /api/jobs/scan starts a background scan and returns job_id."""
    app, db = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'test.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'job_id' in data
    assert data['job_id'].startswith('scan-')


def test_job_scan_invalid_root(app_and_db):
    """POST /api/jobs/scan with invalid root returns 400."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.post('/api/jobs/scan', json={'root': '/nonexistent/path'})
    assert resp.status_code == 400


def test_job_status_endpoint(app_and_db, tmp_path):
    """GET /api/jobs/<id> returns job status."""
    app, db = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'test.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    job_id = resp.get_json()['job_id']

    data = wait_for_job_via_client(client, job_id)

    assert resp.status_code == 200
    assert data['status'] == 'completed'


def test_jobs_list(app_and_db):
    """GET /api/jobs returns active and history lists."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.get('/api/jobs')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'active' in data
    assert 'history' in data


def test_jobs_list_strips_result_payload(app_and_db):
    """Heavy ``result`` payloads must not ride along on the polling
    response — duplicate-scan can stash thousands of proposals there
    and re-shipping that on every poll freezes the browser."""
    app, _ = app_and_db
    client = app.test_client()

    runner = app._job_runner
    big_result = {"proposals": [{"i": i, "filename": f"p_{i}.jpg"} for i in range(2000)]}
    fake_job = {
        "id": "fake-duplicate-scan-1",
        "type": "duplicate-scan",
        "status": "completed",
        "started_at": "2026-04-29T20:00:00",
        "finished_at": "2026-04-29T20:01:00",
        "progress": {"current": 0, "total": 0},
        "errors": [],
        "config": {},
        "result": big_result,
    }
    with runner._lock:
        runner._jobs["fake-duplicate-scan-1"] = fake_job

    resp = client.get('/api/jobs')
    data = resp.get_json()
    listed = next(j for j in data["active"] if j["id"] == "fake-duplicate-scan-1")
    assert "result" not in listed, (
        "result must be stripped from /api/jobs polling response so "
        "huge payloads don't ship on every poll"
    )
    assert listed["has_result"] is True, (
        "has_result lets the UI distinguish 'no result yet' from "
        "'result available, fetch on demand'"
    )


def test_jobs_list_marks_has_result_false_for_running_jobs(app_and_db):
    """A running job with no result yet must report has_result=False."""
    app, _ = app_and_db
    client = app.test_client()

    runner = app._job_runner
    fake_job = {
        "id": "fake-running-1",
        "type": "pipeline",
        "status": "running",
        "started_at": "2026-04-29T20:00:00",
        "finished_at": None,
        "progress": {"current": 5, "total": 100},
        "errors": [],
        "config": {},
        "result": None,
    }
    with runner._lock:
        runner._jobs["fake-running-1"] = fake_job

    data = client.get('/api/jobs').get_json()
    listed = next(j for j in data["active"] if j["id"] == "fake-running-1")
    assert listed["has_result"] is False


def test_job_detail_endpoint_returns_full_result(app_and_db):
    """GET /api/jobs/<id> still returns the full result so callers
    that need the heavy payload (e.g. duplicates page) can fetch on
    demand instead of paying for it on every poll."""
    app, _ = app_and_db
    client = app.test_client()

    runner = app._job_runner
    big_result = {"proposals": [{"i": i} for i in range(100)]}
    with runner._lock:
        runner._jobs["fake-detail-1"] = {
            "id": "fake-detail-1",
            "type": "duplicate-scan",
            "status": "completed",
            "started_at": "2026-04-29T20:00:00",
            "finished_at": "2026-04-29T20:01:00",
            "progress": {"current": 0, "total": 0},
            "errors": [],
            "config": {},
            "result": big_result,
        }

    detail = client.get('/api/jobs/fake-detail-1').get_json()
    assert detail["result"] == big_result


def test_scan_status_includes_extended_stats(app_and_db):
    """GET /api/scan/status includes keyword count, db_size, etc."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.get('/api/scan/status')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'photo_count' in data
    assert 'keyword_count' in data
    assert 'db_size' in data
    assert 'thumb_cache_size' in data


def test_ingest_job_starts(app_and_db, tmp_path):
    """POST /api/jobs/ingest starts a background job and returns job_id."""
    app, db = app_and_db
    src = tmp_path / "sd_card"
    dst = tmp_path / "nas_dest"
    src.mkdir()
    dst.mkdir()

    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(src / "bird.jpg"))

    with app.test_client() as c:
        resp = c.post("/api/jobs/ingest", json={
            "source": str(src),
            "destination": str(dst),
        })
        assert resp.status_code == 200
        data = resp.get_json()
        assert "job_id" in data
        assert data["job_id"].startswith("ingest-")


def test_ingest_missing_params(app_and_db, tmp_path):
    """POST /api/jobs/ingest returns error when source or destination missing."""
    app, db = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/jobs/ingest", json={})
        assert resp.status_code == 400

        resp = c.post("/api/jobs/ingest", json={"source": str(tmp_path)})
        assert resp.status_code == 400

        resp = c.post("/api/jobs/ingest", json={"destination": str(tmp_path)})
        assert resp.status_code == 400


def test_ingest_nonexistent_source(app_and_db, tmp_path):
    """POST /api/jobs/ingest returns error for non-existent source directory."""
    app, db = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/jobs/ingest", json={
            "source": str(tmp_path / "does_not_exist"),
            "destination": str(tmp_path),
        })
        assert resp.status_code == 400
        assert "not found" in resp.get_json()["error"]


def test_ingest_relative_destination(app_and_db, tmp_path):
    """POST /api/jobs/ingest validates destination is absolute."""
    app, db = app_and_db
    src = tmp_path / "src"
    src.mkdir()
    with app.test_client() as c:
        resp = c.post("/api/jobs/ingest", json={
            "source": str(src),
            "destination": "relative/path",
        })
        assert resp.status_code == 400


def test_pipeline_job_requires_source_or_collection(app_and_db):
    """Pipeline endpoint should require either source or collection_id."""
    app, _ = app_and_db
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={})
        assert resp.status_code == 400


def test_pipeline_job_rejects_relative_destination(app_and_db, tmp_path):
    """Pipeline endpoint should reject relative destination paths."""
    app, _ = app_and_db
    src = tmp_path / "src"
    src.mkdir()
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "source": str(src),
            "destination": "relative/path",
        })
        assert resp.status_code == 400
        assert "absolute" in resp.get_json()["error"]


def test_jobs_page_returns_200(app_and_db):
    """GET /jobs returns the jobs page."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/jobs')
    assert resp.status_code == 200
    assert b'Jobs' in resp.data


def test_navbar_has_jobs_link(app_and_db):
    """Navbar on any page includes a link to /jobs."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/browse')
    assert b'href="/jobs"' in resp.data


def test_bottom_panel_has_compact_jobs(app_and_db):
    """Bottom panel Jobs tab has compact layout with View link."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/browse')
    html = resp.data.decode()
    assert 'bp-compact-job' in html or 'View history' in html or 'No active jobs' in html


def test_scan_job_has_steps(app_and_db, tmp_path):
    """Scan job defines steps for the jobs page tree view."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'test.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    job_id = resp.get_json()['job_id']

    data = wait_for_job_via_client(client, job_id)

    assert data['status'] == 'completed'
    assert 'steps' in data
    assert len(data['steps']) >= 2
    assert data['steps'][0]['id'] == 'scan'
    assert data['steps'][0]['status'] == 'completed'


def test_job_history_includes_parsed_tree(app_and_db, tmp_path):
    """GET /api/jobs/history returns parsed tree data for completed jobs."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'test.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    job_id = resp.get_json()['job_id']

    wait_for_job_via_client(client, job_id, wait_for_history=True)

    resp = client.get('/api/jobs/history?limit=5')
    assert resp.status_code == 200
    history = resp.get_json()
    assert len(history) > 0
    found = [h for h in history if h['id'] == job_id]
    assert len(found) > 0
    entry = found[0]
    # tree should be a parsed list, not a JSON string
    assert 'tree' in entry
    assert isinstance(entry['tree'], list)
    assert 'summary' in entry


def test_jobs_list_includes_active_workspace_id(app_and_db):
    """GET /api/jobs includes active_workspace_id."""
    app, db = app_and_db
    client = app.test_client()

    resp = client.get('/api/jobs')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'active_workspace_id' in data
    assert data['active_workspace_id'] == db._active_workspace_id


def test_jobs_list_includes_workspace_names(app_and_db):
    """GET /api/jobs includes workspace_names mapping."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.get('/api/jobs')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'workspace_names' in data
    assert isinstance(data['workspace_names'], dict)
    # Default workspace should be present
    assert len(data['workspace_names']) >= 1


def test_readiness_includes_exiftool_status(app_and_db):
    """Readiness endpoint should report exiftool installation status."""
    app, _ = app_and_db
    with app.test_client() as client:
        resp = client.get("/api/classify/readiness")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "exiftool" in data
        assert "installed" in data["exiftool"]
        assert "brew_available" in data["exiftool"]
        assert isinstance(data["exiftool"]["installed"], bool)
        assert isinstance(data["exiftool"]["brew_available"], bool)


def test_install_exiftool_endpoint_exists(app_and_db, monkeypatch):
    """Install-exiftool endpoint should exist and return JSON."""
    import shutil
    import subprocess
    original_which = shutil.which
    monkeypatch.setattr(shutil, "which", lambda cmd: "/usr/local/bin/brew" if cmd == "brew" else (None if cmd == "exiftool" else original_which(cmd)))
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: type("R", (), {"returncode": 0, "stderr": ""})())
    app, _ = app_and_db
    with app.test_client() as client:
        resp = client.post("/api/system/install-exiftool")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data.get("success") is True


def test_install_exiftool_fails_without_brew(app_and_db, monkeypatch):
    """Install endpoint should fail gracefully when brew is not available."""
    import shutil
    original_which = shutil.which
    monkeypatch.setattr(shutil, "which", lambda cmd: None if cmd in ("brew", "exiftool") else original_which(cmd))
    app, _ = app_and_db
    with app.test_client() as client:
        resp = client.post("/api/system/install-exiftool")
        data = resp.get_json()
        assert data.get("success") is False
        assert "brew" in data.get("error", "").lower()


def test_pipeline_job_with_collection_returns_job_id(app_and_db):
    """Pipeline with collection_id should start and return job_id."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)
    col_id = db.add_collection("Test", json.dumps([]))

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        data = resp.get_json()
        assert "job_id" in data
        assert data["job_id"].startswith("pipeline-")


def test_pipeline_auto_skips_classify_when_no_model(app_and_db):
    """Pipeline should auto-skip classify/extract/regroup when no model is available."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)
    col_id = db.add_collection("Test", json.dumps([]))

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            # Don't set skip_classify — let the auto-skip kick in
        })
        assert resp.status_code == 200
        data = resp.get_json()
        assert "job_id" in data
        assert "model_warning" in data
        assert "skipped" in data["model_warning"].lower() or "no model" in data["model_warning"].lower()


def test_update_step_current_file(app_and_db):
    """update_step supports current_file field on steps."""
    from jobs import JobRunner
    runner = JobRunner.__new__(JobRunner)
    runner._jobs = {}
    runner._subscribers = {}
    runner._lock = __import__('threading').Lock()
    runner._history_db_path = None

    job_id = "test-cf"
    runner._jobs[job_id] = {
        "id": job_id,
        "steps": [
            {"id": "scan", "label": "Scan", "status": "running"},
        ],
    }
    runner.update_step(job_id, "scan", current_file="DSC_0001.NEF")
    assert runner._jobs[job_id]["steps"][0]["current_file"] == "DSC_0001.NEF"


def test_scan_step_has_progress(app_and_db, tmp_path):
    """Scan step reports step-level progress with current/total."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'a.jpg'))
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'b.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    job_id = resp.get_json()['job_id']

    data = wait_for_job_via_client(client, job_id)

    assert data['status'] == 'completed'
    scan_step = data['steps'][0]
    assert scan_step['id'] == 'scan'
    # After completion, progress should have current == total
    assert 'progress' in scan_step
    assert scan_step['progress']['current'] == scan_step['progress']['total']
    assert scan_step['progress']['total'] >= 2


def test_pipeline_thumbnail_step_has_progress(app_and_db, tmp_path):
    """Thumbnail step in pipeline reports step-level progress."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'a.jpg'))

    resp = client.post('/api/jobs/pipeline', json={
        'source': scan_dir,
        'skip_classify': True,
        'skip_extract_masks': True,
        'skip_regroup': True,
    })
    job_id = resp.get_json()['job_id']

    data = wait_for_job_via_client(client, job_id)

    assert data['status'] == 'completed'
    thumb_step = next(s for s in data['steps'] if s['id'] == 'thumbnails')
    assert 'progress' in thumb_step
    assert thumb_step['progress']['current'] > 0


def test_pipeline_preview_step_has_progress(app_and_db, tmp_path):
    """Preview step in pipeline reports step-level progress."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'a.jpg'))

    resp = client.post('/api/jobs/pipeline', json={
        'source': scan_dir,
        'skip_classify': True,
        'skip_extract_masks': True,
        'skip_regroup': True,
    })
    job_id = resp.get_json()['job_id']

    data = wait_for_job_via_client(client, job_id)

    assert data['status'] == 'completed'
    preview_step = next(s for s in data['steps'] if s['id'] == 'previews')
    assert 'progress' in preview_step
    assert preview_step['progress']['total'] > 0


def test_job_export_returns_job_id(app_and_db, tmp_path):
    """POST /api/jobs/export starts a background export and returns job_id."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchall()
    photo_ids = [p["id"] for p in photos]
    dest = str(tmp_path / "export_out")

    resp = client.post("/api/jobs/export", json={
        "photo_ids": photo_ids,
        "destination": dest,
        "naming_template": "{original}",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("export-")


def test_job_export_missing_photo_ids(app_and_db):
    """POST /api/jobs/export without photo_ids returns 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/export", json={"destination": "/tmp/out"})
    assert resp.status_code == 400


def test_job_export_missing_destination(app_and_db):
    """POST /api/jobs/export without destination returns 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/export", json={"photo_ids": [1]})
    assert resp.status_code == 400


def test_job_export_relative_destination(app_and_db):
    """POST /api/jobs/export with relative destination returns 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/export", json={
        "photo_ids": [1],
        "destination": "relative/path",
    })
    assert resp.status_code == 400


def test_pipeline_ingest_saves_recent_destination(app_and_db, tmp_path, monkeypatch):
    """Starting a pipeline with a destination saves it to recent_destinations in config."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    app, db = app_and_db
    src = tmp_path / "src"
    src.mkdir()
    dst = tmp_path / "dst"
    dst.mkdir()

    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(src / "bird.jpg"))

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(dst),
        })
        assert resp.status_code == 200

    config = cfg.load()
    assert config["ingest"]["recent_destinations"] == [str(dst)]


def test_recent_destinations_deduplicates_and_limits(app_and_db, tmp_path, monkeypatch):
    """Recent destinations deduplicates and limits to 5 entries."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    app, db = app_and_db
    src = tmp_path / "src"
    src.mkdir()

    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(src / "bird.jpg"))

    # Create 6 destination directories
    dsts = []
    for i in range(6):
        d = tmp_path / f"dst{i}"
        d.mkdir()
        dsts.append(str(d))

    with app.test_client() as c:
        # Fill up 6 destinations
        for d in dsts:
            c.post("/api/jobs/pipeline", json={
                "sources": [str(src)],
                "destination": d,
            })

    config = cfg.load()
    recents = config["ingest"]["recent_destinations"]
    assert len(recents) == 5
    # Most recent first
    assert recents[0] == dsts[5]
    # Oldest dropped
    assert dsts[0] not in recents

    # Re-use dst1 — should move to front
    with app.test_client() as c:
        c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": dsts[1],
        })
    config = cfg.load()
    recents = config["ingest"]["recent_destinations"]
    assert recents[0] == dsts[1]
    assert len(recents) == 5


def test_job_cancel_unknown_job_returns_404(app_and_db):
    """POST /api/jobs/<id>/cancel returns 404 for unknown job."""
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/jobs/does-not-exist/cancel")
        assert resp.status_code == 404


def test_job_cancel_running_job_marks_cancelled(app_and_db):
    """POST /api/jobs/<id>/cancel signals a running job, which then finishes
    with status 'cancelled' instead of 'completed'."""
    app, _ = app_and_db
    runner = app._job_runner

    release = {"go": False}

    def slow_work(job):
        # Poll for cancellation so the work function exits promptly.
        for _ in range(200):
            if runner.is_cancelled(job["id"]):
                return {"stopped": True}
            if release["go"]:
                return {"stopped": False}
            time.sleep(0.05)
        return {"stopped": False}

    job_id = runner.start("test", slow_work)

    with app.test_client() as c:
        resp = c.post(f"/api/jobs/{job_id}/cancel")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data.get("cancelled") is True

    # Wait for the work function to observe cancellation and exit.
    job = wait_for_job_via_runner(runner, job_id)
    assert job["status"] == "cancelled"


def test_job_cancel_finished_job_returns_404(app_and_db):
    """Cancelling a job that has already finished returns 404."""
    app, _ = app_and_db
    runner = app._job_runner

    def quick_work(job):
        return {"ok": True}

    job_id = runner.start("test", quick_work)

    wait_for_job_via_runner(runner, job_id)

    with app.test_client() as c:
        resp = c.post(f"/api/jobs/{job_id}/cancel")
        assert resp.status_code == 404


# --- Pipeline metadata auto-repair tests ---

def test_find_broken_metadata_folders_returns_empty_when_healthy(app_and_db):
    """_find_broken_metadata_folders returns [] when all photos have good
    metadata."""
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    # The fixture's 3 photos all have populated timestamps and .jpg ext,
    # so none should match the detection rule.
    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    assert _find_broken_metadata_folders(db, photo_ids) == []


def test_find_broken_metadata_folders_detects_null_timestamp(app_and_db, monkeypatch):
    """_find_broken_metadata_folders flags a photo whose timestamp is NULL
    and returns its file path so the repair path can restrict the scan."""
    import pipeline_job
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    # Fixture rows point at synthetic /photos paths that don't exist on
    # disk; treat all DB rows as present for the SQL/grouping assertions.
    monkeypatch.setattr(pipeline_job.os.path, "isfile", lambda _p: True)

    # Break one photo's timestamp.
    row = db.conn.execute(
        "SELECT id, folder_id FROM photos WHERE filename='bird1.jpg'"
    ).fetchone()
    db.conn.execute("UPDATE photos SET timestamp=NULL WHERE id=?", (row["id"],))
    db.conn.commit()

    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    result = _find_broken_metadata_folders(db, photo_ids)
    folder_path = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (row["folder_id"],)
    ).fetchone()["path"]
    assert result == [(folder_path, [os.path.join(folder_path, "bird1.jpg")])]


def test_find_broken_metadata_folders_detects_raw_thumb_dims(app_and_db, monkeypatch):
    """_find_broken_metadata_folders flags a RAW photo with sub-1000px
    width (the embedded-thumbnail bug) even when timestamp is populated."""
    import pipeline_job
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    monkeypatch.setattr(pipeline_job.os.path, "isfile", lambda _p: True)

    row = db.conn.execute(
        "SELECT id, folder_id FROM photos WHERE filename='bird1.jpg'"
    ).fetchone()
    db.conn.execute(
        "UPDATE photos SET extension='.nef', width=160, height=120 "
        "WHERE id=?", (row["id"],)
    )
    db.conn.commit()

    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    result = _find_broken_metadata_folders(db, photo_ids)
    assert len(result) == 1
    # One file in the folder — bird1.jpg with the fake .nef extension.
    assert len(result[0][1]) == 1
    assert result[0][1][0].endswith("bird1.jpg")


def test_find_broken_metadata_folders_ignores_out_of_scope(app_and_db):
    """Broken photos outside the passed photo_ids list are not returned."""
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    # Break bird1, but only pass bird2's id in scope.
    bird1 = db.conn.execute(
        "SELECT id FROM photos WHERE filename='bird1.jpg'"
    ).fetchone()["id"]
    bird2 = db.conn.execute(
        "SELECT id FROM photos WHERE filename='bird2.jpg'"
    ).fetchone()["id"]
    db.conn.execute("UPDATE photos SET timestamp=NULL WHERE id=?", (bird1,))
    db.conn.commit()

    assert _find_broken_metadata_folders(db, [bird2]) == []


def test_find_broken_metadata_folders_excludes_exif_extracted(app_and_db):
    """A row whose timestamp is NULL but exif_data is populated is NOT
    returned. Such rows represent photos where ExifTool already ran and
    the source file genuinely has no DateTimeOriginal (e.g. screenshots).
    The scanner's exif_extracted guard would skip them on re-scan anyway,
    so flagging them as repairable would cause the repair path to fire
    forever without doing useful work."""
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    row = db.conn.execute(
        "SELECT id FROM photos WHERE filename='bird1.jpg'"
    ).fetchone()
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL, exif_data='{\"File\":{}}' "
        "WHERE id=?", (row["id"],)
    )
    db.conn.commit()

    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    assert _find_broken_metadata_folders(db, photo_ids) == []


def test_find_broken_metadata_folders_groups_by_folder(app_and_db, monkeypatch):
    """Multiple broken photos in the same folder are returned as one
    folder entry with a count."""
    import pipeline_job
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    monkeypatch.setattr(pipeline_job.os.path, "isfile", lambda _p: True)

    # Break both photos in folder '/photos/2024' (bird1 and bird3).
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL "
        "WHERE filename IN ('bird1.jpg', 'bird3.jpg')"
    )
    db.conn.commit()

    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    result = _find_broken_metadata_folders(db, photo_ids)
    assert len(result) == 1
    folder, paths = result[0]
    assert folder == '/photos/2024'
    assert len(paths) == 2
    assert sorted(os.path.basename(p) for p in paths) == ['bird1.jpg', 'bird3.jpg']


def test_find_broken_metadata_folders_filters_missing_files(app_and_db, tmp_path):
    """Rows whose file no longer exists on disk are filtered out. The
    scanner can only repair files it rediscovers via Path.iterdir(), so
    a missing-file row would stay broken forever and keep the collection
    stuck in repair mode on every pipeline run."""
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    # Real folder with one real file (broken) and one DB row pointing at
    # a deleted file (also broken-looking in the DB).
    real_dir = tmp_path / "photos"
    real_dir.mkdir()
    present = real_dir / "present.jpg"
    Image.new("RGB", (640, 480)).save(str(present), "JPEG")

    fid = db.add_folder(str(real_dir), name="photos")
    p_present = db.add_photo(
        folder_id=fid, filename="present.jpg", extension=".jpg",
        file_size=present.stat().st_size, file_mtime=present.stat().st_mtime,
        timestamp=None,
    )
    p_missing = db.add_photo(
        folder_id=fid, filename="ghost.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp=None,
    )
    db.conn.commit()

    result = _find_broken_metadata_folders(db, [p_present, p_missing])
    assert len(result) == 1
    folder, paths = result[0]
    assert folder == str(real_dir)
    assert paths == [str(present)]


def test_find_broken_metadata_folders_chunks_large_id_lists(app_and_db, monkeypatch):
    """Passing more photo_ids than SQLite's default variable cap (999) must
    not raise 'too many SQL variables'. The helper chunks internally."""
    import pipeline_job
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    monkeypatch.setattr(pipeline_job.os.path, "isfile", lambda _p: True)

    folder_id = db.conn.execute(
        "SELECT id FROM folders WHERE path='/photos/2024'"
    ).fetchone()["id"]
    # Bulk-insert 1200 photos, all with populated timestamps (none broken).
    db.conn.executemany(
        "INSERT INTO photos (folder_id, filename, extension, file_size, "
        "file_mtime, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (folder_id, f"bulk_{i}.jpg", ".jpg", 1000, float(i),
             "2024-01-01T00:00:00")
            for i in range(1200)
        ],
    )
    db.conn.commit()

    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    assert len(photo_ids) > 999
    # Must not raise OperationalError: too many SQL variables.
    assert _find_broken_metadata_folders(db, photo_ids) == []

    # Break one of the bulk rows and confirm it's still found across chunks.
    target = db.conn.execute(
        "SELECT id, folder_id FROM photos WHERE filename='bulk_1100.jpg'"
    ).fetchone()
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL WHERE id=?", (target["id"],)
    )
    db.conn.commit()
    result = _find_broken_metadata_folders(db, photo_ids)
    assert len(result) == 1
    assert any(p.endswith("bulk_1100.jpg") for p in result[0][1])


def test_pipeline_with_healthy_collection_skips_scan(app_and_db):
    """A pipeline run against a collection of healthy photos reports the
    scan stage as skipped — preserving existing behavior when nothing's
    broken. The downstream thumbnail stage will fail because the fixture
    files don't exist on disk; we only care about the scan step."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)

    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    col_id = db.add_collection(
        "healthy", json.dumps([{"field": "photo_ids", "value": photo_ids}])
    )

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "skip_extract_masks": True,
            "skip_regroup": True,
            "skip_classify": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]

        data = wait_for_job_via_client(client, job_id)

        scan_step = next(s for s in data["steps"] if s["id"] == "scan")
        assert scan_step.get("summary") == "Skipped (using collection)"


def test_pipeline_with_broken_collection_repairs_metadata(app_and_db, tmp_path, monkeypatch):
    """A pipeline run against a collection with broken-metadata photos
    triggers a targeted repair scan before downstream stages. Broken
    rows end with populated timestamp and corrected dimensions."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)

    # Create a real image file on disk and register a new folder+photo
    # pointing at it. The fixture's /photos/... folders don't exist on
    # disk, so we need our own.
    photos_root = tmp_path / "real_photos"
    photos_root.mkdir()
    image_file = photos_root / "broken.jpg"
    Image.new("RGB", (640, 480), color="red").save(str(image_file), "JPEG")

    fid = db.add_folder(str(photos_root), name="real_photos")
    db.add_workspace_folder(db._active_workspace_id, fid)
    pid = db.add_photo(
        folder_id=fid, filename="broken.jpg", extension=".jpg",
        file_size=image_file.stat().st_size,
        file_mtime=image_file.stat().st_mtime,
        timestamp=None, width=160, height=120,
    )
    # Force broken RAW-thumbnail state so the detection rule fires.
    db.conn.execute(
        "UPDATE photos SET extension='.nef', timestamp=NULL, "
        "exif_data=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    # Mock ExifTool so the test doesn't depend on the binary.
    import scanner
    def fake_extract(paths, restricted_tags=None):
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {"DateTimeOriginal": "2024:06:15 10:00:00"},
                    "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    col_id = db.add_collection(
        "broken", json.dumps([{"field": "photo_ids", "value": [pid]}])
    )

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "skip_extract_masks": True,
            "skip_regroup": True,
            "skip_classify": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]

        data = wait_for_job_via_client(client, job_id)

        assert data["status"] == "completed", data
        scan_step = next(s for s in data["steps"] if s["id"] == "scan")
        # Scan stage should have run (not been skipped) because of repair.
        assert "repair" in (scan_step.get("summary") or "").lower()

    # Verify the broken row now has correct metadata.
    row = db.conn.execute(
        "SELECT timestamp, width, height FROM photos WHERE id=?", (pid,)
    ).fetchone()
    assert row["timestamp"] == "2024-06-15T10:00:00"
    assert row["width"] == 640
    assert row["height"] == 480


def test_pipeline_repair_does_not_ingest_untracked_files(app_and_db, tmp_path, monkeypatch):
    """When the repair path scans a folder to fix broken metadata, new
    files that were added to that folder but never scanned do NOT get
    ingested as a side effect. The repair must touch only the specific
    photos flagged as broken."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)

    # Create a real folder with two files — one in the DB (broken), one
    # untracked. The repair run should touch the broken one and leave
    # the untracked one alone.
    photos_root = tmp_path / "real_photos"
    photos_root.mkdir()
    tracked = photos_root / "tracked.jpg"
    untracked = photos_root / "untracked.jpg"
    Image.new("RGB", (640, 480), color="red").save(str(tracked), "JPEG")
    Image.new("RGB", (640, 480), color="blue").save(str(untracked), "JPEG")

    fid = db.add_folder(str(photos_root), name="real_photos")
    db.add_workspace_folder(db._active_workspace_id, fid)
    pid = db.add_photo(
        folder_id=fid, filename="tracked.jpg", extension=".jpg",
        file_size=tracked.stat().st_size,
        file_mtime=tracked.stat().st_mtime,
    )
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL, exif_data=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    import scanner
    def fake_extract(paths, restricted_tags=None):
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {"DateTimeOriginal": "2024:06:15 10:00:00"},
                    "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    col_id = db.add_collection(
        "broken_tracked",
        json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "skip_extract_masks": True,
            "skip_regroup": True,
            "skip_classify": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]

        data = wait_for_job_via_client(client, job_id)

    # Tracked photo repaired: timestamp populated.
    tracked_row = db.conn.execute(
        "SELECT timestamp FROM photos WHERE filename='tracked.jpg'"
    ).fetchone()
    assert tracked_row["timestamp"] == "2024-06-15T10:00:00"
    # Untracked file NOT ingested.
    untracked_row = db.conn.execute(
        "SELECT COUNT(*) AS n FROM photos WHERE filename='untracked.jpg'"
    ).fetchone()
    assert untracked_row["n"] == 0


def test_pipeline_with_broken_collection_handles_unreachable_folder(app_and_db):
    """A pipeline run where a broken photo's underlying file no longer
    exists on disk handles the missing file gracefully — the broken row
    is filtered out of the repair scope (since the scanner couldn't
    repair it anyway), and the scan stage falls through to the normal
    'Skipped (using collection)' fast path instead of looping forever
    in repair mode."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)

    bird1 = db.conn.execute(
        "SELECT id FROM photos WHERE filename='bird1.jpg'"
    ).fetchone()["id"]
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL, exif_data=NULL WHERE id=?",
        (bird1,),
    )
    db.conn.commit()

    col_id = db.add_collection(
        "broken_unreachable",
        json.dumps([{"field": "photo_ids", "value": [bird1]}]),
    )

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "skip_extract_masks": True,
            "skip_regroup": True,
            "skip_classify": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]

        data = wait_for_job_via_client(client, job_id)

        # The scan step should have completed without error. With the
        # broken row's file missing on disk, the repair-target filter
        # drops it and the scan stage falls through to "Skipped (using
        # collection)" — this is exactly the behavior we want to keep
        # the collection from getting stuck in repair mode forever.
        scan_step = next(s for s in data["steps"] if s["id"] == "scan")
        assert scan_step["status"] == "completed"
        assert scan_step.get("summary") == "Skipped (using collection)"


def test_pipeline_repair_does_not_double_process_thumbnails(
    app_and_db, tmp_path, monkeypatch,
):
    """Repaired photos must only be processed once by the thumbnail stage.
    The collection-replay loop in thumbnail_stage() already covers every
    photo in the collection, so the scan-queue callback must not also
    enqueue repaired files — doing so would generate the thumbnail twice
    and inflate the thumbnail totals beyond the collection size."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)

    photos_root = tmp_path / "real_photos"
    photos_root.mkdir()
    image_file = photos_root / "broken.jpg"
    Image.new("RGB", (640, 480), color="red").save(str(image_file), "JPEG")

    fid = db.add_folder(str(photos_root), name="real_photos")
    db.add_workspace_folder(db._active_workspace_id, fid)
    pid = db.add_photo(
        folder_id=fid, filename="broken.jpg", extension=".jpg",
        file_size=image_file.stat().st_size,
        file_mtime=image_file.stat().st_mtime,
        timestamp=None, width=160, height=120,
    )
    db.conn.execute(
        "UPDATE photos SET extension='.nef', timestamp=NULL, "
        "exif_data=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    import scanner
    def fake_extract(paths, restricted_tags=None):
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {"DateTimeOriginal": "2024:06:15 10:00:00"},
                    "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    # Count generate_thumbnail invocations per photo_id. If the P2 bug
    # existed the repaired photo would be generated twice (once from the
    # scan queue, once from the collection replay).
    from collections import Counter
    calls = Counter()

    import thumbnails
    real_generate = thumbnails.generate_thumbnail
    def counting_generate(photo_id, *args, **kwargs):
        calls[photo_id] += 1
        return real_generate(photo_id, *args, **kwargs)
    monkeypatch.setattr(thumbnails, "generate_thumbnail", counting_generate)

    col_id = db.add_collection(
        "broken_thumb", json.dumps([{"field": "photo_ids", "value": [pid]}])
    )

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "skip_extract_masks": True,
            "skip_regroup": True,
            "skip_classify": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]

        data = wait_for_job_via_client(client, job_id)

        assert data["status"] == "completed", data

        thumb_step = next(s for s in data["steps"] if s["id"] == "thumbnails")
        # Progress total must match the collection size, not double it.
        progress = thumb_step.get("progress") or {}
        assert progress.get("total") == 1, thumb_step

    # Each repaired photo should be handed to generate_thumbnail exactly once.
    assert calls[pid] == 1, dict(calls)


def test_pipeline_repair_respects_excluded_photo_ids(
    app_and_db, monkeypatch,
):
    """Photos excluded from the run via ``exclude_photo_ids`` must not
    trigger a repair scan — the downstream stages would skip them anyway,
    so rewriting their metadata would be unexpected out-of-scope work."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)

    # The fixture's bird1.jpg doesn't exist on disk, but that's fine —
    # we only need to verify the scan stage takes the fast-path skip
    # instead of firing the repair scan. Force it into broken state so
    # that without the exclusion filter it WOULD be picked up for repair.
    bird1 = db.conn.execute(
        "SELECT id FROM photos WHERE filename='bird1.jpg'"
    ).fetchone()["id"]
    bird2 = db.conn.execute(
        "SELECT id FROM photos WHERE filename='bird2.jpg'"
    ).fetchone()["id"]
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL, exif_data=NULL WHERE id=?",
        (bird1,),
    )
    db.conn.commit()

    # If repair ever runs for the excluded photo it would call
    # extract_metadata; fail loudly if so.
    import scanner
    def fail_extract(paths, restricted_tags=None):
        raise AssertionError(
            f"extract_metadata must not run for excluded photos: {paths}"
        )
    monkeypatch.setattr(scanner, "extract_metadata", fail_extract)

    col_id = db.add_collection(
        "with_excluded",
        json.dumps([{"field": "photo_ids", "value": [bird1, bird2]}]),
    )

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "exclude_photo_ids": [bird1],
            "skip_extract_masks": True,
            "skip_regroup": True,
            "skip_classify": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]

        data = wait_for_job_via_client(client, job_id)

        # The scan step should have reported the "Skipped" fast path
        # because the only broken photo was excluded from the run.
        # Downstream stages may still fail on the stub fixture files —
        # that's unrelated and does not affect the scan-stage assertion.
        scan_step = next(s for s in data["steps"] if s["id"] == "scan")
        summary = (scan_step.get("summary") or "").lower()
        assert "skipped" in summary, scan_step
        assert "repair" not in summary, scan_step

    # Excluded photo's broken metadata is untouched.
    row = db.conn.execute(
        "SELECT timestamp, exif_data FROM photos WHERE id=?", (bird1,)
    ).fetchone()
    assert row["timestamp"] is None
    assert row["exif_data"] is None


# ---------------------------------------------------------------------------
# /api/jobs/extract-masks — standalone masking route writes photo_masks rows.
#
# Phase 2 of the SAM mask history plan migrated the unified pipeline's
# masking stage to write photo_masks rows. The standalone route in
# api_job_extract_masks is a separate code path and must follow the
# same flow: write a per-variant photo_masks row, denormalize via
# set_active_mask_variant, and skip SAM when the cached row's prompt
# still matches the photo's primary detection.
# ---------------------------------------------------------------------------


def _patch_extract_masks_deps(monkeypatch, generate_mask_calls):
    """Stub out the heavy SAM2 / DINOv2 / proxy / quality modules so the
    extract-masks route runs deterministically without ONNX weights.

    `generate_mask_calls` is a list mutated in place — every fake
    generate_mask call appends a (variant, det_box_tuple) tuple. Tests
    assert against this list to check whether SAM was invoked.
    """
    import dino_embed
    import masking
    import numpy as np
    import quality

    def fake_render_proxy(image_path, longest_edge=None):
        return np.zeros((4, 4, 3), dtype=np.uint8)

    def fake_generate_mask(proxy, det_box, variant=None):
        generate_mask_calls.append(
            (variant, tuple(sorted(det_box.items())))
        )
        return np.ones((4, 4), dtype=bool)

    monkeypatch.setattr(masking, "render_proxy", fake_render_proxy)
    monkeypatch.setattr(masking, "generate_mask", fake_generate_mask)
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 0.9)
    monkeypatch.setattr(
        masking, "crop_subject", lambda p, m, margin=0.15: None,
    )
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


def _seed_photo_with_detection(db, tmp_path, filename, box, model):
    """Create a folder + photo + JPEG on disk + a single detection row.
    Returns the photo_id."""
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = None
    existing = db.conn.execute(
        "SELECT id FROM folders WHERE path=?", (folder_path,)
    ).fetchone()
    if existing:
        folder_id = existing["id"]
    else:
        folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, filename, ".jpg", 1000, 1.0)
    Image.new("RGB", (16, 16), "black").save(
        os.path.join(folder_path, filename)
    )
    x, y, w, h = box
    db.save_detections(
        pid,
        [{"box": {"x": x, "y": y, "w": w, "h": h},
          "confidence": 0.9, "category": "animal"}],
        detector_model=model,
    )
    return pid


def test_extract_masks_route_writes_photo_masks_row(
    app_and_db, tmp_path, monkeypatch,
):
    """POSTing /api/jobs/extract-masks must write a photo_masks row for
    the configured variant with the right prompt, AND set
    photos.active_mask_variant. Before the Phase 2 fix-up the route
    only updated photos.mask_path and left photo_masks empty."""
    import config as cfg
    cfg.save({
        "pipeline": {
            "sam2_variant": "sam2-small",
            "dinov2_variant": "vit-b14",
        },
    })

    app, db = app_and_db
    pid = _seed_photo_with_detection(
        db, tmp_path, "bird.jpg", (10, 20, 100, 200), "MegaDetector",
    )

    calls = []
    _patch_extract_masks_deps(monkeypatch, calls)

    col_id = db.add_collection(
        "extract-masks-test",
        json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    client = app.test_client()
    resp = client.post(
        "/api/jobs/extract-masks", json={"collection_id": col_id},
    )
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]
    data = wait_for_job_via_client(client, job_id)
    assert data["status"] == "completed", data

    # SAM was invoked exactly once.
    assert len(calls) == 1, calls

    # photo_masks row exists with the right variant + prompt.
    row = db.conn.execute(
        "SELECT variant, detector_model, prompt_x, prompt_y, "
        "prompt_w, prompt_h, path FROM photo_masks WHERE photo_id=?",
        (pid,),
    ).fetchone()
    assert row is not None, "photo_masks row must be written"
    assert row["variant"] == "sam2-small"
    assert row["detector_model"] == "MegaDetector"
    assert row["prompt_x"] == 10
    assert row["prompt_y"] == 20
    assert row["prompt_w"] == 100
    assert row["prompt_h"] == 200
    assert row["path"] and os.path.isfile(row["path"]), (
        "mask PNG must be on disk"
    )

    # photos.active_mask_variant was denormalized.
    pr = db.conn.execute(
        "SELECT active_mask_variant, mask_path FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert pr["active_mask_variant"] == "sam2-small"
    assert pr["mask_path"] == row["path"]


def test_extract_masks_route_skips_sam_when_cached(
    app_and_db, tmp_path, monkeypatch,
):
    """Re-hitting /api/jobs/extract-masks with the same configured
    variant + unchanged detection prompt must skip generate_mask: the
    cached photo_masks row is reused and active_mask_variant is
    re-applied. Before the Phase 2 fix-up the route's `mask_path
    IS NULL` gate skipped these photos entirely instead, so the cache
    short-circuit was never tested for this route."""
    import config as cfg
    cfg.save({
        "pipeline": {
            "sam2_variant": "sam2-small",
            "dinov2_variant": "vit-b14",
        },
    })

    app, db = app_and_db
    pid = _seed_photo_with_detection(
        db, tmp_path, "bird.jpg", (10, 20, 100, 200), "MegaDetector",
    )

    calls = []
    _patch_extract_masks_deps(monkeypatch, calls)

    col_id = db.add_collection(
        "extract-masks-cache-test",
        json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    client = app.test_client()

    # First run: SAM is called once.
    resp = client.post(
        "/api/jobs/extract-masks", json={"collection_id": col_id},
    )
    assert resp.status_code == 200
    data = wait_for_job_via_client(
        client, resp.get_json()["job_id"],
    )
    assert data["status"] == "completed"
    assert len(calls) == 1, (
        f"first run should call generate_mask once; got {calls}"
    )

    # Second run with the same config + same detection: SAM must NOT
    # be called again. The cache hit re-applies active_mask_variant
    # and counts the photo as masked without re-running SAM.
    calls.clear()
    resp2 = client.post(
        "/api/jobs/extract-masks", json={"collection_id": col_id},
    )
    assert resp2.status_code == 200
    data2 = wait_for_job_via_client(
        client, resp2.get_json()["job_id"],
    )
    assert data2["status"] == "completed"
    assert calls == [], (
        f"second run with cached prompt must skip generate_mask; got {calls}"
    )

    # Still exactly one row for this (photo, variant).
    n = db.conn.execute(
        "SELECT COUNT(*) FROM photo_masks "
        "WHERE photo_id=? AND variant='sam2-small'",
        (pid,),
    ).fetchone()[0]
    assert n == 1


def test_extract_masks_route_workspace_branch_respects_detector_confidence(
    app_and_db, tmp_path, monkeypatch,
):
    """Without a ``collection_id``, the workspace SQL branch must filter
    detections by the workspace-effective ``detector_confidence`` floor —
    matching the collection branch (which goes through
    ``get_detections``) and the legacy ``get_photos_missing_masks``
    path. Otherwise SAM/DINO runs on noisy below-threshold boxes and
    activates masks the user shouldn't see."""
    import config as cfg
    cfg.save({
        "pipeline": {
            "sam2_variant": "sam2-small",
            "dinov2_variant": "vit-b14",
        },
        "detector_confidence": 0.5,
    })

    app, db = app_and_db

    # Two photos in the workspace, both with a non-full-image detection.
    # Photo A's detection clears the 0.5 floor; Photo B's does not.
    pid_high = _seed_photo_with_detection(
        db, tmp_path, "above.jpg", (10, 20, 100, 200), "MegaDetector",
    )
    pid_low = _seed_photo_with_detection(
        db, tmp_path, "below.jpg", (30, 40, 80, 90), "MegaDetector",
    )
    # _seed_photo_with_detection hardcodes confidence=0.9, so override
    # the low-confidence row directly.
    db.conn.execute(
        "UPDATE detections SET detector_confidence=0.05 WHERE photo_id=?",
        (pid_low,),
    )
    db.conn.commit()

    calls = []
    _patch_extract_masks_deps(monkeypatch, calls)

    client = app.test_client()
    resp = client.post("/api/jobs/extract-masks", json={})
    assert resp.status_code == 200
    data = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert data["status"] == "completed", data

    # SAM was invoked exactly once — for the above-threshold photo only.
    assert len(calls) == 1, calls

    # photo_masks row exists for the above-threshold photo.
    high_row = db.conn.execute(
        "SELECT variant FROM photo_masks WHERE photo_id=?", (pid_high,),
    ).fetchone()
    assert high_row is not None, "above-threshold photo must be masked"

    # Below-threshold photo is skipped: no photo_masks row, no
    # active variant denormalized onto photos.
    low_row = db.conn.execute(
        "SELECT variant FROM photo_masks WHERE photo_id=?", (pid_low,),
    ).fetchone()
    assert low_row is None, (
        "below-threshold photo must not get a mask"
    )
    low_active = db.conn.execute(
        "SELECT active_mask_variant FROM photos WHERE id=?", (pid_low,),
    ).fetchone()
    assert low_active["active_mask_variant"] is None
