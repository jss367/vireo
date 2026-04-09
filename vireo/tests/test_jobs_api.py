import os
import time

from PIL import Image


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

    for _ in range(50):
        resp = client.get(f'/api/jobs/{job_id}')
        data = resp.get_json()
        if data['status'] in ('completed', 'failed'):
            break
        time.sleep(0.1)

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
    import time
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'test.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    job_id = resp.get_json()['job_id']

    for _ in range(50):
        resp = client.get(f'/api/jobs/{job_id}')
        data = resp.get_json()
        if data['status'] in ('completed', 'failed'):
            break
        time.sleep(0.1)

    assert data['status'] == 'completed'
    assert 'steps' in data
    assert len(data['steps']) >= 2
    assert data['steps'][0]['id'] == 'scan'
    assert data['steps'][0]['status'] == 'completed'


def test_job_history_includes_parsed_tree(app_and_db, tmp_path):
    """GET /api/jobs/history returns parsed tree data for completed jobs."""
    import time
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'test.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    job_id = resp.get_json()['job_id']

    for _ in range(80):
        resp = client.get(f'/api/jobs/{job_id}')
        data = resp.get_json()
        if data['status'] in ('completed', 'failed'):
            break
        time.sleep(0.1)

    time.sleep(0.5)

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
    import time
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'a.jpg'))
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'b.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    job_id = resp.get_json()['job_id']

    for _ in range(50):
        resp = client.get(f'/api/jobs/{job_id}')
        data = resp.get_json()
        if data['status'] in ('completed', 'failed'):
            break
        time.sleep(0.1)

    assert data['status'] == 'completed'
    scan_step = data['steps'][0]
    assert scan_step['id'] == 'scan'
    # After completion, progress should have current == total
    assert 'progress' in scan_step
    assert scan_step['progress']['current'] == scan_step['progress']['total']
    assert scan_step['progress']['total'] >= 2


def test_pipeline_thumbnail_step_has_progress(app_and_db, tmp_path):
    """Thumbnail step in pipeline reports step-level progress."""
    import time
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

    for _ in range(100):
        resp = client.get(f'/api/jobs/{job_id}')
        data = resp.get_json()
        if data['status'] in ('completed', 'failed'):
            break
        time.sleep(0.1)

    assert data['status'] == 'completed'
    thumb_step = next(s for s in data['steps'] if s['id'] == 'thumbnails')
    assert 'progress' in thumb_step
    assert thumb_step['progress']['current'] > 0


def test_pipeline_preview_step_has_progress(app_and_db, tmp_path):
    """Preview step in pipeline reports step-level progress."""
    import time
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

    for _ in range(100):
        resp = client.get(f'/api/jobs/{job_id}')
        data = resp.get_json()
        if data['status'] in ('completed', 'failed'):
            break
        time.sleep(0.1)

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
