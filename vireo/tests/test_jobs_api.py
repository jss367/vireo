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
