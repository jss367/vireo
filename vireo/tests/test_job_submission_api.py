"""Tests for job submission and history API routes."""

import os
import time

from PIL import Image


def test_job_thumbnails_returns_job_id(app_and_db):
    """POST /api/jobs/thumbnails returns job_id starting with 'thumbnails-'."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/thumbnails")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("thumbnails-")


def test_job_cull_returns_job_id(app_and_db):
    """POST /api/jobs/cull with empty json returns job_id starting with 'cull-'."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/cull", json={})
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("cull-")


def test_job_classify_requires_collection_id(app_and_db):
    """POST /api/jobs/classify with empty json returns 400 with 'collection_id' in error."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/classify", json={})
    assert resp.status_code == 400
    data = resp.get_json()
    assert "collection_id" in data["error"]


def test_job_history(app_and_db, tmp_path):
    """Start a scan job, wait for completion, then GET /api/jobs/history returns a list."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "historytest")
    os.makedirs(scan_dir)
    Image.new("RGB", (100, 100)).save(os.path.join(scan_dir, "photo.jpg"))

    resp = client.post("/api/jobs/scan", json={"root": scan_dir})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    # Poll until job completes or fails
    for _ in range(50):
        status_resp = client.get(f"/api/jobs/{job_id}")
        status_data = status_resp.get_json()
        if status_data.get("status") in ("completed", "failed"):
            break
        time.sleep(0.1)

    history_resp = client.get("/api/jobs/history")
    assert history_resp.status_code == 200
    history = history_resp.get_json()
    assert isinstance(history, list)


def test_job_history_respects_limit(app_and_db, tmp_path):
    """GET /api/jobs/history?limit=1 returns list with at most 1 entry."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "limittest")
    os.makedirs(scan_dir)
    Image.new("RGB", (100, 100)).save(os.path.join(scan_dir, "photo.jpg"))

    resp = client.post("/api/jobs/scan", json={"root": scan_dir})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    for _ in range(50):
        status_resp = client.get(f"/api/jobs/{job_id}")
        status_data = status_resp.get_json()
        if status_data.get("status") in ("completed", "failed"):
            break
        time.sleep(0.1)

    history_resp = client.get("/api/jobs/history?limit=1")
    assert history_resp.status_code == 200
    history = history_resp.get_json()
    assert isinstance(history, list)
    assert len(history) <= 1


def test_job_develop_requires_photo_ids(app_and_db):
    """POST /api/jobs/develop with empty json returns 400 with 'photo_ids' in error."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/develop", json={})
    assert resp.status_code == 400
    data = resp.get_json()
    assert "photo_ids" in data["error"]


def test_job_previews_returns_job_id(app_and_db):
    """POST /api/jobs/previews returns job_id starting with 'previews-'."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/previews")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("previews-")


def test_job_sync_returns_job_id(app_and_db):
    """POST /api/jobs/sync returns job_id starting with 'sync-'."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/sync")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("sync-")
