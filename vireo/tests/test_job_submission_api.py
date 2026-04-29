"""Tests for job submission and history API routes."""

import os
import time

from PIL import Image
from wait import wait_for_job_via_client


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


def test_job_cull_passes_thumb_cache_parent_as_vireo_dir(tmp_path, monkeypatch):
    """api_job_cull must derive vireo_dir from THUMB_CACHE_DIR's parent,
    matching scan/classify — not from db_path's parent. Users who pass a
    custom --thumb-dir on a filesystem separate from the DB must still
    have culling find their working copies in the right place."""
    import config as cfg
    import culling as culling_module
    from app import create_app
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # DB and thumbnails live under DIFFERENT parents — the bug case.
    db_dir = tmp_path / "db_root"
    db_dir.mkdir()
    db_path = str(db_dir / "app.db")

    thumb_parent = tmp_path / "thumb_root"
    thumb_parent.mkdir()
    thumb_dir = str(thumb_parent / "thumbnails")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)

    captured = {}

    def spy(db, **kwargs):
        captured.update(kwargs)
        return {
            "species_groups": [],
            "total_photos": 0,
            "suggested_keepers": 0,
            "suggested_rejects": 0,
            "photos_missing_phash": 0,
        }

    monkeypatch.setattr(culling_module, "analyze_for_culling", spy)

    client = app.test_client()
    resp = client.post("/api/jobs/cull", json={})
    assert resp.status_code == 200

    deadline = time.time() + 5.0
    while "vireo_dir" not in captured and time.time() < deadline:
        time.sleep(0.02)

    assert "vireo_dir" in captured, "analyze_for_culling was never called"
    assert captured["vireo_dir"] == str(thumb_parent), (
        f"expected {thumb_parent!r}, got {captured['vireo_dir']!r}"
    )


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

    # Reads /api/jobs/history below — must wait for the row to flush.
    wait_for_job_via_client(client, job_id, wait_for_history=True)

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

    # Reads /api/jobs/history below — must wait for the row to flush.
    wait_for_job_via_client(client, job_id, wait_for_history=True)

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
