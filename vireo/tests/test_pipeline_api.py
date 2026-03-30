"""Tests for the full-chain import pipeline endpoint."""
import os
import shutil
import tempfile

import pytest
from app import create_app


@pytest.fixture
def setup(tmp_path):
    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir, exist_ok=True)
    app = create_app(db_path, thumb_dir)
    app.config["TESTING"] = True

    import config as cfg
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    return app, db_path


def test_import_full_returns_job_id(setup):
    app, db_path = setup
    # Create a source dir with a JPEG
    src = tempfile.mkdtemp()
    dest = tempfile.mkdtemp()
    try:
        # Create a minimal JPEG file (smallest valid JPEG)
        jpeg_bytes = bytes([
            0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00,
            0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xD9
        ])
        with open(os.path.join(src, "test.jpg"), "wb") as f:
            f.write(jpeg_bytes)

        with app.test_client() as c:
            resp = c.post("/api/jobs/import-full", json={
                "source": src,
                "destination": dest,
            })
            data = resp.get_json()
            assert resp.status_code == 200
            assert "job_id" in data
            assert data["job_id"].startswith("import-full-")
    finally:
        shutil.rmtree(src, ignore_errors=True)
        shutil.rmtree(dest, ignore_errors=True)


def test_import_full_requires_source_and_destination(setup):
    app, db_path = setup
    with app.test_client() as c:
        resp = c.post("/api/jobs/import-full", json={"source": "/tmp/x"})
        assert resp.status_code == 400

        resp = c.post("/api/jobs/import-full", json={"destination": "/tmp/x"})
        assert resp.status_code == 400


def test_import_full_rejects_nonexistent_source(setup):
    app, db_path = setup
    with app.test_client() as c:
        resp = c.post("/api/jobs/import-full", json={
            "source": "/nonexistent/path",
            "destination": "/tmp/dest",
        })
        assert resp.status_code == 400
        assert "not found" in resp.get_json()["error"].lower()


def test_import_full_rejects_relative_destination(setup):
    app, db_path = setup
    src = tempfile.mkdtemp()
    try:
        with app.test_client() as c:
            resp = c.post("/api/jobs/import-full", json={
                "source": src,
                "destination": "relative/path",
            })
            assert resp.status_code == 400
            assert "absolute" in resp.get_json()["error"].lower()
    finally:
        shutil.rmtree(src, ignore_errors=True)


def test_import_full_scan_only_returns_job_id(setup):
    """copy=false skips ingest, just scans the source folder."""
    app, db_path = setup
    src = tempfile.mkdtemp()
    try:
        jpeg_bytes = bytes([
            0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00,
            0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xD9
        ])
        with open(os.path.join(src, "test.jpg"), "wb") as f:
            f.write(jpeg_bytes)

        with app.test_client() as c:
            resp = c.post("/api/jobs/import-full", json={
                "source": src,
                "copy": False,
            })
            data = resp.get_json()
            assert resp.status_code == 200
            assert "job_id" in data
    finally:
        shutil.rmtree(src, ignore_errors=True)


def test_import_full_scan_only_no_destination_required(setup):
    """copy=false does not require destination."""
    app, db_path = setup
    src = tempfile.mkdtemp()
    try:
        with app.test_client() as c:
            resp = c.post("/api/jobs/import-full", json={
                "source": src,
                "copy": False,
            })
            assert resp.status_code == 200
    finally:
        shutil.rmtree(src, ignore_errors=True)


def test_import_full_copy_true_still_requires_destination(setup):
    """copy=true (explicit) still requires destination."""
    app, db_path = setup
    src = tempfile.mkdtemp()
    try:
        with app.test_client() as c:
            resp = c.post("/api/jobs/import-full", json={
                "source": src,
                "copy": True,
            })
            assert resp.status_code == 400
    finally:
        shutil.rmtree(src, ignore_errors=True)
