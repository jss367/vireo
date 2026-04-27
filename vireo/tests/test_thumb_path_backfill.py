"""Startup self-healing for photos.thumb_path.

The dashboard's coverage card reads ``thumb_path IS NOT NULL`` to count how
many photos have a generated thumbnail. Until PR #6xx the column was never
populated by production code, so the dashboard always reported "0 of N
thumbnails made" even when 40k JPEGs sat on disk in ``~/.vireo/thumbnails/``.

These tests exercise the ephemeral startup job that:

* sets ``thumb_path`` for legacy photos whose JPEG already exists on disk, and
* clears ``thumb_path`` if the file has since been deleted (drift correction).
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from PIL import Image


def _make_jpeg(path, w=200, h=150):
    Image.new("RGB", (w, h), (100, 100, 100)).save(str(path), "JPEG", quality=85)


def _wait_for_job(runner, job_type, deadline_s=5):
    deadline = time.time() + deadline_s
    while time.time() < deadline:
        jobs = [j for j in runner.list_jobs() if j["type"] == job_type]
        if jobs and jobs[0]["status"] in ("completed", "failed"):
            return jobs[0]
        time.sleep(0.05)
    return None


def test_startup_thumb_path_backfill_skips_when_synced(tmp_path, monkeypatch):
    """If every photo's thumb_path matches disk reality, no ephemeral
    thumb_path_backfill job is started — steady-state restarts pay nothing."""
    import config as cfg
    import models
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    from app import create_app
    from db import Database

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)
    Database(db_path)  # create empty DB with workspace

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir, api_token="t")
    app._kickoff_thumb_path_backfill()

    backfill_jobs = [
        j for j in app._job_runner.list_jobs()
        if j["type"] == "thumb_path_backfill"
    ]
    assert backfill_jobs == []


def test_startup_thumb_path_backfill_sets_unsynced_rows(tmp_path, monkeypatch):
    """A photo with NULL thumb_path whose JPEG exists in the cache must have
    its column populated by the startup ephemeral job. This is the production
    repair path: 40k thumbnails on disk reported as 0/40k forever."""
    import config as cfg
    import models
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    from app import create_app
    from db import Database

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "a.jpg"
    _make_jpeg(str(src))

    db_path = str(tmp_path / "test.db")
    thumb_dir = tmp_path / "thumbs"
    thumb_dir.mkdir()

    db = Database(db_path)
    folder_id = db.add_folder(str(photos_dir))
    pid = db.add_photo(
        folder_id, "a.jpg", ".jpg",
        file_size=os.path.getsize(str(src)),
        file_mtime=os.path.getmtime(str(src)),
        width=200, height=150,
    )
    # Pre-existing on-disk thumbnail (the legacy state).
    _make_jpeg(thumb_dir / f"{pid}.jpg")
    db.conn.close()

    app = create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir), api_token="t")
    app._kickoff_thumb_path_backfill()

    job = _wait_for_job(app._job_runner, "thumb_path_backfill")
    assert job is not None, "backfill job should have been started"
    assert job["status"] == "completed", f"job: {job}"
    assert job.get("ephemeral") is True

    db2 = Database(db_path)
    row = db2.conn.execute(
        "SELECT thumb_path FROM photos WHERE id=?", (pid,)
    ).fetchone()
    assert row["thumb_path"] == f"{pid}.jpg"


def test_startup_thumb_path_backfill_does_not_persist_to_history(tmp_path, monkeypatch):
    """Ephemeral job must NOT land in job_history — every restart would
    otherwise add a noise row."""
    import config as cfg
    import models
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

    from app import create_app
    from db import Database

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "a.jpg"
    _make_jpeg(str(src))

    db_path = str(tmp_path / "test.db")
    thumb_dir = tmp_path / "thumbs"
    thumb_dir.mkdir()

    db = Database(db_path)
    folder_id = db.add_folder(str(photos_dir))
    pid = db.add_photo(
        folder_id, "a.jpg", ".jpg",
        file_size=os.path.getsize(str(src)),
        file_mtime=os.path.getmtime(str(src)),
        width=200, height=150,
    )
    _make_jpeg(thumb_dir / f"{pid}.jpg")
    db.conn.close()

    app = create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir), api_token="t")
    app._kickoff_thumb_path_backfill()

    _wait_for_job(app._job_runner, "thumb_path_backfill")

    db2 = Database(db_path)
    rows = db2.conn.execute(
        "SELECT id FROM job_history WHERE type='thumb_path_backfill'"
    ).fetchall()
    assert rows == [], "ephemeral job must not persist to history"
