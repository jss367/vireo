import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from PIL import Image


def _touch_image(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    Image.new("RGB", (1, 1), "white").save(path, "JPEG")


@pytest.fixture(autouse=True)
def _clear_shared_new_images_cache():
    from new_images import get_shared_cache
    get_shared_cache().clear()
    yield
    get_shared_cache().clear()


@pytest.fixture
def app_and_db(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    from app import create_app
    from db import Database

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    return app, db, ws_id, tmp_path


def test_api_new_images_reports_unscanned_files(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    db.add_folder(str(root), name="shoot")

    client = app.test_client()
    resp = client.get("/api/workspaces/active/new-images")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["new_count"] == 1
    assert len(data["per_root"]) == 1
    assert data["workspace_id"] == ws_id


def test_api_new_images_zero_when_fully_ingested(app_and_db):
    app, db, ws_id, tmp_path = app_and_db
    root = tmp_path / "shoot"
    _touch_image(str(root / "IMG.JPG"))
    fid = db.add_folder(str(root), name="shoot")
    db.add_photo(folder_id=fid, filename="IMG.JPG", extension=".JPG",
                 file_size=1, file_mtime=0.0)

    client = app.test_client()
    resp = client.get("/api/workspaces/active/new-images")
    data = resp.get_json()
    assert data["new_count"] == 0
    assert data["workspace_id"] == ws_id


def test_api_new_images_returns_null_workspace_when_none_active(app_and_db, monkeypatch):
    app, db, ws_id, tmp_path = app_and_db
    # Each request creates its own Database via _get_db(), which auto-restores
    # the last-used workspace. To simulate "no active workspace", patch
    # set_active_workspace to a no-op so the per-request db starts with
    # _active_workspace_id = None.
    from db import Database
    monkeypatch.setattr(Database, "set_active_workspace",
                        lambda self, ws_id: None)

    client = app.test_client()
    resp = client.get("/api/workspaces/active/new-images")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["workspace_id"] is None
    assert data["new_count"] == 0
    assert data["per_root"] == []
