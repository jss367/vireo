import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from db import Database
from PIL import Image


@pytest.fixture
def db(tmp_path):
    """Return a Database backed by a temp file."""
    return Database(str(tmp_path / "test.db"))


@pytest.fixture
def app_and_db(tmp_path, monkeypatch):
    """Create a test app with sample data."""
    from db import Database
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    # `models.DEFAULT_MODELS_DIR` and `models.CONFIG_PATH` are resolved
    # at import time from the real `~`. Redirect them so tests don't see
    # the developer's locally-downloaded weights and don't write to the
    # real `~/.vireo/models.json`.
    monkeypatch.setattr(
        models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"),
    )
    monkeypatch.setattr(
        models, "CONFIG_PATH", str(tmp_path / "models.json"),
    )

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder('/photos/2024', name='2024')
    fid2 = db.add_folder('/photos/2024/January', name='January', parent_id=fid)

    p1 = db.add_photo(folder_id=fid, filename='bird1.jpg', extension='.jpg',
                      file_size=1000, file_mtime=1.0, timestamp='2024-01-15T10:00:00')
    p2 = db.add_photo(folder_id=fid2, filename='bird2.jpg', extension='.jpg',
                      file_size=2000, file_mtime=2.0, timestamp='2024-01-20T14:00:00')
    p3 = db.add_photo(folder_id=fid, filename='bird3.jpg', extension='.jpg',
                      file_size=3000, file_mtime=3.0, timestamp='2024-06-10T09:00:00')

    db.update_photo_rating(p1, 3)
    db.update_photo_rating(p3, 5)

    k1 = db.add_keyword('Cardinal')
    k2 = db.add_keyword('Sparrow')
    db.tag_photo(p1, k1)
    db.tag_photo(p2, k2)

    for pid in [p1, p2, p3]:
        Image.new('RGB', (100, 100)).save(os.path.join(thumb_dir, f"{pid}.jpg"))

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir, api_token="test-token-123")
    return app, db


@pytest.fixture
def client_with_photo(tmp_path, monkeypatch):
    """Flask test client with one real photo whose source file exists.

    Returns (app, db, photo_id). Use for preview/LRU tests that need
    serve_photo_preview to actually generate a preview file.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(
        models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"),
    )
    monkeypatch.setattr(
        models, "CONFIG_PATH", str(tmp_path / "models.json"),
    )

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "test.jpg"
    Image.new("RGB", (800, 600), (180, 90, 40)).save(str(src), "JPEG", quality=85)

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="test.jpg", extension=".jpg",
        file_size=os.path.getsize(src),
        file_mtime=os.path.getmtime(src),
        width=800, height=600,
    )

    app = create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir), api_token="test-token-123")
    return app, db, pid
