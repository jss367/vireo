import os
import socket
import sys
import threading

import pytest
from PIL import Image
from werkzeug.serving import make_server

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'vireo'))

import config as cfg
from app import create_app
from db import Database


def _free_port():
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def seed_e2e_data(db, thumb_dir):
    """Seed database with data for E2E tests."""
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    f1 = db.add_folder("/photos/park", name="park")
    f2 = db.add_folder("/photos/yard", name="yard")

    photos = []
    for fid, name, ts in [
        (f1, "hawk1.jpg", "2024-03-10T08:00:00"),
        (f1, "hawk2.jpg", "2024-03-10T08:01:00"),
        (f1, "hawk3.jpg", "2024-03-10T08:02:00"),
        (f2, "robin1.jpg", "2024-06-15T10:00:00"),
        (f2, "robin2.jpg", "2024-06-15T10:01:00"),
    ]:
        pid = db.add_photo(
            folder_id=fid, filename=name, extension=".jpg",
            file_size=1000, file_mtime=1.0, timestamp=ts,
        )
        photos.append(pid)
        Image.new("RGB", (100, 100), color="green").save(
            os.path.join(thumb_dir, f"{pid}.jpg")
        )

    db.update_photo_rating(photos[0], 4)

    k1 = db.add_keyword("Red-tailed Hawk", is_species=True)
    k2 = db.add_keyword("American Robin", is_species=True)
    db.tag_photo(photos[0], k1)
    db.tag_photo(photos[3], k2)

    db.create_workspace("Field Work")

    return {"photos": photos, "folders": [f1, f2]}


@pytest.fixture()
def live_server(tmp_path, monkeypatch):
    """Start an isolated Flask server with seeded E2E data."""
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    seed_data = seed_e2e_data(db, thumb_dir)

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)

    port = _free_port()
    server = make_server("127.0.0.1", port, app)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()

    yield {"url": f"http://127.0.0.1:{port}", "db": db, "data": seed_data}

    server.shutdown()
    thread.join(timeout=5)
