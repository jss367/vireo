import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from db import Database
from PIL import Image


@pytest.fixture
def app_and_db(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    from app import create_app

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(tmp_path / "library"), name="library")

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    return app, db, fid


def parse_sse_events(response_data):
    """Parse SSE events from raw response bytes."""
    text = response_data.decode("utf-8")
    events = []
    for block in text.split("\n\n"):
        for line in block.strip().split("\n"):
            if line.startswith("data: "):
                events.append(json.loads(line[6:]))
    return events


def test_check_duplicates_marks_known_hashes(app_and_db, tmp_path):
    """Files whose hash exists in DB are reported as duplicates."""
    app, db, fid = app_and_db

    # Create an image that exists in the "library" (scanned, hash in DB)
    library_dir = tmp_path / "library"
    library_dir.mkdir(exist_ok=True)
    img = Image.new("RGB", (50, 50), color="red")
    img.save(str(library_dir / "existing.jpg"))

    # Scan to populate file_hash
    from scanner import scan
    scan(str(library_dir), db)

    # Create source folder with a duplicate and a new file
    source = tmp_path / "source"
    source.mkdir()
    img.save(str(source / "duplicate.jpg"))  # Same content = same hash
    Image.new("RGB", (50, 50), color="blue").save(str(source / "unique.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "duplicate.jpg"), str(source / "unique.jpg")],
    })
    assert resp.status_code == 200

    events = parse_sse_events(resp.data)
    # Find the done event
    done = [e for e in events if e.get("done")]
    assert len(done) == 1
    assert done[0]["duplicate_count"] == 1

    # Collect all duplicate paths across batch events
    all_duplicates = []
    for e in events:
        if "duplicates" in e:
            all_duplicates.extend(e["duplicates"])
    assert str(source / "duplicate.jpg") in all_duplicates
    assert str(source / "unique.jpg") not in all_duplicates


def test_check_duplicates_no_paths(app_and_db):
    """Returns error when no paths provided."""
    app, _, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={"paths": []})
    assert resp.status_code == 400


def test_check_duplicates_all_new(app_and_db, tmp_path):
    """When no files match DB hashes, duplicate_count is 0."""
    app, db, fid = app_and_db

    source = tmp_path / "source"
    source.mkdir()
    Image.new("RGB", (50, 50), color="green").save(str(source / "new1.jpg"))
    Image.new("RGB", (50, 50), color="yellow").save(str(source / "new2.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "new1.jpg"), str(source / "new2.jpg")],
    })

    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["duplicate_count"] == 0


def test_check_duplicates_missing_file_skipped(app_and_db, tmp_path):
    """Missing files are skipped without crashing."""
    app, db, fid = app_and_db

    source = tmp_path / "source"
    source.mkdir()
    Image.new("RGB", (50, 50), color="green").save(str(source / "real.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "real.jpg"), str(source / "gone.jpg")],
    })
    assert resp.status_code == 200

    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert len(done) == 1
    assert done[0]["checked"] == 2  # Both counted as checked
