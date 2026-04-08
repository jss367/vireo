"""Tests for photo deletion API and database cleanup."""
import json
import os

from PIL import Image


def test_delete_photos_removes_from_db(app_and_db):
    """Deleting photos removes them from the photos table."""
    app, db = app_and_db
    photos = db.get_photos()
    pid = photos[0]["id"]

    result = db.delete_photos([pid])

    assert result["deleted"] == 1
    assert db.get_photo(pid) is None


def test_delete_photos_removes_keywords(app_and_db):
    """Deleting a photo removes its keyword associations."""
    app, db = app_and_db
    photos = db.get_photos()
    # bird1.jpg has keyword 'Cardinal' (from conftest)
    pid = photos[0]["id"]
    assert len(db.get_photo_keywords(pid)) > 0

    db.delete_photos([pid])

    rows = db.conn.execute(
        "SELECT * FROM photo_keywords WHERE photo_id = ?", (pid,)
    ).fetchall()
    assert len(rows) == 0


def test_delete_photos_removes_predictions(app_and_db):
    """Deleting a photo removes its predictions."""
    app, db = app_and_db
    photos = db.get_photos()
    pid = photos[0]["id"]
    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Cardinal', 0.95, 'test-model')

    db.delete_photos([pid])

    rows = db.conn.execute(
        """SELECT pr.* FROM predictions pr
           JOIN detections d ON d.id = pr.detection_id
           WHERE d.photo_id = ?""", (pid,)
    ).fetchall()
    assert len(rows) == 0


def test_delete_photos_removes_pending_changes(app_and_db):
    """Deleting a photo removes its pending changes."""
    app, db = app_and_db
    photos = db.get_photos()
    pid = photos[0]["id"]
    db.queue_change(pid, "rating", "5")

    db.delete_photos([pid])

    rows = db.conn.execute(
        "SELECT * FROM pending_changes WHERE photo_id = ?", (pid,)
    ).fetchall()
    assert len(rows) == 0


def test_delete_photos_updates_folder_count(app_and_db):
    """Deleting photos decrements the folder's photo_count."""
    app, db = app_and_db
    photos = db.get_photos()
    fid = photos[0]["folder_id"]
    original_count = db.conn.execute(
        "SELECT photo_count FROM folders WHERE id = ?", (fid,)
    ).fetchone()["photo_count"]

    # Delete one photo from this folder
    db.delete_photos([photos[0]["id"]])

    new_count = db.conn.execute(
        "SELECT photo_count FROM folders WHERE id = ?", (fid,)
    ).fetchone()["photo_count"]
    assert new_count == original_count - 1


def test_delete_photos_cleans_collection_rules(app_and_db):
    """Deleting photos removes their IDs from static collection rules."""
    app, db = app_and_db
    photos = db.get_photos()
    pid1, pid2 = photos[0]["id"], photos[1]["id"]

    rules = [{"field": "photo_ids", "value": [pid1, pid2, 9999]}]
    cid = db.add_collection("Test Collection", json.dumps(rules))

    db.delete_photos([pid1])

    row = db.conn.execute("SELECT rules FROM collections WHERE id = ?", (cid,)).fetchone()
    updated_rules = json.loads(row["rules"])
    assert pid1 not in updated_rules[0]["value"]
    assert pid2 in updated_rules[0]["value"]
    assert 9999 in updated_rules[0]["value"]


def test_delete_photos_returns_file_info(app_and_db):
    """delete_photos returns folder paths and photo IDs for file cleanup."""
    app, db = app_and_db
    photos = db.get_photos()
    pid = photos[0]["id"]

    result = db.delete_photos([pid])

    assert result["deleted"] == 1
    assert len(result["files"]) == 1
    assert result["files"][0]["photo_id"] == pid
    assert "folder_path" in result["files"][0]
    assert "filename" in result["files"][0]


def test_delete_photos_skips_missing_ids(app_and_db):
    """Deleting non-existent photo IDs is silently skipped."""
    app, db = app_and_db

    result = db.delete_photos([99999])

    assert result["deleted"] == 0
    assert result["files"] == []


def test_delete_photos_batch(app_and_db):
    """Deleting multiple photos works in a single call."""
    app, db = app_and_db
    photos = db.get_photos()
    all_ids = [p["id"] for p in photos]

    result = db.delete_photos(all_ids)

    assert result["deleted"] == len(all_ids)
    for pid in all_ids:
        assert db.get_photo(pid) is None


def test_delete_photos_resolves_companions(app_and_db):
    """When include_companions=True, companion photos are also deleted."""
    app, db = app_and_db
    photos = db.get_photos()
    # Use photos[0] and photos[2] which are in the same folder (fid)
    pid1, pid2 = photos[0]["id"], photos[2]["id"]

    # Set pid2 as companion of pid1
    db.conn.execute(
        "UPDATE photos SET companion_path = ? WHERE id = ?",
        (photos[2]["filename"], pid1),
    )
    db.conn.commit()

    result = db.delete_photos([pid1], include_companions=True)

    assert result["deleted"] == 2
    assert db.get_photo(pid1) is None
    assert db.get_photo(pid2) is None


def test_delete_photos_empty_list(app_and_db):
    """Calling delete_photos with empty list is a no-op."""
    app, db = app_and_db

    result = db.delete_photos([])

    assert result["deleted"] == 0


def test_api_batch_delete_vireo_mode(app_and_db):
    """API endpoint removes photos from DB without touching disk."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "vireo",
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["deleted"] == 1
    assert db.get_photo(pid) is None


def test_api_batch_delete_disk_mode(app_and_db, tmp_path):
    """API endpoint in disk mode moves files to trash (or deletes them)."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]
    photo = db.get_photo(pid)

    # Point folder to a writable tmp_path location and create a real file
    folder_path = str(tmp_path / "disk_photos")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (folder_path, photo["folder_id"]),
    )
    db.conn.commit()
    os.makedirs(folder_path, exist_ok=True)
    real_file = os.path.join(folder_path, photo["filename"])
    Image.new("RGB", (10, 10)).save(real_file)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "disk",
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["deleted"] == 1
    assert db.get_photo(pid) is None


def test_api_batch_delete_removes_thumbnails(app_and_db):
    """Deleting a photo removes its thumbnail file."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]
    thumb_dir = app.config["THUMB_CACHE_DIR"]
    thumb_path = os.path.join(thumb_dir, f"{pid}.jpg")
    assert os.path.exists(thumb_path)

    client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "vireo",
    })

    assert not os.path.exists(thumb_path)


def test_api_batch_delete_removes_working_copy(app_and_db):
    """Deleting a photo removes its working copy file."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]

    # Create a working copy file for this photo
    thumb_dir = app.config["THUMB_CACHE_DIR"]
    working_dir = os.path.join(os.path.dirname(thumb_dir), "working")
    os.makedirs(working_dir, exist_ok=True)
    working_path = os.path.join(working_dir, f"{pid}.jpg")
    Image.new("RGB", (100, 100)).save(working_path)
    assert os.path.exists(working_path)

    client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "vireo",
    })

    assert not os.path.exists(working_path)


def test_api_batch_delete_requires_photo_ids(app_and_db):
    """API returns error when photo_ids is missing."""
    app, db = app_and_db
    client = app.test_client()

    resp = client.post("/api/batch/delete", json={"mode": "vireo"})

    assert resp.status_code == 400


def test_api_batch_delete_invalid_mode(app_and_db):
    """API returns error for unknown mode."""
    app, db = app_and_db
    client = app.test_client()

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [1],
        "mode": "invalid",
    })

    assert resp.status_code == 400


def test_api_batch_delete_disk_permanent_retry_with_paths(app_and_db, tmp_path):
    """disk_permanent retry works with paths after DB rows are already gone."""
    app, db = app_and_db
    client = app.test_client()

    # Create files to delete
    file1 = str(tmp_path / "photo1.jpg")
    file2 = str(tmp_path / "photo2.jpg")
    Image.new("RGB", (10, 10)).save(file1)
    Image.new("RGB", (10, 10)).save(file2)
    assert os.path.exists(file1)
    assert os.path.exists(file2)

    # Retry with paths (no photo_ids needed)
    resp = client.post("/api/batch/delete", json={
        "mode": "disk_permanent",
        "paths": [file1, file2],
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["trashed"] == 2
    assert not os.path.exists(file1)
    assert not os.path.exists(file2)


def test_api_batch_delete_disk_deletes_companion_file(app_and_db, tmp_path):
    """Disk mode deletes companion files when include_companions is true."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]

    # Set up companion_path on the photo
    db.conn.execute(
        "UPDATE photos SET companion_path = 'companion.jpg' WHERE id = ?",
        (pid,),
    )
    folder_path = str(tmp_path / "disk_photos")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (folder_path, photos[0]["folder_id"]),
    )
    db.conn.commit()

    # Create both primary and companion files
    os.makedirs(folder_path, exist_ok=True)
    primary_file = os.path.join(folder_path, photos[0]["filename"])
    companion_file = os.path.join(folder_path, "companion.jpg")
    Image.new("RGB", (10, 10)).save(primary_file)
    Image.new("RGB", (10, 10)).save(companion_file)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "disk",
        "include_companions": True,
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    # Both primary and companion should have been trashed
    assert data["trashed"] == 2
    assert not os.path.exists(primary_file)
    assert not os.path.exists(companion_file)


def test_api_batch_delete_disk_skips_companion_when_unchecked(app_and_db, tmp_path):
    """Disk mode leaves companion files when include_companions is false."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]

    db.conn.execute(
        "UPDATE photos SET companion_path = 'companion.jpg' WHERE id = ?",
        (pid,),
    )
    folder_path = str(tmp_path / "disk_photos")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (folder_path, photos[0]["folder_id"]),
    )
    db.conn.commit()

    os.makedirs(folder_path, exist_ok=True)
    primary_file = os.path.join(folder_path, photos[0]["filename"])
    companion_file = os.path.join(folder_path, "companion.jpg")
    Image.new("RGB", (10, 10)).save(primary_file)
    Image.new("RGB", (10, 10)).save(companion_file)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "disk",
        "include_companions": False,
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["trashed"] == 1
    assert not os.path.exists(primary_file)
    assert os.path.exists(companion_file)


def test_api_batch_delete_disk_permanent_with_photo_ids(app_and_db, tmp_path):
    """disk_permanent mode with photo_ids permanently deletes files."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]["id"]

    folder_path = str(tmp_path / "disk_photos")
    db.conn.execute(
        "UPDATE folders SET path = ? WHERE id = ?",
        (folder_path, photos[0]["folder_id"]),
    )
    db.conn.commit()

    os.makedirs(folder_path, exist_ok=True)
    real_file = os.path.join(folder_path, photos[0]["filename"])
    Image.new("RGB", (10, 10)).save(real_file)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "disk_permanent",
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["deleted"] == 1
    assert data["trashed"] == 1
    assert not os.path.exists(real_file)
    assert db.get_photo(pid) is None
