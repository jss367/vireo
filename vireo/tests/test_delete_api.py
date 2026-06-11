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


def test_api_batch_delete_purges_sized_preview_variants(app_and_db):
    """Delete removes every <id>_<size>.jpg preview variant, not just <id>.jpg.

    Without this, SQLite id reuse could cause a newly inserted photo to be
    served stale bytes from a previous photo's cached preview.
    """
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)

    # Seed cache with both legacy <id>.jpg and sized variants
    legacy = os.path.join(preview_dir, f"{pid}.jpg")
    v1920 = os.path.join(preview_dir, f"{pid}_1920.jpg")
    v2560 = os.path.join(preview_dir, f"{pid}_2560.jpg")
    for p in (legacy, v1920, v2560):
        Image.new("RGB", (10, 10)).save(p, "JPEG")

    resp = client.post("/api/batch/delete", json={
        "photo_ids": [pid],
        "mode": "vireo",
    })
    assert resp.status_code == 200

    assert not os.path.exists(legacy)
    assert not os.path.exists(v1920)
    assert not os.path.exists(v2560)


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


def test_api_batch_delete_chunks_large_photo_id_lists(app_and_db, monkeypatch):
    """Route must chunk photo_ids before calling delete_photos so SQLite's
    bound-parameter cap (~999 on legacy builds) can't trip on bulk deletes."""
    import app as appmod
    from db import Database

    app, db = app_and_db
    client = app.test_client()

    fid = db.add_folder('/photos/bulk', name='bulk')
    bulk_ids = [
        db.add_photo(
            folder_id=fid,
            filename=f"bulk{i}.jpg",
            extension='.jpg',
            file_size=10,
            file_mtime=float(i),
        )
        for i in range(5)
    ]

    def small_chunked(seq, size=2):
        for i in range(0, len(seq), size):
            yield seq[i:i + size]
    monkeypatch.setattr(appmod, "_chunked", small_chunked)

    chunks_seen = []
    real_delete = Database.delete_photos

    def spy(self, photo_ids, **kwargs):
        chunks_seen.append(list(photo_ids))
        return real_delete(self, photo_ids, **kwargs)

    monkeypatch.setattr(Database, "delete_photos", spy)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": bulk_ids,
        "mode": "vireo",
    })

    assert resp.status_code == 200
    assert resp.get_json()["deleted"] == len(bulk_ids)
    assert len(chunks_seen) >= 3, f"expected chunked calls, got {chunks_seen}"
    assert all(len(c) <= 2 for c in chunks_seen)
    for pid in bulk_ids:
        assert db.get_photo(pid) is None


def test_api_batch_delete_chunked_loop_is_atomic_on_failure(app_and_db, monkeypatch):
    """A failure in a later chunk must roll back earlier chunks so DB rows
    and cached files don't drift apart. Without a shared transaction the
    earlier chunks' commits would survive, leaving the route 500ing after
    deleting only part of the selection."""
    import app as appmod
    from db import Database

    app, db = app_and_db
    client = app.test_client()

    fid = db.add_folder('/photos/atomic', name='atomic')
    bulk_ids = [
        db.add_photo(
            folder_id=fid,
            filename=f"atomic{i}.jpg",
            extension='.jpg',
            file_size=10,
            file_mtime=float(i),
        )
        for i in range(5)
    ]

    def small_chunked(seq, size=2):
        for i in range(0, len(seq), size):
            yield seq[i:i + size]
    monkeypatch.setattr(appmod, "_chunked", small_chunked)

    real_delete = Database.delete_photos
    calls = {"n": 0}

    def flaky(self, photo_ids, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("simulated SQLite error mid-loop")
        return real_delete(self, photo_ids, **kwargs)

    monkeypatch.setattr(Database, "delete_photos", flaky)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": bulk_ids,
        "mode": "vireo",
    })

    # Route should fail visibly rather than silently dropping rows.
    assert resp.status_code >= 500
    # No DB rows should be missing — the first chunk's DML must roll back
    # along with the failed chunk's, restoring the all-or-nothing semantics
    # the single-call path had.
    for pid in bulk_ids:
        assert db.get_photo(pid) is not None, f"photo {pid} was deleted despite rollback"


def test_api_batch_delete_chunked_failure_preserves_pipeline_cache(
    app_and_db, monkeypatch
):
    """When a later chunk fails and the route rolls the DB back, the
    pipeline review cache must NOT have already been pruned for the
    earlier chunks' photos — those rows still exist, so pruning them
    would orphan their pipeline entries with no way to restore them
    (the cache file is non-transactional)."""
    import app as appmod
    from db import Database

    app, db = app_and_db
    client = app.test_client()

    fid = db.add_folder('/photos/pcache', name='pcache')
    bulk_ids = [
        db.add_photo(
            folder_id=fid,
            filename=f"pcache{i}.jpg",
            extension='.jpg',
            file_size=10,
            file_mtime=float(i),
        )
        for i in range(5)
    ]

    # Seed a pipeline review cache that references every photo.
    cache_dir = os.path.dirname(db._db_path)
    cache_path = os.path.join(
        cache_dir, f"pipeline_results_ws{db._active_workspace_id}.json"
    )
    cache = {
        "encounters": [{
            "species": None,
            "photo_count": len(bulk_ids),
            "burst_count": 1,
            "photo_ids": list(bulk_ids),
            "bursts": [{
                "photo_ids": list(bulk_ids),
                "species_predictions": [],
                "species_override": None,
            }],
        }],
        "photos": [{"id": pid, "label": "KEEP"} for pid in bulk_ids],
        "summary": {
            "total_photos": len(bulk_ids),
            "encounter_count": 1,
            "burst_count": 1,
            "keep_count": len(bulk_ids),
            "review_count": 0,
            "reject_count": 0,
            "rarity_protected": 0,
        },
    }
    with open(cache_path, "w") as f:
        json.dump(cache, f)

    def small_chunked(seq, size=2):
        for i in range(0, len(seq), size):
            yield seq[i:i + size]
    monkeypatch.setattr(appmod, "_chunked", small_chunked)

    real_delete = Database.delete_photos
    calls = {"n": 0}

    def flaky(self, photo_ids, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("simulated SQLite error mid-loop")
        return real_delete(self, photo_ids, **kwargs)

    monkeypatch.setattr(Database, "delete_photos", flaky)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": bulk_ids,
        "mode": "vireo",
    })

    assert resp.status_code >= 500

    # The on-disk pipeline cache must still reference every original
    # photo — none were actually deleted (the DB rolled back), so the
    # cache must not have been pre-pruned for the first chunk.
    with open(cache_path) as f:
        cache_after = json.load(f)
    assert [p["id"] for p in cache_after["photos"]] == list(bulk_ids)
    assert cache_after["encounters"][0]["photo_ids"] == list(bulk_ids)
    assert cache_after["encounters"][0]["bursts"][0]["photo_ids"] == list(bulk_ids)
    assert cache_after["summary"]["total_photos"] == len(bulk_ids)


def test_api_batch_delete_chunked_success_prunes_pipeline_cache(
    app_and_db, monkeypatch
):
    """On the happy path (all chunks commit), the pipeline cache must
    still be pruned of the deleted photos exactly once after the outer
    commit — i.e., deferring the prune doesn't drop it on the floor."""
    import app as appmod

    app, db = app_and_db
    client = app.test_client()

    fid = db.add_folder('/photos/pchunk', name='pchunk')
    bulk_ids = [
        db.add_photo(
            folder_id=fid,
            filename=f"pchunk{i}.jpg",
            extension='.jpg',
            file_size=10,
            file_mtime=float(i),
        )
        for i in range(5)
    ]
    survivor = db.add_photo(
        folder_id=fid,
        filename="survivor.jpg",
        extension='.jpg',
        file_size=10,
        file_mtime=99.0,
    )

    cache_dir = os.path.dirname(db._db_path)
    cache_path = os.path.join(
        cache_dir, f"pipeline_results_ws{db._active_workspace_id}.json"
    )
    all_in_cache = list(bulk_ids) + [survivor]
    cache = {
        "encounters": [{
            "species": None,
            "photo_count": len(all_in_cache),
            "burst_count": 1,
            "photo_ids": list(all_in_cache),
            "bursts": [{
                "photo_ids": list(all_in_cache),
                "species_predictions": [],
                "species_override": None,
            }],
        }],
        "photos": [{"id": pid, "label": "KEEP"} for pid in all_in_cache],
        "summary": {
            "total_photos": len(all_in_cache),
            "encounter_count": 1,
            "burst_count": 1,
            "keep_count": len(all_in_cache),
            "review_count": 0,
            "reject_count": 0,
            "rarity_protected": 0,
        },
    }
    with open(cache_path, "w") as f:
        json.dump(cache, f)

    def small_chunked(seq, size=2):
        for i in range(0, len(seq), size):
            yield seq[i:i + size]
    monkeypatch.setattr(appmod, "_chunked", small_chunked)

    resp = client.post("/api/batch/delete", json={
        "photo_ids": bulk_ids,
        "mode": "vireo",
    })
    assert resp.status_code == 200

    with open(cache_path) as f:
        cache_after = json.load(f)
    assert [p["id"] for p in cache_after["photos"]] == [survivor]
    assert cache_after["encounters"][0]["photo_ids"] == [survivor]
    assert cache_after["summary"]["total_photos"] == 1


def test_api_batch_delete_disk_permanent_retry_with_paths(app_and_db, tmp_path):
    """disk_permanent retry works with paths after DB rows are already gone.

    The photo rows are deleted by the initial call, but their folder rows
    survive — the retry validates paths against those.
    """
    app, db = app_and_db
    client = app.test_client()

    # Create files to delete, inside a Vireo-managed folder
    db.add_folder(str(tmp_path), name=tmp_path.name)
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


def test_api_batch_delete_retry_refuses_paths_outside_vireo_folders(app_and_db, tmp_path):
    """The disk_permanent retry must not delete arbitrary client-supplied
    paths — only files directly inside a known Vireo folder."""
    app, db = app_and_db
    client = app.test_client()

    outside = str(tmp_path / "secrets.txt")
    with open(outside, "w") as f:
        f.write("not a vireo photo")

    resp = client.post("/api/batch/delete", json={
        "mode": "disk_permanent",
        "paths": [outside],
    })

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["trashed"] == 0
    assert os.path.exists(outside)  # file untouched
    assert any(
        t["path"] == outside and "not in a Vireo folder" in t.get("error", "")
        for t in data["trash_failed"]
    )


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


def test_delete_photos_prunes_pipeline_cache(app_and_db):
    """Deleting photos strips them from the pipeline review cache so
    they don't render as blank cards on the pipeline review page."""
    app, db = app_and_db
    photos = db.get_photos()
    pid_to_delete = photos[0]["id"]
    surviving_ids = [p["id"] for p in photos[1:]]

    cache_dir = os.path.dirname(db._db_path)
    cache_path = os.path.join(
        cache_dir, f"pipeline_results_ws{db._active_workspace_id}.json"
    )
    cache = {
        "encounters": [{
            "species": None,
            "photo_count": len(photos),
            "burst_count": 1,
            "photo_ids": [p["id"] for p in photos],
            "bursts": [{
                "photo_ids": [p["id"] for p in photos],
                "species_predictions": [],
                "species_override": None,
            }],
        }],
        "photos": [{"id": p["id"], "label": "KEEP"} for p in photos],
        "summary": {
            "total_photos": len(photos),
            "encounter_count": 1,
            "burst_count": 1,
            "keep_count": len(photos),
            "review_count": 0,
            "reject_count": 0,
            "rarity_protected": 0,
        },
    }
    with open(cache_path, "w") as f:
        json.dump(cache, f)

    db.delete_photos([pid_to_delete])

    with open(cache_path) as f:
        pruned = json.load(f)
    assert [p["id"] for p in pruned["photos"]] == surviving_ids
    assert pruned["encounters"][0]["photo_ids"] == surviving_ids
    assert pruned["encounters"][0]["bursts"][0]["photo_ids"] == surviving_ids
    assert pruned["encounters"][0]["photo_count"] == len(surviving_ids)
    assert pruned["summary"]["total_photos"] == len(surviving_ids)
    assert pruned["summary"]["keep_count"] == len(surviving_ids)


def test_delete_photos_no_pipeline_cache_does_not_raise(app_and_db):
    """delete_photos succeeds even when no pipeline cache file exists."""
    app, db = app_and_db
    photos = db.get_photos()

    cache_dir = os.path.dirname(db._db_path)
    cache_path = os.path.join(
        cache_dir, f"pipeline_results_ws{db._active_workspace_id}.json"
    )
    assert not os.path.exists(cache_path)

    result = db.delete_photos([photos[0]["id"]])
    assert result["deleted"] == 1


def test_delete_photos_with_companions_chunks_expanded_ids(tmp_path):
    """``include_companions=True`` can double the id count inside
    ``delete_photos`` (each input id may pull in its companion), so the
    internal ``IN (?, ?, …)`` DELETEs must chunk on the expanded list,
    not on the caller's input. The api/batch/delete endpoint pre-chunks
    by 900 (under SQLite's legacy 999 ``SQLITE_LIMIT_VARIABLE_NUMBER``),
    but after companion expansion the all_ids list can reach ~1800 —
    which then trips "too many SQL variables" after the file-trash
    step already ran.

    The host's actual cap is build-dependent (999 on old, 250000+ on
    new), so we lower the cap via ``setlimit`` to the legacy value. The
    input chunk (900) stays under it, but the expanded all_ids (1800)
    would exceed it without internal chunking.
    """
    import sqlite3
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder(str(tmp_path / "lib"), name="lib")

    # 900 primaries + 900 companions = 1800 expanded ids.
    primary_ids = []
    companion_filenames = []
    for i in range(900):
        comp_name = f"img_{i:04d}.jpg.xmp"
        companion_filenames.append(comp_name)
        pid = db.add_photo(
            folder_id=fid, filename=f"img_{i:04d}.jpg", extension=".jpg",
            file_size=100, file_mtime=1.0,
        )
        db.add_photo(
            folder_id=fid, filename=comp_name, extension=".xmp",
            file_size=10, file_mtime=1.0,
        )
        primary_ids.append(pid)

    # Link primary → companion so include_companions resolves the sidecar.
    db.conn.executemany(
        "UPDATE photos SET companion_path = ? WHERE id = ?",
        list(zip(companion_filenames, primary_ids)),
    )
    db.conn.commit()

    # Legacy SQLite cap — 900 input fits, 1800 expanded does not.
    db.conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)

    result = db.delete_photos(primary_ids, include_companions=True)

    assert result["deleted"] == 1800  # primaries + companions
    remaining = db.conn.execute("SELECT COUNT(*) AS n FROM photos").fetchone()["n"]
    assert remaining == 0
