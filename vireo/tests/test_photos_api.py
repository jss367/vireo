def test_api_photos_default(app_and_db):
    """GET /api/photos returns all photos."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos')
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data['photos']) == 3
    assert 'total' in data


def test_api_photos_pagination(app_and_db):
    """GET /api/photos supports pagination."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos?per_page=2&page=1')
    data = resp.get_json()
    assert len(data['photos']) == 2

    resp = client.get('/api/photos?per_page=2&page=2')
    data = resp.get_json()
    assert len(data['photos']) == 1


def test_api_photos_filter_folder(app_and_db):
    """GET /api/photos?folder_id= filters by folder."""
    app, db = app_and_db
    folders = db.get_folder_tree()
    jan = [f for f in folders if f['name'] == 'January'][0]

    client = app.test_client()
    resp = client.get(f'/api/photos?folder_id={jan["id"]}')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['filename'] == 'bird2.jpg'


def test_api_photos_filter_rating(app_and_db):
    """GET /api/photos?rating_min= filters by minimum rating."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos?rating_min=4')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['filename'] == 'bird3.jpg'


def test_api_photos_filter_date_range(app_and_db):
    """GET /api/photos?date_from=&date_to= filters by date range."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos?date_from=2024-01-01&date_to=2024-02-01')
    data = resp.get_json()
    assert len(data['photos']) == 2


def test_api_photos_filter_single_day(app_and_db):
    """GET /api/photos with date_to including time captures all photos on that day."""
    app, _ = app_and_db
    client = app.test_client()
    # Without time suffix, a bare date like 2024-01-15 would miss timestamps
    # like 2024-01-15T10:00:00 because string comparison puts it after the date
    resp = client.get('/api/photos?date_from=2024-01-15&date_to=2024-01-15T23:59:59')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['filename'] == 'bird1.jpg'


def test_api_photos_filter_keyword(app_and_db):
    """GET /api/photos?keyword= filters by keyword."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos?keyword=Cardinal')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['filename'] == 'bird1.jpg'


def test_api_photo_detail(app_and_db):
    """GET /api/photos/<id> returns photo with keywords."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.get(f'/api/photos/{pid}')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['filename'] == 'bird1.jpg'
    assert 'keywords' in data


def test_api_photos_calendar(app_and_db):
    """GET /api/photos/calendar returns daily photo counts for a year."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/photos/calendar?year=2024")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["year"] == 2024
    assert "2024-01-15" in data["days"]
    assert data["min_year"] == 2024
    assert data["max_year"] == 2024


def test_api_photos_calendar_with_filters(app_and_db):
    """GET /api/photos/calendar respects folder_id and rating_min filters."""
    app, db = app_and_db
    client = app.test_client()
    resp = client.get("/api/photos/calendar?year=2024&rating_min=4")
    data = resp.get_json()
    assert list(data["days"].keys()) == ["2024-06-10"]


def test_api_photos_calendar_default_year(app_and_db):
    """GET /api/photos/calendar defaults to current year."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/photos/calendar")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "year" in data
    assert "days" in data


def test_thumbnail_serving(app_and_db):
    """GET /thumbnails/<id>.jpg serves thumbnail from cache."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.get(f'/thumbnails/{pid}.jpg')
    assert resp.status_code == 200
    assert resp.content_type in ('image/jpeg', 'image/jpg')


def test_api_photos_geo_returns_geolocated(app_and_db):
    """GET /api/photos/geo returns only geolocated photos."""
    app, db = app_and_db
    # Set GPS on bird1 only
    db.conn.execute("UPDATE photos SET latitude=37.77, longitude=-122.42 WHERE filename='bird1.jpg'")
    db.conn.commit()

    client = app.test_client()
    resp = client.get('/api/photos/geo')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'photos' in data
    assert 'total_filtered' in data
    assert 'total_with_gps' in data
    assert 'total_photos' in data
    assert data['total_filtered'] == 1
    assert data['total_with_gps'] == 1
    assert data['total_photos'] == 3
    assert len(data['photos']) == 1
    assert data['photos'][0]['latitude'] == 37.77
    assert data['photos'][0]['longitude'] == -122.42


def test_api_photos_geo_with_filters(app_and_db):
    """GET /api/photos/geo passes through rating filter."""
    app, db = app_and_db
    # Set GPS on bird1 (rating 3) and bird3 (rating 5)
    db.conn.execute("UPDATE photos SET latitude=1.0, longitude=2.0 WHERE filename IN ('bird1.jpg','bird3.jpg')")
    db.conn.commit()

    client = app.test_client()
    resp = client.get('/api/photos/geo?rating_min=4')
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['filename'] == 'bird3.jpg'


def test_api_photos_geo_empty(app_and_db):
    """GET /api/photos/geo returns empty list when no geolocated photos."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos/geo')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['photos'] == []
    assert data['total_filtered'] == 0


def test_api_photos_geo_includes_gps_stats(app_and_db):
    """GET /api/photos/geo response includes consistent global GPS stats."""
    app, db = app_and_db
    db.conn.execute("UPDATE photos SET latitude=37.77, longitude=-122.42 WHERE filename='bird1.jpg'")
    db.conn.commit()

    client = app.test_client()
    resp = client.get('/api/photos/geo')
    data = resp.get_json()
    assert data['total_with_gps'] == 1
    assert data['total_without_gps'] == 2
    assert data['total_photos'] == 3
    # Verify global stats stay consistent even with filters active
    resp2 = client.get('/api/photos/geo?rating_min=5')
    data2 = resp2.get_json()
    assert data2['total_filtered'] == 0  # no rated-5 geo photos
    assert data2['total_with_gps'] == 1  # global count unchanged
    assert data2['total_without_gps'] == 2  # global count unchanged


def test_api_photos_geo_species_filter(app_and_db):
    """GET /api/photos/geo?species= filters by species."""
    app, db = app_and_db
    db.conn.execute("UPDATE photos SET latitude=1.0, longitude=2.0 WHERE filename IN ('bird1.jpg','bird3.jpg')")
    db.conn.commit()
    # Add detections then predictions
    det1 = db.save_detections(1, [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}], detector_model="MDV6")
    det3 = db.save_detections(3, [{"box": {"x": 0.2, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.8, "category": "animal"}], detector_model="MDV6")
    db.add_prediction(det1[0], 'Cardinal', 0.9, 'bioclip')
    db.add_prediction(det3[0], 'Sparrow', 0.8, 'bioclip')
    preds = db.get_predictions(photo_ids=[1, 3])
    for pr in preds:
        db.accept_prediction(pr['id'])

    client = app.test_client()
    resp = client.get('/api/photos/geo?species=Cardinal')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['species'] == 'Cardinal'


def test_api_species_list(app_and_db):
    """GET /api/species returns accepted species from geolocated photos."""
    app, db = app_and_db
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id=1")
    det_ids = db.save_detections(1, [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Cardinal', 0.9, 'bioclip')
    preds = db.get_predictions(photo_ids=[1])
    db.accept_prediction(preds[0]['id'])

    client = app.test_client()
    resp = client.get('/api/species')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'species' in data
    assert 'Cardinal' in data['species']


def test_api_species_empty(app_and_db):
    """GET /api/species returns empty list with no accepted predictions."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/api/species')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['species'] == []


def test_photo_detail_includes_metadata(app_and_db):
    """GET /api/photos/<id> includes parsed metadata when exif_data is populated."""
    app, db = app_and_db
    import json

    # Manually set exif_data on a photo
    test_meta = {"EXIF": {"Make": "TestCam", "Model": "X100"}, "File": {"FileType": "JPEG"}}
    db.conn.execute(
        "UPDATE photos SET exif_data = ? WHERE id = 1",
        (json.dumps(test_meta),),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.get('/api/photos/1')
    assert resp.status_code == 200
    data = resp.get_json()
    assert "metadata" in data
    assert data["metadata"]["EXIF"]["Make"] == "TestCam"


def test_photo_detail_metadata_null_when_empty(app_and_db):
    """GET /api/photos/<id> returns metadata as null when exif_data is not populated."""
    app, db = app_and_db
    client = app.test_client()
    resp = client.get('/api/photos/1')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data.get("metadata") is None


def test_add_keyword_with_type_override(app_and_db):
    """POST /api/photos/<id>/keywords with type param sets keyword type in DB."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/keywords',
                       json={"name": "Tim", "type": "people"})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    kid = data["keyword_id"]

    # Verify the keyword type is "people" in the database
    row = db.conn.execute("SELECT type FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row is not None
    assert row["type"] == "people"


def test_batch_keyword_with_type_override(app_and_db):
    """POST /api/batch/keyword with type param sets keyword type in DB."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    photo_ids = [p['id'] for p in photos]

    resp = client.post('/api/batch/keyword',
                       json={"photo_ids": photo_ids, "name": "Central Park", "type": "location"})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["updated"] == len(photo_ids)

    # Verify the keyword type is "location" in the database
    row = db.conn.execute(
        "SELECT type FROM keywords WHERE name = 'Central Park'").fetchone()
    assert row is not None
    assert row["type"] == "location"


# --- Working copy integration tests for serving endpoints ---

import os


def test_preview_uses_working_copy(app_and_db):
    """Preview endpoint loads from working copy instead of original."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Set a working copy path on the photo
    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    Image.new("RGB", (4096, 2731), color=(0, 255, 0)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    # Clear preview cache
    preview_dir = os.path.join(vireo_dir, "previews")
    cache_file = os.path.join(preview_dir, f"{pid}.jpg")
    if os.path.exists(cache_file):
        os.remove(cache_file)

    resp = client.get(f"/photos/{pid}/full")
    assert resp.status_code == 200


def test_preview_falls_back_to_original(app_and_db, tmp_path):
    """Preview endpoint falls back to original when no working copy exists."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Point folder to a writable tmp location and create a real image
    from PIL import Image
    photo_dir = tmp_path / "photo_files"
    photo_dir.mkdir()
    db.conn.execute(
        "UPDATE folders SET path=? WHERE id=?",
        (str(photo_dir), photos[0]["folder_id"]),
    )
    db.conn.commit()
    img_path = os.path.join(str(photo_dir), photos[0]["filename"])
    Image.new("RGB", (2000, 1500)).save(img_path, "JPEG")

    # Clear preview cache
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    cache_file = os.path.join(preview_dir, f"{pid}.jpg")
    if os.path.exists(cache_file):
        os.remove(cache_file)

    resp = client.get(f"/photos/{pid}/full")
    assert resp.status_code == 200


def test_preview_sized_caches_per_size(app_and_db):
    """Preview endpoint caches each requested size separately."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    Image.new("RGB", (4096, 2731), color=(0, 255, 0)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    resp = client.get(f"/photos/{pid}/preview?size=1920")
    assert resp.status_code == 200
    resp = client.get(f"/photos/{pid}/preview?size=2560")
    assert resp.status_code == 200

    preview_dir = os.path.join(vireo_dir, "previews")
    assert os.path.exists(os.path.join(preview_dir, f"{pid}_1920.jpg"))
    assert os.path.exists(os.path.join(preview_dir, f"{pid}_2560.jpg"))

    # The 2560 variant should actually be larger on disk than the 1920 variant
    size_1920 = os.path.getsize(os.path.join(preview_dir, f"{pid}_1920.jpg"))
    size_2560 = os.path.getsize(os.path.join(preview_dir, f"{pid}_2560.jpg"))
    assert size_2560 > size_1920


def test_preview_returns_404_for_deleted_photo_even_with_stale_cache(app_and_db):
    """Defense against SQLite id reuse: don't serve a cached image for a row
    that no longer exists.
    """
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]

    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)

    # Delete the photo (cascades FK-dependent rows) then simulate a leftover
    # cache file, e.g. crash-after-commit-before-cleanup.
    db.delete_photos([pid])
    stale = os.path.join(preview_dir, f"{pid}_1920.jpg")
    Image.new("RGB", (10, 10)).save(stale, "JPEG")

    resp = client.get(f"/photos/{pid}/preview?size=1920")
    assert resp.status_code == 404


def test_preview_rejects_unsupported_size(app_and_db):
    """Preview endpoint rejects sizes outside the allowlist to prevent cache-bombing."""
    app, db = app_and_db
    client = app.test_client()

    pid = db.get_photos()[0]["id"]
    resp = client.get(f"/photos/{pid}/preview?size=9999")
    assert resp.status_code == 400
    resp = client.get(f"/photos/{pid}/preview?size=abc")
    assert resp.status_code == 400


def test_original_serves_full_res_working_copy(app_and_db):
    """Original endpoint serves working copy directly when it is full-res."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Set photo dimensions to match working copy
    db.conn.execute("UPDATE photos SET width=800, height=600 WHERE id=?", (pid,))
    db.conn.commit()

    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    # Working copy is 800x600 which matches original dimensions
    Image.new("RGB", (800, 600)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    resp = client.get(f"/photos/{pid}/original")
    assert resp.status_code == 200


def test_original_endpoint_upgrades_working_copy_to_full_res(app_and_db, tmp_path):
    """Original endpoint extracts full-res when working copy is capped."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Set dimensions larger than working copy
    db.conn.execute("UPDATE photos SET width=6000, height=4000 WHERE id=?", (pid,))
    db.conn.commit()

    # Create a capped working copy (smaller than original dimensions)
    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    Image.new("RGB", (4096, 2731)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    # Point folder to a writable tmp location and create a real source image
    photo_dir = tmp_path / "photo_files"
    photo_dir.mkdir()
    db.conn.execute(
        "UPDATE folders SET path=? WHERE id=?",
        (str(photo_dir), photos[0]["folder_id"]),
    )
    db.conn.commit()
    img_path = os.path.join(str(photo_dir), photos[0]["filename"])
    Image.new("RGB", (6000, 4000)).save(img_path, "JPEG")

    resp = client.get(f"/photos/{pid}/original")
    assert resp.status_code == 200


def test_original_serves_native_jpeg_directly(app_and_db, tmp_path):
    """Original endpoint serves JPEG file directly when no working copy."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Point folder to a writable tmp location and create a real JPEG
    from PIL import Image
    photo_dir = tmp_path / "photo_files"
    photo_dir.mkdir()
    db.conn.execute(
        "UPDATE folders SET path=? WHERE id=?",
        (str(photo_dir), photos[0]["folder_id"]),
    )
    db.conn.commit()
    img_path = os.path.join(str(photo_dir), photos[0]["filename"])
    Image.new("RGB", (3000, 2000)).save(img_path, "JPEG")

    # Ensure no working copy is set
    db.conn.execute(
        "UPDATE photos SET working_copy_path=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    resp = client.get(f"/photos/{pid}/original")
    assert resp.status_code == 200


def test_crop_preview_uses_working_copy(app_and_db):
    """Crop preview endpoint loads from working copy when available."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Create a working copy
    from PIL import Image
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    working_dir = os.path.join(vireo_dir, "working")
    os.makedirs(working_dir, exist_ok=True)
    wc_path = os.path.join(working_dir, f"{pid}.jpg")
    Image.new("RGB", (4096, 2731), color=(255, 0, 0)).save(wc_path, "JPEG")
    db.conn.execute(
        "UPDATE photos SET working_copy_path=? WHERE id=?",
        (f"working/{pid}.jpg", pid),
    )
    db.conn.commit()

    resp = client.get(f"/photos/{pid}/crop")
    assert resp.status_code == 200
    assert resp.content_type == "image/jpeg"


def test_crop_preview_falls_back_to_original(app_and_db, tmp_path):
    """Crop preview endpoint falls back to original when no working copy."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.get_photos()
    pid = photos[0]["id"]

    # Point folder to a writable tmp location and create a real image
    from PIL import Image
    photo_dir = tmp_path / "photo_files"
    photo_dir.mkdir()
    db.conn.execute(
        "UPDATE folders SET path=? WHERE id=?",
        (str(photo_dir), photos[0]["folder_id"]),
    )
    db.conn.commit()
    img_path = os.path.join(str(photo_dir), photos[0]["filename"])
    Image.new("RGB", (2000, 1500)).save(img_path, "JPEG")

    # Ensure no working copy
    db.conn.execute(
        "UPDATE photos SET working_copy_path=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    resp = client.get(f"/photos/{pid}/crop")
    assert resp.status_code == 200
    assert resp.content_type == "image/jpeg"


# ---- Preview cache (LRU) tests ----


def test_preview_cache_miss_creates_row(client_with_photo):
    """First request to a size inserts a preview_cache row."""
    app, db, photo_id = client_with_photo
    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert resp.status_code == 200
    row = db.preview_cache_get(photo_id, 1920)
    assert row is not None
    assert row["bytes"] > 0


def test_preview_cache_hit_updates_last_access(client_with_photo):
    """Second request touches last_access_at."""
    import time
    app, db, photo_id = client_with_photo
    client = app.test_client()
    client.get(f"/photos/{photo_id}/preview?size=1920")
    row1 = db.preview_cache_get(photo_id, 1920)
    time.sleep(0.05)
    client.get(f"/photos/{photo_id}/preview?size=1920")
    row2 = db.preview_cache_get(photo_id, 1920)
    assert row2["last_access_at"] > row1["last_access_at"]


def test_preview_adopts_existing_file_on_first_access(client_with_photo):
    """A cached file left over from the old scheme is adopted into the LRU."""
    import os
    import time
    app, db, photo_id = client_with_photo
    # Create a cache file manually without a DB row
    preview_dir = os.path.join(
        os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews"
    )
    os.makedirs(preview_dir, exist_ok=True)
    cache_path = os.path.join(preview_dir, f"{photo_id}_1920.jpg")
    with open(cache_path, "wb") as f:
        f.write(b"x" * 12345)
    # Backdate mtime
    past = time.time() - 3600
    os.utime(cache_path, (past, past))
    assert db.preview_cache_get(photo_id, 1920) is None

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert resp.status_code == 200
    row = db.preview_cache_get(photo_id, 1920)
    assert row is not None
    assert row["bytes"] == 12345
