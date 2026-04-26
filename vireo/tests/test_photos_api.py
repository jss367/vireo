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


def test_api_photos_includes_all_detections(app_and_db):
    """GET /api/photos attaches a `detections` list with every box, not just primary."""
    app, db = app_and_db
    photos = db.get_photos()
    target = [p for p in photos if p['filename'] == 'bird1.jpg'][0]
    db.save_detections(target['id'], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.7, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.2}, "confidence": 0.95, "category": "animal"},
    ], detector_model="MDV6")

    client = app.test_client()
    resp = client.get('/api/photos')
    data = resp.get_json()

    bird1 = [p for p in data['photos'] if p['filename'] == 'bird1.jpg'][0]
    assert 'detections' in bird1
    assert len(bird1['detections']) == 2
    assert bird1['detections'][0]['confidence'] == 0.95
    assert bird1['detections'][0]['x'] == 0.5
    assert bird1['detections'][1]['confidence'] == 0.7
    assert bird1['detections'][0]['category'] == 'animal'

    # Photos without detections get an empty list, not a missing key
    bird3 = [p for p in data['photos'] if p['filename'] == 'bird3.jpg'][0]
    assert bird3['detections'] == []


def test_api_photos_detections_honor_workspace_threshold(app_and_db):
    """Lowering the workspace's `detector_confidence` surfaces more boxes at
    read time, without rewriting any detection rows.

    Exercises the global-detections design: boxes are cached once, each
    workspace filters on its own threshold when reading.
    """
    app, db = app_and_db
    photos = db.get_photos()
    target = [p for p in photos if p['filename'] == 'bird1.jpg'][0]

    # Save two boxes: one above the default 0.2 threshold, one below it.
    db.save_detections(target['id'], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2}, "confidence": 0.05, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.2}, "confidence": 0.95, "category": "animal"},
    ], detector_model="MDV6")

    client = app.test_client()

    # Default workspace threshold (0.2) hides the low-confidence box.
    resp = client.get('/api/photos')
    bird1 = [p for p in resp.get_json()['photos'] if p['filename'] == 'bird1.jpg'][0]
    assert len(bird1['detections']) == 1
    assert bird1['detections'][0]['confidence'] == 0.95

    # Lower the workspace threshold via a per-workspace config override —
    # no detection rows are rewritten, only the read-time filter changes.
    db.update_workspace(db._active_workspace_id,
                        config_overrides={"detector_confidence": 0.01})

    resp = client.get('/api/photos')
    bird1 = [p for p in resp.get_json()['photos'] if p['filename'] == 'bird1.jpg'][0]
    assert len(bird1['detections']) == 2, (
        "lowering detector_confidence should surface more cached boxes"
    )
    # Still ordered by confidence DESC.
    assert bird1['detections'][0]['confidence'] == 0.95
    assert bird1['detections'][1]['confidence'] == 0.05

    # And no new rows were written.
    raw = db.conn.execute(
        "SELECT COUNT(*) FROM detections WHERE photo_id = ?", (target['id'],)
    ).fetchone()[0]
    assert raw == 2


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


def test_api_photo_detail_includes_on_disk_path(app_and_db):
    """GET /api/photos/<id> returns a `path` field equal to folder_path + '/' + filename.

    The browse-grid right-click "Copy Path" action depends on this field being
    present in the photo detail response. PHOTO_DETAIL_COLS intentionally does
    not store the full on-disk path in the photos table, so the route handler
    must compute it by joining the owning folder's path with the photo's
    filename (same idiom as /api/files/reveal).
    """
    import os as _os

    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    target = [p for p in photos if p['filename'] == 'bird1.jpg'][0]
    folder_row = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (target['folder_id'],)
    ).fetchone()
    expected_path = _os.path.join(folder_row['path'], target['filename'])

    resp = client.get(f"/api/photos/{target['id']}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'path' in data, "photo detail should expose full on-disk path"
    assert data['path'] == expected_path


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


def test_api_species_summary_filters_to_latest_fingerprint(app_and_db):
    """GET /api/species/summary must surface only the most recent
    labels_fingerprint per (detection, classifier_model). Stale species
    cached under an old label set must NOT contribute to counts —
    otherwise the variant explorer mixes pre- and post-relabel results.
    """
    app, db = app_and_db
    det_ids = db.save_detections(1, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"},
    ], detector_model="MDV6")
    # Stale fingerprint: species the user used to track.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-old', 'Finch', 0.9, '2026-01-01')",
        (det_ids[0],),
    )
    # Current fingerprint: species under the active label set.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Robin', 0.8, '2026-04-24')",
        (det_ids[0],),
    )
    db.conn.commit()

    client = app.test_client()
    resp = client.get('/api/species/summary')
    assert resp.status_code == 200
    species = [r['species'] for r in resp.get_json()]
    assert 'Robin' in species
    assert 'Finch' not in species, (
        "Species summary leaked a stale-fingerprint species into counts "
        "— variant explorer would mix pre- and post-relabel results."
    )


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


def test_full_is_alias_for_preview_at_configured_size(client_with_photo, monkeypatch):
    """/full returns the same bytes as /preview?size=<preview_max_size>."""
    import config as cfg
    # Pin preview_max_size to 1920 for determinism.
    monkeypatch.setattr(
        cfg, "get",
        lambda k: 1920 if k == "preview_max_size" else cfg.DEFAULTS.get(k),
    )
    app, db, photo_id = client_with_photo
    client = app.test_client()
    full = client.get(f"/photos/{photo_id}/full").data
    preview = client.get(f"/photos/{photo_id}/preview?size=1920").data
    assert full == preview
    assert len(full) > 0


def test_eviction_removes_oldest_files_when_over_quota(tmp_path, monkeypatch):
    """When writes push cache over quota, oldest-accessed entries are evicted."""
    import os
    import time

    import config as cfg
    from app import create_app
    from db import Database
    from PIL import Image

    # Custom fixture with TWO photos because we need to race two writes.
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    # Quota of 0 MB → eviction should clear the cache after each write.
    cfg.save({**cfg.DEFAULTS, "preview_cache_max_mb": 0})

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src1 = photos_dir / "a.jpg"
    src2 = photos_dir / "b.jpg"
    Image.new("RGB", (800, 600), (180, 90, 40)).save(str(src1), "JPEG")
    Image.new("RGB", (800, 600), (40, 180, 90)).save(str(src2), "JPEG")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid1 = db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=os.path.getsize(src1), file_mtime=os.path.getmtime(src1),
        width=800, height=600,
    )
    pid2 = db.add_photo(
        folder_id=fid, filename="b.jpg", extension=".jpg",
        file_size=os.path.getsize(src2), file_mtime=os.path.getmtime(src2),
        width=800, height=600,
    )

    app = create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))
    client = app.test_client()

    client.get(f"/photos/{pid1}/preview?size=1920")
    time.sleep(0.05)
    client.get(f"/photos/{pid2}/preview?size=1920")

    # Quota is 0 MB so after each write eviction drains everything.
    assert db.preview_cache_total_bytes() == 0
    preview_dir = vireo_dir / "previews"
    assert not (preview_dir / f"{pid1}_1920.jpg").exists()
    assert not (preview_dir / f"{pid2}_1920.jpg").exists()


def test_preview_cache_endpoint_uses_db(client_with_photo):
    """/api/preview-cache returns totals from preview_cache table, not filesystem."""
    app, db, photo_id = client_with_photo
    client = app.test_client()
    client.get(f"/photos/{photo_id}/preview?size=1920")

    resp = client.get("/api/preview-cache")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["count"] == 1
    assert data["total_size"] > 0
    assert "quota_bytes" in data


def test_preview_cache_clear_removes_all(client_with_photo):
    """POST /api/preview-cache/clear empties the table and files."""
    import os
    app, db, photo_id = client_with_photo
    client = app.test_client()
    client.get(f"/photos/{photo_id}/preview?size=1920")
    assert db.preview_cache_total_bytes() > 0

    resp = client.post("/api/preview-cache/clear")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "files_removed" in data

    assert db.preview_cache_total_bytes() == 0
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    assert not os.path.exists(
        os.path.join(vireo_dir, "previews", f"{photo_id}_1920.jpg")
    )


def test_preview_serves_bytes_when_quota_is_zero(client_with_photo, monkeypatch):
    """With preview_cache_max_mb=0, the preview response body is non-empty
    even though eviction runs immediately after generation."""
    import config as cfg
    monkeypatch.setattr(
        cfg, "load",
        lambda: {**cfg.DEFAULTS, "preview_cache_max_mb": 0},
    )
    app, db, photo_id = client_with_photo
    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert resp.status_code == 200
    assert len(resp.data) > 100  # real JPEG, not empty
    # Eviction clears the table + file
    assert db.preview_cache_total_bytes() == 0


def test_legacy_full_cache_files_are_migrated_at_startup(tmp_path, monkeypatch):
    """Pre-refactor /full cache files ({id}.jpg) get renamed to
    {id}_{preview_max_size}.jpg and inserted into preview_cache on
    app startup so they're visible to accounting and eviction."""
    import os

    import config as cfg
    from app import create_app
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 1920})

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "test.jpg"
    Image.new("RGB", (800, 600), (180, 90, 40)).save(str(src), "JPEG", quality=85)

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    # Set up a photo and a pre-existing legacy preview file.
    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="test.jpg", extension=".jpg",
        file_size=os.path.getsize(src), file_mtime=os.path.getmtime(src),
        width=800, height=600,
    )

    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    legacy = preview_dir / f"{pid}.jpg"
    with open(legacy, "wb") as f:
        f.write(b"\xff\xd8\xff\xe0" + b"x" * 2048)

    # Creating the app triggers the migration.
    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    # Legacy file renamed, new sized file exists, row inserted.
    assert not legacy.exists()
    new_path = preview_dir / f"{pid}_1920.jpg"
    assert new_path.exists()
    row = db.preview_cache_get(pid, 1920)
    assert row is not None
    assert row["bytes"] == os.path.getsize(new_path)


def test_preview_job_writes_sized_filename_and_tracks(client_with_photo):
    """The /api/jobs/previews precompute writes {id}_{size}.jpg and
    inserts a preview_cache row, not the legacy {id}.jpg path."""
    import os
    import time

    app, db, photo_id = client_with_photo
    client = app.test_client()
    resp = client.post("/api/jobs/previews", json={})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    # Poll until the job finishes (it's a single-photo fixture, so fast).
    deadline = time.time() + 10
    while time.time() < deadline:
        status_resp = client.get(f"/api/jobs/{job_id}")
        if status_resp.status_code != 200:
            time.sleep(0.05)
            continue
        data = status_resp.get_json()
        if data.get("status") in ("completed", "failed"):
            break
        time.sleep(0.05)
    assert data["status"] == "completed"

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")

    # New naming + tracked
    assert os.path.exists(os.path.join(preview_dir, f"{photo_id}_1920.jpg"))
    assert db.preview_cache_get(photo_id, 1920) is not None

    # Legacy naming NOT produced
    assert not os.path.exists(os.path.join(preview_dir, f"{photo_id}.jpg"))


def test_eviction_keeps_row_when_unlink_fails(client_with_photo, monkeypatch):
    """If os.remove raises OSError (not FileNotFoundError), the preview_cache
    row is kept so future passes can retry instead of leaking bytes."""
    import os

    import config as cfg

    app, db, photo_id = client_with_photo
    client = app.test_client()
    client.get(f"/photos/{photo_id}/preview?size=1920")
    assert db.preview_cache_total_bytes() > 0

    # Simulate a permission error on unlink.
    real_remove = os.remove

    def flaky_remove(path, *args, **kwargs):
        if path.endswith(f"{photo_id}_1920.jpg"):
            raise PermissionError("simulated")
        return real_remove(path, *args, **kwargs)

    monkeypatch.setattr(os, "remove", flaky_remove)
    monkeypatch.setattr(
        cfg, "load",
        lambda: {**cfg.DEFAULTS, "preview_cache_max_mb": 0},
    )

    # Trigger eviction via a config save. The unlink will fail, so the
    # row should remain so a subsequent pass can retry.
    resp = client.post("/api/config", json={"preview_cache_max_mb": 0})
    assert resp.status_code == 200
    assert db.preview_cache_get(photo_id, 1920) is not None


def test_startup_evicts_when_migration_pushes_over_quota(tmp_path, monkeypatch):
    """If legacy migration inserts rows that exceed the quota, startup
    eviction drains them without waiting for a later cache write."""
    import os

    import config as cfg
    from app import create_app
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({
        **cfg.DEFAULTS,
        "preview_max_size": 1920,
        "preview_cache_max_mb": 0,  # quota of 0 drains everything
    })

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "test.jpg"
    Image.new("RGB", (800, 600), (180, 90, 40)).save(str(src), "JPEG", quality=85)

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="test.jpg", extension=".jpg",
        file_size=os.path.getsize(src), file_mtime=os.path.getmtime(src),
        width=800, height=600,
    )

    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    legacy = preview_dir / f"{pid}.jpg"
    with open(legacy, "wb") as f:
        f.write(b"\xff\xd8\xff\xe0" + b"x" * 4096)

    # Creating the app runs migration (inserts row) then eviction (drains).
    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    assert db.preview_cache_total_bytes() == 0
    assert not legacy.exists()
    assert not (preview_dir / f"{pid}_1920.jpg").exists()


def test_legacy_migration_skips_orphaned_photo_ids(tmp_path, monkeypatch):
    """Legacy {id}.jpg where id is no longer in photos table is unlinked,
    not inserted (which would fail the FK constraint)."""

    import config as cfg
    from app import create_app

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 1920})

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    # Drop a legacy file for a photo id that won't exist in the DB.
    orphan = preview_dir / "99999.jpg"
    with open(orphan, "wb") as f:
        f.write(b"\xff\xd8\xff\xe0orphan")

    # Must not raise. The orphan file should be removed so disk doesn't
    # keep pointing at a vanished photo.
    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))
    assert not orphan.exists()
    assert not (preview_dir / "99999_1920.jpg").exists()


def test_legacy_sized_preview_files_are_backfilled_at_startup(tmp_path, monkeypatch):
    """Pre-existing sized {id}_{size}.jpg files (written before
    preview_cache existed) get adopted into the LRU at startup so
    accounting and eviction can see them."""
    import os

    import config as cfg
    from app import create_app
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 1920})

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "test.jpg"
    Image.new("RGB", (800, 600), (40, 90, 180)).save(str(src), "JPEG", quality=85)

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid_kept = db.add_photo(
        folder_id=fid, filename="test.jpg", extension=".jpg",
        file_size=os.path.getsize(src), file_mtime=os.path.getmtime(src),
        width=800, height=600,
    )

    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    # Untracked sized preview for an existing photo at two tiers.
    sized_a = preview_dir / f"{pid_kept}_1920.jpg"
    sized_b = preview_dir / f"{pid_kept}_2560.jpg"
    sized_a.write_bytes(b"\xff\xd8\xff\xe0" + b"a" * 1024)
    sized_b.write_bytes(b"\xff\xd8\xff\xe0" + b"b" * 2048)
    # Sized preview pointing at a deleted photo — should be unlinked.
    orphan = preview_dir / "999999_1920.jpg"
    orphan.write_bytes(b"\xff\xd8\xff\xe0orphan")

    assert db.preview_cache_get(pid_kept, 1920) is None
    assert db.preview_cache_get(pid_kept, 2560) is None

    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    # Both real sized files are now tracked, with correct byte counts.
    row_a = db.preview_cache_get(pid_kept, 1920)
    row_b = db.preview_cache_get(pid_kept, 2560)
    assert row_a is not None and row_a["bytes"] == os.path.getsize(sized_a)
    assert row_b is not None and row_b["bytes"] == os.path.getsize(sized_b)
    # Orphan was removed; no row inserted (would have raised FK error).
    assert not orphan.exists()


def test_legacy_sized_preview_backfill_skips_already_tracked(tmp_path, monkeypatch):
    """Sized files with an existing preview_cache row are left alone —
    the migration must not overwrite last_access_at on a fresh row."""
    import os
    import time

    import config as cfg
    from app import create_app
    from db import Database
    from PIL import Image

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 1920})

    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    src = photos_dir / "test.jpg"
    Image.new("RGB", (400, 300), (10, 20, 30)).save(str(src), "JPEG", quality=85)

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(photos_dir), name="photos")
    pid = db.add_photo(
        folder_id=fid, filename="test.jpg", extension=".jpg",
        file_size=os.path.getsize(src), file_mtime=os.path.getmtime(src),
        width=400, height=300,
    )

    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    sized = preview_dir / f"{pid}_1920.jpg"
    sized.write_bytes(b"\xff\xd8\xff\xe0" + b"x" * 1024)

    # Row already exists with a recent last_access_at and the real size.
    db.preview_cache_insert(pid, 1920, os.path.getsize(sized))
    original_access = db.preview_cache_get(pid, 1920)["last_access_at"]

    # Wait long enough that an unintended re-insert would change the timestamp.
    time.sleep(0.05)
    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    row = db.preview_cache_get(pid, 1920)
    assert row is not None
    assert row["last_access_at"] == original_access


def test_legacy_migration_preserves_preview_max_size_zero(tmp_path, monkeypatch):
    """When preview_max_size=0 (full-res), legacy files are left alone —
    they can't be assigned to a size tier."""

    import config as cfg
    from app import create_app

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({**cfg.DEFAULTS, "preview_max_size": 0})

    vireo_dir = tmp_path / "vireo"
    thumb_dir = vireo_dir / "thumbnails"
    thumb_dir.mkdir(parents=True)
    preview_dir = vireo_dir / "previews"
    preview_dir.mkdir()
    db_path = str(vireo_dir / "vireo.db")

    legacy = preview_dir / "42.jpg"
    with open(legacy, "wb") as f:
        f.write(b"\xff\xd8\xff\xe0leave-me")

    create_app(db_path=db_path, thumb_cache_dir=str(thumb_dir))

    # File stays exactly where it was; no renamed version was produced.
    assert legacy.exists()
    assert not (preview_dir / "42_1920.jpg").exists()


def test_storage_clear_previews_resets_preview_cache(client_with_photo):
    """/api/storage/clear type=previews drops preview_cache rows so
    Settings "Current usage" doesn't report phantom bytes."""
    app, db, photo_id = client_with_photo
    client = app.test_client()
    # Populate the cache
    client.get(f"/photos/{photo_id}/preview?size=1920")
    assert db.preview_cache_total_bytes() > 0

    resp = client.post("/api/storage/clear", json={"type": "previews"})
    assert resp.status_code == 200
    assert db.preview_cache_total_bytes() == 0


def test_storage_delete_files_syncs_preview_cache(client_with_photo):
    """/api/storage/delete-files type=previews removes matching
    preview_cache rows for each sized-preview filename deleted."""
    app, db, photo_id = client_with_photo
    client = app.test_client()
    client.get(f"/photos/{photo_id}/preview?size=1920")
    assert db.preview_cache_get(photo_id, 1920) is not None

    resp = client.post(
        "/api/storage/delete-files",
        json={"type": "previews", "files": [f"{photo_id}_1920.jpg"]},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted"] == 1
    assert db.preview_cache_get(photo_id, 1920) is None


def test_preview_adoption_enforces_quota(client_with_photo, monkeypatch):
    """Lazily-adopting a legacy on-disk preview file still runs eviction,
    so with preview_cache_max_mb=0 the adopted file is drained like a
    freshly generated one."""
    import os

    import config as cfg

    monkeypatch.setattr(
        cfg, "load",
        lambda: {**cfg.DEFAULTS, "preview_cache_max_mb": 0},
    )
    app, db, photo_id = client_with_photo
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)

    # Pre-seed a legacy on-disk preview with no preview_cache row.
    cache_path = os.path.join(preview_dir, f"{photo_id}_1920.jpg")
    with open(cache_path, "wb") as f:
        f.write(b"\xff\xd8\xff\xe0" + b"x" * 4096)
    assert db.preview_cache_get(photo_id, 1920) is None

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert resp.status_code == 200
    assert len(resp.data) > 100  # served from memory
    # Quota is 0, so eviction drained the row and file after adoption.
    assert db.preview_cache_total_bytes() == 0
    assert not os.path.exists(cache_path)


def test_preview_cache_clear_removes_untracked_and_legacy(client_with_photo):
    """/api/preview-cache/clear removes orphaned and legacy files, not just tracked rows."""
    import os
    app, db, photo_id = client_with_photo
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)

    # Simulate: one tracked preview, one untracked sized preview, one legacy /full cache
    tracked = os.path.join(preview_dir, f"{photo_id}_1920.jpg")
    untracked = os.path.join(preview_dir, f"{photo_id}_2560.jpg")  # no row in preview_cache
    legacy = os.path.join(preview_dir, f"{photo_id}.jpg")

    for p in (tracked, untracked, legacy):
        with open(p, "wb") as f:
            f.write(b"\xff\xd8\xff\xe0fake")
    db.preview_cache_insert(photo_id, 1920, os.path.getsize(tracked))

    client = app.test_client()
    resp = client.post("/api/preview-cache/clear")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["cleared"] == 1
    assert data["files_removed"] == 3  # all three matching files

    # All three files are gone
    assert not os.path.exists(tracked)
    assert not os.path.exists(untracked)
    assert not os.path.exists(legacy)
    assert db.preview_cache_total_bytes() == 0


def test_preview_cache_clear_handles_many_unlink_failures(client_with_photo, monkeypatch):
    """Clear must survive hundreds of unlink failures without hitting the
    SQLite variable limit (~999) on the DELETE NOT IN clause. Accounting
    for failed-to-unlink files must also be preserved so usage reporting
    continues to reflect the leaked bytes.
    """
    import os
    app, db, photo_id = client_with_photo
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    os.makedirs(preview_dir, exist_ok=True)

    # Seed ~600 tracked sized preview files; well above SQLite's default
    # 999-parameter limit when multiplied by two bind params per pair.
    N = 600
    sized_files = []
    for size in range(1000, 1000 + N):
        p = os.path.join(preview_dir, f"{photo_id}_{size}.jpg")
        with open(p, "wb") as f:
            f.write(b"\xff\xd8\xff\xe0x")
        db.preview_cache_insert(photo_id, size, os.path.getsize(p))
        sized_files.append((p, photo_id, size))

    # Fail every unlink for these files — simulates a locked/read-only dir.
    real_remove = os.remove
    sized_set = {p for p, _, _ in sized_files}

    def flaky_remove(path, *a, **kw):
        if path in sized_set:
            raise OSError("simulated lock")
        return real_remove(path, *a, **kw)

    monkeypatch.setattr(os, "remove", flaky_remove)

    client = app.test_client()
    resp = client.post("/api/preview-cache/clear")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["failed"] == N
    assert data["files_removed"] == 0

    # All the rows whose files we couldn't unlink must be kept for
    # accounting purposes; usage must still reflect the leaked bytes.
    remaining = db.conn.execute(
        "SELECT COUNT(*) AS c FROM preview_cache"
    ).fetchone()["c"]
    assert remaining == N
    assert db.preview_cache_total_bytes() > 0


def test_settings_save_triggers_eviction_when_quota_shrinks(client_with_photo):
    """POSTing a smaller preview_cache_max_mb evicts down to the new quota."""
    app, db, photo_id = client_with_photo
    client = app.test_client()

    # Populate cache
    client.get(f"/photos/{photo_id}/preview?size=1920")
    assert db.preview_cache_total_bytes() > 0

    # Shrink quota to 0 via the config endpoint (same path the UI uses)
    resp = client.post("/api/config", json={"preview_cache_max_mb": 0})
    assert resp.status_code == 200

    assert db.preview_cache_total_bytes() == 0


def test_full_respects_workspace_preview_max_size_override(client_with_photo):
    """/full uses the workspace-effective preview_max_size, not just global.

    Set a workspace override to 2560 and confirm /full serves the same bytes
    as /preview?size=2560 (the handler must read get_effective_config, not
    plain cfg.get).
    """
    app, db, photo_id = client_with_photo
    # Write a workspace override for preview_max_size.
    db.update_workspace(
        db._active_workspace_id,
        config_overrides={"preview_max_size": 2560},
    )
    client = app.test_client()
    full = client.get(f"/photos/{photo_id}/full")
    preview = client.get(f"/photos/{photo_id}/preview?size=2560")
    assert full.status_code == 200
    assert preview.status_code == 200
    assert full.data == preview.data
    # Sanity: a row at size=2560 was created (not 1920).
    assert db.preview_cache_get(photo_id, 2560) is not None


def test_preview_precompute_respects_workspace_preview_max_size_override(client_with_photo):
    """/api/jobs/previews uses workspace-effective preview_max_size.

    Otherwise precompute warms the wrong tier (global size) while /full
    serves the workspace override, causing a cache miss + regenerate on
    first view and accumulating duplicate tiers on disk.
    """
    import time

    app, db, photo_id = client_with_photo
    db.update_workspace(
        db._active_workspace_id,
        config_overrides={"preview_max_size": 2560},
    )
    client = app.test_client()
    resp = client.post("/api/jobs/previews")
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    for _ in range(100):
        r = client.get(f"/api/jobs/{job_id}")
        data = r.get_json()
        if data["status"] in ("completed", "failed"):
            break
        time.sleep(0.05)
    assert data["status"] == "completed"

    # Precompute must have warmed the workspace override tier (2560),
    # not the global default (1920).
    assert db.preview_cache_get(photo_id, 2560) is not None
    assert db.preview_cache_get(photo_id, 1920) is None


def test_zero_byte_cache_file_is_regenerated(client_with_photo):
    """An interrupted write leaves a 0-byte cache file; serve regenerates it.

    Simulates a prior crashed write by dropping an empty file at the cache
    path and asserting the next GET produces a real (non-empty) preview
    and leaves a populated file on disk.
    """
    import os
    app, db, photo_id = client_with_photo
    preview_dir = os.path.join(
        os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews"
    )
    os.makedirs(preview_dir, exist_ok=True)
    cache_path = os.path.join(preview_dir, f"{photo_id}_1920.jpg")
    # Drop a zero-byte file in place as if a prior write was interrupted.
    with open(cache_path, "wb"):
        pass
    assert os.path.getsize(cache_path) == 0

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert resp.status_code == 200
    assert len(resp.data) > 0
    # File was regenerated with real bytes.
    assert os.path.getsize(cache_path) > 0
    # And tracked in the cache.
    row = db.preview_cache_get(photo_id, 1920)
    assert row is not None
    assert row["bytes"] > 0


# --- POST/DELETE /api/photos/<id>/location ----------------------------------
#
# The autocomplete pick path: client sends a Google ``place_id``, server
# looks it up via the Places HTTP wrapper and writes a leaf+parent-chain of
# ``type='location'`` keywords. ``places.place_details`` is monkeypatched
# so no HTTP traffic happens during tests.

def _central_park_details():
    """Canned Place Details dict shaped like ``vireo.places.place_details``.

    Mirrors what Google would return for Central Park, NYC: a leaf with
    coords + a four-level parent chain (city -> county -> state -> country).
    Google's ``address_components`` order is narrowest-first, which the
    upsert logic in ``Database._upsert_location_parent_chain`` reverses
    when chaining ``parent_id`` upward.
    """
    return {
        "place_id": "ChIJ4zGFAZpYwokRGUGph3Mf37k",
        "name": "Central Park",
        "lat": 40.7828,
        "lng": -73.9654,
        "address_components": [
            {"name": "New York", "short_name": "New York", "types": ["locality"]},
            {"name": "New York County", "short_name": "New York County",
             "types": ["administrative_area_level_2"]},
            {"name": "New York", "short_name": "NY", "types": ["administrative_area_level_1"]},
            {"name": "United States", "short_name": "US", "types": ["country"]},
        ],
    }


def test_post_photo_location_with_valid_place_id(app_and_db, monkeypatch):
    """Valid pick: route stores leaf + parents and returns the serialized location."""
    import config as cfg
    import places
    app, db = app_and_db

    # API key must be present or the route short-circuits with no_api_key.
    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})

    captured = {}

    def fake_place_details(place_id, key):
        captured["place_id"] = place_id
        captured["key"] = key
        return _central_park_details()

    # The route imports ``places`` at module level via ``import places``.
    monkeypatch.setattr(places, "place_details", fake_place_details)

    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location",
        json={"place_id": "ChIJ_x"},
    )
    assert resp.status_code == 200, resp.get_json()
    data = resp.get_json()

    # Response shape — leaf fields + parent chain (broadest -> narrowest, no leaf).
    loc = data["location"]
    assert loc["name"] == "Central Park"
    assert loc["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"
    assert loc["latitude"] == 40.7828
    assert loc["longitude"] == -73.9654
    assert [p["name"] for p in loc["parent_chain"]] == [
        "United States", "New York", "New York County", "New York",
    ]

    # And the route actually called Google with the body's place_id + config key.
    assert captured == {"place_id": "ChIJ_x", "key": "FAKE-KEY"}

    # Photo now has exactly one type='location' keyword link, pointing at the
    # leaf row that carries the place_id.
    rows = db.conn.execute(
        "SELECT k.id, k.name, k.place_id FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location'",
        (pid,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["place_id"] == "ChIJ4zGFAZpYwokRGUGph3Mf37k"
    assert rows[0]["name"] == "Central Park"


def test_post_photo_location_returns_400_on_missing_place_id(app_and_db):
    """Empty body / missing place_id is a 400 — never reaches Google."""
    app, db = app_and_db
    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(f"/api/photos/{pid}/location", json={})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "missing place_id"


def test_post_photo_location_returns_400_on_empty_api_key(app_and_db):
    """No configured API key: degrade to a 400 ``no_api_key`` error."""
    import config as cfg
    app, db = app_and_db
    # Explicitly clear the key so the route hits the empty-key branch even
    # if a previous test left one behind in the same temp config file.
    cfg.save({**cfg.load(), "google_maps_api_key": ""})

    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location",
        json={"place_id": "ChIJ_x"},
    )
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "no_api_key"


def test_post_photo_location_returns_404_when_google_returns_none(app_and_db, monkeypatch):
    """Google returns ZERO_RESULTS / NOT_FOUND -> wrapper returns None -> 404."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})
    monkeypatch.setattr(places, "place_details", lambda place_id, key: None)

    photo = db.get_photos()[0]
    pid = photo["id"]

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location",
        json={"place_id": "ChIJ_unknown"},
    )
    assert resp.status_code == 404
    assert resp.get_json()["error"] == "place_not_found"


def test_delete_photo_location_clears_links(app_and_db):
    """DELETE removes location keyword links but leaves the keyword row intact."""
    app, db = app_and_db
    photo = db.get_photos()[0]
    pid = photo["id"]

    # Set up an existing location link directly via DB methods (no Google
    # round-trip needed for the delete path).
    leaf_id = db.upsert_place_chain(_central_park_details())
    db.set_photo_location(pid, leaf_id)
    # Sanity: the link exists before DELETE.
    pre_links = db.conn.execute(
        "SELECT 1 FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location'",
        (pid,),
    ).fetchall()
    assert len(pre_links) == 1

    client = app.test_client()
    resp = client.delete(f"/api/photos/{pid}/location")
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True}

    # No location links remain on the photo.
    post_links = db.conn.execute(
        "SELECT 1 FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location'",
        (pid,),
    ).fetchall()
    assert post_links == []

    # The keyword row itself is preserved — other photos / future links may
    # still reference it.
    leaf_row = db.conn.execute(
        "SELECT id, name FROM keywords WHERE id = ?", (leaf_id,),
    ).fetchone()
    assert leaf_row is not None
    assert leaf_row["name"] == "Central Park"
def test_post_photo_location_records_edit(app_and_db, monkeypatch):
    """POST adds an entry to the audit log so the action is undoable/visible."""
    import config as cfg
    import places
    app, db = app_and_db

    cfg.save({**cfg.load(), "google_maps_api_key": "FAKE-KEY"})
    monkeypatch.setattr(places, "place_details",
                        lambda place_id, key: _central_park_details())

    photo = db.get_photos()[0]
    pid = photo["id"]

    pre_history = db.get_edit_history()
    pre_count = len(pre_history)

    client = app.test_client()
    resp = client.post(
        f"/api/photos/{pid}/location",
        json={"place_id": "ChIJ_x"},
    )
    assert resp.status_code == 200, resp.get_json()

    post_history = db.get_edit_history()
    assert len(post_history) == pre_count + 1
    # Most recent first.
    entry = post_history[0]
    assert entry["action_type"] == "location_set"
    assert "Central Park" in entry["description"]


def test_delete_photo_location_records_edit(app_and_db):
    """DELETE adds an entry to the audit log even though it doesn't write a sidecar."""
    app, db = app_and_db

    leaf_id = db.upsert_place_chain(_central_park_details())
    photo = db.get_photos()[0]
    pid = photo["id"]
    db.set_photo_location(pid, leaf_id)

    pre_history = db.get_edit_history()
    pre_count = len(pre_history)

    client = app.test_client()
    resp = client.delete(f"/api/photos/{pid}/location")
    assert resp.status_code == 200

    post_history = db.get_edit_history()
    assert len(post_history) == pre_count + 1
    entry = post_history[0]
    assert entry["action_type"] == "location_clear"
    assert entry["description"] == "cleared location"
