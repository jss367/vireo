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
    # Add accepted predictions
    db.add_prediction(1, 'Cardinal', 0.9, 'bioclip')
    db.add_prediction(3, 'Sparrow', 0.8, 'bioclip')
    preds = db.get_predictions(photo_ids=[1, 3])
    for pr in preds:
        db.conn.execute("UPDATE predictions SET status='accepted' WHERE id=?", (pr['id'],))
    db.conn.commit()

    client = app.test_client()
    resp = client.get('/api/photos/geo?species=Cardinal')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['species'] == 'Cardinal'


def test_api_species_list(app_and_db):
    """GET /api/species returns accepted species from geolocated photos."""
    app, db = app_and_db
    db.conn.execute("UPDATE photos SET latitude=37.0, longitude=-122.0 WHERE id=1")
    db.add_prediction(1, 'Cardinal', 0.9, 'bioclip')
    preds = db.get_predictions(photo_ids=[1])
    db.conn.execute("UPDATE predictions SET status='accepted' WHERE id=?", (preds[0]['id'],))
    db.conn.commit()

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
