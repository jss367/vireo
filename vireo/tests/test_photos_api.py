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


def test_thumbnail_serving(app_and_db):
    """GET /thumbnails/<id>.jpg serves thumbnail from cache."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.get(f'/thumbnails/{pid}.jpg')
    assert resp.status_code == 200
    assert resp.content_type in ('image/jpeg', 'image/jpg')
