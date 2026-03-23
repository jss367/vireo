# vireo/tests/test_app.py
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lr-migration'))

from PIL import Image


def _setup_app(tmp_path):
    """Create a test app with sample data."""
    from db import Database
    from app import create_app

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
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

    # Create thumbnail files
    for pid in [p1, p2, p3]:
        Image.new('RGB', (100, 100)).save(os.path.join(thumb_dir, f"{pid}.jpg"))

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    return app, db


def test_index_redirects_to_browse(tmp_path):
    """GET / redirects to /browse."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()
    resp = client.get('/')
    assert resp.status_code == 302
    assert '/browse' in resp.headers['Location']


def test_browse_page(tmp_path):
    """GET /browse returns 200."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()
    resp = client.get('/browse')
    assert resp.status_code == 200


def test_api_folders(tmp_path):
    """GET /api/folders returns folder tree."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()
    resp = client.get('/api/folders')
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 2
    paths = {f['path'] for f in data}
    assert '/photos/2024' in paths


def test_api_photos_default(tmp_path):
    """GET /api/photos returns all photos."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()
    resp = client.get('/api/photos')
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data['photos']) == 3
    assert 'total' in data


def test_api_photos_pagination(tmp_path):
    """GET /api/photos supports pagination."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()
    resp = client.get('/api/photos?per_page=2&page=1')
    data = resp.get_json()
    assert len(data['photos']) == 2

    resp = client.get('/api/photos?per_page=2&page=2')
    data = resp.get_json()
    assert len(data['photos']) == 1


def test_api_photos_filter_folder(tmp_path):
    """GET /api/photos?folder_id= filters by folder."""
    app, db = _setup_app(tmp_path)
    folders = db.get_folder_tree()
    jan = [f for f in folders if f['name'] == 'January'][0]

    client = app.test_client()
    resp = client.get(f'/api/photos?folder_id={jan["id"]}')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['filename'] == 'bird2.jpg'


def test_api_photos_filter_rating(tmp_path):
    """GET /api/photos?rating_min= filters by minimum rating."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()
    resp = client.get('/api/photos?rating_min=4')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['filename'] == 'bird3.jpg'


def test_api_photos_filter_date_range(tmp_path):
    """GET /api/photos?date_from=&date_to= filters by date range."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()
    resp = client.get('/api/photos?date_from=2024-01-01&date_to=2024-02-01')
    data = resp.get_json()
    assert len(data['photos']) == 2


def test_api_photos_filter_keyword(tmp_path):
    """GET /api/photos?keyword= filters by keyword."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()
    resp = client.get('/api/photos?keyword=Cardinal')
    data = resp.get_json()
    assert len(data['photos']) == 1
    assert data['photos'][0]['filename'] == 'bird1.jpg'


def test_api_photo_detail(tmp_path):
    """GET /api/photos/<id> returns photo with keywords."""
    app, db = _setup_app(tmp_path)
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.get(f'/api/photos/{pid}')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['filename'] == 'bird1.jpg'
    assert 'keywords' in data


def test_api_keywords(tmp_path):
    """GET /api/keywords returns keyword tree."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()
    resp = client.get('/api/keywords')
    assert resp.status_code == 200
    data = resp.get_json()
    names = {k['name'] for k in data}
    assert 'Cardinal' in names
    assert 'Sparrow' in names


def test_thumbnail_serving(tmp_path):
    """GET /thumbnails/<id>.jpg serves thumbnail from cache."""
    app, db = _setup_app(tmp_path)
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.get(f'/thumbnails/{pid}.jpg')
    assert resp.status_code == 200
    assert resp.content_type in ('image/jpeg', 'image/jpg')


# ---------- Edit API Tests (Task 7) ----------

def test_set_rating(tmp_path):
    """POST /api/photos/<id>/rating updates rating and queues pending change."""
    app, db = _setup_app(tmp_path)
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/rating',
                       json={'rating': 5})
    assert resp.status_code == 200

    photo = db.get_photo(pid)
    assert photo['rating'] == 5

    changes = db.get_pending_changes()
    assert any(c['photo_id'] == pid and c['change_type'] == 'rating' for c in changes)


def test_set_flag(tmp_path):
    """POST /api/photos/<id>/flag updates flag and queues pending change."""
    app, db = _setup_app(tmp_path)
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/flag',
                       json={'flag': 'flagged'})
    assert resp.status_code == 200

    photo = db.get_photo(pid)
    assert photo['flag'] == 'flagged'

    changes = db.get_pending_changes()
    assert any(c['photo_id'] == pid and c['change_type'] == 'flag' for c in changes)


def test_add_keyword_to_photo(tmp_path):
    """POST /api/photos/<id>/keywords adds keyword and queues pending change."""
    app, db = _setup_app(tmp_path)
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/keywords',
                       json={'name': 'Woodpecker'})
    assert resp.status_code == 200

    keywords = db.get_photo_keywords(pid)
    kw_names = {k['name'] for k in keywords}
    assert 'Woodpecker' in kw_names

    changes = db.get_pending_changes()
    assert any(c['photo_id'] == pid and c['change_type'] == 'keyword_add' for c in changes)


def test_remove_keyword_from_photo(tmp_path):
    """DELETE /api/photos/<id>/keywords/<kid> removes keyword and queues pending change."""
    app, db = _setup_app(tmp_path)
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    # bird1 has 'Cardinal' keyword
    keywords = db.get_photo_keywords(pid)
    kid = keywords[0]['id']

    resp = client.delete(f'/api/photos/{pid}/keywords/{kid}')
    assert resp.status_code == 200

    keywords = db.get_photo_keywords(pid)
    assert len(keywords) == 0

    changes = db.get_pending_changes()
    assert any(c['photo_id'] == pid and c['change_type'] == 'keyword_remove' for c in changes)


def test_sync_status(tmp_path):
    """GET /api/sync/status returns pending count."""
    app, db = _setup_app(tmp_path)
    client = app.test_client()

    resp = client.get('/api/sync/status')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['pending_count'] == 0

    # Queue a change
    photos = db.get_photos()
    db.queue_change(photos[0]['id'], 'rating', '3')

    resp = client.get('/api/sync/status')
    data = resp.get_json()
    assert data['pending_count'] == 1


# ---------- Job API Tests ----------

def test_job_scan_returns_job_id(tmp_path):
    """POST /api/jobs/scan starts a background scan and returns job_id."""
    app, db = _setup_app(tmp_path)
    client = app.test_client()

    # Create a scannable directory
    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'test.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'job_id' in data
    assert data['job_id'].startswith('scan-')


def test_job_scan_invalid_root(tmp_path):
    """POST /api/jobs/scan with invalid root returns 400."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()

    resp = client.post('/api/jobs/scan', json={'root': '/nonexistent/path'})
    assert resp.status_code == 400


def test_job_status_endpoint(tmp_path):
    """GET /api/jobs/<id> returns job status."""
    import time
    app, db = _setup_app(tmp_path)
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'test.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    job_id = resp.get_json()['job_id']

    # Wait for completion
    for _ in range(50):
        resp = client.get(f'/api/jobs/{job_id}')
        data = resp.get_json()
        if data['status'] in ('completed', 'failed'):
            break
        time.sleep(0.1)

    assert resp.status_code == 200
    assert data['status'] == 'completed'


def test_jobs_list(tmp_path):
    """GET /api/jobs returns active and history lists."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()

    resp = client.get('/api/jobs')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'active' in data
    assert 'history' in data


def test_scan_status_includes_extended_stats(tmp_path):
    """GET /api/scan/status includes keyword count, db_size, etc."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()

    resp = client.get('/api/scan/status')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'photo_count' in data
    assert 'keyword_count' in data
    assert 'db_size' in data
    assert 'thumb_cache_size' in data


def test_logs_recent(tmp_path):
    """GET /api/logs/recent returns recent log entries."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()

    resp = client.get('/api/logs/recent?count=10')
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list)


def test_logs_page(tmp_path):
    """GET /logs returns 200."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()

    resp = client.get('/logs')
    assert resp.status_code == 200


def test_api_darktable_status(tmp_path):
    """GET /api/darktable/status returns availability info."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()
    resp = client.get('/api/darktable/status')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'available' in data
    assert isinstance(data['available'], bool)
    assert 'bin' in data


def test_api_job_develop_requires_photo_ids(tmp_path):
    """POST /api/jobs/develop returns 400 without photo_ids."""
    app, _ = _setup_app(tmp_path)
    client = app.test_client()
    resp = client.post('/api/jobs/develop',
                       data=json.dumps({}),
                       content_type='application/json')
    assert resp.status_code == 400


def test_api_job_develop_requires_darktable(tmp_path):
    """POST /api/jobs/develop returns 400 when darktable not available."""
    app, db = _setup_app(tmp_path)
    client = app.test_client()
    resp = client.post('/api/jobs/develop',
                       data=json.dumps({"photo_ids": [1]}),
                       content_type='application/json')
    assert resp.status_code == 400
    data = resp.get_json()
    assert 'darktable' in data['error'].lower()
