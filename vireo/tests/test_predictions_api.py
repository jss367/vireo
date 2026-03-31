"""Tests for prediction API routes (/api/predictions/*)."""
import json

_DET = {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}


def _make_detection(db, photo_id):
    """Create a detection for a photo and return its ID."""
    return db.save_detections(photo_id, [_DET], detector_model="MDV6")[0]


def _seed_predictions(db):
    """Add two predictions in the same group for the first two photos."""
    photos = db.get_photos()
    det0 = _make_detection(db, photos[0]['id'])
    det1 = _make_detection(db, photos[1]['id'])
    db.add_prediction(det0, 'Northern Cardinal', 0.95, 'test-model',
                      category='new', group_id=1)
    db.add_prediction(det1, 'House Sparrow', 0.80, 'test-model',
                      category='new', group_id=1)
    return photos


def test_list_predictions(app_and_db):
    """GET /api/predictions returns seeded predictions."""
    app, db = app_and_db
    photos = _seed_predictions(db)
    client = app.test_client()

    resp = client.get('/api/predictions')
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list)
    assert len(data) == 2

    species_set = {p['species'] for p in data}
    assert 'Northern Cardinal' in species_set
    assert 'House Sparrow' in species_set


def test_list_predictions_filter_by_status(app_and_db):
    """GET /api/predictions?status=pending returns only pending predictions."""
    app, db = app_and_db
    photos = _seed_predictions(db)
    client = app.test_client()

    # Reject one prediction so it is no longer pending
    pred = db.conn.execute(
        "SELECT id FROM predictions WHERE species = 'House Sparrow'"
    ).fetchone()
    db.update_prediction_status(pred['id'], 'rejected')

    resp = client.get('/api/predictions?status=pending')
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 1
    assert data[0]['species'] == 'Northern Cardinal'
    assert data[0]['status'] == 'pending'

    # Verify rejected filter also works
    resp = client.get('/api/predictions?status=rejected')
    data = resp.get_json()
    assert len(data) == 1
    assert data[0]['species'] == 'House Sparrow'


def test_accept_prediction(app_and_db):
    """POST accept marks prediction as accepted and adds species keyword to photo."""
    app, db = app_and_db
    photos = _seed_predictions(db)
    client = app.test_client()

    # Get the Northern Cardinal prediction (not in a group for this test —
    # add a standalone prediction to avoid group-accept behavior)
    det2 = _make_detection(db, photos[2]['id'])
    db.add_prediction(det2, 'Blue Jay', 0.90, 'test-model',
                      category='new', group_id=None)
    pred = db.conn.execute(
        "SELECT id FROM predictions WHERE species = 'Blue Jay'"
    ).fetchone()

    resp = client.post(f'/api/predictions/{pred["id"]}/accept')
    assert resp.status_code == 200
    assert resp.get_json()['ok'] is True

    # Prediction status should be accepted
    row = db.conn.execute(
        "SELECT status FROM predictions WHERE id = ?", (pred['id'],)
    ).fetchone()
    assert row['status'] == 'accepted'

    # Species keyword should have been added to the photo
    keywords = db.get_photo_keywords(photos[2]['id'])
    kw_names = {k['name'] for k in keywords}
    assert 'Blue Jay' in kw_names


def test_reject_prediction(app_and_db):
    """POST reject marks prediction as rejected."""
    app, db = app_and_db
    photos = _seed_predictions(db)
    client = app.test_client()

    pred = db.conn.execute(
        "SELECT id FROM predictions WHERE species = 'Northern Cardinal'"
    ).fetchone()

    resp = client.post(f'/api/predictions/{pred["id"]}/reject')
    assert resp.status_code == 200
    assert resp.get_json()['ok'] is True

    row = db.conn.execute(
        "SELECT status FROM predictions WHERE id = ?", (pred['id'],)
    ).fetchone()
    assert row['status'] == 'rejected'

    # Verify no species keyword was added
    keywords = db.get_photo_keywords(photos[0]['id'])
    kw_names = {k['name'] for k in keywords}
    assert 'Northern Cardinal' not in kw_names


def test_get_prediction_group(app_and_db):
    """GET /api/predictions/group/1 returns both group members."""
    app, db = app_and_db
    _seed_predictions(db)
    client = app.test_client()

    resp = client.get('/api/predictions/group/1')
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list)
    assert len(data) == 2

    species_set = {p['species'] for p in data}
    assert 'Northern Cardinal' in species_set
    assert 'House Sparrow' in species_set

    # Each member should have photo data fields
    for member in data:
        assert 'filename' in member
        assert 'photo_id' in member


def test_prediction_group_apply(app_and_db):
    """POST group/apply flags picks, rejects rejects, adds species keyword."""
    app, db = app_and_db
    photos = _seed_predictions(db)
    client = app.test_client()

    pick_id = photos[0]['id']
    reject_id = photos[1]['id']

    resp = client.post('/api/predictions/group/apply', json={
        'picks': [pick_id],
        'rejects': [reject_id],
        'species': 'Northern Cardinal',
    })
    assert resp.status_code == 200
    assert resp.get_json()['ok'] is True

    # Pick photo should be flagged and have the species keyword
    pick_photo = db.get_photo(pick_id)
    assert pick_photo['flag'] == 'flagged'

    pick_kws = {k['name'] for k in db.get_photo_keywords(pick_id)}
    assert 'Northern Cardinal' in pick_kws

    # Reject photo should be rejected
    reject_photo = db.get_photo(reject_id)
    assert reject_photo['flag'] == 'rejected'

    # Predictions for the pick should be accepted
    pick_preds = db.conn.execute(
        """SELECT pr.status FROM predictions pr
           JOIN detections d ON d.id = pr.detection_id
           WHERE d.photo_id = ?""", (pick_id,)
    ).fetchall()
    assert all(p['status'] == 'accepted' for p in pick_preds)

    # Predictions for the reject should be rejected
    reject_preds = db.conn.execute(
        """SELECT pr.status FROM predictions pr
           JOIN detections d ON d.id = pr.detection_id
           WHERE d.photo_id = ?""", (reject_id,)
    ).fetchall()
    assert all(p['status'] == 'rejected' for p in reject_preds)


def test_predictions_for_collection(app_and_db):
    """GET /api/predictions?collection_id=N scopes to that collection's photos."""
    app, db = app_and_db
    photos = _seed_predictions(db)
    client = app.test_client()

    # Create a static collection containing only the first photo
    rules = json.dumps([{"field": "photo_ids", "value": [photos[0]['id']]}])
    coll_id = db.add_collection('Test Collection', rules)

    resp = client.get(f'/api/predictions?collection_id={coll_id}')
    assert resp.status_code == 200
    data = resp.get_json()

    # Only the prediction for the first photo should be returned
    assert len(data) == 1
    assert data[0]['species'] == 'Northern Cardinal'
    assert data[0]['photo_id'] == photos[0]['id']
