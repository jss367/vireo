"""Tests for prediction API routes (/api/predictions/*)."""
import json

_DET = {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}


def _make_detection(db, photo_id):
    """Create a detection for a photo and return its ID."""
    return db.save_detections(photo_id, [_DET], detector_model="MDV6")[0]


def _seed_predictions(db):
    """Add predictions using the detection-based schema."""
    photos = db.get_photos()
    det0 = _make_detection(db, photos[0]['id'])
    det1 = _make_detection(db, photos[1]['id'])
    db.add_prediction(detection_id=det0, species='Northern Cardinal',
                      confidence=0.95, model='test-model', category='new', group_id='g1')
    db.add_prediction(detection_id=det1, species='House Sparrow',
                      confidence=0.80, model='test-model', category='new', group_id='g1')
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

    # Get the Blue Jay prediction (not in a group for this test —
    # add a standalone prediction to avoid group-accept behavior)
    det2 = _make_detection(db, photos[2]['id'])
    db.add_prediction(detection_id=det2, species='Blue Jay', confidence=0.90,
                      model='test-model', category='new', group_id=None)
    pred = db.conn.execute(
        "SELECT id FROM predictions WHERE species = 'Blue Jay'"
    ).fetchone()

    resp = client.post(f'/api/predictions/{pred["id"]}/accept')
    assert resp.status_code == 200
    assert resp.get_json()['ok'] is True

    # Prediction status should be accepted (workspace-scoped via prediction_review)
    assert db.get_review_status(pred['id'], db._active_workspace_id) == 'accepted'

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

    assert db.get_review_status(pred['id'], db._active_workspace_id) == 'rejected'

    # Verify no species keyword was added
    keywords = db.get_photo_keywords(photos[0]['id'])
    kw_names = {k['name'] for k in keywords}
    assert 'Northern Cardinal' not in kw_names


def test_reject_prediction_missing_id_returns_404(app_and_db):
    """Stale prediction IDs should 404, not 500.

    prediction_review has an FK on prediction_id, so blindly writing
    review state for a non-existent prediction would now raise an
    IntegrityError. The endpoint must check existence first.
    """
    app, db = app_and_db
    _seed_predictions(db)
    client = app.test_client()

    resp = client.post('/api/predictions/999999/reject')
    assert resp.status_code == 404
    # And nothing got written to prediction_review for the stale id
    assert db.get_review_status(999999, db._active_workspace_id) == 'pending'


def test_get_prediction_group(app_and_db):
    """GET /api/predictions/group/1 returns both group members."""
    app, db = app_and_db
    _seed_predictions(db)
    client = app.test_client()

    resp = client.get('/api/predictions/group/g1')
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

    # Predictions for the pick should be accepted (review state in prediction_review)
    ws_id = db._active_workspace_id
    pick_preds = db.conn.execute(
        """SELECT COALESCE(pr_rev.status, 'pending') AS status
           FROM predictions pr
           JOIN detections d ON d.id = pr.detection_id
           LEFT JOIN prediction_review pr_rev
             ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
           WHERE d.photo_id = ?""", (ws_id, pick_id)
    ).fetchall()
    assert all(p['status'] == 'accepted' for p in pick_preds)

    # Predictions for the reject should be rejected
    reject_preds = db.conn.execute(
        """SELECT COALESCE(pr_rev.status, 'pending') AS status
           FROM predictions pr
           JOIN detections d ON d.id = pr.detection_id
           LEFT JOIN prediction_review pr_rev
             ON pr_rev.prediction_id = pr.id AND pr_rev.workspace_id = ?
           WHERE d.photo_id = ?""", (ws_id, reject_id)
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


def test_predictions_include_alternatives(app_and_db):
    """GET /api/predictions includes alternatives for each prediction."""
    app, db = app_and_db
    photos = db.get_photos()
    det_ids = db.save_detections(photos[0]['id'], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9}
    ], detector_model="MDV6")
    det_id = det_ids[0]
    db.add_prediction(detection_id=det_id, species='Robin', confidence=0.85,
                      model='test-model')
    db.add_prediction(detection_id=det_id, species='Sparrow', confidence=0.10,
                      model='test-model')
    db.add_prediction(detection_id=det_id, species='Finch', confidence=0.05,
                      model='test-model')
    # Mark alternatives in the prediction_review table for this workspace
    ws_id = db._active_workspace_id
    for sp in ('Sparrow', 'Finch'):
        row = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", (sp,)
        ).fetchone()
        db.set_review_status(row['id'], ws_id, 'alternative')

    client = app.test_client()
    resp = client.get('/api/predictions')
    data = resp.get_json()

    # Should return only pending predictions at top level
    pending = [p for p in data if p['status'] == 'pending']
    assert len(pending) == 1
    assert pending[0]['species'] == 'Robin'

    # Each pending prediction should have alternatives attached
    assert 'alternatives' in pending[0]
    alt_species = [a['species'] for a in pending[0]['alternatives']]
    assert alt_species == ['Sparrow', 'Finch']


def test_accept_alternative_prediction(app_and_db):
    """Accepting an alternative marks it accepted, rejects the top-1, and adds keyword."""
    app, db = app_and_db
    photos = db.get_photos()
    det_ids = db.save_detections(photos[0]['id'], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9}
    ], detector_model="MDV6")
    det_id = det_ids[0]
    db.add_prediction(detection_id=det_id, species='Robin', confidence=0.85,
                      model='test-model')
    db.add_prediction(detection_id=det_id, species='Sparrow', confidence=0.10,
                      model='test-model')

    ws_id = db._active_workspace_id
    # Mark Sparrow as an alternative in the workspace's review table.
    alt = db.conn.execute(
        "SELECT id FROM predictions WHERE species = 'Sparrow'"
    ).fetchone()
    db.set_review_status(alt['id'], ws_id, 'alternative')

    client = app.test_client()
    resp = client.post(f'/api/predictions/{alt["id"]}/accept')
    assert resp.status_code == 200

    # Alternative should be accepted
    assert db.get_review_status(alt['id'], ws_id) == 'accepted'

    # Original top-1 should be rejected
    robin = db.conn.execute(
        "SELECT id FROM predictions WHERE species = 'Robin'"
    ).fetchone()
    assert db.get_review_status(robin['id'], ws_id) == 'rejected'

    # Sparrow keyword should be on the photo
    keywords = db.get_photo_keywords(photos[0]['id'])
    kw_names = {k['name'] for k in keywords}
    assert 'Sparrow' in kw_names
