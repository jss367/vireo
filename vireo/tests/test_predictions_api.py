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
    data = resp.get_json()['predictions']
    assert isinstance(data, list)
    assert len(data) == 2

    species_set = {p['species'] for p in data}
    assert 'Northern Cardinal' in species_set
    assert 'House Sparrow' in species_set


def test_list_predictions_includes_photo_edit_recipe(app_and_db):
    """GET /api/predictions exposes photo edit recipes for review cards."""
    app, db = app_and_db
    photos = _seed_predictions(db)
    db.set_photo_edit_recipe(photos[0]["id"], {"rotation": 90})
    client = app.test_client()

    resp = client.get('/api/predictions')
    assert resp.status_code == 200
    data = resp.get_json()['predictions']
    by_photo = {p["photo_id"]: p for p in data}
    assert by_photo[photos[0]["id"]]["edit_recipe"] == {"version": 1, "rotation": 90}
    assert by_photo[photos[1]["id"]]["edit_recipe"] is None


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
    data = resp.get_json()['predictions']
    assert len(data) == 1
    assert data[0]['species'] == 'Northern Cardinal'
    assert data[0]['status'] == 'pending'

    # Verify rejected filter also works
    resp = client.get('/api/predictions?status=rejected')
    data = resp.get_json()['predictions']
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


def test_mark_prediction_reviewed(app_and_db):
    """POST reviewed marks a pending prediction as reviewed."""
    app, db = app_and_db
    photos = _seed_predictions(db)
    client = app.test_client()

    det = _make_detection(db, photos[2]['id'])
    db.add_prediction(detection_id=det, species='Blue Jay', confidence=0.90,
                      model='test-model', category='new', group_id=None)
    pred = db.conn.execute(
        "SELECT id FROM predictions WHERE species = 'Blue Jay'"
    ).fetchone()

    resp = client.post(f'/api/predictions/{pred["id"]}/reviewed')
    assert resp.status_code == 200
    assert resp.get_json()['ok'] is True
    assert db.get_review_status(pred['id'], db._active_workspace_id) == 'reviewed'


def test_mark_prediction_reviewed_rejects_non_pending(app_and_db):
    """Only pending predictions may transition to reviewed.

    A stale/double request or direct API call against an already
    accepted/rejected prediction must not silently overwrite the prior
    decision; the endpoint returns 409 and the status is preserved.
    """
    app, db = app_and_db
    photos = _seed_predictions(db)
    client = app.test_client()

    det_acc = _make_detection(db, photos[2]['id'])
    db.add_prediction(detection_id=det_acc, species='Blue Jay', confidence=0.90,
                      model='test-model', category='new', group_id=None)
    accepted = db.conn.execute(
        "SELECT id FROM predictions WHERE species = 'Blue Jay'"
    ).fetchone()
    db.update_prediction_status(accepted['id'], 'accepted')

    resp = client.post(f'/api/predictions/{accepted["id"]}/reviewed')
    assert resp.status_code == 409
    assert db.get_review_status(
        accepted['id'], db._active_workspace_id) == 'accepted'

    rejected = db.conn.execute(
        "SELECT id FROM predictions WHERE species = 'Northern Cardinal'"
    ).fetchone()
    db.update_prediction_status(rejected['id'], 'rejected')

    resp = client.post(f'/api/predictions/{rejected["id"]}/reviewed')
    assert resp.status_code == 409
    assert db.get_review_status(
        rejected['id'], db._active_workspace_id) == 'rejected'


def test_mark_prediction_reviewed_missing_id_returns_404(app_and_db):
    """Stale prediction IDs should 404, not 500 or a silent write."""
    app, db = app_and_db
    _seed_predictions(db)
    client = app.test_client()

    resp = client.post('/api/predictions/999999/reviewed')
    assert resp.status_code == 404
    assert db.get_review_status(999999, db._active_workspace_id) == 'pending'


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
    data = resp.get_json()['predictions']

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
    data = resp.get_json()['predictions']

    # Should return only pending predictions at top level
    pending = [p for p in data if p['status'] == 'pending']
    assert len(pending) == 1
    assert pending[0]['species'] == 'Robin'

    # Each pending prediction should have alternatives attached
    assert 'alternatives' in pending[0]
    alt_species = [a['species'] for a in pending[0]['alternatives']]
    assert alt_species == ['Sparrow', 'Finch']


def test_predictions_alternatives_survive_row_level_rules(app_and_db):
    """Row-level Review filters must not strip alternatives off the
    parent prediction.

    ``get_predictions()`` re-applies row-level predicates (like
    ``prediction_confidence`` / ``prediction_status``) to each returned
    row. If we forward the same ``rules`` to the ``status='alternative'``
    lookup, alternatives whose own confidence/status differ from the
    parent are dropped before ``alts_by_key`` is built — the parent then
    renders in the Review grid with an empty ``alternatives`` list and
    the user cannot accept an alternate species in that filtered view.
    """
    app, db = app_and_db
    photos = db.get_photos()
    det_ids = db.save_detections(photos[0]['id'], [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.9}
    ], detector_model="MDV6")
    det_id = det_ids[0]
    db.add_prediction(detection_id=det_id, species='Robin', confidence=0.95,
                      model='test-model')
    db.add_prediction(detection_id=det_id, species='Sparrow', confidence=0.10,
                      model='test-model')
    db.add_prediction(detection_id=det_id, species='Finch', confidence=0.05,
                      model='test-model')
    ws_id = db._active_workspace_id
    for sp in ('Sparrow', 'Finch'):
        row = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", (sp,)
        ).fetchone()
        db.set_review_status(row['id'], ws_id, 'alternative')

    client = app.test_client()
    rules = json.dumps([
        {"field": "prediction_confidence", "op": ">=", "value": 0.8},
    ])
    resp = client.get(f'/api/predictions?rules={rules}')
    assert resp.status_code == 200
    preds = resp.get_json()['predictions']
    assert len(preds) == 1
    assert preds[0]['species'] == 'Robin'
    alt_species = [a['species'] for a in preds[0]['alternatives']]
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


def test_list_predictions_gates_representative_on_current_eligibility(app_and_db):
    """A stale representative preference must not light up the Review-card
    badge for a photo that is now rejected or no longer carries the stored
    species keyword. get_predictions() only pulls filename/timestamp from
    photos, so _attach_species_representatives can't see p.flag on prediction
    dicts — this test protects the eligible-representative lookup that
    replaces the missing-column check.
    """
    app, db = app_and_db
    photos = _seed_predictions(db)

    live_pid = photos[0]['id']
    rejected_pid = photos[1]['id']
    det_untagged = _make_detection(db, photos[2]['id'])
    db.add_prediction(detection_id=det_untagged, species='Coyote Untagged',
                      confidence=0.90, model='test-model', category='new',
                      group_id=None)

    # Tag each photo with its own species so failure modes are independent.
    kid_live = db.add_keyword('Coyote Live', is_species=True)
    kid_rejected = db.add_keyword('Coyote Rejected', is_species=True)
    kid_untagged = db.add_keyword('Coyote Untagged', is_species=True)
    db.tag_photo(live_pid, kid_live)
    db.tag_photo(rejected_pid, kid_rejected)
    db.tag_photo(photos[2]['id'], kid_untagged)
    db.set_species_representative('Coyote Live', live_pid)
    db.set_species_representative('Coyote Rejected', rejected_pid)
    db.set_species_representative('Coyote Untagged', photos[2]['id'])

    # Make each stale in one of the two ways the eligibility gate covers.
    # Preference rows themselves remain intact (undo-friendly).
    db.update_photo_flag(rejected_pid, 'rejected')
    db.untag_photo(photos[2]['id'], kid_untagged)

    client = app.test_client()
    resp = client.get('/api/predictions')
    assert resp.status_code == 200
    by_photo = {p['photo_id']: p for p in resp.get_json()['predictions']}

    # Eligible representative still lights up on the review card.
    assert by_photo[live_pid]['is_species_representative'] is True
    # Rejected photo no longer counts as a representative even though the
    # preference row still points at it.
    assert by_photo[rejected_pid]['is_species_representative'] is False
    # Photo whose species keyword was untagged no longer counts either.
    assert by_photo[photos[2]['id']]['is_species_representative'] is False


def test_get_predictions_species_rule_keeps_disagreement_rows(app_and_db):
    """A photo confirmed as species X with a pending prediction of species Y
    must surface under a ``species is X`` filter — that disagreement row is
    exactly what a reviewer filters for. The row-level pass must not
    re-check the prediction's proposed species against the keyword filter
    and hide it (species is a photo-keyword field, not a per-row field)."""
    _, db = app_and_db
    photos = db.get_photos()
    # Confirmed species keyword on p1.
    robin_id = db.add_keyword('Robin', is_species=True)
    db.tag_photo(photos[0]['id'], robin_id)
    # Pending prediction proposes Sparrow — the reviewer wants to see it.
    det = _make_detection(db, photos[0]['id'])
    db.add_prediction(detection_id=det, species='Sparrow', confidence=0.9,
                      model='test-model', category='new')

    rules = [{'field': 'species', 'op': 'is', 'value': 'Robin'}]
    preds = db.get_predictions(rules=rules)

    assert [p['species'] for p in preds] == ['Sparrow'], (
        'row-level pass hid the disagreement prediction the filter selected'
    )


def test_get_predictions_none_group_mixes_metadata_and_prediction(app_and_db):
    """``none`` group over metadata + prediction leaves must not drop rows
    the SQL subquery already validated. Concretely,
    ``none(rating >= 5, prediction_confidence >= 0.8)`` selects photos with
    rating<5 whose predictions are all under 0.8; every returned row is
    valid. Treating ``rating >= 5`` as True per-row would make the ``none``
    False and drop all rows — the very rows the filter was designed to
    show."""
    _, db = app_and_db
    photos = db.get_photos()
    # Fixture: photos[0] has rating 3; give it two low-confidence preds.
    low_photo = photos[0]['id']
    det = _make_detection(db, low_photo)
    db.add_prediction(detection_id=det, species='A', confidence=0.10,
                      model='test-model', category='new')
    db.add_prediction(detection_id=det, species='B', confidence=0.05,
                      model='test-model', category='new')

    rules = {
        'mode': 'none',
        'rules': [
            {'field': 'rating', 'op': '>=', 'value': 5},
            {'field': 'prediction_confidence', 'op': '>=', 'value': 0.8},
        ],
    }
    preds = db.get_predictions(rules=rules)

    returned = sorted(p['species'] for p in preds if p['photo_id'] == low_photo)
    assert returned == ['A', 'B'], (
        'row-level pass dropped valid low-confidence rows because it '
        'shortcut the metadata leaf inside a `none` group'
    )


def test_get_predictions_all_group_still_narrows_by_prediction_confidence(app_and_db):
    """The row-level narrowing must still fire for pure ``all`` trees —
    e.g. ``all(rating >= 3, prediction_confidence >= 0.8)`` must hide the
    low-confidence sibling row on a rating-3 photo that also has one
    high-confidence prediction."""
    _, db = app_and_db
    photos = db.get_photos()
    # p1 already has rating 3 in the fixture.
    photo_id = photos[0]['id']
    det = _make_detection(db, photo_id)
    db.add_prediction(detection_id=det, species='High', confidence=0.95,
                      model='test-model', category='new')
    db.add_prediction(detection_id=det, species='Low', confidence=0.10,
                      model='test-model', category='new')

    rules = [
        {'field': 'rating', 'op': '>=', 'value': 3},
        {'field': 'prediction_confidence', 'op': '>=', 'value': 0.8},
    ]
    preds = db.get_predictions(rules=rules)

    returned = sorted(p['species'] for p in preds if p['photo_id'] == photo_id)
    assert returned == ['High'], (
        'row-level narrowing regressed for pure `all` trees; the low-'
        'confidence sibling row was not filtered out'
    )


def test_get_predictions_any_group_mixes_metadata_and_prediction(app_and_db):
    """``any(rating >= 5, prediction_confidence >= 0.8)`` on a rating-3
    photo with one 0.95 and one 0.10 prediction: the SQL subquery keeps
    the photo (the 0.95 sibling satisfies the OR at the photo level),
    but the row-level pass must still drop the 0.10 row — shortcutting
    ``rating >= 5`` to True inside the ``any`` group would let it through
    even though rating is 3.
    """
    _, db = app_and_db
    photos = db.get_photos()
    # p1 has rating 3 in the fixture.
    photo_id = photos[0]['id']
    det = _make_detection(db, photo_id)
    db.add_prediction(detection_id=det, species='High', confidence=0.95,
                      model='test-model', category='new')
    db.add_prediction(detection_id=det, species='Low', confidence=0.10,
                      model='test-model', category='new')

    rules = {
        'mode': 'any',
        'rules': [
            {'field': 'rating', 'op': '>=', 'value': 5},
            {'field': 'prediction_confidence', 'op': '>=', 'value': 0.8},
        ],
    }
    preds = db.get_predictions(rules=rules)
    returned = sorted(p['species'] for p in preds if p['photo_id'] == photo_id)
    assert returned == ['High'], (
        'row-level narrowing regressed for mixed `any` groups; the low-'
        'confidence sibling row leaked through because the metadata leaf '
        'was shortcut to True'
    )


def test_get_predictions_any_group_none_prediction_branch_broadens(app_and_db):
    """``any(none(prediction_confidence >= 0.8), rating >= 5)`` on a
    rating-3 photo with only a 0.10 prediction: the ``none(...)`` branch
    is TRUE per row for the 0.10 sibling, so the outer OR must let the
    row through even though ``rating >= 5`` is FALSE. Previously the
    relaxation stripped the prediction leaf and left ``none()`` empty,
    which compiled to no SQL clause under the ``any``; the photo was
    photo-scoped to just ``rating >= 5`` and dropped before the row
    filter could keep it (see r3619014565).
    """
    _, db = app_and_db
    photos = db.get_photos()
    # p1 has rating 3 in the fixture.
    photo_id = photos[0]['id']
    det = _make_detection(db, photo_id)
    db.add_prediction(detection_id=det, species='Low', confidence=0.10,
                      model='test-model', category='new')

    rules = {
        'mode': 'any',
        'rules': [
            {
                'mode': 'none',
                'rules': [
                    {'field': 'prediction_confidence', 'op': '>=', 'value': 0.8},
                ],
            },
            {'field': 'rating', 'op': '>=', 'value': 5},
        ],
    }
    preds = db.get_predictions(rules=rules)
    returned = sorted(p['species'] for p in preds if p['photo_id'] == photo_id)
    assert returned == ['Low'], (
        'emptied `none` branch under `any` was dropped from the SQL; the '
        'photo was scoped to just `rating >= 5` and the low-confidence row '
        'the outer OR should have surfaced never reached the row filter'
    )


def test_get_predictions_any_group_none_mixed_subgroup_broadens(app_and_db):
    """``any(none(all(rating >= 5, prediction_confidence >= 0.8)),
    rating >= 999)`` on a rating-3 photo with a 0.10 prediction: the
    inner ``all`` is FALSE per row (rating != 5), so ``none(...)`` is
    TRUE and the outer OR keeps the row. The relaxation drops the whole
    negated mixed subgroup, which must broaden the OR — not disappear
    under it — so the photo isn't scoped away by ``rating >= 999`` alone.
    """
    _, db = app_and_db
    photos = db.get_photos()
    photo_id = photos[0]['id']
    det = _make_detection(db, photo_id)
    db.add_prediction(detection_id=det, species='Low', confidence=0.10,
                      model='test-model', category='new')

    rules = {
        'mode': 'any',
        'rules': [
            {
                'mode': 'none',
                'rules': [
                    {
                        'mode': 'all',
                        'rules': [
                            {'field': 'rating', 'op': '>=', 'value': 5},
                            {'field': 'prediction_confidence', 'op': '>=', 'value': 0.8},
                        ],
                    },
                ],
            },
            {'field': 'rating', 'op': '>=', 'value': 999},
        ],
    }
    preds = db.get_predictions(rules=rules)
    returned = sorted(p['species'] for p in preds if p['photo_id'] == photo_id)
    assert returned == ['Low'], (
        'emptied mixed `none` subgroup under `any` was dropped from the '
        'SQL; the outer OR compiled to just the impossible `rating >= 999` '
        'clause and hid the row the outer expression matched at the row level'
    )


def test_get_predictions_status_is_not_keeps_pending_siblings(app_and_db):
    """``prediction_status is not Rejected`` on a photo with one pending
    and one rejected sibling must return the pending row. The previous
    SQL translated ``is not`` as a photo-level NOT EXISTS, dropping the
    entire photo the moment any sibling was Rejected — hiding the pending
    row the visible filter should have surfaced.
    """
    _, db = app_and_db
    photos = db.get_photos()
    photo_id = photos[0]['id']
    det = _make_detection(db, photo_id)
    # Two predictions on the same detection.
    db.add_prediction(detection_id=det, species='PendingPick', confidence=0.9,
                      model='test-model', category='new')
    db.add_prediction(detection_id=det, species='RejectedPick', confidence=0.5,
                      model='test-model', category='new')
    rejected = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id=? AND species='RejectedPick'",
        (det,),
    ).fetchone()
    db.update_prediction_status(rejected['id'], 'rejected')

    rules = [{'field': 'prediction_status', 'op': 'is not', 'value': 'rejected'}]
    preds = db.get_predictions(rules=rules)
    returned = sorted(p['species'] for p in preds if p['photo_id'] == photo_id)
    assert 'PendingPick' in returned, (
        'is-not translated to NOT EXISTS at the photo level and dropped '
        'the whole photo, hiding the pending sibling the filter should '
        'have kept'
    )
    assert 'RejectedPick' not in returned, (
        'row-level pass failed to drop the rejected sibling from the '
        'is-not result set'
    )


def test_get_predictions_status_not_in_keeps_pending_siblings(app_and_db):
    """Same sibling-visibility guarantee for ``not_in`` — the multi-value
    form must not use NOT EXISTS at the photo level and drop photos with
    a single Rejected sibling.
    """
    _, db = app_and_db
    photos = db.get_photos()
    photo_id = photos[0]['id']
    det = _make_detection(db, photo_id)
    db.add_prediction(detection_id=det, species='PendingPick', confidence=0.9,
                      model='test-model', category='new')
    db.add_prediction(detection_id=det, species='RejectedPick', confidence=0.5,
                      model='test-model', category='new')
    rejected = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id=? AND species='RejectedPick'",
        (det,),
    ).fetchone()
    db.update_prediction_status(rejected['id'], 'rejected')

    rules = [{'field': 'prediction_status', 'op': 'not_in',
              'value': ['rejected', 'accepted']}]
    preds = db.get_predictions(rules=rules)
    returned = sorted(p['species'] for p in preds if p['photo_id'] == photo_id)
    assert returned == ['PendingPick'], (
        'not_in translated to NOT EXISTS at the photo level and dropped '
        'the whole photo, hiding the pending sibling the filter should '
        'have kept'
    )


def test_get_predictions_classifier_model_is_not_keeps_other_model_siblings(app_and_db):
    """``classifier_model is not X`` on a photo with predictions from
    both model X and model Y must return the Y row. The previous SQL
    translated ``is not`` as a photo-level NOT EXISTS, dropping the entire
    photo the moment any sibling used model X — hiding the Y row the
    visible filter should have surfaced.
    """
    _, db = app_and_db
    photos = db.get_photos()
    photo_id = photos[0]['id']
    det = _make_detection(db, photo_id)
    db.add_prediction(detection_id=det, species='PickA', confidence=0.9,
                      model='model-a', category='new')
    db.add_prediction(detection_id=det, species='PickB', confidence=0.5,
                      model='model-b', category='new')

    rules = [{'field': 'classifier_model', 'op': 'is not', 'value': 'model-a'}]
    preds = db.get_predictions(rules=rules)
    returned = sorted(p['species'] for p in preds if p['photo_id'] == photo_id)
    assert 'PickB' in returned, (
        'is-not translated to NOT EXISTS at the photo level and dropped '
        'the whole photo, hiding the model-b sibling the filter should '
        'have kept'
    )
    assert 'PickA' not in returned, (
        'row-level pass failed to drop the model-a sibling from the '
        'is-not result set'
    )
