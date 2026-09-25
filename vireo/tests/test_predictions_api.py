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
    assert resp.get_json()['photo_ids'] == [photos[2]['id']]

    # Prediction status should be accepted (workspace-scoped via prediction_review)
    assert db.get_review_status(pred['id'], db._active_workspace_id) == 'accepted'

    # Species keyword should have been added to the photo
    keywords = db.get_photo_keywords(photos[2]['id'])
    kw_names = {k['name'] for k in keywords}
    assert 'Blue Jay' in kw_names


def _burst_pair(db, group_id, species, photo_indexes=(0, 1)):
    """Two photos of one burst carrying the same species from one model."""
    photos = db.get_photos()
    pred_ids = []
    for offset, index in enumerate(photo_indexes):
        det = _make_detection(db, photos[index]['id'])
        db.add_prediction(detection_id=det, species=species,
                          confidence=0.95 - 0.05 * offset, model='test-model',
                          category='new', group_id=group_id)
        pred_ids.append(db.conn.execute(
            "SELECT id FROM predictions WHERE detection_id = ?", (det,)
        ).fetchone()['id'])
    return [photos[i]['id'] for i in photo_indexes], pred_ids


def test_grouped_accept_names_every_row_it_decided(app_and_db):
    """The accept response lists the burst rows it settled, not just the URL's.

    ID Conflicts' batch accept loops this single-row route once per selected
    photo. Two selected photos of one burst are a single grouped decision on
    the server: the first request accepts both rows, and the second meets the
    terminal-status 409 for work that demonstrably landed. Without an explicit
    list of what the write covered, the client has no truthful way to tell
    that 409 apart from a row somebody else decided, so it reports an applied
    decision as "not applied" — a claim about the database that is false.

    The list has to come from the transaction that wrote it. Inferring it
    instead from an "already accepted" status echoed back in the 409 would
    call *any* prior accept this batch's own success, including one made in
    another tab under different scope.
    """
    app, db = app_and_db
    photo_ids, (pred_a, pred_b) = _burst_pair(db, 'gburst', 'Steller Jay')
    ws = db._active_workspace_id
    client = app.test_client()

    resp = client.post(f'/api/predictions/{pred_a}/accept')
    assert resp.status_code == 200
    assert sorted(resp.get_json()['prediction_ids']) == sorted([pred_a, pred_b])
    assert sorted(resp.get_json()['photo_ids']) == sorted(photo_ids)
    assert db.get_review_status(pred_b, ws) == 'accepted'

    # The request the loop would otherwise send next. It is a genuine refusal
    # — the row is settled — which is exactly why the client must skip it
    # rather than classify it after the fact.
    assert client.post(f'/api/predictions/{pred_b}/accept').status_code == 409


def test_grouped_replace_names_every_row_it_decided(app_and_db):
    """Replace expands through the burst too, so it reports the same list.

    Replace is the case that rules out reading idempotence off the 409's
    status: a replace answering "already accepted" may be sitting on a plain
    accept that never stripped the old species keywords, so treating it as
    success would report a replacement that never happened.
    """
    app, db = app_and_db
    _, (pred_a, pred_b) = _burst_pair(db, 'greplace', 'Scrub Jay')
    ws = db._active_workspace_id
    client = app.test_client()

    resp = client.post(f'/api/predictions/{pred_a}/replace-keywords')
    assert resp.status_code == 200
    assert sorted(resp.get_json()['prediction_ids']) == sorted([pred_a, pred_b])
    assert db.get_review_status(pred_b, ws) == 'accepted'


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
    photos = _seed_predictions(db)
    db.set_photo_edit_recipe(photos[0]["id"], {"rotation": 90})
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
    by_photo = {member["photo_id"]: member for member in data}
    assert by_photo[photos[0]["id"]]["edit_recipe"] == {
        "version": 1,
        "rotation": 90,
    }
    assert by_photo[photos[1]["id"]]["edit_recipe"] is None


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
    for sp in ('Sparrow', 'Finch'):
        row = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", (sp,)
        ).fetchone()
        db.update_prediction_status(row['id'], 'alternative')

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
    for sp in ('Sparrow', 'Finch'):
        row = db.conn.execute(
            "SELECT id FROM predictions WHERE species = ?", (sp,)
        ).fetchone()
        db.update_prediction_status(row['id'], 'alternative')

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


def test_get_predictions_needs_review_no_keeps_non_pending_siblings(app_and_db):
    """``Needs review is No`` on a photo with one pending and one rejected
    sibling must return the rejected row. The previous SQL translated the
    False case as a photo-level ``NOT EXISTS(pending)``, dropping the whole
    photo the moment any sibling was pending — hiding the non-pending row
    the visible filter should have surfaced (r3619118948).
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

    rules = [{'field': 'needs_review', 'op': 'is', 'value': False}]
    preds = db.get_predictions(rules=rules)
    returned = sorted(p['species'] for p in preds if p['photo_id'] == photo_id)
    assert 'RejectedPick' in returned, (
        'needs_review=false translated to NOT EXISTS(pending) at the photo '
        'level and dropped the whole photo, hiding the non-pending sibling '
        'the filter should have kept'
    )
    assert 'PendingPick' not in returned, (
        'row-level pass failed to drop the pending sibling from the '
        'needs_review=false result set'
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


def _one_prediction(db, species='Blue Jay', photo_index=2, model='test-model'):
    """Seed one ungrouped prediction and return (prediction_id, photo_id).

    Each call makes its own detection, so several predictions can share a
    photo without colliding — the fixture catalog is small.
    """
    photos = db.get_photos()
    photo_id = photos[photo_index % len(photos)]['id']
    det = _make_detection(db, photo_id)
    db.add_prediction(detection_id=det, species=species, confidence=0.90,
                      model=model, category='new', group_id=None)
    row = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND species = ?",
        (det, species),
    ).fetchone()
    return row['id'], photo_id


def test_decision_routes_leave_no_transaction_open(app_and_db):
    """Every path out of the lock releases it, including the refusals.

    ``BEGIN IMMEDIATE`` holds SQLite's single writer lock, so a 404 or a 409
    that returned without committing would stall every later decision served
    on the same connection. The successful accept after the refusals is the
    assertion that matters — without the release it would block until the
    30 s ``busy_timeout`` and come back 503.
    """
    app, db = app_and_db
    _seed_predictions(db)
    client = app.test_client()

    assert client.post('/api/predictions/999999/reject').status_code == 404
    # /accept answers 200 for a missing id (``accept_prediction`` returns None
    # and the route reports a no-op) — unchanged behaviour, exercised here
    # because it is another path that leaves the lock without writing.
    assert client.post('/api/predictions/999999/accept').status_code == 200

    settled, _ = _one_prediction(db, species='Woodhouse Scrub Jay')
    db.update_prediction_status(settled, 'reviewed')
    assert client.post(f'/api/predictions/{settled}/accept').status_code == 409

    fresh, photo_id = _one_prediction(db, species='Florida Scrub Jay',
                                      photo_index=3)
    resp = client.post(f'/api/predictions/{fresh}/accept')
    assert resp.status_code == 200
    kw_names = {k['name'] for k in db.get_photo_keywords(photo_id)}
    assert 'Florida Scrub Jay' in kw_names


def test_undo_of_an_accept_still_works_under_the_decision_lock(app_and_db):
    """Undo restores prediction status from inside the shared writer lock.

    ``/api/undo`` replays ``prediction_review`` statuses out of edit history,
    which makes it a decision route: it was the last of the unlocked ones, and
    wrapping it must not change what it does. Nothing to undo also has to
    release the lock, so the accept afterwards is part of the assertion.
    """
    app, db = app_and_db
    _seed_predictions(db)
    client = app.test_client()
    ws = db._active_workspace_id

    pred_id, photo_id = _one_prediction(db, species='Unicolored Jay')
    assert client.post(f'/api/predictions/{pred_id}/accept').status_code == 200
    assert db.get_review_status(pred_id, ws) == 'accepted'
    kw_names = {k['name'] for k in db.get_photo_keywords(photo_id)}
    assert 'Unicolored Jay' in kw_names

    assert client.post('/api/undo').status_code == 200
    assert db.get_review_status(pred_id, ws) == 'pending'
    kw_names = {k['name'] for k in db.get_photo_keywords(photo_id)}
    assert 'Unicolored Jay' not in kw_names

    assert client.post('/api/redo').status_code == 200
    assert db.get_review_status(pred_id, ws) == 'accepted'

    # "Nothing to redo" returns without writing; the lock must still be gone.
    assert client.post('/api/redo').status_code == 400
    second, _ = _one_prediction(db, species='White-throated Magpie-Jay',
                                photo_index=1)
    assert client.post(f'/api/predictions/{second}/accept').status_code == 200
    assert db.get_review_status(second, ws) == 'accepted'


def test_group_apply_records_decisions_under_the_lock(app_and_db):
    """Burst group apply writes review statuses, so it holds the lock too.

    Its status writes have no precondition of their own — that is precisely
    why it was easy to miss — but they still have to be serialized against the
    batch endpoints' check-then-write window rather than landing inside it.
    """
    app, db = app_and_db
    photos = db.get_photos()
    pick, reject = photos[0]['id'], photos[1]['id']
    det_a = _make_detection(db, pick)
    det_b = _make_detection(db, reject)
    db.add_prediction(detection_id=det_a, species='Azure Jay', confidence=0.9,
                      model='test-model', category='new', group_id='gapply')
    db.add_prediction(detection_id=det_b, species='Azure Jay', confidence=0.8,
                      model='test-model', category='new', group_id='gapply')
    pred_a = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?", (det_a,)
    ).fetchone()['id']
    pred_b = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?", (det_b,)
    ).fetchone()['id']
    ws = db._active_workspace_id

    client = app.test_client()
    resp = client.post('/api/predictions/group/apply', json={
        'picks': [pick], 'rejects': [reject], 'removed': [], 'species': '',
    })
    assert resp.status_code == 200
    assert db.get_review_status(pred_a, ws) == 'accepted'
    assert db.get_review_status(pred_b, ws) == 'rejected'

    # The lock was released, so a following decision goes through.
    fresh, _ = _one_prediction(db, species='Plush-crested Jay')
    assert client.post(f'/api/predictions/{fresh}/reject').status_code == 200


def test_group_apply_preserves_already_decided_predictions(app_and_db):
    """A stale group apply must not overwrite a decision the lock committed.

    The decision lock orders group apply against every other decision
    route, but ordering alone is not enough: ``_apply_group_decisions``
    used to unconditionally rewrite every ``prediction_review`` row for a
    picked/rejected photo, so a stale group-apply payload — the Review
    modal still has the photo in ``rejects`` while Browse's Accept for
    the same row commits first — would flip the earlier ``accepted`` to
    ``rejected`` under the lock. The species keyword the accept added
    stayed on the photo, so keyword state and review state contradicted
    each other with no UI cue pointing at the mismatch.

    Both directions are guarded: an earlier ``rejected`` (from Browse's
    single-row Reject) must survive a stale group-apply pick just as an
    earlier ``accepted`` must survive a stale group-apply reject. Same
    guard, same reason — the second writer never claims the first
    writer's row.

    What makes this payload *stale* rather than a deliberate re-decision
    is the baseline it carries: it says it was rendered while both rows
    were pending, and they are not pending any more. Nothing else in the
    request distinguishes the two cases — see
    ``test_group_apply_allows_a_deliberate_re_decision`` for the payload
    with the same shape that must go through.
    """
    app, db = app_and_db
    photos = db.get_photos()
    accepted_photo = photos[0]['id']
    rejected_photo = photos[1]['id']
    det_a = _make_detection(db, accepted_photo)
    det_b = _make_detection(db, rejected_photo)
    db.add_prediction(detection_id=det_a, species='Azure Jay', confidence=0.9,
                      model='test-model', category='new', group_id=None)
    db.add_prediction(detection_id=det_b, species='Azure Jay', confidence=0.8,
                      model='test-model', category='new', group_id=None)
    pred_a = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?", (det_a,)
    ).fetchone()['id']
    pred_b = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?", (det_b,)
    ).fetchone()['id']
    ws = db._active_workspace_id

    client = app.test_client()

    # First writers: single-row Accept on one photo, single-row Reject on
    # the other. Both commit through the decision lock and record their
    # keyword state on the photo.
    assert client.post(
        f'/api/predictions/{pred_a}/accept').status_code == 200
    assert client.post(
        f'/api/predictions/{pred_b}/reject').status_code == 200
    assert db.get_review_status(pred_a, ws) == 'accepted'
    assert db.get_review_status(pred_b, ws) == 'rejected'
    accepted_keywords_before = {
        k['name'] for k in db.get_photo_keywords(accepted_photo)}
    assert 'Azure Jay' in accepted_keywords_before

    # Stale group apply arrives with the mirror decisions: it asks to
    # reject the already-accepted photo and pick (accept) the already-
    # rejected photo. Without the guard both statuses flip.
    resp = client.post('/api/predictions/group/apply', json={
        'picks': [rejected_photo],
        'rejects': [accepted_photo],
        'removed': [],
        'species': 'Azure Jay',
        'observed': {str(pred_a): 'pending', str(pred_b): 'pending'},
    })
    assert resp.status_code == 200
    # Named, not just skipped: a group apply that quietly does less than
    # the modal promised is the same black box this PR exists to remove.
    assert resp.get_json()['already_decided'] == 2

    # The lock-committed decisions survive.
    assert db.get_review_status(pred_a, ws) == 'accepted'
    assert db.get_review_status(pred_b, ws) == 'rejected'

    # The keyword the accept added is still attached — same-side
    # confirmation that status and keyword did not drift apart.
    accepted_keywords_after = {
        k['name'] for k in db.get_photo_keywords(accepted_photo)}
    assert 'Azure Jay' in accepted_keywords_after

    # And the mirror: the stale *pick* on the already-rejected photo must
    # not tag it either. A guard that only defends the status write runs
    # after the keyword write, so it leaves the species attached to a row
    # that stays ``rejected`` — the same contradiction, entered from the
    # other side. Skipping the photo before any write is what closes it.
    rejected_keywords = {
        k['name'] for k in db.get_photo_keywords(rejected_photo)}
    assert 'Azure Jay' not in rejected_keywords
    assert (db.get_photo(rejected_photo)['flag'] or 'none') == 'none'

    # Rows that were still pending are still writable: a fresh prediction
    # on a third photo takes the group-apply status normally, so the
    # guard preserves committed decisions without freezing the row set.
    third_photo = photos[2]['id']
    det_c = _make_detection(db, third_photo)
    db.add_prediction(detection_id=det_c, species='Azure Jay',
                      confidence=0.7, model='test-model',
                      category='new', group_id=None)
    pred_c = db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ?", (det_c,)
    ).fetchone()['id']
    assert db.get_review_status(pred_c, ws) == 'pending'
    resp = client.post('/api/predictions/group/apply', json={
        'picks': [third_photo],
        'rejects': [],
        'removed': [],
        'species': '',
        'observed': {str(pred_c): 'pending'},
    })
    assert resp.status_code == 200
    assert resp.get_json()['already_decided'] == 0
    assert db.get_review_status(pred_c, ws) == 'accepted'


def _seed_burst_group(db, group_id='gbaseline'):
    """Two grouped predictions on two photos; returns (pred, photo) pairs."""
    photos = db.get_photos()
    out = []
    for photo in photos[:2]:
        det = _make_detection(db, photo['id'])
        db.add_prediction(detection_id=det, species='Azure Jay',
                          confidence=0.9, model='test-model',
                          category='new', group_id=group_id)
        pred_id = db.conn.execute(
            "SELECT id FROM predictions WHERE detection_id = ?", (det,)
        ).fetchone()['id']
        out.append((pred_id, photo['id']))
    return out


def test_group_apply_skips_photos_decided_since_the_modal_rendered(app_and_db):
    """A stale burst apply must not overwrite a decision made after it loaded.

    The shape: a prediction is rejected elsewhere while the burst modal still
    holds that photo in its picks. Serializing the two requests (which the
    decision lock already does) only orders them — an unconditional update
    still lands last, flipping the rejected row to ``accepted`` and tagging
    the photo with a species the user just dismissed. The modal sends the
    statuses it displayed, so the server can tell that this photo's meaning
    changed underneath it and leave the whole photo alone: no status write,
    no flag, no keyword.
    """
    app, db = app_and_db
    (pred_a, photo_a), (pred_b, photo_b) = _seed_burst_group(db)
    ws = db._active_workspace_id
    client = app.test_client()

    # A decision lands on photo A alone after the modal rendered. Written
    # directly because both the accept and reject routes expand a grouped row
    # to the whole burst — every member would then be stale, which is correct
    # but tests nothing about applying the untouched remainder.
    db.update_prediction_status(pred_a, 'rejected')
    assert db.get_review_status(pred_a, ws) == 'rejected'
    flag_before = db.get_photo(photo_a)['flag'] or 'none'

    resp = client.post('/api/predictions/group/apply', json={
        'picks': [photo_a],
        'rejects': [photo_b],
        'removed': [],
        'species': 'Azure Jay',
        # What the modal showed when it opened: both pending.
        'observed': {str(pred_a): 'pending', str(pred_b): 'pending'},
    })
    assert resp.status_code == 200
    assert resp.get_json()['already_decided'] == 1

    # A's completed decision survives, and nothing was written for it —
    # including the keyword, which is the half an under-the-lock status guard
    # alone would still have written.
    assert db.get_review_status(pred_a, ws) == 'rejected'
    assert (db.get_photo(photo_a)['flag'] or 'none') == flag_before
    assert 'Azure Jay' not in {k['name'] for k in db.get_photo_keywords(photo_a)}

    # The rest of the burst still applied.
    assert db.get_review_status(pred_b, ws) == 'rejected'
    assert db.get_photo(photo_b)['flag'] == 'rejected'


def test_group_apply_allows_a_deliberate_re_decision(app_and_db):
    """Re-opening an applied burst and changing the split still works.

    The reason group apply compares against a baseline instead of refusing
    every decided row the way the single-row endpoints do: the burst modal is
    reachable from any card carrying a ``group_id``, including one this same
    user just applied, and it re-derives picks/rejects from quality scores
    rather than from the stored statuses. "Skip anything decided" would make
    a second apply silently freeze the statuses while still moving the flags,
    and would describe the user's own decision as somebody else's.
    """
    app, db = app_and_db
    (pred_a, photo_a), (pred_b, photo_b) = _seed_burst_group(db, 'gredecide')
    ws = db._active_workspace_id
    client = app.test_client()

    first = client.post('/api/predictions/group/apply', json={
        'picks': [photo_a], 'rejects': [photo_b], 'removed': [],
        'species': 'Azure Jay',
        'observed': {str(pred_a): 'pending', str(pred_b): 'pending'},
    })
    assert first.status_code == 200
    assert first.get_json()['already_decided'] == 0
    assert db.get_review_status(pred_a, ws) == 'accepted'
    assert db.get_review_status(pred_b, ws) == 'rejected'

    # The modal is re-opened: it now displays the statuses just written, and
    # the user swaps the split.
    second = client.post('/api/predictions/group/apply', json={
        'picks': [photo_b], 'rejects': [photo_a], 'removed': [],
        'species': 'Azure Jay',
        'observed': {str(pred_a): 'accepted', str(pred_b): 'rejected'},
    })
    assert second.status_code == 200
    assert second.get_json()['already_decided'] == 0
    assert db.get_review_status(pred_a, ws) == 'rejected'
    assert db.get_review_status(pred_b, ws) == 'accepted'


def test_group_apply_writes_every_side_effect_in_one_transaction(
    app_and_db, monkeypatch,
):
    """A burst apply is one decision, so it is one transaction.

    This is the structural property that makes a single staleness check
    sufficient. The route used to check the baseline twice — once before the
    flag/keyword writes and once at the status write — with those writes
    committing on their own in between. A decision landing in that gap made
    the second check skip the status update for a photo whose flag, keyword,
    pending change and history rows had already committed: flagged and tagged
    with a species whose prediction reads rejected.

    The test drives the same boundary from the side that can be forced
    deterministically. A photo leaves the workspace part-way through the
    apply, which is the one mid-flight failure the route can hit for real
    (its pre-lock membership check runs before ``BEGIN IMMEDIATE``). Every
    write made for the *earlier* photo has to be gone. Under the old shape
    they were already committed and stayed — which is why "the flag and
    keyword writes are not review state" was never a safe reason to leave
    them outside.
    """
    from repositories.photo_review import PhotoReviewRepository

    app, db = app_and_db
    (pred_a, photo_a), (pred_b, photo_b) = _seed_burst_group(db, 'gatomic')
    ws = db._active_workspace_id
    client = app.test_client()

    # Photo B detaches from the workspace after the route's pre-lock check
    # passed — detected at the first write that touches it.
    original_verify = PhotoReviewRepository._verify_photo

    def detached_for_b(self, photo_id):
        if photo_id == photo_b:
            raise ValueError(
                f"Photo {photo_id} does not belong to the active workspace"
            )
        return original_verify(self, photo_id)

    monkeypatch.setattr(
        PhotoReviewRepository, "_verify_photo", detached_for_b,
    )

    history_before = len(db.get_edit_history(limit=100))
    resp = client.post('/api/predictions/group/apply', json={
        'picks': [photo_a, photo_b],
        'rejects': [],
        'removed': [],
        'species': 'Azure Jay',
        'observed': {str(pred_a): 'pending', str(pred_b): 'pending'},
    })
    assert resp.status_code == 403

    # Nothing was written for photo A, which the loop reached first.
    assert (db.get_photo(photo_a)['flag'] or 'none') == 'none'
    assert 'Azure Jay' not in {k['name'] for k in db.get_photo_keywords(photo_a)}
    assert db.get_review_status(pred_a, ws) == 'pending'
    assert not [
        c for c in db.get_pending_changes() if c['photo_id'] == photo_a
    ]
    assert len(db.get_edit_history(limit=100)) == history_before
    # Not even the keyword row: ``add_keyword`` runs inside the same
    # transaction, so a refused burst leaves no orphan species behind.
    assert db.conn.execute(
        "SELECT 1 FROM keywords WHERE name = 'Azure Jay'"
    ).fetchone() is None


def test_group_apply_treats_regenerated_predictions_as_stale(app_and_db):
    """Classification rerunning while the burst modal is open makes the photo stale.

    The bug: ``_stale_group_apply_photos`` checked only the observed row's
    status. If a classifier rerun inserted a new prediction row for the same
    ``(detection, classifier_model)`` while the modal was open, the observed
    row's status was still pending, so the check passed — and
    ``update_predictions_status_by_photo`` blindly wrote the modal's decision
    to *every* prediction on the photo, including the newly generated row the
    user never saw, tagging the photo with the modal's stale species.

    The fix: an observed row that is no longer the latest ``labels_fingerprint``
    for its ``(detection, classifier_model)`` invalidates its photo. The photo
    is dropped from the apply the same way status drift drops one.
    """
    app, db = app_and_db
    (pred_a, photo_a), (pred_b, photo_b) = _seed_burst_group(db, 'gregen')
    ws = db._active_workspace_id
    client = app.test_client()

    # Classifier reruns on photo A between render and click: a new prediction
    # row is inserted for the same detection with a newer labels_fingerprint.
    # The old row stays pending, so status-only staleness would clear.
    det_a = db.conn.execute(
        "SELECT detection_id FROM predictions WHERE id = ?", (pred_a,)
    ).fetchone()['detection_id']
    db.add_prediction(
        detection_id=det_a, species='Verdin', confidence=0.7,
        model='test-model', category='new',
        labels_fingerprint='rerun-v2',
    )
    new_pred_a = db.conn.execute(
        """SELECT id FROM predictions
             WHERE detection_id = ? AND classifier_model = ? AND species = ?""",
        (det_a, 'test-model', 'Verdin'),
    ).fetchone()['id']
    assert new_pred_a != pred_a
    flag_before = db.get_photo(photo_a)['flag'] or 'none'

    resp = client.post('/api/predictions/group/apply', json={
        'picks': [photo_a],
        'rejects': [photo_b],
        'removed': [],
        'species': 'Azure Jay',
        # What the modal showed when it opened: both pending, only the *old*
        # predictions listed. The new row is not observed.
        'observed': {str(pred_a): 'pending', str(pred_b): 'pending'},
    })
    assert resp.status_code == 200
    assert resp.get_json()['already_decided'] == 1

    # Nothing was written for photo A — including the new row that the write
    # would have blindly touched — and the modal's species did not attach.
    assert db.get_review_status(pred_a, ws) == 'pending'
    assert db.get_review_status(new_pred_a, ws) == 'pending'
    assert (db.get_photo(photo_a)['flag'] or 'none') == flag_before
    assert 'Azure Jay' not in {k['name'] for k in db.get_photo_keywords(photo_a)}

    # Photo B, whose detections were untouched, still applies.
    assert db.get_review_status(pred_b, ws) == 'rejected'
    assert db.get_photo(photo_b)['flag'] == 'rejected'


def test_group_apply_rejects_a_malformed_baseline(app_and_db):
    """``observed`` is validated rather than silently ignored.

    A baseline the server cannot read is not the same as no baseline: quietly
    dropping it would turn every write back into the unconditional overwrite
    this precondition exists to prevent, with nothing on screen to say so.
    """
    app, db = app_and_db
    (pred_a, photo_a), _ = _seed_burst_group(db, 'gbadbaseline')
    client = app.test_client()

    for bad in (['pending'], {'not-an-id': 'pending'}, {'1': 7}):
        resp = client.post('/api/predictions/group/apply', json={
            'picks': [photo_a], 'rejects': [], 'removed': [], 'species': '',
            'observed': bad,
        })
        assert resp.status_code == 400, bad


def test_group_apply_client_sends_the_observed_baseline(app_and_db):
    """Review's burst modal must send what it displayed.

    The server can only refuse a write the client claims to have seen a
    status for, so an omitted ``observed`` map turns the check off for the
    one caller that has one. Asserted against the rendered page for the same
    reason ``test_browse_panel_treats_reviewed_status_as_decided`` is: the
    guarantee is a property of the pair, and a server-side test alone would
    stay green while the modal quietly stopped participating.
    """
    app, _ = app_and_db
    html = app.test_client().get('/review').get_data(as_text=True)
    assert "observed[it.id] = it.status || 'pending';" in html, (
        "the burst modal must build its baseline from the statuses it "
        "displayed"
    )
    assert 'observed: observed,' in html, (
        "the burst modal must send its baseline to /api/predictions/group/apply"
    )


def test_reject_covers_the_whole_burst_like_accept(app_and_db):
    """"Not X" on a burst card rejects every undecided member, not just the lead.

    Accept expands across the burst group, so the card reads "Tag N photos as
    X". Reject used to write only the lead row: the card then showed
    "Rejected" while the other members stayed pending and hidden behind it,
    where a later Accept All tagged them X anyway.
    """
    app, db = app_and_db
    _, (pred_a, pred_b) = _burst_pair(db, 'grejectburst', 'Pinyon Jay')
    ws = db._active_workspace_id
    client = app.test_client()

    resp = client.post(f'/api/predictions/{pred_a}/reject')
    assert resp.status_code == 200
    assert sorted(resp.get_json()['rejected_prediction_ids']) == sorted(
        [pred_a, pred_b])
    assert db.get_review_status(pred_a, ws) == 'rejected'
    assert db.get_review_status(pred_b, ws) == 'rejected'


def test_reject_expansion_leaves_decided_burst_members_alone(app_and_db):
    """Expansion only picks up undecided members, as accept's does."""
    app, db = app_and_db
    _, (pred_a, pred_b) = _burst_pair(db, 'grejectdecided', 'Gray Jay')
    ws = db._active_workspace_id
    db.update_prediction_status(pred_b, 'reviewed')
    client = app.test_client()

    resp = client.post(f'/api/predictions/{pred_a}/reject')
    assert resp.status_code == 200
    assert resp.get_json()['rejected_prediction_ids'] == [pred_a]
    assert db.get_review_status(pred_b, ws) == 'reviewed'


def test_group_apply_pick_accepts_only_its_member_and_undo_restores_it(
    app_and_db,
):
    """A burst pick accepts the member row, not the runner-up species.

    ``update_predictions_status_by_photo(pid, 'accepted')`` used to mark every
    prediction on the picked photo accepted, including the ``alternative``
    rows the modal only listed as runners-up. None of those status writes were
    in edit history either, so undo removed the species keyword but left the
    rows accepted, and a later single Accept of the photo answered 409.
    """
    app, db = app_and_db
    photos = db.get_photos()
    pick, reject = photos[0]['id'], photos[1]['id']
    det_pick = _make_detection(db, pick)
    det_reject = _make_detection(db, reject)
    db.add_prediction(detection_id=det_pick, species='Canada Jay',
                      confidence=0.9, model='test-model', category='new',
                      group_id='galt')
    db.add_prediction(detection_id=det_pick, species='Blue Jay',
                      confidence=0.1, model='test-model', category='new',
                      group_id=None)
    db.add_prediction(detection_id=det_reject, species='Canada Jay',
                      confidence=0.8, model='test-model', category='new',
                      group_id='galt')

    def _pred(det, species):
        return db.conn.execute(
            "SELECT id FROM predictions WHERE detection_id = ? AND species = ?",
            (det, species),
        ).fetchone()['id']

    member = _pred(det_pick, 'Canada Jay')
    runner_up = _pred(det_pick, 'Blue Jay')
    reject_member = _pred(det_reject, 'Canada Jay')
    db.update_prediction_status(runner_up, 'alternative')
    ws = db._active_workspace_id
    client = app.test_client()

    resp = client.post('/api/predictions/group/apply', json={
        'picks': [pick], 'rejects': [reject], 'removed': [],
        'species': 'Canada Jay',
        'observed': {str(member): 'pending', str(reject_member): 'pending'},
    })
    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert db.get_review_status(member, ws) == 'accepted'
    assert db.get_review_status(runner_up, ws) == 'rejected'
    assert 'Canada Jay' in {k['name'] for k in db.get_photo_keywords(pick)}

    # Undo reverses the keyword and the pick's statuses together.
    assert client.post('/api/undo').status_code == 200
    assert 'Canada Jay' not in {k['name'] for k in db.get_photo_keywords(pick)}
    assert db.get_review_status(member, ws) == 'pending'
    assert db.get_review_status(runner_up, ws) == 'alternative'

    # The photo is reviewable again rather than stuck behind a 409.
    assert client.post(f'/api/predictions/{member}/accept').status_code == 200


def test_group_apply_without_species_is_undoable(app_and_db):
    """A species-less pick records a no-tag accept that undo can replay."""
    app, db = app_and_db
    (pred_a, pick), (_pred_b, reject) = _seed_burst_group(db, 'gnospecies')
    ws = db._active_workspace_id
    client = app.test_client()

    resp = client.post('/api/predictions/group/apply', json={
        'picks': [pick], 'rejects': [reject], 'removed': [], 'species': '',
    })
    assert resp.status_code == 200
    assert db.get_review_status(pred_a, ws) == 'accepted'

    assert client.post('/api/undo').status_code == 200
    assert db.get_review_status(pred_a, ws) == 'pending'
    assert client.post('/api/redo').status_code == 200
    assert db.get_review_status(pred_a, ws) == 'accepted'


def test_undo_replay_is_one_transaction(app_and_db, monkeypatch):
    """Undo holds the caller's transaction for the whole replay.

    ``/api/undo`` runs under the prediction-decision lock (``BEGIN
    IMMEDIATE``), but the replay's per-row setters each committed, which
    ended that transaction after the first write and let another decision
    interleave with the rest. Commits are now held until the replay
    finishes, and a failure part-way rolls every step back.
    """
    app, db = app_and_db
    client = app.test_client()
    ws = db._active_workspace_id
    pred_a, _ = _one_prediction(db, species='Brown Jay', photo_index=0)
    pred_b, _ = _one_prediction(db, species='Brown Jay', photo_index=1)
    resp = client.post('/api/predictions/batch-accept', json={
        'prediction_ids': [pred_a, pred_b],
    })
    assert resp.status_code == 200, resp.get_data(as_text=True)
    assert db.get_review_status(pred_a, ws) == 'accepted'
    assert db.get_review_status(pred_b, ws) == 'accepted'

    original = type(db)._undo_prediction_accept_statuses
    seen_in_transaction = []

    def spying(self, old_meta, old_val):
        result = original(self, old_meta, old_val)
        seen_in_transaction.append(self.conn.in_transaction)
        return result

    monkeypatch.setattr(type(db), '_undo_prediction_accept_statuses', spying)
    db.conn.execute('BEGIN IMMEDIATE')
    db.undo_last_edit()
    assert seen_in_transaction and all(seen_in_transaction), (
        'a replay step committed and ended the transaction early'
    )
    assert not db.conn.in_transaction
    assert db.get_review_status(pred_a, ws) == 'pending'
    assert db.get_review_status(pred_b, ws) == 'pending'

    # A failure after the first photo's replay rolls the whole undo back.
    db.redo_last_undo()
    assert db.get_review_status(pred_a, ws) == 'accepted'
    assert db.get_review_status(pred_b, ws) == 'accepted'
    real_untag = type(db)._untag_for_edit
    untagged = []

    def failing_untag(self, pid, kid):
        if untagged:
            raise RuntimeError('replay failed part-way')
        untagged.append(pid)
        return real_untag(self, pid, kid)

    monkeypatch.setattr(type(db), '_untag_for_edit', failing_untag)
    try:
        db.undo_last_edit()
    except RuntimeError:
        pass
    else:
        raise AssertionError('the injected failure did not fire')
    assert db.get_review_status(pred_a, ws) == 'accepted'
    assert db.get_review_status(pred_b, ws) == 'accepted'
