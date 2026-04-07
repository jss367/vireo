def test_set_color_label(app_and_db):
    """POST /api/photos/<id>/color_label sets the color label."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/color_label', json={'color': 'red'})
    assert resp.status_code == 200
    assert db.get_color_label(pid) == 'red'


def test_remove_color_label(app_and_db):
    """POST /api/photos/<id>/color_label with null removes the label."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    client.post(f'/api/photos/{pid}/color_label', json={'color': 'blue'})
    resp = client.post(f'/api/photos/{pid}/color_label', json={'color': None})
    assert resp.status_code == 200
    assert db.get_color_label(pid) is None


def test_set_color_label_invalid(app_and_db):
    """POST /api/photos/<id>/color_label rejects invalid colors."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/color_label', json={'color': 'orange'})
    assert resp.status_code == 400


def test_batch_color_label(app_and_db):
    """POST /api/batch/color_label sets labels on multiple photos."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:2]]

    resp = client.post('/api/batch/color_label', json={'photo_ids': pids, 'color': 'green'})
    assert resp.status_code == 200
    assert db.get_color_label(pids[0]) == 'green'
    assert db.get_color_label(pids[1]) == 'green'


def test_set_rating(app_and_db):
    """POST /api/photos/<id>/rating updates rating and queues pending change."""
    app, db = app_and_db
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


def test_undo_noop_rating_edit_preserves_earlier_pending_change(app_and_db):
    """Undoing a repeated same-value rating edit should not clear the earlier pending sync."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/rating', json={'rating': 4})
    assert resp.status_code == 200

    resp = client.post(f'/api/photos/{pid}/rating', json={'rating': 4})
    assert resp.status_code == 200

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    photo = db.get_photo(pid)
    assert photo['rating'] == 4

    changes = db.get_pending_changes()
    rating_changes = [c for c in changes if c['photo_id'] == pid and c['change_type'] == 'rating']
    assert len(rating_changes) == 1
    assert rating_changes[0]['value'] == '4'


def test_undo_old_rating_action_does_not_clear_new_pending_change_reusing_id(app_and_db):
    """Undo must not delete unrelated pending work even if an old row id is reused."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/rating', json={'rating': 4})
    assert resp.status_code == 200

    old_change = next(
        c for c in db.get_pending_changes()
        if c['photo_id'] == pid and c['change_type'] == 'rating' and c['value'] == '4'
    )
    db.clear_pending([old_change['id']])

    db.conn.execute(
        """INSERT INTO pending_changes (id, photo_id, change_type, value, change_token, workspace_id)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (old_change['id'], pid, 'keyword_add', 'Woodpecker', 'replacement-token', db._ws_id()),
    )
    db.conn.commit()

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    changes = db.get_pending_changes()
    assert any(
        c['id'] == old_change['id']
        and c['change_type'] == 'keyword_add'
        and c['value'] == 'Woodpecker'
        for c in changes
    )


def test_set_flag(app_and_db):
    """POST /api/photos/<id>/flag updates the local flag without queuing XMP sync."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    resp = client.post(f'/api/photos/{pid}/flag',
                       json={'flag': 'flagged'})
    assert resp.status_code == 200

    photo = db.get_photo(pid)
    assert photo['flag'] == 'flagged'

    changes = db.get_pending_changes()
    assert not any(c['photo_id'] == pid and c['change_type'] == 'flag' for c in changes)


def test_add_keyword_to_photo(app_and_db):
    """POST /api/photos/<id>/keywords adds keyword and queues pending change."""
    app, db = app_and_db
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


def test_remove_keyword_from_photo(app_and_db):
    """DELETE /api/photos/<id>/keywords/<kid> removes keyword and queues pending change."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    keywords = db.get_photo_keywords(pid)
    kid = keywords[0]['id']

    resp = client.delete(f'/api/photos/{pid}/keywords/{kid}')
    assert resp.status_code == 200

    keywords = db.get_photo_keywords(pid)
    assert len(keywords) == 0

    changes = db.get_pending_changes()
    assert any(c['photo_id'] == pid and c['change_type'] == 'keyword_remove' for c in changes)


def test_undo_keyword_remove_clears_pending_change(app_and_db):
    """Undoing a keyword removal restores the tag and removes the pending delete."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    keywords = db.get_photo_keywords(pid)
    kid = keywords[0]['id']
    kw_name = keywords[0]['name']

    resp = client.delete(f'/api/photos/{pid}/keywords/{kid}')
    assert resp.status_code == 200

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    keywords = db.get_photo_keywords(pid)
    assert {k['name'] for k in keywords} == {kw_name}

    changes = db.get_pending_changes()
    assert not any(
        c['photo_id'] == pid and c['change_type'] == 'keyword_remove' and c['value'] == kw_name
        for c in changes
    )


def test_readding_removed_keyword_cancels_pending_remove(app_and_db):
    """Removing and re-adding the same keyword before sync leaves no pending keyword change."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    keywords = db.get_photo_keywords(pid)
    kid = keywords[0]['id']
    kw_name = keywords[0]['name']

    resp = client.delete(f'/api/photos/{pid}/keywords/{kid}')
    assert resp.status_code == 200

    resp = client.post(f'/api/photos/{pid}/keywords', json={'name': kw_name})
    assert resp.status_code == 200

    changes = db.get_pending_changes()
    assert not any(c['photo_id'] == pid and c['value'] == kw_name for c in changes)


def test_sync_status(app_and_db):
    """GET /api/sync/status returns pending count."""
    app, db = app_and_db
    client = app.test_client()

    resp = client.get('/api/sync/status')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data['pending_count'] == 0

    photos = db.get_photos()
    db.queue_change(photos[0]['id'], 'rating', '3')

    resp = client.get('/api/sync/status')
    data = resp.get_json()
    assert data['pending_count'] == 1


def test_edit_history_recorded_on_rating(app_and_db):
    """Setting a rating records an entry in edit_history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    client.post(f'/api/photos/{pid}/rating', json={'rating': 5})

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'rating'
    assert 'rating' in history[0]['description'].lower()


def test_edit_history_recorded_on_flag(app_and_db):
    """Setting a flag records an entry in edit_history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    client.post(f'/api/photos/{pid}/flag', json={'flag': 'flagged'})

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'flag'


def test_edit_history_recorded_on_keyword_add(app_and_db):
    """Adding a keyword records an entry in edit_history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    client.post(f'/api/photos/{pid}/keywords', json={'name': 'Eagle'})

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'keyword_add'


def test_edit_history_recorded_on_keyword_remove(app_and_db):
    """Removing a keyword records an entry in edit_history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']
    keywords = db.get_photo_keywords(pid)
    kid = keywords[0]['id']

    client.delete(f'/api/photos/{pid}/keywords/{kid}')

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'keyword_remove'


def test_edit_history_recorded_on_batch_rating(app_and_db):
    """Batch rating records a single grouped entry in edit_history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:2]]

    client.post('/api/batch/rating', json={'photo_ids': pids, 'rating': 4})

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['is_batch'] == 1
    assert history[0]['item_count'] == 2


def test_undo_api_uses_db(app_and_db):
    """POST /api/undo restores from DB-backed edit history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']
    original_rating = photos[0]['rating']

    client.post(f'/api/photos/{pid}/rating', json={'rating': 5})
    assert db.get_photo(pid)['rating'] == 5

    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert db.get_photo(pid)['rating'] == original_rating
    assert len(db.get_edit_history()) == 0


def test_undo_status_uses_db(app_and_db):
    """GET /api/undo/status reflects DB state."""
    app, db = app_and_db
    client = app.test_client()

    resp = client.get('/api/undo/status')
    assert resp.get_json()['available'] is False

    photos = db.get_photos()
    client.post(f'/api/photos/{photos[0]["id"]}/rating', json={'rating': 5})

    resp = client.get('/api/undo/status')
    data = resp.get_json()
    assert data['available'] is True
    assert data['count'] == 1


def test_edit_history_api(app_and_db):
    """GET /api/edit-history returns paginated history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    client.post(f'/api/photos/{pid}/rating', json={'rating': 1})
    client.post(f'/api/photos/{pid}/rating', json={'rating': 2})

    resp = client.get('/api/edit-history')
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 2
    assert data[0]['new_value'] == '2'  # most recent first


# -- History tracking for predictions, culling, labeling, species, discard --


def test_accept_prediction_records_history(app_and_db):
    """Accepting a prediction records prediction_accept in edit history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Blue Jay', 0.95, 'test-model')
    preds = db.get_predictions(photo_ids=[pid])
    pred_id = preds[0]['id']

    resp = client.post(f'/api/predictions/{pred_id}/accept')
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'prediction_accept'
    assert 'Blue Jay' in history[0]['description']


def test_accept_prediction_undo_restores_status(app_and_db):
    """Undoing an accepted prediction restores keyword, pending changes, and prediction status."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    det_ids = db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Blue Jay', 0.95, 'test-model')
    pred = db.get_predictions(photo_ids=[pid])[0]
    pred_id = pred['id']

    # Accept
    resp = client.post(f'/api/predictions/{pred_id}/accept')
    assert resp.status_code == 200

    # Verify accepted state
    pred_row = db.conn.execute("SELECT status FROM predictions WHERE id = ?", (pred_id,)).fetchone()
    assert pred_row['status'] == 'accepted'
    kws = {k['name'] for k in db.get_photo_keywords(pid)}
    assert 'Blue Jay' in kws

    # Undo
    resp = client.post('/api/undo')
    assert resp.status_code == 200

    # Prediction status restored to pending
    pred_row = db.conn.execute("SELECT status FROM predictions WHERE id = ?", (pred_id,)).fetchone()
    assert pred_row['status'] == 'pending'

    # Keyword removed
    kws = {k['name'] for k in db.get_photo_keywords(pid)}
    assert 'Blue Jay' not in kws

    # Pending keyword change removed
    changes = db.get_pending_changes()
    assert not any(c['change_type'] == 'keyword_add' and c['value'] == 'Blue Jay' for c in changes)


def test_reject_prediction_records_history(app_and_db):
    """Rejecting a prediction records prediction_reject in edit history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    det_ids = db.save_detections(pid, [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'House Sparrow', 0.60, 'test-model')
    preds = db.get_predictions(photo_ids=[pid])
    pred_id = preds[0]['id']

    resp = client.post(f'/api/predictions/{pred_id}/reject')
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'prediction_reject'
    assert 'House Sparrow' in history[0]['description']


def test_prediction_group_apply_records_history(app_and_db):
    """Group apply records separate flag and keyword_add history entries."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:3]]

    resp = client.post('/api/predictions/group/apply',
                       json={'picks': [pids[0], pids[1]],
                             'rejects': [pids[2]],
                             'species': 'Northern Cardinal'})
    assert resp.status_code == 200

    history = db.get_edit_history()
    action_types = {h['action_type'] for h in history}
    assert 'keyword_add' in action_types
    assert 'flag' in action_types
    assert len(history) == 2


def test_culling_apply_records_history(app_and_db):
    """Culling apply records flag changes in edit history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:3]]

    resp = client.post('/api/culling/apply',
                       json={'keepers': [pids[0]], 'rejects': [pids[1], pids[2]]})
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'flag'
    assert history[0]['is_batch'] == 1
    assert history[0]['item_count'] == 3


def test_culling_apply_undo_restores_flags(app_and_db):
    """Undoing culling restores original flag values."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']
    original_flag = photos[0]['flag'] or 'none'

    client.post('/api/culling/apply', json={'keepers': [pid], 'rejects': []})
    assert db.get_photo(pid)['flag'] == 'flagged'

    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert (db.get_photo(pid)['flag'] or 'none') == original_flag


def test_label_cluster_records_history(app_and_db):
    """Label cluster records keyword_add in edit history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:2]]

    resp = client.post('/api/species/label-cluster',
                       json={'photo_ids': pids, 'label': 'juvenile'})
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'keyword_add'
    assert 'juvenile' in history[0]['description']
    assert history[0]['item_count'] == 2


def test_encounter_species_records_history(app_and_db):
    """Confirming encounter species records keyword_add in edit history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:2]]

    resp = client.post('/api/encounters/species',
                       json={'species': 'Red-tailed Hawk', 'photo_ids': pids})
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'keyword_add'
    assert 'Red-tailed Hawk' in history[0]['description']


def test_sync_discard_records_history(app_and_db):
    """Discarding pending changes records discard in edit history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    db.queue_change(pid, 'rating', '5')
    changes = db.get_pending_changes()
    change_ids = [c['id'] for c in changes]

    resp = client.post('/api/sync/discard', json={'change_ids': change_ids})
    assert resp.status_code == 200

    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'discard'
    assert db.get_pending_changes() == []


def test_undo_skips_non_undoable_entries(app_and_db):
    """Undo skips prediction_reject and discard entries to reach real undoable edits."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    # Create an undoable edit (rating change)
    original_rating = db.get_photo(pid)['rating']
    client.post(f'/api/photos/{pid}/rating', json={'rating': 5})
    assert db.get_photo(pid)['rating'] == 5

    # Create a non-undoable entry (reject prediction)
    det_ids = db.save_detections(pid, [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'House Sparrow', 0.60, 'test-model')
    preds = db.get_predictions(photo_ids=[pid])
    client.post(f'/api/predictions/{preds[-1]["id"]}/reject')

    # History has 2 entries: prediction_reject (most recent) and rating
    history = db.get_edit_history()
    assert len(history) == 2

    # Undo should skip the prediction_reject and undo the rating
    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert db.get_photo(pid)['rating'] == original_rating

    # prediction_reject entry still in history, rating entry removed
    history = db.get_edit_history()
    assert len(history) == 1
    assert history[0]['action_type'] == 'prediction_reject'


def test_undo_status_skips_non_undoable(app_and_db):
    """Undo status reports the next undoable entry, not a non-undoable one."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    # Create an undoable edit
    client.post(f'/api/photos/{pid}/rating', json={'rating': 5})

    # Create a non-undoable entry on top
    det_ids = db.save_detections(pid, [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Crow', 0.50, 'test-model')
    preds = db.get_predictions(photo_ids=[pid])
    client.post(f'/api/predictions/{preds[-1]["id"]}/reject')

    # Undo status should show the rating edit, not the reject
    resp = client.get('/api/undo/status')
    data = resp.get_json()
    assert data['available'] is True
    assert 'rating' in data['description'].lower()
    assert data['count'] == 1  # only 1 undoable entry


def test_undo_nothing_when_only_non_undoable(app_and_db):
    """Undo returns error when only non-undoable entries exist."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    # Only non-undoable entries
    det_ids = db.save_detections(pid, [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}], detector_model="MDV6")
    db.add_prediction(det_ids[0], 'Robin', 0.70, 'test-model')
    preds = db.get_predictions(photo_ids=[pid])
    client.post(f'/api/predictions/{preds[0]["id"]}/reject')

    resp = client.post('/api/undo')
    assert resp.status_code == 400  # "nothing to undo"

    resp = client.get('/api/undo/status')
    assert resp.get_json()['available'] is False


# -- Undo coverage for individual action types --


def test_undo_flag_restores_original(app_and_db):
    """Undoing a flag change restores the photo's original flag value."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']
    original_flag = photos[0]['flag'] or 'none'

    client.post(f'/api/photos/{pid}/flag', json={'flag': 'flagged'})
    assert db.get_photo(pid)['flag'] == 'flagged'

    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert (db.get_photo(pid)['flag'] or 'none') == original_flag


def test_undo_keyword_add_removes_keyword(app_and_db):
    """Undoing a keyword addition removes the keyword and clears pending change."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    client.post(f'/api/photos/{pid}/keywords', json={'name': 'Heron'})
    kw_names = {k['name'] for k in db.get_photo_keywords(pid)}
    assert 'Heron' in kw_names

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    kw_names = {k['name'] for k in db.get_photo_keywords(pid)}
    assert 'Heron' not in kw_names

    changes = db.get_pending_changes()
    assert not any(c['change_type'] == 'keyword_add' and c['value'] == 'Heron' for c in changes)


# -- Undo coverage for batch operations --


def test_undo_batch_rating_restores_all_photos(app_and_db):
    """Undoing a batch rating restores each photo's original rating."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:3]]
    originals = {p['id']: p['rating'] for p in photos[:3]}

    client.post('/api/batch/rating', json={'photo_ids': pids, 'rating': 1})
    for pid in pids:
        assert db.get_photo(pid)['rating'] == 1

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    for pid in pids:
        assert db.get_photo(pid)['rating'] == originals[pid]


def test_undo_batch_flag_restores_all_photos(app_and_db):
    """Undoing a batch flag restores each photo's original flag."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:3]]
    originals = {p['id']: (p['flag'] or 'none') for p in photos[:3]}

    client.post('/api/batch/flag', json={'photo_ids': pids, 'flag': 'rejected'})
    for pid in pids:
        assert db.get_photo(pid)['flag'] == 'rejected'

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    for pid in pids:
        assert (db.get_photo(pid)['flag'] or 'none') == originals[pid]


def test_undo_batch_keyword_add_removes_from_all_photos(app_and_db):
    """Undoing a batch keyword add removes the keyword from every photo."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pids = [p['id'] for p in photos[:3]]

    client.post('/api/batch/keyword', json={'photo_ids': pids, 'name': 'Owl'})
    for pid in pids:
        assert 'Owl' in {k['name'] for k in db.get_photo_keywords(pid)}

    resp = client.post('/api/undo')
    assert resp.status_code == 200

    for pid in pids:
        assert 'Owl' not in {k['name'] for k in db.get_photo_keywords(pid)}

    changes = db.get_pending_changes()
    assert not any(c['change_type'] == 'keyword_add' and c['value'] == 'Owl' for c in changes)


# -- Sequential undo --


def test_multiple_sequential_undos(app_and_db):
    """Multiple undos in sequence each reverse the correct action."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']
    original_rating = photos[0]['rating']
    original_flag = photos[0]['flag'] or 'none'

    # Action 1: change rating
    client.post(f'/api/photos/{pid}/rating', json={'rating': 2})
    # Action 2: change flag
    client.post(f'/api/photos/{pid}/flag', json={'flag': 'rejected'})
    # Action 3: add keyword
    client.post(f'/api/photos/{pid}/keywords', json={'name': 'Finch'})

    assert len(db.get_edit_history()) == 3

    # Undo 3: keyword add reversed
    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert 'Finch' not in {k['name'] for k in db.get_photo_keywords(pid)}

    # Undo 2: flag reversed
    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert (db.get_photo(pid)['flag'] or 'none') == original_flag

    # Undo 1: rating reversed
    resp = client.post('/api/undo')
    assert resp.status_code == 200
    assert db.get_photo(pid)['rating'] == original_rating

    # Nothing left
    resp = client.post('/api/undo')
    assert resp.status_code == 400


# -- Pruning --


def test_history_pruning_respects_max(app_and_db):
    """Old history entries are pruned when exceeding max_edit_history."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    import config as cfg
    cfg.set('max_edit_history', 3)

    # Create 5 edits — only the newest 3 should survive
    for r in range(5):
        client.post(f'/api/photos/{pid}/rating', json={'rating': r})

    history = db.get_edit_history(limit=100)
    assert len(history) == 3
    # Most recent should be the last rating set
    assert history[0]['new_value'] == '4'


# -- Workspace isolation --


def test_history_isolated_between_workspaces(app_and_db):
    """History in one workspace is invisible to another; undo doesn't cross workspaces."""
    app, db = app_and_db
    client = app.test_client()
    photos = db.get_photos()
    pid = photos[0]['id']

    # Record an edit in the default workspace
    client.post(f'/api/photos/{pid}/rating', json={'rating': 5})
    assert len(db.get_edit_history()) == 1

    # Create and switch to a new workspace
    ws2 = db.create_workspace('Second')
    db.set_active_workspace(ws2)

    # New workspace has no history
    assert len(db.get_edit_history()) == 0

    # Undo in new workspace finds nothing
    result = db.undo_last_edit()
    assert result is None

    # Original workspace still has its history
    ws1 = db.conn.execute("SELECT id FROM workspaces WHERE name = 'Default'").fetchone()['id']
    db.set_active_workspace(ws1)
    assert len(db.get_edit_history()) == 1
