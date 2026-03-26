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
