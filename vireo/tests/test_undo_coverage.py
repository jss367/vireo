"""Round-trip the reversible decisions that used to be audit-only."""

import sqlite3

import pytest


def _location_ids(db, pid):
    return {k['id'] for k in db.keywords.get_for_photo(pid) if k['type'] == 'location'}


@pytest.mark.parametrize('route', ['saved', 'text', 'place', 'clear', 'batch-saved', 'batch-text', 'batch-place'])
def test_location_history_restores_mixed_originals_and_provenance(app_and_db, route):
    app, db = app_and_db
    client = app.test_client()
    pids = [p['id'] for p in db.photos.list_page()][:2]
    old_ids = [db.get_or_create_text_location(name) for name in ['Old place', 'Other place']]
    db.tag_photo(pids[0], old_ids[0], source=None)
    db.tag_photo(pids[0], old_ids[1], source='accept')
    db.save_location_gps_reviews([(pids[0], 'kept-gps')])
    db.commit()
    before = [db.edit_history.photo_location_state(pid) for pid in pids]
    new_id = db.get_or_create_text_location('New place')
    body = {'keyword_id': new_id}
    if 'text' in route:
        body = {'name': 'New place'}
    if 'place' in route:
        body = {'place_id': 'new-place', 'place': {
            'place_id': 'new-place', 'name': 'New place',
            'latitude': 12.0, 'longitude': 34.0, 'parents': [],
        }}
    if route.startswith('batch-'):
        body['photo_ids'] = pids
        url = '/api/batch/location' + ('/text' if 'text' in route else '')
        response = client.post(url, json=body)
    else:
        url = f'/api/photos/{pids[0]}/location'
        if route == 'clear':
            response = client.delete(url)
        else:
            response = client.post(url + ('/text' if 'text' in route else ''), json=body)
    assert response.status_code == 200, response.json
    after = [db.edit_history.photo_location_state(pid) for pid in pids]
    assert after != before
    assert client.get('/api/undo/status').json['count'] == 1
    for _ in range(2):
        assert client.post('/api/undo').status_code == 200
        assert [db.edit_history.photo_location_state(pid) for pid in pids] == before
        assert client.post('/api/redo').status_code == 200
        assert [db.edit_history.photo_location_state(pid) for pid in pids] == after
    # Replaying must queue a corrective sidecar write, even after prior sync.
    assert db.has_pending_location_change(pids[0])


def test_repeating_location_assignment_and_empty_clear_do_not_create_history(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    pid = db.photos.list_page()[0]['id']
    url = f'/api/photos/{pid}/location'
    assert client.delete(url).status_code == 200
    assert client.get('/api/undo/status').json['available'] is False
    body = {'name': 'Same place'}
    assert client.post(url + '/text', json=body).status_code == 200
    assert client.post(url + '/text', json=body).status_code == 200
    assert client.get('/api/undo/status').json['count'] == 1
    assert client.post('/api/undo').status_code == 200
    assert not _location_ids(db, pid)
    # A no-op clear also preserves the redo stack.
    assert client.delete(url).status_code == 200
    assert client.get('/api/redo/status').json['available'] is True


def test_batch_location_history_failure_rolls_back_all_writes(app_and_db, monkeypatch):
    app, db = app_and_db
    client = app.test_client()
    pids = [p['id'] for p in db.photos.list_page()]
    from db import Database

    def fail(self, *args, **kwargs):
        # An independent connection must still see the original assignments.
        with sqlite3.connect(db._db_path) as reader:
            count = reader.execute("SELECT COUNT(*) FROM photo_keywords pk JOIN keywords k ON k.id=pk.keyword_id WHERE k.type='location'").fetchone()[0]
            assert count == 0
        raise RuntimeError('history failed')

    monkeypatch.setattr(Database, 'record_edit', fail)
    response = client.post('/api/batch/location/text', json={'photo_ids': pids, 'name': 'Failed place'})
    assert response.status_code == 500
    assert all(not _location_ids(db, pid) for pid in pids)
    assert not db.edit_history.list_recent()
    assert db.conn.execute("SELECT id FROM keywords WHERE name = 'Failed place'").fetchone() is None


def _predictions(db, pid, group=None):
    did = db.save_detections(pid, [{'box': {'x': .1, 'y': .1, 'w': .5, 'h': .5}, 'confidence': .9, 'category': 'animal'}], detector_model='test')[0]
    db.predictions.add(did, 'Robin', .7, 'test', labels_fingerprint='other')
    db.predictions.add(did, 'Bald Eagle', .9, 'test', group_id=group)
    db.predictions.add(did, 'Golden Eagle', .8, 'test', status='alternative')
    rows = db.conn.execute('SELECT id, species FROM predictions WHERE detection_id = ?', (did,)).fetchall()
    return tuple(next(row['id'] for row in rows if row['species'] == species)
                 for species in ('Bald Eagle', 'Golden Eagle', 'Robin'))


@pytest.mark.parametrize('mode', ['single', 'batch', 'burst', 'reviewed'])
def test_prediction_decisions_round_trip_exact_scopes(app_and_db, mode):
    app, db = app_and_db
    client = app.test_client()
    pids = [p['id'] for p in db.photos.list_page()][:2]
    group = 'burst' if mode == 'burst' else None
    ids = [_predictions(db, pid, group) for pid in pids]
    keywords_before = [db.keywords.get_for_photo(pid) for pid in pids]
    before = {pred_id: db.predictions.current_status(pred_id) for rows in ids for pred_id in rows}
    if mode == 'batch':
        response = client.post('/api/predictions/batch-reject', json={'prediction_ids': [rows[0] for rows in ids]})
    else:
        response = client.post(f'/api/predictions/{ids[0][0]}/' + ('reviewed' if mode == 'reviewed' else 'reject'))
    assert response.status_code == 200, response.json
    after = {pred_id: db.predictions.current_status(pred_id) for pred_id in before}
    assert before != after
    assert all(after[rows[2]] == 'pending' for rows in ids)
    if mode == 'burst':
        assert all(after[rows[0]] == 'rejected' and after[rows[1]] == 'rejected' for rows in ids)
    assert client.get('/api/undo/status').json['count'] == 1
    for _ in range(2):
        assert client.post('/api/undo').status_code == 200
        assert {pid: db.predictions.current_status(pid) for pid in before} == before
        assert client.post('/api/redo').status_code == 200
        assert {pid: db.predictions.current_status(pid) for pid in before} == after
    # Status-only undo never invents keywords.
    assert [db.keywords.get_for_photo(pid) for pid in pids] == keywords_before


def test_legacy_incomplete_audit_rows_stay_excluded(app_and_db):
    app, db = app_and_db
    pid = db.photos.list_page()[0]['id']
    for action in ('location_set', 'location_clear', 'prediction_reject', 'prediction_reviewed'):
        db.record_edit(action, 'Legacy edit', '', [{'photo_id': pid, 'old_value': '', 'new_value': ''}])
    assert app.test_client().get('/api/undo/status').json['available'] is False


def test_every_mutating_route_has_an_explicit_undo_decision(app_and_db):
    """New write routes must be assessed instead of silently lacking history."""
    import json
    from pathlib import Path

    app, _ = app_and_db
    inventory = json.loads((Path(__file__).parent / 'contracts' / 'undo_coverage.json').read_text(encoding='utf-8'))
    listed = [route for group in inventory.values() for route in group['routes']]
    actual = {
        f'{method} {rule.rule}'
        for rule in app.url_map.iter_rules()
        for method in rule.methods
        if method in {'POST', 'PUT', 'PATCH', 'DELETE'}
    }
    assert len(listed) == len(set(listed)), 'A route has conflicting undo decisions'
    assert set(listed) == actual, (
        f'Assess undo coverage for new routes: {sorted(actual - set(listed))}; '
        f'remove retired routes: {sorted(set(listed) - actual)}'
    )


def test_recorded_action_literals_have_replay_or_explicit_exclusion():
    """An added history type must never advance Undo without a replay handler."""
    import ast
    from pathlib import Path

    from db import Database

    assert set(Database._UNDO_HANDLERS) == set(Database._REDO_HANDLERS)
    supported = (
        set(Database._EDIT_FIELD_SETTERS) | set(Database._UNDO_HANDLERS)
        | {'pipeline_grouping', 'species_confirm_cache'} | set(Database._NON_UNDOABLE)
    )
    root = Path(__file__).parents[1]
    for directory in ('services', 'web'):
        for path in (root / directory).glob('*.py'):
            for call in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
                if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Attribute):
                    continue
                if call.func.attr != 'record_edit' or not call.args:
                    continue
                action = call.args[0]
                if isinstance(action, ast.Constant) and isinstance(action.value, str):
                    assert action.value in supported, f'{path.name}:{call.lineno}: missing replay for {action.value}'


def test_later_location_group_conflict_preserves_successful_assignments(app_and_db):
    """A handled place-chain conflict cannot roll back earlier groups' history."""
    from services.edit_history import change_photo_location

    _, db = app_and_db
    pids = [p['id'] for p in db.photos.list_page()]
    good = db.get_or_create_text_location('Good place')
    country = db.get_or_create_text_location('Conflict country')
    db.conn.execute("INSERT INTO keywords(name, type, parent_id) VALUES ('Conflict state', 'general', ?)", (country,))
    db.commit()
    with db._commits_held():
        item = change_photo_location(db, pids[0], good)
        db.record_edit('photo_location', 'Assigned good place', str(good), [item], _commit=False)
        with pytest.raises(RuntimeError):
            db.upsert_place_chain({
                'place_id': 'bad-place', 'name': 'Bad place',
                'address_components': [
                    {'name': 'Conflict country', 'types': ['country']},
                    {'name': 'Conflict state', 'types': ['administrative_area_level_1']},
                ],
            })
    assert _location_ids(db, pids[0]) == {good}
    assert db.edit_history.latest_undoable() is not None
    db.undo_last_edit()
    assert not _location_ids(db, pids[0])
    db.redo_last_undo()
    assert _location_ids(db, pids[0]) == {good}


@pytest.mark.parametrize('batch', [False, True])
def test_unchanged_text_location_with_new_coordinates_queues_sync_and_keeps_redo(app_and_db, batch):
    import config as cfg

    app, db = app_and_db
    cfg.save({**cfg.load(), 'write_assigned_location_to_xmp': True})
    client = app.test_client()
    pids = [p['id'] for p in db.photos.list_page()][:2 if batch else 1]
    keyword_id = db.get_or_create_text_location('Existing place')
    for pid in pids:
        db.set_photo_location(pid, keyword_id)
    assert not any(db.has_pending_location_change(pid) for pid in pids)
    # An unrelated undone edit must remain redoable after this assignment.
    assert client.post(f'/api/photos/{pids[0]}/rating', json={'rating': 5}).status_code == 200
    assert client.post('/api/undo').status_code == 200
    count = db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0]
    body = {'name': 'Existing place', 'latitude': 12.0, 'longitude': 34.0}
    if batch:
        body['photo_ids'] = pids
        url = '/api/batch/location/text'
    else:
        url = f'/api/photos/{pids[0]}/location/text'
    response = client.post(url, json=body)
    assert response.status_code == 200, response.json
    row = db.get_keyword_place_row(keyword_id)
    assert (row['latitude'], row['longitude']) == (12.0, 34.0)
    assert all(db.has_pending_location_change(pid) for pid in pids)
    assert db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0] == count
    assert client.get('/api/redo/status').json['available'] is True
    assert client.get('/api/undo/status').json['available'] is False


@pytest.mark.parametrize('direction', ['undo', 'redo'])
@pytest.mark.parametrize('changed', ['species', 'model', 'fingerprint', 'detection', 'photo', 'bounds', 'confidence', 'created_at'])
def test_prediction_status_history_does_not_change_reused_id(app_and_db, direction, changed):
    app, db = app_and_db
    client = app.test_client()
    pids = [p['id'] for p in db.photos.list_page()][:2]
    did = db.save_detections(pids[0], [
        {'box': {'x': .1, 'y': .1, 'w': .5, 'h': .5}, 'confidence': .9, 'category': 'animal'},
    ], detector_model='test')[0]
    db.predictions.add(did, 'Bald Eagle', .9, 'test')
    original_id = db.conn.execute('SELECT id FROM predictions WHERE detection_id = ?', (did,)).fetchone()[0]
    original_time = db.conn.execute('SELECT created_at FROM predictions WHERE id = ?', (original_id,)).fetchone()[0]
    assert client.post(f'/api/predictions/{original_id}/reject').status_code == 200
    if direction == 'redo':
        assert client.post('/api/undo').status_code == 200
    db.predictions.clear(model='test')
    if changed == 'detection':
        did = db.save_detections(pids[0], [
            {'box': {'x': .1, 'y': .1, 'w': .5, 'h': .5}, 'confidence': .9, 'category': 'animal'},
        ], detector_model='replacement')[0]
    elif changed == 'photo':
        db.conn.execute('UPDATE detections SET photo_id = ? WHERE id = ?', (pids[1], did))
        db.commit()
    elif changed == 'bounds':
        db.conn.execute('UPDATE detections SET box_x = .3 WHERE id = ?', (did,))
        db.commit()
    db.predictions.add(
        did, 'Golden Eagle' if changed == 'species' else 'Bald Eagle',
        .8 if changed == 'confidence' else .9,
        'other-model' if changed == 'model' else 'test', status='reviewed',
        labels_fingerprint='other' if changed == 'fingerprint' else 'legacy',
    )
    replacement_id = db.conn.execute('SELECT id FROM predictions WHERE detection_id = ?', (did,)).fetchone()[0]
    assert replacement_id == original_id, 'The regression must exercise SQLite ID reuse'
    replacement_time = '2000-01-01 00:00:00' if changed == 'created_at' else original_time
    db.conn.execute('UPDATE predictions SET created_at = ? WHERE id = ?', (replacement_time, replacement_id))
    db.commit()
    assert client.post('/api/' + direction).status_code == 200
    assert db.predictions.current_status(replacement_id) == 'reviewed'


def test_id_only_prediction_status_history_cannot_change_a_current_prediction(app_and_db):
    import json

    app, db = app_and_db
    pid = db.photos.list_page()[0]['id']
    pred_id, _, _ = _predictions(db, pid)
    db.record_edit('prediction_status', 'Old ID-only decision', 'rejected', [{
        'photo_id': pid, 'old_value': json.dumps({str(pred_id): 'rejected'}),
        'new_value': json.dumps({str(pred_id): 'pending'}),
    }])
    assert app.test_client().post('/api/undo').status_code == 200
    assert db.predictions.current_status(pred_id) == 'pending'



def test_history_source_contract_reads_utf8_with_a_legacy_windows_default(monkeypatch):
    from pathlib import Path

    read_text = Path.read_text

    def legacy_default(path, encoding=None, errors=None, **kwargs):
        return read_text(path, encoding=encoding or 'cp1252', errors=errors, **kwargs)

    monkeypatch.setattr(Path, 'read_text', legacy_default)
    test_recorded_action_literals_have_replay_or_explicit_exclusion()


@pytest.mark.parametrize('direction', ['undo', 'redo'])
def test_identical_prediction_recreated_in_same_second_cannot_receive_old_history(app_and_db, direction):
    import json

    app, db = app_and_db
    client = app.test_client()
    pid = db.photos.list_page()[0]['id']
    did = db.save_detections(pid, [
        {'box': {'x': .1, 'y': .1, 'w': .5, 'h': .5}, 'confidence': .9, 'category': 'animal'},
    ], detector_model='test')[0]
    db.predictions.add(did, 'Bald Eagle', .9, 'test')
    pred_id = db.conn.execute('SELECT id FROM predictions WHERE detection_id = ?', (did,)).fetchone()[0]
    assert client.post(f'/api/predictions/{pred_id}/reviewed').status_code == 200
    original_row = dict(db.conn.execute('SELECT * FROM predictions WHERE id = ?', (pred_id,)).fetchone())
    before = json.loads(db.edit_history.prediction_status_state([pred_id]))[str(pred_id)]['identity']
    if direction == 'redo':
        assert client.post('/api/undo').status_code == 200
    db.predictions.clear(model='test')
    assert db.conn.execute('SELECT COUNT(*) FROM prediction_history_instances').fetchone()[0] == 0
    db.predictions.add(did, 'Bald Eagle', .9, 'test', status='accepted')
    replacement_id = db.conn.execute('SELECT id FROM predictions WHERE detection_id = ?', (did,)).fetchone()[0]
    assert replacement_id == pred_id
    db.conn.execute('UPDATE predictions SET created_at = ? WHERE id = ?', (original_row['created_at'], pred_id))
    # Even a replacement captured by another history action gets a fresh token.
    db.edit_history.ensure_prediction_instances([pred_id])
    db.commit()
    after = json.loads(db.edit_history.prediction_status_state([pred_id]))[str(pred_id)]['identity']
    assert before['prediction_instance_id'] != after['prediction_instance_id']
    assert dict(db.conn.execute('SELECT * FROM predictions WHERE id = ?', (pred_id,)).fetchone()) == original_row
    assert client.post('/api/' + direction).status_code == 200
    assert db.predictions.current_status(pred_id) == 'accepted'


def test_prediction_instance_is_stable_on_repeated_reads_and_catalog_reopen(app_and_db):
    import json

    from db import Database

    _, db = app_and_db
    pid = db.photos.list_page()[0]['id']
    pred_id, _, _ = _predictions(db, pid)
    db.edit_history.ensure_prediction_instances([pred_id])
    db.commit()
    before = json.loads(db.edit_history.prediction_status_state([pred_id]))
    assert not db.in_transaction
    db.edit_history.prediction_status_state([pred_id])
    assert not db.in_transaction, 'Reading an identity must not take the writer lock'
    with Database(db._db_path, initialize_schema=False) as reader:
        reader.edit_history.ensure_prediction_instances([pred_id])
        reader.commit()
        assert json.loads(reader.edit_history.prediction_status_state([pred_id])) == before


@pytest.mark.parametrize('direction', ['undo', 'redo'])
@pytest.mark.parametrize('enrichment', ['full_fingerprint', 'refresh_output'])
def test_same_prediction_instance_remains_undoable_after_metadata_enrichment(app_and_db, direction, enrichment):
    app, db = app_and_db
    client = app.test_client()
    pid = db.photos.list_page()[0]['id']
    pred_id, _, _ = _predictions(db, pid)
    pred = db.predictions.get_decision_row(pred_id)
    assert client.post(f'/api/predictions/{pred_id}/reject').status_code == 200
    if direction == 'redo':
        assert client.post('/api/undo').status_code == 200
    instance_id = db.conn.execute('SELECT instance_id FROM prediction_history_instances WHERE prediction_id = ?', (pred_id,)).fetchone()[0]
    if enrichment == 'full_fingerprint':
        # Computation-cache import enriches this field on the existing row.
        db.conn.execute('UPDATE predictions SET labels_fingerprint_full = ? WHERE id = ?', ('full fingerprint', pred_id))
        db.commit()
    else:
        db.predictions.add(
            pred['detection_id'], pred['species'], .8, pred['model'],
            taxonomy={'taxon_id': 987654}, refresh_output=True, preserve_manual_review=True,
        )
    assert db.conn.execute('SELECT instance_id FROM prediction_history_instances WHERE prediction_id = ?', (pred_id,)).fetchone()[0] == instance_id
    assert db.predictions.current_status(pred_id) == ('rejected' if direction == 'undo' else 'pending')
    assert client.post('/api/' + direction).status_code == 200
    assert db.predictions.current_status(pred_id) == ('pending' if direction == 'undo' else 'rejected')
    # Undo/Redo changes the review, preserving the newly enriched raw fields.
    row = db.conn.execute('SELECT labels_fingerprint_full, confidence, source_taxon_id FROM predictions WHERE id = ?', (pred_id,)).fetchone()
    if enrichment == 'full_fingerprint':
        assert row['labels_fingerprint_full'] == 'full fingerprint'
    else:
        assert (row['confidence'], row['source_taxon_id']) == (.8, 987654)
