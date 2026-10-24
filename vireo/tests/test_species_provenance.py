"""Evidence belongs to a photo/species association and follows normal undo."""

import sqlite3

import pytest
from db import Database


def confirm(client, species, photos=(1,), **kwargs):
    response = client.post('/api/encounters/species', json={
        'species': species, 'photo_ids': list(photos), 'add': True, **kwargs,
    })
    assert response.status_code == 200, response.get_json()
    return response.get_json()


def assignments(client, pid=1):
    response = client.get(f'/api/photos/{pid}')
    assert response.status_code == 200
    return {a['species']: a for a in response.get_json()['species_assignments']}


def evidence(client, assignment, **body):
    return client.post(f"/api/photos/{assignment['photo_id']}/species/{assignment['keyword_id']}/provenance", json=body)


def test_mixed_species_evidence_reload_and_flags(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    db.photo_review.set_flag(1, 'flagged')
    confirm(client, 'Robin', provenance={'method': 'direct_visual', 'reviewer': 'Reviewer',
                                      'evidence_source': 'Visible plumage'})
    confirm(client, 'Mallard', provenance={'method': 'adjacent_frames', 'supporting_photo_ids': [2],
                                        'reason': 'Same subject in the next frame'})
    actual = assignments(client)
    assert actual['Robin']['method'] == 'direct_visual'
    assert actual['Mallard']['method'] == 'adjacent_frames'
    assert actual['Mallard']['supporting_photo_ids'] == [2]
    assert actual['Mallard']['confidence'] is None
    assert actual['Mallard']['confidence_meaning'] == 'Not recorded'
    assert client.get('/api/photos/1').get_json()['flag'] == 'flagged'
    with Database(db._db_path) as reopened:
        reloaded = {a['species']: a for a in reopened.species_provenance.assignments(1)}
    assert reloaded == actual
    item = db.conn.execute('SELECT species_provenance FROM edit_history_items ORDER BY id DESC LIMIT 1').fetchone()
    assert 'adjacent_frames' in item[0]


def test_existing_manual_and_imports_are_unspecified(app_and_db):
    app, db = app_and_db
    manual = db.add_keyword('Manual bird', is_species=True)
    imported = db.add_keyword('Imported bird', is_species=True)
    db.tag_photo(1, manual)
    db.tag_photo(1, imported, source=None)
    result = assignments(app.test_client())
    assert result['Manual bird']['method'] == 'manual_unspecified'
    assert result['Imported bird']['method'] == 'imported_unknown'


def test_update_existing_evidence_undo_redo_and_audit(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    confirm(client, 'Robin')
    original = assignments(client)['Robin']
    response = evidence(client, original, method='adjacent_frames', reviewer='Test reviewer',
                        reason='Same bird beside the branch', supporting_photo_ids=[2])
    assert response.status_code == 200
    updated = assignments(client)['Robin']
    assert updated['confidence'] is None
    entry = db.edit_history.list_recent()[0]
    assert entry['source'] == 'Test reviewer'
    assert entry['reason'] == 'Same bird beside the branch'
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Robin'] == original
    assert client.post('/api/redo').status_code == 200
    assert assignments(client)['Robin'] == updated


def test_replace_and_remove_preserve_provenance_on_undo(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    confirm(client, 'Robin', provenance={'method': 'adjacent_frames', 'reason': 'Adjacent subject'})
    original = assignments(client)['Robin']
    from test_app import _seed_encounter_cache
    _seed_encounter_cache(app, db, [1], confirmed_species='Robin')
    response = client.post('/api/encounters/species', json={
        'species': 'Mallard', 'photo_ids': [1], 'previous_species': 'Robin',
        'provenance': {'method': 'direct_visual', 'reviewer': 'Human'},
    })
    assert response.status_code == 200, response.get_json()
    assert 'Robin' not in assignments(client)
    new = assignments(client)['Mallard']
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Robin'] == original
    assert client.post('/api/redo').status_code == 200
    assert assignments(client)['Mallard'] == new
    response = client.delete(f"/api/photos/1/keywords/{new['keyword_id']}")
    assert response.status_code == 200, response.get_json()
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Mallard'] == new


def test_mixed_new_and_existing_confirmation_undo(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    confirm(client, 'Robin')
    confirm(client, 'Robin', photos=(1, 2), provenance={'method': 'adjacent_frames', 'reason': 'Same bird'})
    assert assignments(client)['Robin']['method'] == 'adjacent_frames'
    assert assignments(client, 2)['Robin']['method'] == 'adjacent_frames'
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Robin']['method'] == 'manual_unspecified'
    assert 'Robin' not in assignments(client, 2)
    assert client.post('/api/redo').status_code == 200
    assert assignments(client)['Robin']['method'] == 'adjacent_frames'
    assert assignments(client, 2)['Robin']['method'] == 'adjacent_frames'


@pytest.mark.parametrize('value', [
    {}, {'method': 'bogus'}, {'method': 'adjacent_frames'},
    {'method': 'direct_visual', 'confidence': 0.8},
    {'method': 'direct_visual', 'confidence': 0.8, 'confidence_meaning': 'Not recorded'},
    {'method': 'model_proposed', 'confidence': 10**400, 'confidence_meaning': 'Invalid score'},
    {'method': 'model_proposed', 'confidence': True, 'confidence_meaning': 'Model score'},
    {'method': 'model_proposed', 'confidence': float('nan'), 'confidence_meaning': 'Model score'},
    {'method': 'direct_visual', 'supporting_photo_ids': [True]},
    {'method': 'adjacent_frames', 'supporting_photo_ids': [99999]},
    {'method': 'direct_visual', 'reviewer': 12},
    {'method': 'direct_visual', 'detection_id': 99999},
])
def test_invalid_evidence_is_atomic(app_and_db, value):
    app, db = app_and_db
    client = app.test_client()
    before = db.edit_history.list_recent()
    response = client.post('/api/encounters/species', json={
        'species': 'Robin', 'photo_ids': [1], 'add': True, 'provenance': value,
    })
    assert response.status_code == 400, response.get_json()
    assert 'Robin' not in assignments(client)
    assert db.edit_history.list_recent() == before


def test_migration_keeps_historical_labels_unspecified(app_and_db):
    import schema
    app, db = app_and_db
    kid = db.add_keyword('Old manual bird', is_species=True)
    db.tag_photo(1, kid)
    with sqlite3.connect(db._db_path) as conn:
        conn.execute('ALTER TABLE photo_keywords DROP COLUMN species_provenance')
        conn.execute('ALTER TABLE edit_history_items DROP COLUMN species_provenance')
        conn.execute('PRAGMA user_version=24')
    schema.ensure_schema(db._db_path)
    schema.ensure_schema(db._db_path)
    assert assignments(app.test_client())['Old manual bird']['method'] == 'manual_unspecified'


def test_subject_scoping_and_confidence_meaning(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    did = db.detections.save(1, [{'box': {'x': .1, 'y': .1, 'w': .5, 'h': .5},
                                  'confidence': .9, 'category': 'animal'}], detector_model='test')[0]
    confirm(client, 'Robin', provenance={'method': 'model_proposed', 'detection_id': did,
                                      'confidence': .8, 'confidence_meaning': 'Uncalibrated model similarity score',
                                      'evidence_source': 'test-model'})
    actual = assignments(client)['Robin']
    assert actual['detection_id'] == did
    assert actual['confidence'] == .8
    assert actual['confidence_meaning'] == 'Uncalibrated model similarity score'
    response = client.post('/api/encounters/species', json={
        'species': 'Robin', 'photo_ids': [2], 'add': True,
        'provenance': {'method': 'direct_visual', 'detection_id': did},
    })
    assert response.status_code == 400
    assert 'Robin' not in assignments(client, 2)


def test_unknown_source_survives_remove_and_undo(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword('Imported bird', is_species=True)
    db.tag_photo(1, kid, source=None)
    original = assignments(client)['Imported bird']
    assert client.delete(f'/api/photos/1/keywords/{kid}').status_code == 200
    assert client.post('/api/undo').status_code == 200
    assert assignments(client)['Imported bird'] == original


def test_history_replays_after_reopening_catalog(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    confirm(client, 'Robin', provenance={'method': 'adjacent_frames', 'reason': 'Same bird'})
    original = assignments(client)['Robin']
    with Database(db._db_path) as reopened:
        reopened.undo_last_edit()
        assert not reopened.species_provenance.assignments(1)
    with Database(db._db_path) as reopened:
        reopened.redo_last_undo()
    assert assignments(client)['Robin'] == original


def test_same_named_distinct_species_do_not_share_evidence(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    # Imported homonyms may legitimately coexist; normal add canonicalizes names.
    first = db.conn.execute(
        "INSERT INTO keywords(name, is_species, type) VALUES ('Robin', 1, 'general')"
    ).lastrowid
    second = db.conn.execute(
        "INSERT INTO keywords(name, is_species, type) VALUES ('robin', 1, 'taxonomy')"
    ).lastrowid
    db.conn.execute("INSERT INTO taxa(id, name, rank) VALUES (101, 'First robin', 'species')")
    db.conn.execute("INSERT INTO taxa(id, name, rank) VALUES (102, 'Other robin', 'species')")
    db.conn.execute('UPDATE keywords SET taxon_id=101 WHERE id=?', (first,))
    db.conn.execute('UPDATE keywords SET taxon_id=102 WHERE id=?', (second,))
    db.tag_photo(1, first)
    db.tag_photo(1, second)
    assignment = next(a for a in db.species_provenance.assignments(1) if a['keyword_id'] == first)
    assert evidence(client, assignment, method='direct_visual', reason='First subject markings').status_code == 200
    actual = {a['keyword_id']: a for a in db.species_provenance.assignments(1)}
    assert actual[first]['method'] == 'direct_visual'
    assert actual[second]['method'] == 'manual_unspecified'
    assert db.photo_species.equivalent_assignment_ids([1], first) == {1: [first]}
