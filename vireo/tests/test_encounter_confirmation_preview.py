"""Exact photo-set decisions, transitions, mixed subjects and durable undo."""
import copy
import json
import os

import pytest


def seed(app, db, species=None):
    ids = [row['id'] for row in db.conn.execute('SELECT id FROM photos ORDER BY id')]
    if species:
        kid = db.add_keyword(species, is_species=True)
        for pid in ids:
            db.tag_photo(pid, kid)
    photos = [dict(row, label='REVIEW') for row in db.conn.execute(
        'SELECT id, filename, timestamp, flag FROM photos ORDER BY id')]
    result = {
        'photos': photos,
        'encounters': [{
            'photo_ids': ids, 'photo_count': len(ids), 'burst_count': 1,
            'bursts': [{'photo_ids': ids[:], 'species_override': None}],
            'confirmed_species': species, 'confirmed_species_list': [species] if species else [],
            'species_confirmed': bool(species), 'species': [species or 'Unknown', .9],
            'species_predictions': [], 'time_range': [None, None],
        }],
        'summary': {'total_photos': len(ids), 'encounter_count': 1, 'burst_count': 1},
    }
    path = os.path.join(os.path.dirname(app.config['DB_PATH']), f'pipeline_results_ws{db.active_workspace_id}.json')
    with open(path, 'w') as handle:
        json.dump(result, handle)
    return ids, path


def request_preview(client, body):
    response = client.post('/api/encounters/species', json={**body, 'preview': True})
    assert response.status_code == 200, response.get_json()
    return response.get_json()


def apply(client, body, preview):
    return client.post('/api/encounters/species', json={**body, 'preview_token': preview['preview_token']})


@pytest.mark.parametrize(('first', 'second'), [
    ("Cassin's Kingbird", 'Yellow-rumped Warbler'), ('Western Grebe', 'Eared Grebe'),
])
def test_explicit_transition_exact_subset_and_single_undo(app_and_db, first, second):
    app, db = app_and_db
    client = app.test_client()
    ids, path = seed(app, db, first)
    other = db.add_keyword('American Wigeon', is_species=True)
    db.tag_photo(ids[1], other)
    db.photo_review.set_flag(ids[1], 'flagged')
    before = json.load(open(path))
    body = {'species': second, 'previous_species': first, 'photo_ids': ids[1:],
            'source_photo_ids': ids, 'split': True}
    preview = request_preview(client, body)
    assert preview['photo_ids'] == ids[1:]
    assert preview['photo_count'] == 2
    assert preview['photos'][0]['removals'] == []
    assert preview['photos'][1]['removals'] == [first]
    assert preview['photos'][1]['additions'] == [second]
    assert 'American Wigeon' in preview['photos'][1]['existing_species']
    assert json.load(open(path)) == before
    response = apply(client, body, preview)
    assert response.status_code == 200, response.get_json()
    after = response.get_json()['encounters']
    assert [enc['photo_ids'] for enc in after] == [ids[:1], ids[1:]]
    assert [enc['confirmed_species'] for enc in after] == [first, second]
    actual = db.photo_species.get_species_for_photos(ids)
    assert actual[ids[0]] == [first]
    assert set(actual[ids[1]]) == {second, 'American Wigeon'}
    assert actual[ids[2]] == [second]
    assert db.photos.get(ids[1])['flag'] == 'flagged'
    undo = client.post('/api/undo')
    assert undo.status_code == 200, undo.get_json()
    assert json.load(open(path))['encounters'] == before['encounters']
    assert set(db.photo_species.get_species_for_photos(ids)[ids[1]]) == {first, 'American Wigeon'}
    assert db.photos.get(ids[1])['flag'] == 'flagged'
    redo = client.post('/api/redo')
    assert redo.status_code == 200, redo.get_json()
    assert json.load(open(path))['encounters'] == after


def test_mixed_subject_matrix_add_preserves_species_and_membership(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids, path = seed(app, db, 'American Wigeon')
    detections = db.detections.save(ids[0], [
        {'box': {'x': .1, 'y': .1, 'w': .2, 'h': .3}, 'confidence': .9, 'category': 'animal'},
        {'box': {'x': .6, 'y': .1, 'w': .2, 'h': .3}, 'confidence': .9, 'category': 'animal'},
    ], detector_model='test')
    for did, name in zip(detections, ['American Wigeon', 'Blue-winged Teal'], strict=True):
        db.predictions.add(detection_id=did, species=name, confidence=.95, model='test')
    body = {'species': 'Blue-winged Teal', 'photo_ids': ids[:1], 'source_photo_ids': ids, 'add': True}
    preview = request_preview(client, body)
    subjects = preview['photos'][0]['subjects']
    assert {s['detection_id']: s['predictions'][0]['species'] for s in subjects} == dict(zip(detections, ['American Wigeon', 'Blue-winged Teal'], strict=True))
    assert preview['photos'][1]['selected'] is False
    assert preview['photos'][1]['additions'] == []
    assert apply(client, body, preview).status_code == 200
    cache = json.load(open(path))
    assert cache['encounters'][0]['photo_ids'] == ids
    assert cache['encounters'][0]['confirmed_species_list'] == ['American Wigeon']
    assert set(db.photo_species.get_species_for_photos(ids)[ids[0]]) == {'American Wigeon', 'Blue-winged Teal'}
    assert client.post('/api/undo').status_code == 200
    assert db.photo_species.get_species_for_photos(ids)[ids[0]] == ['American Wigeon']


@pytest.mark.parametrize('change', ['membership', 'keyword', 'flag', 'choice', 'prediction'])
def test_stale_preview_fails_without_species_writes(app_and_db, change):
    app, db = app_and_db
    client = app.test_client()
    ids, path = seed(app, db, 'Western Grebe')
    body = {'species': 'Eared Grebe', 'photo_ids': ids[1:], 'source_photo_ids': ids, 'add': True}
    preview = request_preview(client, body)
    if change == 'membership':
        cached = json.load(open(path))
        cached['encounters'][0]['bursts'].append({'photo_ids': []})
        with open(path, 'w') as handle:
            json.dump(cached, handle)
    elif change == 'keyword':
        db.tag_photo(ids[0], db.add_keyword('Robin', is_species=True))
    elif change == 'flag':
        db.photo_review.set_flag(ids[0], 'flagged')
    elif change == 'choice':
        body['photo_ids'] = ids[:1]
    else:
        did = db.detections.save(ids[0], [
            {'box': {'x': .1, 'y': .1, 'w': .2, 'h': .3}, 'confidence': .9, 'category': 'animal'},
        ], detector_model='test')[0]
        db.predictions.add(detection_id=did, species='Eared Grebe', confidence=.99, model='test')
    before = copy.deepcopy(db.photo_species.get_species_for_photos(ids))
    response = apply(client, body, preview)
    assert response.status_code == 409
    assert 'Preview again' in response.get_json()['error']
    assert db.photo_species.get_species_for_photos(ids) == before


def test_already_confirmed_scoped_split_has_undo_without_keyword_changes(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids, path = seed(app, db, 'Eared Grebe')
    body = {'species': 'Eared Grebe', 'photo_ids': ids[1:], 'source_photo_ids': ids, 'add': True, 'split': True}
    preview = request_preview(client, body)
    assert all(not p['additions'] and not p['removals'] for p in preview['photos'])
    assert apply(client, body, preview).status_code == 200
    assert len(json.load(open(path))['encounters']) == 2
    assert client.post('/api/undo').status_code == 200
    assert len(json.load(open(path))['encounters']) == 1
    assert all(names == ['Eared Grebe'] for names in db.photo_species.get_species_for_photos(ids).values())


def test_preview_identifies_historical_labels_and_low_confidence_subjects(app_and_db):
    app, db = app_and_db
    ids, _ = seed(app, db)
    did = db.detections.save(ids[0], [
        {'box': {'x': .1, 'y': .2, 'w': .3, 'h': .4}, 'confidence': .1, 'category': 'animal'},
    ], detector_model='test')[0]
    old = db.predictions.add(detection_id=did, species='Old suggestion', confidence=.99, model='test', labels_fingerprint='old-labels')
    new = db.predictions.add(detection_id=did, species='Current suggestion', confidence=.6, model='test', labels_fingerprint='new-labels')
    db.conn.execute("UPDATE predictions SET created_at='2026-01-01' WHERE id=?", (old,))
    db.conn.execute("UPDATE predictions SET created_at='2026-01-02' WHERE id=?", (new,))
    db.commit()
    preview = request_preview(app.test_client(), {'species': 'Robin', 'photo_ids': ids, 'add': True})
    subject = preview['photos'][0]['subjects'][0]
    assert subject['below_threshold'] is True
    assert {p['labels_fingerprint']: p['historical'] for p in subject['predictions']} == {'old-labels': True, 'new-labels': False}


def test_preview_revalidates_manual_provenance_even_when_species_name_is_unchanged(app_and_db):
    app, db = app_and_db
    ids, _ = seed(app, db, 'Western Grebe')
    client = app.test_client()
    db.conn.execute("UPDATE photo_keywords SET source='pipeline' WHERE photo_id=?", (ids[0],))
    db.commit()
    body = {'species': 'Eared Grebe', 'photo_ids': ids, 'add': True}
    preview = request_preview(client, body)
    db.conn.execute("UPDATE photo_keywords SET source='manual' WHERE photo_id=?", (ids[0],))
    db.commit()
    assert apply(client, body, preview).status_code == 409
    assert all('Eared Grebe' not in names for names in db.photo_species.get_species_for_photos(ids).values())


def test_mixed_encounter_without_common_cached_species_can_replace_a_named_frame_species(app_and_db):
    app, db = app_and_db
    ids, path = seed(app, db)
    db.tag_photo(ids[1], db.add_keyword('Western Grebe', is_species=True))
    db.tag_photo(ids[1], db.add_keyword('American Wigeon', is_species=True))
    body = {'species': 'Eared Grebe', 'photo_ids': [ids[1]], 'source_photo_ids': ids,
            'previous_species': 'Western Grebe'}
    client = app.test_client()
    preview = request_preview(client, body)
    assert apply(client, body, preview).status_code == 200
    assert set(db.photo_species.get_species_for_photos(ids)[ids[1]]) == {'Eared Grebe', 'American Wigeon'}
    cache = json.load(open(path))
    assert cache['encounters'][0]['confirmed_species'] is None
    assert cache['encounters'][0]['species_confirmed'] is False


@pytest.mark.parametrize('change', ['rename', 'retype', 'relink', 'source_identity', 'taxon_identity'])
def test_preview_binds_unattached_target_keyword_identity(app_and_db, change):
    app, db = app_and_db
    ids, path = seed(app, db, 'Western Grebe')
    target_id = db.add_keyword('Eared Grebe', is_species=True)
    db.conn.executemany(
        'INSERT INTO taxa (id, inat_id, name, rank) VALUES (?, ?, ?, ?)',
        [(900001, 800001, 'Podiceps nigricollis', 'species'),
         (900002, 800002, 'Podiceps auritus', 'species')],
    )
    db.conn.execute('UPDATE keywords SET taxon_id=?, source_taxon_id=? WHERE id=?',
                    (900001, 800001, target_id))
    db.commit()
    body = {'species': 'Eared Grebe', 'photo_ids': ids, 'add': True}
    client = app.test_client()
    preview = request_preview(client, body)
    assert all('Eared Grebe' not in row['existing_species'] for row in preview['photos'])
    if change == 'rename':
        db.conn.execute("UPDATE keywords SET name='Changed Grebe' WHERE id=?", (target_id,))
    elif change == 'retype':
        db.conn.execute("UPDATE keywords SET type='individual', is_species=0 WHERE id=?", (target_id,))
    elif change == 'relink':
        db.conn.execute('UPDATE keywords SET taxon_id=900002 WHERE id=?', (target_id,))
    elif change == 'source_identity':
        db.conn.execute('UPDATE keywords SET source_taxon_id=800002 WHERE id=?', (target_id,))
    else:
        db.conn.execute('UPDATE taxa SET inat_id=800003 WHERE id=900001')
    db.commit()
    before_species = db.photo_species.get_species_for_photos(ids)
    before_cache = json.load(open(path))
    before_history = db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0]
    response = apply(client, body, preview)
    assert response.status_code == 409, response.get_json()
    assert db.photo_species.get_species_for_photos(ids) == before_species
    assert json.load(open(path)) == before_cache
    assert db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0] == before_history


def test_preview_binds_inferred_taxon_when_target_keyword_has_not_been_created(app_and_db):
    app, db = app_and_db
    ids, _ = seed(app, db, 'Western Grebe')
    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, common_name, rank) VALUES (900001, 800001, 'Podiceps nigricollis', 'Eared Grebe', 'species')",
    )
    db.commit()
    client = app.test_client()
    body = {'species': 'Eared Grebe', 'photo_ids': ids, 'add': True}
    preview = request_preview(client, body)
    assert db.keywords.species_confirmation_target('Eared Grebe')['keyword'] is None
    db.conn.execute('UPDATE taxa SET inat_id=800002 WHERE id=900001')
    db.commit()
    assert apply(client, body, preview).status_code == 409
    assert db.keywords.species_confirmation_target('Eared Grebe')['keyword'] is None
