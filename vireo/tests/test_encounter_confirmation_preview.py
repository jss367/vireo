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


@pytest.mark.parametrize('combined', [False, True])
def test_v1_species_confirmation_alias_preserves_undo_contract(app_and_db, combined):
    app, db = app_and_db
    ids, _ = seed(app, db, 'Western Grebe')
    client = app.test_client()
    body = combined_request(ids) if combined else {'species': 'Eared Grebe', 'photo_ids': ids, 'add': True}
    headers = {'X-Vireo-Token': 'test-token-123'}
    preview = client.post('/api/v1/encounters/species', json={**body, 'preview': True}, headers=headers)
    assert preview.status_code == 200, preview.get_json()
    result = client.post('/api/v1/encounters/species', json={
        **body, 'preview_token': preview.get_json()['preview_token'],
    }, headers=headers)
    assert result.status_code == 200, result.get_json()
    names = db.photo_species.get_species_for_photos(ids)
    assert all('Eared Grebe' in names[pid] for pid in body['photo_ids'])
    if combined:
        assert db.photos.get(ids[0])['flag'] == 'flagged'
        assert db.photos.get(ids[1])['flag'] == 'rejected'
    assert client.post('/api/undo').status_code == 200
    assert all(names == ['Western Grebe'] for names in db.photo_species.get_species_for_photos(ids).values())
    assert all(db.photos.get(pid)['flag'] == 'none' for pid in ids)


@pytest.mark.parametrize(('change', 'source_id', 'inat_id'), [
    ('source_identity', 800001, 800001),
    ('missing_source_identity', None, 800001),
    ('removed_source_identity', 800002, 800001),
    ('inat_identity', None, 800001),
    ('missing_inat_identity', None, None),
    ('removed_inat_identity', None, 800001),
    ('scientific_identity', None, None),
])
def test_preview_binds_attached_species_source_identity(app_and_db, change, source_id, inat_id):
    app, db = app_and_db
    ids, path = seed(app, db, 'Western Grebe')
    keyword = db.keywords.top_level_species_keyword('Western Grebe')['id']
    db.conn.execute(
        "INSERT INTO taxa (id, inat_id, name, rank) VALUES (900001, ?, 'Aechmophorus occidentalis', 'species')",
        (inat_id,),
    )
    db.conn.execute('UPDATE keywords SET taxon_id=900001, source_taxon_id=? WHERE id=?', (source_id, keyword))
    db.commit()
    client = app.test_client()
    body = {'species': 'Eared Grebe', 'photo_ids': ids, 'add': True}
    before_names = db.photo_species.get_species_for_photos(ids)
    before_identities = db.photo_species.get_species_for_photos(ids, include_identities=True)
    preview = request_preview(client, body)
    if change in ('source_identity', 'missing_source_identity'):
        db.conn.execute('UPDATE keywords SET source_taxon_id=800002 WHERE id=?', (keyword,))
    elif change == 'removed_source_identity':
        db.conn.execute('UPDATE keywords SET source_taxon_id=NULL WHERE id=?', (keyword,))
    elif change in ('inat_identity', 'missing_inat_identity'):
        db.conn.execute('UPDATE taxa SET inat_id=800002 WHERE id=900001')
    elif change == 'removed_inat_identity':
        db.conn.execute('UPDATE taxa SET inat_id=NULL WHERE id=900001')
    else:
        db.conn.execute("UPDATE taxa SET name='Aechmophorus clarkii' WHERE id=900001")
    db.commit()
    assert db.photo_species.get_species_for_photos(ids) == before_names
    assert db.photo_species.get_species_for_photos(ids, include_identities=True) != before_identities
    before_cache = json.load(open(path))
    before_history = db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0]
    before_keywords = db.conn.execute('SELECT COUNT(*) FROM keywords').fetchone()[0]
    response = apply(client, body, preview)
    assert response.status_code == 409, response.get_json()
    assert db.photo_species.get_species_for_photos(ids) == before_names
    assert json.load(open(path)) == before_cache
    assert db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0] == before_history
    assert db.conn.execute('SELECT COUNT(*) FROM keywords').fetchone()[0] == before_keywords


def combined_request(ids):
    return {'species': 'Eared Grebe', 'photo_ids': ids[:1], 'source_photo_ids': ids[:2],
            'burst_index': 0, 'add': True, 'split': True,
            'group_changes': {'picks': ids[:1], 'rejects': ids[1:2], 'candidates': [],
                              'removed': ids[2:], 'encounter_index': 0, 'burst_index': 0,
                              'expected_burst_photo_ids': ids}}


def test_combined_species_flags_split_and_removal_share_one_undo(app_and_db):
    app, db = app_and_db
    ids, path = seed(app, db, 'Western Grebe')
    client = app.test_client()
    body = combined_request(ids)
    before = json.load(open(path))
    before_history = db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0]
    preview = request_preview(client, body)
    response = apply(client, body, preview)
    assert response.status_code == 200, response.get_json()
    assert db.photos.get(ids[0])['flag'] == 'flagged'
    assert db.photos.get(ids[1])['flag'] == 'rejected'
    assert set(db.photo_species.get_species_for_photos(ids)[ids[0]]) == {'Western Grebe', 'Eared Grebe'}
    assert db.photo_species.get_species_for_photos(ids)[ids[2]] == ['Western Grebe']
    after = json.load(open(path))
    assert [enc['photo_ids'] for enc in after['encounters']] == [ids[:1], ids[1:]]
    assert [burst['photo_ids'] for burst in after['encounters'][1]['bursts']] == [ids[1:2], ids[2:]]
    assert db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0] == before_history + 1
    undone = client.post('/api/undo')
    assert undone.status_code == 200
    assert undone.get_json()['flags_changed'] is True
    assert all(photo['flag'] == 'none' for photo in json.load(open(path))['photos'])
    assert all(db.photos.get(pid)['flag'] == 'none' for pid in ids)
    assert all(names == ['Western Grebe'] for names in db.photo_species.get_species_for_photos(ids).values())
    assert json.load(open(path))['encounters'] == before['encounters']
    redone = client.post('/api/redo')
    assert redone.status_code == 200
    assert redone.get_json()['flags_changed'] is True
    assert db.photos.get(ids[0])['flag'] == 'flagged'
    assert db.photos.get(ids[1])['flag'] == 'rejected'
    assert 'Eared Grebe' in db.photo_species.get_species_for_photos(ids)[ids[0]]
    assert json.load(open(path))['encounters'] == after['encounters']


@pytest.mark.parametrize('failure', ['flag', 'removal', 'history', 'save', 'after_save', 'commit'])
def test_combined_apply_failure_rolls_back_every_effect(app_and_db, monkeypatch, failure):
    app, db = app_and_db
    ids, path = seed(app, db, 'Western Grebe')
    # Production caches are written through save_results_raw, which stores
    # normalized summary counts. Match that on-disk shape for exact rollback.
    from pipeline import load_results_raw, save_results_raw
    cache_dir = os.path.dirname(path)
    save_results_raw(load_results_raw(cache_dir, db.active_workspace_id), cache_dir, db.active_workspace_id)
    client = app.test_client()
    body = combined_request(ids)
    preview = request_preview(client, body)
    before = json.load(open(path))
    before_species = db.photo_species.get_species_for_photos(ids)
    before_history = db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0]
    before_pending = [tuple(row) for row in db.conn.execute('SELECT * FROM pending_changes')]
    import pipeline
    from db import Database
    from repositories.photo_review import PhotoReviewRepository
    from services import grouping_history, review_group_changes

    def fail(*args, **kwargs):
        raise RuntimeError('injected combined apply failure')

    with monkeypatch.context() as patch:
        if failure == 'flag':
            original = PhotoReviewRepository.set_flag
            def fail_second_flag(self, pid, *args, **kwargs):
                result = original(self, pid, *args, **kwargs)
                if pid == ids[1]:
                    fail()
                return result
            patch.setattr(PhotoReviewRepository, 'set_flag', fail_second_flag)
        elif failure == 'removal':
            original = review_group_changes.detach_review_photo
            def fail_after_removal(*args, **kwargs):
                original(*args, **kwargs)
                fail()
            patch.setattr(review_group_changes, 'detach_review_photo', fail_after_removal)
        elif failure == 'history':
            patch.setattr(grouping_history, 'convert_to_grouping_edit', fail)
        elif failure == 'after_save':
            original_save = pipeline.save_results_raw
            saved = False
            def fail_after_save(*args, **kwargs):
                nonlocal saved
                result = original_save(*args, **kwargs)
                if not saved:
                    saved = True
                    fail()
                return result
            patch.setattr(pipeline, 'save_results_raw', fail_after_save)
        elif failure == 'save':
            patch.setattr(pipeline, 'save_results_raw', fail)
        else:
            patch.setattr(Database, 'commit', fail)
        assert apply(client, body, preview).status_code == 500
    assert all(db.photos.get(pid)['flag'] == 'none' for pid in ids)
    assert db.photo_species.get_species_for_photos(ids) == before_species
    assert json.load(open(path)) == before
    assert db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0] == before_history
    assert [tuple(row) for row in db.conn.execute('SELECT * FROM pending_changes')] == before_pending


@pytest.mark.parametrize('stale', ['membership', 'decisions'])
def test_combined_apply_rejects_stale_membership_or_decisions_before_writes(app_and_db, stale):
    app, db = app_and_db
    ids, path = seed(app, db, 'Western Grebe')
    client = app.test_client()
    body = combined_request(ids)
    preview = request_preview(client, body)
    if stale == 'membership':
        cached = json.load(open(path))
        cached['encounters'][0]['bursts'][0]['photo_ids'] = ids[:2]
        cached['encounters'][0]['bursts'].append({'photo_ids': ids[2:]})
        with open(path, 'w') as handle:
            json.dump(cached, handle)
    else:
        body['group_changes']['picks'], body['group_changes']['rejects'] = body['group_changes']['rejects'], body['group_changes']['picks']
    before = json.load(open(path))
    assert apply(client, body, preview).status_code == 409
    assert all(db.photos.get(pid)['flag'] == 'none' for pid in ids)
    assert all(names == ['Western Grebe'] for names in db.photo_species.get_species_for_photos(ids).values())
    assert json.load(open(path)) == before


def test_combined_apply_supports_legacy_raw_bursts(app_and_db):
    app, db = app_and_db
    ids, path = seed(app, db, 'Western Grebe')
    cached = json.load(open(path))
    cached['encounters'][0]['bursts'] = [ids]
    with open(path, 'w') as handle:
        json.dump(cached, handle)
    client = app.test_client()
    body = combined_request(ids)
    response = apply(client, body, request_preview(client, body))
    assert response.status_code == 200, response.get_json()
    assert db.photos.get(ids[0])['flag'] == 'flagged'
    assert db.photos.get(ids[1])['flag'] == 'rejected'
    assert [enc['photo_ids'] for enc in response.get_json()['encounters']] == [ids[:1], ids[1:]]
    assert client.post('/api/undo').status_code == 200
    assert json.load(open(path))['encounters'] == cached['encounters']


def test_combined_already_confirmed_species_keeps_flags_in_one_undo(app_and_db):
    app, db = app_and_db
    ids, path = seed(app, db, 'Western Grebe')
    client = app.test_client()
    body = combined_request(ids)
    body.update(species='Western Grebe', split=False)
    before = json.load(open(path))
    count = db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0]
    assert apply(client, body, request_preview(client, body)).status_code == 200
    assert db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0] == count + 1
    response = client.post('/api/undo')
    assert response.status_code == 200
    assert response.get_json()['flags_changed'] is True
    assert all(db.photos.get(pid)['flag'] == 'none' for pid in ids)
    assert all(photo['flag'] == 'none' for photo in json.load(open(path))['photos'])
    assert json.load(open(path))['encounters'] == before['encounters']
    assert all(names == ['Western Grebe'] for names in db.photo_species.get_species_for_photos(ids).values())
    assert client.post('/api/redo').status_code == 200
    assert db.photos.get(ids[0])['flag'] == 'flagged'
    assert db.photos.get(ids[1])['flag'] == 'rejected'


def test_stale_combined_history_retains_species_and_flags_after_retirement(app_and_db):
    app, db = app_and_db
    ids, path = seed(app, db, 'Western Grebe')
    client = app.test_client()
    body = combined_request(ids)
    assert apply(client, body, request_preview(client, body)).status_code == 200
    newer = json.load(open(path))
    newer['encounters'][0]['bursts'].append({'photo_ids': []})
    with open(path, 'w') as handle:
        json.dump(newer, handle)
    # Retire the obsolete structure while keeping both kinds of photo edits.
    assert client.post('/api/undo').status_code == 409
    response = client.post('/api/undo')
    assert response.status_code == 200
    assert response.get_json()['flags_changed'] is True
    assert all(db.photos.get(pid)['flag'] == 'none' for pid in ids)
    assert all(names == ['Western Grebe'] for names in db.photo_species.get_species_for_photos(ids).values())
    assert json.load(open(path))['encounters'] == newer['encounters']
    assert client.post('/api/redo').status_code == 200
    assert db.photos.get(ids[0])['flag'] == 'flagged'
    assert db.photos.get(ids[1])['flag'] == 'rejected'
    assert 'Eared Grebe' in db.photo_species.get_species_for_photos(ids)[ids[0]]
    assert json.load(open(path))['encounters'] == newer['encounters']


def homonym_preview_setup(app, db, mode):
    ids, path = seed(app, db)
    db.conn.executemany('INSERT INTO taxa (id, inat_id, name, common_name, rank) VALUES (?, ?, ?, ?, ?)', [
        (900001, 800001, 'Preview alpha', 'Preview Robin', 'species'),
        (900002, 800002, 'Preview beta', 'Other Preview Robin', 'species'),
    ])
    root = db.add_keyword('Preview Robin', is_species=True)
    db.conn.execute('UPDATE keywords SET taxon_id=NULL, source_taxon_id=NULL WHERE id=?', (root,))
    parent = db.add_keyword('Preview field guide')
    leaf = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type) VALUES ('Preview Robin', ?, 1, 'taxonomy')",
        (parent,),
    ).lastrowid
    parent = db.add_keyword('Unattached preview guide')
    homonym = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, is_species, type, taxon_id) VALUES ('Preview Robin', ?, 1, 'taxonomy', 900002)",
        (parent,),
    ).lastrowid
    for pid in ids:
        db.tag_photo(pid, leaf)
        if mode != 'add':
            db.tag_photo(pid, root)
    db.commit()
    body = {'species': 'Preview Robin', 'photo_ids': ids}
    if mode == 'add':
        body['add'] = True
    elif mode == 'remove':
        body['remove'] = True
    else:
        body.update(species='New Preview Species', previous_species='Preview Robin')
    return ids, path, homonym, body


@pytest.mark.parametrize('mode', ['add', 'remove', 'replace'])
@pytest.mark.parametrize('change', ['relink', 'source_identity', 'taxon_identity'])
def test_preview_binds_unattached_canonical_homonym_identity(app_and_db, mode, change):
    app, db = app_and_db
    ids, path, homonym, body = homonym_preview_setup(app, db, mode)
    client = app.test_client()
    preview = request_preview(client, body)
    if change == 'relink':
        db.conn.execute('UPDATE keywords SET taxon_id=900001 WHERE id=?', (homonym,))
    elif change == 'source_identity':
        db.conn.execute('UPDATE keywords SET source_taxon_id=800003 WHERE id=?', (homonym,))
    else:
        db.conn.execute('UPDATE taxa SET inat_id=800003 WHERE id=900002')
    db.commit()
    # The reviewed photo rows are unchanged: this is catalog-wide evidence.
    assert request_preview(client, body)['photos'] == preview['photos']
    before = json.load(open(path))
    before_keywords = [tuple(row) for row in db.conn.execute('SELECT * FROM photo_keywords')]
    before_history = [tuple(row) for row in db.conn.execute('SELECT * FROM edit_history')]
    before_pending = [tuple(row) for row in db.conn.execute('SELECT * FROM pending_changes')]
    assert apply(client, body, preview).status_code == 409
    assert json.load(open(path)) == before
    assert [tuple(row) for row in db.conn.execute('SELECT * FROM photo_keywords')] == before_keywords
    assert [tuple(row) for row in db.conn.execute('SELECT * FROM edit_history')] == before_history
    assert [tuple(row) for row in db.conn.execute('SELECT * FROM pending_changes')] == before_pending


@pytest.mark.parametrize('mode', ['add', 'remove', 'replace'])
@pytest.mark.parametrize('change', ['rename', 'relink', 'taxon_identity', 'rename_into_match'])
def test_preview_homonym_snapshot_excludes_unrelated_catalog_edits(app_and_db, mode, change):
    app, db = app_and_db
    _, _, _, body = homonym_preview_setup(app, db, mode)
    db.conn.execute("INSERT INTO taxa (id, inat_id, name, rank) VALUES (900003, 800003, 'Unrelated bird', 'species')")
    unrelated = db.conn.execute(
        "INSERT INTO keywords (name, is_species, type, taxon_id) VALUES ('Unrelated bird', 1, 'taxonomy', 900003)",
    ).lastrowid
    db.commit()
    client = app.test_client()
    preview = request_preview(client, body)
    if change == 'rename':
        db.conn.execute("UPDATE keywords SET name='Different unrelated bird' WHERE id=?", (unrelated,))
    elif change == 'relink':
        db.conn.execute('UPDATE keywords SET taxon_id=900002 WHERE id=?', (unrelated,))
    elif change == 'taxon_identity':
        db.conn.execute('UPDATE taxa SET inat_id=800004 WHERE id=900003')
    else:
        # Edge quotes, whitespace and ASCII casing use the guards' exact key.
        db.conn.execute("UPDATE keywords SET name=' ‘PREVIEW ROBIN’ ' WHERE id=?", (unrelated,))
    db.commit()
    current = request_preview(client, body)
    if change == 'rename_into_match':
        assert current['preview_token'] != preview['preview_token']
        assert apply(client, body, preview).status_code == 409
    else:
        assert current['preview_token'] == preview['preview_token']
        assert apply(client, body, preview).status_code == 200


@pytest.mark.parametrize('selected_index', [0, 2])
def test_split_drops_classifier_label_supported_only_by_detached_frames(app_and_db, selected_index):
    app, db = app_and_db
    ids, path = seed(app, db, 'Western Grebe')
    selected = ids[selected_index:selected_index + 1]
    remaining = [pid for pid in ids if pid not in selected]
    cached = json.load(open(path))
    cached['encounters'][0]['species'] = ['Classifier Spoonbill', .9]
    cached['encounters'][0]['bursts'][0]['species'] = ['Classifier Spoonbill', .9]
    for photo in cached['photos']:
        photo['species_top5'] = [['Classifier Spoonbill', .9, 'test']] if photo['id'] in selected else []
    with open(path, 'w') as handle:
        json.dump(cached, handle)
    client = app.test_client()
    body = {'species': 'Eared Grebe', 'photo_ids': selected, 'source_photo_ids': ids, 'add': True, 'split': True}
    response = apply(client, body, request_preview(client, body))
    assert response.status_code == 200, response.get_json()
    by_ids = {tuple(enc['photo_ids']): enc for enc in response.get_json()['encounters']}
    source, detached = by_ids[tuple(remaining)], by_ids[tuple(selected)]
    assert source['species'] == [None, 0.0]
    assert source['species_predictions'] == []
    assert source['bursts'][0]['species'] == [None, 0.0]
    assert detached['species'] == ['Classifier Spoonbill', .9]
    assert detached['species_predictions'][0]['count'] == 1
    assert detached['bursts'][0]['species_predictions'][0]['count'] == 1
    assert source['confirmed_species_list'] == ['Western Grebe']
    assert set(detached['confirmed_species_list']) == {'Western Grebe', 'Eared Grebe'}
    assert source['species_confirmed'] and detached['species_confirmed']
    assert client.post('/api/undo').status_code == 200
    assert json.load(open(path))['encounters'] == cached['encounters']
    assert client.post('/api/redo').status_code == 200
    assert json.load(open(path))['encounters'] == response.get_json()['encounters']


@pytest.mark.parametrize(('timestamps', 'expected'), [
    ([None, '2026-10-20T10:01:00', '2026-10-20T10:02:00'], [1, 0]),
    (['2026-10-20T10:00:00', None, None], [0, 2]),
    ([None, None, None], [1, 2]),
])
def test_split_recounts_missing_timestamps_for_each_new_membership(app_and_db, timestamps, expected):
    app, db = app_and_db
    ids, path = seed(app, db, 'Western Grebe')
    cached = json.load(open(path))
    for photo, timestamp in zip(cached['photos'], timestamps, strict=True):
        photo['timestamp'] = timestamp
    cached['encounters'][0]['missing_timestamp_count'] = sum(ts is None for ts in timestamps)
    with open(path, 'w') as handle:
        json.dump(cached, handle)
    client = app.test_client()
    body = {'species': 'Eared Grebe', 'photo_ids': ids[:1], 'source_photo_ids': ids, 'add': True, 'split': True}
    response = apply(client, body, request_preview(client, body))
    assert response.status_code == 200, response.get_json()
    by_ids = {tuple(enc['photo_ids']): enc for enc in response.get_json()['encounters']}
    detached, source = by_ids[tuple(ids[:1])], by_ids[tuple(ids[1:])]
    assert [detached['missing_timestamp_count'], source['missing_timestamp_count']] == expected
    assert detached['time_range'] == [timestamps[0], timestamps[0]]
    assert source['time_range'] == [timestamps[1], timestamps[2]]
    assert detached['photo_count'] == 1 and source['photo_count'] == 2
    assert detached['burst_count'] == source['burst_count'] == 1
    assert client.post('/api/undo').status_code == 200
    assert json.load(open(path))['encounters'] == cached['encounters']
    assert client.post('/api/redo').status_code == 200
    assert json.load(open(path))['encounters'] == response.get_json()['encounters']


@pytest.mark.parametrize('split', [False, True])
def test_split_discards_membership_bound_cohorts_without_changing_confirmation_modes(app_and_db, split):
    app, db = app_and_db
    ids, path = seed(app, db, 'Western Grebe')
    cached = json.load(open(path))
    enc = cached['encounters'][0]
    enc.update(species=['Saved classifier candidate', .8], trace=[{'pair': ids[:2]}],
               focus_cohorts={'body': ids})
    enc['bursts'][0].update(trace=[{'pair': ids[:2]}], focus_cohorts={'body': ids})
    with open(path, 'w') as handle:
        json.dump(cached, handle)
    client = app.test_client()
    body = {'species': 'Eared Grebe', 'photo_ids': ids[:1], 'source_photo_ids': ids, 'add': True, 'split': split}
    response = apply(client, body, request_preview(client, body))
    assert response.status_code == 200, response.get_json()
    groups = response.get_json()['encounters']
    assert len(groups) == (2 if split else 1)
    for group in groups:
        assert 'trace' not in group
        assert 'missing_timestamp_count' not in group  # Preserve old cache shape.
        if split:
            assert 'focus_cohorts' not in group
            assert group['species'] == [None, 0.0]
            assert all('trace' not in burst and 'focus_cohorts' not in burst for burst in group['bursts'])
        else:
            assert group['focus_cohorts'] == {'body': ids}
            assert group['bursts'][0]['focus_cohorts'] == {'body': ids}
            assert group['species'] == ['Saved classifier candidate', .8]
    actual = db.photo_species.get_species_for_photos(ids)
    assert set(actual[ids[0]]) == {'Western Grebe', 'Eared Grebe'}
    assert actual[ids[1]] == actual[ids[2]] == ['Western Grebe']
