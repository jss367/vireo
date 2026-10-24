"""Compact/full confirmation contracts and atomic additive multi-species batches."""
import copy
import json
from pathlib import Path

import pytest
from test_encounter_confirmation_preview import apply, request_preview
from test_encounter_confirmation_preview import seed as source_seed


def seed(app, db, species=None):
    from pipeline import load_results_raw, save_results_raw
    ids, path = source_seed(app, db, species)
    cache_dir = str(Path(path).parent)
    save_results_raw(load_results_raw(cache_dir, db.active_workspace_id), cache_dir, db.active_workspace_id)
    return ids, path


def assert_undo(client, result):
    undo = result['undo']
    assert undo['requires_latest'] is True
    response = client.post(undo['endpoint'], json={'edit_id': undo['edit_id']})
    assert response.status_code == 200, response.json


@pytest.mark.parametrize('mode', ['full', 'compact'])
def test_confirmation_contract_counts_identity_warnings_and_undo(app_and_db, mode):
    app, db = app_and_db
    client = app.test_client()
    ids, path = seed(app, db, 'American Wigeon')
    before = json.loads(Path(path).read_text())
    kid = db.add_keyword('Blue Jay', is_species=True)
    db.tag_photo(ids[0], kid)
    db.photo_review.set_flag(ids[0], 'flagged')
    db.photo_review.set_flag(ids[1], 'rejected')
    db.detections.save(ids[1], [{'box': {'x': .1, 'y': .1, 'w': .2, 'h': .3},
                               'confidence': .01, 'category': 'animal'}], detector_model='test')
    body = {'species': 'blue jay', 'photo_ids': ids[:2], 'source_photo_ids': ids, 'add': True,
            'response_mode': mode}
    result = apply(client, body, request_preview(client, body)).json
    assert result['ok'] and result['response_mode'] == mode
    assert result['species'] == 'Blue Jay'
    assert result['low_confidence_photo_ids'] == ids[1:2]
    if mode == 'full':
        assert result['encounters'] == json.loads(Path(path).read_text())['encounters']
        assert 'encounter_changes' not in result
    else:
        assert 'encounters' not in result
        assert result['photo_ids'] == ids[:2]
        assert result['changed_photo_ids'] == ids[1:2]
        assert result['unchanged_photo_ids'] == ids[:1]
        assert result['counts'] == {'selected': 2, 'changed': 1, 'unchanged': 1, 'added': 1, 'removed': 0}
        assert result['keyword']['id'] == kid
        assert result['keyword']['name'] == 'Blue Jay'
        assert result['warnings'] == [{'code': 'low_detector_confidence', 'photo_ids': ids[1:2]}]
        assert_undo(client, result)
        assert json.loads(Path(path).read_text())['encounters'] == before['encounters']
        assert set(db.photo_species.get_species_for_photos(ids)[ids[0]]) == {'Blue Jay', 'American Wigeon'}
    assert [db.photos.get(pid)['flag'] for pid in ids[:2]] == ['flagged', 'rejected']


def test_compact_noop_has_no_unrelated_undo_reference(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids, path = seed(app, db)
    body = {'species': 'Blue Jay', 'photo_ids': ids, 'add': True, 'response_mode': 'compact'}
    first = client.post('/api/encounters/species', json=body).json
    second = client.post('/api/encounters/species', json=body).json
    assert first['undo']
    assert second['undo'] is None
    assert second['counts']['changed'] == 0
    assert second['counts']['unchanged'] == len(ids)
    assert second['cache_changed'] is False
    assert second['encounter_changes']['removed'] == []
    assert second['encounter_changes']['upserts'] == []


def test_compact_split_returns_only_affected_encounters(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids, path = seed(app, db, 'Western Grebe')
    cached = json.loads(Path(path).read_text())
    unrelated = copy.deepcopy(cached['encounters'][0])
    unrelated['photo_ids'] = [99999]
    cached['encounters'].append(unrelated)
    Path(path).write_text(json.dumps(cached))
    body = {'species': 'Eared Grebe', 'previous_species': 'Western Grebe', 'photo_ids': ids[1:],
            'source_photo_ids': ids, 'split': True, 'response_mode': 'compact'}
    result = apply(client, body, request_preview(client, body)).json
    assert result['counts'] == {'selected': 2, 'changed': 2, 'unchanged': 0, 'added': 2, 'removed': 2}
    assert result['encounter_changes']['removed'] == [ids]
    upserts = result['encounter_changes']['upserts']
    assert len(upserts) == 2
    assert all(99999 not in u['encounter']['photo_ids'] for u in upserts)
    assert_undo(client, result)


@pytest.mark.parametrize('cached', [True, False])
def test_bulk_distinct_and_overlapping_targets_one_undo_preserves_flags_and_subjects(app_and_db, cached):
    app, db = app_and_db
    client = app.test_client()
    ids, path = seed(app, db, 'American Wigeon')
    if not cached:
        Path(path).unlink()
    db.photo_review.set_flag(ids[0], 'flagged')
    db.photo_review.set_flag(ids[1], 'rejected')
    dids = db.detections.save(ids[0], [
        {'box': {'x': x, 'y': .1, 'w': .2, 'h': .3}, 'confidence': .01, 'category': 'animal'}
        for x in (.1, .6)], detector_model='test')
    for did, name in zip(dids, ['Blue Jay', 'Cardinal'], strict=True):
        db.predictions.add(detection_id=did, species=name, confidence=.9, model='test')
    preds = [row['id'] for row in db.predictions.get_rows(photo_ids=ids)]
    assignments = [
        {'species': 'Blue Jay', 'photo_ids': ids[:1], 'detection_ids': dids[:1]},
        {'species': 'Eared Grebe', 'photo_ids': ids[:2]},
        {'species': 'Blue Jay', 'photo_ids': ids[:1]},  # ordered duplicate becomes a no-op
    ]
    before = db.photo_species.get_species_for_photos(ids)
    result = client.post('/api/encounters/species/bulk', json={'assignments': assignments}).json
    assert result['ok'] and result['atomic'] and result['response_mode'] == 'compact'
    assert result['counts'] == {'assignments': 3, 'selected': 2, 'changed': 2, 'unchanged': 0, 'added': 3}
    assert result['assignments'][0]['detection_ids'] == dids[:1]
    assert result['assignments'][2]['counts']['changed'] == 0
    actual = db.photo_species.get_species_for_photos(ids)
    assert set(actual[ids[0]]) == {'American Wigeon', 'Blue Jay', 'Eared Grebe'}
    assert set(actual[ids[1]]) == {'American Wigeon', 'Eared Grebe'}
    assert actual[ids[2]] == ['American Wigeon']
    assert all(db.predictions.current_status(pred) == 'pending' for pred in preds)
    assert client.get('/api/undo/status').json['count'] == 1
    assert result['warnings']
    assert_undo(client, result)
    assert db.photo_species.get_species_for_photos(ids) == before
    assert [db.photos.get(pid)['flag'] for pid in ids[:2]] == ['flagged', 'rejected']
    assert client.post('/api/redo').status_code == 200
    assert db.photo_species.get_species_for_photos(ids) == actual


@pytest.mark.parametrize('invalid', [
    {'species': 'Robin', 'photo_ids': [99999]},
    {'species': '', 'photo_ids': [1]},
    {'species': 'Robin', 'photo_ids': [1], 'detection_ids': [99999]},
    {'species': 'Robin', 'photo_ids': [1], 'add': False},
])
def test_bulk_invalid_later_selection_writes_nothing(app_and_db, invalid):
    app, db = app_and_db
    client = app.test_client()
    ids, path = seed(app, db)
    before = Path(path).read_bytes()
    response = client.post('/api/encounters/species/bulk', json={'assignments': [
        {'species': 'Blue Jay', 'photo_ids': ids}, invalid]})
    assert response.status_code == 400
    assert 'Assignment 1:' in response.json['error']
    assert Path(path).read_bytes() == before
    assert not db.photo_species.get_species_for_photos(ids)
    assert client.get('/api/undo/status').json['count'] == 0


def test_bulk_late_save_failure_restores_catalog_cache_and_history(app_and_db, monkeypatch):
    import pipeline
    app, db = app_and_db
    ids, path = seed(app, db)
    before = Path(path).read_bytes()
    original = pipeline.save_results_raw
    calls = []
    def fail_after_replace(*args):
        original(*args)
        calls.append(True)
        if len(calls) == 1:
            raise OSError('invented late save failure')
    monkeypatch.setattr(pipeline, 'save_results_raw', fail_after_replace)
    response = app.test_client().post('/api/encounters/species/bulk', json={'assignments': [
        {'species': 'Blue Jay', 'photo_ids': ids[:1]}, {'species': 'Robin', 'photo_ids': ids[1:]}]})
    assert response.status_code == 500
    assert json.loads(Path(path).read_bytes()) == json.loads(before)
    assert not db.photo_species.get_species_for_photos(ids)
    assert not db.edit_history.list_recent()
    assert not db.pending_changes.list_all()


def test_undo_reference_refuses_to_reverse_a_newer_edit(app_and_db):
    app, db = app_and_db
    ids, _ = seed(app, db)
    client = app.test_client()
    result = client.post('/api/encounters/species/bulk', json={'assignments': [
        {'species': 'Blue Jay', 'photo_ids': ids[:1]}]}).json
    client.post('/api/encounters/species', json={'species': 'Robin', 'photo_ids': ids[1:]})
    response = client.post('/api/undo', json={'edit_id': result['undo']['edit_id']})
    assert response.status_code == 409
    assert db.photo_species.get_species_for_photos(ids)[ids[1]] == ['Robin']


@pytest.mark.parametrize('body', [{'response_mode': 'invalid', 'species': 'Robin', 'photo_ids': [1]}, [1]])
def test_response_mode_and_json_shape_are_validated(app_and_db, body):
    app, _ = app_and_db
    assert app.test_client().post('/api/encounters/species', json=body).status_code == 400


def test_bulk_response_mode_and_selection_shapes(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    for body in [None, [], {}, {'assignments': []}, {'assignments': [1]},
                 {'assignments': [{'species': 'Robin', 'photo_ids': [True]}]},
                 {'assignments': [{'species': 'Robin', 'photo_ids': [1, 1]}]},
                 {'assignments': [{'species': 'Robin', 'photo_ids': [1]}], 'response_mode': 'full'}]:
        assert client.post('/api/encounters/species/bulk', json=body).status_code == 400


def test_versioned_bulk_route_and_noop_history(app_and_db):
    app, db = app_and_db
    ids, _ = seed(app, db, 'Blue Jay')
    client = app.test_client()
    body = {'assignments': [{'species': 'Blue Jay', 'photo_ids': ids}]}
    first = client.post('/api/v1/encounters/species/bulk', json=body, headers={'X-Vireo-Token': 'test-token-123'})
    assert first.status_code == 200, first.json
    # Refreshing derived cache evidence can be an undoable cache-only change.
    second = client.post('/api/v1/encounters/species/bulk', json=body, headers={'X-Vireo-Token': 'test-token-123'}).json
    assert second['counts']['changed'] == 0
    assert second['undo'] is None
    assert second['cache_changed'] is False


def test_compact_removal_counts_and_cache_only_undo(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    ids, path = seed(app, db)
    kid = db.add_keyword('Blue Jay', is_species=True)
    for pid in ids:
        db.tag_photo(pid, kid)
    body = {'species': 'Blue Jay', 'photo_ids': ids, 'response_mode': 'compact'}
    confirmed = client.post('/api/encounters/species', json=body).json
    assert confirmed['counts']['changed'] == 0
    assert confirmed['cache_changed'] is True
    assert_undo(client, confirmed)
    assert all(names == ['Blue Jay'] for names in db.photo_species.get_species_for_photos(ids).values())
    assert client.post('/api/redo').status_code == 200
    body = {**body, 'photo_ids': ids[:1], 'source_photo_ids': ids, 'remove': True}
    removed = apply(client, body, request_preview(client, body)).json
    assert removed['counts'] == {'selected': 1, 'changed': 1, 'unchanged': 0, 'added': 0, 'removed': 1}
    assert removed['keyword']['id'] == kid
    assert_undo(client, removed)
    assert db.photo_species.get_species_for_photos(ids)[ids[0]] == ['Blue Jay']


def test_compact_does_not_normalize_or_serialize_unrelated_legacy_bursts(app_and_db):
    app, db = app_and_db
    ids, path = seed(app, db)
    cached = json.loads(Path(path).read_text())
    cached['encounters'].append({'photo_ids': [99999], 'bursts': [[99999]], 'burst_count': 1})
    Path(path).write_text(json.dumps(cached))
    result = app.test_client().post('/api/encounters/species', json={
        'species': 'Blue Jay', 'photo_ids': ids, 'response_mode': 'compact'}).json
    assert result['encounter_changes']['removed'] == [ids]
    assert len(result['encounter_changes']['upserts']) == 1
    assert json.loads(Path(path).read_text())['encounters'][-1] == cached['encounters'][-1]


def test_bulk_failure_after_first_addition_rolls_back_all_selections(app_and_db, monkeypatch):
    from db import Database
    app, db = app_and_db
    ids, path = seed(app, db)
    before = Path(path).read_bytes()
    original = Database.tag_photo
    calls = []
    def fail_on_second(self, *args, **kwargs):
        calls.append(True)
        if len(calls) == 2:
            raise ValueError('invented second-assignment failure')
        return original(self, *args, **kwargs)
    monkeypatch.setattr(Database, 'tag_photo', fail_on_second)
    response = app.test_client().post('/api/encounters/species/bulk', json={'assignments': [
        {'species': 'Blue Jay', 'photo_ids': ids[:1]}, {'species': 'Eared Grebe', 'photo_ids': ids[1:]}]})
    assert response.status_code == 500
    assert Path(path).read_bytes() == before
    assert not db.photo_species.get_species_for_photos(ids)
    assert not db.pending_changes.list_all()
    assert not db.edit_history.list_recent()


def test_compact_patch_digest_binds_ordered_encounter_and_burst_memberships():
    import hashlib

    from services.species_assignment_results import encounter_changes, encounter_structure_digest
    before = {'encounters': [{'photo_ids': [2, 1], 'bursts': [[2], {'photo_ids': [1]}]}]}
    expected = hashlib.sha256(b'[{"photo_ids":[2,1],"bursts":[[2],[1]]}]').hexdigest()
    assert encounter_structure_digest(before) == expected
    after = copy.deepcopy(before)
    after['encounters'][0]['confirmed_species'] = 'Blue Jay'
    assert encounter_changes(before, after)['base_structure_digest'] == expected
    assert encounter_structure_digest(after) == expected
    after['encounters'][0]['bursts'].reverse()
    assert encounter_structure_digest(after) != expected
