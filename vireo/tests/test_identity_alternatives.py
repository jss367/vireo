"""Alternative identities are review evidence, not multiple confirmed species."""

import sqlite3

import pytest
from canonical_schema import create_identity_alternatives
from services import photo_ingest

CANDIDATES = ['Tropical Kingbird', "Couch's Kingbird"]


def save(client, ids, **changes):
    return client.post('/api/identity-alternatives', json={
        'photo_ids': ids, 'candidates': CANDIDATES,
        'reason': 'Still photos do not distinguish the pair reliably',
        'missing_evidence': 'A recording of the call', **changes,
    })


def current_group(client):
    return client.get('/api/identity-alternatives').get_json()['groups'][0]


def resolve(client, group, **changes):
    return client.post('/api/identity-alternatives/resolve', json={
        'photo_ids': group['photo_ids'], 'group_id': group['group_id'],
        'species': CANDIDATES[0], **changes,
    })


def test_31_kingbird_frames_are_one_unresolved_group_without_species_or_flag_changes(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    folder = db.add_folder('/kingbirds')
    ids = [photo_ingest.add_photo(db, folder_id=folder, filename=f'kingbird-{n}.jpg',
                                extension='.jpg', file_size=100, file_mtime=1) for n in range(31)]
    db.photo_review.set_flag(ids[0], 'flagged')
    db.photo_review.set_flag(ids[-1], 'rejected')
    flags = {pid: db.photos.get(pid)['flag'] for pid in ids}
    result = save(client, ids)
    assert result.status_code == 200
    payload = client.get('/api/identity-alternatives').get_json()
    assert payload['photo_count'] == 31
    assert len(payload['groups']) == 1
    group = payload['groups'][0]
    assert group['photo_ids'] == ids
    assert group['candidates'] == CANDIDATES
    assert group['status'] == 'unresolved'
    assert group['missing_evidence'] == 'A recording of the call'
    assert db.photo_species.get_species_for_photos(ids) == {}
    assert db.keyword_taxa.species_names_matching('Kingbird', False, False) == []
    assert {pid: db.photos.get(pid)['flag'] for pid in ids} == flags
    assert db.identity_alternatives.photo_ids() == set(ids)


def test_marking_confirmations_uncertain_removes_only_candidates_and_undo_restores_provenance(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    first = db.add_keyword(CANDIDATES[0], is_species=True)
    second = db.add_keyword(CANDIDATES[1], is_species=True)
    other = db.add_keyword('Great Egret', is_species=True)
    db.tag_photo(1, first, source='accept')
    db.tag_photo(1, second)
    db.tag_photo(1, other)
    db.photo_review.set_flag(1, 'rejected')
    assert save(client, [1]).status_code == 200
    assert db.photo_species.get_species_for_photos([1]) == {1: ['Great Egret']}
    assert client.post('/api/undo').status_code == 200
    assert set(db.photo_species.get_species_for_photos([1])[1]) == {*CANDIDATES, 'Great Egret'}
    assert not db.identity_alternatives.photo_ids()
    assert db.conn.execute('SELECT source FROM photo_keywords WHERE photo_id=1 AND keyword_id=?', (first,)).fetchone()[0] == 'accept'
    assert client.post('/api/redo').status_code == 200
    assert db.photo_species.get_species_for_photos([1]) == {1: ['Great Egret']}
    assert db.identity_alternatives.photo_ids() == {1}
    assert db.photos.get(1)['flag'] == 'rejected'


def test_resolution_is_one_audited_undoable_edit_and_preserves_genuine_mixed_species(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    other = db.add_keyword('Great Egret', is_species=True)
    db.tag_photo(1, other)
    db.photo_review.set_flag(1, 'flagged')
    assert save(client, [1, 2]).status_code == 200
    group = current_group(client)
    result = resolve(client, group)
    assert result.status_code == 200
    assert not result.get_json()['flags_changed']
    assert client.get('/api/identity-alternatives').get_json()['groups'] == []
    assert db.photo_species.get_species_for_photos([1, 2]) == {
        1: ['Great Egret', CANDIDATES[0]], 2: [CANDIDATES[0]],
    }
    latest = db.edit_history.latest_undoable()
    assert latest['id'] == result.get_json()['entry_id']
    assert 'Resolved species' in latest['description']
    assert client.post('/api/undo').status_code == 200
    assert current_group(client) == group
    assert db.photo_species.get_species_for_photos([1, 2]) == {1: ['Great Egret']}
    assert client.post('/api/redo').status_code == 200
    assert db.photo_species.get_species_for_photos([1])[1] == ['Great Egret', CANDIDATES[0]]
    assert db.photos.get(1)['flag'] == 'flagged'
    assert db.identity_alternatives.photo_ids() == set()


def test_partial_resolution_keeps_other_frames_unresolved(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    save(client, [1, 2, 3])
    group = current_group(client)
    assert resolve(client, group, photo_ids=[1]).status_code == 200
    assert current_group(client)['photo_ids'] == [2, 3]
    assert db.photo_species.get_species_for_photos([1, 2, 3]) == {1: [CANDIDATES[0]]}


def test_reason_and_missing_evidence_can_be_revisited_with_history(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    save(client, [1, 2])
    group = current_group(client)
    assert save(client, [1, 2], group_id=group['group_id'], missing_evidence='An audio recording or expert review').status_code == 200
    assert current_group(client)['missing_evidence'] == 'An audio recording or expert review'
    assert client.post('/api/undo').status_code == 200
    assert current_group(client) == group


def test_stale_group_and_foreign_photos_fail_without_partial_writes(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    save(client, [1, 2])
    group = current_group(client)
    assert save(client, [1, 2]).status_code == 409
    assert resolve(client, group, group_id='outdated').status_code == 409
    assert resolve(client, group, photo_ids=[1, 3]).status_code == 409
    assert save(client, [1, 99999]).status_code == 404
    assert resolve(client, group, species='Great Egret').status_code == 400
    assert current_group(client) == group
    assert db.photo_species.get_species_for_photos([1, 2]) == {}


@pytest.mark.parametrize('changes', [
    {'photo_ids': [True]}, {'photo_ids': [0]}, {'photo_ids': '1'}, {'photo_ids': []},
    {'candidates': ['One']}, {'candidates': ['One', 'one']}, {'candidates': ['One', 2]},
    {'candidates': ['One', '']}, {'reason': ''}, {'reason': []}, {'missing_evidence': 3},
])
def test_invalid_payload_does_not_write_review_state(app_and_db, changes):
    app, db = app_and_db
    client = app.test_client()
    assert save(client, [1], **changes).status_code == 400
    assert not db.identity_alternatives.photo_ids()
    assert not db.edit_history.latest_undoable()


def test_groups_are_durable_and_workspace_scoped(app_and_db, tmp_path):
    from db import Database
    app, db = app_and_db
    client = app.test_client()
    save(client, [1, 2])
    with Database(str(tmp_path / 'test.db')) as reopened:
        assert reopened.identity_alternatives.photo_ids() == {1, 2}
        other_ws = reopened.create_workspace('Other')
        reopened.set_active_workspace(other_ws)
        assert reopened.identity_alternatives.photo_ids() == set()


def test_failure_partway_through_batch_rolls_back_tags_state_and_history(app_and_db, monkeypatch):
    from repositories.identity_alternatives import IdentityAlternativesRepository
    app, db = app_and_db
    client = app.test_client()
    app.config['TESTING'] = True
    kid = db.add_keyword(CANDIDATES[0], is_species=True)
    db.tag_photo(1, kid)
    original = IdentityAlternativesRepository.set_state

    def fail_second(self, photo_id, state):
        if photo_id == 2:
            raise sqlite3.OperationalError('injected failure')
        return original(self, photo_id, state)

    monkeypatch.setattr(IdentityAlternativesRepository, 'set_state', fail_second)
    assert save(client, [1, 2]).status_code == 500
    assert not db.identity_alternatives.photo_ids()
    assert db.photo_species.get_species_for_photos([1]) == {1: [CANDIDATES[0]]}
    assert not db.edit_history.latest_undoable()


def test_schema_migration_is_idempotent(db):
    db.conn.execute('DROP TABLE photo_identity_alternatives')
    create_identity_alternatives(db.conn)
    create_identity_alternatives(db.conn)
    assert db.identity_alternatives.photo_ids() == set()


def test_detached_frames_are_hidden_and_cannot_be_resolved(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    save(client, [1, 2])
    group = current_group(client)
    db.remove_workspace_folder(db.require_workspace_id(), db.photos.get(1)['folder_id'])
    assert db.identity_alternatives.photo_ids() == {2}
    assert current_group(client)['photo_ids'] == [2]
    assert resolve(client, group).status_code == 404


def test_export_uses_unknown_name_and_does_not_write_candidate_species(app_and_db, tmp_path, monkeypatch):
    import export as export_module
    from PIL import Image
    app, db = app_and_db
    client = app.test_client()
    source = tmp_path / 'source'
    source.mkdir()
    Image.new('RGB', (16, 16), 'green').save(source / 'bird.jpg')
    folder = db.add_folder(str(source))
    pid = photo_ingest.add_photo(db, folder_id=folder, filename='bird.jpg', extension='.jpg', file_size=100, file_mtime=1)
    kid = db.add_keyword(CANDIDATES[0], is_species=True)
    db.tag_photo(pid, kid)
    assert save(client, [pid]).status_code == 200
    written_args = []

    def write_metadata(jobs, cancel_check=None):
        written_args.extend(arg for _, _, args in jobs for arg in args)
        return len(jobs), [], [out for out, _, _ in jobs]

    monkeypatch.setattr(export_module, '_write_export_metadata_batch', write_metadata)
    destination = tmp_path / 'exports'
    result = export_module.export_photos(
        db=db, vireo_dir=str(tmp_path), photo_ids=[pid], destination=str(destination),
        options={'naming_template': '{species}-{original}', 'metadata_fields': ['species', 'location']},
    )
    assert result['exported'] == 1, result
    assert (destination / 'unknown-bird.jpg').exists()
    assert not any('Kingbird' in arg for arg in written_args)


def test_cache_refresh_failure_reports_saved_edit_and_warning(app_and_db, monkeypatch):
    import pipeline
    app, db = app_and_db
    client = app.test_client()

    def fail_refresh(*args, **kwargs):
        raise OSError('cache is read-only')

    monkeypatch.setattr(pipeline, 'refresh_cache_species_for_photos', fail_refresh)
    response = save(client, [1])
    assert response.status_code == 200
    assert response.get_json()['pipeline_cache_stale']
    assert 'saved' in response.get_json()['warning']
    assert db.identity_alternatives.photo_ids() == {1}


def test_process_cache_and_summary_do_not_claim_unresolved_candidates(app_and_db, tmp_path):
    from pipeline import load_results_raw, save_results_raw
    app, db = app_and_db
    client = app.test_client()
    kid = db.add_keyword(CANDIDATES[0], is_species=True)
    db.tag_photo(1, kid)
    cache = {
        'photos': [{'id': 1, 'confirmed_species': CANDIDATES[0], 'confirmed_species_list': [CANDIDATES[0]]}],
        'encounters': [{'photo_ids': [1], 'confirmed_species': CANDIDATES[0],
                        'confirmed_species_list': [CANDIDATES[0]], 'species_confirmed': True,
                        'bursts': [{'photo_ids': [1], 'species_override': {'species': CANDIDATES[0], 'confirmed': True}}]}],
    }
    save_results_raw(cache, str(tmp_path), db.active_workspace_id)
    assert save(client, [1]).status_code == 200
    updated = load_results_raw(str(tmp_path), db.active_workspace_id)
    assert updated['photos'][0]['confirmed_species_list'] == []
    assert not updated['encounters'][0]['species_confirmed']
    assert not updated['encounters'][0]['confirmed_species_list']
    assert client.post('/api/undo').status_code == 200
    restored = load_results_raw(str(tmp_path), db.active_workspace_id)
    assert restored['photos'][0]['confirmed_species_list'] == [CANDIDATES[0]]
