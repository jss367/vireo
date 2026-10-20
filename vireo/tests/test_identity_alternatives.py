"""Alternative identities are review evidence, not multiple confirmed species."""

import json
import sqlite3

import pytest
from canonical_schema import create_identity_alternatives
from services import photo_ingest

CANDIDATES = ['Tropical Kingbird', "Couch's Kingbird"]


@pytest.mark.parametrize('operation', ['merge', 'rename'])
@pytest.mark.parametrize('history_state', ['live', 'undone', 'resolved'])
def test_keyword_names_follow_live_and_empty_link_identity_history(app_and_db, operation, history_state):
    app, db = app_and_db
    client = app.test_client()
    source = db.add_keyword(CANDIDATES[0], is_species=True)
    target_name = CANDIDATES[1] if operation == 'merge' else 'Renamed Kingbird'
    survivor = db.add_keyword(target_name, is_species=True) if operation == 'merge' else source
    assert save(client, [1]).status_code == 200
    if history_state == 'undone':
        assert client.post('/api/undo').status_code == 200
    elif history_state == 'resolved':
        assert resolve(client, current_group(client)).status_code == 200
    assert client.put(f'/api/keywords/{source}', json={'name': target_name}).status_code == 200
    if history_state == 'undone':
        assert client.post('/api/redo').status_code == 200
    elif history_state == 'resolved':
        assert client.post('/api/undo').status_code == 200
    expected = [target_name] if operation == 'merge' else [target_name, CANDIDATES[1]]
    group = current_group(client)
    assert group['candidates'] == expected
    assert group['missing_evidence'] == 'A recording of the call'
    assert resolve(client, group, species=target_name).status_code == 200
    assert db.photo_species.get_species_for_photos([1]) == {1: [target_name]}
    assert db.conn.execute('SELECT id FROM keywords WHERE name=?', (CANDIDATES[0],)).fetchone() is None
    assert db.conn.execute(
        'SELECT 1 FROM photo_keywords WHERE photo_id=1 AND keyword_id=?', (survivor,),
    ).fetchone() is not None
    assert client.post('/api/undo').status_code == 200
    assert current_group(client)['candidates'] == expected
    assert client.post('/api/redo').status_code == 200
    assert db.photo_species.get_species_for_photos([1]) == {1: [target_name]}


@pytest.mark.parametrize('merge_after_undo', [False, True])
@pytest.mark.parametrize('operation', ['save', 'resolve'])
@pytest.mark.parametrize('preexisting_survivor', [False, True])
def test_keyword_merge_retargets_identity_history_without_removing_existing_survivor(
    app_and_db, operation, preexisting_survivor, merge_after_undo,
):
    app, db = app_and_db
    client = app.test_client()
    db.photo_review.set_flag(1, 'rejected')
    source = db.add_keyword(CANDIDATES[0], is_species=True)
    survivor = db.add_keyword('Surviving Kingbird', is_species=True)
    if preexisting_survivor:
        db.tag_photo(1, survivor)
    if operation == 'save':
        db.tag_photo(1, source, source='accept')
    assert save(client, [1]).status_code == 200
    if operation == 'resolve':
        assert resolve(client, current_group(client)).status_code == 200
    if merge_after_undo:
        assert client.post('/api/undo').status_code == 200
    db._merge_keyword_into(source, survivor)
    db.commit()
    if merge_after_undo:
        assert client.post('/api/redo').status_code == 200

    assert client.post('/api/undo').status_code == 200
    expected = ['Surviving Kingbird'] if operation == 'save' or preexisting_survivor else []
    assert db.photo_species.get_species_for_photos([1]).get(1, []) == expected
    assert db.identity_alternatives.photo_ids() == ({1} if operation == 'resolve' else set())
    if operation == 'save':
        stamp = db.conn.execute(
            'SELECT source FROM photo_keywords WHERE photo_id=1 AND keyword_id=?', (survivor,),
        ).fetchone()[0]
        assert stamp == ('manual' if preexisting_survivor else 'accept')
    assert client.post('/api/redo').status_code == 200
    expected = ['Surviving Kingbird'] if operation == 'resolve' or preexisting_survivor else []
    assert db.photo_species.get_species_for_photos([1]).get(1, []) == expected
    assert db.identity_alternatives.photo_ids() == ({1} if operation == 'save' else set())
    assert db.photos.get(1)['flag'] == 'rejected'


@pytest.mark.parametrize('merge_after_undo', [False, True])
def test_keyword_merge_folds_converged_identity_snapshot_provenance(app_and_db, merge_after_undo):
    app, db = app_and_db
    client = app.test_client()
    source = db.add_keyword(CANDIDATES[0], is_species=True)
    survivor = db.add_keyword(CANDIDATES[1], is_species=True)
    db.tag_photo(1, source)
    db.tag_photo(1, survivor, source='accept')
    assert save(client, [1]).status_code == 200
    if merge_after_undo:
        assert client.post('/api/undo').status_code == 200
    db._merge_keyword_into(source, survivor)
    db.commit()
    if merge_after_undo:
        assert client.post('/api/redo').status_code == 200
    assert client.post('/api/undo').status_code == 200
    assert db.photo_species.get_species_for_photos([1]) == {1: [CANDIDATES[1]]}
    assert db.conn.execute(
        'SELECT source FROM photo_keywords WHERE photo_id=1 AND keyword_id=?', (survivor,),
    ).fetchone()[0] == 'manual'
    assert client.post('/api/redo').status_code == 200
    assert db.photo_species.get_species_for_photos([1]) == {}


@pytest.mark.parametrize('source_name', [CANDIDATES[0], 'tropical KINGBIRD'])
@pytest.mark.parametrize('maintenance', ['merge', 'rename'])
@pytest.mark.parametrize('collapse', [False, True])
def test_keyword_maintenance_retargets_empty_link_groups_and_both_history_states(app_and_db, maintenance, collapse, source_name):
    app, db = app_and_db
    client = app.test_client()
    source = db.add_keyword(source_name, is_species=True)
    target_name = CANDIDATES[1] if collapse else 'Surviving Kingbird'
    db.photo_review.set_flag(1, 'rejected')
    assert save(client, [1]).status_code == 200
    group = current_group(client)
    assert save(client, [1], group_id=group['group_id'], reason='Still awaiting a call').status_code == 200
    if maintenance == 'merge':
        survivor = db.add_keyword(target_name, is_species=True)
        db._merge_keyword_into(source, survivor)
        db.commit()
    else:
        assert client.put(f'/api/keywords/{source}', json={'name': target_name}).status_code == 200
    expected = [target_name] if collapse else [target_name, CANDIDATES[1]]
    assert current_group(client)['candidates'] == expected
    assert db.photo_species.get_species_for_photos([1]) == {}
    assert db.identity_alternatives.photo_ids() == {1}
    assert client.post('/api/undo').status_code == 200
    assert current_group(client)['candidates'] == expected
    assert current_group(client)['reason'] == group['reason']
    assert current_group(client)['missing_evidence'] == group['missing_evidence']
    assert client.post('/api/redo').status_code == 200
    assert current_group(client)['candidates'] == expected
    # A collapsed group can still update evidence without confirming it.
    group = current_group(client)
    assert save(client, [1], group_id=group['group_id'], candidates=expected).status_code == 200
    assert not db.photo_species.get_species_for_photos([1])
    assert resolve(client, current_group(client), species=target_name).status_code == 200
    assert db.photo_species.get_species_for_photos([1]) == {1: [target_name]}
    assert db.conn.execute('SELECT id FROM keywords WHERE name=? COLLATE NOCASE', (CANDIDATES[0],)).fetchone() is None
    assert db.photos.get(1)['flag'] == 'rejected'
    rows = db.conn.execute(
        "SELECT i.old_value, i.new_value FROM edit_history_items i "
        "JOIN edit_history e ON e.id=i.edit_id WHERE e.action_type='identity_alternatives'",
    )
    for row in rows:
        for payload in row:
            identity = json.loads(payload)['identity']
            if identity:
                assert identity['candidates'] == expected


@pytest.mark.parametrize('maintenance', ['merge', 'rename', 'normalize'])
def test_keyword_maintenance_retargets_resolved_history_before_undo(app_and_db, maintenance):
    app, db = app_and_db
    client = app.test_client()
    source = db.add_keyword(CANDIDATES[0], is_species=True)
    assert save(client, [1]).status_code == 200
    assert resolve(client, current_group(client)).status_code == 200
    target_name = 'Surviving Kingbird'
    if maintenance == 'merge':
        survivor = db.add_keyword(target_name, is_species=True)
        db._merge_keyword_into(source, survivor)
    elif maintenance == 'rename':
        db.update_keyword(source, name=target_name)
    else:
        # Internal canonicalization and collision renames share this hook.
        db._rename_keyword_dependents(source, CANDIDATES[0], target_name)
        db.keyword_tree.apply_update(source, {'name': target_name})
    db.commit()
    assert client.post('/api/undo').status_code == 200
    group = current_group(client)
    assert group['candidates'] == [target_name, CANDIDATES[1]]
    assert not db.photo_species.get_species_for_photos([1])
    assert client.post('/api/redo').status_code == 200
    assert db.photo_species.get_species_for_photos([1]) == {1: [target_name]}


@pytest.mark.parametrize('maintenance', ['merge', 'rename', 'dependents'])
def test_non_species_keyword_maintenance_leaves_same_named_alternatives_unchanged(app_and_db, maintenance):
    app, db = app_and_db
    client = app.test_client()
    source = db.add_keyword(CANDIDATES[0], kw_type='individual')
    assert save(client, [1]).status_code == 200
    before = current_group(client)
    if maintenance == 'merge':
        survivor = db.add_keyword('An individual bird', kw_type='individual')
        db._merge_keyword_into(source, survivor)
    elif maintenance == 'rename':
        db.update_keyword(source, name='An individual bird')
    else:
        db._rename_keyword_dependents(source, CANDIDATES[0], 'An individual bird')
    db.commit()
    assert current_group(client) == before
    assert client.post('/api/undo').status_code == 200
    assert client.post('/api/redo').status_code == 200
    assert current_group(client) == before


def save(client, ids, **changes):
    workspace_id = client.get('/api/identity-alternatives').get_json()['workspace_id']
    return client.post('/api/identity-alternatives', json={
        'photo_ids': ids, 'candidates': CANDIDATES,
        'observed_workspace_id': workspace_id,
        'reason': 'Still photos do not distinguish the pair reliably',
        'missing_evidence': 'A recording of the call', **changes,
    })


def current_group(client):
    payload = client.get('/api/identity-alternatives').get_json()
    return {**payload['groups'][0], 'observed_workspace_id': payload['workspace_id']}


def resolve(client, group, **changes):
    return client.post('/api/identity-alternatives/resolve', json={
        'photo_ids': group['photo_ids'], 'group_id': group['group_id'],
        'observed_workspace_id': group['observed_workspace_id'],
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
    assert current_group(client)['group_id'] == group['group_id']
    assert current_group(client)['missing_evidence'] == group['missing_evidence']
    assert db.photo_species.get_species_for_photos([1, 2, 3]) == {1: [CANDIDATES[0]]}
    assert client.post('/api/undo').status_code == 200
    assert current_group(client) == group
    assert db.photo_species.get_species_for_photos([1, 2, 3]) == {}
    assert client.post('/api/redo').status_code == 200
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


def test_candidate_hierarchy_delimiter_error_explains_the_restriction(app_and_db):
    app, db = app_and_db
    response = save(app.test_client(), [1], candidates=['Kingbird|Tropical', "Couch's Kingbird"])
    assert response.status_code == 400
    assert "'|' hierarchy delimiter" in response.get_json()['error']
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


def test_species_decisions_and_history_refresh_all_shared_workspace_caches(app_and_db, tmp_path):
    from pipeline import load_results_raw, save_results_raw
    app, db = app_and_db
    client = app.test_client()
    active_ws = db.require_workspace_id()
    folder_shared = db.create_workspace('Shared folder')
    photo_shared = db.create_workspace('Shared photo only')
    db.add_workspace_folder(folder_shared, db.photos.get(1)['folder_id'])
    db.photo_visibility.grant(photo_shared, [1])
    db.commit()
    kid = db.add_keyword(CANDIDATES[0], is_species=True)
    db.tag_photo(1, kid)
    workspaces = [active_ws, folder_shared, photo_shared]
    for ws_id in workspaces:
        save_results_raw({
            'photos': [{'id': 1, 'confirmed_species': CANDIDATES[0], 'confirmed_species_list': [CANDIDATES[0]]}],
            'encounters': [{'photo_ids': [1], 'confirmed_species': CANDIDATES[0],
                            'confirmed_species_list': [CANDIDATES[0]], 'species_confirmed': True,
                            'bursts': [{'photo_ids': [1], 'species_override': {'species': CANDIDATES[0], 'confirmed': True}}]}],
        }, str(tmp_path), ws_id)

    def assert_species(names):
        for ws_id in workspaces:
            cache = load_results_raw(str(tmp_path), ws_id)
            assert cache['photos'][0]['confirmed_species_list'] == names, ws_id
            assert cache['encounters'][0]['confirmed_species_list'] == names, ws_id
            assert cache['encounters'][0]['species_confirmed'] == bool(names), ws_id

    assert save(client, [1]).status_code == 200
    assert_species([])
    assert client.post('/api/undo').status_code == 200
    assert_species([CANDIDATES[0]])
    assert client.post('/api/redo').status_code == 200
    assert_species([])
    group = current_group(client)
    assert resolve(client, group, species=CANDIDATES[1]).status_code == 200
    assert_species([CANDIDATES[1]])
    assert client.post('/api/undo').status_code == 200
    assert_species([])
    assert client.post('/api/redo').status_code == 200
    assert_species([CANDIDATES[1]])


def test_shared_cache_failure_still_attempts_other_workspaces_after_commit(app_and_db, monkeypatch):
    import pipeline
    app, db = app_and_db
    client = app.test_client()
    second = db.create_workspace('Second')
    third = db.create_workspace('Third')
    db.photo_visibility.grant(second, [1])
    db.photo_visibility.grant(third, [1])
    db.commit()
    calls = []

    def refresh(cache_dir, workspace_id, species):
        calls.append(workspace_id)
        if workspace_id == second:
            raise OSError('read-only shared cache')

    monkeypatch.setattr(pipeline, 'refresh_cache_species_for_photos', refresh)
    response = save(client, [1])
    assert response.status_code == 200
    assert response.get_json()['pipeline_cache_stale']
    assert calls == [db.require_workspace_id(), second, third]
    assert db.identity_alternatives.photo_ids() == {1}


def test_collection_coverage_counts_real_unresolved_state_with_unrelated_confirmed_species(app_and_db):
    import json

    from services.collection_coverage import collection_coverage
    app, db = app_and_db
    client = app.test_client()
    other = db.add_keyword('Great Egret', is_species=True)
    db.tag_photo(1, other)
    collection = db.collections.add('Photos to inspect', json.dumps([{'field': 'photo_ids', 'value': [1, 2]}]))
    assert save(client, [1, 3]).status_code == 200
    review = collection_coverage(db, collection, {})['review']
    assert review['denominator'] == 2
    assert review['unresolved_identity_count'] == 1
    assert review['with_species_count'] == 1
    assert review['without_species_count'] == 1
    assert resolve(client, current_group(client), photo_ids=[1]).status_code == 200
    assert collection_coverage(db, collection, {})['review']['unresolved_identity_count'] == 0
    assert client.post('/api/undo').status_code == 200
    assert collection_coverage(db, collection, {})['review']['unresolved_identity_count'] == 1


@pytest.mark.parametrize('operation', ['create', 'resolve'])
def test_workspace_switch_before_writer_lock_rejects_shared_photo_edits(app_and_db, tmp_path, monkeypatch, operation):
    from db import Database
    from services import prediction_decisions
    app, db = app_and_db
    client = app.test_client()
    original_ws = db.require_workspace_id()
    kid = db.add_keyword(CANDIDATES[0], is_species=True)
    db.tag_photo(1, kid)
    group = None
    if operation == 'resolve':
        assert save(client, [1]).status_code == 200
        group = current_group(client)
    other_ws = db.create_workspace('Shared alternate workspace')
    db.add_workspace_folder(other_ws, db.photos.get(1)['folder_id'])
    before_species = db.photo_species.get_species_for_photos([1])
    before_state = db.identity_alternatives.for_photos([1])
    history_before = db.edit_history.latest_undoable()
    original_begin = prediction_decisions.begin_prediction_decision

    def switch_before_lock(request_db, *, json_error):
        assert request_db.require_workspace_id() == original_ws
        with Database(str(tmp_path / 'test.db')) as other:
            other.workspaces.update(other_ws, last_opened_at='2099-01-01T00:00:00')
        # The request connection still caches the old id here. The writer
        # must read persisted activation after BEGIN IMMEDIATE, too.
        assert request_db.require_workspace_id() == original_ws
        return original_begin(request_db, json_error=json_error)

    monkeypatch.setattr(prediction_decisions, 'begin_prediction_decision', switch_before_lock)
    response = resolve(client, group) if group else save(client, [1])
    assert response.status_code == 409
    assert 'workspace changed' in response.get_json()['error'].lower()
    assert db.photo_species.get_species_for_photos([1]) == before_species
    assert db.identity_alternatives.for_photos([1]) == before_state
    assert db.edit_history.latest_undoable() == history_before
