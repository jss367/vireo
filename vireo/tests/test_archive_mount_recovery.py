"""Missing mount evidence requires confirmation without weakening move guards."""
import json
from pathlib import Path

import pytest
from test_jobs_api import _import_for_review
from wait import wait_for_job_via_client


@pytest.fixture
def recovery(app_and_db, tmp_path, monkeypatch):
    import import_staging
    import pipeline_job

    app, db = app_and_db
    imported = _import_for_review(app, db, tmp_path, monkeypatch)
    archive_id = imported['config']['pending_archive_id']
    archive = dict(db.pending_archives.get(archive_id))
    target = json.loads(archive['target_json'])
    root = str(tmp_path / 'NAS')
    target.update(mount_baseline={root: True}, mount_identities={root: None})
    db.pending_archives.update_target(archive_id, target)
    db.commit()
    state = {'identity': ('network-share', 'smb://nas/photos', '/'), 'offline': None}
    monkeypatch.setattr(import_staging, '_archive_mount_identities',
                        lambda baseline: {key: state['identity'] for key in baseline})
    monkeypatch.setattr(pipeline_job, '_unmounted_since_baseline', lambda baseline: state['offline'])
    monkeypatch.setattr(pipeline_job, '_missing_archive_mount_root', lambda destination: None)
    return app.test_client(), db, archive, root, state


def test_missing_identity_requires_confirmation_and_persists_for_retry(recovery, monkeypatch):
    import pending_archives

    client, db, archive, root, state = recovery
    url = f"/api/import/pending-archives/{archive['id']}/send"
    response = client.post(url, json={'sync_first': True})
    assert response.status_code == 409
    proposal = response.get_json()['mount_confirmation']
    assert proposal == {root: list(state['identity'])}
    assert json.loads(db.pending_archives.get(archive['id'])['target_json'])['mount_identities'][root] is None
    assert Path(archive['staging_destination']).is_dir()

    # A subsequent transfer failure must not discard the confirmed evidence.
    def fail(*args, **kwargs):
        raise ValueError('Simulated transfer failure')
    monkeypatch.setattr(pending_archives, 'send_pending_archive', fail)
    response = client.post(url, json={'sync_first': True, 'confirmed_mount_identities': proposal})
    assert response.status_code == 200, response.get_json()
    job = wait_for_job_via_client(client, response.get_json()['job_id'])
    assert job['status'] == 'failed'
    assert job['config']['sync_first'] is True
    saved = json.loads(db.pending_archives.get(archive['id'])['target_json'])
    assert saved['mount_identities'] == proposal
    assert Path(archive['staging_destination']).is_dir()


@pytest.mark.parametrize('change', ['offline', 'replaced', 'unreadable'])
def test_confirmation_rechecks_mount_and_preserves_missing_record(recovery, change):
    client, db, archive, root, state = recovery
    url = f"/api/import/pending-archives/{archive['id']}/send"
    proposal = client.post(url).get_json()['mount_confirmation']
    if change == 'offline':
        state['offline'] = root
    else:
        state['identity'] = None if change == 'unreadable' else ('network-share', 'smb://other/photos', '/')
    response = client.post(url, json={'confirmed_mount_identities': proposal})
    assert response.status_code == 409
    assert 'mount_confirmation' not in response.get_json()
    assert json.loads(db.pending_archives.get(archive['id'])['target_json'])['mount_identities'][root] is None
    assert Path(archive['staging_destination']).is_dir()


@pytest.mark.parametrize('stale_confirmation', [True, False])
def test_confirmation_is_rechecked_after_another_tab_records_evidence(recovery, monkeypatch, stale_confirmation):
    from unittest.mock import Mock

    import pending_archives

    client, db, archive, root, state = recovery
    source = Path(archive['staging_destination'])
    originals = {str(p.relative_to(source)): p.read_bytes() for p in source.rglob('*') if p.is_file()}
    assert originals
    url = f"/api/import/pending-archives/{archive['id']}/send"
    proposal_a = client.post(url).get_json()['mount_confirmation']
    state['identity'] = ('network-share', 'smb://replacement/photos', '/')
    proposal_b = client.post(url).get_json()['mount_confirmation']
    transfer = Mock(side_effect=ValueError('Simulated transfer failure; no files moved'))
    monkeypatch.setattr(pending_archives, 'send_pending_archive', transfer)
    response = client.post(url, json={'confirmed_mount_identities': proposal_b})
    assert response.status_code == 200
    assert wait_for_job_via_client(client, response.get_json()['job_id'])['status'] == 'failed'
    transfer.assert_called_once()
    saved = db.pending_archives.get(archive['id'])['target_json']
    assert json.loads(saved)['mount_identities'] == proposal_b

    # Tab A's open prompt must not authorize B, even though B is now saved.
    transfer.reset_mock()
    supplied = proposal_a if stale_confirmation else proposal_b
    response = client.post(url, json={'confirmed_mount_identities': supplied})
    if stale_confirmation:
        assert response.status_code == 409
        assert 'changed since confirmation' in response.get_json()['error']
        assert 'job_id' not in response.get_json()
        transfer.assert_not_called()
    else:
        assert response.status_code == 200
        assert wait_for_job_via_client(client, response.get_json()['job_id'])['status'] == 'failed'
        transfer.assert_called_once()
    assert db.pending_archives.get(archive['id'])['target_json'] == saved
    for relative, content in originals.items():
        assert (source / relative).read_bytes() == content


def test_stale_confirmation_matching_live_mount_still_rejects_when_saved_differs(recovery, monkeypatch):
    """Another tab saved B; the mount later reverts to A. Tab A's stale confirmation
    must be rejected before dispatch instead of matching the live snapshot while the
    saved evidence disagrees."""
    from unittest.mock import Mock

    import pending_archives

    client, db, archive, root, state = recovery
    source = Path(archive['staging_destination'])
    originals = {str(p.relative_to(source)): p.read_bytes() for p in source.rglob('*') if p.is_file()}
    assert originals
    url = f"/api/import/pending-archives/{archive['id']}/send"

    proposal_a = client.post(url).get_json()['mount_confirmation']
    state['identity'] = ('network-share', 'smb://replacement/photos', '/')
    proposal_b = client.post(url).get_json()['mount_confirmation']
    transfer = Mock(side_effect=ValueError('Simulated transfer failure; no files moved'))
    monkeypatch.setattr(pending_archives, 'send_pending_archive', transfer)
    response = client.post(url, json={'confirmed_mount_identities': proposal_b})
    assert response.status_code == 200
    assert wait_for_job_via_client(client, response.get_json()['job_id'])['status'] == 'failed'
    transfer.assert_called_once()
    saved = db.pending_archives.get(archive['id'])['target_json']
    assert json.loads(saved)['mount_identities'] == proposal_b

    # Mount reverts to A, so Tab A's stale confirmation now matches the live snapshot.
    state['identity'] = ('network-share', 'smb://nas/photos', '/')
    transfer.reset_mock()
    response = client.post(url, json={'confirmed_mount_identities': proposal_a})
    assert response.status_code == 409
    assert 'changed since confirmation' in response.get_json()['error']
    assert 'job_id' not in response.get_json()
    transfer.assert_not_called()
    assert db.pending_archives.get(archive['id'])['target_json'] == saved
    for relative, content in originals.items():
        assert (source / relative).read_bytes() == content


def test_recovery_cannot_replace_known_mount_evidence(recovery):
    from pending_archives import prepare_archive_mount

    client, db, archive, root, state = recovery
    target = json.loads(db.pending_archives.get(archive['id'])['target_json'])
    target['mount_identities']['/another/mount'] = ['network-share', 'smb://original/photos', '/']
    archive['target_json'] = json.dumps(target)
    with pytest.raises(ValueError, match='NAS volume changed'):
        prepare_archive_mount(db, archive, {root: list(state['identity'])})


@pytest.mark.parametrize('confirmation', [{}, [], {'/unrelated/mount': ['other']}])
def test_invalid_confirmation_does_not_probe_unrelated_roots(recovery, monkeypatch, confirmation):
    from unittest.mock import Mock

    import import_staging
    from pending_archives import prepare_archive_mount

    _client, db, archive, root, state = recovery
    target = json.loads(archive['target_json'])
    target['mount_identities'][root] = list(state['identity'])
    archive['target_json'] = json.dumps(target)
    snapshot = Mock()
    monkeypatch.setattr(import_staging, '_archive_mount_identities', snapshot)
    with pytest.raises(ValueError, match='changed since confirmation'):
        prepare_archive_mount(db, archive, confirmation)
    snapshot.assert_not_called()
    assert json.loads(archive['target_json']) == target


def test_unreadable_confirmation_cannot_match_absent_live_evidence(recovery):
    from pending_archives import prepare_archive_mount

    _client, db, archive, root, state = recovery
    target = json.loads(archive['target_json'])
    target['mount_identities'][root] = list(state['identity'])
    archive['target_json'] = json.dumps(target)
    state['identity'] = None
    with pytest.raises(ValueError, match='changed since confirmation'):
        prepare_archive_mount(db, archive, {root: None})
    assert json.loads(archive['target_json']) == target


def test_offline_missing_identity_reports_unavailable(recovery):
    client, db, archive, root, state = recovery
    state['offline'] = root
    response = client.post(f"/api/import/pending-archives/{archive['id']}/send")
    assert response.status_code == 409
    assert 'NAS volume unavailable' in response.get_json()['error']
    assert 'mount_confirmation' not in response.get_json()


@pytest.mark.parametrize('replace_during_copy', [False, True])
def test_confirmed_transfer_still_checks_identity_before_removing_originals(recovery, monkeypatch, replace_during_copy):
    import shutil

    import move
    from move import mounted_verify

    client, db, archive, root, state = recovery

    # The optional preflight can succeed even if the route disappears by the send.
    monkeypatch.setattr(mounted_verify, 'mounted_verification_target',
                        lambda *args: ({"ssh_key": "private-key-path"}, None))
    check = client.get(f"/api/import/pending-archives/{archive['id']}/verification")
    assert check.get_json() == mounted_verify.verification_notice(None, preflight=True)
    routes = iter([({"ssh_key": "private-key-path"}, None), (None, "missing_route")])
    monkeypatch.setattr(move.folder_transfer, 'mounted_verification_target', lambda *args: next(routes))
    source = Path(archive['staging_destination'])
    originals = {str(path.relative_to(source)): path.read_bytes() for path in source.rglob('*') if path.is_file()}
    assert originals

    def copy(source_path, destination, *args, **kwargs):
        shutil.copytree(source_path, destination, dirs_exist_ok=True)
        if replace_during_copy:
            state['identity'] = ('network-share', 'smb://replacement/photos', '/')
        return 0, '', False

    monkeypatch.setattr(move.folder_transfer, '_run_rsync_streamed', copy)
    url = f"/api/import/pending-archives/{archive['id']}/send"
    proposal = client.post(url).get_json()['mount_confirmation']
    response = client.post(url, json={'confirmed_mount_identities': proposal})
    assert response.status_code == 200, response.get_json()
    job = wait_for_job_via_client(client, response.get_json()['job_id'])
    steps = {step['id']: step for step in job['steps']}
    assert steps['check']['nas_verification'] == check.get_json()
    assert steps['verify']['nas_verification'] == mounted_verify.verification_notice('missing_route')
    if replace_during_copy:
        assert job['status'] == 'failed', job
        assert 'NAS volume changed' in str(job['errors'])
        for relative, content in originals.items():
            assert (source / relative).read_bytes() == content
    else:
        assert job['status'] == 'completed', job
        assert db.pending_archives.get(archive['id'])['state'] == 'complete'
        for relative, content in originals.items():
            assert (Path(archive['destination']) / relative).read_bytes() == content
