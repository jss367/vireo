"""Mounted NAS checksums must retain the move's deletion safety guarantees."""

import builtins
import os
import shutil
import subprocess
from types import SimpleNamespace

import pytest
from move import mounted_verify as mv
from move import verify
from move.folder_transfer import _FolderTransfer


@pytest.fixture
def trees(tmp_path, monkeypatch):
    source, dest = tmp_path / 'source', tmp_path / 'mount' / 'shoot'
    source.mkdir()
    dest.mkdir(parents=True)
    (source / 'day one').mkdir()
    for name in ('photo.nef', 'day one/photo.xmp', '-odd\nname.jpg'):
        (source / name).write_bytes(b'photo bytes' * 100)
    shutil.copytree(source, dest, dirs_exist_ok=True)
    remote = {
        'host': 'nas', 'user': 'photographer', 'port': 22, 'ssh_key': '',
        'verify_path': '/volume1/Photos/shoot', 'rsync_bin': 'rsync', 'ssh_bin': 'ssh',
    }
    monkeypatch.setattr(mv, '_probe_matches', lambda *a: True)
    return source, dest, remote


def settings(mount):
    return {'remote_targets': [{
        'host': 'nas', 'user': 'photographer', 'remote_path': '/volume1/Photos',
        'mount_path': str(mount),
    }]}


def test_select_exact_subpath_and_do_not_redirect_symlinks(trees, monkeypatch):
    source, dest, _ = trees
    monkeypatch.setattr(mv, 'resolve_rsync_bin', lambda _: '/tools/rsync')
    monkeypatch.setattr(mv, 'resolve_ssh_bin', lambda _: '/tools/ssh')
    config = settings(dest.parent)
    target = mv.mounted_verification_target(str(dest), config)
    assert target['verify_path'] == '/volume1/Photos/shoot'
    assert target['rsync_bin'] == '/tools/rsync'
    assert mv.mounted_verification_target(str(dest.parent), config)['verify_path'] == '/volume1/Photos'
    assert mv.mounted_verification_target(str(dest.parent) + '-other/shoot', config) is None
    link = dest.parent / 'redirect'
    link.symlink_to(source, target_is_directory=True)
    assert mv.mounted_verification_target(str(link), config) is None
    config['remote_targets'] *= 2
    assert mv.mounted_verification_target(str(dest), config) is None


def test_no_ssh_or_gnu_rsync_keeps_local_verification(trees, monkeypatch):
    _, dest, _ = trees
    monkeypatch.setattr(mv, 'resolve_rsync_bin', lambda _: None)
    monkeypatch.setattr(mv, 'resolve_ssh_bin', lambda _: '/tools/ssh')
    assert mv.mounted_verification_target(str(dest), settings(dest.parent)) is None


def run_verify(trees, **kwargs):
    source, dest, remote = trees
    return mv.verify_mounted_copy(str(source), str(dest), remote,
                                 is_merge=kwargs.pop('is_merge', False),
                                 progress=kwargs.pop('progress', lambda *a: None), **kwargs)


def test_batches_preserve_paths_count_only_verified_files_and_never_read_mount(trees, monkeypatch):
    source, dest, _ = trees
    calls, manifests, events = [], [], []
    monkeypatch.setattr(mv, 'VERIFY_BATCH_SIZE', 2)
    real_open = builtins.open

    def no_mounted_photo_reads(path, *args, **kwargs):
        assert not str(path).startswith(str(dest)) or '.vireo-verify-' in str(path)
        return real_open(path, *args, **kwargs)

    def checksum(binary, src, target, remote, **kwargs):
        assert src == str(source)
        assert target == 'photographer@nas:/volume1/Photos/shoot'
        manifest = kwargs['files_from']
        manifests.append(manifest)
        with real_open(manifest, 'rb') as stream:
            calls.extend(os.fsdecode(n) for n in stream.read().split(b'\0') if n)
        return None

    monkeypatch.setattr(builtins, 'open', no_mounted_photo_reads)
    monkeypatch.setattr(mv, '_remote_verify_complete', checksum)
    assert run_verify(trees, progress=lambda *a: events.append(a)) == (True, None)
    assert set(calls) == {'./photo.nef', './day one/photo.xmp', './-odd\nname.jpg'}
    assert len(manifests) == 2
    assert all(not os.path.exists(p) for p in manifests)
    assert not list(dest.glob('.vireo-verify-*'))
    counts = [e[0] for e in events if e[3] == 'Verifying copy on NAS']
    assert counts == [0, 0, 2, 2, 3, 3]


@pytest.mark.parametrize('failure', ['probe', 'ssh', 'tool', 'remount'])
def test_unavailable_verification_falls_back_and_cleans_probe(trees, monkeypatch, failure):
    _, dest, _ = trees
    probes = iter([True, False]) if failure == 'remount' else iter([failure != 'probe'] * 2)
    monkeypatch.setattr(mv, '_probe_matches', lambda *a: next(probes))

    def checksum(*a, **k):
        if failure == 'ssh':
            raise subprocess.TimeoutExpired('ssh', 20)
        return ('__ERROR__', 'rsync missing') if failure == 'tool' else None

    monkeypatch.setattr(mv, '_remote_verify_complete', checksum)
    assert run_verify(trees) == (False, None)
    assert not list(dest.glob('.vireo-verify-*'))


def test_checksum_mismatch_blocks_deletion_without_fallback(trees, monkeypatch):
    monkeypatch.setattr(mv, '_remote_verify_complete', lambda *a, **k: ('photo.nef', None))
    assert run_verify(trees) == (True, 'photo.nef')


@pytest.mark.parametrize('damage', ['missing', 'symlink', 'hardlink', 'directory_link', 'after_hash'])
def test_mounted_structure_must_still_be_independent(trees, monkeypatch, damage):
    source, dest, _ = trees

    def damage_destination():
        path = dest / 'photo.nef'
        path.unlink()
        if damage == 'hardlink':
            os.link(source / 'photo.nef', path)
        elif damage != 'missing':
            path.symlink_to(source / 'photo.nef')

    if damage == 'directory_link':
        shutil.rmtree(dest / 'day one')
        (dest / 'day one').symlink_to(source / 'day one', target_is_directory=True)
    elif damage != 'after_hash':
        damage_destination()

    def checksum(*a, **k):
        if damage == 'after_hash':
            damage_destination()
        else:
            pytest.fail('unsafe mounted structure must be rejected before hashing')

    monkeypatch.setattr(mv, '_remote_verify_complete', checksum)
    handled, mismatch = run_verify(trees)
    assert handled and mismatch
    assert (source / 'photo.nef').exists()


def test_source_added_during_hash_cannot_be_deleted_unverified(trees, monkeypatch):
    source, _, _ = trees

    def checksum(*a, **k):
        (source / 'new.nef').write_bytes(b'new photo')

    monkeypatch.setattr(mv, '_remote_verify_complete', checksum)
    assert run_verify(trees) == (True, 'source files changed during checksum verification')


@pytest.mark.parametrize('change', ['rewrite', 'replace'])
def test_destination_changed_after_hash_cannot_authorize_deletion(trees, monkeypatch, change):
    _, dest, _ = trees

    def checksum(*a, **k):
        path = dest / 'photo.nef'
        before = path.stat()
        if change == 'replace':
            path.unlink()
        path.write_bytes(b'x' * before.st_size)
        # POSIX ctime also catches preserved mtime. Windows ctime records
        # creation time, so use a changed mtime there instead.
        if os.name != 'nt':
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        else:
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 2_000_000_000))

    monkeypatch.setattr(mv, '_remote_verify_complete', checksum)
    assert run_verify(trees) == (True, 'destination files changed during checksum verification')


def test_merge_ignores_only_regular_finder_metadata(trees, monkeypatch):
    source, dest, _ = trees
    (source / '.DS_Store').write_bytes(b'local metadata')
    (dest / '.DS_Store').write_bytes(b'other metadata')

    def checksum(*a, **k):
        with open(k['files_from'], 'rb') as stream:
            assert b'.DS_Store' not in stream.read()

    monkeypatch.setattr(mv, '_remote_verify_complete', checksum)
    assert run_verify(trees, is_merge=True) == (True, None)
    (source / '.DS_Store').unlink()
    (source / '.DS_Store').symlink_to('photo.nef')
    assert run_verify(trees, is_merge=True) == (True, '.DS_Store')


def test_cancellation_propagates_and_removes_probe(trees, monkeypatch):
    from job_runner.work_control import JobCancelled

    def checksum(*a, **k):
        raise JobCancelled()

    monkeypatch.setattr(mv, '_remote_verify_complete', checksum)
    with pytest.raises(JobCancelled):
        run_verify(trees)
    assert not list(trees[1].glob('.vireo-verify-*'))


def test_manifest_rsync_command_checks_contents_not_smb_metadata(monkeypatch):
    captured = []
    monkeypatch.setattr(verify.subprocess, 'run', lambda cmd, **k:
                        captured.append(cmd) or subprocess.CompletedProcess(cmd, 0, '', ''))
    remote = {'host': 'nas', 'user': 'user'}
    assert verify._remote_verify_complete('rsync', '/source', 'user@nas:/Photos with spaces',
                                         remote, files_from='/tmp/file list') is None
    cmd = captured[0]
    assert '-rln' in cmd and '-an' not in cmd
    assert '--checksum' in cmd and '--protect-args' in cmd and '--from0' in cmd
    assert '--files-from=/tmp/file list' in cmd


@pytest.mark.parametrize('name', [
    'photo.nef', '#photo.nef', ';photo.nef', '#shoot/photo.nef', ';shoot/photo.nef',
])
def test_real_rsync_checks_same_size_corruption_and_ignores_timestamp_changes(trees, monkeypatch, name):
    binary = mv.resolve_rsync_bin()
    if binary is None:
        pytest.skip('GNU rsync is not installed')
    source, dest, remote = trees
    remote['rsync_bin'] = binary
    (source / name).parent.mkdir(parents=True, exist_ok=True)
    (dest / name).parent.mkdir(parents=True, exist_ok=True)
    (source / name).write_bytes(b'original photo bytes')
    (dest / name).write_bytes(b'original photo bytes')
    # Exercise real rsync checksum + NUL manifest semantics using local trees;
    # only the transport is replaced. No NAS or SSH service is needed in CI.
    monkeypatch.setattr(mv, 'rsync_dest_spec', lambda *a: str(dest))
    os.utime(dest / name, (1, 1))
    assert run_verify(trees) == (True, None)
    original = (source / name).read_bytes()
    (dest / name).write_bytes(b'x' * len(original))
    handled, mismatch = run_verify(trees)
    assert handled and mismatch == name


def test_ssh_probe_quotes_destination_and_requires_exact_nonce(monkeypatch):
    captured = []
    remote = {'host': 'nas', 'user': 'user', 'verify_path': "/Photos ' $(false)"}
    monkeypatch.setattr(mv.subprocess, 'run', lambda cmd, **k:
                        captured.append(cmd) or subprocess.CompletedProcess(cmd, 0, 'nonce', ''))
    assert mv._probe_matches(remote, '.probe', 'nonce', None)
    import shlex
    assert shlex.split(captured[0][-1]) == ['cat', '--', "/Photos ' $(false)/.probe"]
    assert not mv._probe_matches(remote, '.probe', 'different', None)


@pytest.mark.parametrize('remote_result', [
    (False, None), (True, 'photo.nef'), (True, None),
    (True, 'source files changed during checksum verification'),
    (True, 'destination files changed during checksum verification'),
])
def test_transfer_uses_full_byte_fallback_or_remote_verdict(trees, monkeypatch, remote_result):
    import move.folder_transfer as ft
    source, dest, remote = trees
    (dest / 'photo.nef').write_bytes(b'bad bytes!!' * 100)
    monkeypatch.setattr(ft, 'mounted_verification_target', lambda *a: remote)
    monkeypatch.setattr(ft, 'verify_mounted_copy', lambda *a, **k: remote_result)
    events = []
    transfer = _FolderTransfer(
        SimpleNamespace(effective_config=lambda: {}), str(source),
        SimpleNamespace(transfer_dest=str(dest), dest_exists=True),
        remote=None, verify_contents=True, progress_cb=None,
        progress=lambda *a: events.append(a),
    )
    transfer.total_files = 3
    result = transfer.verify_copy()
    if remote_result == (True, None):
        assert result is None
    else:
        assert result['moved'] == 0 and 'Originals preserved' in result['errors'][0]
        if remote_result[0]:
            assert f'Verification failed ({remote_result[1]})' in result['errors'][0]
    if not remote_result[0]:
        assert any('NAS verification unavailable' in e[3] for e in events)


def test_job_step_names_actual_verification_method():
    import threading

    from job_runner.steps import StepTree
    from services.move_steps import MoveSteps, folder_move_steps

    job = {'id': 'job'}
    runner = StepTree(threading.RLock(), {'job': job})
    steps = MoveSteps(runner, job, folder_move_steps(remote=False, verify_contents=True))
    for phase in ('Checking NAS verification', 'Verifying copy on NAS',
                  'Verifying copy locally (NAS verification unavailable)'):
        steps.report(0, 3, '', phase)
        actual = next(s for s in job['steps'] if s['id'] == 'verify')
        assert actual['label'] == phase
        assert actual['status'] == 'running'
        assert actual['progress'] == {'current': 0, 'total': 3}
