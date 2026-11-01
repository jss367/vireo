"""Thumbnail presence is maintained on writes and rechecked explicitly."""

import threading
from pathlib import Path
from unittest.mock import Mock

import pytest
from repositories.caches import CachesRepository
from testing.waits import synchronization_timeout
from wait import wait_for_job_via_runner


def test_startup_never_schedules_thumbnail_scan(client_with_photo, monkeypatch):
    from app import _schedule_startup_backfills
    from services.startup_tasks import StartupTasks

    app, db, _ = client_with_photo
    startup = StartupTasks(app, db._db_path, db)
    scheduled = []
    monkeypatch.delenv('VIREO_DISABLE_STARTUP_BACKFILL_TIMERS')
    monkeypatch.setattr(threading, 'Timer', lambda delay, fn: scheduled.append(fn) or Mock())
    monkeypatch.setattr(CachesRepository, 'thumbnail_path_rows', Mock(side_effect=AssertionError('scan')))
    startup.kickoff_label_identity_backfill = Mock()
    startup.kickoff_exif_search_backfill = Mock()
    _schedule_startup_backfills(app, startup)
    for launch in scheduled:
        launch()
    assert scheduled == [startup.kickoff_label_identity_backfill, startup.kickoff_exif_search_backfill]
    assert not any(j['type'] == 'thumb_path_backfill' for j in app._job_runner.list_jobs())


def test_explicit_recheck_is_one_pass_and_keeps_user_history(client_with_photo, monkeypatch):
    import thumbnails

    app, db, pid = client_with_photo
    cache = Path(app.config['THUMB_CACHE_DIR'])
    (cache / f'{pid}.jpg').write_bytes(b'cached')
    reads = []
    checks = []
    original_rows = CachesRepository.thumbnail_path_rows
    original_present = thumbnails._thumbnail_present
    def rows(repo):
        reads.append(1)
        return original_rows(repo)
    def present(root, photo_id):
        checks.append(photo_id)
        return original_present(root, photo_id)
    monkeypatch.setattr(CachesRepository, 'thumbnail_path_rows', rows)
    monkeypatch.setattr(thumbnails, '_thumbnail_present', present)
    response = app.test_client().post('/api/thumbnail-cache/recheck')
    assert response.status_code == 202
    job = wait_for_job_via_runner(app._job_runner, response.json['job_id'], wait_for_history=True)
    assert job['status'] == 'completed', job
    assert job['origin'] == 'user'
    assert job['workspace_id'] is None
    assert job['result']['set'] == 1
    assert job['result']['checked'] == 1
    assert job['result']['unchecked'] == 0
    assert 'remaining' not in job['result']
    assert reads == [1] and checks == [pid]
    assert db.photos.get(pid)['thumb_path'] == f'{pid}.jpg'
    assert db.conn.execute("SELECT origin FROM job_history WHERE id=?", (job['id'],)).fetchone()[0] == 'user'


def test_thumbnail_recheck_joins_active_job(client_with_photo, monkeypatch):
    import thumbnails

    app, _, _ = client_with_photo
    entered, release = threading.Event(), threading.Event()
    def recheck(*args, **kwargs):
        entered.set()
        assert release.wait(synchronization_timeout(5))
        return {'checked': 0}
    monkeypatch.setattr(thumbnails, 'backfill_thumb_paths', recheck)
    try:
        first = app.test_client().post('/api/thumbnail-cache/recheck').json
        assert entered.wait(synchronization_timeout(5))
        second = app.test_client().post('/api/thumbnail-cache/recheck').json
        assert second == {'job_id': first['job_id'], 'joined': True}
    finally:
        release.set()
    assert wait_for_job_via_runner(app._job_runner, first['job_id'])['status'] == 'completed'


def test_cancelled_recheck_reports_unchecked_records(client_with_photo):
    from thumbnails import backfill_thumb_paths

    app, db, _ = client_with_photo
    result = backfill_thumb_paths(db, app.config['THUMB_CACHE_DIR'], cancel_check=lambda: True)
    assert result['checked'] == 0
    assert result['unchecked'] == 1
    assert 'remaining' not in result


@pytest.mark.parametrize('failure', [FileNotFoundError, PermissionError, OSError])
def test_unavailable_thumbnail_cache_preserves_records(client_with_photo, monkeypatch, failure):
    import thumbnails

    app, db, pid = client_with_photo
    db.photos.set_thumb_path(pid, f'{pid}.jpg')
    original = thumbnails.os.stat
    root = app.config['THUMB_CACHE_DIR']
    def stat(path, *args, **kwargs):
        if str(path).startswith(root):
            raise failure('unavailable')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(thumbnails.os, 'stat', stat)
    with pytest.raises(OSError):
        thumbnails.backfill_thumb_paths(db, root)
    assert db.photos.get(pid)['thumb_path'] == f'{pid}.jpg'


def test_cancelled_recheck_keeps_singleton_until_worker_finishes(client_with_photo, monkeypatch):
    import thumbnails

    app, _, _ = client_with_photo
    entered, release = threading.Event(), threading.Event()
    calls = []
    def recheck(*args, **kwargs):
        calls.append(1)
        entered.set()
        assert release.wait(synchronization_timeout(5))
        return {'checked': 0}
    monkeypatch.setattr(thumbnails, 'backfill_thumb_paths', recheck)
    try:
        first = app.test_client().post('/api/thumbnail-cache/recheck').json
        assert entered.wait(synchronization_timeout(5))
        app._job_runner.cancel_job(first['job_id'])
        second = app.test_client().post('/api/thumbnail-cache/recheck').json
        assert second == {'job_id': first['job_id'], 'joined': True, 'cancelling': True}
        assert calls == [1]
    finally:
        release.set()
    assert wait_for_job_via_runner(app._job_runner, first['job_id'])['status'] == 'cancelled'
    third = app.test_client().post('/api/thumbnail-cache/recheck').json
    assert third['job_id'] != first['job_id']
    assert third['joined'] is False
    assert wait_for_job_via_runner(app._job_runner, third['job_id'])['status'] == 'completed'
    assert calls == [1, 1]
