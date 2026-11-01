"""Preview maintenance stays off startup and tolerates live publishers."""

import threading
from pathlib import Path
from unittest.mock import Mock

import pytest
from db import Database
from repositories.caches import CachesRepository
from testing.waits import synchronization_timeout
from wait import wait_for_job_via_runner


@pytest.mark.parametrize('photo_count', [1, 2000])
def test_app_construction_does_not_read_preview_or_thumbnail_inventory(tmp_path, monkeypatch, photo_count):
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(cfg, 'CONFIG_PATH', str(tmp_path / 'config.json'))
    monkeypatch.setattr(models, 'DEFAULT_MODELS_DIR', str(tmp_path / 'models'))
    monkeypatch.setattr(models, 'CONFIG_PATH', str(tmp_path / 'models.json'))
    from services import workspace_folders
    with Database(str(tmp_path / 'catalog.db')) as db:
        folder_id = workspace_folders.add_folder(db, str(tmp_path / 'photos'))
        db.conn.executemany(
            'INSERT INTO photos(folder_id, filename, extension, file_size, file_mtime) VALUES (?, ?, ?, ?, ?)',
            [(folder_id, f'{i}.jpg', '.jpg', 1, 1) for i in range(photo_count)],
        )
        db.commit()
    for name in ('thumbnail_path_rows', 'preview_oldest_first', 'paired_preview_oldest_first'):
        monkeypatch.setattr(CachesRepository, name, Mock(side_effect=AssertionError(name)))
    app = create_app(str(tmp_path / 'catalog.db'), str(tmp_path / 'thumbnails'))
    try:
        assert app.test_client().get('/api/jobs').status_code == 200
    finally:
        app._cleanup_app_resources()


def test_preview_job_yields_requests_joins_and_reconciles_before_eviction(client_with_photo, monkeypatch):
    import preview_cache

    app, _, _ = client_with_photo
    entered, release = threading.Event(), threading.Event()
    calls = []
    def reconcile(db, root, **kwargs):
        entered.set()
        assert release.wait(synchronization_timeout(5))
        calls.append('reconcile')
        return 1
    def evict(db, root, **kwargs):
        calls.append('evict')
        return 2
    monkeypatch.setattr(preview_cache, 'reconcile_preview_cache', reconcile)
    monkeypatch.setattr(preview_cache, 'evict_if_over_quota', evict)
    try:
        job_id = app._kickoff_preview_cache_maintenance()
        assert entered.wait(synchronization_timeout(5))
        assert app.test_client().get('/api/jobs').status_code == 200
        assert app._kickoff_preview_cache_maintenance() == job_id
    finally:
        release.set()
    job = wait_for_job_via_runner(app._job_runner, job_id, wait_for_history=True)
    assert job['status'] == 'completed', job
    assert calls == ['reconcile', 'evict']
    assert job['origin'] == 'system' and job['reason']
    assert job['workspace_id'] is None
    assert job['result']['reconciled'] == 1
    assert job['result']['evicted'] == 2


def test_preview_job_records_error_and_skips_eviction(client_with_photo, monkeypatch):
    import preview_cache

    app, _, _ = client_with_photo
    monkeypatch.setattr(preview_cache, 'reconcile_preview_cache', Mock(side_effect=OSError('offline cache')))
    evict = Mock()
    monkeypatch.setattr(preview_cache, 'evict_if_over_quota', evict)
    job = wait_for_job_via_runner(app._job_runner, app._kickoff_preview_cache_maintenance(), wait_for_history=True)
    assert job['status'] == 'failed'
    assert 'offline cache' in str(job)
    evict.assert_not_called()


def _seed(db, pid, root, paired):
    directory = root / 'previews'
    if paired:
        directory /= 'paired'
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / (f'{pid}_1920_jpeg_state.jpg' if paired else f'{pid}_1920.jpg')
    path.write_bytes(b'old')
    _register(db, pid, path, paired, 3)
    return path


def _register(db, pid, path, paired, size):
    with db.transaction(immediate=True):
        if paired:
            db.caches.paired_preview_insert(pid, path.name, size)
        else:
            db.caches.preview_insert(pid, 1920, size)


@pytest.mark.parametrize('paired', [False, True])
def test_reconcile_preserves_preview_republished_after_missing_snapshot(client_with_photo, monkeypatch, paired):
    import preview_cache

    app, db, pid = client_with_photo
    root = Path(app.config['THUMB_CACHE_DIR']).parent
    path = _seed(db, pid, root, paired)
    path.unlink()
    original = preview_cache._preview_file_missing
    with Database(db._db_path, initialize_schema=False) as publisher:
        def missing(file, directory):
            result = original(file, directory)
            if result:
                path.write_bytes(b'new preview')
                _register(publisher, pid, path, paired, 11)
            return result
        monkeypatch.setattr(preview_cache, '_preview_file_missing', missing)
        assert preview_cache.reconcile_preview_cache(db, str(root)) == 0
    assert path.read_bytes() == b'new preview'
    assert db.caches.preview_total_bytes() == 11


@pytest.mark.parametrize('paired', [False, True])
def test_eviction_preserves_republished_candidate(client_with_photo, monkeypatch, paired):
    import config as cfg
    import preview_cache

    app, db, pid = client_with_photo
    root = Path(app.config['THUMB_CACHE_DIR']).parent
    path = _seed(db, pid, root, paired)
    monkeypatch.setattr(cfg, 'load', lambda: {'preview_cache_max_mb': 0})
    original = preview_cache._preview_entries
    with Database(db._db_path, initialize_schema=False) as publisher:
        def entries(database):
            snapshot = original(database)
            path.write_bytes(b'new preview')
            _register(publisher, pid, path, paired, 11)
            return snapshot
        monkeypatch.setattr(preview_cache, '_preview_entries', entries)
        assert preview_cache.evict_if_over_quota(db, str(root)) == 0
    assert path.read_bytes() == b'new preview'
    assert db.caches.preview_total_bytes() == 11


def test_overlapping_eviction_does_not_delete_an_extra_preview(client_with_photo, monkeypatch):
    import config as cfg
    import preview_cache

    app, db, pid = client_with_photo
    root = Path(app.config['THUMB_CACHE_DIR']).parent
    ordinary = _seed(db, pid, root, False)
    paired = _seed(db, pid, root, True)
    ordinary.write_bytes(b'a' * 700_000)
    paired.write_bytes(b'b' * 700_000)
    _register(db, pid, ordinary, False, 700_000)
    _register(db, pid, paired, True, 700_000)
    monkeypatch.setattr(cfg, 'load', lambda: {'preview_cache_max_mb': 1})
    original = preview_cache._preview_entries
    with Database(db._db_path, initialize_schema=False) as other:
        def entries(database):
            snapshot = original(database)
            if database is db:
                preview_cache.evict_if_over_quota(other, str(root))
            return snapshot
        monkeypatch.setattr(preview_cache, '_preview_entries', entries)
        assert preview_cache.evict_if_over_quota(db, str(root)) == 0
    assert db.caches.preview_total_bytes() == 700_000
    assert paired.exists() and not ordinary.exists()


def test_ordinary_publication_registers_inside_writer_transaction(client_with_photo, monkeypatch):
    import preview_materializer

    app, db, pid = client_with_photo
    path = Path(app.config['THUMB_CACHE_DIR']).parent / 'previews' / f'{pid}_1920.jpg'
    writer = preview_materializer.atomic_write_bytes
    observed = []
    def write(data, destination):
        observed.append(db.in_transaction)
        writer(data, destination)
    monkeypatch.setattr(preview_materializer, 'atomic_write_bytes', write)
    assert preview_materializer._publish_preview(
        db, pid, 1920, b'preview', str(path), publication_guard=None, publish_best_effort=False,
    )
    assert observed == [True]
    assert db.caches.preview_total_bytes() == 7
    assert not db.in_transaction


def test_best_effort_preview_serves_without_publication_when_writer_busy(client_with_photo, monkeypatch):
    import sqlite3

    import preview_materializer

    app, db, pid = client_with_photo
    path = Path(app.config['THUMB_CACHE_DIR']).parent / 'previews' / f'{pid}_1920.jpg'
    monkeypatch.setattr(db, 'transaction', Mock(side_effect=sqlite3.OperationalError('busy')))
    assert not preview_materializer._publish_preview(
        db, pid, 1920, b'preview', str(path), publication_guard=None, publish_best_effort=True,
    )
    assert not path.exists()


def test_preview_job_cancellation_skips_eviction(client_with_photo, monkeypatch):
    import preview_cache

    app, _, _ = client_with_photo
    entered, release = threading.Event(), threading.Event()
    def reconcile(db, root, *, checkpoint):
        entered.set()
        assert release.wait(synchronization_timeout(5))
        checkpoint()
        return 0
    evict = Mock()
    monkeypatch.setattr(preview_cache, 'reconcile_preview_cache', reconcile)
    monkeypatch.setattr(preview_cache, 'evict_if_over_quota', evict)
    try:
        job_id = app._kickoff_preview_cache_maintenance()
        assert entered.wait(synchronization_timeout(5))
        app._job_runner.cancel_job(job_id)
    finally:
        release.set()
    job = wait_for_job_via_runner(app._job_runner, job_id)
    assert job['status'] == 'cancelled'
    evict.assert_not_called()
