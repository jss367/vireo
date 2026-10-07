"""Durable source-specific previews share the ordinary preview disk quota."""
import os
from pathlib import Path

import config as cfg
import pytest
from db import Database
from preview_cache import (
    cleanup_cached_files_for_deleted_photos,
    evict_if_over_quota,
    paired_preview_ready,
    reconcile_preview_cache,
)


def seed(db, root, *, size=700_000):
    folder = db.add_folder(str(root / 'photos'))
    pid = db.add_photo(folder, 'a.NEF', '.nef', file_size=1, file_mtime=1)
    ordinary = root / 'previews' / f'{pid}_1920.jpg'
    paired = ordinary.parent / 'paired' / f'{pid}_1920_jpeg_state.jpg'
    paired.parent.mkdir(parents=True, exist_ok=True)
    ordinary.write_bytes(b'a' * size)
    paired.write_bytes(b'b' * size)
    db.preview_cache_insert(pid, 1920, size)
    with db.conn:
        db.paired_preview_cache_insert(pid, paired.name, size)
    return pid, ordinary, paired


@pytest.mark.parametrize('oldest', ['ordinary', 'paired'])
def test_preview_families_share_one_lru(db, tmp_path, monkeypatch, oldest):
    pid, ordinary, paired = seed(db, tmp_path)
    monkeypatch.setattr(cfg, 'load', lambda: {'preview_cache_max_mb': 1})
    assert db.preview_cache_total_bytes() == 1_400_000
    with db.conn:
        db.conn.execute('UPDATE preview_cache SET last_access_at=?',
                        (1 if oldest == 'ordinary' else 2,))
        db.conn.execute('UPDATE paired_preview_cache SET last_access_at=?',
                        (1 if oldest == 'paired' else 2,))
    evict_if_over_quota(db, str(tmp_path))
    assert ordinary.exists() == (oldest == 'paired')
    assert paired.exists() == (oldest == 'ordinary')
    assert paired_preview_ready(db, str(paired)) == (oldest == 'ordinary')
    assert db.preview_cache_total_bytes() == 700_000


def test_failed_paired_unlink_remains_accounted_for(db, tmp_path, monkeypatch):
    _, _, paired = seed(db, tmp_path)
    monkeypatch.setattr(cfg, 'load', lambda: {'preview_cache_max_mb': 0})
    real_remove = os.remove

    def fail_paired(path):
        if Path(path) == paired:
            raise PermissionError('locked')
        real_remove(path)

    monkeypatch.setattr(os, 'remove', fail_paired)
    evict_if_over_quota(db, str(tmp_path))
    assert db.preview_cache_total_bytes() == 700_000
    assert paired_preview_ready(db, str(paired))
    monkeypatch.setattr(os, 'remove', real_remove)
    evict_if_over_quota(db, str(tmp_path))
    assert db.preview_cache_total_bytes() == 0
    assert not paired.exists()


def test_paired_cache_survives_reopen_and_startup_reconciles_orphans(db, tmp_path):
    _, _, paired = seed(db, tmp_path, size=10)
    os.utime(paired, (1, 1))
    untracked = paired.with_name('999_1920_jpeg_legacy.jpg')
    untracked.write_bytes(b'legacy shadow cache')
    with Database(db._db_path, initialize_schema=False) as reopened:
        assert paired_preview_ready(reopened, str(paired))
        assert not paired_preview_ready(reopened, str(untracked))
        assert reconcile_preview_cache(reopened, str(tmp_path)) == 0
        assert paired.exists()
        assert not untracked.exists()
        paired.unlink()
        assert reconcile_preview_cache(reopened, str(tmp_path)) == 1
        assert reopened.paired_preview_cache_get(paired.name) is None
        assert reopened.preview_cache_total_bytes() == 10


def test_delete_removes_paired_files_and_rows_before_photo_id_reuse(db, tmp_path):
    pid, _, paired = seed(db, tmp_path)
    files = db.delete_photos([pid])
    cleanup_cached_files_for_deleted_photos(str(tmp_path / 'thumbs'), files["files"],
                                          vireo_dir=str(tmp_path))
    assert not paired.exists()
    assert db.paired_preview_cache_get(paired.name) is None
    assert db.preview_cache_total_bytes() == 0


def test_storage_reports_lists_and_clears_paired_previews(client_with_photo):
    app, db, _ = client_with_photo
    root = Path(app.config['THUMB_CACHE_DIR']).parent
    _, ordinary, paired = seed(db, root, size=10)
    client = app.test_client()
    stats = client.get('/api/storage').get_json()['previews']
    assert stats['size'] >= 20
    files = client.get('/api/storage/files?type=previews').get_json()['files']
    assert {'name': 'paired/' + paired.name, 'size': 10} in files
    response = client.post('/api/storage/delete-files', json={
        'type': 'previews', 'files': ['paired/' + paired.name],
    })
    assert response.status_code == 200
    assert not paired.exists()
    assert db.paired_preview_cache_get(paired.name) is None
    assert ordinary.exists()
    paired.write_bytes(b'0123456789')
    with db.conn:
        db.paired_preview_cache_insert(int(paired.name.split('_')[0]), paired.name, 10)
    response = client.post('/api/storage/clear', json={'type': 'previews'})
    assert response.status_code == 200
    assert db.preview_cache_total_bytes() == 0
    assert not paired.exists()


@pytest.mark.parametrize('locked', [False, True])
def test_dedicated_clear_control_includes_paired_cache(client_with_photo, monkeypatch, locked):
    app, db, _ = client_with_photo
    root = Path(app.config['THUMB_CACHE_DIR']).parent
    _, ordinary, paired = seed(db, root, size=10)
    legacy = paired.with_name('999_1920_jpeg_old.jpg')
    legacy.write_bytes(b'old shadow')
    if locked:
        real_remove = os.remove

        def fail_paired(path):
            if Path(path) == paired:
                raise PermissionError('locked paired preview')
            real_remove(path)

        monkeypatch.setattr(os, 'remove', fail_paired)
    client = app.test_client()
    before = client.get('/api/preview-cache').get_json()
    assert before['count'] == 2
    assert before['total_size'] == 20
    result = client.post('/api/preview-cache/clear').get_json()
    assert result == {
        'cleared': 1 if locked else 2,
        'files_removed': 2 if locked else 3,
        'failed': 1 if locked else 0,
    }
    after = client.get('/api/preview-cache').get_json()
    assert after['count'] == (1 if locked else 0)
    assert after['total_size'] == (10 if locked else 0)
    assert paired.exists() == locked
    assert not ordinary.exists()
    assert not legacy.exists()


@pytest.mark.parametrize('ordinary_count,paired_count,limit', [
    (501, 1, 500), (1, 501, 500), (3, 3, 4), (1, 1, 2), (0, 3, 4), (3, 0, 4),
])
def test_limited_storage_listing_shares_space_between_preview_families(
    client_with_photo, ordinary_count, paired_count, limit,
):
    app, _, _ = client_with_photo
    root = Path(app.config['THUMB_CACHE_DIR']).parent / 'previews'
    paired = root / 'paired'
    paired.mkdir(parents=True, exist_ok=True)
    for count, directory in [(ordinary_count, root), (paired_count, paired)]:
        for index in range(count):
            (directory / f'{index}_1920.jpg').write_bytes(b'preview')
    result = app.test_client().get(
        f'/api/storage/files?type=previews&limit={limit}'
    ).get_json()
    names = [entry['name'] for entry in result['files']]
    assert len(names) == min(limit, ordinary_count + paired_count)
    assert result['truncated'] == (ordinary_count + paired_count > limit)
    assert any(name.startswith('paired/') for name in names) == bool(paired_count)
    assert any(not name.startswith('paired/') for name in names) == bool(ordinary_count)
