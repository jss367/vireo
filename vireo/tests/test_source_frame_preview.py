"""Inspection frames reuse the shared cache without applying saved edit geometry."""
import io
from pathlib import Path

import pytest
from PIL import Image
from services import photo_ingest, workspace_folders


def _source_photo(app_and_db, tmp_path):
    app, db = app_and_db
    folder = workspace_folders.add_folder(db, str(tmp_path), name='Inspection source')
    path = tmp_path / 'source.jpg'
    Image.new('RGB', (600, 400), 'red').save(path)
    pid = photo_ingest.add_photo(db, folder_id=folder, filename=path.name, extension='.jpg',
                                file_size=path.stat().st_size, file_mtime=path.stat().st_mtime)
    db.commit()
    return app, db, pid, path


def test_source_frame_cache_reuses_decode_ignores_edits_and_revalidates_source(app_and_db, tmp_path, monkeypatch):
    import web.media as media
    app, db, pid, path = _source_photo(app_and_db, tmp_path)
    original_loader = media._focus_source_image
    calls = []

    def counted_loader(*args):
        calls.append(args)
        return original_loader(*args)

    monkeypatch.setattr(media, '_focus_source_image', counted_loader)
    recipe = {'crop': {'x': .1, 'y': .1, 'w': .5, 'h': .5}, 'rotation': 90}
    db.set_photo_edit_recipe(pid, recipe)
    db.commit()
    recipe = db.get_photo_edit_recipe(pid)
    # An ordinary preview is deliberately incompatible with source geometry.
    cache = Path(app.config['THUMB_CACHE_DIR']).parent / 'previews'
    cache.mkdir(exist_ok=True)
    Image.new('RGB', (100, 200), 'blue').save(cache / f'{pid}_1920.jpg')
    before = dict(db.photos.get(pid))
    client = app.test_client()
    url = f'/photos/{pid}/preview?size=1920&view=source'
    first = client.get(url)
    assert first.status_code == 200
    assert Image.open(io.BytesIO(first.data)).size == (600, 400)
    assert 'no-store' not in first.headers['Cache-Control']
    rows = db.caches.paired_preview_oldest_first()
    assert len(rows) == 1 and '_source_' in rows[0]['filename']
    assert rows[0]['bytes'] == len(first.data)
    assert client.get(url).data == first.data
    assert client.get(url, headers={'If-None-Match': first.headers['ETag']}).status_code == 304
    assert len(calls) == 1
    assert db.caches.paired_preview_oldest_first()[0]['last_access_at'] >= rows[0]['last_access_at']
    assert db.get_photo_edit_recipe(pid) == recipe
    assert dict(db.photos.get(pid)) == before
    db.set_photo_edit_recipe(pid, {'rotation': 180})
    db.commit()
    assert client.get(url).data == first.data
    assert len(calls) == 1  # Saved adjustments cannot invalidate source geometry.
    Image.new('RGB', (400, 600), 'green').save(path)
    replaced = client.get(url)
    assert replaced.status_code == 200
    assert Image.open(io.BytesIO(replaced.data)).size == (400, 600)
    assert replaced.data != first.data and len(calls) == 2


def test_source_frame_working_copy_survives_offline_original(app_and_db, tmp_path, monkeypatch):
    import image_loader
    app, db, pid, path = _source_photo(app_and_db, tmp_path)
    root = Path(app.config['THUMB_CACHE_DIR']).parent
    (root / 'working').mkdir(exist_ok=True)
    working = root / 'working' / f'{pid}.jpg'
    Image.new('RGB', (300, 200), 'blue').save(working)
    db.conn.execute('UPDATE photos SET working_copy_path=?,width=600,height=400 WHERE id=?',
                    (f'working/{pid}.jpg', pid))
    db.commit()
    path.unlink()
    # Offline working pixels must not invoke a RAW/editor decode at all.
    monkeypatch.setattr(image_loader, 'load_image', lambda *a, **k: pytest.fail('original decoder called'))
    response = app.test_client().get(f'/photos/{pid}/preview?size=1920&view=source')
    assert response.status_code == 200
    assert Image.open(io.BytesIO(response.data)).size == (300, 200)


def test_source_frame_raw_uses_jpeg_first_loader_and_caches_result(app_and_db, tmp_path, monkeypatch):
    import image_loader
    app, db, pid, path = _source_photo(app_and_db, tmp_path)
    raw = path.with_suffix('.NEF')
    path.rename(raw)
    db.conn.execute('UPDATE photos SET filename=?,extension=? WHERE id=?', (raw.name, '.nef', pid))
    db.commit()
    calls = []

    def raw_loader(source, max_size=None, **kwargs):
        calls.append((source, max_size, kwargs))
        return Image.new('RGB', (600, 400), 'red')

    monkeypatch.setattr(image_loader, 'load_image', raw_loader)
    client = app.test_client()
    url = f'/photos/{pid}/preview?size=1920&view=source'
    assert client.get(url).status_code == 200
    assert client.get(url).status_code == 200
    assert calls == [(str(raw), None, {})]  # Default JPEG-first, never the linear edit worker.


def test_source_change_during_decode_never_publishes_stale_frame(app_and_db, tmp_path, monkeypatch):
    import web.media as media
    app, db, pid, path = _source_photo(app_and_db, tmp_path)
    original_loader = media._focus_source_image

    def changing_loader(*args):
        result = original_loader(*args)
        Image.new('RGB', (200, 300), 'green').save(path)
        return result

    monkeypatch.setattr(media, '_focus_source_image', changing_loader)
    response = app.test_client().get(f'/photos/{pid}/preview?size=1920&view=source')
    assert response.status_code == 404
    assert db.caches.paired_preview_oldest_first() == []
    monkeypatch.setattr(media, '_focus_source_image', original_loader)
    assert app.test_client().get(f'/photos/{pid}/preview?size=1920&view=source').status_code == 200


def test_source_frame_cache_write_failure_still_serves_available_pixels(app_and_db, tmp_path, monkeypatch):
    from services import source_frame_preview
    app, db, pid, _path = _source_photo(app_and_db, tmp_path)

    def fail_write(*args):
        raise OSError('read-only cache')

    monkeypatch.setattr(source_frame_preview, 'atomic_write_bytes', fail_write)
    response = app.test_client().get(f'/photos/{pid}/preview?size=1920&view=source')
    assert response.status_code == 200
    assert Image.open(io.BytesIO(response.data)).size == (600, 400)
    assert db.caches.paired_preview_oldest_first() == []


def test_source_frame_missing_photo_does_not_load_or_adopt_cache(app_and_db, monkeypatch):
    import web.media as media
    app, _db = app_and_db
    monkeypatch.setattr(media, '_focus_source_image', lambda *a: pytest.fail('unscoped source loaded'))
    assert app.test_client().get('/photos/999999/preview?size=1920&view=source').status_code == 404


def test_source_frame_eviction_and_workspace_scope_apply_to_registered_hits(app_and_db, tmp_path, monkeypatch):
    import config as cfg
    from preview_cache import evict_if_over_quota
    from services.source_frame_preview import SourceFrameUnavailable, source_frame_bytes
    app, db, pid, _path = _source_photo(app_and_db, tmp_path)
    client = app.test_client()
    url = f'/photos/{pid}/preview?size=1920&view=source'
    assert client.get(url).status_code == 200
    row = db.caches.paired_preview_oldest_first()[0]
    root = Path(app.config['THUMB_CACHE_DIR']).parent
    artifact = root / 'previews' / 'paired' / row['filename']
    assert artifact.is_file()
    active = db._active_workspace_id
    db.set_active_workspace(db.create_workspace('Other inspection workspace'))
    with pytest.raises(SourceFrameUnavailable):
        source_frame_bytes(db, pid, 1920, str(root), 90,
                           lambda *a: pytest.fail('cross-workspace decode'))
    db.set_active_workspace(active)
    monkeypatch.setattr(cfg, 'load', lambda: {'preview_cache_max_mb': 0})
    evict_if_over_quota(db, str(root))
    assert not artifact.exists()
    assert db.caches.paired_preview_oldest_first() == []
    assert db.caches.preview_total_bytes() == 0
