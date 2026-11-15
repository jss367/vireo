"""Synthetic upgrade regression; touches only temporary cache and DB files."""
import pytest
from db import Database
from services import photo_ingest, workspace_folders


@pytest.mark.parametrize('fail_first_purge', [False, True])
def test_new_raw_look_invalidates_recipe_free_raw_artifacts(tmp_path, monkeypatch, fail_first_purge):
    import config as cfg
    from app import create_app
    from image_edits import EDIT_MATH_VERSION
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(cfg, 'CONFIG_PATH', str(tmp_path / 'config.json'))
    vireo_dir = tmp_path / 'vireo'
    thumbs = vireo_dir / 'thumbnails'
    previews = vireo_dir / 'previews'
    thumbs.mkdir(parents=True)
    previews.mkdir()
    db_path = str(vireo_dir / 'vireo.db')
    db = Database(db_path)
    try:
        folder = workspace_folders.add_folder(db, str(tmp_path / 'synthetic-photos'))
        pid = photo_ingest.add_photo(db, folder, 'recipe-free.NEF', '.nef', 1, 1)
        jpeg_id = photo_ingest.add_photo(db, folder, 'plain.jpg', '.jpg', 1, 1)
        db.meta.set('edit_math_version', str(EDIT_MATH_VERSION - 1))
        raw_thumb = thumbs / f'{pid}_raw.jpg'
        raw_thumb.write_bytes(b'stale synthetic scene-linear render')
        ordinary_thumb = thumbs / f'{pid}.jpg'
        ordinary_thumb.write_bytes(b'unchanged embedded JPEG')
        pair_jpeg_thumb = thumbs / f'{pid}_jpeg.jpg'
        pair_jpeg_thumb.write_bytes(b'unchanged paired JPEG')
        db.conn.execute('UPDATE photos SET thumb_path = ? WHERE id = ?', (ordinary_thumb.name, pid))
        preview = previews / f'{pid}_1920.jpg'
        preview.write_bytes(b'stale synthetic scene-linear preview')
        jpeg_thumb = thumbs / f'{jpeg_id}.jpg'
        jpeg_thumb.write_bytes(b'unchanged synthetic camera JPEG')
        db.caches.preview_insert(pid, 1920, preview.stat().st_size)
        db.commit()
        assert db.edits.edited_photo_ids() == []
        if fail_first_purge:
            import app as app_module
            real_remove = app_module.os.remove

            def remove_except_raw(path):
                if str(path) == str(raw_thumb):
                    raise PermissionError('synthetic locked RAW pair thumbnail')
                return real_remove(path)

            with monkeypatch.context() as blocked:
                blocked.setattr(app_module.os, 'remove', remove_except_raw)
                create_app(db_path=db_path, thumb_cache_dir=str(thumbs))
                assert db.meta.get('edit_math_version') == str(EDIT_MATH_VERSION - 1)
                assert raw_thumb.exists()
        create_app(db_path=db_path, thumb_cache_dir=str(thumbs))
        assert not raw_thumb.exists(), 'recipe-free RAW pair thumbnail survives look migration'
        assert ordinary_thumb.read_bytes() == b'unchanged embedded JPEG'
        assert pair_jpeg_thumb.read_bytes() == b'unchanged paired JPEG'
        assert preview.exists(), 'ordinary recipe-free previews use the JPEG-first decode'
        assert db.caches.preview_get(pid, 1920) is not None
        assert db.photos.get(pid)['thumb_path'] == ordinary_thumb.name
        assert jpeg_thumb.read_bytes() == b'unchanged synthetic camera JPEG'
        assert db.meta.get('edit_math_version') == str(EDIT_MATH_VERSION)
    finally:
        db.close()


def test_recipe_free_url_changes_with_render_math(monkeypatch):
    import image_edits
    from photo_payload import render_key_for_recipe
    monkeypatch.setattr(image_edits, 'EDIT_MATH_VERSION', 13)
    before = render_key_for_recipe(None)
    monkeypatch.setattr(image_edits, 'EDIT_MATH_VERSION', 14)
    assert before != render_key_for_recipe(None)


def test_recipe_free_raw_pair_preview_identity_changes_with_render_math(tmp_path, monkeypatch):
    import image_edits
    from web.media_common import _paired_render_state_hash
    photo = {'id': 1, 'filename': 'synthetic.nef'}
    source = str(tmp_path / 'synthetic.nef')
    monkeypatch.setattr(image_edits, 'EDIT_MATH_VERSION', 13)
    before = _paired_render_state_hash(photo, 1920, 'raw', source, None)
    monkeypatch.setattr(image_edits, 'EDIT_MATH_VERSION', 14)
    assert before != _paired_render_state_hash(photo, 1920, 'raw', source, None)
