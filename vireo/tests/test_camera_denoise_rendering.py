"""Preview, thumbnail and export agree on camera metadata and rendered pixels."""

import io
import json
from pathlib import Path

import numpy as np
import pytest
from camera_denoise import cache_save_options
from export import load_export_image
from image_edits import apply_recipe_to_loaded_image
from PIL import Image
from preview_materializer import render_preview_bytes
from thumbnails import generate_all


def test_camera_metadata_reaches_all_render_paths(app_and_db, tmp_path):
    app, db = app_and_db
    folder = tmp_path / 'photos'
    folder.mkdir()
    source = folder / 'noisy.png'
    pixels = np.clip(120 + np.random.default_rng(13).normal(0, 9, (128, 192, 3)), 0, 255).astype(np.uint8)
    Image.fromarray(pixels).save(source)
    fid = db.add_folder(str(folder))
    pid = db.add_photo(
        folder_id=fid, filename=source.name, extension='.png',
        width=192, height=128, file_size=source.stat().st_size,
        file_mtime=source.stat().st_mtime,
    )
    metadata = {'EXIF': {'Make': 'NIKON CORPORATION', 'Model': 'NIKON Z 8', 'ISO': 25600}}
    db.conn.execute('UPDATE photos SET exif_data=? WHERE id=?', (json.dumps(metadata), pid))
    db.conn.commit()
    photo = db.get_photo(pid)
    recipe = {'adjustments': {'denoise_mode': 'camera', 'noise_reduction': 85}}
    client = app.test_client()
    assert client.put(f'/api/photos/{pid}/edit-recipe', json={'recipe': recipe}).status_code == 200
    info = client.get(f'/api/photos/{pid}').get_json()
    assert info['denoise_profile']['source'] == 'camera'
    assert info['denoise_profile']['iso'] == 25600

    expected = apply_recipe_to_loaded_image(Image.fromarray(pixels), recipe, camera_metadata=photo)
    without_metadata = apply_recipe_to_loaded_image(Image.fromarray(pixels), recipe)
    assert not np.array_equal(expected, without_metadata)
    encoded = io.BytesIO()
    expected.save(encoded, format='JPEG', quality=90)
    expected_jpeg = encoded.getvalue()

    preview = client.get(f'/photos/{pid}/edit-preview', query_string={'size': 256, 'recipe': json.dumps(recipe)})
    assert preview.status_code == 200
    assert preview.data == expected_jpeg
    vireo_dir = str(tmp_path)
    warmed = render_preview_bytes(db, photo, str(folder), size=256, vireo_dir=vireo_dir, preview_quality=90, recipe=recipe)
    cached_encoding = io.BytesIO()
    expected.save(cached_encoding, format='JPEG', quality=90, **cache_save_options(photo, recipe))
    assert warmed == cached_encoding.getvalue()

    exported = load_export_image(photo, vireo_dir, {fid: str(folder)}, recipe=recipe)
    np.testing.assert_array_equal(exported, expected)
    # Export queries may omit the large EXIF column and supply it separately.
    lean_photo = {k: v for k, v in dict(photo).items() if k != 'exif_data'}
    exported = load_export_image(lean_photo, vireo_dir, {fid: str(folder)}, recipe=recipe, exif_data=json.dumps(metadata))
    np.testing.assert_array_equal(exported, expected)

    thumbs = tmp_path / 'camera-thumbnails'
    generate_all(db, str(thumbs), config={'thumbnail_size': 256, 'thumbnail_quality': 90}, vireo_dir=vireo_dir)
    assert (thumbs / f'{pid}.jpg').read_bytes() == cached_encoding.getvalue()


@pytest.mark.parametrize('metadata_source', ['promoted_columns', 'grouped_exif'])
def test_cached_renders_refresh_after_camera_metadata_changes(app_and_db, tmp_path, monkeypatch, metadata_source):
    import subprocess

    import config as cfg
    import inat
    from web.media import _paired_render_state_hash

    app, db = app_and_db
    folder = tmp_path / 'cache-photos'
    folder.mkdir()
    source = folder / 'noise.png'
    pixels = np.clip(120 + np.random.default_rng(17).normal(0, 9, (96, 144, 3)), 0, 255).astype(np.uint8)
    Image.fromarray(pixels).save(source)
    fid = db.add_folder(str(folder))
    pid = db.add_photo(folder_id=fid, filename=source.name, extension='.png', width=144, height=96,
                       file_size=source.stat().st_size, file_mtime=source.stat().st_mtime)
    recipe = {'adjustments': {'denoise_mode': 'camera', 'noise_reduction': 85}}
    db.set_photo_edit_recipe(pid, recipe)
    client = app.test_client()
    vireo_dir = Path(app.config['THUMB_CACHE_DIR']).parent
    monkeypatch.setattr(subprocess, 'run', lambda *a, **k: subprocess.CompletedProcess(a[0] if a else [], 0))
    monkeypatch.setattr(subprocess, 'Popen', lambda *a, **k: None)
    monkeypatch.setattr(inat, 'submit_observation', lambda **kwargs: (123, 'https://www.inaturalist.org/observations/123'))
    cfg.save({'inat_token': 'fake-token'})

    urls = [f'/thumbnails/{pid}.jpg', f'/photos/{pid}/preview?size=1920', f'/photos/{pid}/original']

    def renders():
        result = []
        for url in urls:
            response = client.get(url)
            assert response.status_code == 200, response.get_data(as_text=True)
            result.append(response.data)
        response = client.post('/api/photos/open-external', json={'photo_ids': [pid], 'editor_index': None})
        handoff = vireo_dir / 'external-edits' / f'{pid}.jpg'
        assert handoff.exists(), response.get_json()
        result.append(handoff.read_bytes())
        response = client.post('/api/inat/submit', json={'photo_id': pid})
        assert response.status_code == 200, response.get_json()
        result.append((vireo_dir / 'inat-uploads' / f'{pid}.jpg').read_bytes())
        return result

    before = renders()
    assert renders() == before
    original_stat = source.stat()
    paired_before = _paired_render_state_hash(db.get_photo(pid), 1920, 'raw', str(source), recipe)
    if metadata_source == 'promoted_columns':
        db.conn.execute('UPDATE photos SET camera_make=?, camera_model=?, iso=? WHERE id=?',
                        ('NIKON CORPORATION', 'NIKON Z 8', 100, pid))
    else:
        db.conn.execute('UPDATE photos SET exif_data=? WHERE id=?',
                        (json.dumps({'EXIF': {'Make': 'NIKON CORPORATION', 'Model': 'NIKON Z 8', 'ISO': 100}}), pid))
    db.conn.commit()
    assert source.stat().st_mtime_ns == original_stat.st_mtime_ns
    assert source.stat().st_size == original_stat.st_size
    paired_after = _paired_render_state_hash(db.get_photo(pid), 1920, 'raw', str(source), recipe)
    assert paired_before != paired_after
    after = renders()
    for old, new in zip(before, after, strict=True):
        old_pixels = np.asarray(Image.open(io.BytesIO(old)))
        new_pixels = np.asarray(Image.open(io.BytesIO(new)))
        assert not np.array_equal(old_pixels, new_pixels)
    assert renders() == after


def test_preview_warmer_and_thumbnail_generator_refresh_profile(tmp_path):
    from camera_denoise import cache_matches
    from preview_materializer import materialize_preview
    from thumbnails import generate_thumbnail

    source = tmp_path / 'source.png'
    Image.fromarray(np.random.default_rng(12).integers(80, 140, (64, 96, 3), dtype=np.uint8)).save(source)
    photo = {'id': 7, 'folder_id': 3, 'filename': source.name, 'width': 96, 'height': 64,
             'working_copy_path': None, 'companion_path': None, 'camera_make': 'Nikon', 'camera_model': 'Z8', 'iso': 100}
    recipe = {'adjustments': {'denoise_mode': 'camera', 'noise_reduction': 80}}
    preview = tmp_path / 'preview.jpg'
    first = materialize_preview(None, photo, str(tmp_path), size=1920, vireo_dir=str(tmp_path),
                                preview_quality=90, recipe=recipe, cache_path=str(preview))
    assert first.generated
    thumb = generate_thumbnail(7, str(source), str(tmp_path / 'thumbs'), recipe=recipe, camera_metadata=photo)
    old_preview, old_thumb = preview.read_bytes(), Path(thumb).read_bytes()
    photo['iso'] = 25600
    second = materialize_preview(None, photo, str(tmp_path), size=1920, vireo_dir=str(tmp_path),
                                 preview_quality=90, recipe=recipe, cache_path=str(preview))
    assert second.generated
    assert preview.read_bytes() != old_preview
    generate_thumbnail(7, str(source), str(tmp_path / 'thumbs'), recipe=recipe, camera_metadata=photo)
    assert Path(thumb).read_bytes() != old_thumb
    assert cache_matches(preview, photo, recipe)
    assert cache_matches(thumb, photo, recipe)
    third = materialize_preview(None, photo, str(tmp_path), size=1920, vireo_dir=str(tmp_path),
                                preview_quality=90, recipe=recipe, cache_path=str(preview))
    assert not third.generated


def test_exports_use_promoted_camera_fields_without_full_exif_or_metadata_embedding(app_and_db, tmp_path):
    from camera_denoise import resolve_profile
    from export import export_photos

    app, db = app_and_db
    folder = tmp_path / 'promoted-camera-photos'
    folder.mkdir()
    source = folder / 'nikon.png'
    pixels = np.clip(120 + np.random.default_rng(23).normal(0, 10, (96, 144, 3)), 0, 255).astype(np.uint8)
    Image.fromarray(pixels).save(source)
    fid = db.add_folder(str(folder))
    pid = db.add_photo(folder_id=fid, filename=source.name, extension='.png', width=144, height=96,
                       file_size=source.stat().st_size, file_mtime=source.stat().st_mtime)
    # This is how extraction stores summary-only metadata when full EXIF is disabled.
    db.conn.execute('UPDATE photos SET camera_make=?, camera_model=?, iso=?, exif_data=? WHERE id=?',
                    ('NIKON CORPORATION', 'NIKON D850', 100, '{}', pid))
    db.conn.commit()
    recipe = {'adjustments': {'denoise_mode': 'camera', 'noise_reduction': 85}}
    db.set_photo_edit_recipe(pid, recipe)
    detail_photo = db.get_photo(pid)
    bulk_photo = db.get_photos_by_ids([pid])[pid]
    assert resolve_profile(bulk_photo)['source'] == 'camera'
    assert resolve_profile(bulk_photo) == resolve_profile(detail_photo)
    expected = apply_recipe_to_loaded_image(Image.fromarray(pixels), recipe, camera_metadata=detail_photo)
    fallback = apply_recipe_to_loaded_image(Image.fromarray(pixels), recipe)
    assert not np.array_equal(expected, fallback)

    vireo_dir = str(Path(app.config['THUMB_CACHE_DIR']).parent)
    # Panoramas and site publishing/export share this loader and bulk photo query.
    rendered = load_export_image(bulk_photo, vireo_dir, {fid: str(folder)}, recipe=recipe, exif_data='{}')
    np.testing.assert_array_equal(rendered, expected)
    destination = tmp_path / 'export-without-metadata'
    result = export_photos(db, vireo_dir, [pid], destination=str(destination),
                           options={'format': 'png', 'metadata_fields': [], 'collect_files': True})
    assert result['errors'] == []
    assert result['exported'] == 1
    with Image.open(result['files'][0]) as exported:
        np.testing.assert_array_equal(exported, expected)
        assert not exported.getexif()
