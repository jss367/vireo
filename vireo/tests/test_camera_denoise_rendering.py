"""Preview, thumbnail and export agree on camera metadata and rendered pixels."""

import io
import json

import numpy as np
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
    assert warmed == expected_jpeg

    exported = load_export_image(photo, vireo_dir, {fid: str(folder)}, recipe=recipe)
    np.testing.assert_array_equal(exported, expected)
    # Export queries may omit the large EXIF column and supply it separately.
    lean_photo = {k: v for k, v in dict(photo).items() if k != 'exif_data'}
    exported = load_export_image(lean_photo, vireo_dir, {fid: str(folder)}, recipe=recipe, exif_data=json.dumps(metadata))
    np.testing.assert_array_equal(exported, expected)

    thumbs = tmp_path / 'camera-thumbnails'
    generate_all(db, str(thumbs), config={'thumbnail_size': 256, 'thumbnail_quality': 90}, vireo_dir=vireo_dir)
    assert (thumbs / f'{pid}.jpg').read_bytes() == expected_jpeg


def test_export_paths_resolve_camera_profile_from_promoted_columns(app_and_db, tmp_path):
    """Exports render the same pixels as previews when EXIF is empty.

    ``PHOTO_COLS`` omits camera_make/camera_model/iso, and when
    ``pipeline.extract_full_metadata`` is disabled ``exif_data`` is stored as
    ``{}``. Every export path (photo export, panorama, site publishing, site
    export) fetches its photos through ``get_photos_by_ids``, so unless the
    promoted camera identity columns are loaded independently, camera-aware
    denoising silently falls back to the image-only estimate and diverges
    from previews/thumbnails on the same recipe.
    """
    import export
    import site_publish

    app, db = app_and_db
    folder = tmp_path / 'export-camera'
    folder.mkdir()
    source = folder / 'noisy.png'
    pixels = np.clip(120 + np.random.default_rng(29).normal(0, 9, (128, 192, 3)), 0, 255).astype(np.uint8)
    Image.fromarray(pixels).save(source)
    fid = db.add_folder(str(folder))
    pid = db.add_photo(
        folder_id=fid, filename=source.name, extension='.png',
        width=192, height=128, file_size=source.stat().st_size,
        file_mtime=source.stat().st_mtime,
    )
    # Simulate ``pipeline.extract_full_metadata`` disabled: promoted columns
    # populated, ``exif_data`` empty.
    db.conn.execute(
        'UPDATE photos SET camera_make=?, camera_model=?, iso=?, exif_data=? WHERE id=?',
        ('NIKON CORPORATION', 'NIKON Z 8', 25600, json.dumps({}), pid),
    )
    db.conn.commit()

    thin_photo = db.get_photos_by_ids([pid])[pid]
    assert 'camera_make' not in dict(thin_photo)
    assert 'iso' not in dict(thin_photo)

    recipe = {'adjustments': {'denoise_mode': 'camera', 'noise_reduction': 85}}
    expected = apply_recipe_to_loaded_image(
        Image.fromarray(pixels), recipe,
        camera_metadata={'camera_make': 'NIKON CORPORATION',
                         'camera_model': 'NIKON Z 8', 'iso': 25600},
    )
    without_metadata = apply_recipe_to_loaded_image(Image.fromarray(pixels), recipe)
    # Sanity check: camera-aware denoising actually diverges from the fallback,
    # otherwise this test would pass even under the missing-fields regression.
    assert not np.array_equal(expected, without_metadata)

    render_camera = export._get_photo_render_camera_fields(db, [pid])
    assert render_camera[pid] == {
        'camera_make': 'NIKON CORPORATION',
        'camera_model': 'NIKON Z 8',
        'iso': 25600,
    }

    # Photo export path.
    exported = export.load_export_image(
        thin_photo, str(tmp_path), {fid: str(folder)},
        recipe=recipe, exif_data='{}',
        camera_fields=render_camera[pid],
    )
    try:
        np.testing.assert_array_equal(exported, expected)
    finally:
        exported.close()

    # Falling back to the fallback (no camera_fields passed) exercises the
    # regression: without the promoted columns, the render matches the
    # image-only estimate rather than the camera-aware one.
    regressed = export.load_export_image(
        thin_photo, str(tmp_path), {fid: str(folder)},
        recipe=recipe, exif_data='{}',
    )
    try:
        assert not np.array_equal(regressed, expected)
    finally:
        regressed.close()

    # site_publish._export_image threads camera_fields through the same
    # load_export_image call, so a smoke test confirms the wiring.
    site_dest = tmp_path / 'site-publish'
    site_dest.mkdir()
    ok, err = site_publish._export_image(
        str(tmp_path), dict(thin_photo), 'photo.jpg', str(site_dest),
        {'quality': 95}, {fid: str(folder)}, export._DevelopedDirIndex(),
        recipe, '{}', camera_fields=render_camera[pid],
    )
    assert ok, err
    with Image.open(site_dest / 'photo.jpg') as saved:
        saved_pixels = np.asarray(saved.convert('RGB'))
    with Image.fromarray(pixels) as pristine:
        expected_site = apply_recipe_to_loaded_image(
            pristine, recipe,
            camera_metadata={'camera_make': 'NIKON CORPORATION',
                             'camera_model': 'NIKON Z 8', 'iso': 25600},
        )
        try:
            expected_bytes = io.BytesIO()
            expected_site.save(expected_bytes, 'JPEG', quality=95)
        finally:
            expected_site.close()
    with Image.open(io.BytesIO(expected_bytes.getvalue())) as reference:
        np.testing.assert_array_equal(saved_pixels, np.asarray(reference.convert('RGB')))

    # site_export captures the photo dict up-front; a photo carrying the
    # promoted columns must survive the snapshot round-trip.
    enriched = dict(thin_photo)
    for key, value in render_camera[pid].items():
        enriched.setdefault(key, value)
    assert enriched.get('camera_make') == 'NIKON CORPORATION'
    round_tripped = json.loads(json.dumps({'photo': enriched}))['photo']
    exported_snapshot = export.load_export_image(
        round_tripped, str(tmp_path), {fid: str(folder)},
        recipe=recipe, exif_data='{}',
    )
    try:
        np.testing.assert_array_equal(exported_snapshot, expected)
    finally:
        exported_snapshot.close()
