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
