"""Camera denoising survives editor saves, presets, history, and resets."""

import json

import numpy as np
import pytest
from PIL import Image
from playwright.sync_api import expect


@pytest.fixture
def denoise_photo(live_server, tmp_path):
    folder = tmp_path / 'camera-photos'
    folder.mkdir()
    pixels = np.clip(120 + np.random.default_rng(7).normal(0, 12, (160, 240, 3)), 0, 255).astype(np.uint8)
    path = folder / 'high-iso-wildlife.png'
    Image.fromarray(pixels).save(path)
    db = live_server['db']
    folder_id = db.add_folder(str(folder))
    pid = db.add_photo(
        folder_id=folder_id, filename=path.name, extension='.png',
        file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
        width=240, height=160,
    )
    db.conn.execute('UPDATE photos SET exif_data=? WHERE id=?', (
        json.dumps({'EXIF': {'Make': 'NIKON CORPORATION', 'Model': 'NIKON Z 8', 'ISO': 3200}}), pid,
    ))
    db.conn.commit()
    return pid


def _open(page, live_server, pid):
    page.goto(f"{live_server['url']}/edit/{pid}")
    expect(page.locator('#editorFilename')).to_have_text('high-iso-wildlife.png')
    page.wait_for_function('!editorState.loading')


def test_camera_denoise_save_reload_history_reset(live_server, page, denoise_photo):
    _open(page, live_server, denoise_photo)
    page.get_by_label('Denoise method', exact=True).select_option('camera')
    expect(page.locator('#denoiseProfileStatus')).to_contain_text('NIKON Z 8 · ISO 3200')
    expect(page.locator('#denoiseProfileStatus')).to_contain_text('Camera profile + image noise estimate')
    page.evaluate("setAdjustment('noise_reduction', 70)")
    with page.expect_response('**/edit-preview?*') as preview:
        page.evaluate('updatePreview()')
    assert preview.value.ok
    assert page.evaluate('saveRecipe()') is True
    recipe = live_server['db'].get_photo_edit_recipe(denoise_photo)
    assert recipe['adjustments'] == {'denoise_mode': 'camera', 'noise_reduction': 70.0}
    page.reload()
    page.wait_for_function('!editorState.loading')
    expect(page.locator('#denoiseModeSelect')).to_have_value('camera')
    expect(page.locator('#noise_reductionRange')).to_have_value('70')
    expect(page.locator('#saveBtn')).to_be_disabled()
    page.evaluate('resetAdjustments()')
    expect(page.locator('#denoiseModeSelect')).to_have_value('camera')
    page.get_by_role('button', name='Reset Detail', exact=True).click()
    expect(page.locator('#denoiseModeSelect')).to_have_value('standard')
    expect(page.locator('#noise_reductionRange')).to_have_value('0')
    assert page.evaluate('saveRecipe()') is True
    assert page.evaluate('doUndo()') is True
    expect(page.locator('#denoiseModeSelect')).to_have_value('camera')
    assert page.evaluate('doRedo()') is True
    expect(page.locator('#denoiseModeSelect')).to_have_value('standard')


def test_unknown_camera_fallback_and_preset(live_server, page, denoise_photo):
    db = live_server['db']
    db.conn.execute('UPDATE photos SET exif_data=NULL WHERE id=?', (denoise_photo,))
    db.conn.commit()
    _open(page, live_server, denoise_photo)
    page.get_by_label('Denoise method', exact=True).select_option('camera')
    page.evaluate("setAdjustment('noise_reduction', 55)")
    expect(page.locator('#denoiseProfileStatus')).to_contain_text('using image noise estimate')
    page.locator('#presetNameInput').fill('Camera noise reduction')
    page.evaluate('saveCurrentAsPreset()')
    page.evaluate('resetDetail()')
    page.evaluate('applySelectedPreset()')
    expect(page.locator('#denoiseModeSelect')).to_have_value('camera')
    expect(page.locator('#noise_reductionRange')).to_have_value('55')
    # The recipe serializer used by presets must retain the method without
    # embedding the current photo's camera identity or coefficients.
    recipe = page.evaluate('recipeForSave(editorState.recipe)')
    assert recipe['adjustments'] == {'denoise_mode': 'camera', 'noise_reduction': 55}
    assert 'camera' not in recipe

    page.evaluate('autoTone()')
    expect(page.locator('#toastContainer')).to_contain_text('Auto-balanced tones')
    expect(page.locator('#denoiseModeSelect')).to_have_value('camera')
    assert page.evaluate('recipeForSave(editorState.recipe).adjustments.denoise_mode') == 'camera'
