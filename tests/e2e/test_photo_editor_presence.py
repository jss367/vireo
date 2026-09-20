"""Presence controls persist through editor workflows and render previews."""

import io
import json
from urllib.parse import parse_qs, urlparse

import pytest
from PIL import Image
from playwright.sync_api import expect


@pytest.fixture
def presence_photo(live_server, tmp_path):
    folder = tmp_path / 'presence-photos'
    folder.mkdir()
    image = Image.new('RGB', (256, 128))
    image.putdata([(60 + x // 2, 80 + x // 3, 100 + x // 4) for _ in range(128) for x in range(256)])
    path = folder / 'hazy-landscape.png'
    image.save(path)
    db = live_server['db']
    folder_id = db.add_folder(str(folder))
    return db.add_photo(
        folder_id=folder_id, filename=path.name, extension='.png',
        file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
        width=image.width, height=image.height,
    )


def _set_range(page, name, value):
    page.locator(f'#{name}Range').evaluate(
        """(el, value) => {
            el.value = String(value);
            el.dispatchEvent(new Event('input', {bubbles: true}));
        }""", value,
    )


def _open(page, live_server, photo_id):
    page.goto(f"{live_server['url']}/edit/{photo_id}")
    expect(page.locator('#editorFilename')).to_have_text('hazy-landscape.png')
    page.wait_for_function('!editorState.loading')


def test_presence_preview_save_reload_and_history(live_server, page, presence_photo):
    _open(page, live_server, presence_photo)
    values = {'texture': 45, 'clarity': -30, 'dehaze': 55}
    for key, value in values.items():
        slider = page.get_by_label(key.capitalize(), exact=True)
        expect(slider).to_have_attribute('min', '-100')
        expect(slider).to_have_attribute('max', '100')
        _set_range(page, key, value)
        expect(page.locator(f'#{key}Value')).to_have_text(str(value))
    with page.expect_response('**/edit-preview?*') as preview:
        page.evaluate('updatePreview()')
    assert preview.value.ok
    query = parse_qs(urlparse(preview.value.url).query)
    assert json.loads(query['recipe'][0])['adjustments'] == values
    assert Image.open(io.BytesIO(preview.value.body())).size == (256, 128)

    assert page.evaluate('saveRecipe()') is True
    assert live_server['db'].get_photo_edit_recipe(presence_photo)['adjustments'] == values
    page.reload()
    for key, value in values.items():
        expect(page.locator(f'#{key}Range')).to_have_value(str(value))
    expect(page.locator('#saveBtn')).to_be_disabled()

    _set_range(page, 'texture', -60)
    assert page.evaluate('saveRecipe()') is True
    assert page.evaluate('doUndo()') is True
    expect(page.locator('#textureRange')).to_have_value('45')
    assert page.evaluate('doRedo()') is True
    expect(page.locator('#textureRange')).to_have_value('-60')


def test_presence_presets_reset_and_auto_tone(live_server, page, presence_photo):
    _open(page, live_server, presence_photo)
    values = {'texture': -25, 'clarity': 40, 'dehaze': -20}
    for key, value in values.items():
        _set_range(page, key, value)
    page.locator('#presetNameInput').fill('Soft landscape')
    page.locator('#savePresetBtn').click()
    page.get_by_role('dialog').get_by_role('button', name='Save preset', exact=True).click()
    expect(page.locator('#applyPresetBtn')).to_be_enabled()
    page.evaluate('resetAdjustments()')
    assert page.evaluate('recipeForSave(editorState.recipe).adjustments || {}') == {}
    for key in values:
        expect(page.locator(f'#{key}Range')).to_have_value('0')
    page.locator('#applyPresetBtn').click()
    page.get_by_role('dialog').get_by_role('button', name='Apply selected settings').click()
    for key, value in values.items():
        expect(page.locator(f'#{key}Range')).to_have_value(str(value))
    page.evaluate('autoTone()')
    adjustments = page.evaluate('recipeForSave(editorState.recipe).adjustments')
    for key, value in values.items():
        assert adjustments[key] == value
    expect(page.locator('#toastContainer')).to_contain_text('Auto-balanced tones')
