"""Batch development works through visible controls and preserves individual edits."""

from pathlib import Path

import pytest
from PIL import Image
from playwright.sync_api import expect


@pytest.fixture
def batch_photos(live_server, tmp_path):
    db = live_server['db']
    folder = tmp_path / 'batch-photos'
    folder.mkdir()
    folder_id = db.add_folder(str(folder))
    ids = []
    for index, exposure in enumerate([-.5, 1.2]):
        path = folder / f'field-bird-{index}.jpg'
        Image.new('RGB', (240, 160), (80 + index * 30, 110, 90)).save(path)
        pid = db.add_photo(folder_id=folder_id, filename=path.name, extension='.jpg',
                           file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
                           width=240, height=160)
        db.set_photo_edit_recipe(pid, {
            'crop': {'x': .1 * index, 'y': .1, 'w': .8, 'h': .8},
            'adjustments': {'exposure': exposure, 'white_balance': {'temperature': index * 15 + 5}},
        })
        ids.append(pid)
    return ids


def _open_batch(page, live_server, ids):
    page.goto(live_server['url'] + '/browse')
    for pid in ids:
        page.locator(f'.grid-card[data-id="{pid}"]').click(modifiers=['Meta'])
    page.locator('#batchDevelopBtn').click()
    dialog = page.get_by_role('dialog')
    expect(dialog.get_by_label('Adjustment', exact=True)).to_be_enabled()
    return dialog


def test_live_relative_edit_preserves_crops_and_supports_undo(live_server, page, batch_photos):
    db = live_server['db']
    before = [db.get_photo_edit_recipe(pid) for pid in batch_photos]
    dialog = _open_batch(page, live_server, batch_photos)
    expect(dialog.get_by_text('Current values differ.', exact=False)).to_be_visible()
    dialog.get_by_label('Adjustment mode').select_option('relative')
    number = dialog.get_by_label('Numeric adjustment value')
    number.fill('0.3')
    number.press('Tab')
    expect(dialog.get_by_role('status')).to_have_text('Updated 2 photos.')
    expect(number).to_have_value('0')
    for index, pid in enumerate(batch_photos):
        after = db.get_photo_edit_recipe(pid)
        assert after['crop'] == before[index]['crop']
        assert after['adjustments']['exposure'] == pytest.approx(before[index]['adjustments']['exposure'] + .3)
        assert after['adjustments']['white_balance'] == before[index]['adjustments']['white_balance']
    expect(dialog.get_by_role('button', name='Done', exact=True)).to_be_enabled()
    Path('.context').mkdir(exist_ok=True)
    page.screenshot(path='.context/batch-development.png')
    dialog.get_by_role('button', name='Done', exact=True).click()
    assert page.request.post(live_server['url'] + '/api/undo').ok
    assert [db.get_photo_edit_recipe(pid) for pid in batch_photos] == before


def test_selective_paste_only_changes_checked_white_balance(live_server, page, batch_photos):
    db = live_server['db']
    before = [db.get_photo_edit_recipe(pid) for pid in batch_photos]
    dialog = _open_batch(page, live_server, batch_photos)
    page.evaluate("vireoEditNav.setCopiedRecipe({rotation:90, adjustments:{exposure:3, white_balance:{temperature:40}}})")
    dialog.get_by_role('button', name='Paste settings…').click()
    picker = page.get_by_role('dialog', name='Paste development settings to 2 photos')
    expect(picker).to_be_visible()
    expect(dialog.get_by_role('status')).to_be_empty()
    picker.get_by_role('button', name='Select none', exact=True).click()
    picker.get_by_label('Temperature', exact=True).check()
    picker.get_by_role('button', name='Apply selected settings').click()
    expect(dialog.get_by_role('status')).to_have_text('Updated 2 photos.')
    for index, pid in enumerate(batch_photos):
        after = db.get_photo_edit_recipe(pid)
        assert after['crop'] == before[index]['crop']
        assert after.get('rotation', 0) == 0
        assert after['adjustments']['exposure'] == before[index]['adjustments']['exposure']
        assert after['adjustments']['white_balance'] == {'temperature': 40}


def test_geometry_preset_merges_with_unsaved_edits(live_server, page, batch_photos):
    pid = batch_photos[0]
    response = page.request.post(live_server['url'] + '/api/edit-presets', data={
        'name': 'Portrait rotation', 'recipe': {'rotation': 90}, 'fields': ['rotation'],
    })
    preset_id = response.json()['preset']['id']
    page.goto(live_server['url'] + f'/edit/{pid}')
    page.wait_for_function('!editorState.loading')
    page.locator('#exposureRange').evaluate("el => {el.value='2.3'; el.dispatchEvent(new Event('input'));}")
    page.locator('#presetSelect').select_option(str(preset_id))
    page.locator('#applyPresetBtn').click()
    picker = page.get_by_role('dialog')
    expect(picker.get_by_label('Rotation', exact=True)).to_be_checked()
    expect(picker.get_by_label('Exposure', exact=True)).to_have_count(0)
    picker.get_by_role('button', name='Apply selected settings').click()
    page.wait_for_function('editorState.recipe.rotation === 90')
    expect(page.locator('#exposureRange')).to_have_value('2.3')
    assert live_server['db'].get_photo_edit_recipe(pid)['adjustments']['exposure'] == -.5
    page.locator('#saveBtn').click()
    expect(page.locator('#saveBtn')).to_be_disabled()
    assert live_server['db'].get_photo_edit_recipe(pid)['rotation'] == 90


def test_batch_preset_changes_only_its_saved_fields(live_server, page, batch_photos):
    db = live_server['db']
    before = [db.get_photo_edit_recipe(pid) for pid in batch_photos]
    response = page.request.post(live_server['url'] + '/api/edit-presets', data={
        'name': 'Warm light', 'recipe': {'adjustments': {'white_balance': {'temperature': 25}}},
        'fields': ['adjustments.white_balance.temperature'],
    })
    dialog = _open_batch(page, live_server, batch_photos)
    dialog.get_by_label('Batch preset').select_option(str(response.json()['preset']['id']))
    dialog.get_by_role('button', name='Apply preset…').click()
    picker = page.get_by_role('dialog', name='Apply preset “Warm light”')
    expect(picker).to_be_visible()
    expect(dialog.get_by_role('status')).to_be_empty()
    expect(picker.get_by_role('checkbox')).to_have_count(1)
    picker.get_by_role('button', name='Apply selected settings').click()
    expect(dialog.get_by_role('status')).to_have_text('Updated 2 photos.')
    for index, pid in enumerate(batch_photos):
        after = db.get_photo_edit_recipe(pid)
        assert after['crop'] == before[index]['crop']
        assert after['adjustments']['exposure'] == before[index]['adjustments']['exposure']
        assert after['adjustments']['white_balance']['temperature'] == 25


def test_live_absolute_slider_preserves_other_controls(live_server, page, batch_photos):
    db = live_server['db']
    before = [db.get_photo_edit_recipe(pid) for pid in batch_photos]
    dialog = _open_batch(page, live_server, batch_photos)
    slider = dialog.get_by_label('Adjustment value', exact=True)
    slider.evaluate("""el => {
      el.value = '0.7';
      el.dispatchEvent(new Event('input', {bubbles:true}));
      el.dispatchEvent(new Event('change', {bubbles:true}));
    }""")
    expect(dialog.get_by_role('status')).to_have_text('Updated 2 photos.')
    expect(dialog.get_by_label('Numeric adjustment value')).to_have_value('0.7')
    for index, pid in enumerate(batch_photos):
        after = db.get_photo_edit_recipe(pid)
        assert after['crop'] == before[index]['crop']
        assert after['adjustments']['exposure'] == .7
        assert after['adjustments']['white_balance'] == before[index]['adjustments']['white_balance']


def test_batch_adjustments_work_when_optional_presets_fail(live_server, page, batch_photos):
    page.route('**/api/edit-presets', lambda route: route.abort())
    dialog = _open_batch(page, live_server, batch_photos)
    expect(dialog.get_by_text('Presets could not load.', exact=False)).to_be_visible()
    expect(dialog.get_by_label('Batch preset')).to_be_disabled()
    number = dialog.get_by_label('Numeric adjustment value')
    number.fill('0.7')
    number.press('Enter')
    expect(dialog.get_by_role('status')).to_have_text('Updated 2 photos.')
    expect(dialog.get_by_role('button', name='Paste settings…')).to_be_enabled()
    for pid in batch_photos:
        assert live_server['db'].get_photo_edit_recipe(pid)['adjustments']['exposure'] == .7
