"""Working photo edits undo before saving, with one step per gesture."""

import pytest
from PIL import Image
from playwright.sync_api import expect


@pytest.fixture
def editor_photo(live_server, page, tmp_path):
    path = tmp_path / 'undo-photo.jpg'
    Image.new('RGB', (600, 400), (80, 120, 160)).save(path)
    db = live_server['db']
    folder_id = db.add_folder(str(tmp_path))
    photo_id = db.add_photo(
        folder_id=folder_id, filename=path.name, extension='.jpg',
        file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
        width=600, height=400,
    )
    page.goto(f"{live_server['url']}/edit/{photo_id}")
    expect(page.locator('#editorFilename')).to_have_text(path.name)
    page.wait_for_function('!editorState.loading')
    return photo_id


def set_range(page, name, value):
    page.locator('#' + name + 'Range').evaluate("""(el, value) => {
        el.value = String(value);
        el.dispatchEvent(new Event('input', {bubbles: true}));
    }""", value)


@pytest.mark.parametrize('modifier', ['Meta', 'Control'])
def test_unsaved_adjustments_keyboard_undo_redo(live_server, page, editor_photo, modifier):
    requests = []
    page.on('request', lambda req: requests.append(req) if req.method in ('PUT', 'POST') else None)
    set_range(page, 'exposure', 1)
    set_range(page, 'contrast', 25)
    # Sliders retain focus after editing; shortcuts must still reach the editor.
    page.locator('#contrastRange').focus()
    page.keyboard.press(modifier + '+z')
    expect(page.locator('#contrastRange')).to_have_value('0')
    expect(page.locator('#exposureRange')).to_have_value('1')
    page.keyboard.press(modifier + '+z')
    expect(page.locator('#exposureRange')).to_have_value('0')
    expect(page.locator('#saveBtn')).to_be_disabled()
    page.keyboard.press(modifier + '+Shift+z')
    expect(page.locator('#exposureRange')).to_have_value('1')
    page.keyboard.press(modifier + '+Shift+z')
    expect(page.locator('#contrastRange')).to_have_value('25')
    assert live_server['db'].get_photo_edit_recipe(editor_photo) is None
    assert not requests


def test_slider_drag_is_one_step_and_new_edit_discards_redo(page, editor_photo):
    slider = page.locator('#exposureRange')
    slider.dispatch_event('pointerdown', {'pointerId': 5, 'button': 0})
    for value in (0.5, 1, 1.5, 2):
        set_range(page, 'exposure', value)
    slider.dispatch_event('pointerup', {'pointerId': 5})
    page.locator('#historyUndoBtn').click()
    expect(slider).to_have_value('0')
    page.locator('#historyRedoBtn').click()
    expect(slider).to_have_value('2')
    page.locator('#historyUndoBtn').click()
    set_range(page, 'contrast', 15)
    expect(page.locator('#historyRedoBtn')).to_be_disabled()
    assert page.evaluate('doRedo()') is False
    expect(slider).to_have_value('0')
    expect(page.locator('#contrastRange')).to_have_value('15')


def test_crop_drag_and_reset_restore_recipe_and_preview(page, editor_photo):
    page.wait_for_function("""() => {
        const img = document.getElementById('editorImg');
        return img.complete && img.naturalWidth && editorImageMatchesZoomRecipe(img);
    }""")
    handle = page.locator('#editorCropBox [data-handle="se"]')
    box = handle.bounding_box()
    x, y = box['x'] + box['width'] / 2, box['y'] + box['height'] / 2
    page.mouse.move(x, y)
    page.mouse.down()
    page.mouse.move(x - 80, y - 60, steps=8)
    page.mouse.up()
    cropped = page.evaluate('recipeForSave(editorState.recipe)')
    assert cropped['crop']['w'] < 1
    page.keyboard.press('Meta+z')
    assert page.evaluate('recipeForSave(editorState.recipe)') == {}
    page.keyboard.press('Meta+Shift+z')
    assert page.evaluate('recipeForSave(editorState.recipe)') == cropped
    page.locator('#resetAllBtn').click()
    assert page.evaluate('recipeForSave(editorState.recipe)') == {}
    page.keyboard.press('Meta+z')
    assert page.evaluate('recipeForSave(editorState.recipe)') == cropped
    assert page.evaluate('previewRecipe().crop || null') is None  # Editable full-frame preview.


def test_save_starts_checkpoint_history_and_navigation_clears_local_steps(live_server, page, editor_photo):
    set_range(page, 'exposure', 1)
    assert page.evaluate('saveRecipe()') is True
    set_range(page, 'contrast', 20)
    assert page.evaluate('doUndo()') is True
    expect(page.locator('#contrastRange')).to_have_value('0')
    assert live_server['db'].get_photo_edit_recipe(editor_photo)['adjustments'] == {'exposure': 1}
    assert page.evaluate('doRedo()') is True
    assert page.evaluate('saveRecipe()') is True
    # With no working steps, Cmd-Z restores the previous saved checkpoint.
    page.locator('#editorCropBox').focus()
    page.keyboard.press('Meta+z')
    expect(page.locator('#contrastRange')).to_have_value('0')
    expect(page.locator('#exposureRange')).to_have_value('1')
    page.wait_for_function('!vireoHistoryBusy()')
    set_range(page, 'contrast', 30)
    other_id = live_server['data']['photos'][0]
    page.evaluate('(id) => loadPhoto(id)', other_id)
    expect(page.locator('#editorFilename')).to_have_text('hawk1.jpg')
    assert page.evaluate('editorHistory.undo.length + editorHistory.redo.length') == 0


def test_text_entry_and_modal_keep_native_shortcuts(page, editor_photo):
    set_range(page, 'exposure', 1)
    search = page.locator('#editorSearchInput')
    search.focus()
    page.keyboard.type('bird')
    page.keyboard.press('ControlOrMeta+z')
    expect(search).to_have_value('')
    expect(page.locator('#exposureRange')).to_have_value('1')
    # Exercise modal precedence without saving via openExportModal().
    page.evaluate("document.getElementById('exportOverlay').classList.add('open')")
    page.locator('#exportSubmitBtn').focus()
    page.keyboard.press('Meta+z')
    expect(page.locator('#exposureRange')).to_have_value('1')


def test_failed_save_preserves_working_undo_and_pending_save_blocks_it(page, editor_photo):
    set_range(page, 'exposure', 1)
    page.evaluate('editorState.savingPhotoIds[String(editorState.photoId)] = true')
    assert page.evaluate('doUndo()') is False
    expect(page.locator('#exposureRange')).to_have_value('1')
    page.evaluate('editorState.savingPhotoIds = {}')
    page.route('**/edit-recipe', lambda route: route.abort())
    assert page.evaluate('saveRecipe()') is False
    assert page.evaluate('doUndo()') is True
    expect(page.locator('#exposureRange')).to_have_value('0')


def test_presets_color_and_geometry_are_individual_steps(page, editor_photo):
    page.evaluate("""() => {
        editorState.presets = [{id: 1, name: 'Warm', recipe: {
            adjustments: {exposure: 0.5, white_balance: {temperature: 20}}
        }}];
        renderPresetOptions();
    }""")
    page.locator('#presetSelect').select_option('1')
    page.locator('#applyPresetBtn').click()
    preset = page.evaluate('recipeForSave(editorState.recipe)')
    page.evaluate('rotateRecipe(90)')
    rotated = page.evaluate('recipeForSave(editorState.recipe)')
    page.locator('#curveAddPoint').click()
    page.locator('#curveOutput').fill('65')
    page.locator('#curveOutput').press('Tab')
    curved = page.evaluate('recipeForSave(editorState.recipe)')
    assert curved['adjustments']['point_curves']['rgb'][1] == [50, 65]
    assert page.evaluate('doUndo()') is True
    assert page.evaluate('recipeForSave(editorState.recipe)') == rotated
    assert page.evaluate('doUndo()') is True
    assert page.evaluate('recipeForSave(editorState.recipe)') == preset
    assert page.evaluate('doUndo()') is True
    assert page.evaluate('recipeForSave(editorState.recipe)') == {}
    for _ in range(3):
        assert page.evaluate('doRedo()') is True
    assert page.evaluate('recipeForSave(editorState.recipe)') == curved


def test_local_adjustment_and_late_mask_update_do_not_overwrite_undo(page, editor_photo):
    page.route('**/local-mask/snapshot', lambda route: route.fulfill(
        json={'mask': {'ref': 'test-mask', 'source_digest': 'test-digest'}},
    ))
    page.evaluate("setLocalAdjustment('subject', 'exposure', 0.5, true)")
    page.wait_for_function('editorState.recipe.local')
    adjusted = page.evaluate('recipeForSave(editorState.recipe)')
    assert adjusted['local']['regions'][0]['adjustments']['exposure'] == 0.5
    # A mask refresh started before Undo must not attach itself afterward.
    page.evaluate("""() => {
        const original = safeFetch;
        window.safeFetch = function(url, options, config) {
            if (url.includes('/local-mask/snapshot')) {
                return new Promise(resolve => { window.resolveMask = resolve; });
            }
            return original(url, options, config);
        };
        window.maskUpdate = updateLocalMask();
    }""")
    assert page.evaluate('doUndo()') is True
    page.evaluate("""async () => {
        resolveMask({mask: {ref: 'late-mask', source_digest: 'late-digest'}});
        await maskUpdate;
    }""")
    assert page.evaluate('recipeForSave(editorState.recipe)') == {}
    assert page.evaluate('doRedo()') is True
    assert page.evaluate('recipeForSave(editorState.recipe)') == adjusted


def test_webview_history_events_use_working_history(page, editor_photo):
    set_range(page, 'exposure', 1)
    page.locator('#editorCropBox').evaluate("""el => el.dispatchEvent(new InputEvent(
        'beforeinput', {inputType: 'historyUndo', bubbles: true, cancelable: true}
    ))""")
    expect(page.locator('#exposureRange')).to_have_value('0')
    page.locator('#editorCropBox').evaluate("""el => el.dispatchEvent(new InputEvent(
        'beforeinput', {inputType: 'historyRedo', bubbles: true, cancelable: true}
    ))""")
    expect(page.locator('#exposureRange')).to_have_value('1')


@pytest.mark.parametrize('status_after_edits', [False, True])
def test_delayed_mask_staleness_survives_keyboard_undo_redo(page, editor_photo, status_after_edits):
    saved = {'local': {
        'mask': {'ref': 'saved-mask', 'source_digest': 'saved-digest'},
        'regions': [{'region': 'subject', 'adjustments': {'exposure': 0.5}}],
    }}
    page.route(f'**/api/photos/{editor_photo}', lambda route: route.fulfill(json={
        'id': editor_photo, 'filename': 'undo-photo.jpg', 'width': 600, 'height': 400,
        'edit_recipe': saved,
    }))
    held = []
    page.route(f'**/api/photos/{editor_photo}/edit-recipe', lambda route: held.append(route))
    page.evaluate('(id) => loadPhoto(id)', editor_photo)
    page.wait_for_function('!editorState.loading')
    page.wait_for_timeout(50)
    assert len(held) == 1

    def finish_status():
        held.pop().fulfill(json={'recipe': saved, 'local_mask_stale': True})
        expect(page.locator('#localStaleBanner')).to_be_visible()

    if not status_after_edits:
        finish_status()
    page.locator('#exposureRange').focus()
    page.keyboard.press('ArrowRight')
    page.keyboard.press('ArrowRight')
    page.keyboard.press('Meta+z')
    expect(page.locator('#exposureRange')).to_have_value('0.1')
    if status_after_edits:
        finish_status()  # Both undo and redo snapshots already exist.
    page.keyboard.press('Meta+z')
    expect(page.locator('#exposureRange')).to_have_value('0')
    expect(page.locator('#localStaleBanner')).to_be_visible()
    for value in ('0.1', '0.2'):
        page.keyboard.press('Meta+Shift+z')
        expect(page.locator('#exposureRange')).to_have_value(value)
        expect(page.locator('#localStaleBanner')).to_be_visible()


@pytest.mark.parametrize('end_event', ['pointerup', 'pointercancel', 'lostpointercapture'])
def test_slider_gesture_with_no_net_change_preserves_redo(page, editor_photo, end_event):
    set_range(page, 'contrast', 10)
    set_range(page, 'exposure', 1)
    assert page.evaluate('doUndo()') is True
    slider = page.locator('#contrastRange')
    slider.dispatch_event('pointerdown', {'pointerId': 5, 'button': 0})
    set_range(page, 'contrast', 15)
    set_range(page, 'contrast', 10)
    slider.dispatch_event(end_event, {'pointerId': 5})
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    assert page.evaluate('doRedo()') is True
    expect(page.locator('#exposureRange')).to_have_value('1')
    expect(slider).to_have_value('10')
    assert page.evaluate('doUndo()') is True
    assert page.evaluate('doUndo()') is True
    expect(slider).to_have_value('0')


def test_slider_gesture_with_no_net_change_keeps_oldest_undo(page, editor_photo, tmp_path):
    # Exercise the full history limit without launching 100 image renders.
    page.route(f'**/photos/{editor_photo}/edit-preview?*', lambda route: route.fulfill(
        path=str(tmp_path / 'undo-photo.jpg'),
    ))
    page.evaluate("() => { for (let value = 1; value <= 100; value++) setAdjustment('contrast', value); }")
    slider = page.locator('#contrastRange')
    slider.dispatch_event('pointerdown', {'pointerId': 5, 'button': 0})
    set_range(page, 'contrast', 90)
    set_range(page, 'contrast', 100)
    slider.dispatch_event('pointerup', {'pointerId': 5})
    assert page.evaluate("""async () => {
        for (let step = 0; step < 100; step++) {
            if (!await doUndo()) return false;
        }
        return true;
    }""") is True
    expect(slider).to_have_value('0')
