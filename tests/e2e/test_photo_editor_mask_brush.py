"""Manual mask corrections persist and remain undoable across editor workflows."""

import io
import json

import numpy as np
import pytest
from PIL import Image
from playwright.sync_api import expect

from e2e import test_photo_editor_show_mask

masked_photo = test_photo_editor_show_mask.masked_photo


def open_mask(page, live_server, photo):
    page.goto(f"{live_server['url']}/edit/{photo}")
    page.wait_for_function('!editorState.loading && document.getElementById("editorImg").naturalWidth > 0')
    expect(page.locator('#localBand')).to_be_visible()


def stroke(page, mode, x, y):
    page.locator('#maskBrushAdd' if mode == 'add' else '#maskBrushSubtract').click()
    page.wait_for_function('maskBrush.mode && !maskBrush.busy')
    box = page.locator('#editorImg').bounding_box()
    with page.expect_response('**/local-mask/correct') as response:
        page.mouse.click(box['x'] + x * box['width'], box['y'] + y * box['height'])
    assert response.value.ok
    page.wait_for_function('!maskBrush.busy')
    return page.evaluate('editorState.recipe.local.mask.ref')


def test_brush_save_reload_undo_and_local_render(page, live_server, masked_photo):
    open_mask(page, live_server, masked_photo)
    added = stroke(page, 'add', 0.8, 0.5)
    assert page.evaluate('editorState.recipe.local.regions') == []
    assert page.evaluate('doUndo()') is True
    assert page.evaluate('editorState.recipe.local || null') is None
    assert page.evaluate('doRedo()') is True
    assert page.evaluate('editorState.recipe.local.mask.ref') == added
    page.wait_for_function('editorImageMatchesZoomRecipe(document.getElementById("editorImg"))')
    removed = stroke(page, 'subtract', 0.2, 0.5)
    assert removed != added
    assert page.evaluate('saveRecipe()') is True
    page.reload()
    page.wait_for_function('!editorState.loading && !!editorState.recipe.local')
    assert page.evaluate('editorState.recipe.local.mask.ref') == removed
    expect(page.locator('#saveBtn')).to_be_disabled()
    with page.expect_response('**/edit-mask-preview?*') as response:
        page.locator('#maskOverlayBtn').click()
    alpha = np.asarray(Image.open(io.BytesIO(response.value.body())))[..., 3]
    assert alpha[64, 205] > 60  # added background now belongs to subject
    assert alpha[64, 51] == 0  # removed foreground no longer belongs to subject
    page.evaluate("setLocalAdjustment('subject', 'exposure', 1, true)")
    assert page.evaluate('editorState.recipe.local.mask.ref') == removed
    assert page.evaluate('saveRecipe()') is True
    saved = live_server['db'].get_photo_edit_recipe(masked_photo)
    assert saved['local']['mask']['corrected'] is True
    # Exact saved recipe drives the same preview after reopening.
    endpoint = f"{live_server['url']}/photos/{masked_photo}/edit-preview?size=256&recipe="
    from urllib.parse import quote
    preview = page.request.get(endpoint + quote(json.dumps(saved)))
    assert preview.ok
    pixels = np.asarray(Image.open(io.BytesIO(preview.body())))
    assert pixels[64, 205].mean() > pixels[64, 51].mean() + 15
    page.evaluate('editorState.localStale = true; updateLocalBandVisibility()')
    expect(page.locator('#localStaleBanner')).to_contain_text('clears painted corrections')
    expect(page.locator('#localStaleBanner button')).to_have_text('Replace Mask')
    page.evaluate('updateLocalMask()')
    assert page.evaluate('!!editorState.recipe.local.mask.corrected') is False
    assert page.evaluate('doUndo()') is True
    assert page.evaluate('editorState.recipe.local.mask.ref') == removed


@pytest.mark.parametrize('rotation', [0, 90, 180, 270])
@pytest.mark.parametrize('flip', [False, True])
def test_brush_inverse_geometry(page, live_server, masked_photo, rotation, flip):
    open_mask(page, live_server, masked_photo)
    # Forward-transform a known source point, then invert it through the UI.
    point = page.evaluate('''({rotation, flip}) => {
      const recipe = {rotation, flip: {horizontal: flip}, straighten: 17,
        crop: {x: 0.1, y: 0.1, w: 0.8, h: 0.8}};
      let x=0.3, y=0.6, w=256, h=128;
      if(rotation===90) [x,y]=[1-y,x];
      if(rotation===180) [x,y]=[1-x,1-y];
      if(rotation===270) [x,y]=[y,1-x];
      if(rotation===90 || rotation===270) [w,h]=[h,w];
      if(flip) x=1-x;
      const a=17*Math.PI/180, px=(x-0.5)*w, py=(y-0.5)*h;
      x=(Math.cos(a)*px-Math.sin(a)*py)/w+0.5;
      y=(Math.sin(a)*px+Math.cos(a)*py)/h+0.5;
      return maskBrushSourcePoint((x-0.1)/0.8,(y-0.1)/0.8,recipe,true,{width:256,height:128});
    }''', {'rotation': rotation, 'flip': flip})
    assert point == pytest.approx([0.3, 0.6])


def test_brush_failure_and_cancel_preserve_recipe(page, live_server, masked_photo):
    open_mask(page, live_server, masked_photo)
    page.route('**/local-mask/correct', lambda route: route.fulfill(status=400, json={'error': 'Mask unavailable'}))
    page.locator('#maskBrushAdd').click()
    page.wait_for_function('maskBrush.mode && !maskBrush.busy')
    box = page.locator('#editorImg').bounding_box()
    page.mouse.click(box['x'] + box['width'] * 0.8, box['y'] + box['height'] * 0.5)
    expect(page.locator('#maskBrushStatus')).to_contain_text('Mask unavailable')
    assert page.evaluate('editorState.recipe.local || null') is None
    page.keyboard.press('Escape')
    assert page.evaluate('maskBrush.mode') is None


def test_reset_discards_a_late_brush_response(page, live_server, masked_photo):
    open_mask(page, live_server, masked_photo)
    held = []
    page.route('**/local-mask/correct', lambda route: held.append(route))
    page.locator('#maskBrushAdd').click()
    page.wait_for_function('maskBrush.mode && !maskBrush.busy')
    box = page.locator('#editorImg').bounding_box()
    page.mouse.click(box['x'] + box['width'] * 0.8, box['y'] + box['height'] * 0.5)
    page.wait_for_function('maskBrush.busy')
    assert page.evaluate('saveRecipe()') is False
    page.evaluate('resetAllEdits()')
    assert len(held) == 1
    response = held[0].fetch()
    assert response.ok
    held[0].fulfill(response=response)
    page.wait_for_timeout(100)
    assert page.evaluate('editorState.recipe.local || null') is None
    assert page.evaluate('maskBrush.busy') is False


def test_painted_mask_follows_rotated_cropped_preview(page, live_server, masked_photo):
    open_mask(page, live_server, masked_photo)
    page.evaluate('''() => {
      editorState.recipe = {rotation: 90, flip: {horizontal: true}, straighten: 12,
        crop: {x: 0.1, y: 0.1, w: 0.8, h: 0.8}};
      editorState.cropEditing = false;
      syncControls(); updatePreview();
    }''')
    page.wait_for_function('editorImageMatchesZoomRecipe(document.getElementById("editorImg"))')
    # The original left half maps to the upper half after rotation + flip.
    # Paint the lower half and check its rendered overlay, not just helper math.
    stroke(page, 'add', 0.5, 0.8)
    page.wait_for_function('document.getElementById("maskOverlayImg").complete')
    overlay = page.locator('#maskOverlayImg').get_attribute('src')
    result = page.request.get(live_server['url'] + overlay)
    assert result.ok
    alpha = np.asarray(Image.open(io.BytesIO(result.body())))[..., 3]
    assert alpha[round(alpha.shape[0] * 0.8), alpha.shape[1] // 2] > 60


def test_point_color_picker_exits_mask_brush_without_painting(page, live_server, masked_photo):
    open_mask(page, live_server, masked_photo)
    page.locator('#maskBrushAdd').click()
    page.wait_for_function('maskBrush.mode && !maskBrush.busy')
    corrections = []
    page.on('request', lambda request: corrections.append(request.url)
            if '/local-mask/correct' in request.url else None)
    page.locator('#pointColorPick').click()
    expect(page.locator('#pointColorStatus')).to_contain_text('Click a color')
    assert page.evaluate('maskBrush.mode') is None
    box = page.locator('#editorImg').bounding_box()
    page.mouse.click(box['x'] + box['width'] * 0.8, box['y'] + box['height'] * 0.5)
    page.wait_for_function('pointColorSamples().length === 1')
    assert corrections == []
    assert page.evaluate('editorState.recipe.local || null') is None
    # Selecting the brush again exits the picker in the other direction too.
    page.locator('#maskBrushAdd').click()
    page.wait_for_function('maskBrush.mode && !maskBrush.busy')
    assert page.evaluate('colorEditor.picking') is False

def test_brush_reacquires_mask_after_feather_only_edit(page, live_server, masked_photo):
    open_mask(page, live_server, masked_photo)
    page.locator('#maskBrushAdd').click()
    page.wait_for_function('maskBrush.mode && !maskBrush.busy')
    page.locator('#featherRange').evaluate("el => {el.value='10'; el.dispatchEvent(new Event('input', {bubbles:true}));}")
    page.wait_for_function('editorImageMatchesZoomRecipe(document.getElementById("editorImg")) && !editorPreviewQueue.active')
    assert page.evaluate('editorState.localMask') is None
    box = page.locator('#editorImg').bounding_box()
    with page.expect_response('**/local-mask/correct') as response:
        page.mouse.click(box['x'] + box['width'] * 0.8, box['y'] + box['height'] * 0.5)
    assert response.value.ok
    page.wait_for_function('!maskBrush.busy')
    assert page.evaluate('editorState.recipe.local.mask.corrected') is True
    assert page.evaluate('editorState.recipe.local.mask.feather') == 10
    assert page.evaluate('saveRecipe()') is True


def test_cancel_during_brush_mask_reacquisition_never_paints(page, live_server, masked_photo):
    open_mask(page, live_server, masked_photo)
    page.locator('#maskBrushAdd').click()
    page.wait_for_function('maskBrush.mode && !maskBrush.busy')
    page.evaluate('setLocalFeather(10)')
    page.wait_for_function('!editorPreviewQueue.active')
    held = []
    corrections = []
    page.route('**/local-mask/snapshot', lambda route: held.append(route))
    page.on('request', lambda request: corrections.append(request.url)
            if '/local-mask/correct' in request.url else None)
    box = page.locator('#editorImg').bounding_box()
    page.mouse.click(box['x'] + box['width'] * 0.8, box['y'] + box['height'] * 0.5)
    page.wait_for_function('maskBrush.busy')
    page.wait_for_timeout(50)
    assert len(held) == 1
    page.keyboard.press('Escape')
    held[0].fulfill(response=held[0].fetch())
    page.wait_for_timeout(100)
    assert corrections == []
    assert page.evaluate('editorState.recipe.local || null') is None
    assert page.evaluate('maskBrush.mode') is None


def set_brush_control(page, name, value):
    page.locator('#maskBrush' + name).evaluate(
        "(el, value) => {el.value=value; el.dispatchEvent(new Event('input', {bubbles:true}));}", str(value))


def test_soft_brush_controls_persist_and_alt_temporarily_subtracts(page, live_server, masked_photo):
    open_mask(page, live_server, masked_photo)
    set_brush_control(page, 'Size', 120)
    set_brush_control(page, 'Softness', 100)
    set_brush_control(page, 'Strength', 50)
    expect(page.locator('#maskBrushSoftnessValue')).to_have_text('100%')
    expect(page.locator('#maskBrushStrengthValue')).to_have_text('50%')
    requests = []
    page.on('request', lambda request: requests.append(request.post_data_json)
            if '/local-mask/correct' in request.url else None)
    added = stroke(page, 'add', 0.8, 0.5)
    assert requests[-1]['softness'] == 1
    assert requests[-1]['strength'] == 0.5
    page.wait_for_function('editorImageMatchesZoomRecipe(document.getElementById("editorImg"))')
    box = page.locator('#editorImg').bounding_box()
    page.mouse.move(box['x'] + box['width'] * 0.2, box['y'] + box['height'] * 0.5)
    page.keyboard.down('Alt')
    expect(page.locator('#maskBrushCursor')).to_have_class('mask-brush-cursor subtract')
    with page.expect_response('**/local-mask/correct') as response:
        page.mouse.down()
        # Parameters and mode belong to the entire stroke, including its end.
        page.keyboard.up('Alt')
        set_brush_control(page, 'Strength', 100)
        page.mouse.up()
    assert response.value.ok
    page.wait_for_function('!maskBrush.busy')
    assert requests[-1]['mode'] == 'subtract'
    assert requests[-1]['strength'] == 0.5
    expect(page.locator('#maskBrushAdd')).to_have_attribute('aria-pressed', 'true')
    expect(page.locator('#maskBrushCursor')).not_to_have_class('mask-brush-cursor subtract')
    corrected = page.evaluate('editorState.recipe.local.mask.ref')
    assert page.evaluate('doUndo()') is True
    assert page.evaluate('editorState.recipe.local.mask.ref') == added
    assert page.evaluate('doRedo()') is True
    assert page.evaluate('saveRecipe()') is True
    page.reload()
    page.wait_for_function('!editorState.loading && !!editorState.recipe.local')
    assert page.evaluate('editorState.recipe.local.mask.ref') == corrected
    with page.expect_response('**/edit-mask-preview?*') as response:
        page.locator('#maskOverlayBtn').click()
    alpha = np.asarray(Image.open(io.BytesIO(response.value.body())))[..., 3]
    assert alpha[64, 205] == pytest.approx(alpha[64, 0] / 2, abs=2)
    assert alpha[64, 51] == pytest.approx(alpha[64, 0] / 2, abs=2)
    assert 0 < alpha[64, 216] < alpha[64, 205]  # soft edge persisted


def test_brush_cursor_shortcuts_and_focus_boundaries(page, live_server, masked_photo):
    open_mask(page, live_server, masked_photo)
    page.locator('#maskBrushAdd').click()
    page.wait_for_function('maskBrush.mode && !maskBrush.busy')
    box = page.locator('#editorImg').bounding_box()
    page.mouse.move(box['x'] + box['width'] * 0.8, box['y'] + box['height'] * 0.5)
    cursor = page.locator('#maskBrushCursor')
    expect(cursor).to_be_visible()
    assert cursor.bounding_box()['width'] == pytest.approx(40)
    page.keyboard.press(']')
    expect(page.locator('#maskBrushSizeValue')).to_have_text('44')
    assert cursor.bounding_box()['width'] == pytest.approx(44)
    page.keyboard.press('[')
    expect(page.locator('#maskBrushSize')).to_have_value('40')
    set_brush_control(page, 'Size', 120)
    page.keyboard.press(']')
    expect(page.locator('#maskBrushSize')).to_have_value('120')
    set_brush_control(page, 'Size', 8)
    page.keyboard.press('[')
    expect(page.locator('#maskBrushSize')).to_have_value('8')
    page.locator('#editorSearchInput').focus()
    page.keyboard.press(']')
    expect(page.locator('#editorSearchInput')).to_have_value(']')
    expect(page.locator('#maskBrushSize')).to_have_value('8')
    page.locator('#editorSearchInput').fill('')
    page.locator('#editorSearchInput').blur()
    page.evaluate('setEditorSpacePan(true)')
    expect(cursor).to_be_hidden()
    page.evaluate('setEditorSpacePan(false)')
    expect(cursor).to_be_visible()
    page.keyboard.down('Alt')
    page.evaluate("window.dispatchEvent(new Event('blur'))")
    expect(cursor).to_be_hidden()
    assert page.evaluate('maskBrush.erase') is False
    page.keyboard.up('Alt')
    page.mouse.move(box['x'] + box['width'] * 0.7, box['y'] + box['height'] * 0.5)
    expect(cursor).to_be_visible()
    page.keyboard.press('Escape')
    expect(cursor).to_be_hidden()
    assert page.evaluate('maskBrush.mode') is None


def test_brush_outline_tracks_radius_limit_and_zoom(page, live_server, masked_photo):
    open_mask(page, live_server, masked_photo)
    page.locator('#maskBrushAdd').click()
    page.wait_for_function('maskBrush.mode && !maskBrush.busy')
    set_brush_control(page, 'Size', 120)
    page.evaluate('setEditorZoomToActual()')
    page.wait_for_function('editorImageMatchesZoomRecipe(document.getElementById("editorImg"))')
    box = page.locator('#editorImg').bounding_box()
    page.mouse.move(box['x'] + box['width'] * 0.7, box['y'] + box['height'] * 0.5)
    cursor = page.locator('#maskBrushCursor')
    expect(cursor).to_be_visible()
    # The server caps radius at a quarter of the native short side (128px).
    assert cursor.bounding_box()['width'] == pytest.approx(64)
    page.evaluate('setEditorZoom(200)')
    page.wait_for_function('editorImageMatchesZoomRecipe(document.getElementById("editorImg"))')
    box = page.locator('#editorImg').bounding_box()
    page.mouse.move(box['x'] + box['width'] * 0.7, box['y'] + box['height'] * 0.5)
    assert cursor.bounding_box()['width'] == pytest.approx(120)
    with page.expect_request('**/local-mask/correct') as request:
        page.mouse.click(box['x'] + box['width'] * 0.7, box['y'] + box['height'] * 0.5)
    assert request.value.post_data_json['radius'] * 128 * 2 * 2 == pytest.approx(120)
    page.wait_for_function('!maskBrush.busy')
