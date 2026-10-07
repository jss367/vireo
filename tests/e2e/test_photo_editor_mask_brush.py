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
