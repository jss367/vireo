"""Quick adjustments are independent of slider and save history."""

import io
import json
import re
import time
from urllib.parse import parse_qs, urlparse

import pytest
from PIL import Image
from playwright.sync_api import expect


@pytest.fixture
def adjustment_photo(live_server, tmp_path):
    folder = tmp_path / 'adjustment-photos'
    folder.mkdir()
    image = Image.new('RGB', (256, 64))
    image.putdata([(x, x // 2, 255 - x) for _y in range(64) for x in range(256)])
    path = folder / 'gradient.png'
    image.save(path)
    db = live_server['db']
    folder_id = db.add_folder(str(folder))
    return db.add_photo(
        folder_id=folder_id, filename=path.name, extension='.png',
        file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
        width=256, height=64,
    )


def _open(page, live_server, photo_id):
    page.goto(live_server['url'] + '/browse')
    page.evaluate("id => openLightbox(id, 'gradient.png')", photo_id)
    page.wait_for_function("""() => {
      const img = document.getElementById('lightboxImg');
      return img.complete && img.naturalWidth === 256 && _lbEditRecipeLoaded;
    }""")
    page.locator('#lightboxAdjustBtn').click()
    expect(page.locator('#lbAdjExposure')).to_be_enabled()


def _set_exposure(page, value):
    page.locator('#lbAdjExposure').evaluate("""(el, value) => {
      el.value = String(value);
      el.dispatchEvent(new Event('input', {bubbles: true}));
    }""", value)


def _wait_saved(page):
    expect(page.locator('#lightboxAdjustStatus')).to_have_text('Saved')
    page.wait_for_function("""() => {
      const img = document.getElementById('lightboxImg');
      return img.complete && !document.getElementById('lightboxToneCanvas').classList.contains('show')
        && !document.getElementById('lightboxAdjustmentImage')?.classList.contains('show');
    }""")


@pytest.mark.parametrize('saved_exposure', [0, -1])
def test_exposure_preview_is_identical_after_saved_round_trip(
    live_server, page, adjustment_photo, saved_exposure,
):
    if saved_exposure:
        live_server['db'].set_photo_edit_recipe(
            adjustment_photo, {'adjustments': {'exposure': saved_exposure}},
        )
    _open(page, live_server, adjustment_photo)
    page.evaluate("""() => {
      window.previewFrames = [];
      const render = VireoToneGL.render;
      VireoToneGL.render = function(img, uniforms) {
        const ok = render(img, uniforms);
        if (ok) {
          const canvas = document.getElementById('lightboxToneCanvas');
          const gl = canvas.getContext('webgl');
          const pixels = new Uint8Array(canvas.width * 4);
          gl.readPixels(0, 0, canvas.width, 1, gl.RGBA, gl.UNSIGNED_BYTE, pixels);
          previewFrames.push({pixels: Array.from(pixels), exposure: uniforms.exposure});
        }
        return ok;
      };
    }""")
    _set_exposure(page, 5)
    page.wait_for_function('previewFrames.length === 1')
    first = page.evaluate('previewFrames[0]')
    _wait_saved(page)
    _set_exposure(page, -2)
    page.wait_for_function('previewFrames.length === 2')
    _wait_saved(page)
    _set_exposure(page, 5)
    page.wait_for_function('previewFrames.length === 3')
    last = page.evaluate('previewFrames[2]')
    assert last['pixels'] == first['pixels']
    assert first['exposure'] == last['exposure'] == 5
    assert len(set(first['pixels'])) > 10, 'must render the gradient, not a blank canvas'
    _wait_saved(page)
    assert live_server['db'].get_photo_edit_recipe(adjustment_photo)['adjustments']['exposure'] == 5


def test_save_response_does_not_replace_newer_slider_input(live_server, page, adjustment_photo):
    _open(page, live_server, adjustment_photo)
    held = []

    def hold_first_save(route):
        if route.request.method == 'PUT' and not held:
            held.append((route, route.fetch()))
        else:
            route.continue_()

    page.route(f'**/api/photos/{adjustment_photo}/edit-recipe', hold_first_save)
    _set_exposure(page, 1)
    expect(page.locator('#lightboxAdjustStatus')).to_have_text('Saving...')
    # Pump browser events until the committed response is held in transit.
    deadline = time.monotonic() + 5
    while not held and time.monotonic() < deadline:
        page.wait_for_timeout(25)
    assert held
    _set_exposure(page, 5)
    route, response = held[0]
    route.fulfill(response=response)
    _wait_saved(page)
    expect(page.locator('#lbAdjExposure')).to_have_value('5')
    assert live_server['db'].get_photo_edit_recipe(adjustment_photo)['adjustments']['exposure'] == 5


@pytest.mark.parametrize('advanced', [False, True])
def test_server_preview_uses_complete_recipe_and_preserves_geometry(
    live_server, page, adjustment_photo, advanced,
):
    recipe = {'rotation': 90, 'crop': {'x': 0.1, 'y': 0.1, 'w': 0.8, 'h': 0.8}}
    if advanced:
        recipe['adjustments'] = {'tone_curve': {'midtones': 60}}
    live_server['db'].set_photo_edit_recipe(adjustment_photo, recipe)
    page.goto(live_server['url'] + '/browse')
    page.evaluate("id => openLightbox(id, 'gradient.png')", adjustment_photo)
    page.wait_for_function('_lbEditRecipeLoaded')
    if not advanced:
        page.evaluate('VireoToneGL.supported = () => false')
    page.locator('#lightboxAdjustBtn').click()
    with page.expect_response('**/edit-preview?*') as preview:
        _set_exposure(page, 2)
    assert preview.value.ok
    query = parse_qs(urlparse(preview.value.url).query)
    rendered = json.loads(query['recipe'][0])
    assert rendered['rotation'] == 90
    assert rendered['crop'] == recipe['crop']
    assert rendered['adjustments']['exposure'] == 2
    if advanced:
        assert rendered['adjustments']['tone_curve'] == {'midtones': 60}
    assert query['apply_crop'] == ['1']
    _wait_saved(page)


@pytest.mark.parametrize('server_preview', [False, True])
def test_late_preview_cannot_reappear_after_closing_lightbox(
    live_server, page, adjustment_photo, server_preview,
):
    if server_preview:
        page.add_init_script("""(() => {
          const getContext = HTMLCanvasElement.prototype.getContext;
          HTMLCanvasElement.prototype.getContext = function(kind, ...args) {
            if (kind === 'webgl' || kind === 'experimental-webgl') return null;
            return getContext.call(this, kind, ...args);
          };
        })();""")
    held = []
    page.route('**/edit-preview?*', lambda route: held.append(route))
    _open(page, live_server, adjustment_photo)
    _set_exposure(page, 2)
    deadline = time.monotonic() + 5
    while not held and time.monotonic() < deadline:
        page.wait_for_timeout(25)
    assert held
    page.evaluate('closeLightbox()')
    with page.expect_response('**/edit-preview?*'):
        held[0].continue_()
    # Let the decoded image's onload and promise callbacks run.
    page.wait_for_timeout(100)
    expect(page.locator('#lightboxToneCanvas')).not_to_have_class('lb-tone-canvas show')
    expect(page.locator('#lightboxAdjustmentImage')).not_to_have_class('lb-tone-canvas show')


@pytest.fixture
def paired_adjustment_photo(live_server, page, adjustment_photo):
    db = live_server['db']
    db.conn.execute(
        "UPDATE photos SET filename='gradient.nef', extension='.nef', companion_path='gradient.jpg' WHERE id=?",
        (adjustment_photo,),
    )
    db.conn.commit()
    raw = io.BytesIO()
    jpeg = io.BytesIO()
    Image.new('RGB', (256, 64), 'red').save(raw, 'PNG')
    Image.new('RGB', (64, 256), 'green').save(jpeg, 'PNG')
    page.route(
        re.compile(rf'/(thumbnails/{adjustment_photo}\.jpg|photos/{adjustment_photo}/(full|original|preview))'),
        lambda route: route.fulfill(
            body=raw.getvalue() if 'source=raw' in route.request.url else jpeg.getvalue(),
            content_type='image/png',
        ),
    )
    return adjustment_photo, raw.getvalue(), jpeg.getvalue()


@pytest.mark.parametrize('server_preview', [False, True])
def test_paired_jpeg_disables_adjustments_and_discards_pending_raw_preview(
    live_server, page, paired_adjustment_photo, server_preview,
):
    adjustment_photo, raw, _jpeg = paired_adjustment_photo
    db = live_server['db']
    held_previews = []
    page.route('**/edit-preview?*', lambda route: held_previews.append(route))
    page.goto(live_server['url'] + '/browse')
    if server_preview:
        page.evaluate('VireoToneGL.supported = () => false')
    page.evaluate("id => openLightbox(id, 'gradient.nef')", adjustment_photo)
    page.wait_for_function('_lbEditRecipeLoaded')
    source = page.locator('#lightboxSourceControl')
    adjust = page.locator('#lightboxAdjustBtn')
    exposure = page.locator('#lbAdjExposure')
    expect(source).to_have_text('Viewing JPEG · Show RAW')
    expect(adjust).to_be_disabled()
    expect(adjust).to_have_attribute('title', 'Switch to RAW to use quick adjustments')
    expect(exposure).to_be_disabled()
    assert page.evaluate("""() => [
      toggleLightboxAdjustPanel(),
      onLightboxAdjustmentInput(document.getElementById('lbAdjExposure')),
      resetLightboxAdjustments(),
      _lbSaveAdjustmentRecipe({exposure: 5})
    ]""") == [False] * 4
    assert not held_previews
    assert db.get_photo_edit_recipe(adjustment_photo) is None

    source.click()
    expect(source).to_have_text('Viewing RAW · Show JPEG')
    expect(adjust).to_be_enabled()
    adjust.click()
    expect(exposure).to_be_enabled()
    _set_exposure(page, 2)
    deadline = time.monotonic() + 5
    while not held_previews and time.monotonic() < deadline:
        page.wait_for_timeout(25)
    assert held_previews
    # Switching must flush the pending RAW edit, close its panel, and cancel
    # both neutral-source callbacks and complete server preview callbacks.
    source.click()
    expect(source).to_have_text('Viewing JPEG · Show RAW')
    expect(adjust).to_be_disabled()
    expect(page.locator('#lightboxAdjustPanel')).not_to_have_class('lightbox-adjust-panel open')
    with page.expect_response('**/edit-preview?*'):
        held_previews[0].fulfill(body=raw, content_type='image/png')
    page.wait_for_timeout(100)
    expect(page.locator('#lightboxToneCanvas')).not_to_have_class('lb-tone-canvas show')
    expect(page.locator('#lightboxAdjustmentImage')).not_to_have_class('lb-tone-canvas show')
    _wait_saved(page)
    assert page.locator('#lightboxImg').evaluate('img => [img.naturalWidth, img.naturalHeight]') == [64, 256]
    assert db.get_photo_edit_recipe(adjustment_photo)['adjustments']['exposure'] == 2


def test_failed_jpeg_switch_reloads_raw_edit_saved_during_switch(
    live_server, page, paired_adjustment_photo,
):
    photo_id, raw, _jpeg = paired_adjustment_photo
    page.route('**/edit-preview?*', lambda route: route.fulfill(body=raw, content_type='image/png'))
    page.goto(live_server['url'] + '/browse')
    page.evaluate("id => openLightbox(id, 'gradient.nef')", photo_id)
    page.wait_for_function('_lbEditRecipeLoaded')
    source = page.locator('#lightboxSourceControl')
    expect(source).to_have_text('Viewing JPEG · Show RAW')
    source.click()
    expect(source).to_have_text('Viewing RAW · Show JPEG')
    old_url = page.locator('#lightboxImg').get_attribute('src')

    held_jpeg = []
    page.route('**/photos/*/full?*source=jpeg*', lambda route: held_jpeg.append(route))
    page.locator('#lightboxAdjustBtn').click()
    # Start the source switch in the same turn as input, before its debounced
    # save starts, and hold JPEG until that save has finished.
    page.evaluate("""() => {
      const input = document.getElementById('lbAdjExposure');
      input.value = '2';
      onLightboxAdjustmentInput(input);
      vireoTogglePairSource(_lightboxCurrentId);
    }""")
    expect(source).to_contain_text('Loading JPEG')
    _wait_saved(page)
    deadline = time.monotonic() + 5
    while not held_jpeg and time.monotonic() < deadline:
        page.wait_for_timeout(25)
    assert held_jpeg
    assert live_server['db'].get_photo_edit_recipe(photo_id)['adjustments']['exposure'] == 2
    expected_url = page.evaluate("""oldUrl => vireoRenderedUrl(
      _vireoBaseRenderedUrl(oldUrl), _lightboxCurrentId
    )""", old_url)
    assert expected_url != old_url
    with page.expect_request(lambda request: request.url.endswith(expected_url)):
        held_jpeg[0].abort()
    expect(source).to_have_text('Viewing RAW · Show JPEG')
    expect(page.locator('#lightboxImg')).to_have_attribute('src', expected_url)
    expect(page.locator('#lightboxAdjustBtn')).to_be_enabled()
