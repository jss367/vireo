"""Progressive preview latency, request bounds, and obsolete-response protection."""

import io
import json
import math
import statistics
from urllib.parse import parse_qs, urlparse

import pytest
from PIL import Image
from playwright.sync_api import expect


@pytest.fixture
def preview_photo(live_server, tmp_path):
    folder = tmp_path / 'preview-photos'
    folder.mkdir()
    path = folder / 'large-photo.jpg'
    Image.new('RGB', (3072, 2048), (100, 140, 160)).save(path)
    db = live_server['db']
    folder_id = db.add_folder(str(folder))
    return db.add_photo(folder_id=folder_id, filename=path.name, extension='.jpg',
                        file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
                        width=3072, height=2048)


def open_photo(page, live_server, photo_id):
    page.goto(f"{live_server['url']}/edit/{photo_id}")
    page.wait_for_function('!editorState.loading && document.getElementById("editorImg").naturalWidth > 0')
    page.evaluate('editorState.previewTimings = []')


def query(url):
    return parse_qs(urlparse(url).query)


def fill(route):
    buffer = io.BytesIO()
    Image.new('RGB', (120, 80), (90, 120, 150)).save(buffer, format='JPEG')
    route.fulfill(status=200, content_type='image/jpeg', body=buffer.getvalue())


def test_quick_preview_then_native_refinement(page, live_server, preview_photo, record_property):
    open_photo(page, live_server, preview_photo)
    requests = []
    page.on('request', lambda request: requests.append(request.url) if '/edit-preview?' in request.url else None)
    page.evaluate("editorState.zoomMode='custom'; editorState.zoomPercent=100; setAdjustment('exposure', 0.8)")
    page.wait_for_function('editorState.previewTimings.some(t => t.interactive)')
    page.wait_for_function('editorState.previewTimings.some(t => !t.interactive && t.requestedSize === 3072)')
    sizes = [int(query(url)['size'][0]) for url in requests]
    assert sizes == [1024, 3072]
    assert page.locator('#editorImg').evaluate('img => img.naturalWidth') == 3072
    expect(page.locator('#previewStatus')).to_contain_text('3072×2048 render')
    timings = page.evaluate('editorState.previewTimings')
    record_property('preview_latency_samples', json.dumps(timings))
    # A broad integration ceiling catches a stalled queue; release benchmarks
    # report percentiles separately rather than treating shared-host noise as speed.
    assert all(0 <= t['elapsedMs'] < 15000 for t in timings)
    # Repeated warm edits produce a useful latency report for CI artifacts.
    for exposure in [0.2, 0.4, 0.6, 1.0]:
        previous = page.evaluate('editorState.previewTimings.filter(t => !t.interactive).length')
        page.evaluate("value => setAdjustment('exposure', value)", exposure)
        page.wait_for_function('previous => editorState.previewTimings.filter(t => !t.interactive).length > previous', arg=previous)
    samples = page.evaluate('editorState.previewTimings')
    report = {}
    for label, interactive in [('quick', True), ('refined', False)]:
        values = sorted(t['elapsedMs'] for t in samples if t['interactive'] == interactive)
        assert len(values) == 5
        report[label] = {'samples': len(values), 'p50_ms': round(statistics.median(values), 1),
                         'p95_ms': round(values[math.ceil(0.95 * len(values)) - 1], 1)}
    record_property('preview_latency_summary', json.dumps(report))


def test_continuous_input_delivers_intermediate_frames(page, live_server, preview_photo):
    open_photo(page, live_server, preview_photo)
    page.route('**/edit-preview?*', fill)
    page.evaluate('''() => new Promise(resolve => {
      let n=0;
      const timer=setInterval(() => {
        setAdjustment('exposure', ++n/10);
        if(n===12) {clearInterval(timer); resolve();}
      }, 100);
    })''')
    assert page.evaluate('editorState.previewTimings.filter(t => t.interactive).length') >= 2
    page.wait_for_function('editorState.previewTimings.some(t => !t.interactive)')
    assert json.loads(query(page.locator('#editorImg').get_attribute('src'))['recipe'][0])['adjustments']['exposure'] == 1.2


def test_slow_request_is_superseded_and_never_displays_old_recipe(page, live_server, preview_photo):
    open_photo(page, live_server, preview_photo)
    original = page.locator('#editorImg').get_attribute('src')
    held = []
    page.route('**/edit-preview?*', lambda route: held.append(route))
    page.evaluate("setAdjustment('exposure', 0.5)")
    page.wait_for_function('editorPreviewQueue.active !== null')
    page.wait_for_timeout(100)
    assert len(held) == 1
    page.evaluate("for (let i=1;i<=20;i++) setAdjustment('exposure', i/10)")
    page.wait_for_timeout(400)
    assert len(held) == 2  # obsolete request detached; only latest input dispatched
    fill(held.pop(0))
    page.wait_for_timeout(100)
    assert page.locator('#editorImg').get_attribute('src') == original
    assert len(held) == 1
    assert json.loads(query(held[0].request.url)['recipe'][0])['adjustments']['exposure'] == 2
    fill(held.pop(0))
    page.wait_for_timeout(100)
    assert len(held) == 1
    fill(held.pop(0))
    page.wait_for_function('editorPreviewQueue.active === null')
    assert json.loads(query(page.locator('#editorImg').get_attribute('src'))['recipe'][0])['adjustments']['exposure'] == 2


def test_failed_render_keeps_last_image_and_queue_recovers(page, live_server, preview_photo):
    open_photo(page, live_server, preview_photo)
    original = page.locator('#editorImg').get_attribute('src')
    page.route('**/edit-preview?*', lambda route: route.fulfill(status=500, body='render failed'))
    page.evaluate("setAdjustment('exposure', 0.5)")
    expect(page.locator('#previewStatus')).to_contain_text('Could not render preview')
    assert page.locator('#editorImg').get_attribute('src') == original
    page.unroute('**/edit-preview?*')
    page.evaluate("setAdjustment('exposure', 1)")
    page.wait_for_function('editorImageMatchesZoomRecipe(document.getElementById("editorImg"))')


@pytest.mark.parametrize('latest_exposure', [0.5, 1.5])
def test_stalled_preview_releases_slot_for_latest_edit(page, live_server, preview_photo, latest_exposure):
    open_photo(page, live_server, preview_photo)
    original = page.locator('#editorImg').get_attribute('src')
    held = []
    page.route('**/edit-preview?*', lambda route: held.append(route))
    page.evaluate("EDITOR_PREVIEW_TIMEOUT = 1200; setAdjustment('exposure', 0.5)")
    page.wait_for_function('editorPreviewQueue.active !== null')
    page.evaluate('() => { window.obsoletePreviewCallback = editorPreviewQueue.active.image.onload; }')
    if latest_exposure != 0.5:
        page.evaluate("value => setAdjustment('exposure', value)", latest_exposure)
    page.wait_for_function('''value => editorPreviewQueue.active && editorPreviewQueue.active.size > 1024 &&
      JSON.parse(new URL(editorPreviewQueue.active.url, location.href).searchParams.get('recipe'))
        .adjustments.exposure === value''', arg=latest_exposure)
    assert len(held) == (2 if latest_exposure == 0.5 else 3)
    assert page.locator('#editorImg').get_attribute('src') == original
    expect(page.locator('#previewStatus')).to_contain_text('Refining preview')
    page.evaluate('window.obsoletePreviewCallback()')
    fill(held.pop())
    page.wait_for_function('editorPreviewQueue.active === null')
    assert json.loads(query(page.locator('#editorImg').get_attribute('src'))['recipe'][0])['adjustments']['exposure'] == latest_exposure
    expect(page.locator('#previewRetryBtn')).to_be_hidden()
    for route in held:
        route.abort()


def test_stalled_preview_offers_manual_retry_without_retry_loop(page, live_server, preview_photo):
    open_photo(page, live_server, preview_photo)
    original = page.locator('#editorImg').get_attribute('src')
    held = []
    page.route('**/edit-preview?*', lambda route: held.append(route))
    page.evaluate("EDITOR_PREVIEW_TIMEOUT = 500; editorState.recipe.adjustments = {exposure: 1}; updatePreview()")
    expect(page.locator('#previewStatus')).to_contain_text('Preview timed out')
    expect(page.locator('#previewRetryBtn')).to_be_visible()
    assert page.locator('#editorImg').get_attribute('src') == original
    page.wait_for_timeout(700)
    assert len(held) == 1
    held.pop().abort()
    page.unroute('**/edit-preview?*')
    page.route('**/edit-preview?*', fill)
    page.locator('#previewRetryBtn').click()
    page.wait_for_function('editorImageMatchesZoomRecipe(document.getElementById("editorImg"))')
    expect(page.locator('#previewRetryBtn')).to_be_hidden()


def test_reusing_detailed_preview_invalidates_slow_quick_render(page, live_server, preview_photo):
    open_photo(page, live_server, preview_photo)
    original = page.locator('#editorImg').get_attribute('src')
    original_width = page.locator('#editorImg').evaluate('img => img.naturalWidth')
    held = []
    page.route('**/edit-preview?*', lambda route: held.append(route))
    # A resize within the same render-size bucket schedules the same recipe.
    page.evaluate('schedulePreview()')
    page.wait_for_function('editorState.previewTimings.some(t => !t.interactive)')
    assert len(held) == 1
    assert int(query(held[0].request.url)['size'][0]) == 1024
    assert page.locator('#editorImg').get_attribute('src') == original
    fill(held.pop())
    page.wait_for_function('editorPreviewQueue.active === null')
    assert page.locator('#editorImg').get_attribute('src') == original
    assert page.locator('#editorImg').evaluate('img => img.naturalWidth') == original_width
    expect(page.locator('#previewStatus')).not_to_contain_text('Quick preview')


def test_photo_switch_invalidates_pending_render(page, live_server, preview_photo):
    open_photo(page, live_server, preview_photo)
    held = []
    page.route(f'**/photos/{preview_photo}/edit-preview?*', lambda route: held.append(route))
    page.evaluate("setAdjustment('exposure', 0.5)")
    page.wait_for_timeout(200)
    assert len(held) == 1
    other = live_server['data']['photos'][0]
    page.route(f'**/photos/{other}/edit-preview?*', fill)
    page.evaluate('(id) => { loadPhoto(id); }', other)
    page.wait_for_function('(id) => editorState.photoId === id && !editorState.loading', arg=other)
    # The new photo must display without waiting for the obsolete request.
    page.wait_for_function('''id => {
      const img = document.getElementById('editorImg');
      return img.getAttribute('src').startsWith('/photos/' + id + '/edit-preview?') &&
        img.complete && img.naturalWidth > 0;
    }''', arg=other, timeout=5000)
    fill(held.pop(0))
    page.wait_for_timeout(100)
    displayed = page.locator('#editorImg').get_attribute('src')
    assert json.loads(query(displayed)['recipe'][0]).get('adjustments') is None
    assert page.evaluate('editorState.recipe.adjustments || null') is None


def test_hung_render_does_not_block_the_next_photo_preview(page, live_server, preview_photo):
    # A slow or hung render on the previous photo must not keep its pixels on
    # screen while the metadata describes a newly loaded photo: cancelling
    # the preview must release the active request so the new photo's render
    # can start without waiting for the stalled one to resolve.
    open_photo(page, live_server, preview_photo)
    held = []
    page.route(f'**/photos/{preview_photo}/edit-preview?*', lambda route: held.append(route))
    page.evaluate("setAdjustment('exposure', 0.5)")
    page.wait_for_function('editorPreviewQueue.active !== null')
    assert len(held) == 1
    other = live_server['data']['photos'][0]
    page.route(f'**/photos/{other}/edit-preview?*', fill)
    page.evaluate('(id) => { loadPhoto(id); }', other)
    page.wait_for_function('(id) => editorState.photoId === id && !editorState.loading', arg=other)
    # Without unblocking the previous photo's held request, the new photo's
    # preview still displays — matching its loaded photo id and clean recipe.
    page.wait_for_function(
        '(id) => {'
        '  var img = document.getElementById("editorImg");'
        '  if (!img || !img.getAttribute("src")) return false;'
        '  var m = img.getAttribute("src").match(/\\/photos\\/(\\d+)\\/edit-preview/);'
        '  return !!m && Number(m[1]) === id && img.complete && img.naturalWidth > 0;'
        '}',
        arg=other,
    )
    displayed = page.locator('#editorImg').get_attribute('src')
    assert json.loads(query(displayed)['recipe'][0]).get('adjustments') is None
    held.pop().abort()


def test_slow_quick_preview_still_displays_before_queued_refinement(page, live_server, preview_photo):
    open_photo(page, live_server, preview_photo)
    held = []
    page.route('**/edit-preview?*', lambda route: held.append(route))
    page.evaluate("setAdjustment('exposure', 0.5)")
    page.wait_for_timeout(450)
    assert len(held) == 1
    assert int(query(held[0].request.url)['size'][0]) == 1024
    fill(held.pop(0))
    expect(page.locator('#previewStatus')).to_contain_text('Quick preview')
    assert int(query(page.locator('#editorImg').get_attribute('src'))['size'][0]) == 1024
    page.wait_for_timeout(100)
    assert len(held) == 1
    fill(held.pop(0))
    page.wait_for_function('editorPreviewQueue.active === null')
    assert int(query(page.locator('#editorImg').get_attribute('src'))['size'][0]) > 1024


def test_tabs_use_independent_sessions_and_navigation_fences_old_requests(page, live_server, preview_photo):
    open_photo(page, live_server, preview_photo)
    second = page.context.new_page()
    try:
        open_photo(second, live_server, preview_photo)
        first_session = query(page.locator('#editorImg').get_attribute('src'))['preview_session'][0]
        second_session = query(second.locator('#editorImg').get_attribute('src'))['preview_session'][0]
        assert first_session != second_session
        held = []
        cancelled = []
        page.route('**/edit-preview?*', lambda route: held.append(route))
        page.on('request', lambda request: cancelled.append(request.post_data_json)
                if request.url.endswith('/api/edit-preview/cancel') else None)
        page.evaluate("setAdjustment('exposure', 0.5)")
        page.wait_for_function('editorPreviewQueue.active !== null')
        page.wait_for_timeout(100)
        previous_sequence = int(query(held[0].request.url)['preview_seq'][0])
        other = live_server['data']['photos'][0]
        page.evaluate('(id) => { loadPhoto(id); }', other)
        page.wait_for_function('id => editorState.photoId === id && !editorState.loading', arg=other)
        assert cancelled[-1]['session'] == first_session
        assert cancelled[-1]['sequence'] > previous_sequence
        page.wait_for_timeout(100)
        current_sequence = int(query(held[-1].request.url)['preview_seq'][0])
        assert current_sequence > cancelled[-1]['sequence']
        # A real request in the other tab still completes on its own session.
        second.evaluate("setAdjustment('exposure', 0.7)")
        second.wait_for_function('editorImageMatchesZoomRecipe(document.getElementById("editorImg"))')
        for route in held:
            route.abort()
    finally:
        second.close()


@pytest.mark.parametrize('event', ['onload', 'onerror'])
def test_obsolete_preview_callback_cannot_release_new_photo_request(page, live_server, preview_photo, event):
    open_photo(page, live_server, preview_photo)
    held = []
    page.route('**/edit-preview?*', lambda route: held.append(route))
    page.evaluate("setAdjustment('exposure', 0.5)")
    page.wait_for_function('editorPreviewQueue.active !== null')
    page.evaluate('event => { window.obsoletePreviewCallback = editorPreviewQueue.active.image[event]; }', event)
    other = live_server['data']['photos'][0]
    page.evaluate('(id) => { loadPhoto(id); }', other)
    page.wait_for_function('''id => editorPreviewQueue.active &&
      editorPreviewQueue.active.url.startsWith('/photos/' + id + '/edit-preview?')''', arg=other)
    # Simulate an old event already queued when navigation cancelled its image.
    assert page.evaluate('''() => {
      const active = editorPreviewQueue.active;
      window.obsoletePreviewCallback();
      return editorPreviewQueue.active === active;
    }''')
    page.wait_for_timeout(100)
    assert len(held) == 2
    fill(held.pop())
    page.wait_for_function('editorPreviewQueue.active === null')
    assert page.locator('#editorImg').get_attribute('src').startswith(f'/photos/{other}/edit-preview?')
    fill(held.pop())
