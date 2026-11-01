"""Exercise real viewport observation with controllable, slow thumbnail bodies."""
import re
from pathlib import Path

import pytest
from playwright.sync_api import expect

QUEUE = Path(__file__).resolve().parents[2] / "vireo/static/vireo-thumbnail-queue.js"


def setup_queue(page, document_viewport=False):
    if document_viewport:
        page.set_viewport_size({"width": 300, "height": 200})
    page.set_content('''
        <style>
          body { margin: 0; }
          #gridContainer { height: 200px; width: 300px; overflow: auto; }
          img { display: block; width: 280px; height: 100px; }
        </style>
        <div id="ROOT"><div id="grid"></div></div>
    '''.replace("ROOT", "duplicateThumbnailGrid" if document_viewport else "gridContainer"))
    page.evaluate('''documentViewport => {
      window.scrollQueue = top => documentViewport
        ? window.scrollTo(0, top) : (gridContainer.scrollTop = top);
    }''', document_viewport)
    page.evaluate('''() => {
      grid.innerHTML = Array.from({length: 30}, (_, i) =>
        `<img id="photo${i}" data-thumbnail-src="/thumbnails/${i}.jpg">`).join('');
      window.requests = [];
      window.revoked = [];
      const revoke = URL.revokeObjectURL.bind(URL);
      URL.revokeObjectURL = url => { revoked.push(url); revoke(url); };
      window.fetch = (url, opts) => new Promise((resolve, reject) => {
        const request = {url, aborted: false, done: false};
        requests.push(request);
        opts.signal.addEventListener('abort', () => {
          request.aborted = true;
          reject(new DOMException('Aborted', 'AbortError'));
        });
        request.finish = (status = 200) => {
          request.done = true;
          resolve(new Response(new Blob([
            '<svg xmlns="http://www.w3.org/2000/svg" width="2" height="2"/>'
          ], {type: 'image/svg+xml'}), {status}));
        };
      });
    }''')
    page.add_script_tag(path=str(QUEUE))
    page.wait_for_function("requests.length === 2")


@pytest.mark.parametrize("document_viewport", [False, True], ids=["browse", "duplicates"])
def test_scroll_cancels_old_requests_and_prioritizes_visible_photos(page, document_viewport):
    setup_queue(page, document_viewport)
    assert page.evaluate("requests.map(r => r.url)") == [
        "/thumbnails/0.jpg", "/thumbnails/1.jpg",
    ]
    page.evaluate("scrollQueue(2000)")
    page.wait_for_function("requests.length === 4")
    assert page.evaluate("requests.slice(0, 2).every(r => r.aborted)")
    assert page.evaluate("requests.slice(2).map(r => r.url)") == [
        "/thumbnails/20.jpg", "/thumbnails/21.jpg",
    ]
    # Intermediate queued thumbnails never get a turn after we leave them.
    assert not page.evaluate("requests.some(r => /\\/(2|3|4)\\.jpg$/.test(r.url))")
    page.evaluate("requests.slice(2).forEach(r => r.finish())")
    page.wait_for_function("document.querySelectorAll('img[src^=\"blob:\"]').length === 2")
    assert page.evaluate("requests.filter(r => !r.done && !r.aborted).length") <= 2
    page.evaluate("scrollQueue(0)")
    page.wait_for_function("requests.filter(r => r.url === '/thumbnails/0.jpg').length === 2")


@pytest.mark.parametrize("document_viewport", [False, True], ids=["browse", "duplicates"])
def test_replacement_aborts_downloads_and_releases_decoded_images(page, document_viewport):
    setup_queue(page, document_viewport)
    page.evaluate("requests[0].finish()")
    page.wait_for_function("photo0.complete && photo0.naturalWidth > 0")
    old_url = page.locator('#photo0').get_attribute('src')
    page.evaluate("grid.innerHTML = '<img id=next data-thumbnail-src=/thumbnails/99.jpg>'")
    page.wait_for_function("requests.some(r => r.url === '/thumbnails/99.jpg')")
    assert old_url in page.evaluate("revoked")
    assert page.evaluate("requests.filter(r => r.url !== '/thumbnails/99.jpg').every(r => r.done || r.aborted)")
    page.evaluate("requests.find(r => r.url === '/thumbnails/99.jpg').finish()")
    page.wait_for_function("next.complete && next.naturalWidth > 0")


def test_edit_invalidates_loaded_and_offscreen_thumbnails(page):
    setup_queue(page)
    page.evaluate("requests[0].finish()")
    page.wait_for_function("photo0.complete && photo0.naturalWidth > 0")
    old_url = page.locator('#photo0').get_attribute('src')
    page.evaluate('''() => {
      photo0.setAttribute('data-thumbnail-src', '/thumbnails/0.jpg?editv=new');
      photo25.setAttribute('data-thumbnail-src', '/thumbnails/25.jpg?source=raw');
      requests[1].finish();
    }''')
    page.wait_for_function("requests.some(r => r.url === '/thumbnails/0.jpg?editv=new')")
    assert old_url in page.evaluate("revoked")
    assert not page.evaluate("requests.some(r => r.url.includes('25.jpg'))")
    page.evaluate("gridContainer.scrollTop = 2500")
    page.wait_for_function("requests.some(r => r.url === '/thumbnails/25.jpg?source=raw')")


def test_http_failure_does_not_spin_and_retries_on_return(page):
    setup_queue(page)
    page.evaluate("requests[0].finish(404)")
    page.wait_for_function("requests.length === 3")
    assert page.evaluate("requests.filter(r => r.url === '/thumbnails/0.jpg').length") == 1
    page.evaluate("gridContainer.scrollTop = 2500")
    page.wait_for_function("requests[1].aborted")
    page.evaluate("gridContainer.scrollTop = 0")
    page.wait_for_function("requests.filter(r => r.url === '/thumbnails/0.jpg').length === 2")


def test_late_body_from_previous_edit_cannot_replace_new_pixels(page):
    setup_queue(page)
    # Let headers arrive while body consumption stays pending, including after
    # cancellation. This exercises the stale-result guard independently of the
    # browser transport's usual AbortError behavior.
    page.evaluate('''() => {
      window.oldBody = null;
      window.oldSignal = null;
      window.fetch = (url, opts) => {
        oldSignal = opts.signal;
        return Promise.resolve({ok: true, blob: () => new Promise(resolve => {
          oldBody = resolve;
        })});
      };
      requests[0].finish();
    }''')
    page.wait_for_function("oldBody !== null")
    page.evaluate('''() => {
      window.lateImage = Array.from(document.querySelectorAll('img')).find(img =>
        img.getAttribute('data-thumbnail-src') === '/thumbnails/2.jpg');
      lateImage.setAttribute('data-thumbnail-src', '/thumbnails/2.jpg?editv=new');
    }''')
    page.wait_for_function("oldSignal.aborted")
    page.evaluate('''() => {
      window.fetch = () => new Promise(() => {});
      oldBody(new Blob(['old pixels'], {type: 'image/jpeg'}));
    }''')
    page.wait_for_function("lateImage.getAttribute('data-thumbnail-src').includes('editv=new')")
    # Flush promise continuations and the next queued download.
    page.evaluate("() => new Promise(resolve => requestAnimationFrame(resolve))")
    assert page.locator('#photo2').get_attribute('src') is None


def test_browse_reject_works_while_thumbnail_downloads_are_pending(live_server, page):
    pending = []
    page.route('**/thumbnails/*', lambda route: pending.append(route))
    page.goto(f"{live_server['url']}/browse", wait_until='domcontentloaded')
    cards = page.locator('#grid > .grid-card')
    expect(cards).to_have_count(5)
    page.wait_for_function("document.querySelectorAll('#grid img[data-thumbnail-src]').length === 5")
    cards.nth(0).click(modifiers=['ControlOrMeta'])
    cards.nth(1).click(modifiers=['ControlOrMeta'])
    with page.expect_response(lambda response: '/api/batch/flag' in response.url) as saved:
        page.keyboard.press('x')
    assert saved.value.ok
    expect(page.locator('#grid .flag-rejected')).to_have_count(2)
    assert pending  # No thumbnail response was needed to apply the action.
    # Edits must refresh the canonical URL even before a thumbnail has a src.
    photo_id = int(cards.nth(0).get_attribute('data-id'))
    page.evaluate('''id => {
      _vireoBumpRenderVersion(id);
      vireoRefreshPhotoRenders([id]);
    }''', photo_id)
    expect(cards.nth(0).locator('img')).to_have_attribute(
        'data-thumbnail-src', re.compile(r'\?.+')
    )
    for route in pending:
        route.abort()


def test_browse_displays_cached_thumbnails_and_refreshes_after_edits(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    image = page.locator('#grid > .grid-card img').first
    expect(image).to_have_attribute('src', re.compile(r'^blob:'))
    page.wait_for_function("document.querySelector('#grid > .grid-card img').naturalWidth > 0")
    photo_id = int(page.locator('#grid > .grid-card').first.get_attribute('data-id'))
    previous_url = image.get_attribute('src')
    page.evaluate("""id => {
      _lbEditVersionByPhoto[String(id)] = 'queue-test';
      _lbRefreshThumbnailCache(id, 'queue-test');
    }""", photo_id)
    expect(image).to_have_attribute('data-thumbnail-src', re.compile(r'editv=queue-test'))
    page.wait_for_function('''previous => {
      const img = document.querySelector('#grid > .grid-card img');
      return img.src !== previous && img.complete && img.naturalWidth > 0;
    }''', arg=previous_url)
