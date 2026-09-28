"""A hidden window stops polling; showing it again catches up at once.

Left open but hidden overnight, the navbar used to keep polling, and two of
those polls start real work: an expired new-images answer re-walks every
library folder and the automatic missing-originals check stats every photo.
"""

from urllib.parse import urlparse

from playwright.sync_api import expect

# Polls that must go quiet while hidden.
_BACKGROUND_POLLS = {
    '/api/workspaces/active/new-images',
    '/api/photos/missing',
    '/api/photos/missing/check',
    '/api/folders/missing',
    '/api/duplicates/disk-cleanup-summary',
    '/api/jobs',
    '/api/workspaces/active/local-folders/blocker',
}

# Everything that came due while hidden and must run as soon as it is shown.
_OVERDUE_ON_SHOW = {
    '/api/workspaces/active/new-images',
    '/api/photos/missing/check',
    '/api/folders/missing',
    '/api/duplicates/disk-cleanup-summary',
    '/api/jobs',
}

# Headless Chromium is always visible; drive document.hidden ourselves.
_FAKE_VISIBILITY = """
  window.__vireoHidden = false;
  Object.defineProperty(Document.prototype, 'hidden', {
    configurable: true, get() { return window.__vireoHidden; }});
  Object.defineProperty(Document.prototype, 'visibilityState', {
    configurable: true, get() { return window.__vireoHidden ? 'hidden' : 'visible'; }});
"""


def _is_live(job):
    return job['status'] in ('running', 'pausing', 'paused', 'queued', 'pending')


def _set_hidden(page, hidden):
    page.evaluate("""hidden => {
      window.__vireoHidden = hidden;
      document.dispatchEvent(new Event('visibilitychange'));
    }""", hidden)


def test_hidden_window_stops_background_polls_and_catches_up_when_shown(live_server, page):
    page.add_init_script(_FAKE_VISIBILITY)
    page.clock.install()
    page.goto(live_server['url'] + '/browse')
    expect(page.locator('.grid-card').first).to_be_visible()
    # Let the page-load new-images check settle, then take job-poll ticks
    # until the navbar has seen no live job: a live job keeps the job poll
    # running while hidden, for the dock progress.
    page.wait_for_function('() => !_newImagesInFlight && _newImagesPendingTimer === null')
    for _ in range(20):
        with page.expect_response('**/api/jobs') as response:
            page.clock.fast_forward(15000)
        if not any(_is_live(job) for job in response.value.json()['active']):
            break
    else:
        raise AssertionError('a job stayed live')
    page.wait_for_timeout(300)

    requested = []
    page.on('request', lambda request: requested.append(urlparse(request.url).path))
    _set_hidden(page, True)
    # Two hours hidden, in half-hour steps so chained timers get their turn.
    for _ in range(4):
        page.clock.fast_forward('30:00')
        page.wait_for_timeout(200)
    assert not [path for path in requested if path in _BACKGROUND_POLLS], requested

    _set_hidden(page, False)
    page.clock.fast_forward(1000)
    for _ in range(50):
        if set(requested) >= _OVERDUE_ON_SHOW:
            break
        page.wait_for_timeout(100)
    assert set(requested) >= _OVERDUE_ON_SHOW, sorted(_OVERDUE_ON_SHOW - set(requested))
