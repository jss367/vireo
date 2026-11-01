"""Every browser transport participates in persistent edit history."""

import re
import time

import pytest
from playwright.sync_api import expect


def _held_route(page, held):
    deadline = time.monotonic() + 5
    while not held and time.monotonic() < deadline:
        page.wait_for_timeout(50)
    assert len(held) == 1, 'expected exactly one held history request'
    return held.pop()


@pytest.mark.parametrize('transport', ['fetch', 'request', 'json'])
def test_pending_browser_writes_guard_history_and_refresh_after_save(live_server, page, transport):
    photo_id = live_server['data']['photos'][0]
    page.goto(live_server['url'] + '/browse')
    page.evaluate('''async photoId => {
      await fetch('/api/photos/' + photoId + '/flag', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({flag: 'flagged'})
      });
    }''', photo_id)
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    held = []
    page.route(f'**/api/photos/{photo_id}/flag', lambda route: held.append(route))
    page.evaluate('''({photoId, transport}) => {
      const url = '/api/photos/' + photoId + '/flag';
      const options = {method: 'POST', headers: {'Content-Type': 'application/json'},
                       body: JSON.stringify({flag: 'rejected'})};
      const write = transport === 'request' ? fetch(new Request(location.origin + url, options))
                  : transport === 'json' ? Vireo.api.json(url, options) : fetch(url, options);
      window.historyTestWrite = write.catch(error => ({error: error.message}));
    }''', {'photoId': photo_id, 'transport': transport})
    expect(page.locator('#historyUndoBtn')).to_be_disabled()
    page.evaluate('doUndo()')
    _held_route(page, held).continue_()
    page.evaluate('window.historyTestWrite')
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    assert live_server['db'].photos.get(photo_id)['flag'] == 'rejected'
    page.locator('#historyUndoBtn').click()
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    assert live_server['db'].photos.get(photo_id)['flag'] == 'flagged'


def test_failed_native_write_releases_history_guard(live_server, page):
    photo_id = live_server['data']['photos'][0]
    page.goto(live_server['url'] + '/browse')
    page.evaluate('''async photoId => {
      await fetch('/api/photos/' + photoId + '/flag', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({flag: 'rejected'})
      });
    }''', photo_id)
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.route(f'**/api/photos/{photo_id}/flag', lambda route: route.abort())
    page.evaluate('''async photoId => {
      try {
        await fetch('/api/photos/' + photoId + '/flag', {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({flag: 'flagged'})
        });
      } catch (error) {}
    }''', photo_id)
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.locator('#historyUndoBtn').click()
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    assert live_server['db'].photos.get(photo_id)['flag'] == 'none'


def test_first_lightbox_adjustment_enables_undo(live_server, page):
    page.goto(live_server['url'] + '/browse')
    expect(page.locator('#historyUndoBtn')).to_be_disabled()
    page.locator('.grid-card').first.dblclick()
    page.locator('#lightboxAdjustBtn').click()
    exposure = page.locator('#lbAdjExposure')
    expect(exposure).to_be_enabled()
    exposure.fill('1')
    exposure.dispatch_event('input')
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.evaluate('doUndo()')
    expect(exposure).to_have_value('0')
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    page.evaluate('doRedo()')
    expect(exposure).to_have_value('1')


def test_pending_cache_save_blocks_undo(live_server, page):
    photo_id = live_server['data']['photos'][0]
    page.goto(live_server['url'] + '/browse')
    page.evaluate('''async photoId => {
      await fetch('/api/photos/' + photoId + '/flag', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({flag: 'rejected'})
      });
    }''', photo_id)
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    held = []
    page.route('**/api/pipeline/save-cache', lambda route: held.append(route))
    page.evaluate('''() => {
      window.pendingCacheSave = fetch('/api/pipeline/save-cache', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}'
      }).catch(() => {});
    }''')
    expect(page.locator('#historyUndoBtn')).to_be_disabled()
    page.evaluate('doUndo()')
    assert live_server['db'].photos.get(photo_id)['flag'] == 'rejected'
    _held_route(page, held).abort()
    page.evaluate('window.pendingCacheSave')
    expect(page.locator('#historyUndoBtn')).to_be_enabled()


@pytest.mark.parametrize('notification', ['poll', 'event'])
def test_background_import_tag_refreshes_history_controls(live_server, page, notification):
    page.clock.install()
    page.goto(live_server['url'] + '/browse')
    expect(page.locator('.grid-card').first).to_be_visible()
    expect(page.locator('#historyUndoBtn')).to_be_disabled()
    # The import worker commits after its start request, outside the browser's
    # request tracker. No focus change or foreground edit should be necessary.
    db = live_server['db']
    photo_id = live_server['data']['photos'][0]
    keyword_id = db.add_keyword('Imported trip')
    db.tag_photo(photo_id, keyword_id)
    db.commit()
    description = 'Added Imported trip during import'
    db.record_edit('keyword_add', description, str(keyword_id), [
        {'photo_id': photo_id, 'old_value': '', 'new_value': str(keyword_id)},
    ])
    if notification == 'poll':
        with page.expect_response('**/api/jobs'):
            page.clock.fast_forward(15000)
    else:
        page.evaluate("window.dispatchEvent(new CustomEvent('vireo-job-done', {detail: {job_id: 'finished-import'}}))")
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    expect(page.locator('#historyUndoBtn')).to_have_attribute('title', re.compile(description))
    page.locator('#historyUndoBtn').click()
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
