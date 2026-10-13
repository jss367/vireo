"""The actual navbar buttons round-trip distinct kinds of saved changes."""

import pytest
from playwright.sync_api import expect


@pytest.mark.parametrize('action', ['flag', 'rating', 'color_label', 'keyword', 'location', 'batch-location', 'reject', 'reviewed', 'edit-recipe'])
def test_navbar_buttons_round_trip_saved_actions(live_server, page, action):
    db = live_server['db']
    pid = live_server['data']['photos'][0]
    pids = live_server['data']['photos'][:2]
    pred_id = db.conn.execute('SELECT p.id FROM predictions p JOIN detections d ON d.id=p.detection_id WHERE d.photo_id=?', (pid,)).fetchone()[0]

    def state():
        photo = db.get_photo(pid)
        return {
            'flag': photo['flag'], 'rating': photo['rating'],
            'color': db.photo_labels.get(pid),
            'keywords': [dict(row) for row in db.get_photo_keywords(pid)],
            'locations': [db.edit_history.photo_location_state(p) for p in pids],
            'prediction': db.get_prediction_status(pred_id),
            'recipe': db.get_photo_edit_recipe(pid),
        }

    url = f'/api/photos/{pid}/' + action
    method = 'POST'
    body = {}
    if action == 'flag':
        body = {'flag': 'rejected'}
    if action == 'rating':
        body = {'rating': 2}
    if action == 'color_label':
        body = {'color': 'red'}
    if action == 'keyword':
        url, body = f'/api/photos/{pid}/keywords', {'name': 'Undo test tag'}
    if action == 'location':
        url, body = f'/api/photos/{pid}/location/text', {'name': 'Undo test place'}
    if action == 'batch-location':
        url, body = '/api/batch/location/text', {'photo_ids': pids, 'name': 'Undo test place'}
    if action in ('reject', 'reviewed'):
        url = f'/api/predictions/{pred_id}/{action}'
    if action == 'edit-recipe':
        method, body = 'PUT', {'rotation': 90}
    before = state()
    page.goto(live_server['url'] + '/browse')
    expect(page.locator('#historyUndoBtn')).to_be_disabled()
    status = page.evaluate('''async ({url, method, body}) => {
      const response = await fetch(url, {method, headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)});
      return response.status;
    }''', {'url': url, 'method': method, 'body': body})
    assert status == 200
    after = state()
    assert after != before
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.locator('#historyUndoBtn').click()
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    assert state() == before
    page.locator('#historyRedoBtn').click()
    page.wait_for_function('!window.vireoHistoryBusy()')
    expect(page.locator('#historyRedoBtn')).to_be_disabled()
    assert state() == after


@pytest.mark.parametrize('path', ['/review', '/highlights', '/life-list', '/locations/review?mode=time'])
def test_history_buttons_work_across_pages(live_server, page, path):
    pid = live_server['data']['photos'][0]
    db = live_server['db']
    page.goto(live_server['url'] + path)
    page.evaluate('''async pid => {
      await fetch('/api/photos/' + pid + '/flag', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({flag: 'rejected'})});
    }''', pid)
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.locator('#historyUndoBtn').click()
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    assert db.get_photo(pid)['flag'] == 'none'
    page.locator('#historyRedoBtn').click()
    page.wait_for_function('!window.vireoHistoryBusy()')
    expect(page.locator('#historyRedoBtn')).to_be_disabled()
    assert db.get_photo(pid)['flag'] == 'rejected'


def test_location_review_rebuilds_queue_after_history_change(live_server, page):
    from e2e.test_location_review import LEAFLET_STUB
    page.add_init_script(LEAFLET_STUB)
    page.goto(live_server['url'] + '/locations/review?mode=time')
    page.wait_for_function('state.groups.length > 0')
    pids = live_server['data']['photos']
    page.evaluate('''async photoIds => {
      await fetch('/api/batch/location/text', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({photo_ids: photoIds, name: 'Test place'})});
    }''', pids)
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.locator('#historyUndoBtn').click()
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    assert page.evaluate('state.groups.flatMap(group => group.photo_ids).length') == len(pids)
    page.locator('#historyRedoBtn').click()
    page.wait_for_function('!window.vireoHistoryBusy()')
    expect(page.locator('#historyRedoBtn')).to_be_disabled()
    assert page.evaluate('state.groups.length') == 0
    expect(page.locator('#locationReviewMain')).to_be_hidden()
    expect(page.locator('#locationReviewEmpty')).to_be_visible()
    page.locator('#historyUndoBtn').click()
    page.wait_for_function('!window.vireoHistoryBusy()')
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    expect(page.locator('#locationReviewMain')).to_be_visible()
    expect(page.locator('#locationReviewEmpty')).to_be_hidden()
    expect(page.locator('#locationReviewSearch')).to_be_visible()
