"""Guard exact collection sources and failed scoped follow-up analysis."""
import json

from playwright.sync_api import expect

from e2e.test_cull_favorites import _open, _results


def test_filter_change_during_collection_load_restarts_collection_source(live_server, page):
    page.route('**/api/collections', lambda route: route.fulfill(json=[{'id': 123, 'name': 'New collection'}]))
    _open(page, live_server)
    held = []
    requests = []
    def analyze(route):
        body = route.request.post_data_json
        requests.append(body)
        if len(requests) == 1:
            held.append(route)
            return
        ids = body.get('photo_ids', [10])
        result = json.loads(json.dumps(_results()))
        result['photos'] = [p for p in result['photos'] if p['id'] in ids]
        for encounter in result['encounters']:
            encounter['photo_ids'] = [i for i in encounter['photo_ids'] if i in ids]
        route.fulfill(json=result)
    page.route('**/api/pipeline/regroup-live', analyze)
    page.locator('#cullCollection').select_option('123')
    page.wait_for_function('cullAnalysisPending === 1')
    page.locator('#cullDateFrom').fill('2024-03-11')
    page.wait_for_function('cullAnalysisPending === 1 && document.querySelectorAll(".cull-card").length > 0')
    held[0].fulfill(json=_results())
    page.wait_for_function('cullAnalysisPending === 0')
    assert requests[1].get('collection_id') == 123
    assert requests[-1]['photo_ids'] == [10]
    expect(page.locator('.cull-card')).to_have_count(1)
    posted = []
    page.route('**/api/culling/apply', lambda route: (posted.append(route.request.post_data_json), route.fulfill(json={'ok': True})))
    page.on('dialog', lambda dialog: dialog.accept())
    page.locator('#applyBtn').click()
    expect(page.locator('#cullStatus')).to_contain_text('Applied!')
    assert posted == [{'keepers': [10], 'rejects': [], 'unflag': []}]


def test_failed_initial_scoped_follow_up_blocks_apply(live_server, page):
    page.route('**/api/pipeline/page-init', lambda route: route.fulfill(json={'results': None}))
    requests = []
    def analyze(route):
        requests.append(route.request.post_data_json)
        if len(requests) == 1:
            route.fulfill(json=_results())
        else:
            route.fulfill(status=503, json={'error': 'Scoped analysis failed'})
    page.route('**/api/pipeline/regroup-live', analyze)
    page.goto(live_server['url'] + '/cull')
    page.locator('#cullDateFrom').fill('2024-03-11')
    page.get_by_role('button', name='Analyze for Culling', exact=True).click()
    expect(page.locator('#cullStatus')).to_contain_text('Scoped analysis failed')
    page.wait_for_function('cullAnalysisPending === 0')
    expect(page.locator('#applyBtn')).to_be_disabled()
