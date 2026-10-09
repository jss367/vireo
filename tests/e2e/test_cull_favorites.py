"""Favorites-first culling: scope, existing picks, comparison and explicit rejection."""
import json

from playwright.sync_api import expect


def _results():
    photos = []
    encounters = []
    for _index, (species, date, count) in enumerate([
        ('Great blue heron', '2024-03-10', 9),
        ('Great blue heron', '2024-03-11', 3),
        ('American robin', '2024-03-10', 3),
    ]):
        ids = []
        for number in range(count):
            photo_id = len(photos) + 1
            ids.append(photo_id)
            photos.append({
                'id': photo_id, 'filename': f'{species}-{photo_id}.jpg',
                'timestamp': f'{date}T08:00:{number:02}',
                'confirmed_species': species, 'species_top5': [[species, 0.99]],
                'label': 'KEEP', 'quality_composite': 0.99 - number / 100,
                # Four existing picks must survive both the AI rejection
                # label and the three-suggestion limit.
                'flag': 'flagged' if photo_id in [1, 2, 3, 4] else 'none',
            })
        encounters.append({
            'photo_ids': ids, 'species': [species], 'burst_count': 2,
            'bursts': [{'photo_ids': ids[:2]}, {'photo_ids': ids[2:]}],
            'time_range': [f'{date}T08:00:00', f'{date}T08:00:09'],
        })
    for p in photos[:4]:
        p['label'] = 'REJECT'
    photos[8]['label'] = 'REJECT'
    return {'photos': photos, 'encounters': encounters, 'summary': {}}


def _open(page, live_server, results=None, requests=None):
    results = results or _results()
    page.route('**/api/pipeline/page-init', lambda route: route.fulfill(json={'results': results}))

    def recompute(route):
        body = route.request.post_data_json
        if requests is not None:
            requests.append(body)
        ids = body.get('photo_ids', [p['id'] for p in results['photos']])
        scoped = json.loads(json.dumps(results))
        scoped['photos'] = [p for p in scoped['photos'] if p['id'] in ids]
        for encounter in scoped['encounters']:
            encounter['photo_ids'] = [i for i in encounter['photo_ids'] if i in ids]
        route.fulfill(json=scoped)

    page.route('**/api/pipeline/regroup-live', recompute)
    page.route('**/api/pipeline/reflow', recompute)
    page.goto(live_server['url'] + '/cull')
    expect(page.locator('.cull-card')).to_have_count(len(results['photos']))


def _select(page, name):
    page.locator('#cullSpeciesSummary').click()
    page.locator('#cullSpeciesOptions label').filter(has_text=name).locator('input').check()
    page.locator('#cullSpeciesSummary').click()
    expect(page.locator('#cullStatus')).to_contain_text('Analysis complete')


def _card(page, photo_id):
    return page.locator(f'.cull-card[data-photo-id="{photo_id}"]')


def test_favorites_include_all_existing_picks_and_only_three_suggestions_per_day(live_server, page):
    _open(page, live_server)
    heron = page.locator('.species-section').filter(has_text='Great blue heron')
    expect(heron.locator('.cull-favorite-card')).to_have_count(10)  # 4 picks + 3 per day
    expect(heron.locator('.cull-favorite-card').filter(has_text='Picked by you')).to_have_count(4)
    expect(heron.locator('.cull-favorite-card').filter(has_text='Suggested')).to_have_count(6)
    expect(_card(page, 8)).to_have_class('cull-card review')
    expect(_card(page, 9)).to_have_class('cull-card review')  # AI REJECT stays undecided
    expect(page.locator('#cullReviewRejected')).to_be_disabled()
    expect(_card(page, 1)).to_contain_text('Picked by you')


def test_species_dates_scope_analysis_and_apply_without_touching_other_photos(live_server, page):
    requests = []
    _open(page, live_server, requests=requests)
    _select(page, 'Great blue heron')
    page.locator('#cullDateFrom').fill('2024-03-11')
    page.locator('#cullDateTo').fill('2024-03-11')
    expect(page.locator('.cull-card')).to_have_count(3)
    expect(page.locator('#applyBtn')).to_be_enabled()
    assert requests[-1]['photo_ids'] == [10, 11, 12]
    assert requests[-1]['save_cache'] is False
    assert 'collection_id' not in requests[-1]
    posted = []
    page.route('**/api/culling/apply', lambda route: (posted.append(route.request.post_data_json), route.fulfill(json={'ok': True})))
    _card(page, 10).get_by_role('button', name='Reject', exact=True).click()
    page.on('dialog', lambda dialog: dialog.accept())
    page.locator('#applyBtn').click()
    expect(page.locator('#cullStatus')).to_contain_text('Applied!')
    assert posted == [{'keepers': [11, 12], 'rejects': [10], 'unflag': []}]
    page.get_by_role('button', name='Clear filters', exact=True).click()
    expect(page.locator('.cull-card')).to_have_count(15)
    expect(_card(page, 1)).to_contain_text('Picked by you')


def test_multi_species_selection_keeps_favorites_separate_and_can_expand_again(live_server, page):
    requests = []
    _open(page, live_server, requests=requests)
    _select(page, 'Great blue heron')
    expect(page.locator('.species-section')).to_have_count(1)
    _select(page, 'American robin')
    expect(page.locator('.species-section')).to_have_count(2)
    assert set(requests[-1]['photo_ids']) == set(range(1, 16))
    page.get_by_role('button', name='Clear filters', exact=True).click()
    expect(page.locator('.species-section')).to_have_count(2)
    assert 'photo_ids' not in requests[-1]


def test_pinning_a_favorite_survives_recompute_and_filter_changes(live_server, page):
    _open(page, live_server)
    _card(page, 9).get_by_role('button', name='Add favorite', exact=True).click()
    expect(_card(page, 9)).to_contain_text('Picked by you')
    page.get_by_role('button', name='Analyze for Culling', exact=True).click()
    expect(page.locator('#cullStatus')).to_contain_text('1 pinned decision kept')
    expect(_card(page, 9)).to_contain_text('Picked by you')
    _select(page, 'American robin')
    expect(_card(page, 9)).to_have_count(0)
    page.get_by_role('button', name='Clear filters', exact=True).click()
    expect(_card(page, 9)).to_contain_text('Picked by you')
    assert page.evaluate('cullManualDecisions[9]') == 'keep'


def test_comparison_preserves_reference_navigation_and_links_zoom(live_server, page):
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    _open(page, live_server)
    _card(page, 2).get_by_role('button', name='Select reference:', exact=False).click()
    _card(page, 8).get_by_role('button', name='Compare:', exact=False).click()
    expect(page.locator('#browseCompareOverlay')).to_have_class('browse-compare-overlay active')
    assert not errors, errors
    expect(page.locator('#browseCompareImgA')).to_have_attribute('data-photo-id', '2')
    expect(page.locator('#browseCompareImgB')).to_have_attribute('data-photo-id', '8')
    page.locator('#browseCompareWrapA').dispatch_event('dblclick')
    expect(page.locator('#browseCompareZoomA')).to_have_text('200%')
    expect(page.locator('#browseCompareZoomB')).to_have_text('200%')
    page.keyboard.press('ArrowRight')
    expect(page.locator('#browseCompareImgA')).to_have_attribute('data-photo-id', '2')
    expect(page.locator('#browseCompareImgB')).to_have_attribute('data-photo-id', '9')
    page.get_by_role('button', name='Add to favorites', exact=True).click()
    expect(_card(page, 9)).to_contain_text('Picked by you')
    expect(page.locator('#browseCompareImgA')).to_have_attribute('data-photo-id', '2')
    expect(page.locator('#browseCompareImgB')).to_have_attribute('data-photo-id', '8')
    page.keyboard.press('Escape')
    expect(page.locator('#browseCompareOverlay')).to_have_class('browse-compare-overlay')


def test_apply_leaves_unreviewed_photos_unflagged(live_server, page):
    _open(page, live_server)
    posted = []
    page.route('**/api/culling/apply', lambda route: (posted.append(route.request.post_data_json), route.fulfill(json={'ok': True})))
    page.on('dialog', lambda dialog: dialog.accept())
    page.locator('#applyBtn').click()
    expect(page.locator('#cullStatus')).to_contain_text('Applied!')
    assert posted[0]['rejects'] == []
    assert 8 not in posted[0]['keepers'] and 9 not in posted[0]['keepers']


def test_rejected_review_counts_companions_and_retains_failed_deletions(live_server, page):
    _open(page, live_server)
    _card(page, 8).get_by_role('button', name='Reject', exact=True).click()
    _card(page, 9).get_by_role('button', name='Reject', exact=True).click()
    page.locator('#cullReviewRejected').click()
    expect(page.locator('#cullRejectedCount')).to_have_text('2 rejected photos in this selection')
    page.route('**/api/photos/companion-count', lambda route: route.fulfill(json={'count': 2}))
    page.locator('#cullDeleteRejected').click()
    expect(page.locator('#deleteModal')).to_have_class('modal-overlay open')
    expect(page.locator('#deleteCompanionLabel')).to_contain_text('2 companion files')
    assert page.evaluate('_deletePhotoIds') == [8, 9]
    # Exercise the callback with the same partial-failure result the existing
    # deletion job returns, without touching any real source files.
    page.evaluate('''() => { _deleteCallback({deleted: 1, failed_photo_ids: [9]}); hideDeleteModal(); }''')
    expect(_card(page, 8)).to_have_count(0)
    expect(_card(page, 9)).to_have_class('cull-card reject')
    assert 8 not in page.evaluate('cullSourceResults.photos.map(p => p.id)')


def test_missing_dates_and_empty_scope_do_not_fall_back_to_all_photos(live_server, page):
    results = _results()
    results['photos'][0]['timestamp'] = None
    requests = []
    _open(page, live_server, results, requests)
    page.locator('#cullDateFrom').fill('2024-03-11')
    expect(page.locator('#applyBtn')).to_be_enabled()
    assert requests[-1]['photo_ids'] == [10, 11, 12]
    page.locator('#cullDateFrom').fill('2025-01-01')
    expect(page.locator('.cull-card')).to_have_count(0)
    expect(page.locator('#applyBtn')).not_to_be_visible()
    assert len(requests) == 1
    page.get_by_role('button', name='Clear filters', exact=True).click()
    expect(_card(page, 1)).to_contain_text('Picked by you')


def test_favorites_prefer_different_encounters_before_adjacent_frames(live_server, page):
    results = _results()
    results['photos'] = results['photos'][4:8]
    for photo in results['photos']:
        photo['flag'] = 'none'
    results['encounters'] = [
        {'photo_ids': [5, 6], 'species': ['Great blue heron']},
        {'photo_ids': [7], 'species': ['Great blue heron']},
        {'photo_ids': [8], 'species': ['Great blue heron']},
    ]
    _open(page, live_server, results)
    expect(_card(page, 5)).to_contain_text('Suggested')
    expect(_card(page, 7)).to_contain_text('Suggested')
    expect(_card(page, 8)).to_contain_text('Suggested')
    expect(_card(page, 6)).to_contain_text('Undecided')


def test_lightbox_flag_changes_move_the_photo_into_favorites(live_server, page):
    _open(page, live_server)
    page.evaluate("document.dispatchEvent(new CustomEvent('lightbox:flagchanged', {detail: {photoId: 9, flag: 'flagged'}}))")
    expect(_card(page, 9)).to_contain_text('Picked by you')
    assert page.evaluate('cullSourceResults.photos.find(p => p.id === 9).flag') == 'flagged'


def test_late_analysis_cannot_replace_a_new_empty_selection(live_server, page):
    _open(page, live_server)
    pending = []
    page.route('**/api/pipeline/regroup-live', lambda route: pending.append(route))
    page.locator('#cullDateTo').fill('2024-03-10')
    page.wait_for_function('cullAnalysisPending === 1')
    page.locator('#cullDateFrom').fill('2025-01-01')
    expect(page.locator('.cull-card')).to_have_count(0)
    pending[0].fulfill(json=_results())
    page.wait_for_function('cullAnalysisPending === 0')
    expect(page.locator('.cull-card')).to_have_count(0)
    expect(page.locator('#applyBtn')).not_to_be_visible()


def test_failed_scoped_analysis_requires_a_successful_retry_before_apply(live_server, page):
    _open(page, live_server)
    page.route('**/api/pipeline/regroup-live', lambda route: route.fulfill(status=503, json={'error': 'Try again'}))
    page.locator('#cullDateFrom').fill('2024-03-11')
    expect(page.locator('#cullStatus')).to_contain_text('Try again')
    expect(page.locator('#applyBtn')).to_be_disabled()
    page.route('**/api/pipeline/regroup-live', lambda route: route.fulfill(json=_results()))
    page.get_by_role('button', name='Analyze for Culling', exact=True).click()
    expect(page.locator('#applyBtn')).to_be_enabled()
    expect(page.locator('.cull-card')).to_have_count(3)


def test_collection_and_date_filters_analyze_only_their_intersection(live_server, page):
    page.route('**/api/collections', lambda route: route.fulfill(json=[{'id': 123, 'name': 'Heron outing'}]))
    requests = []
    _open(page, live_server, requests=requests)
    results = _results()
    collection_ids = [1, 2, 9, 10]

    def scoped_collection(route):
        body = route.request.post_data_json
        requests.append(body)
        ids = body.get('photo_ids', collection_ids)
        response = json.loads(json.dumps(results))
        response['photos'] = [p for p in response['photos'] if p['id'] in ids]
        route.fulfill(json=response)

    page.route('**/api/pipeline/regroup-live', scoped_collection)
    page.locator('#cullDateFrom').fill('2024-03-11')
    expect(page.locator('#applyBtn')).to_be_enabled()
    page.locator('#cullCollection').select_option('123')
    expect(page.locator('.cull-card')).to_have_count(1)
    expect(page.locator('#applyBtn')).to_be_enabled()
    assert requests[-2]['collection_id'] == 123
    assert requests[-1]['photo_ids'] == [10]
    assert 'collection_id' not in requests[-1]


def test_date_can_be_chosen_before_the_first_analysis(live_server, page):
    page.route('**/api/pipeline/page-init', lambda route: route.fulfill(json={'results': None}))
    requests = []
    results = _results()

    def analyze(route):
        body = route.request.post_data_json
        requests.append(body)
        route.fulfill(json=results)

    page.route('**/api/pipeline/regroup-live', analyze)
    page.goto(live_server['url'] + '/cull')
    page.locator('#cullDateFrom').fill('2024-03-11')
    page.get_by_role('button', name='Analyze for Culling', exact=True).click()
    expect(page.locator('#applyBtn')).to_be_enabled()
    expect(page.locator('.cull-card')).to_have_count(3)
    assert len(requests) == 2
    assert 'photo_ids' not in requests[0]
    assert requests[1]['photo_ids'] == [10, 11, 12]
