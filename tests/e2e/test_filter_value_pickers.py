"""Pick library values without typing filter syntax or internal IDs."""
import json

import pytest
from playwright.sync_api import expect

from e2e.test_browse_filterbar import _open_browse, _wait_total


def _add_picker(page, field):
    page.click('.vf-filters-btn')
    page.click('.vf-add-filter')
    page.click(f'[data-add-field="{field}"]')
    picker = page.locator('.vf-value-picker')
    picker.locator('summary').click()
    return picker


@pytest.mark.parametrize('field', ['camera_make', 'camera_model', 'lens', 'species', 'keyword'])
def test_multi_value_picker_search_selection_and_exclusion(live_server, page, field):
    db = live_server['db']
    if field in ['species', 'keyword']:
        first, second = 'Red-tailed Hawk', 'American Robin'
        first_total, both_total, remaining = 1, 2, 3
    else:
        db.conn.execute(f"UPDATE photos SET {field} = CASE WHEN id=? THEN 'Alpha' ELSE 'Beta' END",
                        (live_server['data']['photos'][0],))
        db.conn.commit()
        first, second = 'Alpha', 'Beta'
        first_total, both_total, remaining = 1, 5, 0
    _open_browse(page, live_server)
    picker = _add_picker(page, field)
    expect(page.locator('[data-action="op"]')).to_have_value('in')
    picker.get_by_role('checkbox', name=first, exact=True).check()
    _wait_total(page, first_total)
    search = picker.get_by_role('searchbox')
    search.fill(second)
    picker.get_by_role('checkbox', name=second, exact=True).check()
    _wait_total(page, both_total)
    expect(picker.get_by_role('button', name='Remove ' + first, exact=True)).to_be_visible()
    page.locator('[data-action="op"]').select_option('not_in')
    _wait_total(page, remaining)
    picker.get_by_role('button', name='Remove ' + first, exact=True).click()
    page.locator('[data-action="op"]').select_option('is')
    expect(picker.get_by_role('radio', name=second, exact=True)).to_be_checked()
    page.locator('[data-action="op"]').select_option('contains')
    expect(page.locator('[data-action="value-input"]')).to_have_value(second)
    expect(picker).to_have_count(0)


def test_classifier_picker_uses_models_in_predictions(live_server, page):
    page.goto(live_server['url'] + '/review')
    page.wait_for_function('VireoFilter.isReady()')
    picker = _add_picker(page, 'classifier_model')
    picker.get_by_role('radio', name='BioCLIP-2', exact=True).check()
    assert page.evaluate('VireoFilter.getUserRules().rules[0]') == {
        'field': 'classifier_model', 'op': 'is', 'value': 'BioCLIP-2'}
    page.locator('[data-action="op"]').select_option('contains')
    expect(page.locator('[data-action="value-input"]')).to_have_value('BioCLIP-2')


def test_folder_picker_hierarchy_search_and_subtree(live_server, page):
    db = live_server['db']
    child = db.add_folder('/photos/park/nest', parent_id=live_server['data']['folders'][0], name='nest')
    db.add_photo(folder_id=child, filename='nest.jpg', extension='.jpg', file_size=1, file_mtime=1)
    _open_browse(page, live_server)
    picker = _add_picker(page, 'folder')
    expect(picker.get_by_role('radio', name='nest', exact=True)).to_be_visible()
    picker.get_by_role('button', name='Collapse park', exact=True).click()
    expect(picker.get_by_role('radio', name='nest', exact=True)).to_have_count(0)
    picker.get_by_role('searchbox').fill('nest')
    expect(picker.get_by_role('radio', name='nest', exact=True)).to_be_visible()
    picker.get_by_role('radio', name='/photos/park', exact=True).check()
    _wait_total(page, 4)
    expect(picker).to_contain_text('Includes the selected folder and its subfolders.')
    page.locator('[data-action="op"]').select_option('not_under')
    _wait_total(page, 2)


def test_picker_restores_saved_values_without_case_duplicates(live_server, page):
    db = live_server['db']
    db.conn.execute("UPDATE photos SET camera_model='Alpha'")
    db.conn.commit()
    _open_browse(page, live_server)
    page.evaluate("VireoFilter.loadExpression([{field:'camera_model', op:'in', value:['ALPHA','Absent']}])")
    page.click('.vf-filters-btn')
    picker = page.locator('.vf-value-picker')
    picker.locator('summary').click()
    expect(picker.locator('.vf-choice-status')).to_have_text('')
    expect(picker.get_by_role('checkbox', name='ALPHA', exact=True)).to_have_count(1)
    expect(picker.get_by_role('checkbox', name='Absent', exact=True)).to_be_checked()
    with page.expect_response(lambda r: '/api/workspaces/' in r.url and r.request.method == 'PUT'):
        picker.get_by_role('button', name='Remove ALPHA', exact=True).click()
    page.reload()
    page.wait_for_function('VireoFilter.isReady()')
    page.click('.vf-filters-btn')
    picker.locator('summary').click()
    expect(picker.get_by_role('checkbox', name='Absent', exact=True)).to_be_checked()


def test_picker_search_ignores_stale_results_and_does_not_edit_rule(live_server, page):
    held = []
    page.route('**/api/filters/values?field=camera_model*', lambda route: held.append(route))
    _open_browse(page, live_server)
    picker = _add_picker(page, 'camera_model')
    expect(picker).to_contain_text('Loading choices')
    page.wait_for_timeout(200)
    picker.get_by_role('searchbox').fill('new')
    page.wait_for_timeout(250)
    assert len(held) == 2
    held[1].fulfill(json={'values': [{'value': 'New', 'count': 1}]})
    expect(picker.get_by_role('checkbox', name='New', exact=True)).to_be_visible()
    held[0].fulfill(json={'values': [{'value': 'Old', 'count': 1}]})
    expect(picker.get_by_role('checkbox', name='Old', exact=True)).to_have_count(0)
    assert page.evaluate('VireoFilter.getUserRules().rules[0].value') == []


def test_picker_retry_and_empty_results(live_server, page):
    page.route('**/api/filters/values?field=lens*', lambda route: route.fulfill(status=500, json={'error': 'Unavailable'}))
    _open_browse(page, live_server)
    picker = _add_picker(page, 'lens')
    expect(picker).to_contain_text('Could not load choices')
    page.unroute('**/api/filters/values?field=lens*')
    picker.get_by_role('button', name='Retry', exact=True).click()
    expect(picker).to_contain_text('No choices in this workspace')


@pytest.mark.parametrize('kind,label', [
    ('burst', 'Filter to This Burst'),
    ('duplicate', 'Filter to This Duplicate Group'),
])
def test_photo_group_actions_avoid_typing_identifiers(live_server, page, kind, label):
    from e2e.stack_seed import seed_browse_stack
    db = live_server['db']
    ids = live_server['data']['photos'][:2]
    if kind == 'burst':
        seed_browse_stack(db, ids)
    else:
        db.conn.execute('UPDATE photos SET file_hash=? WHERE id IN (?, ?)', ('group-test', *ids))
        db.conn.commit()
    _open_browse(page, live_server)
    page.locator('#browseStacksToggle').check()
    card = page.locator('#grid .grid-card').filter(has=page.locator('.browse-stack-badge')).first
    expect(card).to_be_visible()
    card.click(button='right')
    page.locator('.vireo-ctx-item', has_text=label).click()
    page.wait_for_function('VireoFilter.getUserRules().rules.some(r => r.field === "photo_ids")')
    rule = page.evaluate('VireoFilter.getUserRules().rules.find(r => r.field === "photo_ids")')
    assert set(rule['value']) == set(ids)
    expect(page.locator('.vf-chips')).to_contain_text('2 selected photos')
    page.locator('#browseStacksToggle').uncheck()
    _wait_total(page, 2)
    page.click('.vf-filters-btn')
    expect(page.locator('.vf-identity-row')).to_contain_text('2 selected photos')
    expect(page.locator('.vf-rule-tree input[type="text"]')).to_have_count(0)
    page.click('.vf-add-filter')
    expect(page.locator('[data-add-field="burst_id"]')).to_have_count(0)
    expect(page.locator('[data-add-field="duplicate_group"]')).to_have_count(0)


def test_saved_multi_value_rules_survive_collection_editor(live_server, page):
    rules = {'mode': 'all', 'rules': [
        {'field': 'keyword', 'op': 'in', 'value': ['Red-tailed Hawk', 'American Robin']},
    ]}
    cid = live_server['db'].add_collection('Selected birds', json.dumps(rules))
    _open_browse(page, live_server)
    page.evaluate('cid => editCollection(cid)', cid)
    expect(page.locator('.collection-preserved-rule')).to_contain_text('Red-tailed Hawk, American Robin')
    assert page.evaluate('serializeCollectionRules()') == rules
