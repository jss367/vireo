"""The unresolved identity workflow remains separate from real species tags."""

import re

import pytest
from playwright.sync_api import expect
from services import workspace_folders


def open_panel(live_server, page):
    page.goto(live_server['url'] + '/review')
    expect(page.locator('#grid .card').first).to_be_visible()
    page.locator('#identityAlternativesPanel > summary').click()
    expect(page.locator('#identityAlternativesCount')).to_contain_text('0 photos')
    page.locator('#identityAlternativesEditor > summary').click()
    page.locator('#identityLoadPhotos').click()
    expect(page.locator('#identityPhotos option')).to_have_count(5)
    ids = live_server['data']['photos'][:2]
    page.locator('#identityPhotos').select_option([str(pid) for pid in ids])
    page.locator('#identityCandidates').fill("Tropical Kingbird\nCouch's Kingbird")
    page.locator('#identityReason').fill('Still photos do not identify this pair reliably')
    page.locator('#identityEvidence').fill('A recording of the call')
    return ids


def test_save_reload_edit_resolve_and_undo(live_server, page):
    ids = open_panel(live_server, page)
    db = live_server['db']
    db.photo_review.set_flag(ids[0], 'rejected')
    before_species = db.photo_species.get_species_for_photos(ids)
    page.locator('#identitySave').click()
    group = page.locator('#identityAlternativesGroups article')
    expect(group).to_have_count(1)
    expect(group).to_contain_text("Tropical Kingbird or Couch's Kingbird — unresolved")
    expect(group).to_contain_text('2 photos')
    expect(group).to_contain_text('A recording of the call')
    assert db.photo_species.get_species_for_photos(ids) == before_species
    page.reload()
    page.locator('#identityAlternativesPanel > summary').click()
    expect(group).to_have_count(1)
    group.get_by_role('button', name='Edit alternatives and evidence').click()
    page.locator('#identityEvidence').fill('An audio recording or expert review')
    page.locator('#identitySave').click()
    expect(group).to_contain_text('An audio recording or expert review')
    expect(group.get_by_role('button', name='Resolve as Tropical Kingbird', exact=True)).to_be_disabled()
    group.get_by_role('button', name='Select all frames to resolve', exact=True).click()
    group.get_by_role('button', name='Resolve as Tropical Kingbird', exact=True).click()
    expect(group).to_have_count(0)
    assert 'Tropical Kingbird' in db.photo_species.get_species_for_photos(ids)[ids[0]]
    assert db.photos.get(ids[0])['flag'] == 'rejected'
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.locator('#historyUndoBtn').click()
    expect(group).to_have_count(1)
    assert db.photo_species.get_species_for_photos(ids) == before_species
    assert db.photos.get(ids[0])['flag'] == 'rejected'


def test_resolve_selected_frames_leaves_others_unresolved(live_server, page):
    ids = open_panel(live_server, page)
    page.locator('#identitySave').click()
    group = page.locator('#identityAlternativesGroups article')
    expect(group).to_have_count(1)
    group.get_by_label('Photos to resolve').select_option(str(ids[0]))
    expect(group).to_contain_text('1 of 2 photos selected to resolve')
    with page.expect_request('**/api/identity-alternatives/resolve') as request:
        group.get_by_role('button', name='Resolve as Tropical Kingbird', exact=True).click()
    assert request.value.post_data_json['photo_ids'] == [ids[0]]
    expect(group).to_have_count(1)
    expect(group).to_contain_text('1 photos.')
    db = live_server['db']
    assert db.identity_alternatives.photo_ids() == {ids[1]}
    assert 'Tropical Kingbird' in db.photo_species.get_species_for_photos(ids)[ids[0]]
    assert ids[1] not in db.photo_species.get_species_for_photos(ids)
    page.reload()
    page.locator('#identityAlternativesPanel > summary').click()
    expect(group).to_have_count(1)
    expect(group.get_by_label('Photos to resolve').locator('option')).to_have_count(1)
    expect(group.get_by_role('button', name='Resolve as Tropical Kingbird', exact=True)).to_be_disabled()


def test_failed_save_keeps_evidence_and_can_retry(live_server, page):
    open_panel(live_server, page)
    page.route('**/api/identity-alternatives', lambda route: route.fulfill(
        status=503, json={'error': 'Another identification decision is in progress'},
    ) if route.request.method == 'POST' else route.continue_())
    page.locator('#identitySave').click()
    expect(page.locator('#identityAlternativesStatus')).to_contain_text('Could not confirm the save')
    expect(page.locator('#identityReason')).to_have_value('Still photos do not identify this pair reliably')
    expect(page.locator('#identitySave')).to_be_enabled()
    page.unroute('**/api/identity-alternatives')
    page.locator('#identitySave').click()
    expect(page.locator('#identityAlternativesGroups article')).to_have_count(1)


def test_stale_create_form_cannot_edit_a_new_active_workspace(live_server, page):
    ids = open_panel(live_server, page)
    db = live_server['db']
    kid = db.add_keyword('Tropical Kingbird', is_species=True)
    db.tag_photo(ids[0], kid)
    before = db.photo_species.get_species_for_photos(ids)
    other = db.create_workspace('Another shared workspace')
    workspace_folders.add_workspace_folder(db, other, db.photos.get(ids[0])['folder_id'])
    db.workspaces.update(other, last_opened_at='2099-01-01T00:00:00')
    page.locator('#identitySave').click()
    expect(page.locator('#identityAlternativesStatus')).to_contain_text('The active workspace changed')
    expect(page.locator('#identitySave')).to_be_disabled()
    assert db.identity_alternatives.photo_ids() == set()
    assert db.photo_species.get_species_for_photos(ids) == before


def test_merged_single_candidate_stays_unconfirmed_until_explicit_resolution(live_server, page):
    ids = open_panel(live_server, page)
    db = live_server['db']
    before_species = db.photo_species.get_species_for_photos(ids)
    source = db.add_keyword('Tropical Kingbird', is_species=True)
    survivor = db.add_keyword("Couch's Kingbird", is_species=True)
    target_name = db.keywords.name_of(survivor)
    page.locator('#identitySave').click()
    group = page.locator('#identityAlternativesGroups article')
    expect(group).to_have_count(1)
    group.get_by_role('button', name='Edit alternatives and evidence').click()
    db._merge_keyword_into(source, survivor)
    db.commit()
    with page.expect_response(lambda response: response.url.endswith('/api/identity-alternatives') and response.request.method == 'POST') as response:
        page.locator('#identitySave').click()
    assert response.value.status == 409
    expect(page.locator('#identityAlternativesStatus')).to_contain_text('Could not confirm the save')
    page.reload()
    page.locator('#identityAlternativesPanel > summary').click()
    expect(group).to_contain_text(target_name + " — unresolved")
    expect(group).to_contain_text('One candidate remains; identification still needs explicit confirmation.')
    assert db.photo_species.get_species_for_photos(ids) == before_species
    group.get_by_role('button', name='Edit alternatives and evidence').click()
    page.locator('#identityEvidence').fill('Expert review still needed')
    page.locator('#identitySave').click()
    expect(group).to_contain_text('Expert review still needed')
    assert db.photo_species.get_species_for_photos(ids) == before_species
    group.get_by_role('button', name='Select all frames to resolve', exact=True).click()
    group.get_by_role('button', name="Resolve as " + target_name, exact=True).click()
    expect(group).to_have_count(0)
    species = db.photo_species.get_species_for_photos(ids)
    assert all(set(species[pid]) == {*before_species.get(pid, []), target_name} for pid in ids)


@pytest.mark.parametrize('history_refresh', ['normal', 'delayed', 'failed', 'failed_status'])
def test_retired_identity_conflict_refreshes_undo_tooltip_before_next_click(live_server, page, history_refresh):
    from services.identity_alternatives import save_alternatives
    ids = open_panel(live_server, page)
    db = live_server['db']
    pid = ids[0]
    original_rating = db.photos.get(pid)['rating']
    changed_rating = 4 if original_rating == 5 else 5
    page.evaluate("args => safeFetch('/api/photos/' + args.id + '/rating', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({rating:args.rating})})",
                  {'id': pid, 'rating': changed_rating})
    description = db.edit_history.latest_undoable()['description']
    page.locator('#identitySave').click()
    group = page.locator('#identityAlternativesGroups article')
    expect(group).to_have_count(1)
    first = db.require_workspace_id()
    second = db.create_workspace('Another reviewer')
    db.photo_visibility.grant(second, ids)
    db.commit()
    db.set_active_workspace(second)
    with db._commits_held():
        save_alternatives(db, ids, ['Tropical Kingbird', "Couch's Kingbird"], 'Newer shared review', 'An audio recording')
    db.set_active_workspace(first)
    page.evaluate("() => switchBpTab('history')")
    # Prime the actual row action before the conflict, including when its pane is hidden.
    expect(page.locator('#bpHistoryList .bp-history-row button')).to_have_count(1)
    held = []
    if history_refresh == 'delayed':
        page.route('**/api/edit-history?*', lambda route: held.append(route))
    elif history_refresh in ('failed', 'failed_status'):
        page.route('**/api/edit-history?*', lambda route: route.fulfill(status=503, json={'error': 'Offline'}))
    if history_refresh == 'failed_status':
        page.route('**/api/undo/status', lambda route: route.fulfill(status=503, json={'error': 'Offline'}))
    undo = page.locator('#historyUndoBtn')
    expect(undo).to_be_enabled()
    with page.expect_response(lambda response: response.url.endswith('/api/undo') and response.request.method == 'POST') as response:
        undo.click()
    assert response.value.status == 409
    # A failed list/status refresh must not expose an old row label for a new action.
    if history_refresh != 'normal':
        expect(page.locator('#bpHistoryList .bp-history-row button')).to_have_count(0)
        assert db.photos.get(pid)['rating'] == changed_rating
    if history_refresh == 'failed_status':
        expect(undo).to_be_disabled()
        page.unroute('**/api/undo/status')
        page.evaluate("() => window.dispatchEvent(new Event('focus'))")
    expect(undo).to_have_attribute('title', re.compile(re.escape('Undo: ' + description)))
    if history_refresh == 'delayed':
        assert held
        held.pop().continue_()
    elif history_refresh in ('failed', 'failed_status'):
        page.unroute('**/api/edit-history?*')
        page.evaluate("() => switchBpTab('history')")
    expect(page.locator('#bpHistoryList .bp-history-row').filter(has_text=description).locator('button')).to_have_count(1)
    expect(page.locator('#bpHistoryList .bp-history-row').filter(has_text='Superseded identification:').locator('button')).to_have_count(0)
    assert db.photos.get(pid)['rating'] == changed_rating
    with page.expect_response(lambda response: response.url.endswith('/api/undo') and response.request.method == 'POST') as final:
        undo.click()
    assert final.value.status == 200
    expect(page.locator('#undoMsg')).not_to_contain_text(description)
    assert db.photos.get(pid)['rating'] == original_rating


def test_same_species_aliases_are_rejected_without_losing_form_or_confirmation(live_server, page):
    ids = open_panel(live_server, page)
    db = live_server['db']
    db.conn.execute("INSERT INTO taxa(id,inat_id,name,common_name,rank) VALUES (88001,88001,'Tyrannus melancholicus','Tropical Kingbird','species')")
    scientific = db.add_keyword('Tyrannus melancholicus', is_species=True)
    db.tag_photo(ids[0], scientific, source='accept')
    db.photo_review.set_flag(ids[0], 'rejected')
    history = [tuple(row) for row in db.conn.execute('SELECT * FROM edit_history_items')]
    page.locator('#identityCandidates').fill('Tropical Kingbird\nTyrannus melancholicus')
    with page.expect_response(lambda response: response.url.endswith('/api/identity-alternatives') and response.request.method == 'POST') as response:
        page.locator('#identitySave').click()
    assert response.value.status == 400
    assert 'same species' in response.value.json()['error']
    expect(page.locator('#identityAlternativesStatus')).to_contain_text('Could not confirm the save')
    expect(page.locator('#identityCandidates')).to_have_value('Tropical Kingbird\nTyrannus melancholicus')
    expect(page.locator('#identityReason')).to_have_value('Still photos do not identify this pair reliably')
    expect(page.locator('#identityEvidence')).to_have_value('A recording of the call')
    expect(page.locator('#identitySave')).to_be_enabled()
    expect(page.locator('#identityAlternativesGroups article')).to_have_count(0)
    assert db.conn.execute('SELECT source FROM photo_keywords WHERE photo_id=? AND keyword_id=?', (ids[0], scientific)).fetchone()[0] == 'accept'
    assert db.photos.get(ids[0])['flag'] == 'rejected'
    assert [tuple(row) for row in db.conn.execute('SELECT * FROM edit_history_items')] == history
