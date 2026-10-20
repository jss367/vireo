"""The unresolved identity workflow remains separate from real species tags."""

from playwright.sync_api import expect


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
