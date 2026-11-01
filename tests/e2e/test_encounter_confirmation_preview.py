"""A reviewer sees the exact species delta before narrowing and separating frames."""
import json

from playwright.sync_api import expect

from e2e.test_burst_group_confirm_split import _cache_path, _write_single_burst_cache


def _seed_transition(live_server):
    db = live_server['db']
    ids = live_server['data']['photos'][:3]
    old, new, other = "Cassin's Kingbird", 'Yellow-rumped Warbler', 'American Wigeon'
    for pid in ids:
        db.tag_photo(pid, db.add_keyword(old, is_species=True))
        db.commit()
    db.tag_photo(ids[1], db.add_keyword(other, is_species=True))
    db.commit()
    db.photo_review.set_flag(ids[1], 'flagged')
    db.commit()
    _write_single_burst_cache(live_server, ids, confirmed_species=old)
    path = _cache_path(live_server)
    with open(path) as handle:
        cache = json.load(handle)
    cache['encounters'][0]['species_predictions'] = [
        {'species': new, 'models': [{'model': 'test', 'confidence': .9}]},
    ]
    with open(path, 'w') as handle:
        json.dump(cache, handle)
    return ids, old, new, other


def _open_preview(page, live_server, species):
    page.goto(f"{live_server['url']}/pipeline/review")
    page.locator('[data-species-widget="1"][data-level="encounter"] .species-name').first.click()
    field = page.locator('.species-dropdown.open input[type=text]')
    field.fill(species)
    field.press('Enter')
    preview = page.locator('#speciesConfirmationPreview')
    expect(preview.locator('.species-preview-apply')).to_be_enabled()
    return preview


def test_reviewer_narrows_transition_and_preserves_other_subject_species(live_server, page):
    ids, old, new, other = _seed_transition(live_server)
    preview = _open_preview(page, live_server, new)
    expect(preview.locator('tbody tr')).to_have_count(3)
    expect(preview.locator('.species-preview-summary')).to_contain_text('3 of 3 photos')
    expect(preview).to_contain_text(other)
    expect(preview.locator('details')).to_have_count(3)
    expect(preview.locator('.species-preview-subject > span')).to_have_count(3)
    preview.locator('.species-preview-from').fill('2')
    preview.locator('.species-preview-through').fill('3')
    preview.locator('.species-preview-range').click()
    expect(preview.locator('.species-preview-summary')).to_contain_text('2 of 3 photos')
    preview.locator('.species-preview-mode').select_option('replace')
    preview.locator('.species-preview-previous').select_option(old)
    expect(preview.locator('tbody tr').nth(1)).to_contain_text('Remove ' + old)
    expect(preview.locator('tbody tr').nth(0)).to_contain_text('Excluded; no changes')
    preview.locator('.species-preview-split').check()
    expect(preview.locator('.species-preview-apply')).to_be_enabled()
    requests = []
    page.on('request', lambda request: requests.append(request.url))
    with page.expect_response(lambda response: '/api/encounters/species' in response.url and not response.request.post_data_json.get('preview')) as response:
        preview.locator('.species-preview-apply').click()
    result = response.value.json()
    assert response.value.request.post_data_json['response_mode'] == 'compact'
    assert result['response_mode'] == 'compact'
    assert 'encounters' not in result
    assert len(result['encounter_changes']['upserts']) == 2
    expect(preview).to_have_count(0)
    expect(page.locator('.encounter-card')).to_have_count(2)
    db = live_server['db']
    species = db.photo_species.get_species_for_photos(ids)
    assert old in species[ids[0]] and new not in species[ids[0]]
    assert {s.lower() for s in species[ids[1]]} == {new.lower(), other.lower()}
    assert [s.lower() for s in species[ids[2]]] == [new.lower()]
    assert db.photos.get(ids[1])['flag'] == 'flagged'
    assert not any('/api/pipeline/page-init' in url or '/api/pipeline/save-cache' in url for url in requests)


def test_preview_cancel_and_stale_revalidation_do_not_apply(live_server, page):
    ids, old, new, other = _seed_transition(live_server)
    preview = _open_preview(page, live_server, new)
    before = live_server['db'].photo_species.get_species_for_photos(ids)
    preview.locator('.species-preview-cancel').click()
    expect(preview).to_have_count(0)
    assert live_server['db'].photo_species.get_species_for_photos(ids) == before
    preview = _open_preview(page, live_server, new)
    # Another review decision after the preview must invalidate the exact set.
    db = live_server['db']
    db.tag_photo(ids[2], db.add_keyword('Eared Grebe', is_species=True))
    db.commit()
    with page.expect_response(lambda response: '/api/encounters/species' in response.url and not response.request.post_data_json.get('preview')) as response:
        preview.locator('.species-preview-apply').click()
    assert response.value.status == 409
    expect(page.locator('#toastContainer [role=alert]').filter(has_text='Preview again')).to_be_visible()
    assert all(new not in names for names in db.photo_species.get_species_for_photos(ids).values())
