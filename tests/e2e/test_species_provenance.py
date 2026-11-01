"""Reviewers can distinguish species evidence and edit it in Browse."""

from playwright.sync_api import expect


def test_species_evidence_editor_persists_and_links_supporting_frames(live_server, page):
    db = live_server['db']
    pid, support = live_server['data']['photos'][:2]
    species = db.add_keyword('Mallard', is_species=True)
    db.tag_photo(pid, species, provenance={
        'method': 'adjacent_frames', 'reason': 'The same bird is visible in the next frame',
        'reviewer': 'Field reviewer', 'supporting_photo_ids': [support],
        'confidence': None, 'confidence_meaning': 'No calibrated probability is available',
    })
    db.commit()
    page.goto(live_server['url'] + f'/browse?photo_id={pid}')
    page.locator(f'.grid-card[data-id="{pid}"]').click()
    panels = page.locator('#detailKeywords .species-provenance')
    expect(panels).to_have_count(2)
    expect(panels.filter(has_text='Red-tailed Hawk').locator('summary')).to_contain_text('Manual / unspecified')
    inferred = panels.filter(has_text='Mallard')
    expect(inferred.locator('summary')).to_contain_text('Inferred from adjacent frames')
    inferred.locator('summary').click()
    expect(inferred).to_contain_text('Confidence: not recorded')
    expect(inferred).to_contain_text('No calibrated probability is available')
    expect(inferred.get_by_role('link', name=f'Supporting frame {support}')).to_have_attribute(
        'href', f'/browse?photo_id={support}')
    inferred.get_by_label('Identification method').select_option('direct_visual')
    inferred.get_by_label('Reason or explanation').fill('Wing markings are visible in this frame')
    inferred.get_by_role('button', name='Save species evidence').click()
    expect(inferred.locator('summary')).to_contain_text('Direct visual confirmation')
    page.reload()
    page.locator(f'.grid-card[data-id="{pid}"]').click()
    expect(inferred.locator('summary')).to_contain_text('Direct visual confirmation')
    inferred.locator('summary').click()
    expect(inferred.get_by_label('Reason or explanation')).to_have_value('Wing markings are visible in this frame')
    inferred.get_by_role('link', name=f'Supporting frame {support}').click()
    page.locator(f'.grid-card[data-id="{support}"]').click()
    expect(page.locator('#detailFilename')).to_have_text('hawk2.jpg')
