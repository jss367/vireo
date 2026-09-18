from playwright.sync_api import expect


def test_species_groups_preserve_paths_and_chart_drill_down(live_server, page):
    db = live_server['db']
    photos = live_server['data']['photos']
    taxon = db.conn.execute(
        "INSERT INTO taxa(name, common_name, rank, inat_id) VALUES ('Testus bird', 'Test bird', 'species', 123)"
    ).lastrowid
    parent = db.add_keyword('Imported birds')
    root = db.add_keyword('Test bird', is_species=True)
    leaf = db.add_keyword('Test Bird', parent_id=parent, is_species=True)
    db.conn.execute('UPDATE keywords SET taxon_id = ? WHERE id IN (?, ?)', (taxon, root, leaf))
    db.tag_photo(photos[0], root)
    db.tag_photo(photos[0], leaf)
    db.tag_photo(photos[1], leaf)
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(live_server['url'] + '/keywords')
    page.locator('#kwSearch').fill('Test bird')
    expect(page.locator('#kwBody tr')).to_have_count(1)
    row = page.locator('#kwBody tr')
    expect(row.locator('td').nth(5)).to_have_text('2')
    row.locator('summary').click()
    expect(row).to_contain_text('Imported birds → Test Bird')
    row.get_by_role('button', name='Edit individual records').click()
    expect(page.locator('#kwBody tr')).to_have_count(2)
    page.goto(live_server['url'] + '/dashboard')
    bar = page.locator('#speciesChart .species-bar', has_text='Test bird')
    expect(bar).to_have_count(1)
    expect(bar.locator('.bar-value')).to_have_text('2')
    bar.click()
    expect(page.locator('.grid-card')).to_have_count(2)
    page.locator('.vf-filters-btn').click()
    page.locator('.vf-advanced input').check()
    identity_row = page.locator('.vf-identity-row')
    expect(identity_row).to_contain_text('Keyword · Test bird')
    expect(identity_row.locator('select, input')).to_have_count(0)
    page.reload()
    expect(page.locator('.grid-card')).to_have_count(2)
    page.locator('.vf-filters-btn').click()
    page.locator('.vf-identity-row').get_by_role('button', name='Remove rule').click()
    expect(page.locator('.grid-card')).to_have_count(len(photos))
    assert errors == []


def test_location_preview_combines_only_chosen_match(live_server, page):
    db = live_server['db']
    photos = live_server['data']['photos']
    source = db.add_keyword('Lake Hodges', kw_type='general')
    target = db.upsert_place_chain({
        'place_id': 'test-place', 'name': 'Lake Hodges', 'lat': 33, 'lng': -117,
        'address_components': [{'name': 'San Diego', 'types': ['locality']}],
    })
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    page.goto(live_server['url'] + '/keywords')
    page.locator('#kwSearch').fill('Lake Hodges')
    expect(page.locator('#kwBody tr')).to_have_count(2)
    page.get_by_role('button', name='Review matching locations').click()
    preview = page.locator('#kwLocationMatches')
    expect(preview).to_contain_text('2 distinct photos after combining')
    expect(preview).to_contain_text('San Diego → Lake Hodges')
    preview.get_by_role('button', name='Use this place').click()
    expect(page.locator('#kwBody tr')).to_have_count(1)
    expect(preview).to_have_text('No matching location keywords to review.')
    expect(page.locator('#kwBody tr .kw-linked-badge')).to_be_visible()
    assert db.get_assigned_photo_location(photos[0])['place_id'] == 'test-place'
    assert db.add_keyword('Lake Hodges', _resolve_alias=True) == target


def test_merge_toolbar_stays_visible_when_scrolled_and_preserves_coordinates(live_server, page):
    db = live_server['db']
    photos = live_server['data']['photos']
    for index in range(60):
        kid = db.add_keyword(f'Scroll keyword {index:02d}', kw_type='general')
        db.tag_photo(photos[0], kid)
    source = db.add_keyword('Wing St. Canyon', kw_type='location')
    target = db.add_keyword('Wing Street Canyon', kw_type='location')
    db.update_keyword(target, latitude=32.7447, longitude=-117.2186)
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], source)
    db.tag_photo(photos[1], target)
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(live_server['url'] + '/keywords')
    page.locator(f'.kw-cb[data-id="{source}"]').check()
    expect(page.locator('#kwBulkMerge')).to_be_hidden()
    page.locator(f'.kw-cb[data-id="{target}"]').check()
    expect(page.locator('#kwBulkMerge')).to_be_in_viewport()
    assert page.locator('#kwSearch').bounding_box()['y'] < 0
    assert page.evaluate('window.scrollY') > 500
    page.locator('#kwBulkMerge').click()
    expect(page.locator('#kwMergeTarget')).to_have_value(str(target))
    expect(page.locator('#kwMergePreview')).to_contain_text('2 distinct photos after merging')
    expect(page.locator('#kwMergePreview')).to_contain_text('32.7447, -117.2186')
    page.get_by_role('button', name='Cancel', exact=True).click()
    assert db.conn.execute('SELECT 1 FROM keywords WHERE id = ?', (source,)).fetchone()
    expect(page.locator('#kwBulkMerge')).to_be_in_viewport()
    page.locator('#kwBulkMerge').click()
    page.locator('#kwMergeConfirm').click()
    expect(page.locator('#kwMergeDialog')).not_to_be_visible()
    expect(page.locator(f'tr[data-id="{source}"]')).to_have_count(0)
    expect(page.locator('#kwBulkBar')).to_be_hidden()
    page.locator('#kwSearch').fill('Wing')
    expect(page.locator('#kwBody tr')).to_have_count(1)
    expect(page.locator('#kwBody tr')).to_contain_text('32.7447, -117.2186')
    expect(page.locator('#kwBody tr td').nth(5)).to_have_text('2')
    assert errors == []


def test_merge_context_menu_linked_place_and_target_validation(live_server, page):
    db = live_server['db']
    photos = live_server['data']['photos']
    source = db.add_keyword('Whatcom Falls Park', kw_type='general')
    target = db.upsert_place_chain({
        'place_id': 'whatcom-falls', 'name': 'Whatcom Falls Park', 'lat': 48.7504, 'lng': -122.4269,
        'address_components': [{'name': 'Bellingham', 'types': ['locality']}],
    })
    db.tag_photo(photos[0], source)
    db.tag_photo(photos[1], target)
    page.goto(live_server['url'] + '/keywords')
    page.locator('#kwSearch').fill('Whatcom')
    page.locator(f'.kw-cb[data-id="{source}"]').check()
    page.locator(f'.kw-cb[data-id="{target}"]').check()
    page.locator(f'tr[data-id="{source}"]').click(button='right')
    page.locator('.vireo-ctx-item', has_text='Merge selected…').click()
    expect(page.locator('#kwMergeTarget')).to_have_value(str(target))
    expect(page.locator('#kwMergePreview')).to_contain_text('Bellingham → Whatcom Falls Park')
    page.locator('#kwMergeTarget').select_option(str(source))
    expect(page.locator('#kwMergeError')).to_be_visible()
    expect(page.locator('#kwMergeConfirm')).to_be_disabled()
    page.locator('#kwMergeTarget').select_option(str(target))
    expect(page.locator('#kwMergeConfirm')).to_be_enabled()
    page.locator('#kwMergeConfirm').click()
    expect(page.locator('#kwBody tr')).to_have_count(1)
    expect(page.locator('#kwBody tr .kw-linked-badge')).to_be_visible()
    assert db.get_assigned_photo_location(photos[0])['place_id'] == 'whatcom-falls'
