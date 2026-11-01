"""Real Review card decisions across classifier groups with unequal membership."""

import pytest
from playwright.sync_api import expect


def _blue_tit_cards(db, photo_ids):
    db.conn.execute('DELETE FROM predictions')
    db.conn.execute(
        "INSERT INTO taxa (inat_id, name, common_name, rank) VALUES (138690, 'Cyanistes caeruleus', 'Blue Tit', 'species')",
    )
    ids = []
    for model, species, indexes, group in [
        ('BioCLIP-2.5', 'Eurasian Blue Tit', (0, 1), 'blue-bio'),
        ('iNat21', 'Blue Tit', (1, 2), 'blue-inat'),
    ]:
        for index in indexes:
            det = db.conn.execute('SELECT id FROM detections WHERE photo_id=?', (photo_ids[index],)).fetchone()[0]
            db.predictions.add(
                det, species, .9, model, labels_fingerprint=model, group_id=group,
                taxonomy={'taxon_id': 138690, 'scientific_name': 'Cyanistes caeruleus'},
                vote_count=2, total_votes=2,
            )
            db.commit()
            ids.append(db.conn.execute('SELECT max(id) FROM predictions').fetchone()[0])
    db.commit()
    return ids


@pytest.mark.parametrize('decision', ['accept', 'reject'])
def test_merged_card_decides_both_models_and_undo_restores(page, live_server, decision):
    db = live_server['db']
    ids = _blue_tit_cards(db, live_server['data']['photos'])
    page.goto(live_server['url'] + '/review')
    cards = page.locator('#grid .card')
    expect(cards).to_have_count(1)
    expect(cards.locator('.review-card-models .badge-group')).to_have_count(2)
    card_box = cards.bounding_box()
    for chip in cards.locator('.review-card-models .badge-group').all():
        chip_box = chip.bounding_box()
        assert chip_box['x'] + chip_box['width'] <= card_box['x'] + card_box['width']
    expect(cards).to_contain_text('BioCLIP-2.5')
    expect(cards).to_contain_text('iNat21')
    expect(page.locator('#stats')).to_contain_text('1 Pending cards')
    expect(cards.locator('.btn-accept')).to_have_text('Tag 3 photos as "Blue Tit"')
    with page.expect_response(lambda r: r.url.endswith('/' + decision) and r.request.method == 'POST') as response:
        cards.locator('.btn-accept' if decision == 'accept' else '.btn-skip').click()
    assert response.value.status == 200
    expect(page.locator('#stats')).to_contain_text('0 Pending cards')
    assert [db.predictions.current_status(pid) for pid in ids] == [
        'accepted' if decision == 'accept' else 'rejected'
    ] * len(ids)
    page.evaluate('window.doUndo()')
    expect(page.locator('#stats')).to_contain_text('1 Pending cards')
    expect(cards).to_have_count(1)
    assert [db.predictions.current_status(pid) for pid in ids] == ['pending'] * len(ids)


def test_model_filter_limits_the_decision_and_clearing_it_reveals_pending_sibling(page, live_server):
    db = live_server['db']
    ids = _blue_tit_cards(db, live_server['data']['photos'])
    page.goto(live_server['url'] + '/review')
    expect(page.locator('#grid .card')).to_have_count(1)
    page.locator('#modelFilter').select_option('BioCLIP-2.5')
    expect(page.locator('.review-card-models .badge-group')).to_have_count(1)
    expect(page.locator('#grid .btn-accept')).to_have_text('Tag 2 photos as "Blue Tit"')
    with page.expect_response(lambda r: r.url.endswith('/accept') and r.request.method == 'POST'):
        page.locator('#grid .btn-accept').click()
    expect(page.locator('#stats')).to_contain_text('0 Pending cards')
    assert [db.predictions.current_status(pid) for pid in ids] == ['accepted', 'accepted', 'pending', 'pending']
    page.locator('#modelFilter').select_option('all')
    expect(page.locator('#stats')).to_contain_text('1 Pending cards')
    page.locator('#tabs .tab').filter(has_text='Pending').click()
    expect(page.locator('#grid .card')).to_have_count(1)
    expect(page.locator('#grid .card')).to_contain_text('iNat21')
