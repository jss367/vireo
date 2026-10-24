"""Subject uncertainty persists without accepting the proposed species."""

from playwright.sync_api import expect


def test_questionable_detections_and_revisitable_uncertainty(page, live_server):
    db = live_server['db']
    pid = live_server['data']['photos'][0]
    det = db.conn.execute('SELECT id FROM detections WHERE photo_id=?', (pid,)).fetchone()[0]
    db.predictions.add(det, 'American Golden-Plover', .9994, 'BioCLIP-2.5')
    db.predictions.add(det, 'Black-bellied Plover', .3651, 'test second classifier')
    before_keywords = [tuple(row) for row in db.photo_keywords.get_for_photo(pid)]
    before_flag = db.photos.get(pid)['flag']
    page.goto(live_server['url'] + f'/review?photo_id={pid}')
    expect(page.locator('.card')).to_have_count(3)
    page.locator('#reviewEvidenceFilter').select_option('questionable')
    expect(page.locator('.card')).to_have_count(1)
    card = page.locator('.card')
    expect(card).to_contain_text('99.94% relative score')
    expect(card).to_contain_text('Models disagree on this region')
    expect(card).to_contain_text('test second classifier: Black-bellied Plover · 36.51% relative score')
    card.get_by_text('Score, subject and location evidence').click()
    expect(card).to_contain_text('not a probability of correct identification')
    expect(card).to_contain_text('Geographic plausibility unknown')
    card.get_by_role('button', name='Not enough evidence', exact=True).click()
    expect(card).to_contain_text('Saved subject assessment: not enough evidence')
    expect(card.locator('.btn-accept')).to_have_count(0)
    expect(page.locator('#acceptAllBtn')).to_be_hidden()
    assert db.photos.get(pid)['flag'] == before_flag
    assert [tuple(row) for row in db.photo_keywords.get_for_photo(pid)] == before_keywords
    page.reload()
    expect(page.locator('#reviewEvidenceFilter')).to_have_value('questionable')
    expect(page.locator('.card')).to_have_count(1)
    expect(page.locator('.card')).to_contain_text('Saved subject assessment: not enough evidence')
    page.locator('.card').get_by_role('button', name='Clear assessment / revisit').click()
    expect(page.locator('.card')).to_contain_text('Saved subject assessment: unreviewed')
    expect(page.locator('.card .btn-accept')).to_have_count(1)
