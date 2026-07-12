from playwright.sync_api import expect


def test_keyword_search_empty_state_and_clear(live_server, page):
    """A zero-result keyword search must not look like an empty library."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")

    search = page.locator("#searchInput")
    expect(search).to_have_attribute("autocomplete", "off")
    expect(search).to_have_attribute("autocorrect", "off")
    expect(search).to_have_attribute("autocapitalize", "none")
    expect(search).to_have_attribute("spellcheck", "false")

    search.fill("definitely-no-such-photo")

    expect(page.locator("#emptyState")).to_be_visible()
    expect(page.locator("#welcomeState")).to_be_hidden()
    expect(page.locator("#emptyState")).to_contain_text("No photos match")

    search.fill("")

    cards.first.wait_for(state="visible")
    expect(page.locator("#emptyState")).to_be_hidden()
    expect(page.locator("#welcomeState")).to_be_hidden()


def test_flag_quick_filters_show_picks_and_rejects(live_server, page):
    """Browse keeps always-visible quick filters for picked and rejected photos."""
    url = live_server["url"]
    db = live_server["db"]
    photos = db.get_photos()
    pick_id = photos[0]["id"]
    reject_id = photos[1]["id"]
    db.update_photo_flag(pick_id, "flagged")
    db.update_photo_flag(reject_id, "rejected")

    page.goto(f"{url}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    pick_btn = page.locator("#pickFilterBtn")
    reject_btn = page.locator("#rejectFilterBtn")
    expect(pick_btn).to_be_visible()
    expect(reject_btn).to_be_visible()

    pick_btn.click()
    expect(pick_btn).to_have_class("flag-filter-btn active-pick")
    expect(page.locator(".grid-card")).to_have_count(1)
    assert page.locator(".grid-card").first.get_attribute("data-id") == str(pick_id)

    reject_btn.click()
    expect(pick_btn).to_have_class("flag-filter-btn")
    expect(reject_btn).to_have_class("flag-filter-btn active-reject")
    expect(page.locator(".grid-card")).to_have_count(1)
    assert page.locator(".grid-card").first.get_attribute("data-id") == str(reject_id)

    reject_btn.click()
    expect(reject_btn).to_have_class("flag-filter-btn")
    expect(page.locator(".grid-card")).to_have_count(5)
