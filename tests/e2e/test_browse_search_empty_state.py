from playwright.sync_api import expect


def test_keyword_search_empty_state_and_clear(live_server, page):
    """A zero-result keyword search must not look like an empty library."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")

    search = page.locator(".vf-search input")
    expect(search).to_have_attribute("autocomplete", "off")
    expect(search).to_have_attribute("spellcheck", "false")

    with page.expect_response(lambda response: "/api/photos/query" in response.url):
        search.fill("definitely-no-such-photo")
        search.press("Enter")

    expect(page.locator("#emptyState")).to_be_visible()
    expect(page.locator("#welcomeState")).to_be_hidden()
    expect(page.locator("#emptyState")).to_contain_text("No photos match")

    with page.expect_response(lambda response: "/api/photos/query" in response.url):
        search.fill("")
        search.press("Enter")

    cards.first.wait_for(state="visible")
    expect(page.locator("#emptyState")).to_be_hidden()
    expect(page.locator("#welcomeState")).to_be_hidden()


def test_clearing_keyword_search_keeps_selected_photo_in_place(live_server, page):
    """Restoring filtered-out photos must not pull focus away from the selection."""
    url = live_server["url"]
    selected_id = live_server["data"]["photos"][3]
    page.goto(f"{url}/browse")

    page.locator(".grid-card").first.wait_for(state="visible")
    page.evaluate("updateThumbSize(400)")

    page.evaluate("VireoFilter.quickSearch('American Robin')")
    page.wait_for_function("() => photos.length === 1")
    selected = page.locator(f'.grid-card[data-id="{selected_id}"]')
    selected.wait_for(state="visible")
    selected.click()

    top_before = page.evaluate(
        """(id) => {
          const card = document.querySelector(`.grid-card[data-id="${id}"]`);
          const container = document.getElementById('gridContainer');
          return card.getBoundingClientRect().top - container.getBoundingClientRect().top;
        }""",
        selected_id,
    )

    page.evaluate("VireoFilter.quickSearch('')")
    page.wait_for_function(
        "(id) => photos.length === 5 && selectedPhotoId === id",
        arg=selected_id,
    )
    page.wait_for_timeout(100)  # allow the anchor-restoration animation frame

    assert page.evaluate("selectedPhotos.size") == 0
    expect(selected).to_have_class("grid-card selected")
    top_after = page.evaluate(
        """(id) => {
          const card = document.querySelector(`.grid-card[data-id="${id}"]`);
          const container = document.getElementById('gridContainer');
          return card.getBoundingClientRect().top - container.getBoundingClientRect().top;
        }""",
        selected_id,
    )
    assert abs(top_after - top_before) < 4


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

    page.click(".vf-filters-btn")
    pick_btn = page.locator('.vf-quick-flags [data-flag="flagged"]')
    reject_btn = page.locator('.vf-quick-flags [data-flag="rejected"]')
    expect(pick_btn).to_be_visible()
    expect(reject_btn).to_be_visible()

    pick_btn.click()
    expect(pick_btn).to_have_class("active")
    expect(page.locator(".grid-card")).to_have_count(1)
    assert page.locator(".grid-card").first.get_attribute("data-id") == str(pick_id)

    # Flags multi-select now: adding Rejected combines into "is one of".
    reject_btn.click()
    expect(page.locator(".grid-card")).to_have_count(2)

    pick_btn.click()
    expect(pick_btn).not_to_have_class("active")
    expect(page.locator(".grid-card")).to_have_count(1)
    assert page.locator(".grid-card").first.get_attribute("data-id") == str(reject_id)

    reject_btn.click()
    expect(page.locator(".grid-card")).to_have_count(5)
