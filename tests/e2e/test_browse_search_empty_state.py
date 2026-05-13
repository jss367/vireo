from playwright.sync_api import expect


def test_keyword_search_empty_state_and_clear(live_server, page):
    """A zero-result keyword search must not look like an empty library."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")

    search = page.locator("#searchInput")
    search.fill("definitely-no-such-photo")
    search.press("Enter")

    expect(page.locator("#emptyState")).to_be_visible()
    expect(page.locator("#welcomeState")).to_be_hidden()
    expect(page.locator("#emptyState")).to_contain_text("No photos match")

    search.fill("")

    cards.first.wait_for(state="visible")
    expect(page.locator("#emptyState")).to_be_hidden()
    expect(page.locator("#welcomeState")).to_be_hidden()
