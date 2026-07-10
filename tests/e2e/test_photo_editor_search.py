from playwright.sync_api import expect


def test_photo_editor_search_opens_matching_species(live_server, page):
    """Typing a species name on /edit opens a matching photo for editing."""
    url = live_server["url"]
    robin_id = live_server["data"]["photos"][3]

    page.goto(f"{url}/edit")
    expect(page.locator("#editorFilename")).to_have_text("No photo to edit")

    with page.expect_response("**/api/photos/ids?*"):
        page.locator("#editorSearchInput").fill("American Robin")

    expect(page).to_have_url(f"{url}/edit/{robin_id}")
    expect(page.locator("#editorFilename")).to_have_text("robin1.jpg")
    expect(page.locator("#editorSearchStatus")).to_have_text("1 match")


def test_photo_editor_clear_search_invalidates_pending_response(live_server, page):
    """Clearing search should ignore an older in-flight search response."""
    url = live_server["url"]
    held_routes = []

    def hold_photo_ids(route):
        held_routes.append(route)

    page.route("**/api/photos/ids?*", hold_photo_ids)
    page.goto(f"{url}/edit")

    page.locator("#editorSearchInput").fill("American Robin")
    for _ in range(20):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "search request was not issued"

    page.locator("#editorSearchInput").fill("")
    held_routes[0].continue_()

    expect(page).to_have_url(f"{url}/edit")
    expect(page.locator("#editorFilename")).to_have_text("No photo to edit")
    expect(page.locator("#editorSearchStatus")).to_have_text("")
