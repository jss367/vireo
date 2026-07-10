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
