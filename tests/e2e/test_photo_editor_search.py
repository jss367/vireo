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


def test_photo_editor_search_invalidates_pending_response_when_query_changes(live_server, page):
    """Typing a new query while a prior search is in flight discards the older response."""
    url = live_server["url"]
    hawk_id = live_server["data"]["photos"][0]
    held_routes = []

    def hold(route):
        held_routes.append(route)

    page.goto(f"{url}/edit/{hawk_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")

    page.route("**/api/photos/ids?*", hold)

    page.locator("#editorSearchInput").fill("American Robin")
    for _ in range(20):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "first search request was not issued"

    # Replace the query while the robin response is still held. The seq must
    # be bumped now, not only when the next debounced search fires 300ms later.
    page.locator("#editorSearchInput").fill("zzzz-no-match")

    held_routes[0].continue_()

    # Give the stale response a chance to (incorrectly) navigate or restatus.
    page.wait_for_timeout(300)

    expect(page).to_have_url(f"{url}/edit/{hawk_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")
    expect(page.locator("#editorSearchStatus")).not_to_have_text("1 match")


def test_photo_editor_search_confirms_dirty_edits_from_in_flight_search(live_server, page):
    """Edits made while a search is in flight must not be silently discarded."""
    url = live_server["url"]
    hawk_id = live_server["data"]["photos"][0]
    held_routes = []

    def hold(route):
        held_routes.append(route)

    page.goto(f"{url}/edit/{hawk_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")

    page.route("**/api/photos/ids?*", hold)

    page.locator("#editorSearchInput").fill("American Robin")
    for _ in range(20):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "search request was not issued"

    # Dirty the recipe while the search is in flight.
    exposure = page.locator("#exposureRange")
    exposure.evaluate("(el) => { el.value = '1.2'; el.dispatchEvent(new Event('input')); }")

    dialogs = []

    def on_dialog(dialog):
        dialogs.append(dialog.message)
        dialog.dismiss()

    page.once("dialog", on_dialog)

    held_routes[0].continue_()

    # The confirm must be shown, and dismissing it keeps us on the current
    # (dirty) photo instead of navigating to a search match.
    for _ in range(20):
        if dialogs:
            break
        page.wait_for_timeout(100)
    assert dialogs, "expected a discard confirmation for dirty in-flight edits"
    assert "Discard" in dialogs[0]
    expect(page).to_have_url(f"{url}/edit/{hawk_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")
