"""E2E tests for the collection sidebar right-click context menu (Task 9).

Menu items:
- Filter by this Collection
- separator
- Rename
- Duplicate
- separator
- Delete Collection
"""

from playwright.sync_api import expect


def _seed_collection(live_server, name="Test Pick"):
    """Create a collection via the Flask test client directly (avoids racy
    page.evaluate(fetch) seeding)."""
    import json as _json
    # Use requests-style client through the live_server's db: easiest path is
    # adding the row directly via the bound db handle, since that matches what
    # other e2e helpers do.
    db = live_server["db"]
    return db.add_collection(name, _json.dumps([]))


def test_collection_right_click_shows_menu(live_server, page):
    """Right-clicking a collection tree-item opens the collection menu."""
    _seed_collection(live_server, "Test Pick")
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(
        ".tree-item[data-collection-id]", has_text="Test Pick"
    ).first
    item.wait_for(state="visible")
    item.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    for label in [
        "Filter by this Collection",
        "Rename",
        "Duplicate",
        "Delete Collection",
    ]:
        expect(
            menu.locator(".vireo-ctx-item", has_text=label)
        ).to_be_visible()


def test_collection_filter_fires_filter(live_server, page):
    """Clicking 'Filter by this Collection' sets activeCollectionId."""
    cid = _seed_collection(live_server, "Picks A")
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(
        ".tree-item[data-collection-id]", has_text="Picks A"
    ).first
    item.wait_for(state="visible")
    item.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu.locator(
        ".vireo-ctx-item", has_text="Filter by this Collection"
    ).click()
    expect(menu).to_be_hidden()

    page.wait_for_function(
        f"window.activeCollectionId === {cid}", timeout=3000
    )


def test_collection_duplicate_fires_endpoint_and_rerenders(live_server, page):
    """Clicking 'Duplicate' POSTs to /duplicate and re-renders the list."""
    _seed_collection(live_server, "Picks B")
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(
        ".tree-item[data-collection-id]", has_text="Picks B"
    ).first
    item.wait_for(state="visible")
    item.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: "/duplicate" in r.url and r.status == 200
    ):
        menu.locator(".vireo-ctx-item", has_text="Duplicate").click()

    # After duplicate, the list re-renders with the copy visible.
    expect(
        page.locator(
            ".tree-item[data-collection-id]", has_text="Picks B (copy)"
        )
    ).to_be_visible()


def test_collection_rename_fires_put(live_server, page):
    """Clicking 'Rename' prompts and PUTs the new name."""
    _seed_collection(live_server, "Picks C")
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(
        ".tree-item[data-collection-id]", has_text="Picks C"
    ).first
    item.wait_for(state="visible")

    # Accept the rename prompt with a new name. Register BEFORE clicking the
    # menu item (the dialog may fire synchronously).
    page.on("dialog", lambda d: d.accept("Picks C Renamed"))

    item.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: "/api/collections/" in r.url
        and r.request.method == "PUT"
        and r.status == 200
    ):
        menu.locator(".vireo-ctx-item", has_text="Rename").click()

    # List re-renders with the new name.
    expect(
        page.locator(
            ".tree-item[data-collection-id]", has_text="Picks C Renamed"
        )
    ).to_be_visible()


def test_collection_delete_fires_endpoint(live_server, page):
    """Clicking 'Delete Collection' confirms and DELETEs."""
    cid = _seed_collection(live_server, "Picks D")
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(
        ".tree-item[data-collection-id]", has_text="Picks D"
    ).first
    item.wait_for(state="visible")

    # Auto-accept the confirm dialog BEFORE clicking.
    page.on("dialog", lambda d: d.accept())

    item.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: r.url.endswith(f"/api/collections/{cid}")
        and r.request.method == "DELETE"
        and r.status == 200
    ):
        menu.locator(".vireo-ctx-item", has_text="Delete Collection").click()

    # List re-renders without the deleted collection.
    expect(
        page.locator(
            ".tree-item[data-collection-id]", has_text="Picks D"
        )
    ).to_have_count(0)
