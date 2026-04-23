"""E2E tests for the keyword row right-click context menu (Task 10).

Menu items (simplified per task prompt — the plan listed two redundant
"Filter Browse" / "Show Photos" items that jump to the same place; keeping
only "Show Photos with this Keyword"):

- Rename                          (single only)
- Set Type chip row (6 types)
- separator
- Show Photos with this Keyword   (single only)
- separator
- Delete
"""

from playwright.sync_api import expect


def test_keyword_right_click_opens_menu(live_server, page):
    """Right-clicking a keyword row opens the keyword context menu."""
    url = live_server["url"]
    page.goto(f"{url}/keywords")

    row = page.locator("tr[data-id]").first
    row.wait_for(state="visible")
    row.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    for label in [
        "Rename",
        "Show Photos with this Keyword",
        "Delete",
    ]:
        expect(
            menu.locator(".vireo-ctx-item", has_text=label)
        ).to_be_visible()

    # Six type chips for the Set Type chip row.
    chips = menu.locator(".vireo-ctx-chip")
    assert chips.count() == 6


def test_keyword_right_click_set_type_chip_fires_put(live_server, page):
    """Clicking a type chip PUTs the new type for the right-clicked keyword."""
    url = live_server["url"]
    page.goto(f"{url}/keywords")

    row = page.locator("tr[data-id]").first
    row.wait_for(state="visible")
    row.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: "/api/keywords/" in r.url
        and r.request.method == "PUT"
        and r.status == 200
    ):
        # The 'location' chip: position 3 of the 6-type chip row.
        menu.locator(".vireo-ctx-chip", has_text="location").click()


def test_keyword_right_click_show_photos_navigates(live_server, page):
    """Clicking 'Show Photos with this Keyword' navigates to /browse?keyword=..."""
    url = live_server["url"]
    page.goto(f"{url}/keywords")

    row = page.locator("tr[data-id]").first
    row.wait_for(state="visible")
    name = row.locator(".name-text").inner_text()
    row.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    menu.locator(
        ".vireo-ctx-item", has_text="Show Photos with this Keyword"
    ).click()

    # Wait for navigation.
    page.wait_for_url("**/browse?keyword=**", timeout=3000)
    assert "keyword=" in page.url
    # The keyword name appears URL-encoded in the query string.
    from urllib.parse import quote
    assert quote(name) in page.url or name in page.url


def test_keyword_right_click_rename_fires_put(live_server, page):
    """Clicking 'Rename' prompts for a new name and PUTs it."""
    url = live_server["url"]
    page.goto(f"{url}/keywords")

    row = page.locator("tr[data-id]").first
    row.wait_for(state="visible")

    # Auto-accept the prompt BEFORE clicking the menu item.
    page.on("dialog", lambda d: d.accept("Renamed Keyword"))

    row.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: "/api/keywords/" in r.url
        and r.request.method == "PUT"
        and r.status == 200
    ):
        menu.locator(".vireo-ctx-item", has_text="Rename").click()


def test_keyword_right_click_delete_fires_delete(live_server, page):
    """Clicking 'Delete' confirms and DELETEs the keyword."""
    url = live_server["url"]
    page.goto(f"{url}/keywords")

    row = page.locator("tr[data-id]").first
    row.wait_for(state="visible")
    kw_id = int(row.get_attribute("data-id"))

    # Auto-accept the confirm dialog BEFORE clicking.
    page.on("dialog", lambda d: d.accept())

    row.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: r.url.endswith(f"/api/keywords/{kw_id}")
        and r.request.method == "DELETE"
        and r.status == 200
    ):
        menu.locator(".vireo-ctx-item", has_text="Delete").click()

    # Row disappears after re-render.
    expect(
        page.locator(f'tr[data-id="{kw_id}"]')
    ).to_have_count(0)


def test_keyword_right_click_multi_disables_single_only_items(live_server, page):
    """When multiple keywords are selected, Rename and Show Photos are disabled."""
    url = live_server["url"]
    page.goto(f"{url}/keywords")

    # Select two rows by toggling their checkboxes.
    rows = page.locator("tr[data-id]")
    rows.first.wait_for(state="visible")
    assert rows.count() >= 2
    rows.nth(0).locator(".kw-cb").check()
    rows.nth(1).locator(".kw-cb").check()

    # Right-click one of the already-selected rows — selection stays multi.
    rows.nth(0).click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    rename = menu.locator(".vireo-ctx-item", has_text="Rename")
    show = menu.locator(
        ".vireo-ctx-item", has_text="Show Photos with this Keyword"
    )
    expect(rename).to_have_class("vireo-ctx-item vireo-ctx-disabled")
    expect(show).to_have_class("vireo-ctx-item vireo-ctx-disabled")


def test_keyword_right_click_outside_selection_coerces(live_server, page):
    """Right-click outside the current selection replaces it Finder-style."""
    url = live_server["url"]
    page.goto(f"{url}/keywords")

    rows = page.locator("tr[data-id]")
    rows.first.wait_for(state="visible")
    assert rows.count() >= 2

    # Check rows 0 and 1.
    rows.nth(0).locator(".kw-cb").check()
    rows.nth(1).locator(".kw-cb").check()

    # Right-click row 2 which is NOT in the selection.
    target = rows.nth(2) if rows.count() >= 3 else rows.nth(1)
    target_id = int(target.get_attribute("data-id"))

    # If we only have 2 rows, fall back to a row outside the first's selection.
    if rows.count() < 3:
        # Uncheck row 1 so row 0 is the sole selected; right-click row 1.
        rows.nth(1).locator(".kw-cb").uncheck()
        rows.nth(0).locator(".kw-cb").check()
        target = rows.nth(1)
        target_id = int(target.get_attribute("data-id"))

    target.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    # Selection now contains only the right-clicked row.
    size = page.evaluate("window.__kwSelectedIds && window.__kwSelectedIds.size")
    assert size == 1
    has_target = page.evaluate(
        f"window.__kwSelectedIds && window.__kwSelectedIds.has({target_id})"
    )
    assert has_target is True
