from playwright.sync_api import expect


def test_right_click_photo_opens_menu(live_server, page):
    """Right-clicking a grid card opens the context menu with chips + key actions."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    # Rating chips row present. 6 rating chips (0..5) + 5 color chips +
    # 3 flag chips = 14 chips total; assert the count is clearly > 5 per plan.
    assert menu.locator(".vireo-ctx-chip").count() > 5
    # Key actions present.
    expect(menu.locator(".vireo-ctx-item", has_text="Reveal in")).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Copy Path")).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Delete")).to_be_visible()


def test_right_click_rating_applies(live_server, page):
    """Clicking a rating chip applies the rating to the right-clicked photo."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    target = cards.nth(1)  # use a photo that starts with no rating
    pid = int(target.get_attribute("data-id"))

    target.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    # Click the "3" chip in the rating row.
    menu.locator(".vireo-ctx-chip", has_text="3").first.click()
    expect(menu).to_be_hidden()

    # setRating writes to the server and updates the local photos array after
    # the fetch resolves. Poll the in-memory photo record for the new rating.
    page.wait_for_function(
        f"(photos.find(p => p.id === {pid}) || {{}}).rating === 3",
        timeout=3000,
    )


def test_right_click_outside_selection_replaces_selection(live_server, page):
    """Right-click on an unselected card replaces selection with that card."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 3

    c2_id = int(cards.nth(2).get_attribute("data-id"))

    # Select cards 0 and 1.
    cards.nth(0).click()
    cards.nth(1).click(modifiers=["Meta"])
    # Right-click card 2, which is NOT in selection.
    cards.nth(2).click(button="right")

    expect(page.locator(".vireo-ctx-menu")).to_be_visible()
    # Selection should now be exactly card 2.
    assert page.evaluate("selectedPhotos.size") == 1
    assert page.evaluate(f"selectedPhotos.has({c2_id})") is True


def test_right_click_inside_selection_preserves_multi(live_server, page):
    """Right-click on a card already in the selection keeps the full set."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 3

    c0_id = int(cards.nth(0).get_attribute("data-id"))
    c1_id = int(cards.nth(1).get_attribute("data-id"))

    # Build a 2-item selection via cmd-clicks so selectedPhotos holds both.
    cards.nth(0).click(modifiers=["Meta"])
    cards.nth(1).click(modifiers=["Meta"])
    assert page.evaluate("selectedPhotos.size") == 2

    # Right-click one of the already-selected cards; the set must survive.
    cards.nth(0).click(button="right")

    expect(page.locator(".vireo-ctx-menu")).to_be_visible()
    assert page.evaluate("selectedPhotos.size") == 2
    assert page.evaluate(f"selectedPhotos.has({c0_id})") is True
    assert page.evaluate(f"selectedPhotos.has({c1_id})") is True


def test_right_click_outside_selection_updates_detail(live_server, page):
    """Right-click on an unselected card refreshes the detail side-panel.

    Regression guard: before the fix, coercing selection on right-click would
    update `selectedPhotoId` but not reload the detail panel, leaving the
    panel stuck on the previously-focused photo.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 3

    # Left-click card 0 to focus it and load its detail panel.
    cards.nth(0).click()
    c0_id = int(cards.nth(0).get_attribute("data-id"))
    page.wait_for_function(
        f"window._detailPhotoId === {c0_id}", timeout=3000
    )

    # Right-click a different card that is NOT in the selection.
    c2_id = int(cards.nth(2).get_attribute("data-id"))
    cards.nth(2).click(button="right")
    expect(page.locator(".vireo-ctx-menu")).to_be_visible()

    # Detail panel must now reflect the right-clicked photo.
    page.wait_for_function(
        f"window._detailPhotoId === {c2_id}", timeout=3000
    )


def test_right_click_updates_shift_range_anchor(live_server, page):
    """Right-click coercion must sync selectedIndex so Shift-range uses the
    right-clicked card as the anchor.

    Regression guard: before the fix, right-click set selectedPhotoId but left
    selectedIndex stale, so a subsequent Shift-click would range-select from
    the previously-focused card (or fail if selectedIndex was -1).
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 4

    # Left-click card 0 so selectedIndex = 0.
    cards.nth(0).click()
    assert page.evaluate("selectedIndex") == 0

    # Right-click card 2 — coercion must update the anchor to index 2.
    cards.nth(2).click(button="right")
    expect(page.locator(".vireo-ctx-menu")).to_be_visible()
    assert page.evaluate("selectedIndex") == 2

    # Dismiss the menu via Escape so the outside-click swallower doesn't
    # fire — an outside mousedown would eat the subsequent Shift-click.
    page.keyboard.press("Escape")
    expect(page.locator(".vireo-ctx-menu")).to_be_hidden()

    # Shift-click card 3 — the shift branch of selectPhoto must see the
    # anchor at 2, producing the range [2..3]. A stale anchor at 0 would
    # produce [0..3] and include cards 0 and 1.
    page.keyboard.down("Shift")
    cards.nth(3).click()
    page.keyboard.up("Shift")
    c0_id = int(cards.nth(0).get_attribute("data-id"))
    c1_id = int(cards.nth(1).get_attribute("data-id"))
    c2_id = int(cards.nth(2).get_attribute("data-id"))
    c3_id = int(cards.nth(3).get_attribute("data-id"))
    selected_array = page.evaluate("Array.from(selectedPhotos)")
    assert c2_id in selected_array, f"card 2 (id={c2_id}) missing from selection {selected_array}"
    assert c3_id in selected_array, f"card 3 (id={c3_id}) missing from selection {selected_array}"
    # Cards 0 and 1 must NOT be included — the anchor moved with the right-click.
    assert c0_id not in selected_array, f"stale anchor selected card 0 (id={c0_id}) {selected_array}"
    assert c1_id not in selected_array, f"stale anchor selected card 1 (id={c1_id}) {selected_array}"


def test_copy_path_menu_item_tolerates_missing_paths(live_server, page):
    """Clicking Copy Path must not throw even if the API omits `path`.

    Regression guard for the Promise.allSettled refactor: the old
    Promise.all + .catch would swallow errors silently but a single
    rejection would drop the whole batch. With allSettled, each response
    is evaluated independently and the handler never throws.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    # Grant clipboard permissions so a real writeText would not raise.
    page.context.grant_permissions(["clipboard-read", "clipboard-write"])

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")

    # Collect JS page errors; clicking Copy Path must not surface any.
    errors = []
    page.on("pageerror", lambda exc: errors.append(str(exc)))

    cards.first.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    copy_item = menu.locator(".vireo-ctx-item", has_text="Copy Path")
    expect(copy_item).to_be_visible()
    copy_item.click()

    # Menu closes and no uncaught JS error was raised.
    expect(menu).to_be_hidden()
    # Give any async handler time to settle.
    page.wait_for_timeout(200)
    assert errors == [], f"copyPhotoPaths raised: {errors}"
