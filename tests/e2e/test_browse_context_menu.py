import re

from playwright.sync_api import expect

_PNG_1X1 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
    "/x8AAwMCAO+/p9sAAAAASUVORK5CYII="
)


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
    expect(menu.get_by_text("View on Map", exact=True)).to_be_visible()
    expect(menu.get_by_text("Copy Development Settings", exact=True)).to_be_visible()
    expect(menu.get_by_text("Paste Development Settings", exact=True)).to_be_visible()


def test_browse_labels_lightbox_adjustments_as_quick_adjust(live_server, page):
    page.goto(f"{live_server['url']}/browse")

    expect(page.locator("#lightboxAdjustBtn")).to_have_text("Quick Adjust")


def test_right_click_copies_and_pastes_development_settings(
    live_server, page
):
    """A grid photo's saved recipe can be copied onto selected targets."""
    page.goto(f"{live_server['url']}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 3
    alternate_id = int(cards.nth(0).get_attribute("data-id"))
    source_id = int(cards.nth(1).get_attribute("data-id"))
    target_ids = [
        alternate_id,
        int(cards.nth(2).get_attribute("data-id")),
    ]

    alternate_response = page.request.put(
        f"{live_server['url']}/api/photos/{alternate_id}/edit-recipe",
        data={"recipe": {"adjustments": {"exposure": -0.5}}},
    )
    response = page.request.put(
        f"{live_server['url']}/api/photos/{source_id}/edit-recipe",
        data={"recipe": {"adjustments": {"exposure": 1.5}}},
    )
    assert alternate_response.ok
    assert response.ok

    # The recipes were added after Browse loaded, so its in-memory photo rows
    # are stale. Copy must stay available and fetch the exact right-clicked
    # photo's authoritative recipe, not the first item in the selection.
    cards.nth(0).click(modifiers=["Meta"])
    cards.nth(1).click(modifiers=["Meta"])
    cards.nth(1).click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    copy_item = menu.get_by_text("Copy Development Settings", exact=True)
    expect(copy_item).not_to_have_class(re.compile(r"vireo-ctx-disabled"))
    copy_item.click()
    page.wait_for_function(
        """() => window.vireoEditNav.getCopiedRecipe()?.recipe
          ?.adjustments?.exposure === 1.5"""
    )

    # Reload to clear the source selection while retaining the shared recipe
    # clipboard, then select both targets and paste from their context menu.
    page.reload()
    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    cards.nth(0).click(modifiers=["Meta"])
    cards.nth(2).click(modifiers=["Meta"])
    cards.nth(0).click(button="right")
    paste_item = page.locator(".vireo-ctx-menu").get_by_text(
        "Paste Development Settings", exact=True
    )
    expect(paste_item).not_to_have_class(re.compile(r"vireo-ctx-disabled"))
    paste_item.click()
    page.get_by_role('dialog').get_by_role('button', name='Apply selected settings').click()

    for target_id in target_ids:
        page.wait_for_function(
            """async photoId => {
              const response = await fetch('/api/photos/' + photoId + '/edit-recipe');
              const data = await response.json();
              return data.recipe?.adjustments?.exposure === 1.5;
            }""",
            arg=target_id,
        )


def test_latest_development_settings_copy_wins(live_server, page):
    """An older, slower recipe response cannot replace the latest copy."""
    page.goto(f"{live_server['url']}/browse")
    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    photo_ids = [
        int(cards.nth(0).get_attribute("data-id")),
        int(cards.nth(1).get_attribute("data-id")),
    ]

    copied_exposure = page.evaluate(
        """async photoIds => {
          const originalSafeFetch = window.safeFetch;
          const pending = {};
          window.safeFetch = function(url, options, config) {
            const match = url.match(/^[/]api[/]photos[/]([0-9]+)[/]edit-recipe$/);
            if (match) {
              return new Promise(resolve => { pending[match[1]] = resolve; });
            }
            return originalSafeFetch(url, options, config);
          };
          try {
            const older = copyDevelopmentSettingsFromPhoto(photoIds[0]);
            const latest = copyDevelopmentSettingsFromPhoto(photoIds[1]);
            while (!pending[String(photoIds[0])] || !pending[String(photoIds[1])]) {
              await new Promise(resolve => setTimeout(resolve, 0));
            }
            pending[String(photoIds[1])]({
              recipe: {adjustments: {exposure: 1.5}},
            });
            await latest;
            pending[String(photoIds[0])]({
              recipe: {adjustments: {exposure: -0.5}},
            });
            await older;
            return window.vireoEditNav.getCopiedRecipe().recipe.adjustments.exposure;
          } finally {
            window.safeFetch = originalSafeFetch;
          }
        }""",
        photo_ids,
    )

    assert copied_exposure == 1.5


def test_later_cross_tab_development_settings_copy_wins(live_server, page):
    """A pending Browse fetch cannot overwrite a newer cross-tab copy."""
    page.goto(f"{live_server['url']}/browse")
    card = page.locator(".grid-card").first
    card.wait_for(state="visible")
    photo_id = int(card.get_attribute("data-id"))
    other_page = page.context.new_page()
    try:
        other_page.goto(f"{live_server['url']}/browse")
        other_page.locator(".grid-card").first.wait_for(state="visible")
        page.evaluate(
            """photoId => {
              const originalSafeFetch = window.safeFetch;
              window.__originalCopySafeFetch = originalSafeFetch;
              window.safeFetch = function(url, options, config) {
                if (url === '/api/photos/' + photoId + '/edit-recipe') {
                  return new Promise(resolve => { window.__resolveOldCopy = resolve; });
                }
                return originalSafeFetch(url, options, config);
              };
              window.__oldCopyPromise = copyDevelopmentSettingsFromPhoto(photoId);
            }""",
            photo_id,
        )
        page.wait_for_function("() => typeof window.__resolveOldCopy === 'function'")
        other_page.evaluate(
            """() => window.vireoEditNav.setCopiedRecipe(
              {adjustments: {exposure: 2.0}},
              {source: 'newer-tab.jpg', at: Date.now()}
            )"""
        )
        page.evaluate(
            """async () => {
              window.__resolveOldCopy({
                recipe: {adjustments: {exposure: -1.0}},
              });
              await window.__oldCopyPromise;
              window.safeFetch = window.__originalCopySafeFetch;
            }"""
        )

        assert page.evaluate(
            "() => window.vireoEditNav.getCopiedRecipe().recipe.adjustments.exposure"
        ) == 2.0
        assert other_page.evaluate(
            "() => window.vireoEditNav.getCopiedRecipe().source"
        ) == "newer-tab.jpg"
    finally:
        other_page.close()


def test_development_settings_paste_blocks_overlapping_requests(
    live_server, page
):
    page.goto(f"{live_server['url']}/browse")
    card = page.locator(".grid-card").first
    card.wait_for(state="visible")
    photo_id = int(card.get_attribute("data-id"))

    pending_state = page.evaluate(
        """async photoId => {
          await window.vireoEditNav.setCopiedRecipe({adjustments: {exposure: 1.0}});
          selectedPhotos.clear();
          selectedPhotos.add(photoId);
          selectedPhotoId = photoId;
          const originalSafeFetch = window.safeFetch;
          window.__originalPasteSafeFetch = originalSafeFetch;
          window.__pasteRequestCount = 0;
          window.safeFetch = function(url, options, config) {
            if (url === '/api/photos/edit-recipe/apply') {
              window.__pasteRequestCount += 1;
              return new Promise(resolve => { window.__resolvePaste = resolve; });
            }
            return originalSafeFetch(url, options, config);
          };
          window.__pastePromise = pasteEditSettingsToSelection();
          pasteEditSettingsToSelection();
          const pasteItem = buildPhotoContextMenu([photoId], photoId)
            .find(item => item.label === 'Paste Development Settings');
          return {
            requestCount: window.__pasteRequestCount,
            inFlight: _browseDevelopmentPasteInFlight,
            menuDisabled: pasteItem.disabled,
            menuHint: pasteItem.disabledHint,
          };
        }""",
        photo_id,
    )
    assert pending_state == {
        "requestCount": 0,
        "inFlight": True,
        "menuDisabled": True,
        "menuHint": "A development settings paste is already running",
    }

    page.get_by_role('dialog').get_by_role('button', name='Apply selected settings').click()
    page.wait_for_function('window.__pasteRequestCount === 1')
    page.evaluate(
        """async () => {
          window.__resolvePaste({applied: [], skipped: [], count: 0, recipes: {}});
          await window.__pastePromise;
          window.safeFetch = window.__originalPasteSafeFetch;
        }"""
    )
    assert page.evaluate("() => _browseDevelopmentPasteInFlight") is False


def test_view_on_map_context_action_targets_right_clicked_photo(live_server, page):
    """Browse's right-click View on Map action targets the context photo."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    pid = int(cards.nth(1).get_attribute("data-id"))

    page.evaluate("window.viewPhotoOnMap = id => { window.__mapTarget = id; }")
    cards.nth(1).click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    menu.get_by_text("View on Map", exact=True).click()
    assert page.evaluate("window.__mapTarget") == pid


def test_browse_selection_opens_burst_review(live_server, page):
    """Selected Browse photos can be opened as a temporary burst review group."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 2

    cards.nth(0).click(modifiers=["Meta"])
    cards.nth(1).click(modifiers=["Meta"])
    assert page.evaluate("selectedPhotos.size") == 2

    expect(page.locator("#burstReviewBtn")).to_be_visible()
    page.locator("#burstReviewBtn").click()

    page.wait_for_function(
        """() => location.pathname === '/pipeline/review'
          && document.querySelector('#grmOverlay.open')
          && document.querySelector('#grmTitle')?.textContent === 'Review Selected Photos'""",
        timeout=5000,
    )
    expect(page.locator("#grmCount")).to_contain_text("unsorted")


def test_browse_selection_review_marks_the_active_photos_eye(live_server, page):
    """The eye marker follows the active photo within a selected review set."""
    page.goto(f"{live_server['url']}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    photo_ids = [int(cards.nth(i).get_attribute("data-id")) for i in range(2)]
    db = live_server["db"]
    db.conn.execute(
        "UPDATE photos SET eye_x = 0.2, eye_y = 0.4, eye_conf = 0.98 WHERE id = ?",
        (photo_ids[0],),
    )
    db.conn.execute(
        "UPDATE photos SET eye_x = 0.8, eye_y = 0.4, eye_conf = 0.98 WHERE id = ?",
        (photo_ids[1],),
    )
    db.conn.commit()

    cards.nth(0).click(modifiers=["Meta"])
    cards.nth(1).click(modifiers=["Meta"])
    page.locator("#burstReviewBtn").click()
    page.wait_for_function(
        "() => document.querySelector('#grmOverlay.open')",
        timeout=5000,
    )
    # The overlay opens before /group/state finishes seeding its selection.
    # Wait before clearing it, or a late seed can reselect the first photo
    # and turn the click below into a deselection (leaving the loupe empty).
    page.wait_for_function("() => grmState.seeded")
    page.evaluate(
        "png => { window.grmPhotoUrl = () => 'data:image/png;base64,' + png; }",
        _PNG_1X1,
    )
    page.evaluate(
        """() => {
          grmState.selected = null;
          grmState.selectedIds.clear();
          grmState.selectionAnchor = null;
          renderGroupModal();
          grmRefreshSelectedLoupe();
        }"""
    )

    first = page.locator(
        f'#grmOverlay .grm-card[data-photo-id="{photo_ids[0]}"]'
    )
    second = page.locator(
        f'#grmOverlay .grm-card[data-photo-id="{photo_ids[1]}"]'
    )
    first.click()
    marker = page.locator("#grmSelectedEyeCrosshair")
    page.wait_for_function(
        """() => {
          const img = document.getElementById('grmLoupePhoto');
          return img.complete && img.naturalWidth > 0;
        }"""
    )
    expect(marker).to_be_visible()
    first_left = marker.evaluate("el => parseFloat(el.style.left)")

    second.click(modifiers=["Meta"])
    page.wait_for_function(
        "left => parseFloat(document.getElementById('grmSelectedEyeCrosshair').style.left) !== left",
        arg=first_left,
    )
    assert page.evaluate("grmState.selectedIds.size") == 2
    assert marker.evaluate("el => parseFloat(el.style.left)") > first_left

    loupe_box = page.locator("#grmLoupeImg").bounding_box()
    assert loupe_box is not None
    drag_x = loupe_box["x"] + loupe_box["width"] / 2
    drag_y = loupe_box["y"] + loupe_box["height"] / 2
    page.mouse.move(drag_x, drag_y)
    before_drag = marker.evaluate("el => parseFloat(el.style.left)")
    page.mouse.down()
    page.mouse.move(drag_x + 24, drag_y)
    page.mouse.up()
    after_drag = marker.evaluate("el => parseFloat(el.style.left)")
    assert after_drag > before_drag + 20

    page.evaluate(
        """() => {
          const active = grmState.items.find(item => item.id === grmState.selected);
          active.edit_recipe = {rotation: 90};
          grmUpdateSelectedEyeCrosshair();
        }"""
    )
    expect(marker).to_be_hidden()


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
