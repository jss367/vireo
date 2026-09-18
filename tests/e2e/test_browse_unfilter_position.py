"""Turning filters off keeps the current photo in view across lazy pages."""

import pytest

from e2e.test_browse_filterbar import _open_browse
from e2e.test_browse_sort_keeps_selection import _stall_first_focused_query

ACTIONS = [
    "quick_missing", "quick_flag", "quick_rules", "rating", "color",
    "chip", "rule", "clear", "clear_popover", "clear_api", "pause",
    "api_toggle", "api_remove", "sidebar_keyword", "calendar_clear",
]


def _prepare(page, live_server, action, photo_link=False):
    db = live_server["db"]
    place = db.add_keyword("Park", kw_type="location")
    keyword = db.add_keyword("Portfolio") if action == "sidebar_keyword" else None
    ids = []
    for i in range(180):
        photo_id = db.add_photo(
            folder_id=live_server["data"]["folders"][0],
            filename=f"bird{i:03d}.jpg", extension=".jpg", file_size=1000 + i,
            file_mtime=1.0, timestamp=("2024-05-02T08:00:00"
                if action == "calendar_clear" and i >= 120 else "2024-05-01T08:00:00"),
        )
        ids.append(photo_id)
        if keyword and i >= 120:
            db.tag_photo(photo_id, keyword)
        if i < 120:
            db.tag_photo(photo_id, place)
        elif action == "color":
            db.set_color_label(photo_id, "red")
    with db.conn:
        db.conn.executemany(
            "UPDATE photos SET rating=5, flag='flagged' WHERE id=?",
            [(photo_id,) for photo_id in ids[120:]],
        )
    if action == "quick_rules":
        response = page.request.post(live_server["url"] + "/api/config", data={
            "filter_shortcuts": [{"id": "keepers", "label": "Keepers",
                                  "rules": {"field": "rating", "op": ">=", "value": 5}}],
        })
        assert response.ok
    page.add_init_script("localStorage.clear()")
    if photo_link:
        page.goto(f"{live_server['url']}/browse?photo_id={ids[0]}")
    else:
        _open_browse(page, live_server)
    page.wait_for_function("VireoFilter.isReady() && !loading && browseDatasetReady")
    page.select_option("#sortSelect", "name")
    page.wait_for_function("!loading && browseDatasetReady")
    page.evaluate("updateThumbSize(300)")
    if action == "quick_missing":
        page.click('.vf-shortcuts [data-field="has_location_keyword"]')
    elif action == "quick_flag":
        page.click('.vf-shortcuts [data-value="flagged"]')
    elif action == "quick_rules":
        page.click('.vf-shortcuts [data-shortcut="keepers"]')
    elif action in ("rating", "color"):
        page.click('.vf-filters-btn')
        page.click('.vf-star[data-rating="5"]' if action == "rating"
                   else '.vf-quick-colors [data-color="red"]')
        page.click('.vf-done')
    elif action == "sidebar_keyword":
        page.locator('#keywordTree .tree-item[data-keyword="Portfolio"]').click()
    elif action == "calendar_clear":
        page.evaluate("selectCalendarDay('2024-05-02', 60)")
    else:
        page.evaluate("VireoFilter.addRule('rating', '>=', 5)")
    page.wait_for_function("!loading && browseDatasetReady && totalPhotos >= 60 && totalPhotos < 70")
    return ids


def _turn_off(page, action):
    if action == "quick_missing":
        page.click('.vf-shortcuts [data-field="has_location_keyword"]')
    elif action == "quick_flag":
        page.click('.vf-shortcuts [data-value="flagged"]')
    elif action == "quick_rules":
        page.click('.vf-shortcuts [data-shortcut="keepers"]')
    elif action == "chip":
        page.click('[data-chip-x="0"]')
    elif action == "clear":
        page.click('.vf-clear')
    elif action == "clear_api":
        page.evaluate("VireoFilter.clearAll()")
    elif action == "pause":
        page.click('.vf-mute')
    elif action == "api_toggle":
        page.evaluate("VireoFilter.addRule('rating', '>=', 5)")
    elif action == "api_remove":
        page.evaluate("VireoFilter.removeField('rating')")
    elif action == "sidebar_keyword":
        # The keyword list lives in the summary panel, which a selection's
        # detail panel hides. Exercise its handler directly in that case.
        if page.evaluate("selectedPhotoId != null"):
            page.evaluate("filterByKeyword('Portfolio')")
        else:
            page.locator('#keywordTree .tree-item[data-keyword="Portfolio"]').click()
    elif action == "calendar_clear":
        page.evaluate("clearCalendarSelection()")
    else:
        page.click('.vf-filters-btn')
        selector = {
            "rating": '.vf-star[data-rating="5"]',
            "color": '.vf-quick-colors [data-color="red"]',
            "rule": '.vf-rule-tree [data-action="remove"]',
            "clear_popover": '.vf-clear-rules',
        }[action]
        page.locator(selector).first.click()
        page.click('.vf-done')


@pytest.mark.parametrize("action", ACTIONS)
@pytest.mark.parametrize("selected", [True, False], ids=["selected", "just_browsing"])
def test_turning_filter_off_keeps_photo_in_view(live_server, page, action, selected):
    _check_turning_off(page, live_server, action, selected)


def _check_turning_off(page, live_server, action, selected, photo_link=False):
    ids = _prepare(page, live_server, action, photo_link=photo_link)
    card = page.locator(f'#grid .grid-card[data-id="{ids[145]}"]')
    card.scroll_into_view_if_needed()
    if selected:
        card.click()
    page.wait_for_timeout(150)
    anchor = page.evaluate(
        "captureSelectedPhotoAnchor()" if selected else "captureBrowseViewportAnchor()"
    )
    assert page.evaluate("gridContainer.scrollTop") > 500
    queries = []
    page.on("request", lambda request: queries.append(request.post_data_json)
            if request.url.endswith('/api/photos/query') else None)
    _turn_off(page, action)
    # Photo links scope Browse to the target's folder (the other two seed
    # photos live elsewhere); removing a filter must preserve that scope.
    page.wait_for_function(
        "total => !loading && browseDatasetReady && totalPhotos === total && anchorScanDepth === 0",
        arg=183 if photo_link else 185,
    )
    page.wait_for_function("""anchor => {
      const card = getGridCard(anchor.photoId);
      if (!card) return false;
      const rect = card.getBoundingClientRect(), box = gridContainer.getBoundingClientRect();
      return rect.bottom > box.top && rect.top < box.bottom &&
        Math.abs(rect.top - box.top - anchor.topOffset) < 4;
    }""", arg=anchor)
    assert page.evaluate("selectedPhotoId") == (anchor["photoId"] if selected else None)
    assert page.evaluate("selectedPhotos.size") == 0
    # The target moves several pages into the expanded library. Resolve it
    # directly, including for Clear filters, rather than loading from page 1.
    assert queries[0].get("focus_photo_id") == anchor["photoId"]


@pytest.mark.parametrize("selected", [True, False])
def test_photo_link_keeps_position_when_removing_chip(live_server, page, selected):
    _check_turning_off(page, live_server, "chip", selected, photo_link=True)


def test_offline_viewport_anchor_survives_filter_removal(live_server, page):
    """Dashboard-scoped collections with ``showOfflineCollectionPhotos`` on can
    render the topmost visible card as an offline placeholder. Filter removal
    must keep that anchor in view instead of dropping to page 1 (Codex review
    r4043586005)."""
    ids = _prepare(page, live_server, "quick_flag")
    card = page.locator(f'#grid .grid-card[data-id="{ids[145]}"]')
    card.scroll_into_view_if_needed()
    page.wait_for_timeout(150)
    anchor = page.evaluate("captureBrowseViewportAnchor()")
    assert page.evaluate("gridContainer.scrollTop") > 500
    # Simulate the offline placeholder the reload would return in a dashboard-
    # scoped collection with offline photos on: rewrite the anchor photo so
    # ``browsePhotoIsAvailable`` reads it as offline after the fetch.
    page.evaluate(
        """id => {
          const original = window.browsePhotoIsAvailable;
          window.browsePhotoIsAvailable = function(photo) {
            if (photo && photo.id === id) return false;
            return original(photo);
          };
        }""",
        anchor["photoId"],
    )
    _turn_off(page, "quick_flag")
    page.wait_for_function(
        "!loading && browseDatasetReady && totalPhotos === 185 && anchorScanDepth === 0"
    )
    page.wait_for_function(
        """anchor => {
          const card = getGridCard(anchor.photoId);
          if (!card) return false;
          const rect = card.getBoundingClientRect(), box = gridContainer.getBoundingClientRect();
          return rect.bottom > box.top && rect.top < box.bottom &&
            Math.abs(rect.top - box.top - anchor.topOffset) < 4;
        }""",
        arg=anchor,
    )
    assert page.evaluate("selectedPhotoId") is None


@pytest.mark.parametrize("selected", [True, False])
def test_second_removal_during_reload_keeps_original_photo(live_server, page, selected):
    _stall_first_focused_query(page)
    ids = _prepare(page, live_server, "quick_flag")
    page.evaluate("VireoFilter.addRule('rating', '>=', 5)")
    page.wait_for_function("!loading && browseDatasetReady")
    card = page.locator(f'#grid .grid-card[data-id="{ids[145]}"]')
    card.scroll_into_view_if_needed()
    if selected:
        card.click()
    anchor = page.evaluate(
        "captureSelectedPhotoAnchor()" if selected else "captureBrowseViewportAnchor()"
    )
    _turn_off(page, "quick_flag")
    page.wait_for_function("window.__focusStalled")
    _turn_off(page, "clear")
    page.wait_for_function("!loading && browseDatasetReady && focusReloadInFlight === 0")
    assert page.evaluate("totalPhotos") == 185
    assert page.evaluate("selectedPhotoId") == (anchor["photoId"] if selected else None)
    assert page.evaluate("""id => {
      const card = getGridCard(id).getBoundingClientRect();
      const box = gridContainer.getBoundingClientRect();
      return card.bottom > box.top && card.top < box.bottom;
    }""", anchor["photoId"])


def test_removing_one_of_two_flags_can_exclude_selected_photo(live_server, page):
    ids = _prepare(page, live_server, "quick_flag")
    live_server["db"].batch_update_photo_flag(ids[:120], "rejected")
    page.click('.vf-shortcuts [data-value="rejected"]')
    page.wait_for_function("!loading && browseDatasetReady && totalPhotos === 180")
    # Select a Picked photo that will disappear when only Rejected remains.
    page.evaluate("async () => { while (!allLoaded) await loadPhotos(); }")
    page.locator(f'#grid .grid-card[data-id="{ids[145]}"]').click()
    with page.expect_response(lambda response:
        response.url.endswith('/api/photos/query') and
        response.request.post_data_json.get('focus_photo_id') == ids[145]
    ) as focused:
        _turn_off(page, "quick_flag")
    assert focused.value.json()["focus_index"] is None
    page.wait_for_function("!loading && browseDatasetReady && focusReloadInFlight === 0")
    assert page.evaluate("selectedPhotoId") is None
    assert page.evaluate("totalPhotos") == 120
    assert page.evaluate("photos.length") < 120, "must not scan all results for an excluded photo"


@pytest.mark.parametrize("op", ["in", "not_in"])
@pytest.mark.parametrize("selected", [True, False])
def test_deselecting_advanced_enum_value_keeps_photo_in_view(live_server, page, op, selected):
    ids = _prepare(page, live_server, "quick_flag")
    values = ["none", "flagged" if op == "in" else "rejected"]
    page.evaluate("rule => VireoFilter.loadExpression([rule])", {
        "field": "flag", "op": op, "value": values,
    })
    page.wait_for_function("!loading && browseDatasetReady")
    page.evaluate("async () => { while (!allLoaded) await loadPhotos(); }")
    card = page.locator(f'#grid .grid-card[data-id="{ids[145]}"]')
    card.scroll_into_view_if_needed()
    if selected:
        card.click()
    page.wait_for_timeout(150)
    anchor = page.evaluate(
        "captureSelectedPhotoAnchor()" if selected else "captureBrowseViewportAnchor()"
    )
    page.click('.vf-filters-btn')
    with page.expect_request('**/api/photos/query') as query:
        page.click('.vf-rule-tree [data-action="multi"][data-value="none"]')
    assert query.value.post_data_json.get('focus_photo_id') == anchor['photoId']
    page.click('.vf-done')
    # Removing an excluded value widens the view; removing an included
    # value narrows it. The flagged anchor remains eligible in both cases.
    page.wait_for_function(
        "total => !loading && browseDatasetReady && totalPhotos === total && anchorScanDepth === 0",
        arg=60 if op == "in" else 185,
    )
    assert page.evaluate("selectedPhotoId") == (anchor["photoId"] if selected else None)
    page.wait_for_function("""anchor => {
      const card = getGridCard(anchor.photoId);
      if (!card) return false;
      const rect = card.getBoundingClientRect(), box = gridContainer.getBoundingClientRect();
      return rect.bottom > box.top && rect.top < box.bottom &&
        Math.abs(rect.top - box.top - anchor.topOffset) < 4;
    }""", arg=anchor)



@pytest.mark.parametrize("selected", [True, False])
def test_deleting_free_entry_enum_value_keeps_photo_in_view(live_server, page, selected):
    """Deleting a value from a suggest-backed in/not_in list (File extension)
    goes through handleRuleEdit's multi-text branch. That path must emit
    filterRemoved when the parsed list drops a previously-present value so
    the focused-anchor reload keeps the current photo in view, just like the
    enum-pill removal does for value-backed enums."""
    db = live_server["db"]
    ids = []
    for i in range(180):
        ext = ".jpg" if i < 120 else ".png"
        photo_id = db.add_photo(
            folder_id=live_server["data"]["folders"][0],
            filename=f"bird{i:03d}{ext}", extension=ext, file_size=1000 + i,
            file_mtime=1.0, timestamp="2024-05-01T08:00:00",
        )
        ids.append(photo_id)
    page.add_init_script("localStorage.clear()")
    _open_browse(page, live_server)
    page.wait_for_function("VireoFilter.isReady() && !loading && browseDatasetReady")
    page.select_option("#sortSelect", "name")
    page.wait_for_function("!loading && browseDatasetReady")
    page.evaluate("updateThumbSize(300)")
    page.evaluate("rule => VireoFilter.loadExpression([rule])", {
        "field": "extension", "op": "in", "value": [".jpg", ".png"],
    })
    page.wait_for_function("!loading && browseDatasetReady && totalPhotos === 180")
    page.evaluate("async () => { while (!allLoaded) await loadPhotos(); }")
    # Anchor a .jpg card; dropping .png from the list narrows the results
    # but the anchor stays eligible.
    card = page.locator(f'#grid .grid-card[data-id="{ids[100]}"]')
    card.scroll_into_view_if_needed()
    if selected:
        card.click()
    page.wait_for_timeout(150)
    anchor = page.evaluate(
        "captureSelectedPhotoAnchor()" if selected else "captureBrowseViewportAnchor()"
    )
    page.click('.vf-filters-btn')
    input_selector = '.vf-rule-tree input[data-action="multi-text"]'
    page.locator(input_selector).first.wait_for()
    with page.expect_request('**/api/photos/query') as query:
        # Simulate a delete-through-typing edit: the user removes ".png"
        # from the comma-separated list, then the debounced input handler
        # commits the shrunk list.
        page.evaluate(
            """selector => {
              const input = document.querySelector(selector);
              input.value = '.jpg';
              input.dispatchEvent(new Event('input', {bubbles: true}));
            }""",
            input_selector,
        )
        page.wait_for_timeout(320)
    assert query.value.post_data_json.get('focus_photo_id') == anchor['photoId']
    page.click('.vf-done')
    page.wait_for_function(
        "!loading && browseDatasetReady && totalPhotos === 120 && anchorScanDepth === 0",
    )
    assert page.evaluate("selectedPhotoId") == (anchor["photoId"] if selected else None)
    page.wait_for_function("""anchor => {
      const card = getGridCard(anchor.photoId);
      if (!card) return false;
      const rect = card.getBoundingClientRect(), box = gridContainer.getBoundingClientRect();
      return rect.bottom > box.top && rect.top < box.bottom &&
        Math.abs(rect.top - box.top - anchor.topOffset) < 4;
    }""", arg=anchor)


def test_narrowing_or_rule_that_excludes_viewport_anchor_holds_position(live_server, page):
    """When there is no selection and a removal narrows an OR-style rule so
    the captured top card is itself filtered out, Browse falls back to a
    neighbour at the same result index rather than resetting to page 1
    (Codex review r4043606240)."""
    db = live_server["db"]
    ids = []
    for i in range(180):
        photo_id = db.add_photo(
            folder_id=live_server["data"]["folders"][0],
            filename=f"bird{i:03d}.jpg", extension=".jpg", file_size=1000 + i,
            file_mtime=1.0, timestamp="2024-05-01T08:00:00",
        )
        ids.append(photo_id)
    # First 90 rejected, last 90 flagged: the anchored rejected card
    # disappears when only ``flagged`` remains.
    with db.conn:
        db.conn.executemany(
            "UPDATE photos SET flag='rejected' WHERE id=?", [(i,) for i in ids[:90]]
        )
        db.conn.executemany(
            "UPDATE photos SET flag='flagged' WHERE id=?", [(i,) for i in ids[90:]]
        )
    page.add_init_script("localStorage.clear()")
    _open_browse(page, live_server)
    page.wait_for_function("VireoFilter.isReady() && !loading && browseDatasetReady")
    page.select_option("#sortSelect", "name")
    page.wait_for_function("!loading && browseDatasetReady")
    page.evaluate("updateThumbSize(300)")
    page.evaluate("rule => VireoFilter.loadExpression([rule])", {
        "field": "flag", "op": "in", "value": ["flagged", "rejected"],
    })
    page.wait_for_function("!loading && browseDatasetReady && totalPhotos === 180")
    # Anchor on a rejected card WITHOUT selecting it: viewportOnly path.
    card = page.locator(f'#grid .grid-card[data-id="{ids[45]}"]')
    card.scroll_into_view_if_needed()
    page.wait_for_timeout(150)
    assert page.evaluate("selectedPhotoId") is None
    scroll_before = page.evaluate("gridContainer.scrollTop")
    assert scroll_before > 200
    page.click('.vf-filters-btn')
    page.click('.vf-rule-tree [data-action="multi"][data-value="rejected"]')
    page.click('.vf-done')
    page.wait_for_function(
        "!loading && browseDatasetReady && anchorScanDepth === 0 && totalPhotos === 90",
    )
    # Anchor card was filtered out — with the fallback fix the grid holds
    # a nearby result index instead of snapping back to scrollTop 0.
    assert page.evaluate("selectedPhotoId") is None
    assert page.evaluate("gridContainer.scrollTop") > 0


def test_folder_handoff_does_not_inherit_keyword_selection(live_server, page):
    ids = _prepare(page, live_server, "sidebar_keyword")
    page.locator(f'#grid .grid-card[data-id="{ids[145]}"]').click()
    folder_id = live_server["data"]["folders"][0]
    # The chosen folder still contains the selected photo, but this action
    # opens a new scope and intentionally starts at the beginning.
    with page.expect_request('**/api/photos/query') as query:
        page.locator(f'#folderTree .tree-item[data-folder-id="{folder_id}"]').click()
    assert 'focus_photo_id' not in query.value.post_data_json
    assert query.value.post_data_json['folder_id'] == folder_id
    page.wait_for_function("!loading && browseDatasetReady")
    assert page.evaluate("selectedPhotoId") is None
    assert page.evaluate("gridContainer.scrollTop") == 0
    assert page.evaluate("VireoFilter.hasFilters()") is False
