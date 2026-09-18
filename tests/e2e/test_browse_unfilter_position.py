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


def test_clearing_filters_keeps_offline_placeholder_in_view(live_server, page):
    ids = _prepare(page, live_server, "clear")
    db = live_server["db"]
    with db.conn:
        db.conn.execute("UPDATE folders SET status='missing' WHERE id=?",
                        (live_server["data"]["folders"][0],))
    collection_id = next(c['id'] for c in db.get_collections() if c['name'] == 'All Photos')
    page.goto(f"{live_server['url']}/browse?collection_id={collection_id}&dashboard_scope=1&rating_min=5")
    page.wait_for_function("VireoFilter.isReady() && !loading && browseDatasetReady")
    page.click('#offlineCollectionToggle')
    page.wait_for_function("!loading && browseDatasetReady && totalPhotos === 60")
    page.evaluate("updateThumbSize(300)")
    page.locator(f'#grid .grid-card.offline[data-id="{ids[145]}"]').scroll_into_view_if_needed()
    anchor = page.evaluate("captureBrowseViewportAnchor()")
    assert page.evaluate("gridContainer.scrollTop") > 500
    with page.expect_request('**/api/photos/query') as query:
        page.click('.vf-clear')
    assert query.value.post_data_json['include_offline'] is True
    assert query.value.post_data_json['collection_id'] == collection_id
    assert query.value.post_data_json['focus_photo_id'] == anchor['photoId']
    page.wait_for_function("!loading && browseDatasetReady && anchorScanDepth === 0 && totalPhotos === 185")
    page.wait_for_function("""anchor => {
      const card = getGridCard(anchor.photoId);
      if (!card || !card.classList.contains('offline')) return false;
      const rect = card.getBoundingClientRect(), box = gridContainer.getBoundingClientRect();
      return rect.bottom > box.top && rect.top < box.bottom &&
        Math.abs(rect.top - box.top - anchor.topOffset) < 4;
    }""", arg=anchor, timeout=3000)
    assert page.evaluate("selectedPhotoId") is None
    assert page.evaluate("getActiveSelection()") == []
