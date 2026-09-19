"""E2E: tagging from Browse must not throw the user back to the top.

Adding a keyword re-evaluated the active filter expression so photos that no
longer match could leave the grid. That reload restarted Browse at page 1 with
``scrollTop = 0`` — and it ran for *any* active filter, so a user tagging their
way down a rating-filtered grid lost their place after every single tag.

Two rules are checked here: a filter the edit cannot move is not reloaded at
all, and a filter it can move is reloaded in place.
"""

import pytest
from playwright.sync_api import expect

from e2e.stack_seed import seed_browse_stack


def _seed_filtered_library(db, folder_id, count=160):
    """Photos that all match one keyword, so a keyword filter is scrollable."""
    kw = db.add_keyword("Marsh")
    for i in range(count):
        pid = db.add_photo(
            folder_id=folder_id,
            filename=f"marsh{i:03d}.jpg",
            extension=".jpg",
            file_size=1000,
            file_mtime=1.0,
            timestamp=f"2024-05-01T{i // 60:02d}:{i % 60:02d}:00",
        )
        db.tag_photo(pid, kw)
    return kw


def _open_filtered_browse(page, live_server, field, op, value):
    page.goto(live_server["url"] + "/browse")
    page.wait_for_selector("#grid .grid-card", timeout=15000)
    page.wait_for_function("window.VireoFilter && VireoFilter.isReady()", timeout=15000)
    page.evaluate(
        "args => VireoFilter.addRule(args[0], args[1], args[2])",
        [field, op, value],
    )
    page.wait_for_function(
        "() => photos.length > 0 && !loading && browseDatasetReady", timeout=15000
    )


def _scroll_and_select(page, target=1800):
    """Scroll into the library, let it settle, then select a visible photo."""
    page.evaluate(
        "top => { document.getElementById('gridContainer').scrollTop = top; }", target
    )
    page.wait_for_timeout(1200)
    page.wait_for_function("() => !loading", timeout=15000)

    card = page.locator("#grid .grid-card").nth(30)
    card.scroll_into_view_if_needed()
    page.wait_for_timeout(400)
    card.click()
    expect(page.locator("#addKeywordInput")).to_be_visible()
    scroll_top = page.evaluate("document.getElementById('gridContainer').scrollTop")
    assert scroll_top > 400, "test needs a scrolled grid"
    return scroll_top


def _add_keyword_to_selection(page, name):
    page.locator("#addKeywordInput").fill(name)
    page.locator("#addKeywordInput").press("Enter")
    page.wait_for_function(
        "name => Array.from(document.querySelectorAll('#keywordTree .tree-item'))"
        "  .some(el => el.dataset.keyword === name)",
        arg=name,
        timeout=15000,
    )
    page.wait_for_timeout(800)
    page.wait_for_function("() => !loading", timeout=15000)


def test_keyword_add_keeps_scroll_position_under_keyword_filter(live_server, page):
    """A keyword filter does have to reload — but not back to the top."""
    _seed_filtered_library(live_server["db"], live_server["data"]["folders"][0])
    _open_filtered_browse(page, live_server, "keyword", "is", "Marsh")
    before = _scroll_and_select(page)

    queries = []
    page.on("request", lambda r: queries.append(r.url) if "/api/photos/query" in r.url else None)
    _add_keyword_to_selection(page, "Golden Hour")

    assert queries, "a keyword filter must be re-evaluated after a tag"
    after = page.evaluate("document.getElementById('gridContainer').scrollTop")
    assert abs(after - before) < 80, f"scrolled from {before} to {after}"


def test_keyword_add_does_not_reload_an_unrelated_filter(live_server, page):
    """A rating filter cannot be moved by a keyword, so nothing is re-queried."""
    _seed_filtered_library(live_server["db"], live_server["data"]["folders"][0])
    _open_filtered_browse(page, live_server, "rating", ">=", 0)
    before = _scroll_and_select(page)

    queries = []
    page.on("request", lambda r: queries.append(r.url) if "/api/photos/query" in r.url else None)
    _add_keyword_to_selection(page, "Golden Hour")

    assert not queries, f"unrelated filter was re-queried: {queries}"
    after = page.evaluate("document.getElementById('gridContainer').scrollTop")
    assert abs(after - before) < 80, f"scrolled from {before} to {after}"
    assert page.evaluate("selectedPhotoId") is not None, "selection was dropped"


def test_untagging_the_filtered_keyword_holds_the_position(live_server, page):
    """The anchored photo itself can leave the grid; hold its place anyway."""
    _seed_filtered_library(live_server["db"], live_server["data"]["folders"][0])
    _open_filtered_browse(page, live_server, "keyword", "is", "Marsh")
    before = _scroll_and_select(page)

    # Remove the keyword the filter is built on from the selected photo.
    page.locator("#detailKeywords .keyword-tag", has_text="Marsh").locator(
        ".remove-kw"
    ).click()
    page.wait_for_timeout(1000)
    page.wait_for_function("() => !loading", timeout=15000)

    after = page.evaluate("document.getElementById('gridContainer').scrollTop")
    assert abs(after - before) < 200, f"scrolled from {before} to {after}"


def _watch_grid_frames(page):
    page.evaluate("""() => {
      window.gridFrames = [];
      window.watchGridFrames = true;
      window.retainedThumbnail = document.querySelectorAll('#grid .grid-card img')[35];
      function sample() {
        gridFrames.push({top: gridContainer.scrollTop,
          count: document.querySelectorAll('#grid .grid-card').length});
        if (watchGridFrames) requestAnimationFrame(sample);
      }
      sample();
    }""")


def _assert_grid_never_jumped(page, before):
    frames = page.evaluate("() => { watchGridFrames = false; return gridFrames; }")
    assert len(frames) > 3, "must observe the pending refresh across browser frames"
    assert all(frame["count"] > 0 for frame in frames), "grid was temporarily emptied"
    assert all(abs(frame["top"] - before) < 2 for frame in frames), frames
    assert page.evaluate("retainedThumbnail.isConnected"), "unchanged thumbnail was replaced"


@pytest.mark.parametrize("sort", ["date", "prediction_confidence", "prediction_confidence_asc"])
@pytest.mark.parametrize("scope", ["keyword", "folder"])
def test_accept_on_all_keeps_selection_for_removing_another_keyword(live_server, page, sort, scope):
    db = live_server["db"]
    _seed_filtered_library(db, live_server["data"]["folders"][0])
    old_keyword = db.add_keyword("Needs identification")
    photo_ids = [row[0] for row in db.conn.execute(
        "SELECT id FROM photos WHERE filename LIKE 'marsh%'"
    )]
    for photo_id in photo_ids:
        db.tag_photo(photo_id, old_keyword)
        detection = db.save_detections(photo_id, [{
            "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
            "confidence": 0.95, "category": "animal",
        }], detector_model="test-detector")[0]
        db.add_prediction(detection_id=detection, species="Red-tailed Hawk",
                          confidence=0.92, model="BioCLIP-2")

    if scope == "keyword":
        _open_filtered_browse(page, live_server, "keyword", "is", "Marsh")
    else:
        page.add_init_script("localStorage.clear()")
        page.goto(live_server["url"] + "/browse")
        page.wait_for_function("!loading && browseDatasetReady && VireoFilter.isReady()")
    page.locator("#sortSelect").select_option(sort)
    page.wait_for_function("!loading && browseDatasetReady")
    if scope == "folder":
        folder_id = live_server["data"]["folders"][0]
        page.locator(f'#folderTree .tree-item[data-folder-id="{folder_id}"]').click()
        page.wait_for_function(
            "id => !loading && browseDatasetReady && activeFolderId === id", arg=folder_id,
        )
        # Clicking a folder keeps the previous sort, including confidence.
        expect(page.locator("#sortSelect")).to_have_value(sort)
        assert page.evaluate("VireoFilter.hasFilters()") is False
    _scroll_and_select(page)
    page.locator("#grid .grid-card").nth(31).click(modifiers=["ControlOrMeta"])
    selected = page.evaluate("getActiveSelection()")
    assert len(selected) == 2
    before = page.evaluate("gridContainer.scrollTop")
    row = page.locator("#selectionPredictions .prediction-row").filter(has_text="Red-tailed Hawk")
    expect(row.get_by_role("button", name="Accept on all", exact=True)).to_be_visible()
    _watch_grid_frames(page)

    with page.expect_response("**/api/predictions/batch-accept") as accepted:
        row.get_by_role("button", name="Accept on all", exact=True).click()
    assert accepted.value.ok
    page.wait_for_function(
        "ids => ids.every(id => { const p = findBrowsePhoto(id);"
        " return p && p.species.includes('Red-tailed Hawk'); })",
        arg=selected,
    )
    page.wait_for_timeout(400)
    page.wait_for_function("!loading")
    assert page.evaluate("getActiveSelection()") == selected
    _assert_grid_never_jumped(page, before)

    # The follow-up action must remain available on the same selection.
    keyword_row = page.locator("#selectionKeywordSuggestions .selection-keyword-row").filter(
        has_text="Needs identification"
    )
    with page.expect_response("**/api/batch/keyword-remove") as removed:
        keyword_row.get_by_role("button", name="Remove from 2", exact=True).click()
    assert removed.value.ok
    expect(keyword_row).to_have_count(0)
    page.wait_for_timeout(400)
    page.wait_for_function("!loading")
    assert page.evaluate("getActiveSelection()") == selected
    assert abs(page.evaluate("gridContainer.scrollTop") - before) < 2
    if scope == "folder":
        assert page.evaluate("activeFolderId") == folder_id
    for photo_id in selected:
        names = {row[0] for row in db.conn.execute(
            "SELECT k.name FROM keywords k JOIN photo_keywords pk ON pk.keyword_id = k.id "
            "WHERE pk.photo_id = ?", (photo_id,),
        )}
        assert "Red-tailed Hawk" in names
        assert "Needs identification" not in names


@pytest.mark.parametrize("batch", [False, True], ids=["single", "batch"])
def test_adding_species_removes_missing_photos_without_any_jump(live_server, page, batch):
    _seed_filtered_library(live_server["db"], live_server["data"]["folders"][0])
    _open_filtered_browse(page, live_server, "has_species", "is", 0)
    _scroll_and_select(page)
    if batch:
        page.locator('#grid .grid-card').nth(31).click(modifiers=["ControlOrMeta"])
    removed_ids = page.evaluate("getActiveSelection()")
    before = page.evaluate("gridContainer.scrollTop")
    initial_total = page.evaluate("totalPhotos")
    _watch_grid_frames(page)

    # Hold the server response so the old reset-and-restore implementation
    # cannot hide its intermediate jump behind a fast local response.
    pending = []
    page.route("**/api/photos/query", lambda route: pending.append(route), times=1)
    page.locator("#addKeywordInput").fill("American Robin")
    with page.expect_request("**/api/photos/query"):
        page.locator("#addKeywordInput").press("Enter")
    page.wait_for_timeout(200)
    assert pending
    assert page.evaluate("gridContainer.scrollTop") == before
    for photo_id in removed_ids:
        expect(page.locator(f'#grid .grid-card[data-id="{photo_id}"]')).to_have_count(1)

    for route in pending:
        route.continue_()
    page.wait_for_function(
        "ids => !loading && ids.every(id => !photos.some(p => p.id === id))",
        arg=removed_ids,
    )
    page.wait_for_timeout(200)
    _assert_grid_never_jumped(page, before)
    assert page.evaluate("totalPhotos") == initial_total - len(removed_ids)
    assert page.evaluate("getActiveSelection()") == []
    assert page.evaluate("photos.map(p => p.id)") == page.locator("#grid .grid-card").evaluate_all(
        "cards => cards.map(card => Number(card.dataset.id))"
    )


def test_membership_refresh_keeps_scrolling_done_while_waiting(live_server, page):
    _seed_filtered_library(live_server["db"], live_server["data"]["folders"][0])
    _open_filtered_browse(page, live_server, "keyword", "is", "Marsh")
    _scroll_and_select(page)
    page.evaluate("async () => { while (!allLoaded) await loadPhotos(); }")
    pending = []
    page.route("**/api/photos/query", lambda route: pending.append(route), times=1)
    page.evaluate("() => { window.refreshDone = resetAndLoad({preserveScroll: true}); }")
    page.wait_for_timeout(200)
    assert pending
    # Move beyond the pages needed when the request started. The staged
    # refresh must extend to this new viewport before committing.
    page.locator("#grid .grid-card").nth(125).scroll_into_view_if_needed()
    before = page.evaluate("gridContainer.scrollTop")
    _watch_grid_frames(page)
    page.wait_for_timeout(100)
    for route in pending:
        route.continue_()
    assert page.evaluate("refreshDone") is True
    page.wait_for_timeout(150)
    _assert_grid_never_jumped(page, before)


def test_failed_membership_refresh_keeps_the_existing_grid(live_server, page):
    _seed_filtered_library(live_server["db"], live_server["data"]["folders"][0])
    _open_filtered_browse(page, live_server, "keyword", "is", "Marsh")
    before = _scroll_and_select(page)
    original_ids = page.evaluate("photos.map(p => p.id)")
    original_selection = page.evaluate("selectedPhotoId")
    _watch_grid_frames(page)
    page.route("**/api/photos/query", lambda route: route.fulfill(
        status=503, content_type="application/json", body='{"error":"Unavailable"}'
    ))
    assert page.evaluate("resetAndLoad({preserveScroll: true})") is False
    page.wait_for_timeout(150)
    _assert_grid_never_jumped(page, before)
    assert page.evaluate("photos.map(p => p.id)") == original_ids
    assert page.evaluate("selectedPhotoId") == original_selection
    assert page.evaluate("loading") is False


def test_membership_refresh_cannot_replace_a_new_filter(live_server, page):
    _seed_filtered_library(live_server["db"], live_server["data"]["folders"][0])
    _open_filtered_browse(page, live_server, "keyword", "is", "Marsh")
    _scroll_and_select(page)
    pending = []
    page.route("**/api/photos/query", lambda route: pending.append(route), times=1)
    page.evaluate("() => { window.refreshDone = resetAndLoad({preserveScroll: true}); }")
    page.wait_for_timeout(200)
    assert pending
    response = pending[0].fetch()
    page.evaluate("() => { VireoFilter.clearAll(true); VireoFilter.addRule('rating', '>=', 4); }")
    page.wait_for_function("!loading && photos.length === 1 && photos[0].rating === 4")
    expected = page.evaluate("photos.map(p => p.id)")
    pending[0].fulfill(response=response)
    assert page.evaluate("refreshDone") is None
    assert page.evaluate("photos.map(p => p.id)") == expected
    assert page.evaluate("selectedPhotoId") is None


def test_membership_refresh_preserves_expanded_stack_and_member_selection(live_server, page):
    db = live_server["db"]
    _seed_filtered_library(db, live_server["data"]["folders"][0])
    ids = [row[0] for row in db.conn.execute(
        "SELECT id FROM photos WHERE filename LIKE 'marsh%' ORDER BY timestamp DESC LIMIT 3 OFFSET 30"
    )]
    seed_browse_stack(db, ids)
    _open_filtered_browse(page, live_server, "keyword", "is", "Marsh")
    page.locator("#browseStacksToggle").check()
    page.wait_for_function("!loading && browseDatasetReady")
    page.evaluate("""async () => {
      while (!photos.some(p => p.browse_stack) && !allLoaded) await loadPhotos();
    }""")
    cover = page.locator("#grid .has-browse-stack").first
    cover.scroll_into_view_if_needed()
    cover_id = int(cover.get_attribute("data-id"))
    page.evaluate("id => toggleBrowseStack(null, id)", cover_id)
    member_id = next(photo_id for photo_id in ids if photo_id != cover_id)
    member = page.locator(f'.browse-stack-member[data-id="{member_id}"]')
    member.click()
    page.evaluate("() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))")
    before = page.evaluate("gridContainer.scrollTop")
    _watch_grid_frames(page)
    _add_keyword_to_selection(page, "Golden Hour")
    _assert_grid_never_jumped(page, before)
    expect(member).to_have_class("browse-stack-member selected")
    assert page.evaluate("selectedPhotoId") == member_id
    assert page.evaluate("id => expandedBrowseStacks.has(id)", cover_id)
    assert sorted(page.evaluate("id => browseStackMembers[id].map(p => p.id)", cover_id)) == sorted(ids)


def test_membership_refresh_keeps_a_focused_window(live_server, page):
    db = live_server["db"]
    _seed_filtered_library(db, live_server["data"]["folders"][0], count=300)
    target = db.conn.execute("SELECT id FROM photos WHERE filename = 'marsh150.jpg'").fetchone()[0]
    page.goto(live_server["url"] + f"/browse?photo_id={target}")
    page.wait_for_function("!loading && browseDatasetReady && earliestPage > 1")
    page.locator(f'#grid .grid-card[data-id="{target}"]').click()
    page.wait_for_timeout(400)
    before = page.evaluate("({first: earliestPage, end: currentPage, ids: photos.map(p => p.id)})")
    requests = []
    page.on("request", lambda r: requests.append(r.post_data_json) if "/api/photos/query" in r.url else None)
    assert page.evaluate("resetAndLoad({preserveScroll: true})") is True
    after = page.evaluate("({first: earliestPage, end: currentPage, ids: photos.map(p => p.id)})")
    assert after == before
    assert requests and all(request["page"] >= before["first"] for request in requests)
    assert page.evaluate("selectedPhotoId") == target


def test_membership_refresh_handles_removing_all_matches(live_server, page):
    db = live_server["db"]
    keyword_id = _seed_filtered_library(db, live_server["data"]["folders"][0])
    _open_filtered_browse(page, live_server, "keyword", "is", "Marsh")
    _scroll_and_select(page)
    with db.conn:
        db.conn.execute("DELETE FROM photo_keywords WHERE keyword_id = ?", (keyword_id,))
    assert page.evaluate("resetAndLoad({preserveScroll: true})") is True
    expect(page.locator("#emptyState")).to_be_visible()
    expect(page.locator("#grid .grid-card")).to_have_count(0)
    assert page.evaluate("getActiveSelection()") == []
    assert page.evaluate("totalPhotos") == 0
    assert page.evaluate("gridContainer.scrollTop") == 0


def test_stack_loading_above_viewport_keeps_browser_scroll_anchoring(live_server, page, browser_name):
    if browser_name != "chromium":
        pytest.skip("WebKit does not implement CSS scroll anchoring")
    db = live_server["db"]
    _seed_filtered_library(db, live_server["data"]["folders"][0])
    ids = [row[0] for row in db.conn.execute(
        "SELECT id FROM photos WHERE filename LIKE 'marsh%' ORDER BY timestamp LIMIT 20 OFFSET 10"
    )]
    seed_browse_stack(db, ids)
    _open_filtered_browse(page, live_server, "keyword", "is", "Marsh")
    page.locator("#browseStacksToggle").check()
    page.wait_for_function("!loading && browseDatasetReady")
    assert page.evaluate("resetAndLoad({preserveScroll: true})") is True
    assert page.evaluate("getComputedStyle(gridContainer).overflowAnchor") == "auto"
    cover = page.locator("#grid .has-browse-stack").first
    cover_id = int(cover.get_attribute("data-id"))
    pending = []
    page.route("**/api/photos/by-ids", lambda route: pending.append(route), times=1)
    page.evaluate("id => { window.stackDone = toggleBrowseStack(null, id); }", cover_id)
    page.wait_for_timeout(100)
    assert pending
    below = page.locator("#grid .grid-card").nth(35)
    below.scroll_into_view_if_needed()
    page.wait_for_timeout(100)
    before = below.bounding_box()["y"]
    pending[0].continue_()
    page.evaluate("stackDone")
    page.wait_for_timeout(150)
    assert abs(below.bounding_box()["y"] - before) < 2


def test_membership_refresh_does_not_refetch_the_historical_tail(live_server, page):
    _seed_filtered_library(live_server["db"], live_server["data"]["folders"][0], count=600)
    _open_filtered_browse(page, live_server, "keyword", "is", "Marsh")
    page.evaluate("async () => { while (!allLoaded) await loadPhotos(); }")
    assert page.evaluate("photos.length") == 600
    before = _scroll_and_select(page)
    _watch_grid_frames(page)
    requests = []
    page.on("request", lambda r: requests.append(r.post_data_json) if "/api/photos/query" in r.url else None)
    _add_keyword_to_selection(page, "Golden Hour")
    _assert_grid_never_jumped(page, before)
    assert requests
    assert sum(request["per_page"] for request in requests) <= 150
    assert page.evaluate("photos.length") < 600
    assert page.evaluate("totalPhotos") == 600
    assert page.evaluate("allLoaded") is False
