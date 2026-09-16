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
    pending = []
    page.route("**/api/photos/query", lambda route: pending.append(route), times=1)
    page.evaluate("() => { window.refreshDone = resetAndLoad({preserveScroll: true}); }")
    page.wait_for_timeout(200)
    assert pending
    page.evaluate("gridContainer.scrollTop += 350")
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


def test_membership_refresh_bounds_pages_when_viewport_is_at_the_top(live_server, page):
    """A tag at page 1 of a deep-loaded library must not re-query every page.

    Before the bound, ``refreshBrowseWindowInPlace`` issued one sequential
    ``/api/photos/query`` per page in the loaded window; scrolling through a
    large library and then tagging from the top turned into hundreds of
    round trips. The refresh now caps its work to the viewport pages plus a
    small buffer, so the request count is bounded by what the user actually
    sees rather than by how far they have scrolled.
    """
    _seed_filtered_library(live_server["db"], live_server["data"]["folders"][0], count=1200)
    _open_filtered_browse(page, live_server, "keyword", "is", "Marsh")
    # Load many pages by scrolling to the bottom, then return to the top.
    page.evaluate("""async () => {
      while (!allLoaded) {
        gridContainer.scrollTop = gridContainer.scrollHeight;
        await new Promise(r => setTimeout(r, 30));
      }
    }""")
    page.wait_for_function("() => allLoaded && !loading && currentPage > 15", timeout=15000)
    unbounded_page_count = page.evaluate("currentPage - earliestPage")
    assert unbounded_page_count > 15, "test needs a deeply-loaded window"

    page.evaluate("gridContainer.scrollTop = 0")
    page.wait_for_timeout(100)
    page.locator('#grid .grid-card').first.click()
    expect(page.locator("#addKeywordInput")).to_be_visible()

    requests = []
    page.on("request", lambda r: requests.append(r.url) if "/api/photos/query" in r.url else None)
    _add_keyword_to_selection(page, "American Robin")

    assert requests, "membership refresh must re-query the visible pages"
    # The bound is viewport + a small buffer, so a page-1 tag should refresh
    # only a handful of pages — vastly fewer than the full loaded window.
    assert len(requests) < unbounded_page_count // 2, (
        f"refresh issued {len(requests)} requests for {unbounded_page_count} loaded pages; "
        "bounded refresh should keep this proportional to the viewport, not scroll history"
    )
    assert page.evaluate("gridContainer.scrollTop") == 0
    # earliestPage is preserved so the user can still scroll up to nothing;
    # only trailing pages beyond the viewport buffer are dropped.
    assert page.evaluate("earliestPage") == 1
