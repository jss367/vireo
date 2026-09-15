"""E2E: tagging from Browse must not throw the user back to the top.

Adding a keyword re-evaluated the active filter expression so photos that no
longer match could leave the grid. That reload restarted Browse at page 1 with
``scrollTop = 0`` — and it ran for *any* active filter, so a user tagging their
way down a rating-filtered grid lost their place after every single tag.

Two rules are checked here: a filter the edit cannot move is not reloaded at
all, and a filter it can move is reloaded in place.
"""

from playwright.sync_api import expect


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
