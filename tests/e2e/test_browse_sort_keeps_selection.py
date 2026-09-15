"""E2E: re-sorting Browse must keep the user with the photo they selected.

Changing the sort order is a re-ordering, not a membership change — the photo
on screen is still in the results, just somewhere else in them. Browse used to
respond by clearing the selection, closing the detail panel and resetting to
``scrollTop = 0``, so "show me these same photos by rating instead" cost the
user their place every time.

The grid pages lazily, so holding onto the photo means knowing where it landed:
these tests check that Browse asks the server for that position once and jumps
straight to the page holding it, rather than paging forward until it appears.
"""

import json

from playwright.sync_api import expect


def _seed_sortable_library(db, folder_id, count=160):
    """Filenames and capture dates that run in opposite directions.

    Photo ``i`` is the ``i``-th by date and the ``count - 1 - i``-th by
    filename, so a photo deep in one order is deep in the other too — the
    focused page has to be computed, not guessed from the old position.
    """
    ids = []
    for i in range(count):
        ids.append(db.add_photo(
            folder_id=folder_id,
            filename=f"bird{count - 1 - i:03d}.jpg",
            extension=".jpg",
            file_size=1000 + i,
            file_mtime=1.0,
            timestamp=f"2024-05-01T{i // 60:02d}:{i % 60:02d}:00",
        ))
    return ids


def _open_browse(page, live_server):
    page.goto(live_server["url"] + "/browse")
    page.wait_for_selector("#grid .grid-card", timeout=15000)
    page.wait_for_function(
        "() => photos.length > 0 && !loading && browseDatasetReady", timeout=15000
    )


def _scroll_until_loaded(page, wanted):
    """Page the grid in until ``wanted`` photos are loaded."""
    for _ in range(40):
        if page.evaluate("photos.length") >= wanted:
            return
        page.evaluate(
            "() => { const c = document.getElementById('gridContainer');"
            "        c.scrollTop = c.scrollHeight; }"
        )
        page.wait_for_timeout(300)
        page.wait_for_function("() => !loading", timeout=15000)
    raise AssertionError(
        f"grid never loaded {wanted} photos (stopped at "
        f"{page.evaluate('photos.length')})"
    )


def _select_photo_at(page, index):
    """Select the ``index``-th loaded card and return its photo id."""
    card = page.locator("#grid .grid-card").nth(index)
    card.scroll_into_view_if_needed()
    page.wait_for_timeout(300)
    card.click()
    expect(page.locator("#addKeywordInput")).to_be_visible()
    photo_id = page.evaluate("selectedPhotoId")
    assert photo_id is not None
    return photo_id


def _change_sort(page, value):
    page.select_option("#sortSelect", value)
    page.wait_for_timeout(600)
    page.wait_for_function(
        "() => !loading && browseDatasetReady", timeout=15000
    )


def test_sort_change_keeps_the_selected_photo(live_server, page):
    """The selection, the detail panel and the photo's place on screen all
    survive a re-sort."""
    _seed_sortable_library(live_server["db"], live_server["data"]["folders"][0])
    _open_browse(page, live_server)
    _scroll_until_loaded(page, 100)
    photo_id = _select_photo_at(page, 80)

    _change_sort(page, "name_desc")

    assert page.evaluate("selectedPhotoId") == photo_id, (
        "re-sorting dropped the selection"
    )
    card = page.locator(f"#grid .grid-card[data-id='{photo_id}']")
    expect(card).to_be_visible()
    # Visible means visible in the scroll container, not merely rendered.
    assert page.evaluate(
        """id => {
             const el = document.querySelector(`#grid .grid-card[data-id='${id}']`);
             const c = document.getElementById('gridContainer');
             const a = el.getBoundingClientRect(), b = c.getBoundingClientRect();
             return a.bottom > b.top && a.top < b.bottom;
           }""",
        photo_id,
    ), "the selected photo is off screen after the re-sort"
    expect(page.locator("#addKeywordInput")).to_be_visible()


def test_sort_change_asks_where_the_photo_went_instead_of_walking_to_it(
    live_server, page,
):
    """Browse asks where the photo went; it does not page towards it.

    A re-sort gives the old position no bearing on the new one, so a paged
    scan would walk the catalog a page at a time. One focused request
    replaces it: nothing below the page the photo landed on is ever fetched.
    Ordinary forward hydration of the viewport still happens and is fine.
    """
    _seed_sortable_library(live_server["db"], live_server["data"]["folders"][0])
    _open_browse(page, live_server)
    _scroll_until_loaded(page, 100)
    photo_id = _select_photo_at(page, 80)

    queries = []
    page.on(
        "request",
        lambda r: queries.append(json.loads(r.post_data))
        if "/api/photos/query" in r.url and r.post_data else None,
    )
    _change_sort(page, "name_desc")

    assert queries, "the re-sort must re-query"
    focused = [q for q in queries if "focus_photo_id" in q]
    assert len(focused) == 1, (
        f"expected exactly one focused query, got {len(focused)}: {queries}"
    )
    assert focused[0]["focus_photo_id"] == photo_id
    assert focused[0]["sort"] == "name_desc"

    landed_on = page.evaluate("earliestPage")
    assert landed_on > 1, "test needs the photo to land past the first page"
    walked = [
        q for q in queries
        if "focus_photo_id" not in q and q["page"] < landed_on
    ]
    assert not walked, f"re-sort paged towards the photo: {walked}"


def test_sort_change_says_how_much_of_the_grid_is_missing(live_server, page):
    """Landing mid-dataset is stated, not hidden.

    The grid no longer starts at the first photo, and Browse's existing
    banner is what says so — leaving it out would make the re-sort look like
    the library had shrunk (CORE_PHILOSOPHY, "no black boxes").
    """
    _seed_sortable_library(live_server["db"], live_server["data"]["folders"][0])
    _open_browse(page, live_server)
    _scroll_until_loaded(page, 100)
    _select_photo_at(page, 80)

    _change_sort(page, "name_desc")

    earliest_page = page.evaluate("earliestPage")
    assert earliest_page > 1, (
        "test needs a photo that lands past the first page after the re-sort"
    )
    banner = page.locator("#loadPreviousPhotosBanner")
    expect(banner).to_be_visible()
    offset = (earliest_page - 1) * page.evaluate("perPage")
    expect(page.locator("#loadPreviousPhotosText")).to_contain_text(
        f"this grid starts at #{offset + 1:,}"
    )


def test_sort_change_without_a_selection_still_starts_at_the_top(live_server, page):
    """Nothing selected, nothing to hold onto — the old behaviour stands."""
    _seed_sortable_library(live_server["db"], live_server["data"]["folders"][0])
    _open_browse(page, live_server)
    _scroll_until_loaded(page, 100)
    page.evaluate(
        "() => { document.getElementById('gridContainer').scrollTop = 1200; }"
    )
    page.wait_for_timeout(400)

    _change_sort(page, "name_desc")

    assert page.evaluate("earliestPage") == 1
    assert page.evaluate(
        "document.getElementById('gridContainer').scrollTop"
    ) == 0
    expect(page.locator("#loadPreviousPhotosBanner")).to_be_hidden()
