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
import time

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
    # The sort control is a persisted view preference
    # (data-view-preference="vireo.browse.sort"), and the server's ephemeral
    # port can repeat between tests — so without this, a sort left behind by an
    # earlier test becomes this one's starting order and its expected pages
    # move. Start every test from the stored-preference-free default.
    page.add_init_script("try { localStorage.clear(); } catch (e) {}")
    page.goto(live_server["url"] + "/browse")
    page.wait_for_selector("#grid .grid-card", timeout=15000)
    page.wait_for_function(
        "() => photos.length > 0 && !loading && browseDatasetReady", timeout=15000
    )
    assert page.evaluate("document.getElementById('sortSelect').value") == "date"


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


def _capture_queries(page):
    """Record every /api/photos/query request body and its response payload."""
    calls = []

    def on_response(response):
        if "/api/photos/query" not in response.url:
            return
        try:
            body = json.loads(response.request.post_data or "{}")
            calls.append({"request": body, "response": response.json()})
        except Exception:
            pass

    page.on("response", on_response)
    return calls


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
    replaces it, and it is the *first* query the re-sort issues — nothing is
    fetched on the way to the photo. (Browse still hydrates the window
    upward afterwards once the viewport sits near the top of the page it
    landed on; that is ordinary lazy paging, not a search.)
    """
    _seed_sortable_library(live_server["db"], live_server["data"]["folders"][0])
    _open_browse(page, live_server)
    _scroll_until_loaded(page, 100)
    photo_id = _select_photo_at(page, 80)

    calls = _capture_queries(page)
    _change_sort(page, "name_desc")

    assert calls, "the re-sort must re-query"
    first = calls[0]
    assert first["request"].get("focus_photo_id") == photo_id, (
        f"the re-sort's first query was not the focused one: {first['request']}"
    )
    assert first["request"]["sort"] == "name_desc"
    # The server really did jump: the photo was not on page 1 of the new order.
    landed_on = first["response"]["focus_page"]
    assert landed_on > 1, (
        "test needs a photo that lands past the first page after the re-sort"
    )
    assert first["response"]["focus_index"] >= 0
    assert photo_id in [photo["id"] for photo in first["response"]["photos"]]


def test_second_sort_change_mid_flight_keeps_the_photo(live_server, page):
    """Changing the sort again before the first load lands must not lose it.

    ``resetAndLoad`` clears the selection before its focused load returns, so
    a second sort change has nothing left to capture and would reset to the
    top. Not an exotic race: a keyboard user arrowing through the sort
    ``<select>`` fires one ``change`` per option (Codex review on PR #1658).
    """
    _seed_sortable_library(live_server["db"], live_server["data"]["folders"][0])
    _open_browse(page, live_server)
    _scroll_until_loaded(page, 100)
    photo_id = _select_photo_at(page, 80)

    # Hold the first focused request open so the second sort change is
    # guaranteed to arrive while it is still in flight.
    held = {"done": False}

    def stall_first_focused(route, request):
        if not held["done"] and "focus_photo_id" in (request.post_data or ""):
            held["done"] = True
            time.sleep(1.5)
        route.continue_()

    page.route("**/api/photos/query", stall_first_focused)

    page.select_option("#sortSelect", "name_desc")
    page.wait_for_timeout(150)
    page.select_option("#sortSelect", "rating")
    page.wait_for_timeout(2500)
    page.wait_for_function("() => !loading && browseDatasetReady", timeout=15000)
    page.unroute("**/api/photos/query")

    assert held["done"], "the first focused request was never stalled"
    assert page.evaluate("document.getElementById('sortSelect').value") == "rating"
    assert page.evaluate("selectedPhotoId") == photo_id, (
        "the second sort change dropped the photo the first one was holding"
    )
    expect(
        page.locator(f"#grid .grid-card[data-id='{photo_id}']")
    ).to_be_visible()


def test_stacked_grid_counts_cards_not_photos_in_the_banner(live_server, page):
    """With Stacks on the window offset counts cards, so say cards.

    One card can stand for a whole burst, so calling the offset "photos"
    undercounts — fifty earlier stacks can be hundreds of frames
    (CORE_PHILOSOPHY, "no black boxes"; Codex review on PR #1658).
    """
    _seed_sortable_library(live_server["db"], live_server["data"]["folders"][0])
    _open_browse(page, live_server)
    page.locator("#browseStacksToggle").check()
    page.wait_for_timeout(600)
    page.wait_for_function("() => !loading && browseDatasetReady", timeout=15000)
    _scroll_until_loaded(page, 100)
    _select_photo_at(page, 80)

    calls = _capture_queries(page)
    _change_sort(page, "name_desc")
    assert calls[0]["request"]["stacks"] is True
    assert calls[0]["response"]["focus_page"] > 1

    state = page.evaluate("""() => {
      const banner = document.getElementById('loadPreviousPhotosBanner');
      return {
        earliestPage,
        perPage,
        shown: banner.style.display !== 'none',
        text: document.getElementById('loadPreviousPhotosText').textContent,
      };
    }""")
    missing_above = (state["earliestPage"] - 1) * state["perPage"]
    if missing_above > 0:
        assert state["shown"]
        assert "cards aren’t loaded" in state["text"], (
            f"stacked banner must count cards, got {state['text']!r}"
        )
        assert "photos" not in state["text"], (
            f"stacked banner must not call stack items photos: {state['text']!r}"
        )


def test_sort_change_says_how_much_of_the_grid_is_missing(live_server, page):
    """A grid that does not start at the first photo has to say so.

    Landing mid-dataset is the price of keeping the user's photo; leaving it
    unsaid would make the re-sort look like the library had shrunk
    (CORE_PHILOSOPHY, "no black boxes"). Browse hydrates the window upward
    once you are near the top of where it landed, so the offset moves —
    assert the invariant rather than one snapshot of it: the banner is shown
    exactly when rows are missing above, and it names the right number.
    """
    _seed_sortable_library(live_server["db"], live_server["data"]["folders"][0])
    _open_browse(page, live_server)
    _scroll_until_loaded(page, 100)
    photo_id = _select_photo_at(page, 80)

    calls = _capture_queries(page)
    _change_sort(page, "name_desc")

    assert calls[0]["response"]["focus_page"] > 1, (
        "test needs a photo that lands past the first page after the re-sort"
    )

    # Read the window and the banner in one go — upward hydration can advance
    # the window between two separate evaluates.
    state = page.evaluate("""() => {
      const banner = document.getElementById('loadPreviousPhotosBanner');
      const text = document.getElementById('loadPreviousPhotosText');
      return {
        earliestPage,
        perPage,
        shown: banner.style.display !== 'none',
        text: text.textContent,
      };
    }""")
    missing_above = (state["earliestPage"] - 1) * state["perPage"]
    if missing_above > 0:
        assert state["shown"], (
            f"{missing_above} rows are missing above the grid with no banner"
        )
        assert f"this grid starts at #{missing_above + 1:,}" in state["text"], (
            f"banner text {state['text']!r} does not match offset {missing_above}"
        )
    else:
        assert not state["shown"], "banner shown while the grid starts at #1"
    assert page.evaluate("selectedPhotoId") == photo_id


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
