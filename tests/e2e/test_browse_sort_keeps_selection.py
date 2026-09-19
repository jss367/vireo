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

import contextlib
import json

from playwright.sync_api import expect

from e2e.stack_seed import seed_browse_stack


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
    """Record /api/photos/query calls in the order the browser *issued* them.

    Ordering has to come from the request event, not the response: a
    non-focused request can start first and finish last, and an
    "the focused query came first" assertion read off completion order would
    pass without proving anything (CodeRabbit review on PR #1658). Responses
    are matched back onto their request entry.
    """
    calls = []

    def on_request(request):
        if "/api/photos/query" not in request.url:
            return
        try:
            body = json.loads(request.post_data or "{}")
        except Exception:
            return
        calls.append({"request": body, "response": None, "_req": request})

    def on_response(response):
        if "/api/photos/query" not in response.url:
            return
        for call in calls:
            if call["_req"] is response.request:
                with contextlib.suppress(Exception):
                    call["response"] = response.json()
                return

    page.on("request", on_request)
    page.on("response", on_response)
    return calls


def _change_sort(page, value):
    page.select_option("#sortSelect", value)
    page.wait_for_timeout(600)
    page.wait_for_function(
        "() => !loading && browseDatasetReady", timeout=15000
    )


def _stall_first_focused_query(page, seconds=1.5):
    """Hold the first focused request open, stalling *in the browser*.

    Must be installed before ``page.goto``. A ``page.route`` handler that
    sleeps would block the test thread as well, so the first request would
    finish before the test could issue the second action and the reloads
    would never actually overlap (CodeRabbit review on PR #1658). Patching
    ``fetch`` inside the page delays only the page.
    """
    page.add_init_script(
        """
        (() => {
          const origFetch = window.fetch;
          window.__focusStallArmed = true;
          window.__focusStalled = false;
          window.fetch = function (input, init) {
            const body = init && init.body ? String(init.body) : '';
            if (window.__focusStallArmed && body.includes('focus_photo_id')) {
              window.__focusStallArmed = false;
              window.__focusStalled = true;
              return new Promise((resolve, reject) => {
                setTimeout(
                  () => origFetch(input, init).then(resolve, reject),
                  %d,
                );
              });
            }
            return origFetch(input, init);
          };
        })();
        """ % int(seconds * 1000)
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
    # Must be armed before the first navigation: it is an init script.
    _stall_first_focused_query(page)
    _open_browse(page, live_server)
    _scroll_until_loaded(page, 100)
    photo_id = _select_photo_at(page, 80)

    # Hold the first focused request open so the second sort change is
    # guaranteed to arrive while it is still in flight.
    page.select_option("#sortSelect", "name_desc")
    page.wait_for_timeout(150)
    page.select_option("#sortSelect", "rating")
    page.wait_for_timeout(2500)
    page.wait_for_function("() => !loading && browseDatasetReady", timeout=15000)
    assert page.evaluate("window.__focusStalled") is True, (
        "the focused request was never stalled"
    )
    assert page.evaluate("document.getElementById('sortSelect').value") == "rating"
    assert page.evaluate("selectedPhotoId") == photo_id, (
        "the second sort change dropped the photo the first one was holding"
    )
    expect(
        page.locator(f"#grid .grid-card[data-id='{photo_id}']")
    ).to_be_visible()


def test_health_refresh_mid_sort_keeps_the_photo(live_server, page):
    """A folder-health refresh must not orphan an in-flight sort's anchor.

    The refresh asks to keep the user's place (``preserveAnchor``) but lands
    while the sort has already torn down the cards, so it captures nothing.
    It then invalidates the sort's request — losing the selection for good
    unless it can inherit the anchor the sort was holding (Codex review on
    PR #1658).
    """
    _seed_sortable_library(live_server["db"], live_server["data"]["folders"][0])
    # Must be armed before the first navigation: it is an init script.
    _stall_first_focused_query(page)
    _open_browse(page, live_server)
    _scroll_until_loaded(page, 100)
    photo_id = _select_photo_at(page, 80)

    page.select_option("#sortSelect", "name_desc")
    page.wait_for_timeout(150)
    # Same scope, different trigger: a folder-health event lands mid-sort.
    page.evaluate(
        "() => document.dispatchEvent("
        "  new CustomEvent('vireo:folder-health-changed', {detail: {}}))"
    )
    page.wait_for_timeout(2500)
    page.wait_for_function("() => !loading && browseDatasetReady", timeout=15000)
    assert page.evaluate("window.__focusStalled") is True, (
        "the focused request was never stalled"
    )
    assert page.evaluate("selectedPhotoId") == photo_id, (
        "a health refresh landing mid-sort dropped the photo"
    )


def test_scope_change_mid_sort_drops_the_photo(live_server, page):
    """The other half: a real scope change must NOT inherit the anchor.

    The photo belongs to the view the user left, so resurrecting it would
    re-select something the scope change deliberately cleared.

    Note this one pins behaviour rather than proving the scope-generation
    guard: it also passes with that guard removed, because the folder it
    switches to does not contain the photo, so the adopted anchor could not
    be restored either way. Reproducing the guard's exact failure needs a
    scope change into a view that still holds the photo. Kept because the
    user-visible rule — a folder change mid-sort does not hand you back the
    old scope's selection — is worth a regression test regardless.
    """
    seeded = _seed_sortable_library(
        live_server["db"], live_server["data"]["folders"][0]
    )
    assert seeded
    # Must be armed before the first navigation: it is an init script.
    _stall_first_focused_query(page)
    _open_browse(page, live_server)
    _scroll_until_loaded(page, 100)
    photo_id = _select_photo_at(page, 80)

    page.select_option("#sortSelect", "name_desc")
    page.wait_for_timeout(150)
    # A sidebar folder click — this bumps browseScopeGen — and then another
    # sort change, which is what reaches the inherit branch at all.
    other_folder = live_server["data"]["folders"][1]
    page.evaluate("id => filterByFolder(id)", other_folder)
    page.wait_for_timeout(100)
    page.select_option("#sortSelect", "rating")
    page.wait_for_timeout(2500)
    page.wait_for_function("() => !loading && browseDatasetReady", timeout=15000)
    assert page.evaluate("window.__focusStalled") is True, (
        "the focused request was never stalled"
    )
    assert page.evaluate("selectedPhotoId") != photo_id, (
        "a photo from the scope the user left was resurrected as the selection"
    )


def test_filter_change_mid_sort_drops_the_pending_anchor(live_server, page):
    """A non-preserving filter reset must clear the in-flight sort's anchor.

    Applying (or narrowing) a filter calls ``resetAndLoad()`` without a
    preservation flag but does not bump ``browseScopeGen`` — the folder is
    unchanged. Without also dropping ``pendingFocusAnchor`` a following sort
    change would adopt the stale anchor and resurrect the photo whenever it
    still matched the new filter, undoing the selection clear the filter
    reset intended (Codex review r4021838076).
    """
    _seed_sortable_library(live_server["db"], live_server["data"]["folders"][0])
    # Must be armed before the first navigation: it is an init script.
    _stall_first_focused_query(page)
    _open_browse(page, live_server)
    _scroll_until_loaded(page, 100)
    photo_id = _select_photo_at(page, 80)

    page.select_option("#sortSelect", "name_desc")
    page.wait_for_timeout(150)
    # A filename filter that still matches every seeded photo. The bug would
    # resurrect ``photo_id`` when the second sort adopts the pending anchor;
    # the fix drops the anchor so the second sort resets cleanly instead.
    page.evaluate(
        "() => VireoFilter.addRule('filename', 'contains', 'bird')"
    )
    page.wait_for_timeout(100)
    page.select_option("#sortSelect", "rating")
    page.wait_for_timeout(2500)
    page.wait_for_function("() => !loading && browseDatasetReady", timeout=15000)
    assert page.evaluate("window.__focusStalled") is True, (
        "the focused request was never stalled"
    )
    assert page.evaluate("selectedPhotoId") != photo_id, (
        "a filter change mid-sort left the pending anchor behind and the "
        "next sort resurrected the cleared selection"
    )


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


def _enable_stacks(page):
    page.locator("#browseStacksToggle").check()
    page.wait_for_timeout(400)
    page.wait_for_function(
        "() => !loading && browseDatasetReady", timeout=15000
    )


def _loaded_stack_cover_id(page):
    """The cover of the one collapsed stack in the loaded window, or None."""
    return page.evaluate(
        """() => {
             const cover = photos.find(
               p => p.browse_stack && p.browse_stack.count > 1
             );
             return cover ? cover.id : null;
           }"""
    )


def _topmost_card_id(page):
    """The first card still on screen — what the viewport anchor holds onto."""
    return page.evaluate(
        """() => {
             const c = document.getElementById('gridContainer');
             const top = c.getBoundingClientRect().top;
             for (const el of document.querySelectorAll('#grid .grid-card')) {
               if (el.getBoundingClientRect().bottom > top + 1) {
                 return parseInt(el.dataset.id, 10);
               }
             }
             return null;
           }"""
    )


def _ids_on_screen(page, ids):
    """Which of ``ids`` have a grid card inside the scroll container."""
    return page.evaluate(
        """ids => {
             const c = document.getElementById('gridContainer');
             const box = c.getBoundingClientRect();
             return ids.filter(id => {
               const el = document.querySelector(
                 `#grid .grid-card[data-id='${id}']`
               );
               if (!el) return false;
               const r = el.getBoundingClientRect();
               return r.bottom > box.top && r.top < box.bottom;
             });
           }""",
        ids,
    )


def test_sort_change_keeps_the_selected_stack(live_server, page):
    """A selected stack is a selected card, and a re-sort keeps it.

    Clicking a collapsed stack card selects every frame it stands for and
    leaves no focused photo — which read to ``captureSelectedPhotoAnchor``
    as "a batch is selected". It declined, and the re-sort cleared the
    selection and snapped back to the top of the new order. A stack is one
    card in one position, so it anchors the reload like any other card.
    """
    ids = _seed_sortable_library(
        live_server["db"], live_server["data"]["folders"][0]
    )
    # Deep enough into both orders that the focused page is not page 1.
    burst_ids = ids[100:103]
    seed_browse_stack(live_server["db"], burst_ids)
    _open_browse(page, live_server)
    _enable_stacks(page)
    _scroll_until_loaded(page, 110)

    cover_id = _loaded_stack_cover_id(page)
    assert cover_id in burst_ids, (
        f"expected the seeded burst {burst_ids} to be the loaded window's "
        f"only stack, got cover {cover_id}"
    )
    card = page.locator(f"#grid .grid-card[data-id='{cover_id}']")
    card.scroll_into_view_if_needed()
    page.wait_for_timeout(300)
    card.click()
    page.wait_for_function(
        """ids => selectedPhotoId === null && selectedPhotos.size === ids.length
             && ids.every(id => selectedPhotos.has(id))""",
        arg=burst_ids,
    )

    _change_sort(page, "name_desc")

    assert page.evaluate("selectedPhotoId") is None
    assert sorted(page.evaluate("() => Array.from(selectedPhotos)")) == sorted(
        burst_ids
    ), "re-sorting dropped the selected stack"
    assert page.evaluate("earliestPage") > 1, (
        "the grid restarted at page 1 instead of jumping to the stack"
    )
    assert _ids_on_screen(page, burst_ids), (
        "the selected stack is off screen after the re-sort"
    )
    expect(page.locator("#selectionCount")).to_have_text(
        "3 photos selected · 1 stack"
    )


def test_sort_change_holds_the_place_of_a_batch_selection(live_server, page):
    """A loose batch has no one card to keep the user with — but it has a place.

    The selection itself does not survive: the new order can put those
    photos anywhere and ``resetAndLoad`` clears ids that may no longer be
    loaded. The position does, so the user lands where they were working
    instead of at the top of the catalog.
    """
    _seed_sortable_library(live_server["db"], live_server["data"]["folders"][0])
    _open_browse(page, live_server)
    _scroll_until_loaded(page, 100)
    _select_photo_at(page, 80)
    page.locator("#grid .grid-card").nth(81).click(modifiers=["ControlOrMeta"])
    page.wait_for_function("() => selectedPhotos.size === 2")
    anchored_id = _topmost_card_id(page)
    assert anchored_id is not None

    _change_sort(page, "name_desc")

    assert page.evaluate("selectedPhotos.size") == 0, (
        "a loose batch cannot survive a re-sort — its ids may not be loaded"
    )
    assert page.evaluate("earliestPage") > 1, (
        "the grid restarted at page 1 instead of holding the batch's place"
    )
    assert _ids_on_screen(page, [anchored_id]) == [anchored_id], (
        "the re-sort threw away the place the batch was working in"
    )


def test_sort_change_keeps_a_stack_selected_from_a_collapsed_tray(
    live_server, page,
):
    """"Select all" in a tray, then collapse it: still a whole-stack selection.

    Collapsing pins the focus to the visible cover and keeps every member in
    ``selectedPhotos`` (``batchHasHiddenMembers`` in toggleBrowseStack),
    because a collapsed tray cannot hold a focus on a hidden frame. Reading
    a focused photo as "this must be a hand-built batch" dropped that
    selection on the next re-sort (Codex P2 on PR #1695).
    """
    ids = _seed_sortable_library(
        live_server["db"], live_server["data"]["folders"][0]
    )
    burst_ids = ids[100:103]
    seed_browse_stack(live_server["db"], burst_ids)
    _open_browse(page, live_server)
    _enable_stacks(page)
    _scroll_until_loaded(page, 110)

    cover_id = _loaded_stack_cover_id(page)
    assert cover_id in burst_ids
    badge = page.locator(
        f"#grid .grid-card[data-id='{cover_id}'] .browse-stack-badge"
    )
    badge.scroll_into_view_if_needed()
    page.wait_for_timeout(300)
    badge.click()
    tray = page.locator(f".browse-stack-tray[data-stack-cover-id='{cover_id}']")
    expect(tray).to_be_visible()
    tray.get_by_role("button", name="Select all").click()
    badge.click()
    expect(tray).to_be_hidden()
    page.wait_for_function(
        """args => selectedPhotoId === args.cover
             && args.ids.every(id => selectedPhotos.has(id))
             && selectedPhotos.size === args.ids.length""",
        arg={"cover": cover_id, "ids": burst_ids},
    )

    _change_sort(page, "name_desc")

    assert sorted(page.evaluate("() => Array.from(selectedPhotos)")) == sorted(
        burst_ids
    ), "re-sorting dropped a stack selected through the tray"
    assert _ids_on_screen(page, burst_ids), (
        "the selected stack is off screen after the re-sort"
    )
