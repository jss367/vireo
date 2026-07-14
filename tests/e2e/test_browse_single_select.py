import json

from playwright.sync_api import expect


def disable_infinite_scroll(page):
    page.add_init_script("""
      class NoopIntersectionObserver {
        observe() {}
        unobserve() {}
        disconnect() {}
      }
      window.IntersectionObserver = NoopIntersectionObserver;
    """)


def test_large_library_uses_bounded_placeholder_runway(live_server, page):
    """A large result set must not expose its unloaded tail as scroll space.

    Browse only loads a contiguous prefix. Reserving the full dataset height
    made an absolute-bottom jump crawl through every preceding page before a
    real card could reach the viewport.
    """
    disable_infinite_scroll(page)
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    state = page.evaluate(
        """() => {
          totalPhotos = 50000;
          allLoaded = false;
          updateGridTail();
          var container = document.getElementById('gridContainer');
          container.scrollTop = container.scrollHeight;
          updateScrollPosition();
          return {
            skeletons: document.querySelectorAll('#gridTail .skel-card').length,
            spacers: document.querySelectorAll('#gridTail .grid-tail-spacer').length,
            position: document.getElementById('filterSummary').textContent,
          };
        }"""
    )

    assert state["skeletons"] == 300
    assert state["spacers"] == 0
    assert state["position"].endswith(" of 50,000")
    assert state["position"] != "≈50,000 of 50,000"

    hydration = page.evaluate(
        """async () => {
          var originalSafeFetch = safeFetch;
          var nextId = 100000;
          var calls = 0;
          safeFetch = function(url, options, fetchOptions) {
            if (url.indexOf('/api/photos?') === 0) {
              calls++;
              var perPage = parseInt(new URL(url, location.origin).searchParams.get('per_page'), 10);
              if (calls >= 10) return Promise.resolve({photos: [], total: totalPhotos});
              var batch = [];
              for (var i = 0; i < perPage; i++) {
                batch.push({id: nextId++, filename: 'photo-' + nextId + '.jpg'});
              }
              return Promise.resolve({photos: batch, total: totalPhotos});
            }
            return originalSafeFetch(url, options, fetchOptions);
          };
          try {
            // The test observer is intentionally non-native; opt into the
            // scroll-driven path directly without enabling observer races.
            infiniteScrollObserverIsNative = true;
            infiniteScrollObserverDisconnected = false;
            var container = document.getElementById('gridContainer');
            container.scrollTop = container.scrollHeight;
            ensureViewportHydrated();

            var deadline = Date.now() + 3000;
            while (Date.now() < deadline) {
              var firstSkeleton = document.querySelector('#gridTail .skel-card');
              var boundaryIsPastViewport = firstSkeleton &&
                firstSkeleton.getBoundingClientRect().top -
                  container.getBoundingClientRect().bottom > 3200;
              if (calls > 0 && !loading && (boundaryIsPastViewport || allLoaded)) break;
              await new Promise(function(resolve) { setTimeout(resolve, 20); });
            }
            return {calls: calls, loaded: photos.length, allLoaded: allLoaded};
          } finally {
            safeFetch = originalSafeFetch;
          }
        }"""
    )

    assert 1 <= hydration["calls"] < 10
    assert hydration["loaded"] < 50000
    assert not hydration["allLoaded"]


def test_single_click_reveals_batch_bar(live_server, page):
    """Normal-click on one photo reveals the batch bar so Develop/Export/Delete
    are reachable with a single photo selected.

    Regression: updateBatchBar() previously only showed the bar when
    selectedPhotos.size > 1, leaving single-click users with no UI path to
    batch actions against the focused photo.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    bar = page.locator("#batchBar")
    expect(bar).to_be_hidden()

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    expect(bar).to_be_visible()
    expect(page.locator("#batchCount")).to_have_text("1 selected")
    expect(page.locator("#developBtn")).to_be_visible()


def test_prepare_full_resolution_uses_active_browse_selection(live_server, page):
    submitted = []

    def start_job(route):
        submitted.append(json.loads(route.request.post_data or "{}"))
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"job_id": "prepare-ui-test", "total": 1}),
        )

    page.route("**/api/jobs/prepare-full-resolution", start_job)
    page.route(
        "**/api/jobs/prepare-ui-test/stream",
        lambda route: route.fulfill(
            status=200,
            content_type="text/event-stream",
            body=(
                "event: progress\n"
                "data: {\"current\":1,\"total\":1,\"current_file\":\"hawk1.jpg\"}\n\n"
                "event: complete\n"
                "data: {\"status\":\"completed\",\"result\":{\"ready\":1,\"copied\":1,\"failed\":0}}\n\n"
            ),
        ),
    )

    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    selected_id = int(first.get_attribute("data-id"))
    first.click()

    button = page.locator("#prepareFullResolutionBtn")
    expect(button).to_be_visible()
    button.click()

    page.wait_for_function(
        "() => window._prepareFullResolutionJobId === null"
    )
    assert submitted == [{"photo_ids": [selected_id]}]
    expect(button).to_be_enabled()
    expect(button).to_have_text("Prepare Full Resolution")


def test_adjust_capture_time_lives_in_native_menu_not_batch_bar(live_server, page):
    """Capture-time adjustment is useful, but too infrequent for the Browse
    batch bar; it remains available through the native Photo menu command.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    page.locator(".grid-card").first.wait_for(state="visible")
    page.locator(".grid-card").first.click()

    expect(page.locator("#batchBar")).to_be_visible()
    assert page.locator("#batchBar button", has_text="Adjust Time").count() == 0

    page.evaluate("window.handleNativeMenuCommand('photo_adjust_capture_time')")

    modal = page.locator("#captureTimeModal.open")
    expect(modal).to_be_visible()
    expect(page.locator("#captureTimeTitle")).to_have_text(
        "Adjust Capture Time for 1 photo"
    )


def test_closing_detail_hides_batch_bar(live_server, page):
    """Closing the detail panel clears single-focus selection and hides the bar."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()

    # Trigger closeDetail via the summary/close button inside the detail panel.
    # Falling back to pressing Escape which browse.html wires to the same path.
    page.evaluate("closeDetail()")

    expect(bar).to_be_hidden()


def test_cmd_click_single_photo_shows_bar(live_server, page):
    """Cmd-clicking one tile (size==1) now reveals the bar too.

    Previously size>1 was required; users had to cmd-click two photos before
    any batch action became reachable.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click(modifiers=["Meta"])

    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()
    expect(page.locator("#batchCount")).to_have_text("1 selected")


def test_clear_button_clears_single_focus(live_server, page):
    """Clear in the batch bar must hide the bar after a single-click focus.

    Regression: clearSelection() only emptied selectedPhotos, so the focused
    selectedPhotoId survived and updateBatchBar() re-showed "1 selected",
    leaving batch actions silently armed against that photo.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()

    page.locator("#batchBar button", has_text="Clear").click()

    expect(bar).to_be_hidden()
    # selectedPhotoId must be cleared too so batch actions no longer target it.
    assert page.evaluate("selectedPhotoId") is None


def test_clear_button_closes_detail_panel(live_server, page):
    """Clear in the batch bar must also hide the detail panel.

    Regression: clearSelection() nulled selectedPhotoId but left the detail
    panel visible. Detail-panel handlers (setFlag, setColorLabel, addKeyword)
    early-return on null selectedPhotoId, so buttons silently did nothing
    while the panel remained on screen.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    # Detail panel gains the "visible" class when a photo is focused.
    page.wait_for_function(
        "document.getElementById('detailContent').classList.contains('visible')",
        timeout=2000,
    )

    page.locator("#batchBar button", has_text="Clear").click()

    # Detail panel must drop the visible class so the summary comes back.
    assert not page.evaluate(
        "document.getElementById('detailContent').classList.contains('visible')"
    )
    assert not page.evaluate(
        "document.getElementById('summaryPanel').classList.contains('hidden')"
    )


def test_add_keyword_input_suggests_existing_keyword(live_server, page):
    """Typing in the add-keyword field should offer matching existing keywords."""
    db = live_server["db"]
    source_id = live_server["data"]["photos"][0]
    target_id = live_server["data"]["photos"][1]
    keyword_id = db.add_keyword("Alan's Hummingbird")
    db.tag_photo(source_id, keyword_id)

    page.goto(f"{live_server['url']}/browse")
    page.locator(f'.grid-card[data-id="{target_id}"]').click()

    keyword_input = page.locator("#addKeywordInput")
    keyword_input.fill("AL")

    suggestion = page.locator(
        "#addKeywordSuggestions .keyword-suggestion-option",
        has_text="Alan's Hummingbird",
    )
    expect(suggestion).to_be_visible()

    with page.expect_response(
        lambda r: f"/api/photos/{target_id}/keywords" in r.url
        and r.request.method == "POST"
        and r.status == 200
    ):
        suggestion.click()

    expect(page.locator("#detailKeywords")).to_contain_text("Alan's Hummingbird")


def test_needs_identification_refreshes_after_identification_added(live_server, page):
    """A photo should leave the active Needs Identification grid after tagging."""
    db = live_server["db"]
    target_id = live_server["data"]["photos"][1]
    collection = db.conn.execute(
        "SELECT id FROM collections WHERE name = 'Needs Identification'"
    ).fetchone()
    assert collection is not None
    collection_id = collection["id"]

    page.goto(f"{live_server['url']}/browse")
    page.evaluate("(id) => filterByCollection(id)", collection_id)

    target_card = page.locator(f'.grid-card[data-id="{target_id}"]').first
    expect(target_card).to_be_visible()
    target_card.click()

    keyword_input = page.locator("#addKeywordInput")
    keyword_input.fill("Red-tailed Hawk")

    with page.expect_response(
        lambda r: f"/api/photos/{target_id}/keywords" in r.url
        and r.request.method == "POST"
        and r.status == 200
    ), page.expect_response(
        lambda r: f"/api/collections/{collection_id}/photos" in r.url
        and r.status == 200
    ):
        keyword_input.press("Enter")

    expect(page.locator(f'.grid-card[data-id="{target_id}"]')).to_have_count(0)


def test_add_keyword_autocomplete_retries_after_fetch_failure(live_server, page):
    """A transient keyword suggestion fetch failure must not poison the cache."""
    db = live_server["db"]
    source_id = live_server["data"]["photos"][0]
    target_id = live_server["data"]["photos"][1]
    keyword_id = db.add_keyword("Alan's Hummingbird")
    db.tag_photo(source_id, keyword_id)
    calls = {"count": 0}

    def route_keyword_all(route):
        calls["count"] += 1
        if calls["count"] == 1:
            route.fulfill(
                status=503,
                content_type="application/json",
                body='{"error":"temporary"}',
            )
            return
        route.continue_()

    page.route("**/api/keywords/all", route_keyword_all)
    page.goto(f"{live_server['url']}/browse")
    page.locator(f'.grid-card[data-id="{target_id}"]').click()

    keyword_input = page.locator("#addKeywordInput")
    with page.expect_response(
        lambda r: "/api/keywords/all" in r.url and r.status == 503
    ):
        keyword_input.click()

    with page.expect_response(
        lambda r: "/api/keywords/all" in r.url and r.status == 200
    ):
        keyword_input.fill("AL")

    expect(
        page.locator(
            "#addKeywordSuggestions .keyword-suggestion-option",
            has_text="Alan's Hummingbird",
        )
    ).to_be_visible()


def test_add_keyword_autocomplete_caches_empty_result(live_server, page):
    """A successful empty keyword list should be treated as loaded."""
    target_id = live_server["data"]["photos"][1]
    calls = {"count": 0}

    def route_keyword_all(route):
        calls["count"] += 1
        route.fulfill(status=200, content_type="application/json", body="[]")

    page.route("**/api/keywords/all", route_keyword_all)
    page.goto(f"{live_server['url']}/browse")
    page.locator(f'.grid-card[data-id="{target_id}"]').click()

    keyword_input = page.locator("#addKeywordInput")
    with page.expect_response(
        lambda r: "/api/keywords/all" in r.url and r.status == 200
    ):
        keyword_input.click()

    keyword_input.fill("AL")
    page.wait_for_timeout(100)
    assert calls["count"] == 1


def test_shift_selected_detail_keyword_add_applies_to_selection(live_server, page):
    """The visible detail keyword field must honor a shift range selection.

    Regression: after click A -> Shift-click C, the detail pane for A stayed
    visible. Typing a keyword there posted to /api/photos/<A>/keywords, even
    though the UI showed three selected photos.
    """
    db = live_server["db"]
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.nth(2).wait_for(state="visible")

    cards.nth(0).click()
    cards.nth(2).click(modifiers=["Shift"])

    selected_ids = page.evaluate(
        "getActiveSelection().slice().sort((a, b) => a - b)"
    )
    assert len(selected_ids) == 3
    expect(page.locator("#batchCount")).to_have_text("3 selected")
    expect(page.locator("#addKeywordInput")).to_be_visible()

    keyword_name = "Range Keyword Smoke"
    keyword_input = page.locator("#addKeywordInput")
    keyword_input.fill(keyword_name)

    with page.expect_response(
        lambda r: "/api/batch/keyword" in r.url
        and r.request.method == "POST"
        and r.status == 200
    ):
        keyword_input.press("Enter")

    rows = db.conn.execute(
        """
        SELECT pk.photo_id
        FROM photo_keywords pk
        JOIN keywords k ON k.id = pk.keyword_id
        WHERE k.name = ? AND pk.photo_id IN ({})
        ORDER BY pk.photo_id
        """.format(",".join("?" for _ in selected_ids)),
        [keyword_name] + selected_ids,
    ).fetchall()
    assert [row["photo_id"] for row in rows] == selected_ids


def test_singleton_multiselect_detail_keyword_add_stays_single_photo(live_server, page):
    """A one-photo set with a focused detail pane should stay a detail edit."""
    db = live_server["db"]
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.nth(1).wait_for(state="visible")

    a_id = int(cards.nth(0).get_attribute("data-id"))
    b_id = int(cards.nth(1).get_attribute("data-id"))

    cards.nth(0).click()
    cards.nth(1).click(modifiers=["Meta"])
    cards.nth(1).click(modifiers=["Meta"])

    assert page.evaluate("Array.from(selectedPhotos)") == [a_id]
    assert page.evaluate("selectedPhotoId") == a_id
    expect(page.locator("#addKeywordInput")).to_be_visible()

    keyword_name = "Singleton Detail Keyword"
    keyword_input = page.locator("#addKeywordInput")
    keyword_input.fill(keyword_name)

    with page.expect_response(
        lambda r: f"/api/photos/{a_id}/keywords" in r.url
        and r.request.method == "POST"
        and r.status == 200
    ):
        keyword_input.press("Enter")

    rows = db.conn.execute(
        """
        SELECT pk.photo_id
        FROM photo_keywords pk
        JOIN keywords k ON k.id = pk.keyword_id
        WHERE k.name = ? AND pk.photo_id IN (?, ?)
        ORDER BY pk.photo_id
        """,
        (keyword_name, a_id, b_id),
    ).fetchall()
    assert [row["photo_id"] for row in rows] == [a_id]


def test_cmd_click_toggles_focus_out_of_set_reconciles(live_server, page):
    """click A, cmd-click B, cmd-click A: the focus must not linger on A.

    Regression: getActiveSelection() prefers selectedPhotos over
    selectedPhotoId, so after this sequence the set was {B} while
    selectedPhotoId was still A. A remained visibly highlighted (and the
    detail panel still showed A), but batch actions silently targeted B.
    Fix: after a cmd-click toggle that removes selectedPhotoId from a
    non-empty set, clear selectedPhotoId and close the stale detail panel.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 2

    a_id = int(cards.nth(0).get_attribute("data-id"))
    b_id = int(cards.nth(1).get_attribute("data-id"))

    cards.nth(0).click()  # click A: focus A
    cards.nth(1).click(modifiers=["Meta"])  # cmd-click B: set={A,B}, focus=A
    cards.nth(0).click(modifiers=["Meta"])  # cmd-click A: set={B}, stale focus

    # Active selection must only contain B, and the focused id must be cleared
    # so the visible highlight and getActiveSelection() agree.
    active = page.evaluate("getActiveSelection()")
    assert active == [b_id], f"expected [{b_id}], got {active}"
    assert page.evaluate("selectedPhotoId") is None

    # The stale detail panel must be hidden so its (now no-op) handlers
    # can't be invoked against a null selectedPhotoId.
    assert not page.evaluate(
        "document.getElementById('detailContent').classList.contains('visible')"
    )

    # Card A must no longer carry the "selected" highlight; card B still does.
    assert not page.evaluate(
        f"document.querySelector('.grid-card[data-id=\"{a_id}\"]').classList.contains('selected')"
    )
    assert page.evaluate(
        f"document.querySelector('.grid-card[data-id=\"{b_id}\"]').classList.contains('selected')"
    )


def test_close_detail_preserves_multiselect_highlight(live_server, page):
    """click A -> cmd-click B -> cmd-click B -> closeDetail must keep A lit.

    Regression: closeDetail() stripped .selected from every card but left
    selectedPhotos intact, so the bar kept showing "1 selected" while no
    card was visibly highlighted. Destructive batch actions (delete/export/
    develop) would then target a photo the user could no longer identify.
    Fix: re-apply the .selected class to any card still in selectedPhotos
    during closeDetail.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 2

    a_id = int(cards.nth(0).get_attribute("data-id"))
    b_id = int(cards.nth(1).get_attribute("data-id"))

    cards.nth(0).click()  # click A: set={}, focus=A
    cards.nth(1).click(modifiers=["Meta"])  # cmd-click B: set={A,B}, focus=A
    cards.nth(1).click(modifiers=["Meta"])  # cmd-click B again: set={A}, focus=A

    page.evaluate("closeDetail()")

    # Bar must still reflect the surviving multi-select entry.
    expect(page.locator("#batchBar")).to_be_visible()
    expect(page.locator("#batchCount")).to_have_text("1 selected")
    assert page.evaluate("Array.from(selectedPhotos)") == [a_id]
    assert page.evaluate("selectedPhotoId") is None

    # Card A must still paint as selected so the user can see what will be acted on.
    assert page.evaluate(
        f"document.querySelector('.grid-card[data-id=\"{a_id}\"]').classList.contains('selected')"
    )
    assert not page.evaluate(
        f"document.querySelector('.grid-card[data-id=\"{b_id}\"]').classList.contains('selected')"
    )


def test_resetAndLoad_clears_multiselect_set(live_server, page):
    """Changing sort/filter/folder must drop a surviving multi-select set.

    Regression: resetAndLoad() cleared selectedPhotoId but left selectedPhotos
    intact, so a cmd-click selection survived sort/filter/folder changes. The
    bar would reappear in the new view with stale ids, arming delete/export/
    develop against photos that might not be present anymore.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click(modifiers=["Meta"])

    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()
    assert page.evaluate("selectedPhotos.size") == 1

    # Simulate any dataset-changing action (sort change, filter, folder click).
    page.evaluate("resetAndLoad()")

    assert page.evaluate("selectedPhotos.size") == 0
    assert page.evaluate("selectedPhotoId") is None
    expect(bar).to_be_hidden()


def test_singleton_set_keyboard_shortcut_applies(live_server, page):
    """Cmd-click one photo, then press a rating shortcut — the rating must apply.

    Regression: the keydown handler used `selectedPhotos.size > 1` while the
    batch bar used `>= 1`, so rating/flag/color shortcuts were silent no-ops
    whenever a one-item set was the only active selection (e.g. a single
    cmd-click from fresh state, or cmd-click-toggle dropping focus). The bar
    advertised "1 selected" but digit keys did nothing.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")

    a_id = int(cards.nth(0).get_attribute("data-id"))
    cards.nth(0).click(modifiers=["Meta"])

    # Cmd-click one item from fresh state leaves set={A} with no single-focus.
    assert page.evaluate("Array.from(selectedPhotos)") == [a_id]
    assert page.evaluate("selectedPhotoId") is None
    expect(page.locator("#batchBar")).to_be_visible()

    # "3" maps to _shortcuts.rate_3 by default.
    page.keyboard.press("3")

    # batchSetRating updates local state after the API call returns.
    page.wait_for_function(
        f"(photos.find(function(p){{return p.id==={a_id};}}) || {{}}).rating === 3",
        timeout=3000,
    )


def test_arrow_navigation_loads_next_page_at_loaded_boundary(live_server, page):
    """Keyboard navigation should continue past the currently loaded page."""
    url = live_server["url"]
    disable_infinite_scroll(page)
    page.route(
        "**/api/config",
        lambda route: route.fulfill(
            json={"photos_per_page": 2, "keyboard_shortcuts": {}}
        ),
    )
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.nth(1).wait_for(state="visible")
    page.wait_for_function("photos.length === 2 && totalPhotos > photos.length")

    cards.nth(1).click()
    assert page.evaluate("selectedIndex") == 1

    page.keyboard.press("ArrowRight")

    page.wait_for_function("photos.length > 2 && selectedIndex === 2", timeout=3000)
    assert page.evaluate("selectedPhotoId === photos[2].id")


def test_vertical_arrow_navigation_moves_by_rendered_grid_columns(live_server, page):
    """Up/down should move spatially by one visible grid row, not by one photo."""
    url = live_server["url"]
    page.set_viewport_size({"width": 1400, "height": 900})
    page.goto(f"{url}/browse")

    page.locator(".grid-card").first.wait_for(state="visible")
    page.locator(".grid-card").first.click()
    columns = page.evaluate("getBrowseGridColumnCount()")
    assert columns >= 2
    page.wait_for_function("cols => photos.length > cols", arg=columns)

    page.keyboard.press("ArrowDown")
    page.wait_for_function("cols => selectedIndex === cols", arg=columns)
    assert page.evaluate("selectedPhotoId === photos[selectedIndex].id")

    page.keyboard.press("ArrowUp")
    page.wait_for_function("selectedIndex === 0")
    assert page.evaluate("selectedPhotoId === photos[0].id")


def test_arrow_down_without_selection_starts_at_first_photo(live_server, page):
    """Starting keyboard navigation with Down should focus the first card."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    page.locator(".grid-card").first.wait_for(state="visible")
    assert page.evaluate("selectedIndex") == -1

    page.keyboard.press("ArrowDown")
    page.wait_for_function("selectedIndex === 0")
    assert page.evaluate("selectedPhotoId === photos[0].id")


def test_shift_arrow_navigation_preserves_range_selection_at_loaded_boundary(
    live_server, page
):
    """Loading another page for keyboard navigation must preserve modifiers."""
    url = live_server["url"]
    disable_infinite_scroll(page)
    page.route(
        "**/api/config",
        lambda route: route.fulfill(
            json={"photos_per_page": 2, "keyboard_shortcuts": {}}
        ),
    )
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.nth(1).wait_for(state="visible")
    page.wait_for_function("photos.length === 2 && totalPhotos > photos.length")

    cards.nth(1).click()
    page.keyboard.press("Shift+ArrowRight")

    page.wait_for_function("photos.length > 2 && selectedPhotos.has(photos[2].id)")
    assert page.evaluate("selectedPhotos.has(photos[1].id)")
    assert page.evaluate("selectedPhotoId === photos[1].id")


def test_multiselect_offers_partial_keyword_fill(live_server, page):
    """Selecting mixed tagged/untagged photos offers one-click keyword fill."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 5

    page.evaluate("""
      photos.forEach(function(p) { selectedPhotos.add(p.id); });
      renderGrid();
      updateBatchBar();
    """)

    expect(page.locator("#selectionPanel")).to_be_visible()
    row = page.locator(".selection-keyword-row", has_text="Red-tailed Hawk")
    expect(row).to_be_visible()
    expect(row).to_contain_text("missing from 4")

    original_with_keyword = page.evaluate("""
      async () => {
        const ids = photos.map(p => p.id);
        const details = await Promise.all(
          ids.map(id => fetch('/api/photos/' + id).then(r => r.json()))
        );
        return details
          .filter(p => (p.keywords || []).some(k => k.name === 'Red-tailed Hawk'))
          .map(p => p.id)
          .sort((a, b) => a - b);
      }
    """)

    row.locator("button", has_text="Add to 4").click()

    page.wait_for_function("""
      async () => {
        const ids = photos.map(p => p.id);
        const details = await Promise.all(
          ids.map(id => fetch('/api/photos/' + id).then(r => r.json()))
        );
        return details.every(p =>
          (p.keywords || []).some(k => k.name === 'Red-tailed Hawk')
        );
      }
    """, timeout=3000)
    expect(
        page.locator(".selection-keyword-row", has_text="Red-tailed Hawk")
    ).to_have_count(0)

    page.evaluate("async () => (await fetch('/api/undo', {method: 'POST'})).ok")
    restored_with_keyword = page.evaluate("""
      async () => {
        const ids = photos.map(p => p.id);
        const details = await Promise.all(
          ids.map(id => fetch('/api/photos/' + id).then(r => r.json()))
        );
        return details
          .filter(p => (p.keywords || []).some(k => k.name === 'Red-tailed Hawk'))
          .map(p => p.id)
          .sort((a, b) => a - b);
      }
    """)
    assert restored_with_keyword == original_with_keyword


def test_multiselect_shrink_to_focused_photo_restores_detail(live_server, page):
    """Leaving multi-select with a focused photo must restore the detail pane."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    page.locator(".grid-card").first.wait_for(state="visible")
    first_filename = page.evaluate("photos[0].filename")

    page.evaluate("""
      selectedPhotoId = photos[0].id;
      selectedIndex = 0;
      selectedPhotos.clear();
      selectedPhotos.add(photos[0].id);
      selectedPhotos.add(photos[1].id);
      updateBatchBar();
    """)
    expect(page.locator("#selectionPanel")).to_be_visible()

    page.evaluate("""
      selectedPhotos.delete(photos[1].id);
      updateBatchBar();
    """)

    expect(page.locator("#selectionPanel")).to_be_hidden()
    page.wait_for_function(
        "document.getElementById('detailContent').classList.contains('visible')",
        timeout=3000,
    )
    expect(page.locator("#detailFilename")).to_have_text(first_filename)


def test_reject_shortcut_keeps_existing_thumbnail_nodes(live_server, page):
    """Rejecting one photo should not rebuild the whole grid.

    Regression: setFlag() called renderGrid(), replacing every thumbnail
    <img> node and causing the visible grid to briefly blank/reload.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    pid = int(first.get_attribute("data-id"))

    page.evaluate(
        """() => {
          window.__firstThumbNode = document.querySelector('.grid-card img');
        }"""
    )

    page.keyboard.press("x")
    page.wait_for_function(
        f"(photos.find(function(p){{return p.id==={pid};}}) || {{}}).flag === 'rejected'",
        timeout=3000,
    )

    assert page.evaluate(
        "() => window.__firstThumbNode === document.querySelector('.grid-card img')"
    )


def test_reject_shortcut_refreshes_rejected_collection_count(live_server, page):
    """Flagging a photo as rejected should refresh matching smart-collection counts."""
    db = live_server["db"]
    rules = json.dumps([{"field": "flag", "op": "is", "value": "rejected"}])
    collection_id = db.add_collection("Rejected", rules)

    url = live_server["url"]
    page.goto(f"{url}/browse")

    count = page.locator(
        f'.tree-item[data-collection-id="{collection_id}"] .count'
    )
    expect(count).to_have_text("0")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    page.keyboard.press("x")

    expect(count).to_have_text("1")


def test_collection_count_refresh_ignores_stale_response(live_server, page):
    """Older collection-count responses must not overwrite newer badge counts."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.wait_for_load_state("networkidle")

    final_count = page.evaluate(
        """async () => {
          renderCollectionList([{id: 999001, name: 'Rejected', photo_count: 0}]);
          collectionCountLoadGen = 0;
          var originalSafeFetch = safeFetch;
          var resolvers = [];
          safeFetch = function(url, opts, options) {
            if (url === '/api/collections') {
              return new Promise(function(resolve) { resolvers.push(resolve); });
            }
            return originalSafeFetch(url, opts, options);
          };
          try {
            var first = loadCollectionCounts();
            var second = loadCollectionCounts();
            resolvers[1]([{id: 999001, photo_count: 2}]);
            await second;
            resolvers[0]([{id: 999001, photo_count: 1}]);
            await first;
            return document.querySelector(
              '.tree-item[data-collection-id="999001"] .count'
            ).textContent;
          } finally {
            safeFetch = originalSafeFetch;
          }
        }"""
    )

    assert final_count == "2"


def test_filterByCollection_clears_multiselect_set(live_server, page):
    """Switching to a collection must drop a surviving multi-select set.

    Regression: filterByCollection() reset `photos = []` and called
    closeDetail() but never cleared selectedPhotos, so a cmd-click selection
    from the previous view survived the collection switch. The batch bar
    would reappear in the new view with stale ids, arming Delete/Export/
    Develop against photos that weren't visible.
    """
    db = live_server["db"]
    rules = json.dumps([{"field": "extension", "op": "is", "value": ".jpg"}])
    collection_id = db.add_collection("All JPGs", rules)

    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click(modifiers=["Meta"])

    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()
    assert page.evaluate("selectedPhotos.size") == 1

    # Switch to a collection; stale selection must drop before loadPhotos.
    page.evaluate(f"filterByCollection({collection_id})")
    page.wait_for_function(
        f"activeCollectionId === {collection_id}", timeout=2000
    )

    assert page.evaluate("selectedPhotos.size") == 0
    assert page.evaluate("selectedPhotoId") is None
    expect(bar).to_be_hidden()


def test_delete_refreshes_smart_collection_count(live_server, page):
    """Deleting a photo from Browse refreshes smart collection counts."""
    db = live_server["db"]
    needs = next(c for c in db.get_collections() if c["name"] == "Needs Identification")
    collection_id = needs["id"]
    delete_id = live_server["data"]["photos"][1]
    before = db.count_collection_photos(collection_id)
    assert delete_id in db.collection_photo_ids(collection_id)

    url = live_server["url"]
    page.goto(f"{url}/browse")

    count = page.locator(
        f"#collectionList .tree-item[data-collection-id='{collection_id}'] .count"
    )
    expect(count).to_have_text(str(before))

    page.locator(f".grid-card[data-id='{delete_id}']").click()
    expect(page.locator("#batchBar")).to_be_visible()
    page.locator("#batchBar button[title='Delete selected photos']").click()
    page.locator("#deleteModal.open").wait_for(state="visible", timeout=3000)
    page.locator("#deleteConfirmBtn").click()

    page.wait_for_function(
        f"!document.querySelector('.grid-card[data-id=\"{delete_id}\"]')",
        timeout=3000,
    )
    expect(count).to_have_text(str(before - 1), timeout=3000)


def test_filterByCollection_cancels_pending_search_debounce(live_server, page):
    """A delayed search apply must not kick the user out of collection mode."""
    db = live_server["db"]
    rules = json.dumps([{"field": "extension", "op": "is", "value": ".jpg"}])
    collection_id = db.add_collection("Debounce Collection", rules)

    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    page.locator("#searchInput").fill("hum")
    page.evaluate(f"filterByCollection({collection_id})")
    page.wait_for_function(
        f"activeCollectionId === {collection_id}", timeout=2000
    )

    page.wait_for_timeout(350)
    assert page.evaluate("activeCollectionId") == collection_id
