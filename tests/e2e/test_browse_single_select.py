import json
import re

from playwright.sync_api import expect

from e2e.stack_seed import seed_browse_stack


def click_more_menu_item(page, label):
    """Open the batch bar's More menu and click the named action.

    The slimmed batch bar keeps only high-frequency verbs; everything else
    lives in the unified context menu that More opens (same builder as
    right-click on a card).
    """
    page.locator("#batchMoreBtn").click()
    item = page.locator(".vireo-ctx-menu .vireo-ctx-item", has_text=label)
    expect(item).to_be_visible()
    item.click()


def disable_infinite_scroll(page):
    page.add_init_script("""
      class NoopIntersectionObserver {
        observe() {}
        unobserve() {}
        disconnect() {}
      }
      window.IntersectionObserver = NoopIntersectionObserver;
    """)


def point_covered_by_batch_bar(page, member):
    """Measure the real overlap, then clear the setup selection for the test.

    System fonts change the stack's height: its center falls under the bar
    on macOS but just above it on Linux. Select once to measure the rendered
    bar instead of assuming that the center is covered.
    """
    member.click()
    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()
    image_box = member.locator(".grid-card-img-wrap").bounding_box()
    bar_box = bar.bounding_box()
    assert image_box is not None and bar_box is not None
    left = max(image_box["x"], bar_box["x"])
    right = min(image_box["x"] + image_box["width"], bar_box["x"] + bar_box["width"])
    top = max(image_box["y"], bar_box["y"])
    bottom = min(image_box["y"] + image_box["height"], bar_box["y"] + bar_box["height"])
    # Leave room for the deliberate-movement test's ten-pixel nudge.
    assert right - left > 24 and bottom - top > 4, "Photo must overlap the batch bar"
    point = [(left + right) / 2, (top + bottom) / 2]

    bar.get_by_role("button", name="Clear", exact=True).click()
    expect(bar).to_be_hidden()
    assert member.evaluate(
        "(el, p) => el.contains(document.elementFromPoint(p[0], p[1]))", point
    ), "Clearing the setup selection must leave the photo at the measured point"
    return point


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
            if (url === '/api/photos/query') {
              calls++;
              var perPage = JSON.parse(options.body).per_page;
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
    """Normal-click on one photo reveals the batch bar so Export/Delete and
    the More menu (Develop, etc.) are reachable with a single photo selected.

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
    expect(page.locator("#batchMoreBtn")).to_be_visible()
    # Develop moved off the bar into the unified More menu; it must still be
    # reachable for a single-photo selection.
    page.locator("#batchMoreBtn").click()
    expect(page.locator(".vireo-ctx-menu").get_by_text(
        "Develop", exact=True
    )).to_be_visible()
    page.keyboard.press("Escape")


def test_batch_bar_starts_inert_so_a_slow_double_click_passes_through(
    live_server, page
):
    """The bar must not intercept clicks while a double-click is in flight.

    It floats over the photo pane, and the click that creates the selection
    is also the first click of a double-click. A fixed hide-delay cannot be
    trusted here — macOS defaults around 500 ms, and accessibility settings
    can push the platform threshold well past a second — so the bar shows
    inert (pointer-events: none) and only activates once the click flurry
    has been quiet for a window that any fresh mousedown refreshes.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    bar = page.locator("#batchBar")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")

    # Stretch the quiet window so this asserts the rule rather than the clock.
    page.evaluate("BATCH_BAR_ACTIVATE_QUIET_MS = 4000")
    first.click()

    # The bar appears immediately for feedback but starts inert — its clicks
    # (and its children's) pass through to the photo underneath.
    expect(bar).to_be_visible()
    expect(bar).to_have_class(re.compile(r"\bbatch-bar-inert\b"))
    expect(page.locator("#batchCount")).to_have_text("1 selected")
    expect(bar).to_have_css("pointer-events", "none")
    # It activates on its own once the quiet window elapses, without any
    # further interaction.
    expect(bar).not_to_have_class(re.compile(r"\bbatch-bar-inert\b"), timeout=8000)
    expect(bar).to_have_css("pointer-events", "auto")


def test_double_click_opens_the_photo_the_batch_bar_would_cover(
    live_server, page
):
    """A double-click low in the grid opens the lightbox, not a batch action.

    Expanding a stack pushes its members down into the strip the batch bar
    occupies, which is where the raised bar used to intercept the second
    click.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?", (burst_ids[1],)
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    cover.locator(".browse-stack-badge").click()

    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    member = tray.locator(f'.browse-stack-member[data-id="{burst_ids[1]}"]')
    expect(member).to_be_visible()
    member.dblclick()

    expect(page.locator("#lightboxFilename")).to_have_text("hawk2.jpg")


def test_batch_bar_activates_as_soon_as_the_pointer_moves(live_server, page):
    """Pointer movement, not a clock, is what ends the click gesture.

    Reaching a button in the bar means moving the pointer there and a
    double-click does not move it, so movement releases the bar immediately
    however long the platform's double-click interval is. The quiet timer is
    only a hatch for a pointer that never moves at all, so it must not be
    what does the work here.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    bar = page.locator("#batchBar")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")

    # Far longer than any platform double-click interval, so the timer cannot
    # be what activates the bar below.
    page.evaluate("BATCH_BAR_ACTIVATE_QUIET_MS = 60000")
    first.click()
    expect(bar).to_have_class(re.compile(r"\bbatch-bar-inert\b"))

    page.mouse.move(5, 5)

    expect(bar).not_to_have_class(re.compile(r"\bbatch-bar-inert\b"))
    expect(bar).to_have_css("pointer-events", "auto")


def test_a_bar_click_after_deliberate_movement_is_not_redirected_to_the_photo(
    live_server, page
):
    """Nudging the pointer off a card and clicking the bar above it hits the
    bar, not the photo.

    The straggling-click guard cancels a click that lands on the bar near a
    recent card mousedown, which is right when the quiet timer stranded the
    user mid-gesture and wrong once the pointer has moved — moving is how
    anyone reaches the bar. Without that distinction, a few pixels of travel
    between selecting a card and clicking the bar over it would turn a batch
    action into a lightbox open.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?", (burst_ids[1],)
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    cover.locator(".browse-stack-badge").click()

    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    member = tray.locator(f'.browse-stack-member[data-id="{burst_ids[1]}"]')
    expect(member).to_be_visible()

    click_x, click_y = point_covered_by_batch_bar(page, member)
    page.mouse.click(click_x, click_y)

    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()
    # Travel far enough to release the bar while remaining inside its overlap
    # with the photo.
    page.mouse.move(click_x + 10, click_y)
    expect(bar).not_to_have_class(re.compile(r"\bbatch-bar-inert\b"))
    assert page.evaluate(
        "p => !!document.elementFromPoint(p[0], p[1]).closest('#batchBar')",
        [click_x + 10, click_y],
    )
    page.mouse.click(click_x + 10, click_y)

    # The click belonged to the bar; it must not have been turned into a
    # double-click on the photo underneath.
    expect(page.locator("#lightboxOverlay")).to_be_hidden()


def test_slow_double_click_still_opens_photo_when_bar_activated_between_clicks(
    live_server, page
):
    """The second click of a slow double-click must open the photo even if
    the batch bar has already activated between the two clicks.

    Accessibility settings can push the platform double-click threshold well
    past the fixed quiet window, so the bar can become clickable before the
    second click lands. A stack expansion pushes a card into the strip the
    bar covers; with the quiet window collapsed, the bar activates before
    the second click; the guard is expected to detect the click on the
    (now-active) bar as the straggling half of a double-click and dispatch
    a dblclick to the card underneath.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?", (burst_ids[1],)
        )

    page.goto(f"{live_server['url']}/browse")
    # Collapse the quiet window so the bar activates immediately after the
    # first click — the scenario the guard has to cover.
    page.evaluate("BATCH_BAR_ACTIVATE_QUIET_MS = 1")

    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    cover.locator(".browse-stack-badge").click()

    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    member = tray.locator(f'.browse-stack-member[data-id="{burst_ids[1]}"]')
    expect(member).to_be_visible()

    # First click of the slow double-click lands on the stack member.
    click_x, click_y = point_covered_by_batch_bar(page, member)
    page.mouse.click(click_x, click_y)

    # The bar shows and, with the shortened quiet window, activates before
    # the second click arrives.
    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()
    expect(bar).not_to_have_class(re.compile(r"\bbatch-bar-inert\b"), timeout=2000)

    # The second click at the same coordinates — the pointer has not moved,
    # as a real double-click's pointer does not — now targets the active
    # bar. The straggling-click guard must redirect it to the card.
    assert page.evaluate(
        "p => !!document.elementFromPoint(p[0], p[1]).closest('#batchBar')",
        [click_x, click_y],
    )
    page.mouse.click(click_x, click_y)

    expect(page.locator("#lightboxFilename")).to_have_text("hawk2.jpg", timeout=3000)


def test_double_click_slower_than_every_timer_still_opens_the_photo(
    live_server, page
):
    """A second click arriving long after the quiet window has lapsed still
    opens the photo.

    A platform double-click interval can be configured past any constant we
    could pick, so the guard is bounded by pointer movement instead of a
    clock: while the pointer has not left the card, a click landing on the
    bar belongs to that card's gesture however late it is. This runs with the
    shipped quiet window and then waits well past it.
    """
    db = live_server["db"]
    burst_ids = live_server["data"]["photos"][:3]
    seed_browse_stack(db, burst_ids)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET quality_score = 0.99 WHERE id = ?", (burst_ids[1],)
        )

    page.goto(f"{live_server['url']}/browse")
    page.locator("#browseStacksToggle").check()
    cover = page.locator(f'.grid-card[data-id="{burst_ids[1]}"]')
    cover.locator(".browse-stack-badge").click()

    tray = page.locator(
        f'.browse-stack-tray[data-stack-cover-id="{burst_ids[1]}"]'
    )
    member = tray.locator(f'.browse-stack-member[data-id="{burst_ids[1]}"]')
    expect(member).to_be_visible()

    click_x, click_y = point_covered_by_batch_bar(page, member)
    page.mouse.click(click_x, click_y)

    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()
    # Let the quiet timer lapse, then wait far longer than any interval a
    # platform offers before the second click of the gesture arrives.
    expect(bar).not_to_have_class(re.compile(r"\bbatch-bar-inert\b"), timeout=5000)
    page.wait_for_timeout(2500)
    assert page.evaluate(
        "p => !!document.elementFromPoint(p[0], p[1]).closest('#batchBar')",
        [click_x, click_y],
    )
    page.mouse.click(click_x, click_y)

    expect(page.locator("#lightboxFilename")).to_have_text("hawk2.jpg", timeout=3000)


def test_keyboard_activated_batch_button_is_not_swallowed_by_the_redirect(
    live_server, page
):
    """Enter/Space on a focused batch button must fire the button.

    The straggling-click redirect keeps the second half of a stationary
    mouse double-click from firing a batch action instead of opening the
    photo. It reads pointer state — where the last card mousedown landed
    and whether the pointer has moved since — which a keyboard-triggered
    click cannot supply. Without an exemption, that click looks identical
    to a stationary mouse click on the bar and is silently cancelled
    (stopImmediatePropagation), so the requested batch action never runs.
    """
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")

    bar = page.locator("#batchBar")
    # Collapse the quiet window so the bar activates immediately and the
    # test does not need to wait a second for the shipped interval.
    page.evaluate("BATCH_BAR_ACTIVATE_QUIET_MS = 1")
    first.click()
    expect(bar).to_be_visible()
    expect(bar).not_to_have_class(re.compile(r"\bbatch-bar-inert\b"), timeout=2000)

    # The Clear button clears the selection when its handler runs; its
    # observable effect (bar hidden, selection empty) is a clean signal
    # that the click reached the button rather than being swallowed.
    clear_btn = page.get_by_role("button", name="Clear", exact=True)
    clear_btn.focus()
    page.keyboard.press("Enter")

    expect(bar).to_be_hidden()
    assert page.evaluate("getActiveSelection().length") == 0


def test_non_card_selection_flow_shows_bar_active_immediately(
    live_server, page
):
    """Ctrl/Cmd+A and Select-all raise the bar without a card gesture to
    shield, so the bar must not start inert.

    The inert-until-quiet stretch only exists to keep the bar from
    intercepting the second half of a card double-click that raised it.
    A selection created without any card mousedown has no such gesture,
    and starting inert there would let a batch-button click inside the
    quiet window pass through the transparent bar to the grid beneath,
    replacing the just-created selection with whichever card sits under
    the cursor.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    bar = page.locator("#batchBar")
    expect(bar).to_be_hidden()

    # Stretch the quiet window so the assertion is about the starting state,
    # not about a timer that would activate the bar anyway on a fast machine.
    page.evaluate("BATCH_BAR_ACTIVATE_QUIET_MS = 60000")

    # Simulate a Select-all-style flow: populate the selection without any
    # card mousedown, then let updateBatchBar() raise the bar.
    selected_ids = live_server["data"]["photos"][:3]
    page.evaluate(
        """
        photoIds => {
          selectedPhotos.clear();
          photoIds.forEach(id => selectedPhotos.add(id));
          selectedPhotoId = null;
          renderGrid();
          updateBatchBar();
        }
        """,
        selected_ids,
    )

    expect(bar).to_be_visible()
    # The bar came up for a non-card flow, so it must not be inert — a
    # transparent bar over the grid would send the very next batch-button
    # click straight through to the card beneath.
    expect(bar).not_to_have_class(re.compile(r"\bbatch-bar-inert\b"))
    expect(bar).to_have_css("pointer-events", "auto")

    # Clicking Clear now actually runs its handler (bar hides, selection
    # empties). Under the pre-fix behavior the click would fall through to
    # the grid, leaving the bar visible with a different selection.
    page.locator("#batchBar button", has_text="Clear").click()
    expect(bar).to_be_hidden()
    assert page.evaluate("getActiveSelection().length") == 0


def test_export_defaults_beside_original_and_offers_folder_browser(
    live_server, page,
):
    """Export starts beside originals and keeps a browsable custom path."""
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.get_by_role("button", name="Export", exact=True).click()

    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    expect(page.locator("#exportDest")).to_have_value("")
    expect(page.locator("#exportDest")).to_have_attribute(
        "placeholder", "Same folder as each original"
    )
    expect(page.locator("#exportSubfolder")).not_to_be_checked()

    page.get_by_role("button", name="Browse…", exact=True).click()
    expect(page.locator("#folderBrowser")).to_have_class(
        "folder-browser-overlay open"
    )
    expect(page.locator("#folderBrowserTitle")).to_have_text(
        "Select Export Folder"
    )
    page.keyboard.press("Escape")
    expect(page.locator("#folderBrowser")).not_to_have_class(
        "folder-browser-overlay open"
    )
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    page.locator("#exportSubfolder").check()

    page.evaluate(
        """() => {
          window.__exportRequest = null;
          window.safeFetch = async function(url, options) {
            if (url === '/api/jobs/export') {
              window.__exportRequest = JSON.parse(options.body);
              return {job_id: 'export-test'};
            }
            return {};
          };
        }"""
    )
    page.locator("#exportSubmitBtn").click()
    page.wait_for_function("() => window.__exportRequest !== null")
    request = page.evaluate("window.__exportRequest")
    assert request["destination"] == ""
    assert request["export_to_subfolder"] is True


def test_export_presets_do_not_overwrite_edits_while_loading(live_server, page):
    """Delayed preset restoration gates export and preserves newer edits."""
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.evaluate(
        """() => {
          const realSafeFetch = window.safeFetch;
          window.safeFetch = function(url, options, config) {
            if (url === '/api/export/presets') {
              return new Promise(function(resolve) {
                window.__resolveExportPresets = resolve;
              });
            }
            return realSafeFetch(url, options, config);
          };
          VireoViewPreferences.write(
            'vireo.export.lastPreset', 'saved:Delayed preset'
          );
        }"""
    )

    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportSubmitBtn")).to_be_disabled()
    expect(page.locator("#exportPreset")).to_be_disabled()
    expect(page.locator("#exportPresetSaveBtn")).to_be_disabled()
    expect(page.locator("#exportPresetDeleteBtn")).to_be_disabled()
    expect(page.locator("#exportDest")).to_be_disabled()
    expect(page.locator("#exportFormat")).to_be_disabled()
    expect(page.locator("#exportTemplate")).to_be_disabled()
    expect(page.locator("#exportMetadataSpecies")).to_be_disabled()
    expect(page.locator("#exportOverlay [data-export-cancel]")).to_be_enabled()
    # Programmatic assignment simulates an integration mutating a control;
    # real user input is gated until restoration completes.
    page.locator("#exportDest").evaluate(
        "(element) => { element.value = '/user-selected'; }"
    )
    page.evaluate("() => VireoExportPresets.markCustom()")
    page.evaluate(
        """() => window.__resolveExportPresets({presets: [{
          name: 'Delayed preset',
          settings: {destination: '/preset-destination'}
        }]})"""
    )

    expect(page.locator("#exportSubmitBtn")).to_be_enabled()
    expect(page.locator("#exportPreset")).to_be_enabled()
    expect(page.locator("#exportPresetSaveBtn")).to_be_enabled()
    expect(page.locator("#exportDest")).to_be_enabled()
    expect(page.locator("#exportFormat")).to_be_enabled()
    expect(page.locator("#exportTemplate")).to_be_enabled()
    expect(page.locator("#exportMetadataSpecies")).to_be_enabled()
    expect(page.locator("#exportDest")).to_have_value("/user-selected")
    expect(page.locator("#exportPreset")).to_have_value("custom")


def test_export_presets_ignore_stale_overlapping_refresh(live_server, page):
    """An older preset-list response cannot replace a newer response."""
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")

    page.evaluate(
        """() => {
          const realSafeFetch = window.safeFetch;
          window.__presetRefreshResolvers = [];
          window.safeFetch = function(url, options, config) {
            if (url === '/api/export/presets' && (!options || !options.method)) {
              return new Promise(function(resolve) {
                window.__presetRefreshResolvers.push(resolve);
              });
            }
            return realSafeFetch(url, options, config);
          };
          window.__presetRefreshes = [
            VireoExportPresets.modalOpened(),
            VireoExportPresets.modalOpened(),
          ];
        }"""
    )
    page.wait_for_function("() => window.__presetRefreshResolvers.length === 2")
    page.evaluate(
        """async () => {
          window.__presetRefreshResolvers[1]({presets: [{
            name: 'Current preset', settings: {}
          }]});
          await window.__presetRefreshes[1];
          window.__presetRefreshResolvers[0]({presets: [{
            name: 'Stale preset', settings: {}
          }]});
          await window.__presetRefreshes[0];
        }"""
    )

    expect(page.locator('option[value="saved:Current preset"]')).to_have_count(1)
    expect(page.locator('option[value="saved:Stale preset"]')).to_have_count(0)


def test_export_preset_persists_browse_preferences_on_apply(live_server, page):
    """Applying a preset must persist the fields the host tracks as prefs.

    Regression: ``applySettings`` uses ``.checked`` / ``.value`` assignments
    that don't fire input/change, so VireoViewPreferences never records the
    preset's subfolder, reveal, or metadata choices. If the user then
    changes any unrelated field (calls markCustom, flipping LAST_USED_KEY
    to 'custom') and closes and reopens, ``restoreAll()`` restores the
    stale pre-preset preferences and silently loses the preset-derived
    choices.
    """
    page.goto(f"{live_server['url']}/browse")
    # Prime the persisted preferences to the OPPOSITE of what the preset
    # will set, so a silent drop of the preset's values would fall back to
    # these values on reopen.
    page.evaluate(
        """() => {
          localStorage.setItem('vireo.browse.export.subfolder', '0');
          localStorage.setItem('vireo.browse.export.revealAfter', '1');
          localStorage.setItem('vireo.browse.export.metadata.rating', '0');
        }"""
    )
    page.reload()
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    # Seed a saved preset and tell the modal to auto-apply it on open.
    page.evaluate(
        """() => {
          window.__presetPayload = {presets: [{name: 'Preset A', settings: {
            export_to_subfolder: true,
            subfolder_name: 'from-preset',
            reveal_after_export: false,
            metadata_fields: ['rating', 'capture_date'],
          }}]};
          const realSafeFetch = window.safeFetch;
          window.safeFetch = function(url, options, config) {
            if (url === '/api/export/presets' &&
                (!options || !options.method || options.method === 'GET')) {
              return Promise.resolve(window.__presetPayload);
            }
            return realSafeFetch(url, options, config);
          };
          VireoViewPreferences.write('vireo.export.lastPreset', 'saved:Preset A');
        }"""
    )
    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportSubfolder")).to_be_checked()
    expect(page.locator("#exportSubfolderName")).to_have_value("from-preset")
    expect(page.locator("#exportRevealAfter")).not_to_be_checked()
    expect(page.locator("#exportMetadataRating")).to_be_checked()
    assert page.evaluate("() => selectedExportMetadataFields()") == [
        "capture_date", "rating",
    ]

    # Tweak an unrelated control (quality) so markCustom fires — this is
    # the trigger for the bug: LAST_USED_KEY becomes 'custom' and the next
    # reopen will restore preferences instead of the saved preset.
    page.locator("#exportQuality").fill("77")
    page.locator("#exportQuality").dispatch_event("input")
    expect(page.locator("#exportPreset")).to_have_value("custom")

    page.locator("#exportOverlay [data-export-cancel]").click()
    expect(page.locator("#exportOverlay")).not_to_have_class(
        "modal-overlay open"
    )

    # Reopen: restoreAll runs from the host, so the persisted preferences
    # decide what the controls show. They must match what the preset
    # actually put on screen, not the pre-preset seed values.
    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportSubfolder")).to_be_checked()
    expect(page.locator("#exportSubfolderName")).to_have_value("from-preset")
    expect(page.locator("#exportRevealAfter")).not_to_be_checked()
    expect(page.locator("#exportMetadataRating")).to_be_checked()
    assert page.evaluate("() => selectedExportMetadataFields()") == [
        "capture_date", "rating",
    ]


def test_export_presets_keep_submit_gated_when_saved_preset_load_fails(
    live_server, page,
):
    """A failed GET cannot unlock export with defaults in place of a preset."""
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")

    page.evaluate(
        """async () => {
          const realSafeFetch = window.safeFetch;
          window.safeFetch = function(url, options, config) {
            if (url === '/api/export/presets' && (!options || !options.method)) {
              return Promise.reject(new Error('offline'));
            }
            return realSafeFetch(url, options, config);
          };
          VireoViewPreferences.write(
            'vireo.export.lastPreset', 'saved:Unavailable preset'
          );
          await VireoExportPresets.modalOpened();
        }"""
    )

    expect(page.locator("#exportSubmitBtn")).to_be_disabled()


def test_export_preset_save_uses_in_page_dialog(live_server, page):
    """Desktop webviews suppress window.prompt(), so naming a preset has to
    happen in a visible in-page dialog and persist through the API."""
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    # A native prompt returns null instantly in the desktop webview, which is
    # what made Save… look dead. Fail loudly if the module reaches for one.
    page.evaluate(
        "() => { window.prompt = function() {"
        " throw new Error('native prompt is unavailable in the desktop app');"
        " }; }"
    )

    page.locator("#exportTemplate").fill("{original}_web")
    page.locator("#exportPresetSaveBtn").click()

    dialog = page.locator("#exportPresetDialog")
    expect(dialog).to_have_class(re.compile(r"\bopen\b"))
    expect(dialog.get_by_role("heading", name="Save export preset")).to_be_visible()

    # An empty name is refused in the dialog rather than silently discarded.
    page.locator("#exportPresetDialogSubmitBtn").click()
    expect(page.locator("#exportPresetDialogError")).to_have_text(
        "Enter a preset name."
    )
    expect(dialog).to_have_class(re.compile(r"\bopen\b"))

    page.locator("#exportPresetDialogName").fill("Web sized")
    page.locator("#exportPresetDialogSubmitBtn").click()

    expect(dialog).not_to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#exportPreset")).to_have_value("saved:Web sized")
    presets = page.evaluate(
        "async () => (await (await fetch('/api/export/presets')).json()).presets"
    )
    assert [preset["name"] for preset in presets] == ["Web sized"]
    assert presets[0]["settings"]["naming_template"] == "{original}_web"


def test_export_preset_dialog_keeps_keyboard_focus_inside(live_server, page):
    """Tab and Shift+Tab cycle within the dialog.

    The export modal underneath stays enabled, so an untrapped Shift+Tab
    reached its Cancel — closing ``#exportOverlay`` and orphaning this
    dialog — or its Export, starting an export mid-save.
    """
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")

    page.locator("#exportPresetSaveBtn").click()
    name = page.locator("#exportPresetDialogName")
    expect(name).to_be_focused()
    # The API rejects anything longer (MAX_EXPORT_PRESET_NAME_LEN in
    # vireo/export.py), so the field must not accept it either.
    assert name.get_attribute("maxlength") == "80"

    # Backwards off the first control wraps to the last, not into the export
    # modal; forwards off the last wraps back to the first.
    page.keyboard.press("Shift+Tab")
    expect(page.locator("#exportPresetDialogSubmitBtn")).to_be_focused()
    page.keyboard.press("Tab")
    expect(name).to_be_focused()

    for _ in range(6):
        page.keyboard.press("Tab")
        assert page.evaluate(
            "() => document.getElementById('exportPresetDialog')"
            ".contains(document.activeElement)"
        )
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")


def test_export_preset_save_dialog_names_the_preset_it_replaces(live_server, page):
    """Reusing a saved name says so before the click, not after."""
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    page.evaluate(
        """async () => {
          const realSafeFetch = window.safeFetch;
          window.safeFetch = function(url, options, config) {
            if (url === '/api/export/presets' && (!options || !options.method)) {
              return Promise.resolve({presets: [{
                name: 'Web sized', settings: {destination: '/web'}
              }]});
            }
            return realSafeFetch(url, options, config);
          };
          await VireoExportPresets.modalOpened();
        }"""
    )

    page.locator("#exportPresetSaveBtn").click()
    submit = page.locator("#exportPresetDialogSubmitBtn")
    description = page.locator("#exportPresetDialogDescription")
    page.locator("#exportPresetDialogName").fill("Something new")
    expect(submit).to_have_text("Save preset")
    expect(description).not_to_contain_text("Replaces")

    page.locator("#exportPresetDialogName").fill("Web sized")
    expect(submit).to_have_text("Replace preset")
    expect(description).to_contain_text("Replaces the saved preset “Web sized”")

    # Escape backs out and leaves the export modal and its settings alone.
    page.keyboard.press("Escape")
    expect(page.locator("#exportPresetDialog")).not_to_have_class(
        re.compile(r"\bopen\b")
    )
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")


def test_export_preset_delete_preserves_newer_selection(live_server, page):
    """A delayed delete completion cannot relabel a newer preset as Custom."""
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")

    page.evaluate(
        """async () => {
          const realSafeFetch = window.safeFetch;
          window.__deletedExportPreset = false;
          window.safeFetch = function(url, options, config) {
            if (url === '/api/export/presets' && (!options || !options.method)) {
              var presets = [{
                name: 'Keep me', settings: {destination: '/keep'}
              }];
              if (!window.__deletedExportPreset) {
                presets.unshift({
                  name: 'Delete me', settings: {destination: '/delete'}
                });
              }
              return Promise.resolve({presets: presets});
            }
            if (url === '/api/export/presets/Delete%20me' &&
                options && options.method === 'DELETE') {
              return new Promise(function(resolve) {
                window.__resolveExportPresetDelete = function() {
                  window.__deletedExportPreset = true;
                  resolve({});
                };
              });
            }
            return realSafeFetch(url, options, config);
          };
          await VireoExportPresets.modalOpened();
        }"""
    )
    page.locator("#exportPreset").select_option("saved:Delete me")
    page.locator("#exportPresetDeleteBtn").click()
    dialog = page.locator("#exportPresetDialog")
    expect(dialog).to_have_class(re.compile(r"\bopen\b"))
    expect(dialog.get_by_role("heading", name="Delete export preset")).to_be_visible()
    expect(page.locator("#exportPresetDialogDescription")).to_contain_text("Delete me")
    page.locator("#exportPresetDialogSubmitBtn").click()
    page.wait_for_function(
        "() => typeof window.__resolveExportPresetDelete === 'function'"
    )
    # The dialog covers the export modal while the delete is in flight, so a
    # user cannot move the dropdown underneath it any more. Force the change
    # anyway: the generation guard exists for programmatic mutations.
    page.locator("#exportPreset").select_option("saved:Keep me", force=True)
    page.evaluate("() => window.__resolveExportPresetDelete()")
    expect(dialog).not_to_have_class(re.compile(r"\bopen\b"))

    expect(page.locator("#exportPreset")).to_have_value("saved:Keep me")
    expect(page.locator("#exportDest")).to_have_value("/keep")
    assert page.evaluate(
        "() => VireoViewPreferences.read('vireo.export.lastPreset')"
    ) == "saved:Keep me"


def test_export_preset_uncached_replace_requires_confirmation(live_server, page):
    """A server-side name conflict is confirmed in-dialog before replacing."""
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")

    page.evaluate(
        """async () => {
          const realSafeFetch = window.safeFetch;
          window.__presetSaveBodies = [];
          window.__presetSaveSucceeded = false;
          window.safeFetch = function(url, options, config) {
            if (url === '/api/export/presets' && options &&
                options.method === 'POST') {
              var body = JSON.parse(options.body);
              window.__presetSaveBodies.push(body);
              if (!body.replace) {
                var conflict = new Error('already exists');
                conflict.code = 'export_preset_exists';
                return Promise.reject(conflict);
              }
              window.__presetSaveSucceeded = true;
              return Promise.resolve({ok: true, replaced: true});
            }
            if (url === '/api/export/presets' && (!options || !options.method)) {
              return Promise.resolve({presets: window.__presetSaveSucceeded ? [{
                name: 'Existing elsewhere', settings: {
                  destination: '', naming_template: '{original}_web'
                }
              }] : []});
            }
            return realSafeFetch(url, options, config);
          };
          await VireoExportPresets.modalOpened();
        }"""
    )
    page.locator("#exportTemplate").fill("  {original}_web  ")
    page.locator("#exportPresetSaveBtn").click()
    dialog = page.locator("#exportPresetDialog")
    expect(dialog).to_have_class(re.compile(r"\bopen\b"))
    page.locator("#exportPresetDialogName").fill("Existing elsewhere")
    # Nothing local knows the name is taken yet, so the dialog offers a save.
    expect(page.locator("#exportPresetDialogSubmitBtn")).to_have_text("Save preset")
    page.locator("#exportPresetDialogSubmitBtn").click()

    # The server rejects it: the dialog stays open and the next click is an
    # explicit replace rather than a silently overwritten preset.
    expect(page.locator("#exportPresetDialogError")).to_contain_text(
        "already exists"
    )
    expect(page.locator("#exportPresetDialogSubmitBtn")).to_have_text(
        "Replace preset"
    )
    expect(dialog).to_have_class(re.compile(r"\bopen\b"))
    page.locator("#exportPresetDialogSubmitBtn").click()

    expect(dialog).not_to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#exportPreset")).to_have_value(
        "saved:Existing elsewhere"
    )
    assert page.evaluate(
        "() => window.__presetSaveBodies.map(body => body.replace)"
    ) == [False, True]
    expect(page.locator("#exportTemplate")).to_have_value("{original}_web")


def test_export_preset_serializes_saves_to_same_name(live_server, page):
    """A second same-name save cannot overtake the first request.

    The dialog enforces it structurally: every control is disabled until the
    in-flight save settles, so there is no way to submit a second one.
    """
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")

    page.evaluate(
        """async () => {
          const realSafeFetch = window.safeFetch;
          window.__serializedSaveBodies = [];
          window.__serializedSaveResolvers = [];
          window.__serializedPresetGets = 0;
          window.safeFetch = function(url, options, config) {
            if (url === '/api/export/presets' && options &&
                options.method === 'POST') {
              window.__serializedSaveBodies.push(JSON.parse(options.body));
              return new Promise(function(resolve) {
                window.__serializedSaveResolvers.push(resolve);
              });
            }
            if (url === '/api/export/presets' && (!options || !options.method)) {
              window.__serializedPresetGets++;
              return Promise.resolve({presets: [{
                name: 'Shared name', settings: {
                  destination: document.getElementById('exportDest').value
                }
              }]});
            }
            return realSafeFetch(url, options, config);
          };
          await VireoExportPresets.modalOpened();
        }"""
    )
    dialog = page.locator("#exportPresetDialog")
    submit = page.locator("#exportPresetDialogSubmitBtn")

    page.locator("#exportDest").fill("/first")
    page.locator("#exportPresetSaveBtn").click()
    page.locator("#exportPresetDialogName").fill("Shared name")
    submit.click()
    page.wait_for_function("() => window.__serializedSaveBodies.length === 1")

    expect(submit).to_be_disabled()
    expect(page.locator("#exportPresetDialogCancelBtn")).to_be_disabled()
    expect(page.locator("#exportPresetDialogName")).to_be_disabled()
    submit.click(force=True)
    assert page.evaluate("() => window.__serializedSaveBodies.length") == 1

    page.evaluate("() => window.__serializedSaveResolvers[0]({ok: true})")
    page.wait_for_function("() => window.__serializedPresetGets === 2")
    expect(dialog).not_to_have_class(re.compile(r"\bopen\b"))

    page.locator("#exportDest").fill("/second")
    page.locator("#exportPresetSaveBtn").click()
    page.locator("#exportPresetDialogName").fill("Shared name")
    expect(submit).to_have_text("Replace preset")
    submit.click()
    page.wait_for_function("() => window.__serializedSaveBodies.length === 2")
    assert page.evaluate(
        "() => window.__serializedSaveBodies.map(body => body.settings.destination)"
    ) == ["/first", "/second"]


def test_export_preset_serializes_save_against_same_name_delete(live_server, page):
    """A replacement save cannot recreate a preset while deletion is pending."""
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")

    page.evaluate(
        """async () => {
          const realSafeFetch = window.safeFetch;
          window.__deleteRequests = 0;
          window.__saveRequestsDuringDelete = 0;
          window.safeFetch = function(url, options, config) {
            if (url === '/api/export/presets/Shared%20name' && options &&
                options.method === 'DELETE') {
              window.__deleteRequests++;
              return new Promise(function(resolve) {
                window.__resolveSharedPresetDelete = resolve;
              });
            }
            if (url === '/api/export/presets' && options &&
                options.method === 'POST') {
              window.__saveRequestsDuringDelete++;
              return Promise.resolve({ok: true});
            }
            if (url === '/api/export/presets' && (!options || !options.method)) {
              return Promise.resolve({presets: [{
                name: 'Shared name', settings: {destination: '/shared'}
              }]});
            }
            return realSafeFetch(url, options, config);
          };
          await VireoExportPresets.modalOpened();
        }"""
    )
    page.locator("#exportPreset").select_option("saved:Shared name")
    page.locator("#exportPresetDeleteBtn").click()
    dialog = page.locator("#exportPresetDialog")
    page.locator("#exportPresetDialogSubmitBtn").click()
    page.wait_for_function("() => window.__deleteRequests === 1")

    # Save… sits behind the dialog overlay, so a user cannot reach it while
    # the delete is in flight. Force the click: the module must still refuse
    # to recreate the preset it is deleting.
    expect(page.locator("#exportPresetDialogSubmitBtn")).to_be_disabled()
    page.locator("#exportPresetSaveBtn").click(force=True)
    expect(dialog.get_by_role("heading", name="Delete export preset")).to_be_visible()
    assert page.evaluate("() => window.__saveRequestsDuringDelete") == 0

    page.evaluate("() => window.__resolveSharedPresetDelete({ok: true})")
    expect(dialog).not_to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#exportPreset")).to_have_value("custom")


def test_export_preset_dropdown_reflects_custom_on_reopen(live_server, page):
    """Reopening after a custom edit shows Custom, not the host's built-in reset.

    Regression: the host resets ``#exportPreset`` to ``original-jpg`` before
    ``modalOpened()`` runs and then ``VireoViewPreferences.restoreAll`` puts
    persisted custom fields (subfolder toggle, metadata boxes, reveal-after)
    back on screen. When ``LAST_USED_KEY`` was ``custom``, ``modalOpened``
    used to return without snapping the dropdown back, so the modal
    advertised "Full-size JPEG" while the fields below no longer matched it.
    """
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    # Simulate a manual tweak: check the subfolder box, which fires the
    # delegated input handler and calls markCustom() -> LAST_USED_KEY=custom.
    page.locator("#exportSubfolder").check()
    expect(page.locator("#exportPreset")).to_have_value("custom")

    page.locator("#exportOverlay [data-export-cancel]").click()
    expect(page.locator("#exportOverlay")).not_to_have_class(
        "modal-overlay open"
    )

    first.click()
    page.get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    # Persisted view preference restored the checkbox, so the dropdown must
    # follow instead of falsely displaying the built-in reset default.
    expect(page.locator("#exportSubfolder")).to_be_checked()
    expect(page.locator("#exportPreset")).to_have_value("custom")


def test_export_checkboxes_remember_the_previous_choices(live_server, page):
    """Export checkbox choices survive closing the dialog and reloading Browse."""
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.get_by_role("button", name="Export", exact=True).click()

    remembered = {
        "exportSubfolder": True,
        "exportRevealAfter": True,
        "exportMetadataSpecies": True,
        "exportMetadataCaptureDateTime": True,
        "exportMetadataRating": False,
        "exportMetadataLocation": True,
        "exportMetadataCamera": False,
    }
    for control_id, checked in remembered.items():
        page.locator(f"#{control_id}").set_checked(checked)

    page.get_by_role("button", name="Cancel", exact=True).click()
    page.get_by_role("button", name="Export", exact=True).click()
    for control_id, checked in remembered.items():
        expect(page.locator(f"#{control_id}")).to_be_checked(checked=checked)

    page.reload()
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.get_by_role("button", name="Export", exact=True).click()
    for control_id, checked in remembered.items():
        expect(page.locator(f"#{control_id}")).to_be_checked(checked=checked)


def test_export_capture_preference_migrates_to_combined_checkbox(live_server, page):
    """Either legacy capture choice keeps capture metadata enabled on upgrade."""
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        """() => {
          localStorage.removeItem('vireo.browse.export.metadata.captureDateTime');
          localStorage.setItem('vireo.browse.export.metadata.captureDate', '0');
          localStorage.setItem('vireo.browse.export.metadata.captureTime', '1');
        }"""
    )
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    page.get_by_role("button", name="Export", exact=True).click()

    expect(page.locator("#exportMetadataCaptureDateTime")).to_be_checked()
    assert page.evaluate(
        "localStorage.getItem('vireo.browse.export.metadata.captureDateTime')"
    ) == "1"


def test_more_menu_scrolls_within_short_viewport(live_server, page):
    """The unified action menu is tall; at Tauri's 600px minimum window
    height it must scroll within the viewport instead of rendering lower
    actions (Prepare Full Resolution, Export, Delete) off-screen where they
    cannot be clicked.
    """
    page.set_viewport_size({"width": 1100, "height": 600})
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    page.locator("#batchMoreBtn").click()
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    box = menu.bounding_box()
    assert box["y"] >= 0
    assert box["y"] + box["height"] <= 600

    delete_item = page.locator(".vireo-ctx-menu .vireo-ctx-item", has_text="Delete")
    delete_item.scroll_into_view_if_needed()
    expect(delete_item).to_be_visible()


def test_batch_bar_wraps_at_minimum_window_width(live_server, page):
    """At Tauri's 800px minimum window width the fixed sidebar leaves ~533px
    of content width. The batch bar must wrap rather than clip, so More —
    the entry point for every action removed from the bar — stays reachable.
    """
    page.set_viewport_size({"width": 800, "height": 600})
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    more = page.locator("#batchMoreBtn")
    expect(more).to_be_visible()
    box = more.bounding_box()
    assert box["x"] >= 0
    assert box["x"] + box["width"] <= 800

    more.click()
    expect(page.locator(".vireo-ctx-menu")).to_be_visible()
    page.keyboard.press("Escape")


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

    click_more_menu_item(page, "Prepare Full Resolution")

    page.wait_for_function(
        "() => window._prepareFullResolutionJobId === null"
    )
    assert submitted == [{"photo_ids": [selected_id]}]


def test_prepare_full_resolution_surfaces_fatal_job_failure(live_server, page):
    page.route(
        "**/api/jobs/prepare-full-resolution",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"job_id": "prepare-failed-test", "total": 1}),
        ),
    )
    page.route(
        "**/api/jobs/prepare-failed-test/stream",
        lambda route: route.fulfill(
            status=200,
            content_type="text/event-stream",
            body=(
                "event: complete\n"
                "data: {\"status\":\"failed\",\"result\":null,"
                "\"errors\":[\"database unavailable\"]}\n\n"
            ),
        ),
    )

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.locator(".grid-card").first.click()
    click_more_menu_item(page, "Prepare Full Resolution")

    page.wait_for_function(
        "() => window._prepareFullResolutionJobId === null"
    )
    expect(page.locator("#toastContainer > div").last).to_have_text(
        "Full-resolution preparation failed: database unavailable"
    )


def test_prepare_full_resolution_summarizes_partial_failure(live_server, page):
    page.route(
        "**/api/jobs/prepare-full-resolution",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"job_id": "prepare-partial-test", "total": 3}),
        ),
    )
    page.route(
        "**/api/jobs/prepare-partial-test/stream",
        lambda route: route.fulfill(
            status=200,
            content_type="text/event-stream",
            body=(
                "event: complete\n"
                "data: {\"status\":\"failed\",\"result\":{"
                "\"ready\":2,\"copied\":2,\"failed\":1},"
                "\"errors\":[\"one source was unavailable\"]}\n\n"
            ),
        ),
    )

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.locator(".grid-card").first.click()
    click_more_menu_item(page, "Prepare Full Resolution")

    page.wait_for_function(
        "() => window._prepareFullResolutionJobId === null"
    )
    expect(page.locator("#toastContainer > div").last).to_have_text(
        "Full-resolution preparation complete: 2 ready, 2 copied locally, "
        "1 failed"
    )


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


def test_multiselect_keyword_picker_shows_and_applies_suggestion(live_server, page):
    """Batch keyword autocomplete must escape the modal and tag every photo."""
    db = live_server["db"]
    selected_ids = live_server["data"]["photos"][:3]
    keyword_names = [
        "Batch suggestion Alder",
        "Batch suggestion Birch",
        "Batch suggestion Cedar",
        "Batch suggestion Dogwood",
    ]
    for name in keyword_names:
        keyword_id = db.add_keyword(name)
        db.tag_photo(live_server["data"]["photos"][-1], keyword_id)

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.evaluate(
        """
        photoIds => {
          selectedPhotos.clear();
          photoIds.forEach(id => selectedPhotos.add(id));
          selectedPhotoId = null;
          renderGrid();
          updateBatchBar();
        }
        """,
        selected_ids,
    )

    page.locator("#batchBar button", has_text="+ Keyword").click()
    page.locator("#batchKeywordInput").fill("Batch suggestion")

    suggestions = page.locator(
        "#batchKeywordSuggestions .keyword-suggestion-option"
    )
    expect(suggestions).to_have_count(4)
    last_suggestion = suggestions.filter(has_text=keyword_names[-1])
    expect(last_suggestion).to_be_visible()

    # The short batch modal used to clip the absolutely positioned list near
    # its action buttons. Verify the final match is actually paintable and
    # clickable, not merely present in the DOM outside the clipping box.
    assert last_suggestion.evaluate(
        """
        option => {
          const rect = option.getBoundingClientRect();
          const hit = document.elementFromPoint(
            rect.left + rect.width / 2,
            rect.top + rect.height / 2
          );
          return hit === option || option.contains(hit);
        }
        """
    )

    with page.expect_response(
        lambda r: "/api/batch/keyword" in r.url
        and r.request.method == "POST"
        and r.status == 200
    ):
        last_suggestion.click()

    page.wait_for_function(
        """
        async ({photoIds, keywordName}) => {
          const details = await Promise.all(
            photoIds.map(id => fetch('/api/photos/' + id).then(r => r.json()))
          );
          return details.every(photo =>
            (photo.keywords || []).some(keyword => keyword.name === keywordName)
          );
        }
        """,
        arg={"photoIds": selected_ids, "keywordName": keyword_names[-1]},
    )


def test_species_badge_tracks_detail_keyword_edits_without_reload(live_server, page):
    """The grid's taxonomy badge must stay in sync with detail keyword edits."""
    db = live_server["db"]
    source_id = live_server["data"]["photos"][0]
    target_id = live_server["data"]["photos"][1]
    old_name = "Hawaii Creeper"
    new_name = "Hawaii Amakihi"
    old_id = db.add_keyword(old_name, kw_type="taxonomy")
    new_id = db.add_keyword(new_name, kw_type="taxonomy")
    db.tag_photo(target_id, old_id)
    db.tag_photo(source_id, new_id)

    page.goto(f"{live_server['url']}/browse")
    card = page.locator(f'.grid-card[data-id="{target_id}"]')
    expect(card.locator(".grid-card-img-wrap .species-badge")).to_have_text(
        old_name
    )
    card.click()

    old_tag = page.locator("#detailKeywords .keyword-tag", has_text=old_name)
    expect(old_tag).to_be_visible()
    with page.expect_response(
        lambda r: f"/api/photos/{target_id}/keywords/{old_id}" in r.url
        and r.request.method == "DELETE"
        and r.status == 200
    ):
        old_tag.locator(".remove-kw").click()

    expect(card.locator(".grid-card-img-wrap .species-badge")).to_have_count(0)
    expect(old_tag).to_have_count(0)

    keyword_input = page.locator("#addKeywordInput")
    keyword_input.fill(new_name)
    suggestion = page.locator(
        "#addKeywordSuggestions .keyword-suggestion-option",
        has_text=new_name,
    )
    expect(suggestion).to_be_visible()
    with page.expect_response(
        lambda r: f"/api/photos/{target_id}/keywords" in r.url
        and r.request.method == "POST"
        and r.status == 200
    ):
        suggestion.click()

    expect(card.locator(".grid-card-img-wrap .species-badge")).to_have_text(
        new_name
    )
    expect(card.locator(".grid-card-img-wrap .species-badge")).not_to_contain_text(
        old_name
    )


def test_needs_identification_refreshes_after_identification_added(live_server, page):
    """A photo should leave the active Needs Identification grid after tagging."""
    db = live_server["db"]
    target_id = live_server["data"]["photos"][1]
    collection = db.conn.execute(
        "SELECT id, rules FROM collections WHERE name = 'Needs Identification'"
    ).fetchone()
    assert collection is not None
    collection_id = collection["id"]
    collection_rules = json.loads(collection["rules"])

    def is_collection_query(response):
        if (
            "/api/photos/query" not in response.url
            or response.request.method != "POST"
            or response.status != 200
        ):
            return False
        body = response.request.post_data_json or {}
        rules = body.get("rules") or {}
        submitted_rules = rules.get("rules", []) if isinstance(rules, dict) else rules
        return submitted_rules == collection_rules

    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function("window.VireoFilter && VireoFilter.isReady()")
    with page.expect_response(is_collection_query):
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
        # Collections open into the filter bar now; membership-change
        # refreshes re-evaluate the expression through the query path.
        is_collection_query
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


def test_arrow_navigation_loads_past_trailing_offline_photo(live_server, page):
    """Keyboard navigation loads again after skipping an offline page tail."""
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    result = page.evaluate(
        """async () => {
          const originalPhotos = photos;
          const originalSelectedIndex = selectedIndex;
          const originalAllLoaded = allLoaded;
          const originalLoadPhotos = loadPhotos;
          const originalSelectPhoto = selectPhoto;
          const originalScrollToCard = scrollToCard;
          let loads = 0;
          let selected = null;
          try {
            photos = [
              {id: 201, folder_status: 'ok'},
              {id: 202, folder_status: 'missing'}
            ];
            selectedIndex = 0;
            allLoaded = false;
            loadPhotos = async function() {
              loads += 1;
              photos.push({id: 203, folder_status: 'ok'});
              allLoaded = true;
            };
            selectPhoto = function(event, id, index) {
              selected = {id, index};
              selectedIndex = index;
            };
            scrollToCard = function() {};

            await moveBrowseSelection(1, {});
            return {loads, selected};
          } finally {
            photos = originalPhotos;
            selectedIndex = originalSelectedIndex;
            allLoaded = originalAllLoaded;
            loadPhotos = originalLoadPhotos;
            selectPhoto = originalSelectPhoto;
            scrollToCard = originalScrollToCard;
          }
        }"""
    )

    assert result == {"loads": 1, "selected": {"id": 203, "index": 2}}


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
    row = page.locator(".selection-keyword-row", has_text="Red-tailed Hawk")
    expect(row).to_be_visible()
    expect(row).to_contain_text("On 5 of 5")
    expect(row.locator("button", has_text="Add to")).to_have_count(0)
    expect(row.locator("button", has_text="Remove from 5")).to_be_visible()

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


def test_multiselect_shows_and_removes_keyword_shared_by_all_photos(live_server, page):
    """A keyword shared by the selection remains visible and removable."""
    db = live_server["db"]
    selected_ids = live_server["data"]["photos"]
    keyword_name = "Shared selection keyword"
    keyword_id = db.add_keyword(keyword_name)
    for photo_id in selected_ids:
        db.tag_photo(photo_id, keyword_id)

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.evaluate("""
      photos.forEach(function(p) { selectedPhotos.add(p.id); });
      renderGrid();
      updateBatchBar();
    """)

    row = page.locator(".selection-keyword-row", has_text=keyword_name)
    expect(row).to_be_visible()
    expect(row).to_contain_text("On 5 of 5")
    expect(row.locator("button", has_text="Add to")).to_have_count(0)
    remove_button = row.locator("button", has_text="Remove from 5")
    expect(remove_button).to_be_visible()

    remove_button.click()
    page.wait_for_function(
        """
        async ({photoIds, keywordId}) => {
          const details = await Promise.all(
            photoIds.map(id => fetch('/api/photos/' + id).then(r => r.json()))
          );
          return details.every(p =>
            !(p.keywords || []).some(k => k.id === keywordId)
          );
        }
        """,
        arg={"photoIds": selected_ids, "keywordId": keyword_id},
    )
    expect(row).to_have_count(0)


def test_multiselect_groups_metadata_and_bulk_updates_wildlife_workflow(
    live_server, page,
):
    db = live_server["db"]
    selected_ids = live_server["data"]["photos"]
    species_id = db.add_keyword("House Sparrow", is_species=True)
    location_id = db.add_keyword("Grangettes nature reserve", kw_type="location")
    for photo_id in selected_ids:
        db.tag_photo(photo_id, species_id)
        db.tag_photo(photo_id, location_id)

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.evaluate("""
      photos.forEach(function(p) { selectedPhotos.add(p.id); });
      renderGrid();
      updateBatchBar();
    """)

    taxonomy = page.locator(
        '.selection-keyword-group[data-keyword-type="taxonomy"]'
    )
    location = page.locator(
        '.selection-keyword-group[data-keyword-type="location"]'
    )
    expect(taxonomy.locator(".selection-keyword-group-title")).to_have_text(
        "Species"
    )
    expect(taxonomy).to_contain_text("House Sparrow")
    expect(location.locator(".selection-keyword-group-title")).to_have_text(
        "Locations"
    )
    expect(location).to_contain_text("Grangettes nature reserve")

    exclude = page.locator("#selectionWildlifeActions button", has_text="Exclude 5")
    expect(exclude).to_be_visible()
    exclude.click()
    page.wait_for_function(
        """
        async (photoIds) => {
          const details = await Promise.all(
            photoIds.map(id => fetch('/api/photos/' + id).then(r => r.json()))
          );
          return details.every(photo => photo.wildlife_excluded);
        }
        """,
        arg=selected_ids,
    )
    include = page.locator("#selectionWildlifeActions button", has_text="Include 5")
    expect(include).to_be_visible()


def test_wildlife_batch_completion_refreshes_current_selection(live_server, page):
    """A slow batch response must not repaint controls for the old selection."""
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.evaluate("""
      selectedPhotoId = null;
      selectedPhotos.clear();
      photos.slice(0, 2).forEach(function(photo) {
        selectedPhotos.add(photo.id);
      });
      updateBatchBar();
    """)
    expect(
        page.locator("#selectionWildlifeActions button", has_text="Exclude 2")
    ).to_be_visible()

    page.evaluate("""
      window.__originalApiJson = window.Vireo.api.json;
      window.Vireo.api.json = function(url, opts, options) {
        if (url === '/api/batch/wildlife-excluded') {
          return new Promise(function(resolve, reject) {
            window.__releaseWildlifeBatch = function() {
              window.__originalApiJson(url, opts, options).then(resolve, reject);
            };
          });
        }
        return window.__originalApiJson(url, opts, options);
      };
      window.__wildlifeBatchDone = false;
      setSelectionWildlifeExcluded(true).finally(function() {
        window.__wildlifeBatchDone = true;
      });
      void 0;
    """)
    page.wait_for_function("typeof window.__releaseWildlifeBatch === 'function'")

    page.evaluate("""
      selectedPhotos.clear();
      photos.slice(2, 5).forEach(function(photo) {
        selectedPhotos.add(photo.id);
      });
      updateBatchBar();
    """)
    current_action = page.locator(
        "#selectionWildlifeActions button", has_text="Exclude 3"
    )
    expect(current_action).to_be_visible()

    page.evaluate("window.__releaseWildlifeBatch()")
    page.wait_for_function("window.__wildlifeBatchDone === true")
    expect(current_action).to_be_visible()


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
    # Collections now open INTO the filter bar as an editable expression
    # (Phase 5) — the reload goes through resetAndLoad, which clears the
    # selection; the chips prove the collection's rules were applied.
    page.evaluate("loadCollections()")
    page.wait_for_function(
        f"window.collectionsById && collectionsById[{collection_id}]", timeout=4000
    )
    page.evaluate(f"filterByCollection({collection_id})")
    page.wait_for_function(
        "document.querySelector('.vf-chips') && "
        "document.querySelector('.vf-chips').textContent.includes('File extension')",
        timeout=4000,
    )

    page.wait_for_function("selectedPhotos.size === 0", timeout=4000)
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

    # Text sitting in the quick-search box (no Enter yet) must not apply
    # later and overwrite the opened collection's expression.
    page.evaluate("loadCollections()")
    page.wait_for_function(
        f"window.collectionsById && collectionsById[{collection_id}]", timeout=4000
    )
    page.locator(".vf-search input").fill("hum")
    page.evaluate(f"filterByCollection({collection_id})")
    page.wait_for_function(
        "document.querySelector('.vf-chips') && "
        "document.querySelector('.vf-chips').textContent.includes('File extension')",
        timeout=4000,
    )

    page.wait_for_timeout(350)
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "File extension" in chips and "hum" not in chips
