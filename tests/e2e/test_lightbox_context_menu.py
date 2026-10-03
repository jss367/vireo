"""E2E tests for the lightbox right-click context menu.

These tests exercise the shared `openContextMenu` handler wired to the
lightbox `<img id="lightboxImg">`. The E2E seed fixture creates photo rows
but the underlying files don't exist on disk, so `/photos/<id>/full` returns
500 and the image element never gets a non-zero size. We dispatch the
`contextmenu` event directly rather than using Playwright's
`click(button="right")` which requires visibility/stability.
"""
from playwright.sync_api import expect


def _open_lightbox(page, url):
    """Navigate to browse and open the lightbox on the first grid card."""
    page.goto(f"{url}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.dblclick()
    # Overlay becomes active synchronously once openLightbox() runs.
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )
    # Wait until `vireoLightboxSession.requestedPhotoId()` is populated (openLightbox assigns it
    # synchronously, but the dblclick → handler chain is async from pytest's
    # point of view).
    page.wait_for_function(
        "typeof vireoLightboxSession.requestedPhotoId() !== 'undefined' && vireoLightboxSession.requestedPhotoId() !== null",
        timeout=3000,
    )


def _fire_contextmenu_on_lightbox(page):
    """Dispatch a contextmenu event on the lightbox image.

    Using dispatch_event bypasses visibility checks (the underlying image
    never loads in the test harness because the photo file doesn't exist
    on disk). The event still reaches the handler, which is what matters.
    """
    page.evaluate(
        """
        const img = document.getElementById('lightboxImg');
        const evt = new MouseEvent('contextmenu', {
            bubbles: true, cancelable: true, clientX: 400, clientY: 300,
            button: 2,
        });
        img.dispatchEvent(evt);
        """
    )


def test_lightbox_right_click_opens_menu(live_server, page):
    """Right-clicking the lightbox image opens the shared context menu."""
    url = live_server["url"]
    _open_lightbox(page, url)

    # Keep the uncommon workflow out of the always-visible action bar. It
    # remains available from the contextual menu below.
    expect(page.locator("#lightboxNotWildlife")).to_have_count(0)

    _fire_contextmenu_on_lightbox(page)
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    # A handful of the lightbox-specific menu items.
    expect(
        menu.locator(".vireo-ctx-item", has_text="Reveal in")
    ).to_be_visible()
    expect(
        menu.locator(".vireo-ctx-item", has_text="Copy Path")
    ).to_be_visible()
    expect(
        menu.locator(".vireo-ctx-item", has_text="Export…")
    ).to_be_visible()
    expect(
        menu.locator(".vireo-ctx-item", has_text="Close Lightbox")
    ).to_be_visible()
    expect(
        menu.locator(".vireo-ctx-item", has_text="Wildlife classification")
    ).to_be_visible()
    # Rating / color / flag chip rows are present (14 chips total).
    assert menu.locator(".vireo-ctx-chip").count() > 5


def test_lightbox_export_resolves_photo_when_action_is_invoked(live_server, page):
    """Lightbox Export targets the displayed photo even after menu-open nav."""
    url = live_server["url"]
    _open_lightbox(page, url)

    current_id = page.evaluate("vireoLightboxSession.requestedPhotoId()")
    page.evaluate(
        """() => {
            selectedPhotos.clear();
            selectedPhotoId = vireoLightboxSession.requestedPhotoId();
            window.__exportRequest = null;
            window.__resolveExport = null;
            window.__rejectPreflight = true;
            window.safeFetch = async function(url, options) {
                if (url === '/api/jobs/export/preflight') {
                    // The modal's live filename check sends only naming
                    // settings; the check Export runs also carries quality.
                    if (!('quality' in JSON.parse(options.body))) {
                        return {rename_count: 0, renames: []};
                    }
                    if (window.__rejectPreflight) {
                        window.__rejectPreflight = false;
                        throw new Error('probe denied');
                    }
                    return {rename_count: 0, renames: []};
                }
                if (url === '/api/jobs/export') {
                    window.__exportRequest = JSON.parse(options.body);
                    return new Promise(function(resolve) {
                        window.__resolveExport = function() {
                            resolve({job_id: 'lightbox-export-test'});
                        };
                    });
                }
                return {};
            };
        }"""
    )

    _fire_contextmenu_on_lightbox(page)
    export_photo = page.evaluate(
        """() => {
            const next = photos.find(photo => photo.id !== vireoLightboxSession.requestedPhotoId());
            vireoLightboxSession.begin(next.id);
            vireoLightboxSession.commit(vireoLightboxSession.capture());
            // Simulate opening a Find Similar result that is not present in
            // Browse's currently loaded page. The lightbox retains its own
            // photo list for metadata-backed export previews.
            photos = photos.filter(photo => photo.id !== next.id);
            return {id: next.id, filename: next.filename};
        }"""
    )
    menu = page.locator(".vireo-ctx-menu")
    menu.locator(".vireo-ctx-item", has_text="Export…").click()

    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    expect(page.locator("#exportOverlay")).to_be_visible()
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay")
    expect(page.locator("#exportSubmitBtn")).to_have_text("Export 1 photo")
    expect(page.locator("#exportPreview")).to_contain_text(
        export_photo["filename"].rsplit(".", 1)[0]
    )
    assert export_photo["id"] != current_id
    assert page.evaluate("_exportPhotoIds") == [export_photo["id"]]

    dialog_messages = []

    def accept_preflight_error(dialog):
        dialog_messages.append(dialog.message)
        dialog.accept()

    page.once("dialog", accept_preflight_error)
    page.locator("#exportSubmitBtn").click()
    assert dialog_messages == ["Export check failed: probe denied"]
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    expect(page.get_by_role("button", name="Cancel", exact=True)).to_be_enabled()
    expect(page.locator("#exportSubmitBtn")).to_be_enabled()
    assert page.evaluate("window.__exportRequest") is None

    page.locator("#exportSubmitBtn").click()
    page.wait_for_function("window.__exportRequest !== null")
    assert page.evaluate("window.__exportRequest.photo_ids") == [export_photo["id"]]
    expect(page.get_by_role("button", name="Cancel", exact=True)).to_be_disabled()
    page.keyboard.press("Escape")
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    page.evaluate("document.getElementById('exportOverlay').click()")
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    page.evaluate("window.__resolveExport()")
    expect(page.locator("#exportOverlay")).not_to_have_class("modal-overlay open")


def test_lightbox_color_description_opens_editor(live_server, page):
    """Lightbox color chips show descriptions and open the shared editor."""
    url = live_server["url"]
    response = page.request.put(
        f"{url}/api/color-label-descriptions/blue",
        data={"description": "Waterbirds"},
    )
    assert response.ok

    _open_lightbox(page, url)
    _fire_contextmenu_on_lightbox(page)

    blue = page.locator('.vireo-ctx-chip[data-color="blue"]')
    expect(blue).to_have_attribute(
        "title", "Blue — Waterbirds · Right-click to edit"
    )
    # Regression: context-menu chips built after the description finished
    # loading used to keep the hardcoded "Blue label" aria-label because
    # no later refreshControls() pass revisited them.
    expect(blue).to_have_attribute("aria-label", "Blue label: Waterbirds")
    expect(page.locator('.vireo-ctx-chip[data-color="purple"]')).to_be_visible()

    blue.click(button="right")
    expect(page.locator("#colorLabelDescriptionModal")).to_have_class(
        "modal-overlay open"
    )
    expect(page.locator("#colorLabelDescriptionInput")).to_have_value("Waterbirds")
    # The editor must stack above the still-open lightbox overlay (z-index
    # 9999) or the newly added flow is invisible to the user.
    stacking = page.evaluate(
        """() => {
            const modal = getComputedStyle(
                document.getElementById('colorLabelDescriptionModal')
            ).zIndex;
            const lb = getComputedStyle(
                document.getElementById('lightboxOverlay')
            ).zIndex;
            return { modal: Number(modal), lightbox: Number(lb) };
        }"""
    )
    assert stacking["modal"] > stacking["lightbox"], stacking


def test_lightbox_menu_sets_species_representative(live_server, page):
    """The shared lightbox menu can set the current photo as representative."""
    url = live_server["url"]
    hawk = live_server["data"]["photos"][0]
    _open_lightbox(page, url)
    page.wait_for_function(
        """() => {
            const id = vireoLightboxSession.requestedPhotoId();
            const data = window._lbPhotoDataByPhoto && window._lbPhotoDataByPhoto[String(id)];
            return data && data.life_list && data.life_list.length > 0;
        }""",
        timeout=3000,
    )

    _fire_contextmenu_on_lightbox(page)
    item = page.locator(
        ".vireo-ctx-item",
        has_text="Set Representative — Red-tailed Hawk",
    )
    expect(item).to_be_visible()
    with page.expect_response(
        lambda r: "/api/photo-preferences" in r.url and r.status == 200
    ):
        item.click()

    life_list = page.evaluate(
        """async (pid) => {
            const r = await fetch('/api/photos/' + pid);
            const d = await r.json();
            return d.life_list;
        }""",
        hawk,
    )
    assert life_list[0]["is_current_photo"] is True
    assert life_list[0]["is_species_representative"] is True


def test_lightbox_menu_adds_species_highlight(live_server, page):
    """The shared lightbox menu can add the current photo to Highlights."""
    db = live_server["db"]
    url = live_server["url"]
    hawk = live_server["data"]["photos"][0]
    db.conn.execute("UPDATE photos SET quality_score = 0.9 WHERE id = ?", (hawk,))
    db.conn.commit()

    _open_lightbox(page, url)
    page.wait_for_function(
        """() => {
            const id = vireoLightboxSession.requestedPhotoId();
            const data = window._lbPhotoDataByPhoto && window._lbPhotoDataByPhoto[String(id)];
            return data && data.highlight_list && data.highlight_list.length > 0;
        }""",
        timeout=3000,
    )

    _fire_contextmenu_on_lightbox(page)
    item = page.locator(
        ".vireo-ctx-item",
        has_text="Add to Highlights — Red-tailed Hawk",
    )
    expect(item).to_be_visible()
    with page.expect_response(
        lambda r: "/api/species-highlights" in r.url and r.status == 200
    ):
        item.click()

    highlight_list = page.evaluate(
        """async (pid) => {
            const r = await fetch('/api/photos/' + pid);
            const d = await r.json();
            return d.highlight_list;
        }""",
        hawk,
    )
    assert highlight_list[0]["is_highlighted"] is True
    assert highlight_list[0]["highlight_rank"] == 1


def test_lightbox_overlay_toggles_persist_and_context_restores(live_server, page):
    """Lightbox overlay visibility toggles persist and stay recoverable.

    Defaults with no stored preference: boxes/eye/masks hidden, info/chrome
    visible. One click on each toggle therefore flips boxes, eye, and masks ON
    but flips info and chrome OFF.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.evaluate(
        """
        [
          'vireo.lb.boxesVisible',
          'vireo.lb.masksVisible',
          'vireo.lb.eyeVisible',
          'vireo.lb.infoVisible',
          'vireo.lb.chromeVisible',
        ].forEach(k => localStorage.removeItem(k));
        """
    )
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.dblclick()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )

    # Visibility toggles live in the View menu popover.
    page.locator("#lightboxViewBtn").click()
    page.locator("#lightboxToggleBoxes").click()
    page.locator("#lightboxToggleMasks").click()
    page.locator("#lightboxToggleEye").click()
    page.locator("#lightboxToggleInfo").click()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('lb-hide-info')",
        timeout=2000,
    )

    page.locator("#lightboxToggleChrome").click()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('lb-hide-chrome')",
        timeout=2000,
    )
    assert page.evaluate("localStorage.getItem('vireo.lb.boxesVisible')") == "1"
    assert page.evaluate("localStorage.getItem('vireo.lb.masksVisible')") == "1"
    assert page.evaluate("localStorage.getItem('vireo.lb.eyeVisible')") == "1"
    assert page.evaluate("localStorage.getItem('vireo.lb.infoVisible')") == "0"
    assert page.evaluate("localStorage.getItem('vireo.lb.chromeVisible')") == "0"

    page.keyboard.press("h")
    page.wait_for_function(
        "!document.getElementById('lightboxOverlay').classList.contains('lb-hide-chrome')",
        timeout=2000,
    )
    assert page.evaluate("localStorage.getItem('vireo.lb.chromeVisible')") == "1"

    page.keyboard.press("h")
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('lb-hide-chrome')",
        timeout=2000,
    )
    assert page.evaluate("localStorage.getItem('vireo.lb.chromeVisible')") == "0"

    _fire_contextmenu_on_lightbox(page)
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu.locator(".vireo-ctx-item", has_text="Lightbox controls: Off").click()
    page.wait_for_function(
        "!document.getElementById('lightboxOverlay').classList.contains('lb-hide-chrome')",
        timeout=2000,
    )
    assert page.evaluate("localStorage.getItem('vireo.lb.chromeVisible')") == "1"

    page.evaluate("closeLightbox()")
    page.reload()
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.dblclick()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('lb-hide-info')",
        timeout=2000,
    )
    page.locator("#lightboxViewBtn").click()
    expect(page.locator("#lightboxToggleBoxes")).to_be_checked()
    expect(page.locator("#lightboxToggleMasks")).to_be_checked()
    expect(page.locator("#lightboxToggleEye")).to_be_checked()


def test_lightbox_right_click_does_not_toggle_zoom(live_server, page):
    """Right-click must not trip the click-to-zoom / pan handlers.

    The lightbox exposes the current zoom level via `vireoLightboxViewport.zoom()`. A contextmenu
    event must not change zoom state.
    """
    url = live_server["url"]
    _open_lightbox(page, url)

    before = page.evaluate(
        "typeof vireoLightboxViewport.zoom() !== 'undefined' ? vireoLightboxViewport.zoom() : null"
    )
    _fire_contextmenu_on_lightbox(page)
    expect(page.locator(".vireo-ctx-menu")).to_be_visible()
    after = page.evaluate(
        "typeof vireoLightboxViewport.zoom() !== 'undefined' ? vireoLightboxViewport.zoom() : null"
    )
    assert before == after


def test_mask_toggle_off_clears_pending_onload(live_server, page):
    """Hiding masks while a mask image request is in flight must not let the
    pending onload handler re-add `show` after the user toggled off.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.evaluate("localStorage.removeItem('vireo.lb.masksVisible');")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.dblclick()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )

    # Simulate the "request in flight" state: toggle on with a URL set,
    # then call _lbApplyMaskVisibility() so it assigns img.onload.
    page.evaluate(
        """
        _lbMasksVisible = true;
        _lbMaskCurrentUrl = '/dummy-mask-url-for-test.png';
        _lbApplyMaskVisibility();
        """
    )
    assert page.evaluate(
        "typeof document.getElementById('lightboxMaskOverlay').onload === 'function'"
    ), "onload should be assigned while a mask request is in flight"

    # User toggles masks off while the load is still pending.
    page.evaluate(
        """
        _lbMasksVisible = false;
        _lbApplyMaskVisibility();
        """
    )
    assert page.evaluate(
        "document.getElementById('lightboxMaskOverlay').onload === null"
    ), "onload must be cleared when masks are toggled off mid-load"

    # Even if a late load fires (simulated by invoking the stored handler
    # captured before the toggle, or any direct .add('show') from a leftover
    # callback), the overlay must remain hidden. Manually re-arm the handler
    # the way the old code did and fire it — defense-in-depth: the handler
    # itself also re-checks visibility before adding `show`.
    page.evaluate(
        """
        const img = document.getElementById('lightboxMaskOverlay');
        // Re-arm as the old buggy assignment used to.
        img.onload = function() {
          if (_lbMasksVisible && _lbMaskCurrentUrl) {
            img.classList.add('show');
          }
        };
        img.onload();
        """
    )
    assert not page.locator("#lightboxMaskOverlay").evaluate(
        "el => el.classList.contains('show')"
    ), "mask overlay must stay hidden after a late load fires while toggled off"


def test_mask_toggle_off_then_on_after_failed_load_stays_hidden(live_server, page):
    """Toggling masks off then back on after a failed load must not surface
    a broken-image overlay through the same-URL fast path.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.evaluate("localStorage.removeItem('vireo.lb.masksVisible');")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.dblclick()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )

    # Drive the overlay to a known failed-load state via a real 404 URL.
    # _lbApplyMaskVisibility() assigns onerror which removes `show`.
    page.evaluate(
        """
        _lbMasksVisible = true;
        _lbMaskCurrentUrl = '/__definitely_missing__/mask.png';
        _lbApplyMaskVisibility();
        """
    )
    # Wait until the browser settles the request (onerror has fired).
    page.wait_for_function(
        "document.getElementById('lightboxMaskOverlay').complete"
        " && document.getElementById('lightboxMaskOverlay').naturalWidth === 0",
        timeout=3000,
    )
    assert not page.locator("#lightboxMaskOverlay").evaluate(
        "el => el.classList.contains('show')"
    ), "failed load should leave the overlay hidden"

    # User toggles masks off, then back on. The src never changes, so the
    # same-URL branch in _lbApplyMaskVisibility() runs. It must NOT add
    # `show`, since the previous load failed and would render as a
    # broken-image icon.
    page.evaluate(
        """
        _lbMasksVisible = false;
        _lbApplyMaskVisibility();
        _lbMasksVisible = true;
        _lbApplyMaskVisibility();
        """
    )
    assert not page.locator("#lightboxMaskOverlay").evaluate(
        "el => el.classList.contains('show')"
    ), "same-URL re-show after a failed load must not reveal a broken overlay"


def test_mask_toggle_off_detaches_loaded_overlay_image(live_server, page):
    """The Hide Masks button should fully detach a loaded overlay image.

    CSS display normally hides `.show`, but keeping the mask src attached can
    leave stale composited pixels visible in embedded WebViews. Removing src
    makes the hidden state unambiguous.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.evaluate("localStorage.removeItem('vireo.lb.masksVisible');")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.dblclick()
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=3000,
    )

    page.evaluate(
        """
        _lbMasksVisible = true;
        _lbMaskCurrentUrl =
          'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII=';
        _lbApplyMaskVisibility();
        """
    )
    # `show` is added from the overlay's onload handler, and Chromium exposes
    # naturalWidth/complete as soon as the bitmap is decoded — one task tick
    # before it dispatches the load event. Waiting on naturalWidth alone races
    # that dispatch, so wait for the class the hide assertions depend on.
    page.wait_for_function(
        """() => {
            const el = document.getElementById('lightboxMaskOverlay');
            return el.naturalWidth > 0 && el.classList.contains('show');
        }""",
        timeout=3000,
    )

    page.locator("#lightboxViewBtn").click()
    page.locator("#lightboxToggleMasks").click()
    assert page.evaluate("localStorage.getItem('vireo.lb.masksVisible')") == "0"
    expect(page.locator("#lightboxToggleMasks")).not_to_be_checked()
    assert not page.locator("#lightboxMaskOverlay").evaluate(
        "el => el.classList.contains('show')"
    ), "hide must remove the visible overlay class"
    assert page.locator("#lightboxMaskOverlay").evaluate(
        "el => !el.hasAttribute('src')"
    ), "hide must detach the loaded mask src"


def test_lightbox_close_menu_item_closes_overlay(live_server, page):
    """The 'Close Lightbox' menu item dismisses the overlay."""
    url = live_server["url"]
    _open_lightbox(page, url)

    _fire_contextmenu_on_lightbox(page)
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu.locator(".vireo-ctx-item", has_text="Close Lightbox").click()
    expect(menu).to_be_hidden()
    page.wait_for_function(
        "!document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=2000,
    )


def test_lightbox_rating_chip_applies(live_server, page):
    """Clicking a rating chip in the lightbox menu rates the current photo."""
    url = live_server["url"]
    _open_lightbox(page, url)

    pid = page.evaluate("vireoLightboxSession.requestedPhotoId()")
    assert pid is not None

    _fire_contextmenu_on_lightbox(page)
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    # First chip row is ratings (0..5). Click the "2" chip.
    menu.locator(".vireo-ctx-chip", has_text="2").first.click()
    expect(menu).to_be_hidden()

    # setRatingFor is defined on browse.html; it updates the server and the
    # local photos[] list. Poll for the rating change.
    page.wait_for_function(
        f"(photos.find(p => p.id === {pid}) || {{}}).rating === 2",
        timeout=3000,
    )


def test_outside_click_dismiss_swallows_next_click(live_server, page):
    """Outside-click dismissal of the context menu must swallow the click.

    If the click propagates after `_outside` tears the menu down, any
    underlying handler (e.g. the lightbox overlay's `onclick=closeLightbox`)
    would fire unexpectedly. We install a body-level click listener,
    dismiss the menu by clicking outside it, and assert the listener never
    saw the click.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.evaluate(
        """() => {
            window.__outside_click_count = 0;
            document.body.addEventListener('click', () => {
                window.__outside_click_count++;
            }, false);
            openContextMenu({clientX: 100, clientY: 100},
                [{label: 'X', onClick: () => {}}]);
        }"""
    )
    expect(page.locator('.vireo-ctx-menu')).to_be_visible()
    page.mouse.click(500, 500)
    expect(page.locator('.vireo-ctx-menu')).to_be_hidden()
    # The outside click should have been swallowed before reaching body.
    count = page.evaluate("window.__outside_click_count")
    assert count == 0, f"outside-click-to-dismiss reached body (count={count})"
