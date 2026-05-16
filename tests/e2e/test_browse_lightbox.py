from playwright.sync_api import expect


def test_browse_lightbox_arrows_navigate(live_server, page):
    """On-screen ◄/► arrows in the lightbox navigate between photos opened from /browse.

    Regression: previously browse.html called openLightbox() without the photo-list
    argument, so _lightboxPhotoList stayed empty and lightboxNav() silently no-op'd.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    first_filename = first_card.get_attribute("data-filename")

    first_card.dblclick()

    overlay = page.locator("#lightboxOverlay")
    expect(overlay).to_have_class("lightbox-overlay active")

    filename_display = page.locator("#lightboxFilename")
    expect(filename_display).to_have_text(first_filename)

    counter = page.locator("#lightboxCounter")
    expect(counter).to_be_visible()
    expect(counter).to_contain_text("1 /")

    page.locator("[title='Next (→)']").click()

    expect(filename_display).not_to_have_text(first_filename)
    expect(counter).to_contain_text("2 /")

    page.locator("[title='Previous (←)']").click()
    expect(filename_display).to_have_text(first_filename)
    expect(counter).to_contain_text("1 /")


def test_browse_lightbox_arrows_preserve_one_to_one_zoom(live_server, page):
    """Navigating from a 1:1 lightbox view keeps the next photo at 1:1."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    with page.expect_response(lambda r: "/api/photos/1" in r.url and r.status == 200):
        first_card.dblclick()

    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")

    page.evaluate(
        """() => {
            window._lbNativeZoom = 2;
            window._lbZoom = 2;
            window._lbPending1To1 = false;
        }"""
    )

    page.locator("[title='Next (→)']").click()
    expect(page.locator("#lightboxCounter")).to_contain_text("2 /")
    assert page.evaluate("window._lbZoom > 1.001") is True
    assert page.evaluate("window._lbPending1To1") is True
    assert page.evaluate("window._lbCurrentSrcKey") == "original"

    restored = page.evaluate(
        """() => {
            window._lbNativeZoom = 2.5;
            window._lbApplyPendingOneToOneZoom();
            return Math.abs(window._lbZoom - window._lbNativeZoom) <= Math.max(0.01, window._lbNativeZoom * 0.01);
        }"""
    )
    assert restored
    assert page.evaluate("window._lbZoom") > 1.001


def test_browse_e_f_g_keyboard_modes(live_server, page):
    """Browse grid shortcuts open the image, request fullscreen, and return to grid."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    first_filename = first_card.get_attribute("data-filename")

    overlay = page.locator("#lightboxOverlay")
    filename_display = page.locator("#lightboxFilename")

    page.keyboard.press("e")
    expect(overlay).to_have_class("lightbox-overlay active")
    expect(filename_display).to_have_text(first_filename)

    page.keyboard.press("g")
    expect(overlay).to_have_class("lightbox-overlay")

    page.evaluate(
        """() => {
            window.__fullscreenRequested = false;
            window.requestLightboxFullscreen = function() {
                window.__fullscreenRequested = true;
            };
        }"""
    )
    page.keyboard.press("f")
    expect(overlay).to_have_class("lightbox-overlay active")
    expect(filename_display).to_have_text(first_filename)
    assert page.evaluate("window.__fullscreenRequested") is True

    page.evaluate(
        """() => {
            window.__fullscreenRequested = false;
            _shortcuts.zoom = 'f';
            window._vireoShortcuts = window._vireoShortcuts || {};
            window._vireoShortcuts.browse = window._vireoShortcuts.browse || {};
            window._vireoShortcuts.browse.zoom = 'f';
            window._lbNativeZoom = 2;
            window._lbZoom = 1;
        }"""
    )
    page.keyboard.press("f")
    assert page.evaluate("window.__fullscreenRequested") is False
    assert page.evaluate("window._lbZoom") > 1

    page.evaluate(
        """() => {
            const overlay = document.getElementById('exportOverlay');
            overlay.classList.add('open');
        }"""
    )
    page.keyboard.press("g")
    expect(overlay).to_have_class("lightbox-overlay active")
    page.evaluate("document.getElementById('exportOverlay').classList.remove('open')")


def test_browse_image_hotkeys_preserve_selection_and_shortcut_remaps(live_server, page):
    """E/F viewing shortcuts should not destroy selection or override user keymaps."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    selected = page.evaluate(
        """() => {
            selectedPhotos.clear();
            selectedPhotoId = null;
            selectedIndex = -1;
            selectedPhotos.add(photos[0].id);
            selectedPhotos.add(photos[1].id);
            renderGrid();
            updateBatchBar();
            return Array.from(selectedPhotos).sort((a, b) => a - b);
        }"""
    )

    page.keyboard.press("e")
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    assert page.evaluate("Array.from(selectedPhotos).sort((a, b) => a - b)") == selected
    page.keyboard.press("g")
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay")
    assert page.evaluate("Array.from(selectedPhotos).sort((a, b) => a - b)") == selected

    page.evaluate(
        """() => {
            selectedPhotos.clear();
            selectedPhotoId = photos[0].id;
            selectedIndex = 0;
            photos[0].flag = null;
            _shortcuts.flag = 'e';
            renderGrid();
            updateBatchBar();
        }"""
    )
    page.keyboard.press("e")
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay")
    page.wait_for_function("photos[0].flag === 'flagged'")
