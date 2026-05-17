import base64

from playwright.sync_api import expect

_PNG_1X1 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
    "/x8AAwMCAO+/p9sAAAAASUVORK5CYII="
)


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


def test_browse_lightbox_one_to_one_nav_falls_back_when_original_fails(live_server, page):
    """1:1 arrow navigation falls back to /full when the original is unavailable."""
    image_body = base64.b64decode(_PNG_1X1)
    page.route("**/photos/*/original", lambda route: route.fulfill(status=503, body="missing"))
    page.route("**/photos/*/full", lambda route: route.fulfill(body=image_body, content_type="image/png"))

    url = live_server["url"]
    page.goto(f"{url}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
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
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return window._lbCurrentSrcKey === 'full' && img && img.complete && img.naturalWidth > 0;
        }"""
    )
    assert "/full" in page.locator("#lightboxImg").get_attribute("src")


def test_browse_lightbox_one_to_one_uses_device_pixels_and_natural_layout(live_server, page):
    """1:1 uses natural image coordinates and maps source pixels to device pixels."""
    page.add_init_script(
        "Object.defineProperty(window, 'devicePixelRatio', { value: 2, configurable: true });"
    )
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#3a7"/>'
        '<path d="M0 0L4000 2000M4000 0L0 2000" stroke="#fff" stroke-width="12"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=svg, content_type="image/svg+xml"),
    )
    page.route(
        "**/photos/*/original",
        lambda route: route.fulfill(body=svg, content_type="image/svg+xml"),
    )

    url = live_server["url"]
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{url}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    first_card.dblclick()

    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth === 4000;
        }"""
    )
    page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
        }"""
    )
    page.wait_for_function("window._lbNativeZoom > 1")

    page.keyboard.press("z")
    metrics = page.evaluate(
        """() => {
            const t = document.getElementById('lightboxTransform');
            const rect = t.getBoundingClientRect();
            return {
                dpr: window.devicePixelRatio,
                zoom: window._lbZoom,
                nativeZoom: window._lbNativeZoom,
                fitScale: window._lbFitScale,
                styleWidth: t.style.width,
                styleHeight: t.style.height,
                rectWidth: rect.width,
                rectHeight: rect.height,
            };
        }"""
    )

    expected_native_zoom = (1 / metrics["dpr"]) / metrics["fitScale"]
    assert abs(metrics["nativeZoom"] - expected_native_zoom) < 0.01
    assert abs(metrics["zoom"] - metrics["nativeZoom"]) < 0.01
    assert metrics["styleWidth"] == "4000px"
    assert metrics["styleHeight"] == "2000px"
    assert abs(metrics["rectWidth"] - 2000) < 2
    assert abs(metrics["rectHeight"] - 1000) < 2


def test_browse_lightbox_defers_one_to_one_until_original_size_known(live_server, page):
    """A loaded preview/full tier must not masquerade as true 1:1."""
    page.add_init_script(
        "Object.defineProperty(window, 'devicePixelRatio', { value: 2, configurable: true });"
    )
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1920" height="960" '
        'viewBox="0 0 1920 960"><rect width="1920" height="960" fill="#274"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=svg, content_type="image/svg+xml"),
    )

    url = live_server["url"]
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{url}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    first_card.dblclick()

    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth === 1920;
        }"""
    )

    state = page.evaluate(
        """() => {
            window._lbPhotoW = null;
            window._lbPhotoH = null;
            window._lbOriginalUnavailable = false;
            window._lbCurrentSrcKey = 'full';
            window._lbZoom = 1;
            window._lbRecomputeNativeZoom();
            window.toggleLightboxZoom();
            return {
                nativeZoom: window._lbNativeZoom,
                pending: window._lbPending1To1,
                zoom: window._lbZoom,
                sourceKey: window._lbPickSourceKey(),
            };
        }"""
    )

    assert state["nativeZoom"] is None
    assert state["pending"] is True
    assert state["zoom"] > 1
    assert state["sourceKey"] == "original"

    upgraded = page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            window._lbApplyPendingOneToOneZoom();
            return Math.abs(window._lbZoom - window._lbNativeZoom) < 0.01;
        }"""
    )
    assert upgraded is True


def test_browse_lightbox_does_not_retry_original_after_unavailable(live_server, page):
    """After /original fails, source selection should stay on preview/full tiers."""
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="8000" height="4000" '
        'viewBox="0 0 8000 4000"><rect width="8000" height="4000" fill="#246"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=svg, content_type="image/svg+xml"),
    )

    url = live_server["url"]
    page.goto(f"{url}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    first_card.dblclick()

    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth === 8000;
        }"""
    )
    choices = page.evaluate(
        """() => {
            window._lbOriginalUnavailable = true;
            window._lbZoom = 2;
            window._lbNativeZoom = null;
            const unknownDimsChoice = window._lbPickSourceKey();
            window._lbPhotoW = 8000;
            window._lbPhotoH = 4000;
            window._lbFullLongEdge = 1000;
            window._lbFitScale = 0.1;
            window._lbNativeZoom = 100;
            window._lbZoom = 100;
            const largeNeededChoice = window._lbPickSourceKey();
            return { unknownDimsChoice, largeNeededChoice };
        }"""
    )

    assert choices["unknownDimsChoice"] == "full"
    assert choices["largeNeededChoice"] == "3840"


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
