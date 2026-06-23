import base64
import time

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
    assert page.evaluate(
        """() => (
            window._lbCurrentSrcKey === 'original' ||
            (window._lbOriginalUnavailable && window._lbCurrentSrcKey === 'full')
        )"""
    ) is True

    restored = page.evaluate(
        """() => {
            window._lbNativeZoom = 2.5;
            window._lbApplyPendingOneToOneZoom();
            return Math.abs(window._lbZoom - window._lbNativeZoom) <= Math.max(0.01, window._lbNativeZoom * 0.01);
        }"""
    )
    assert restored
    assert page.evaluate("window._lbZoom") > 1.001


def test_browse_lightbox_restores_and_carries_zoomed_viewport(live_server, page):
    """Arrow navigation preserves pan/zoom per photo and carries it to unseen photos."""
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#274"/>'
        '<circle cx="1000" cy="1400" r="180" fill="#fff"/></svg>'
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

    first_view = page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            window._lbApplyViewportState({zoom: 2.2, centerX: 0.24, centerY: 0.70});
            window._lbSaveViewportState(window._lightboxCurrentId);
            return window._lbViewportStateFromCurrent();
        }"""
    )

    page.locator("[title='Next (→)']").click()
    expect(page.locator("#lightboxCounter")).to_contain_text("2 /")
    page.wait_for_function("window._lbPendingViewportState === null")
    carried_view = page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            window._lbTryApplyPendingViewportState();
            return window._lbViewportStateFromCurrent();
        }"""
    )
    assert abs(carried_view["zoom"] - first_view["zoom"]) < 0.05
    assert abs(carried_view["centerX"] - first_view["centerX"]) < 0.03
    assert abs(carried_view["centerY"] - first_view["centerY"]) < 0.03

    page.evaluate(
        """() => {
            window._lbApplyViewportState({zoom: 2.2, centerX: 0.78, centerY: 0.30});
            window._lbSaveViewportState(window._lightboxCurrentId);
        }"""
    )
    page.locator("[title='Previous (←)']").click()
    expect(page.locator("#lightboxCounter")).to_contain_text("1 /")
    page.wait_for_function("window._lbPendingViewportState === null")
    restored_view = page.evaluate("window._lbViewportStateFromCurrent()")
    assert abs(restored_view["zoom"] - first_view["zoom"]) < 0.05
    assert abs(restored_view["centerX"] - first_view["centerX"]) < 0.03
    assert abs(restored_view["centerY"] - first_view["centerY"]) < 0.03


def test_browse_lightbox_pending_high_zoom_survives_native_zoom_race(live_server, page):
    """A saved zoom > 4 is not lost when native zoom is unknown at first apply.

    Race: if /api/photos/<id> resolves before the image load event,
    _lbTryApplyPendingViewportState runs while _lbNativeZoom is null. The
    fallback max clamps zoom to 4, so the pending state must be kept (not
    cleared) so a later apply, once native zoom is known, can restore the
    original high zoom.
    """
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#274"/>'
        '<circle cx="1000" cy="1400" r="180" fill="#fff"/></svg>'
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

    # Establish native zoom and pick a target zoom > 4 that is within the
    # real max (nativeZoom * 4) so it would survive an accurate apply.
    setup = page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            const native = window._lbNativeZoom;
            const target = Math.min(native * 2, native * 4 - 0.5);
            return {native: native, target: target};
        }"""
    )
    assert setup["native"] is not None and setup["native"] > 2.0
    assert setup["target"] > 4.0

    # Simulate the race: native zoom unknown when the pending high-zoom
    # state is applied (e.g. API fetch resolved before image load).
    degraded = page.evaluate(
        """(target) => {
            window._lbNativeZoom = null;
            window._lbPendingViewportState = {
                zoom: target, centerX: 0.3, centerY: 0.6,
                oneToOne: false, pending1To1: false,
            };
            const applied = window._lbTryApplyPendingViewportState();
            return {
                applied: applied,
                zoom: window._lbZoom,
                stillPending: window._lbPendingViewportState !== null,
            };
        }""",
        setup["target"],
    )
    assert degraded["applied"] is True
    # Clamped to the fallback max while native zoom was unknown...
    assert abs(degraded["zoom"] - 4.0) < 0.01
    # ...but the pending state must survive so it can be retried.
    assert degraded["stillPending"] is True

    # Native zoom becomes known (image load path): the high zoom is
    # restored accurately and the pending state is finally cleared.
    restored = page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            window._lbTryApplyPendingViewportState();
            return {
                zoom: window._lbZoom,
                stillPending: window._lbPendingViewportState !== null,
            };
        }"""
    )
    assert abs(restored["zoom"] - setup["target"]) < 0.1
    assert restored["stillPending"] is False


def test_browse_lightbox_manual_zoom_cancels_pending_restore(live_server, page):
    """A manual wheel zoom cancels a still-armed pending viewport restore.

    Regression: _lbPendingViewportState is intentionally kept until native
    zoom is known so a high-zoom restore survives the metadata/image-load
    race (see test above). But that same window let a later
    _lbTryApplyPendingViewportState() (image-load callback) snap the
    viewport back after the user had already zoomed the new photo. A
    user-driven viewport mutation must cancel the pending restore.
    """
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#274"/>'
        '<circle cx="1000" cy="1400" r="180" fill="#fff"/></svg>'
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

    # Arm a high-zoom restore while native zoom is unknown — this is the
    # carried-navigation race where the pending state survives an async
    # retry (the precondition for the snap-back bug).
    setup = page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            const native = window._lbNativeZoom;
            const target = Math.min(native * 2, native * 4 - 0.5);
            window._lbNativeZoom = null;
            window._lbPendingViewportState = {
                zoom: target, centerX: 0.3, centerY: 0.6,
                oneToOne: false, pending1To1: false,
            };
            window._lbTryApplyPendingViewportState();
            return {
                native: native,
                target: target,
                stillPending: window._lbPendingViewportState !== null,
            };
        }"""
    )
    assert setup["native"] is not None and setup["native"] > 2.0
    # Sanity: pending must survive the native-zoom race, else there is no bug.
    assert setup["stillPending"] is True

    # The user manually zooms out with the wheel over the image.
    page.locator("#lightboxWrap").hover()
    page.mouse.wheel(0, 600)

    after_wheel = page.evaluate(
        """() => ({
            zoom: window._lbZoom,
            stillPending: window._lbPendingViewportState !== null,
        })"""
    )
    # The manual wheel zoom must have cancelled the pending restore...
    assert after_wheel["stillPending"] is False
    # ...and produced a zoom clearly distinct from the stale pending target.
    assert abs(after_wheel["zoom"] - setup["target"]) > 0.5

    # A later image-load retry (native zoom now known) must NOT snap the
    # viewport back to the stale pending state.
    retried = page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            const applied = window._lbTryApplyPendingViewportState();
            return {applied: applied, zoom: window._lbZoom};
        }"""
    )
    assert retried["applied"] is False
    assert abs(retried["zoom"] - after_wheel["zoom"]) < 0.01


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


def test_browse_lightbox_restored_pending_one_to_one_waits_for_fallback_after_initial_original_fails(
    live_server, page
):
    """A restored pending 1:1 must not snap on /full after initial /original fails."""
    page.add_init_script(
        "Object.defineProperty(window, 'devicePixelRatio', { value: 1, configurable: true });"
    )
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="600" height="400" '
        'viewBox="0 0 600 400"><rect width="600" height="400" fill="#274"/></svg>'
    )
    fallback_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="2560" height="1600" '
        'viewBox="0 0 2560 1600"><rect width="2560" height="1600" fill="#642"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=full_svg, content_type="image/svg+xml"),
    )
    page.route(
        "**/api/photos/1",
        lambda route: route.fulfill(
            json={
                "id": 1,
                "filename": "restored-pending.jpg",
                "width": 4000,
                "height": 2500,
                "flag": "none",
                "wildlife_excluded": False,
            }
        ),
    )
    page.route("**/photos/*/original", lambda route: route.abort())
    held_fallback = {}

    def hold_fallback(route):
        if "released" in held_fallback:
            route.fulfill(body=fallback_svg, content_type="image/svg+xml")
        else:
            held_fallback["route"] = route

    page.route("**/photos/*/preview?size=2560", hold_fallback)
    page.route("**/photos/*/preview?size=3840", hold_fallback)

    url = live_server["url"]
    page.set_viewport_size({"width": 900, "height": 700})
    page.goto(f"{url}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    page.evaluate(
        """() => {
            const p = window.photos[0];
            window._lbViewportByPhotoId[String(p.id)] = {
                zoom: 1,
                centerX: 0.5,
                centerY: 0.5,
                oneToOne: true,
                pending1To1: true,
            };
            window.openLightbox(p.id, p.filename, window.photos);
        }"""
    )
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")

    deadline = time.time() + 3
    while "route" not in held_fallback and time.time() < deadline:
        page.wait_for_timeout(25)
    assert "route" in held_fallback, page.evaluate(
        """() => ({
            pending: window._lbPending1To1,
            zoom: window._lbZoom,
            nativeZoom: window._lbNativeZoom,
            currentSource: window._lbCurrentSrcKey,
            desiredSource: window._lbDesiredSrcKey,
            originalUnavailable: window._lbOriginalUnavailable,
            pendingViewport: window._lbPendingViewportState,
            imgComplete: document.getElementById('lightboxImg')?.complete,
            naturalWidth: document.getElementById('lightboxImg')?.naturalWidth,
        })"""
    )

    waiting = page.evaluate(
        """() => ({
            pending: window._lbPending1To1,
            zoom: window._lbZoom,
            currentSource: window._lbCurrentSrcKey,
            desiredSource: window._lbDesiredSrcKey,
        })"""
    )
    assert waiting["pending"] is True
    assert abs(waiting["zoom"] - 1) < 0.001
    assert waiting["currentSource"] == "full"
    assert waiting["desiredSource"] in ("2560", "3840")

    held_fallback["released"] = True
    held_fallback.pop("route").fulfill(
        body=fallback_svg, content_type="image/svg+xml"
    )
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return (window._lbCurrentSrcKey === '2560' || window._lbCurrentSrcKey === '3840')
                && window._lbPending1To1 === false
                && img && img.complete && img.naturalWidth === 2560
                && window._lbNativeZoom
                && Math.abs(window._lbZoom - window._lbNativeZoom) < 0.01;
        }""",
        timeout=8000,
    )


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
    page.wait_for_function(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            return window._lbNativeZoom > 1;
        }"""
    )

    page.keyboard.press("z")
    # The /full source is already at the original's resolution, so the 1:1 snap
    # applies synchronously — but under CPU contention the 'z' keydown can be
    # processed slightly after page.keyboard.press resolves. Wait for the snap
    # to land before sampling layout so the metrics read can't race it.
    page.wait_for_function("window._lbZoom > 1.001")
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
    """Metadata resolving before the held /original must not flash a soft 1:1.

    Regression guard for the metadata-before-/original race: learning the true
    dimensions (and therefore _lbNativeZoom) while /original is still loading
    must NOT clear the pending 1:1 and snap on the upscaled /full tier. The
    snap may only happen once the high-resolution source is actually current.
    """
    page.add_init_script(
        "Object.defineProperty(window, 'devicePixelRatio', { value: 2, configurable: true });"
    )
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1920" height="960" '
        'viewBox="0 0 1920 960"><rect width="1920" height="960" fill="#274"/></svg>'
    )
    original_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#3a7"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=full_svg, content_type="image/svg+xml"),
    )
    held_original = {}

    def hold_original(route):
        if "released" in held_original:
            route.fulfill(body=original_svg, content_type="image/svg+xml")
        elif "route" not in held_original:
            held_original["route"] = route
        else:
            route.fulfill(body=original_svg, content_type="image/svg+xml")

    page.route("**/photos/*/original", hold_original)

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

    # Press 1:1 while the true size is still unknown: it must defer (stay at
    # fit, pending) and request /original rather than enlarging /full.
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
                desiredSource: window._lbDesiredSrcKey,
            };
        }"""
    )

    assert state["nativeZoom"] is None
    assert state["pending"] is True
    assert state["zoom"] == 1
    assert state["desiredSource"] == "original"

    # Wait until the deferred swap has actually issued the /original request
    # and it is being held, so the metadata step below genuinely races a
    # still-loading high-res source.
    deadline = time.time() + 2
    while "route" not in held_original and time.time() < deadline:
        page.wait_for_timeout(25)
    assert "route" in held_original

    # Metadata resolves before the held /original finishes. Learning the true
    # dimensions must NOT clear the pending state or snap on the upscaled /full.
    still_deferred = page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            window._lbApplyPendingOneToOneZoom();
            return {
                pending: window._lbPending1To1,
                zoom: window._lbZoom,
                currentSource: window._lbCurrentSrcKey,
                nativeZoom: window._lbNativeZoom,
            };
        }"""
    )
    assert still_deferred["pending"] is True
    assert abs(still_deferred["zoom"] - 1) < 0.001
    assert still_deferred["currentSource"] == "full"
    assert still_deferred["nativeZoom"] > 1

    # Releasing /original lets the deferred 1:1 finally snap against the
    # high-resolution source.
    held_original["released"] = True
    held_original.pop("route").fulfill(
        body=original_svg, content_type="image/svg+xml"
    )
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return window._lbCurrentSrcKey === 'original'
                && window._lbPending1To1 === false
                && img && img.complete && img.naturalWidth === 4000
                && Math.abs(window._lbZoom - window._lbNativeZoom) < 0.01;
        }"""
    )


def test_browse_lightbox_pending_one_to_one_guard_schedules_sharper_source(
    live_server, page
):
    """If native 1:1 needs a sharper source, the guard must queue that source."""
    page.add_init_script(
        "Object.defineProperty(window, 'devicePixelRatio', { value: 2, configurable: true });"
    )
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1920" height="960" '
        'viewBox="0 0 1920 960"><rect width="1920" height="960" fill="#274"/></svg>'
    )
    original_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#3a7"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=full_svg, content_type="image/svg+xml"),
    )
    held_original = {}

    def hold_original(route):
        if "released" in held_original:
            route.fulfill(body=original_svg, content_type="image/svg+xml")
        elif "route" not in held_original:
            held_original["route"] = route
        else:
            route.fulfill(body=original_svg, content_type="image/svg+xml")

    page.route("**/photos/*/original", hold_original)

    url = live_server["url"]
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{url}/browse")

    page.locator(".grid-card").first.wait_for(state="visible")
    page.locator(".grid-card").first.dblclick()

    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth === 1920;
        }"""
    )

    state = page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbCurrentSrcKey = 'full';
            window._lbDesiredSrcKey = 'full';
            window._lbPending1To1 = true;
            window._lbZoom = 1;
            window._lbRecomputeNativeZoom();
            window._lbApplyPendingOneToOneZoom();
            return {
                pending: window._lbPending1To1,
                zoom: window._lbZoom,
                nativeZoom: window._lbNativeZoom,
                currentSource: window._lbCurrentSrcKey,
                desiredSource: window._lbDesiredSrcKey,
            };
        }"""
    )
    assert state["pending"] is True
    assert abs(state["zoom"] - 1) < 0.001
    assert state["nativeZoom"] > 1
    assert state["currentSource"] == "full"
    assert state["desiredSource"] == "original"

    deadline = time.time() + 2
    while "route" not in held_original and time.time() < deadline:
        page.wait_for_timeout(25)
    assert "route" in held_original

    held_original["released"] = True
    held_original.pop("route").fulfill(
        body=original_svg,
        content_type="image/svg+xml",
    )
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return window._lbCurrentSrcKey === 'original'
                && window._lbPending1To1 === false
                && img && img.complete && img.naturalWidth === 4000
                && Math.abs(window._lbZoom - window._lbNativeZoom) < 0.01;
        }"""
    )


def test_browse_lightbox_waits_for_original_before_one_to_one_snap(live_server, page):
    """Known 1:1 zoom waits for the high-res source instead of enlarging /full."""
    page.add_init_script(
        "Object.defineProperty(window, 'devicePixelRatio', { value: 2, configurable: true });"
    )
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1920" height="960" '
        'viewBox="0 0 1920 960"><rect width="1920" height="960" fill="#274"/></svg>'
    )
    original_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#3a7"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=full_svg, content_type="image/svg+xml"),
    )
    held_original = {}

    def hold_first_original(route):
        if "released" in held_original:
            route.fulfill(body=original_svg, content_type="image/svg+xml")
        elif "route" not in held_original:
            held_original["route"] = route
        else:
            route.fulfill(body=original_svg, content_type="image/svg+xml")

    page.route("**/photos/*/original", hold_first_original)

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
    page.wait_for_function(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            return window._lbNativeZoom > 1;
        }"""
    )

    page.keyboard.press("z")
    page.wait_for_function(
        "window._lbPending1To1 === true && window._lbDesiredSrcKey === 'original'"
    )
    assert abs(page.evaluate("window._lbZoom") - 1) < 0.001
    assert page.evaluate("window._lbCurrentSrcKey") == "full"
    deadline = time.time() + 2
    while "route" not in held_original and time.time() < deadline:
        page.wait_for_timeout(25)
    assert "route" in held_original

    original_route = held_original.pop("route")
    held_original["released"] = True
    original_route.fulfill(
        body=original_svg,
        content_type="image/svg+xml",
    )
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return window._lbCurrentSrcKey === 'original'
                && img && img.complete && img.naturalWidth === 4000
                && Math.abs(window._lbZoom - window._lbNativeZoom) < 0.01;
        }"""
    )


def test_browse_lightbox_resize_preserves_deferred_one_to_one(live_server, page):
    """A viewport resize while 'loading 1:1' must not drop the deferred snap."""
    page.add_init_script(
        "Object.defineProperty(window, 'devicePixelRatio', { value: 2, configurable: true });"
    )
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1920" height="960" '
        'viewBox="0 0 1920 960"><rect width="1920" height="960" fill="#274"/></svg>'
    )
    original_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#3a7"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=full_svg, content_type="image/svg+xml"),
    )
    held_original = {}

    def hold_first_original(route):
        if "released" in held_original:
            route.fulfill(body=original_svg, content_type="image/svg+xml")
        elif "route" not in held_original:
            held_original["route"] = route
        else:
            route.fulfill(body=original_svg, content_type="image/svg+xml")

    page.route("**/photos/*/original", hold_first_original)

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
    page.wait_for_function(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            return window._lbNativeZoom > 1;
        }"""
    )

    page.keyboard.press("z")
    page.wait_for_function(
        "window._lbPending1To1 === true && window._lbDesiredSrcKey === 'original'"
    )
    assert abs(page.evaluate("window._lbZoom") - 1) < 0.001
    assert page.evaluate("window._lbCurrentSrcKey") == "full"

    # Wait until the deferred swap has actually issued the held /original
    # request — i.e. we are genuinely in the "loading 1:1" state Codex flagged.
    # The swap is scheduled on a debounced timer, so allow generous headroom:
    # under CPU contention (e.g. a full e2e run) that timer plus the preloader
    # round-trip can take well over 2s, and a too-tight deadline here fails the
    # assert before the request is even captured.
    deadline = time.time() + 5
    while "route" not in held_original and time.time() < deadline:
        page.wait_for_timeout(25)
    assert "route" in held_original

    native_zoom_before = page.evaluate("window._lbNativeZoom")

    # Resize while the high-res source is still loading. The image is 4000px
    # wide so _lbFitScale (hence _lbNativeZoom) is width-constrained; shrinking
    # the viewport width forces a deterministic _lbNativeZoom change once the
    # resize handler runs. The handler recomputes _lbNativeZoom unconditionally
    # (before any pending-state logic), so the change below is a fix-independent
    # signal that the debounced handler has actually executed — no fixed sleep.
    page.set_viewport_size({"width": 640, "height": 800})
    page.wait_for_function(
        "Math.abs(window._lbNativeZoom - %r) > 0.1" % native_zoom_before
    )

    # The resize handler has now run while /original is still held. Pre-fix it
    # cleared _lbPending1To1 and retargeted the swap back to /full, so releasing
    # /original below no longer snaps to 1:1 — the deferred zoom request was
    # silently dropped and the snap assertion times out (hard regression).
    original_route = held_original.pop("route")
    held_original["released"] = True
    original_route.fulfill(body=original_svg, content_type="image/svg+xml")

    # Post-fix the deferred intent survives the resize, so the lightbox still
    # snaps to true 1:1 on /original. Pre-fix this never completes (the snap was
    # dropped) and the wait times out — making the regression a hard failure.
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return window._lbCurrentSrcKey === 'original'
                && window._lbPending1To1 === false
                && img && img.complete && img.naturalWidth === 4000
                && Math.abs(window._lbZoom - window._lbNativeZoom) < 0.01;
        }""",
        timeout=8000,
    )


def test_browse_lightbox_does_not_retry_original_after_unavailable(live_server, page):
    """After /original fails, source selection should stay on preview/full tiers."""
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="8000" height="4000" '
        'viewBox="0 0 8000 4000"><rect width="8000" height="4000" fill="#246"/></svg>'
    )
    fallback_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="3840" height="1920" '
        'viewBox="0 0 3840 1920"><rect width="3840" height="1920" fill="#642"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=full_svg, content_type="image/svg+xml"),
    )
    page.route(
        "**/photos/*/original",
        lambda route: route.abort(),
    )
    page.route(
        "**/photos/*/preview?size=3840",
        lambda route: route.fulfill(body=fallback_svg, content_type="image/svg+xml"),
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

    page.evaluate(
        """() => {
            window._lbOriginalUnavailable = false;
            window._lbZoom = 100;
            window._lbNativeZoom = 100;
            window._lbFullLongEdge = 1000;
            window._lbScheduleSourceSwap();
        }"""
    )
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return window._lbOriginalUnavailable &&
                   window._lbCurrentSrcKey === '3840' &&
                   img && img.complete && img.naturalWidth === 3840;
        }"""
    )


def test_browse_lightbox_waits_for_fallback_tier_after_original_fails(live_server, page):
    """If /original fails, deferred 1:1 waits for the best preview fallback."""
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1920" height="960" '
        'viewBox="0 0 1920 960"><rect width="1920" height="960" fill="#246"/></svg>'
    )
    fallback_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="3840" height="1920" '
        'viewBox="0 0 3840 1920"><rect width="3840" height="1920" fill="#642"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=full_svg, content_type="image/svg+xml"),
    )
    page.route("**/photos/*/original", lambda route: route.abort())
    held_fallback = {}

    def hold_3840(route):
        if "released" in held_fallback:
            route.fulfill(body=fallback_svg, content_type="image/svg+xml")
        elif "route" not in held_fallback:
            held_fallback["route"] = route
        else:
            route.fulfill(body=fallback_svg, content_type="image/svg+xml")

    page.route("**/photos/*/preview?size=3840", hold_3840)

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

    page.evaluate(
        """() => {
            window._lbPhotoW = null;
            window._lbPhotoH = null;
            window._lbOriginalUnavailable = false;
            window._lbCurrentSrcKey = 'full';
            window._lbNativeZoom = null;
            window._lbZoom = 1;
            window.toggleLightboxZoom();
        }"""
    )

    deadline = time.time() + 2
    while "route" not in held_fallback and time.time() < deadline:
        page.wait_for_timeout(25)
    assert "route" in held_fallback
    waiting = page.evaluate(
        """() => ({
            pending: window._lbPending1To1,
            zoom: window._lbZoom,
            currentSource: window._lbCurrentSrcKey,
            desiredSource: window._lbDesiredSrcKey,
        })"""
    )
    assert waiting["pending"] is True
    assert abs(waiting["zoom"] - 1) < 0.001
    assert waiting["currentSource"] == "full"
    assert waiting["desiredSource"] == "3840"

    before_resize_transforms = page.evaluate(
        """() => {
            window.__lbResizeTransformCount = 0;
            const originalApplyTransform = window._lbApplyTransform;
            window._lbApplyTransform = function() {
                window.__lbResizeTransformCount += 1;
                return originalApplyTransform.apply(this, arguments);
            };
            return window.__lbResizeTransformCount;
        }"""
    )
    page.set_viewport_size({"width": 760, "height": 800})
    page.wait_for_function(
        "window.__lbResizeTransformCount > %d" % before_resize_transforms
    )
    after_resize = page.evaluate(
        """() => ({
            pending: window._lbPending1To1,
            zoom: window._lbZoom,
            currentSource: window._lbCurrentSrcKey,
            desiredSource: window._lbDesiredSrcKey,
        })"""
    )
    assert after_resize["pending"] is True
    assert abs(after_resize["zoom"] - 1) < 0.001
    assert after_resize["currentSource"] == "full"
    assert after_resize["desiredSource"] == "3840"

    fallback_route = held_fallback.pop("route")
    held_fallback["released"] = True
    fallback_route.fulfill(body=fallback_svg, content_type="image/svg+xml")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return window._lbCurrentSrcKey === '3840'
                && window._lbPending1To1 === false
                && img && img.complete && img.naturalWidth === 3840
                && window._lbZoom > 1;
        }"""
    )


def test_browse_lightbox_ignores_stale_original_failure_after_nav(live_server, page):
    """A late /original error from the previous photo must not poison the next photo."""
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="8000" height="4000" '
        'viewBox="0 0 8000 4000"><rect width="8000" height="4000" fill="#246"/></svg>'
    )
    held_original = {}

    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=full_svg, content_type="image/svg+xml"),
    )

    def hold_original(route):
        if "route" not in held_original:
            held_original["route"] = route
        else:
            route.abort()

    page.route("**/photos/*/original", hold_original)

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

    with page.expect_request("**/photos/*/original"):
        page.evaluate(
            """() => {
                window._lbPhotoW = 8000;
                window._lbPhotoH = 4000;
                window._lbFullLongEdge = 1000;
                window._lbOriginalUnavailable = false;
                window._lbZoom = 100;
                window._lbNativeZoom = 100;
                window._lbCurrentSrcKey = 'full';
                window._lbScheduleSourceSwap();
            }"""
        )
    deadline = time.time() + 2
    while "route" not in held_original and time.time() < deadline:
        page.wait_for_timeout(25)
    assert "route" in held_original

    page.evaluate(
        """() => {
            const next = window._lightboxPhotoList[1];
            window.openLightbox(next.id, next.filename, window._lightboxPhotoList);
        }"""
    )
    page.wait_for_function("window._lightboxCurrentId === window._lightboxPhotoList[1].id")
    held_original.pop("route").abort()
    page.wait_for_timeout(100)

    assert page.evaluate("window._lbOriginalUnavailable") is False


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
    assert page.evaluate("window._lbZoom > 1 || window._lbPending1To1") is True

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


def test_browse_lightbox_deferred_one_to_one_survives_original_failure_to_fallback(
    live_server, page
):
    """Codex P2: a deferred 1:1 must not snap on the upscaled /full when
    /original fails while a higher preview tier is still being fetched.

    When the user presses z before photo dimensions are known and /original
    then fails, the error path reschedules to the sharpest remaining tier.
    The inline resolve must stay deferred while that upgrade is queued —
    otherwise it snaps on the upscaled /full (soft-1:1 flash) and its trailing
    reschedule retargets the swap back to /full, canceling the upgrade and
    stranding the user on /full forever.

    Integer-only tier math (600 -> 2560, devicePixelRatio 1) makes
    _lbPickSourceKey land deterministically so the regression is a hard
    failure rather than a float-boundary coin flip.
    """
    page.add_init_script(
        "Object.defineProperty(window, 'devicePixelRatio', { value: 1, configurable: true });"
    )
    # /full is a small upscaled preview (600px); 1:1 genuinely needs a higher
    # tier. With dims unknown the lightbox can't tell the photo is large, so on
    # /original failure _lbPickSourceKey(_lbNativeZoom) reads the 600px /full
    # decode (== recorded _lbFullLongEdge) and returns 'full' — the boundary
    # that defeats the tier-rank guard the pre-fix code relied on.
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="600" height="400" '
        'viewBox="0 0 600 400"><rect width="600" height="400" fill="#274"/></svg>'
    )
    fallback_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="2560" height="1600" '
        'viewBox="0 0 2560 1600"><rect width="2560" height="1600" fill="#642"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=full_svg, content_type="image/svg+xml"),
    )
    page.route("**/photos/*/original", lambda route: route.abort())
    page.route(
        "**/photos/*/preview?size=2560",
        lambda route: route.fulfill(body=fallback_svg, content_type="image/svg+xml"),
    )
    page.route(
        "**/photos/*/preview?size=3840",
        lambda route: route.fulfill(body=fallback_svg, content_type="image/svg+xml"),
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
            return img && img.complete && img.naturalWidth === 600;
        }"""
    )

    # Simulate pressing z while photo dimensions are still unknown: bump
    # _lbOpenSeq so the in-flight /api/photos dims callback bails (it cannot
    # repopulate _lbPhotoW), then clear dims and toggle. With nativeZoom
    # unknown the deferred path schedules a swap to /original.
    page.evaluate(
        """() => {
            window._lbOpenSeq += 1;
            window._lbPhotoW = null;
            window._lbPhotoH = null;
            window._lbNativeZoom = null;
            window._lbOriginalUnavailable = false;
            window._lbCurrentSrcKey = 'full';
            window.toggleLightboxZoom();
        }"""
    )
    page.wait_for_function(
        "window._lbPending1To1 === true && window._lbDesiredSrcKey === 'original'"
    )
    assert abs(page.evaluate("window._lbZoom") - 1) < 0.001
    assert page.evaluate("window._lbCurrentSrcKey") == "full"

    # /original aborts. The fix must keep the deferral pending and retarget the
    # swap at the sharpest remaining preview tier (a real upgrade vs. /full).
    # Pre-fix the inline resolve snapped on /full (pickSourceKey == 'full', so
    # the tier-rank guard 0 < 0 did not defer) and its trailing reschedule
    # retargeted the swap back to 'full', so this state is never reached and
    # the wait fails fast.
    page.wait_for_function(
        """() => window._lbOriginalUnavailable === true
            && window._lbPending1To1 === true
            && window._lbCurrentSrcKey === 'full'
            && (window._lbDesiredSrcKey === '2560' || window._lbDesiredSrcKey === '3840')""",
        timeout=6000,
    )
    assert abs(page.evaluate("window._lbZoom") - 1) < 0.001

    # Once the fallback tier becomes the current source the deferred snap
    # completes at true 1:1 on that tier — never stranded on the upscaled /full.
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return (window._lbCurrentSrcKey === '2560' || window._lbCurrentSrcKey === '3840')
                && window._lbPending1To1 === false
                && img && img.complete && img.naturalWidth === 2560
                && window._lbNativeZoom
                && Math.abs(window._lbZoom - window._lbNativeZoom) < 0.01;
        }""",
        timeout=8000,
    )


def test_browse_lightbox_resize_preserves_post_original_failure_fallback(
    live_server, page
):
    """Codex P2 (Thread 11): a viewport resize while the post-/original-failure
    fallback tier is still loading must not cancel that upgrade.

    Repro: press z with dims unknown -> /original aborts -> the error path
    keeps the deferral pending and queues the sharpest remaining preview tier
    (2560/3840). While that tier is still loading the user resizes. The resize
    recovery path used to re-derive the swap target from _lbNativeZoom, which —
    because /original is unavailable and the current source is the upscaled
    /full — reflects the /full decode, so it re-picked 'full', canceled the
    in-flight fallback upgrade, and snapped a soft 1:1 on /full. Post-fix the
    deferred intent (and the higher-tier target) survives the resize.
    """
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1920" height="960" '
        'viewBox="0 0 1920 960"><rect width="1920" height="960" fill="#246"/></svg>'
    )
    fallback_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="2560" height="1600" '
        'viewBox="0 0 2560 1600"><rect width="2560" height="1600" fill="#642"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=full_svg, content_type="image/svg+xml"),
    )
    page.route("**/photos/*/original", lambda route: route.abort())
    held_fallback = {}

    def hold_fallback(route):
        # Hold the most recent fallback-tier request (2560 or 3840) until the
        # test explicitly releases it. The resize re-arms the swap, so a later
        # request supersedes the earlier held one — keep only the live route.
        if "released" in held_fallback:
            route.fulfill(body=fallback_svg, content_type="image/svg+xml")
        else:
            held_fallback["route"] = route

    page.route("**/photos/*/preview?size=2560", hold_fallback)
    page.route("**/photos/*/preview?size=3840", hold_fallback)

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

    page.evaluate(
        """() => {
            window._lbPhotoW = null;
            window._lbPhotoH = null;
            window._lbOriginalUnavailable = false;
            window._lbCurrentSrcKey = 'full';
            window._lbNativeZoom = null;
            window._lbZoom = 1;
            window.toggleLightboxZoom();
        }"""
    )

    # /original aborts; the deferred 1:1 must now be pending against the
    # sharpest remaining preview tier (a real upgrade vs. the /full it is on).
    deadline = time.time() + 3
    while "route" not in held_fallback and time.time() < deadline:
        page.wait_for_timeout(25)
    assert "route" in held_fallback
    waiting = page.evaluate(
        """() => ({
            pending: window._lbPending1To1,
            zoom: window._lbZoom,
            currentSource: window._lbCurrentSrcKey,
            desiredSource: window._lbDesiredSrcKey,
        })"""
    )
    assert waiting["pending"] is True
    assert abs(waiting["zoom"] - 1) < 0.001
    assert waiting["currentSource"] == "full"
    assert waiting["desiredSource"] in ("2560", "3840")

    native_zoom_before = page.evaluate("window._lbNativeZoom")

    # Resize while the fallback tier is still held. _lbRecomputeNativeZoom runs
    # unconditionally at the top of the resize handler (before any pending-state
    # logic), so a deterministic change in _lbNativeZoom is a fix-independent
    # signal that the debounced handler actually executed — no fixed sleep.
    page.set_viewport_size({"width": 640, "height": 800})
    page.wait_for_function(
        "Math.abs(window._lbNativeZoom - %r) > 0.1" % native_zoom_before
    )

    # The resize handler has now run while the fallback tier is still loading.
    # Pre-fix it canceled the upgrade and snapped a soft 1:1 on /full; post-fix
    # the deferred intent and the higher-tier target both survive.
    survived = page.evaluate(
        """() => ({
            pending: window._lbPending1To1,
            zoom: window._lbZoom,
            currentSource: window._lbCurrentSrcKey,
            desiredSource: window._lbDesiredSrcKey,
        })"""
    )
    assert survived["pending"] is True
    assert abs(survived["zoom"] - 1) < 0.001
    assert survived["currentSource"] == "full"
    assert survived["desiredSource"] in ("2560", "3840")

    # Releasing the fallback tier lets the deferred 1:1 finally snap against it
    # — never stranded on the upscaled /full.
    held_fallback["released"] = True
    held_fallback.pop("route").fulfill(
        body=fallback_svg, content_type="image/svg+xml"
    )
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return (window._lbCurrentSrcKey === '2560' || window._lbCurrentSrcKey === '3840')
                && window._lbPending1To1 === false
                && img && img.complete && img.naturalWidth === 2560
                && window._lbNativeZoom
                && Math.abs(window._lbZoom - window._lbNativeZoom) < 0.01;
        }""",
        timeout=8000,
    )
