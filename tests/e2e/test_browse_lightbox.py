import base64
import re
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
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
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
    expect(counter).to_contain_text(first_filename)

    page.locator("[title='Next (→)']").click()

    expect(filename_display).not_to_have_text(first_filename)
    expect(counter).to_contain_text("2 /")
    expect(counter).to_contain_text(filename_display.text_content())

    page.locator("[title='Previous (←)']").click()
    expect(filename_display).to_have_text(first_filename)
    expect(counter).to_contain_text("1 /")
    expect(counter).to_contain_text(first_filename)


def test_browse_photo_id_deep_link_loads_target_folder_first_page(live_server, page):
    """Open in Browse must find a target that is not on global Browse page 1."""
    db = live_server["db"]
    folder_a, folder_b = live_server["data"]["folders"]
    target_id = live_server["data"]["photos"][3]  # first photo in folder_b

    for idx in range(60):
        db.add_photo(
            folder_id=folder_a,
            filename=f"older-{idx:02d}.jpg",
            extension=".jpg",
            file_size=1000,
            file_mtime=10 + idx,
            timestamp=f"2024-01-{(idx % 28) + 1:02d}T00:00:00",
        )

    page.goto(f"{live_server['url']}/browse?photo_id={target_id}")

    target_card = page.locator(f'.grid-card[data-id="{target_id}"]')
    expect(target_card).to_be_visible(timeout=5000)
    assert page.evaluate("window.activeFolderId") == folder_b


def test_browse_photo_id_deep_link_loads_target_after_first_folder_page(live_server, page):
    """Open in Browse must page within the target folder without freezing."""
    db = live_server["db"]
    _, folder_b = live_server["data"]["folders"]
    target_id = live_server["data"]["photos"][4]  # robin2 in folder_b

    for idx in range(60):
        db.add_photo(
            folder_id=folder_b,
            filename=f"yard-before-{idx:02d}.jpg",
            extension=".jpg",
            file_size=1000,
            file_mtime=10 + idx,
            timestamp=f"2024-06-14T10:{idx % 60:02d}:00",
        )

    page.goto(f"{live_server['url']}/browse?photo_id={target_id}")

    target_card = page.locator(f'.grid-card[data-id="{target_id}"]')
    expect(target_card).to_be_visible(timeout=5000)
    assert page.evaluate("window.loading") is False


def test_browse_lightbox_arrows_preserve_one_to_one_zoom(live_server, page):
    """Navigating from a 1:1 lightbox view keeps the next photo at 1:1."""
    url = live_server["url"]

    # The pending-1:1 state set on navigation is cleared the instant the next
    # photo's native zoom is learned — which happens via TWO async paths: the
    # /api/photos/<id> metadata fetch and the /original image's onload. If
    # either resolves before the synchronous assertion below, _lbPending1To1
    # has already flipped to False and the test flakes (it did on the v0.23.0
    # release build). Hold both for the target photo so the pending state is
    # deterministic during the assertion window; the second phase then learns
    # native zoom explicitly and verifies the deferred snap applies.
    hold = {"active": False, "held": []}

    def _hold_when_active(route):
        if hold["active"]:
            hold["held"].append(route)  # park it; never resolves during asserts
        else:
            route.continue_()

    page.route(re.compile(r"/api/photos/\d+$"), _hold_when_active)
    page.route("**/photos/*/original", _hold_when_active)

    page.goto(f"{url}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    with page.expect_response(lambda r: "/api/photos/1" in r.url and r.status == 200):
        first_card.dblclick()

    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")

    # Put photo 1 into a 1:1 view. Crucially set _lbPending1To1 = true rather
    # than relying on _lbZoom == _lbNativeZoom: lightboxNav() carries the 1:1
    # intent forward via _lbIsOneToOneZoom(), which returns true immediately when
    # _lbPending1To1 is set but otherwise depends on _lbNativeZoom. The fixture
    # photos are seeded without width/height, so photo 1's async /api/photos/1
    # metadata (width=null) recomputes _lbNativeZoom to null; if that lands after
    # this force (as it does under CI CPU contention), _lbIsOneToOneZoom() would
    # be false at Next and the next photo would not inherit the pending 1:1 —
    # exactly the failure that blocked the v0.24.0 release build. Keying off
    # pending makes the carry-forward immune to that clobber.
    page.evaluate(
        """() => {
            window._lbNativeZoom = 2;
            window._lbZoom = 2;
            window._lbPending1To1 = true;
        }"""
    )

    # From here on, stall the next photo's native-zoom sources so the deferred
    # 1:1 snap cannot resolve before we observe it.
    hold["active"] = True
    page.locator("[title='Next (→)']").click()
    expect(page.locator("#lightboxCounter")).to_contain_text("1 /")
    # Guard that the hold worked: native zoom must still be unknown, so the
    # pending assertion below is genuinely exercising the deferred path.
    assert page.evaluate("window._lbNativeZoom") is None
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

    # Release the parked requests so context teardown doesn't wait on them.
    hold["active"] = False
    for route in hold["held"]:
        route.abort()


def test_browse_lightbox_predecodes_adjacent_photo_for_current_source_tier(
    live_server, page
):
    """The next photo is decoded while the user is still viewing the current one."""
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#274"/></svg>'
    )
    original_requests = []

    def serve_original(route):
        original_requests.append(route.request.url)
        route.fulfill(body=svg, content_type="image/svg+xml")

    page.route("**/photos/*/original", serve_original)
    page.goto(f"{live_server['url']}/browse")
    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    first_card.dblclick()
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")

    next_id = page.evaluate("window._lightboxPhotoList[1].id")
    page.evaluate("window._lbScheduleSourceSwap(100)")
    page.wait_for_function(
        """nextId => Object.values(window._lbAdjacentPreloads).some(entry => (
            entry.photoId === nextId && entry.sourceKey === 'original' && entry.status === 'decoded'
        ))""",
        arg=next_id,
    )

    assert any(f"/photos/{next_id}/original" in url for url in original_requests)
    assert page.evaluate("window._lightboxCurrentId") != next_id


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


def test_browse_lightbox_holds_off_center_transform_until_next_photo_is_ready(
    live_server, page
):
    """100% navigation must keep the outgoing photo intact while loading.

    The incoming photo's metadata often resolves before its image. Previously
    navigation reset pan immediately, visibly jerking the outgoing bitmap to
    center, then restored the carried viewport when the new bitmap decoded.
    Its filename and counter also advanced while that old bitmap was visible.
    """
    first_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#274"/>'
        '<circle cx="1000" cy="1400" r="180" fill="#fff"/></svg>'
    )
    next_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="3000" height="3000" '
        'viewBox="0 0 3000 3000"><rect width="3000" height="3000" fill="#426"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=first_svg, content_type="image/svg+xml"),
    )

    url = live_server["url"]
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{url}/browse")
    page.locator(".grid-card").nth(1).wait_for(state="visible")
    next_id = page.evaluate("window.photos[1].id")
    held_original = {}

    def hold_next_original(route):
        if f"/photos/{next_id}/original" in route.request.url:
            held_original["route"] = route
            return
        route.fulfill(body=first_svg, content_type="image/svg+xml")

    page.route("**/photos/*/original", hold_next_original)

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

    before = page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            window._lbCurrentSrcKey = 'original';
            window._lbApplyViewportState({
                zoom: window._lbNativeZoom,
                centerX: 0.24,
                centerY: 0.70,
                oneToOne: true,
            });
            window._lbSaveViewportState(window._lightboxCurrentId);
            const transform = document.getElementById('lightboxTransform');
            return {
                cssTransform: transform.style.transform,
                width: transform.style.width,
                height: transform.style.height,
                viewport: window._lbViewportStateFromCurrent(),
            };
        }"""
    )
    page.evaluate(
        """() => {
            window.__photoChangedDuringNavigation = [];
            document.addEventListener('lightbox:photochanged', event => {
                window.__photoChangedDuringNavigation.push(event.detail.photoId);
            });
            document.getElementById('lightboxAdjustPanel').classList.add('open');
        }"""
    )

    page.keyboard.press("ArrowRight")
    page.wait_for_function("() => window._lbVisualTransitionPending === true")
    page.wait_for_function("() => window._lightboxCurrentId === window.photos[1].id")
    page.wait_for_timeout(100)

    deadline = time.time() + 2
    while "route" not in held_original and time.time() < deadline:
        page.wait_for_timeout(10)
    assert "route" in held_original

    # The outgoing bitmap remains visible while the incoming original is
    # loading, so its filename and position must remain visible too.
    expect(page.locator("#lightboxFilename")).to_have_text(
        first_card.get_attribute("data-filename")
    )
    expect(page.locator("#lightboxCounter")).to_contain_text("1 /")
    expect(page.locator("#lightboxActions")).to_have_attribute("inert", "")
    expect(page.locator("#lightboxAdjustPanel")).to_have_attribute("inert", "")
    assert page.evaluate("window.__photoChangedDuringNavigation") == []

    # Photo-targeted keyboard actions are suppressed along with the buttons;
    # they must not mutate the incoming photo while the outgoing one is shown.
    page.keyboard.press("p")
    assert page.evaluate("window._lbFlagPendingWrites") == 0

    interaction_state = page.evaluate(
        """() => {
            const img = document.getElementById('lightboxImg');
            const beforeZoom = window._lbZoom;
            img.dispatchEvent(new WheelEvent('wheel', {
                bubbles: true, cancelable: true, deltaY: -120,
                clientX: 400, clientY: 300,
            }));
            img.dispatchEvent(new MouseEvent('contextmenu', {
                bubbles: true, cancelable: true, button: 2,
                clientX: 400, clientY: 300,
            }));
            img.dispatchEvent(new MouseEvent('click', {
                bubbles: true, cancelable: true, button: 0,
                clientX: 400, clientY: 300,
            }));
            return {
                beforeZoom: beforeZoom,
                afterZoom: window._lbZoom,
                nativePhotoIds: window.nativeMenuActivePhotoIds(),
            };
        }"""
    )
    assert interaction_state["afterZoom"] == interaction_state["beforeZoom"]
    assert interaction_state["nativePhotoIds"] == []
    expect(page.locator(".vireo-ctx-menu")).to_have_count(0)

    while_loading = page.evaluate(
        """() => {
            const transform = document.getElementById('lightboxTransform');
            return {
                cssTransform: transform.style.transform,
                width: transform.style.width,
                height: transform.style.height,
            };
        }"""
    )
    assert while_loading == {
        "cssTransform": before["cssTransform"],
        "width": before["width"],
        "height": before["height"],
    }

    held_original.pop("route").fulfill(
        body=next_svg, content_type="image/svg+xml"
    )
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return window._lbVisualTransitionPending === false
                && window._lbPendingViewportState === null
                && img && img.complete && img.naturalWidth === 3000;
        }"""
    )
    expect(page.locator("#lightboxFilename")).to_have_text(
        page.locator(".grid-card").nth(1).get_attribute("data-filename")
    )
    expect(page.locator("#lightboxCounter")).to_contain_text("2 /")
    assert page.evaluate("window.__photoChangedDuringNavigation") == [next_id]
    assert page.evaluate(
        "!document.getElementById('lightboxActions').inert"
    )
    assert page.evaluate(
        "!document.getElementById('lightboxAdjustPanel').inert"
    )
    carried = page.evaluate("window._lbViewportStateFromCurrent()")
    assert abs(carried["centerX"] - before["viewport"]["centerX"]) < 0.03
    assert abs(carried["centerY"] - before["viewport"]["centerY"]) < 0.03


def test_browse_lightbox_mid_transition_save_does_not_leak_outgoing_transform(
    live_server, page
):
    """A save while the outgoing bitmap is still frozen must not stomp the
    incoming photo's intended inspection point.

    Regression: while _lbVisualTransitionPending is true, _lightboxCurrentId
    already points at the incoming photo but the DOM transform still belongs
    to the outgoing bitmap. A save triggered by another arrow press or
    closeLightbox in that window used to read the DOM and record the outgoing
    transform under the incoming photo's id, replacing the state the next
    open of that photo would otherwise restore.
    """
    first_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#274"/>'
        '<circle cx="1000" cy="1400" r="180" fill="#fff"/></svg>'
    )
    next_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="3000" height="3000" '
        'viewBox="0 0 3000 3000"><rect width="3000" height="3000" fill="#426"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=first_svg, content_type="image/svg+xml"),
    )

    url = live_server["url"]
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{url}/browse")
    page.locator(".grid-card").nth(1).wait_for(state="visible")
    next_id = page.evaluate("window.photos[1].id")
    held_original = {}

    def hold_next_original(route):
        if f"/photos/{next_id}/original" in route.request.url:
            held_original["route"] = route
            return
        route.fulfill(body=first_svg, content_type="image/svg+xml")

    page.route("**/photos/*/original", hold_next_original)

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

    outgoing = page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            window._lbCurrentSrcKey = 'original';
            window._lbApplyViewportState({
                zoom: window._lbNativeZoom,
                centerX: 0.20,
                centerY: 0.75,
                oneToOne: true,
            });
            window._lbSaveViewportState(window._lightboxCurrentId);
            return window._lbViewportStateFromCurrent();
        }"""
    )

    # Prime a distinctive saved viewport for the incoming photo so we can
    # tell "our intended state survives" from "outgoing DOM state leaked in".
    incoming_id = page.evaluate("window.photos[1].id")
    intended = {"zoom": 2.5, "centerX": 0.80, "centerY": 0.15}
    page.evaluate(
        """([id, state]) => {
            window._lbViewportByPhotoId[String(id)] = {
                zoom: state.zoom,
                centerX: state.centerX,
                centerY: state.centerY,
                oneToOne: false,
                pending1To1: false,
            };
        }""",
        [incoming_id, intended],
    )

    page.locator("[title='Next (→)']").click()
    expect(page.locator("#lightboxCounter")).to_contain_text("1 /")
    page.wait_for_function("() => window._lbVisualTransitionPending === true")
    page.wait_for_function("() => window._lightboxCurrentId === window.photos[1].id")
    page.wait_for_timeout(50)
    deadline = time.time() + 2
    while "route" not in held_original and time.time() < deadline:
        page.wait_for_timeout(10)
    assert "route" in held_original

    # Simulate the user pressing another arrow / closing the lightbox before
    # the incoming image finishes decoding: openLightbox / lightboxNav /
    # closeLightbox all call _lbSaveViewportState(_lightboxCurrentId) in this
    # state. The DOM transform is still the outgoing bitmap's.
    saved_during_transition = page.evaluate(
        """(id) => {
            const returned = window._lbSaveViewportState(id);
            return {
                returned: returned,
                stored: window._lbViewportByPhotoId[String(id)],
            };
        }""",
        incoming_id,
    )

    # The stored state for the incoming photo must not be the outgoing
    # bitmap's transform. It should be either the pending restore state that
    # openLightbox armed for the incoming photo, or the pre-existing saved
    # state we primed above — never the outgoing photo's centerX/centerY.
    stored = saved_during_transition["stored"]
    assert stored is not None
    # Guard against the specific regression: the outgoing photo's off-center
    # inspection point (0.20, 0.75) must not be recorded under the incoming
    # photo's id.
    assert not (
        abs(stored["centerX"] - outgoing["centerX"]) < 0.05
        and abs(stored["centerY"] - outgoing["centerY"]) < 0.05
    ), (
        "outgoing DOM transform leaked into incoming photo's saved viewport"
    )

    held_original.pop("route").fulfill(
        body=next_svg, content_type="image/svg+xml"
    )
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return window._lbVisualTransitionPending === false
                && img && img.complete && img.naturalWidth === 3000;
        }"""
    )


def test_browse_lightbox_clears_transition_state_when_incoming_image_errors(
    live_server, page
):
    """A non-'original' image error must clear _lbVisualTransitionPending.

    Regression: handleInitialImageError's early-return path (taken when the
    failing tier isn't the /original fallback candidate) previously left
    _lbVisualTransitionPending true indefinitely. That kept the metadata
    callback skipping layout updates and made _lbSaveViewportState treat the
    incoming photo as still mid-transition, freezing the outgoing transform on
    screen until the lightbox was closed or another navigation succeeded.
    """
    first_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#274"/>'
        '<circle cx="1000" cy="1400" r="180" fill="#fff"/></svg>'
    )

    url = live_server["url"]
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{url}/browse")
    page.locator(".grid-card").nth(1).wait_for(state="visible")
    next_id = page.evaluate("window.photos[1].id")

    def route_full(route):
        # Fail the incoming photo's /full tier; the outgoing photo's /full
        # still resolves normally so we can enter the mid-navigation window.
        if f"/photos/{next_id}/full" in route.request.url:
            route.fulfill(status=404, body=b"", content_type="text/plain")
            return
        route.fulfill(body=first_svg, content_type="image/svg+xml")

    page.route("**/photos/*/full", route_full)

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

    # A fit-view navigation opens the incoming photo at _lbCurrentSrcKey='full'
    # with _lbVisualTransitionPending=true. The /full request then 404s, so
    # handleInitialImageError takes the non-'original' early-return path.
    page.locator("[title='Next (→)']").click()
    page.wait_for_function(
        "() => window._lightboxCurrentId === window.photos[1].id"
    )

    page.wait_for_function(
        "() => window._lbVisualTransitionPending === false",
        timeout=3000,
    )
    expect(page.locator("#lightboxFilename")).to_have_text(
        page.locator(".grid-card").nth(1).get_attribute("data-filename")
    )
    expect(page.locator("#lightboxCounter")).to_contain_text("2 /")
    assert page.evaluate(
        "!document.getElementById('lightboxActions').inert"
    )

    # With the pending flag cleared, saving the current photo's viewport must
    # go through the normal (non-guard) path — the guard block only activates
    # while a transition is pending — so the incoming photo id is a legal
    # save target rather than a frozen-outgoing snapshot sink.
    saved = page.evaluate(
        """() => {
            const id = window._lightboxCurrentId;
            const returned = window._lbSaveViewportState(id);
            return {
                returned: returned,
                pendingFlag: window._lbVisualTransitionPending,
            };
        }"""
    )
    assert saved["pendingFlag"] is False
    assert saved["returned"] is not None


def test_browse_lightbox_defers_overlays_while_visual_transition_pending(
    live_server, page
):
    """Detection boxes must not paint against the frozen outgoing bitmap.

    Regression: while `_lbVisualTransitionPending` is true the transform is
    intentionally held on the outgoing image so the swap looks atomic. The
    metadata fetch usually resolves before the incoming bitmap decodes, and
    the metadata callback triggers `_lbLoadDetections` for the incoming
    photo. Without deferral the box overlays render into
    `#lightboxDetections` — a child of `#lightboxTransform` — using the
    incoming photo's coordinates but drawn over the still-frozen outgoing
    bitmap, briefly flashing next-photo boxes over the previous image.
    """
    first_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#274"/>'
        '<circle cx="1000" cy="1400" r="180" fill="#fff"/></svg>'
    )
    next_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="3000" height="3000" '
        'viewBox="0 0 3000 3000"><rect width="3000" height="3000" fill="#426"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=first_svg, content_type="image/svg+xml"),
    )

    url = live_server["url"]
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{url}/browse")
    page.locator(".grid-card").nth(1).wait_for(state="visible")
    next_id = page.evaluate("window.photos[1].id")

    held_original = {}

    def hold_next_original(route):
        if f"/photos/{next_id}/original" in route.request.url:
            held_original["route"] = route
            return
        route.fulfill(body=first_svg, content_type="image/svg+xml")

    page.route("**/photos/*/original", hold_next_original)

    detection_requests = []

    def serve_detections(route):
        detection_requests.append(route.request.url)
        # Two boxes for the incoming photo, one for the outgoing photo so
        # differentiating between "no detections yet" and "outgoing detections
        # still showing" would be trivial had the regression re-surfaced.
        if f"/api/detections/{next_id}" in route.request.url:
            body = (
                '[{"box_x":0.1,"box_y":0.2,"box_w":0.15,"box_h":0.20,'
                '"category":"bird","detector_confidence":0.9},'
                '{"box_x":0.5,"box_y":0.6,"box_w":0.12,"box_h":0.10,'
                '"category":"bird","detector_confidence":0.8}]'
            )
        else:
            body = (
                '[{"box_x":0.3,"box_y":0.3,"box_w":0.1,"box_h":0.1,'
                '"category":"bird","detector_confidence":0.7}]'
            )
        route.fulfill(body=body, content_type="application/json")

    page.route("**/api/detections/*", serve_detections)

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
    # Trigger 1:1 so the arrow navigation opens the incoming photo at /original
    # (which we hold below to keep the transition pending).
    page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            window._lbCurrentSrcKey = 'original';
            window._lbApplyViewportState({
                zoom: window._lbNativeZoom,
                centerX: 0.5,
                centerY: 0.5,
                oneToOne: true,
            });
            window._lbSaveViewportState(window._lightboxCurrentId);
        }"""
    )

    page.locator("[title='Next (→)']").click()
    expect(page.locator("#lightboxCounter")).to_contain_text("1 /")
    page.wait_for_function("() => window._lbVisualTransitionPending === true")
    page.wait_for_function("() => window._lightboxCurrentId === window.photos[1].id")

    # Wait for the incoming photo's metadata /api/photos/{id} to have resolved
    # (which normally fires the detection load) while the image is still held.
    page.wait_for_function(
        """nextId => window._lbPhotoDataByPhoto
            && Object.prototype.hasOwnProperty.call(
                window._lbPhotoDataByPhoto, String(nextId)
            )""",
        arg=next_id,
    )

    deadline = time.time() + 2
    while "route" not in held_original and time.time() < deadline:
        page.wait_for_timeout(10)
    assert "route" in held_original

    # Give the deferred detection fetch a chance to have (incorrectly) fired.
    page.wait_for_timeout(80)

    during_pending = page.evaluate(
        """() => {
            const container = document.getElementById('lightboxDetections');
            return {
                pending: window._lbVisualTransitionPending,
                childCount: container ? container.childElementCount : -1,
                deferred: typeof window._lbDeferredOverlayApply === 'function',
            };
        }"""
    )
    assert during_pending["pending"] is True, (
        "test setup: transition should still be pending while image is held"
    )
    assert during_pending["childCount"] == 0, (
        "detection boxes rendered against the frozen outgoing transform"
    )
    assert during_pending["deferred"] is True, (
        "overlay render was not deferred while the transition was pending"
    )
    # And the network fetch itself should not have gone out yet for the
    # incoming photo — the deferral holds both the request and the render.
    assert not any(
        f"/api/detections/{next_id}" in url for url in detection_requests
    ), "detection request for the incoming photo fired while transition pending"

    held_original.pop("route").fulfill(
        body=next_svg, content_type="image/svg+xml"
    )

    page.wait_for_function(
        "() => window._lbVisualTransitionPending === false"
    )
    # Once the transition clears, the deferred overlay work runs and the
    # incoming photo's detection boxes render normally.
    page.wait_for_function(
        """() => {
            const container = document.getElementById('lightboxDetections');
            return container && container.childElementCount === 2;
        }""",
        timeout=3000,
    )
    assert any(
        f"/api/detections/{next_id}" in url for url in detection_requests
    ), "detection fetch never fired after transition cleared"


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
    page.wait_for_function(
        """() => (
            window._lbZoom > 1.001 &&
            window._lbNativeZoom > 1 &&
            window._lbFitScale > 0 &&
            !window._lbPending1To1
        )"""
    )
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

    # The fixture photos are seeded without width/height, so /api/photos/<id>
    # returns width=null. When that async metadata fetch resolves it overwrites
    # the _lbPhotoW=4000 injected below with null (app: `_lbPhotoW = data.width
    # || null`), which nulls _lbNativeZoom. Depending on whether that lands
    # before or after the native-zoom reads below, the test either crashed
    # ("None is not defined") or hung waiting for native zoom to settle. Force
    # real dimensions into the metadata response so _lbPhotoW stays 4000 and
    # native zoom is stable for the duration of the test.
    def force_photo_dims(route):
        resp = route.fetch()
        data = resp.json()
        data["width"] = 4000
        data["height"] = 2000
        route.fulfill(response=resp, json=data)

    page.route(re.compile(r"/api/photos/\d+$"), force_photo_dims)

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
    # The deferred swap is scheduled on a debounced timer; under CI CPU
    # contention that timer plus the preloader round-trip can take well over 2s,
    # so allow generous headroom before asserting the /original request was held.
    deadline = time.time() + 8
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

    # The fixture photos are seeded without width/height, so /api/photos/<id>
    # returns width=null. When that async metadata fetch resolves it overwrites
    # the _lbPhotoW=4000 injected below with null (app: `_lbPhotoW = data.width
    # || null`), which nulls _lbNativeZoom. Depending on whether that lands
    # before or after the native-zoom reads below, the test either crashed
    # ("None is not defined") or hung waiting for native zoom to settle. Force
    # real dimensions into the metadata response so _lbPhotoW stays 4000 and
    # native zoom is stable for the duration of the test.
    def force_photo_dims(route):
        resp = route.fetch()
        data = resp.json()
        data["width"] = 4000
        data["height"] = 2000
        route.fulfill(response=resp, json=data)

    page.route(re.compile(r"/api/photos/\d+$"), force_photo_dims)

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

    # Stash the pre-resize native zoom in a page variable rather than reading it
    # into Python and interpolating it back. During the deferred /original swap
    # _lbNativeZoom can be transiently unset; a Python None then formats into the
    # wait expression as the literal `None`, which throws "None is not defined"
    # in JS. Guard that it is a finite number first, then compare in-page.
    page.wait_for_function(
        "typeof window._lbNativeZoom === 'number' && isFinite(window._lbNativeZoom)"
    )
    page.evaluate("window._lbNativeZoomBaseline = window._lbNativeZoom")

    # Resize while the high-res source is still loading. The image is 4000px
    # wide so _lbFitScale (hence _lbNativeZoom) is width-constrained; shrinking
    # the viewport width forces a deterministic _lbNativeZoom change once the
    # resize handler runs. The handler recomputes _lbNativeZoom unconditionally
    # (before any pending-state logic), so the change below is a fix-independent
    # signal that the debounced handler has actually executed — no fixed sleep.
    page.set_viewport_size({"width": 640, "height": 800})
    page.wait_for_function(
        "Math.abs(window._lbNativeZoom - window._lbNativeZoomBaseline) > 0.1"
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

    # Stash the pre-resize native zoom in a page variable rather than reading it
    # into Python and interpolating it back. While the fallback tier is loading
    # _lbNativeZoom can be transiently unset; a Python None then formats into the
    # wait expression as the literal `None`, which throws "None is not defined"
    # in JS. Guard that it is a finite number first, then compare in-page.
    page.wait_for_function(
        "typeof window._lbNativeZoom === 'number' && isFinite(window._lbNativeZoom)"
    )
    page.evaluate("window._lbNativeZoomBaseline = window._lbNativeZoom")

    # Resize while the fallback tier is still held. _lbRecomputeNativeZoom runs
    # unconditionally at the top of the resize handler (before any pending-state
    # logic), so a deterministic change in _lbNativeZoom is a fix-independent
    # signal that the debounced handler actually executed — no fixed sleep.
    page.set_viewport_size({"width": 640, "height": 800})
    page.wait_for_function(
        "Math.abs(window._lbNativeZoom - window._lbNativeZoomBaseline) > 0.1"
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
