"""Navigation responsiveness, decoded-memory limits, and progressive quality."""

import io
import re
from functools import lru_cache

import pytest
from PIL import Image
from playwright.sync_api import expect


@lru_cache(maxsize=8)
def _jpeg(width=1920, height=1280, color="#227744"):
    # Use real raster previews: WebKit gives SVGs container-dependent natural
    # dimensions, which do not exercise the photo decoder or its memory costs.
    output = io.BytesIO()
    Image.new("RGB", (width, height), color).save(output, format="JPEG")
    return output.getvalue()


def _open_window(page, live_server, count=30):
    page.route(
        re.compile(r"/api/photos/\d+$"),
        lambda route: route.fulfill(json={
            "id": int(route.request.url.rsplit("/", 1)[1]),
            "width": 6000, "height": 4000, "full_uses_original": False,
            "full_preview_max_size": 1920,
            "edit_recipe": None, "flag": "none",
        }),
    )
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.evaluate(
        """count => {
          // Isolate navigation from the separate idle-time 1:1 warmup.
          _lbScheduleOriginalPreload = function() {};
          const list = Array.from({length: count}, (_, i) => ({
            id: 100 + i, filename: `photo-${i}.jpg`, width: 6000, height: 4000,
            edit_recipe: null
          }));
          openLightbox(115, 'photo-15.jpg', list);
        }""", count,
    )
    page.wait_for_function("_lightboxCommittedId === 115 && !_lbVisualTransitionPending && _lbNativeZoom")


def test_large_window_retains_eight_ahead_four_behind_within_budget(live_server, page):
    page.route("**/photos/*/full*", lambda route: route.fulfill(body=_jpeg(), content_type="image/jpeg"))
    _open_window(page, live_server)
    expected = [111, 112, 113, 114, *range(116, 124)]
    page.wait_for_function(
        """ids => ids.every(id => Object.values(_lbAdjacentPreloads).some(
          entry => entry.photoId === id && entry.status === 'decoded'
        ))""", arg=expected,
    )
    assert page.evaluate("_lbPreloadBytes()") <= 128 * 1024 * 1024
    assert page.evaluate("Object.values(_lbAdjacentPreloads).map(entry => entry.photoId).sort((a,b) => a-b)") == expected
    page.evaluate("lightboxNav(-1)")
    page.wait_for_function("_lightboxCommittedId === 114 && !_lbVisualTransitionPending")
    assert "prefetch=1" in page.locator("#lightboxImg").get_attribute("src")
    page.keyboard.press("Escape")
    page.wait_for_function("_lbSpeculativeLoads.size === 0")
    assert page.evaluate("_lbPreloadBytes()") == 0


def test_slow_preload_does_not_block_other_cached_neighbors(live_server, page):
    held = []

    def serve(route):
        if "/116/" in route.request.url and "prefetch=1" in route.request.url:
            held.append(route)
            return
        route.fulfill(body=_jpeg(), content_type="image/jpeg")

    page.route("**/photos/*/full*", serve)
    _open_window(page, live_server)
    page.wait_for_function(
        """Object.values(_lbAdjacentPreloads).some(entry => entry.photoId === 117 && entry.status === 'decoded')"""
    )
    assert len(held) == 1
    assert page.evaluate("_lbSpeculativeLoads.size") <= 3
    page.evaluate("lightboxNav(2)")
    page.wait_for_function("_lightboxCommittedId === 117 && !_lbVisualTransitionPending")
    held[0].fulfill(body=_jpeg(), content_type="image/jpeg")


def test_actual_decode_sizes_cannot_leave_cache_over_budget(live_server, page):
    # The server unexpectedly supplies four times the anticipated pixel count.
    page.route("**/photos/*/full*", lambda route: route.fulfill(body=_jpeg(3840, 2560), content_type="image/jpeg"))
    page.route("**/photos/*/preview?*", lambda route: route.fulfill(body=_jpeg(3840, 2560), content_type="image/jpeg"))
    _open_window(page, live_server)
    page.wait_for_function("Object.values(_lbAdjacentPreloads).filter(e => e.status === 'decoded').length >= 2")
    page.wait_for_function("_lbSpeculativeLoads.size === 0 && _lbPreloadBytes() <= _lbPreloadBudgetBytes")
    assert page.evaluate("Object.values(_lbAdjacentPreloads).filter(e => e.status === 'decoded').length") <= 3


@pytest.mark.parametrize("one_to_one", [False, True])
def test_ready_preview_commits_before_sharp_image_without_moving_view(live_server, page, one_to_one):
    held = []
    sharp_ready = False

    def serve_original(route):
        if "/116/" in route.request.url and not sharp_ready:
            held.append(route)
            return
        route.fulfill(body=_jpeg(6000, 4000), content_type="image/jpeg")

    page.route("**/photos/*/full*", lambda route: route.fulfill(body=_jpeg(), content_type="image/jpeg"))
    page.route("**/photos/*/original*", serve_original)
    _open_window(page, live_server)
    page.wait_for_function("Object.values(_lbAdjacentPreloads).some(e => e.photoId === 116 && e.status === 'decoded')")
    before = page.evaluate(
        """oneToOne => {
          _lbApplyViewportState({zoom: _lbNativeZoom * 1.2, oneToOne, centerX: 0.4, centerY: 0.6});
          return _lbViewportStateFromCurrent();
        }""", one_to_one,
    )
    page.wait_for_function("_lbCurrentSrcKey === 'original' && !_lbPreviewLoading")
    page.evaluate("lightboxNav(1)")
    page.wait_for_function("_lightboxCommittedId === 116 && !_lbVisualTransitionPending")
    expect(page.locator("#lightboxPreviewStatus")).to_be_visible()
    assert page.evaluate("document.getElementById('lightboxImg').naturalWidth") == 1920
    assert page.evaluate("_lbCurrentSrcKey") == "full"
    preview = page.evaluate("_lbViewportStateFromCurrent()")
    assert abs(preview["centerX"] - before["centerX"]) < 0.005
    assert abs(preview["centerY"] - before["centerY"]) < 0.005
    page.wait_for_function("_lbDesiredSrcKey === 'original'")
    page.wait_for_timeout(200)  # Dispatch the held visible source request.
    assert held
    sharp_ready = True
    for route in held:
        route.fulfill(body=_jpeg(6000, 4000), content_type="image/jpeg")
    page.wait_for_function("_lbCurrentSrcKey === 'original' && !_lbPreviewLoading")
    after = page.evaluate("_lbViewportStateFromCurrent()")
    assert abs(after["centerX"] - preview["centerX"]) < 0.005
    assert abs(after["centerY"] - preview["centerY"]) < 0.005
    assert abs(after["zoom"] - preview["zoom"]) < 0.005


@pytest.mark.parametrize("needed,key", [(2300, "2560"), (3200, "3840"), (5000, "original")])
def test_restored_zoom_requests_only_required_resolution(live_server, page, needed, key):
    requested = []

    def serve(route):
        requested.append(route.request.url)
        route.fulfill(body=_jpeg(6000, 4000), content_type="image/jpeg")

    page.route("**/photos/*/full*", lambda route: route.fulfill(body=_jpeg(), content_type="image/jpeg"))
    page.route("**/photos/*/original*", serve)
    page.route("**/photos/*/preview?*", serve)
    _open_window(page, live_server)
    page.evaluate(
        """needed => {
          // Exercise initial selection without a ready fallback obscuring it.
          _lbClearAdjacentPreloads();
          const wrap = document.getElementById('lightboxWrap');
          const fit = Math.min(1, wrap.clientWidth / 6000, wrap.clientHeight / 4000);
          openLightbox(116, 'photo-16.jpg', _lightboxPhotoList, {
            fallbackViewportState: {zoom: needed / (6000 * fit * devicePixelRatio), centerX: 0.5, centerY: 0.5}
          });
        }""", needed,
    )
    page.wait_for_function("_lightboxCommittedId === 116 && !_lbVisualTransitionPending")
    target = "/original" if key == "original" else f"size={key}"
    visible_requests = [url for url in requested if "/116/" in url and "prefetch=1" not in url]
    assert visible_requests and target in visible_requests[0]
    if key != "original":
        assert not any("/original" in url for url in visible_requests)


@pytest.mark.parametrize("action", ["navigate", "close"])
def test_late_sharp_image_cannot_replace_newer_photo_or_reopen_viewer(live_server, page, action):
    held = []
    release = False

    def serve(route):
        if "/116/" in route.request.url and not release:
            held.append(route)
            return
        route.fulfill(body=_jpeg(6000, 4000, "#a22"), content_type="image/jpeg")

    page.route("**/photos/*/full*", lambda route: route.fulfill(body=_jpeg(), content_type="image/jpeg"))
    page.route("**/photos/*/original*", serve)
    _open_window(page, live_server)
    page.wait_for_function("Object.values(_lbAdjacentPreloads).some(e => e.photoId === 116 && e.status === 'decoded')")
    page.evaluate("_lbApplyViewportState({zoom: _lbNativeZoom, oneToOne: true, centerX: 0.4, centerY: 0.6})")
    page.wait_for_function("_lbCurrentSrcKey === 'original' && !_lbPreviewLoading")
    page.evaluate("lightboxNav(1)")
    page.wait_for_function("_lightboxCommittedId === 116 && _lbPreviewLoading")
    page.wait_for_timeout(200)
    assert held
    if action == "close":
        page.keyboard.press("Escape")
    else:
        page.evaluate("openLightbox(117, 'photo-17.jpg', _lightboxPhotoList, {fallbackViewportState: {zoom: 1}})")
        page.wait_for_function("_lightboxCommittedId === 117 && !_lbVisualTransitionPending")
    release = True
    for route in held:
        route.fulfill(body=_jpeg(6000, 4000, "#a22"), content_type="image/jpeg")
    page.wait_for_timeout(200)
    assert page.evaluate("_lbPreviewLoading") is False
    if action == "close":
        expect(page.locator("#lightboxOverlay")).not_to_have_class("lightbox-overlay active")
        assert page.evaluate("_lightboxCurrentId") is None
    else:
        assert page.evaluate("_lightboxCommittedId") == 117
        assert "/117/" in page.locator("#lightboxImg").get_attribute("src")
        assert page.evaluate("document.getElementById('lightboxImg').naturalWidth") == 1920


def test_originals_larger_than_budget_are_not_preloaded(live_server, page):
    original_requests = []

    def serve(route):
        original_requests.append(route.request.url)
        route.fulfill(body=_jpeg(9000, 6000), content_type="image/jpeg")

    page.route("**/photos/*/full*", lambda route: route.fulfill(body=_jpeg(), content_type="image/jpeg"))
    page.route("**/photos/*/original*", serve)
    _open_window(page, live_server)
    page.evaluate(
        """() => {
          _lightboxPhotoList.forEach(photo => {photo.width = 9000; photo.height = 6000;});
          _lbPrimeAdjacentPhotos('original');
        }"""
    )
    page.wait_for_function("Object.values(_lbAdjacentPreloads).filter(e => e.status === 'decoded').length === 12")
    page.wait_for_function("_lbSpeculativeLoads.size === 0")
    page.evaluate("_lbPrimeAdjacentPhotos('original')")
    page.wait_for_timeout(200)
    assert original_requests == []
    assert page.evaluate("_lbPreloadBytes()") <= 128 * 1024 * 1024


def test_busy_generation_retries_fill_window_without_parallel_producers(live_server, page):
    pending = []
    declined = []
    generated = []

    def serve(route):
        if "prefetch=1" not in route.request.url:
            route.fulfill(body=_jpeg(), content_type="image/jpeg")
        elif pending:
            declined.append(route.request.url)
            route.fulfill(status=204, body="")
        else:
            pending.append(route)
            generated.append(route.request.url)

    page.route("**/photos/*/full*", serve)
    _open_window(page, live_server)
    # Keep the server's single expensive producer busy while the other two
    # client slots discover cold previews and receive non-cacheable declines.
    page.wait_for_function("Object.keys(_lbAdjacentPreloadRetry).length === 11")
    assert len(pending) == 1
    assert len(declined) == 11
    for completed in range(1, 13):
        route = pending.pop()
        route.fulfill(body=_jpeg(), content_type="image/jpeg")
        page.wait_for_function(
            "count => Object.values(_lbAdjacentPreloads).filter(e => e.status === 'decoded').length === count",
            arg=completed,
        )
        if completed < 12:
            page.wait_for_function("_lbSpeculativeLoads.size === 1")
            # Let Playwright dispatch that request to the Python route handler.
            page.wait_for_timeout(50)
            assert len(pending) == 1
    assert len(set(generated)) == 12
    assert len(declined) == 11  # Retries run alone and no longer collide.
    assert page.evaluate("_lbPreloadBytes()") <= 128 * 1024 * 1024


@pytest.mark.parametrize("needed,tier", [(2300, "2560"), (3200, "3840")])
@pytest.mark.parametrize("original_fails", [False, True])
@pytest.mark.parametrize("mode", ["fit", "restored"])
def test_failed_initial_sized_preview_falls_back_to_usable_image(live_server, page, needed, tier, original_fails, mode):
    active = False
    failed_requests = []

    def serve(route):
        url = route.request.url
        if active and "/116/" in url and f"size={tier}" in url:
            failed_requests.append(url)
            route.fulfill(status=503, body="Preview render failed")
        elif active and "/116/original" in url and original_fails:
            route.fulfill(status=404, body="Original unavailable")
        else:
            width, height = (6000, 4000) if "/original" in url else (1920, 1280)
            route.fulfill(body=_jpeg(width, height), content_type="image/jpeg")

    page.route("**/photos/*/full*", serve)
    page.route("**/photos/*/original*", serve)
    page.route("**/photos/*/preview?*", serve)
    if mode == "fit":
        page.set_viewport_size({"width": needed + 160, "height": round(needed * 2 / 3) + 200})
    _open_window(page, live_server)
    active = True
    page.evaluate(
        """({needed, mode}) => {
          _lbClearAdjacentPreloads();
          _lbScheduleAdjacentPhoto = function() {};
          const wrap = document.getElementById('lightboxWrap');
          const fit = Math.min(1, wrap.clientWidth / 6000, wrap.clientHeight / 4000);
          const options = mode === 'restored' ? {
            fallbackViewportState: {zoom: needed / (6000 * fit * devicePixelRatio), centerX: 0.4, centerY: 0.6}
          } : {};
          openLightbox(116, 'photo-16.jpg', _lightboxPhotoList, options);
        }""", {"needed": needed, "mode": mode},
    )
    page.wait_for_function(
        """() => {
          const image = document.getElementById('lightboxImg');
          return _lightboxCommittedId === 116 && !_lbVisualTransitionPending &&
            image.complete && image.naturalWidth > 0;
        }""",
        timeout=5000,
    )
    assert failed_requests
    expect(page.locator("#lightboxFilename")).to_have_text("photo-16.jpg")
    expect(page.locator("#lightboxActions")).not_to_have_attribute("inert", "")
    source = "/full" if original_fails else "/original"
    assert source in page.locator("#lightboxImg").get_attribute("src")
    page.wait_for_timeout(400)
    # Restoring the viewport may attempt the requested tier once more, but
    # its failure must keep the usable fallback and cannot cause a retry loop.
    assert len(failed_requests) <= 2
    assert page.evaluate("document.getElementById('lightboxImg').naturalWidth") > 0


@pytest.mark.parametrize("preview_size,needed", [(1920, 1200), (960, 900), (3840, 3200), (0, 5000)])
def test_small_photo_does_not_lower_workspace_preview_limit(live_server, page, preview_size, needed):
    db = live_server["db"]
    ids = live_server["data"]["photos"]
    db.conn.execute("UPDATE photos SET width=6000, height=4000")
    db.conn.execute("UPDATE photos SET width=800, height=533 WHERE id=?", (ids[0],))
    db.conn.commit()
    db.update_workspace(db._active_workspace_id, config_overrides={"preview_max_size": preview_size})
    requested = []

    def serve(route):
        url = route.request.url
        if "prefetch=1" not in url:
            requested.append(url)
        if f"/photos/{ids[0]}/" in url:
            width, height = 800, 533
        else:
            width = preview_size or 6000
            height = round(width * 2 / 3)
        route.fulfill(body=_jpeg(width, height), content_type="image/jpeg")

    page.route("**/photos/*/full*", serve)
    page.route("**/photos/*/original*", serve)
    page.route("**/photos/*/preview?*", serve)
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()
    page.wait_for_function("_lbFullLongEdge === 800 && _lbPhotoW === 800")
    page.evaluate(
        """needed => {
          _lbClearAdjacentPreloads();
          _lbScheduleAdjacentPhoto = function() {};
          const wrap = document.getElementById('lightboxWrap');
          const fit = Math.min(1, wrap.clientWidth / 6000, wrap.clientHeight / 4000);
          const next = _lightboxPhotoList[1];
          openLightbox(next.id, next.filename, _lightboxPhotoList, {
            fallbackViewportState: {zoom: needed / (6000 * fit * devicePixelRatio), centerX: 0.5, centerY: 0.5}
          });
        }""", needed,
    )
    page.wait_for_function("id => _lightboxCommittedId === id && !_lbVisualTransitionPending", arg=ids[1])
    incoming = [url for url in requested if f"/photos/{ids[1]}/" in url]
    assert incoming and "/full" in incoming[0]


@pytest.mark.parametrize("preview_size,needed", [(3000, 2800), (5000, 4500), (0, 5000)])
def test_navigation_uses_workspace_limit_before_metadata(live_server, page, preview_size, needed):
    db = live_server["db"]
    photo_id = live_server["data"]["photos"][0]
    db.update_workspace(db._active_workspace_id, config_overrides={"preview_max_size": preview_size})
    requested = []
    held_metadata = []
    page.route(re.compile(r"/api/photos/\d+$"), lambda route: held_metadata.append(route))

    def serve(route):
        requested.append(route.request.url)
        route.fulfill(body=_jpeg(), content_type="image/jpeg")

    page.route("**/photos/*/full*", serve)
    page.route("**/photos/*/original*", serve)
    page.route("**/photos/*/preview?*", serve)
    page.set_viewport_size({"width": needed, "height": needed})
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.evaluate(
        """id => {
          _lbScheduleAdjacentPhoto = function() {};
          _lbScheduleOriginalPreload = function() {};
          openLightbox(id, 'first.jpg', [{id, filename: 'first.jpg', width: 6000, height: 4000}]);
        }""", photo_id,
    )
    page.wait_for_function("document.getElementById('lightboxImg').naturalWidth > 0")
    assert held_metadata
    assert requested and "/full" in requested[0]
    page.evaluate(
        """id => openLightbox(id, 'next.jpg', [{id, filename: 'next.jpg', width: 6000, height: 4000}])""",
        photo_id + 1,
    )
    page.wait_for_function("id => _lightboxCommittedId === id", arg=photo_id + 1)
    incoming = [url for url in requested if f"/photos/{photo_id + 1}/" in url]
    assert incoming and "/full" in incoming[0]
    for route in held_metadata:
        route.fulfill(json={
            "id": int(route.request.url.rsplit("/", 1)[1]), "width": 6000, "height": 4000,
            "full_preview_max_size": preview_size, "full_uses_original": preview_size == 0,
        })
