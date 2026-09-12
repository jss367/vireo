import base64
import io
import json
import re
import time

import pytest
from PIL import Image
from playwright.sync_api import expect

_PNG_1X1 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
    "/x8AAwMCAO+/p9sAAAAASUVORK5CYII="
)


def _png_bytes(size, color):
    buf = io.BytesIO()
    Image.new("RGB", size, color).save(buf, "PNG")
    return buf.getvalue()


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

    keywords = page.locator("#lightboxKeywords")
    expect(keywords).to_be_visible()
    expect(keywords).to_contain_text("Keywords")
    expect(keywords.locator(".lightbox-keyword")).to_have_text("Red-tailed Hawk")

    page.locator("[title='Next (→)']").click()

    expect(filename_display).not_to_have_text(first_filename)
    expect(counter).to_contain_text("2 /")
    expect(counter).to_contain_text(filename_display.text_content())
    expect(keywords).to_contain_text("None")
    expect(keywords.locator(".lightbox-keyword")).to_have_count(0)

    page.locator("[title='Previous (←)']").click()
    expect(filename_display).to_have_text(first_filename)
    expect(counter).to_contain_text("1 /")
    expect(counter).to_contain_text(first_filename)
    expect(keywords.locator(".lightbox-keyword")).to_have_text("Red-tailed Hawk")


def test_browse_lightbox_autoloads_next_page_at_navigation_boundary(
    live_server, page,
):
    """Next continues through Browse's lazy-loaded page boundary."""
    page.add_init_script(
        """
        class NoopIntersectionObserver {
          observe() {}
          unobserve() {}
          disconnect() {}
        }
        window.IntersectionObserver = NoopIntersectionObserver;
        """
    )
    page.route(
        "**/api/config",
        lambda route: route.fulfill(
            json={"photos_per_page": 2, "keyboard_shortcuts": {}}
        ),
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
    page.goto(f"{live_server['url']}/browse")

    cards = page.locator(".grid-card")
    cards.nth(1).wait_for(state="visible")
    page.wait_for_function("photos.length === 2 && totalPhotos > photos.length")
    boundary_id = int(cards.nth(1).get_attribute("data-id"))

    cards.nth(1).dblclick()
    page.wait_for_function(
        "photoId => window._lightboxCommittedId === photoId", arg=boundary_id
    )
    page.wait_for_function("photos.length > 2", timeout=5000)
    loaded_count = page.evaluate("photos.length")
    expect(page.locator("#lightboxCounter")).to_contain_text(
        f"2 / {loaded_count}"
    )
    page.evaluate("lightboxNav(1)")

    page.wait_for_function(
        "photoId => window._lightboxCurrentId !== photoId",
        arg=boundary_id,
        timeout=5000,
    )
    assert page.evaluate("window._lightboxPhotoList === photos") is True
    assert page.evaluate("window._lightboxCurrentId === photos[2].id") is True


def test_browse_lightbox_offline_filter_keeps_live_pagination_list(
    live_server, page,
):
    """The available-only lightbox list retains identity as pages load."""
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    result = page.evaluate(
        """() => {
          const originalPhotos = photos;
          const originalShowOffline = showOfflineCollectionPhotos;
          const originalList = window._lightboxPhotoList;
          const originalCurrentId = window._lightboxCurrentId;
          try {
            photos = [
              {id: 101, folder_status: 'ok'},
              {id: 102, folder_status: 'missing'}
            ];
            showOfflineCollectionPhotos = false;
            const defaultListIsLive = availableBrowsePhotos() === photos;

            showOfflineCollectionPhotos = true;
            const filteredList = availableBrowsePhotos();
            window._lightboxPhotoList = filteredList;
            window._lightboxCurrentId = 101;
            photos.push({id: 103, folder_status: 'ok'});
            syncBrowseAvailableLightboxPhotos();

            return {
              defaultListIsLive,
              filteredListKeepsIdentity: window._lightboxPhotoList === filteredList,
              filteredIds: filteredList.map(photo => photo.id),
              ownsLoadedWindow: browseLightboxOwnsLoadedWindow()
            };
          } finally {
            photos = originalPhotos;
            showOfflineCollectionPhotos = originalShowOffline;
            window._lightboxPhotoList = originalList;
            window._lightboxCurrentId = originalCurrentId;
          }
        }"""
    )

    assert result == {
        "defaultListIsLive": True,
        "filteredListKeepsIdentity": True,
        "filteredIds": [101, 103],
        "ownsLoadedWindow": True,
    }


def test_browse_lightbox_single_photo_window_waits_for_previous_page(
    live_server, page,
):
    """Previous is honored while a one-photo deep-link page is prepending."""
    db = live_server["db"]
    folder_id = live_server["data"]["folders"][0]
    existing = len(db.get_photos(folder_id=folder_id))
    for idx in range(50 - existing):
        db.add_photo(
            folder_id=folder_id,
            filename=f"newer-{idx:02d}.jpg",
            extension=".jpg",
            file_size=1000,
            file_mtime=100 + idx,
            timestamp=f"2025-01-{(idx % 28) + 1:02d}T12:00:00",
        )
    target_id = db.add_photo(
        folder_id=folder_id,
        filename="newest-target.jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=1,
        timestamp="2099-01-01T00:00:00",
    )

    page.add_init_script(
        """
        class NoopIntersectionObserver {
          observe() {}
          unobserve() {}
          disconnect() {}
        }
        window.IntersectionObserver = NoopIntersectionObserver;
        """
    )
    page.route(
        "**/api/config",
        lambda route: route.fulfill(
            json={"photos_per_page": 50, "keyboard_shortcuts": {}}
        ),
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
    page.goto(f"{live_server['url']}/browse?photo_id={target_id}")
    page.wait_for_function("photos.length === 1 && earliestPage === 2")
    page.evaluate(
        """() => {
          const originalSafeFetch = window.safeFetch;
          window.safeFetch = async function(url, options, behavior) {
            const body = options && options.body ? JSON.parse(options.body) : null;
            if (url === '/api/photos/query' && body && body.page === 1) {
              const response = originalSafeFetch.call(this, url, options, behavior);
              await new Promise(resolve => { window.__releasePreviousPage = resolve; });
              return response;
            }
            return originalSafeFetch.call(this, url, options, behavior);
          };
        }"""
    )

    page.locator(f'.grid-card[data-id="{target_id}"]').dblclick()
    page.wait_for_function(
        "photoId => window._lightboxCommittedId === photoId", arg=target_id
    )
    page.wait_for_function("loading === true && !!window.__releasePreviousPage")
    page.evaluate("lightboxNav(-1)")
    page.evaluate("window.__releasePreviousPage()")

    page.wait_for_function(
        "photoId => photos.length === 51 && window._lightboxCurrentId !== photoId",
        arg=target_id,
        timeout=5000,
    )
    assert page.evaluate("window._lightboxPhotoList === photos") is True
    assert page.evaluate("window._lightboxCurrentId === photos[49].id") is True


def test_browse_lightbox_delete_preserves_live_pagination_list(live_server, page):
    """Deleting a loaded photo keeps Browse and the lightbox on one array."""
    page.add_init_script(
        """
        class NoopIntersectionObserver {
          observe() {}
          unobserve() {}
          disconnect() {}
        }
        window.IntersectionObserver = NoopIntersectionObserver;
        """
    )
    page.route(
        "**/api/config",
        lambda route: route.fulfill(
            json={"photos_per_page": 2, "keyboard_shortcuts": {}}
        ),
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function("photos.length === 2 && totalPhotos > photos.length")
    deleted_id, remaining_id = page.evaluate("[photos[0].id, photos[1].id]")
    page.evaluate("allLoaded = true")

    page.locator(f'.grid-card[data-id="{deleted_id}"]').dblclick()
    page.wait_for_function(
        "photoId => window._lightboxCommittedId === photoId", arg=deleted_id
    )
    page.evaluate(
        """() => {
          window.lightboxDelete();
          const callback = _deleteCallback;
          hideDeleteModal();
          callback({deleted: 1, failed_photo_ids: []});
        }"""
    )
    page.wait_for_function(
        "photoId => window._lightboxCommittedId === photoId", arg=remaining_id
    )
    assert page.evaluate("window._lightboxPhotoList === photos") is True
    assert page.evaluate("photos.length") == 1

    page.evaluate(
        """() => {
          allLoaded = false;
          document.dispatchEvent(new CustomEvent('lightbox:photochanged', {
            detail: {photoId: window._lightboxCurrentId}
          }));
        }"""
    )
    page.wait_for_function("photos.length > 1", timeout=5000)
    assert page.evaluate("window._lightboxPhotoList === photos") is True


def test_browse_lightbox_stale_boundary_retry_does_not_advance_reopened_session(
    live_server, page,
):
    """A pending page load cannot navigate a newly reopened lightbox."""
    page.add_init_script(
        """
        class NoopIntersectionObserver {
          observe() {}
          unobserve() {}
          disconnect() {}
        }
        window.IntersectionObserver = NoopIntersectionObserver;
        """
    )
    page.route(
        "**/api/config",
        lambda route: route.fulfill(
            json={"photos_per_page": 2, "keyboard_shortcuts": {}}
        ),
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function("photos.length === 2 && totalPhotos > photos.length")
    boundary_id = page.evaluate("photos[1].id")
    page.evaluate(
        """() => {
          const originalSafeFetch = window.safeFetch;
          window.safeFetch = async function(url, options, behavior) {
            const body = options && options.body ? JSON.parse(options.body) : null;
            if (url === '/api/photos/query' && body && body.page === 2) {
              const response = originalSafeFetch.call(this, url, options, behavior);
              await new Promise(resolve => { window.__releaseNextPage = resolve; });
              return response;
            }
            return originalSafeFetch.call(this, url, options, behavior);
          };
        }"""
    )

    page.locator(f'.grid-card[data-id="{boundary_id}"]').dblclick()
    page.wait_for_function(
        "photoId => window._lightboxCommittedId === photoId", arg=boundary_id
    )
    page.wait_for_function("loading === true && !!window.__releaseNextPage")
    page.evaluate("lightboxNav(1)")
    original_session = page.evaluate("browseLightboxSession")

    page.evaluate("closeLightbox()")
    page.evaluate(
        """photoId => {
          const photo = photos.find(item => item.id === photoId);
          openLightbox(photo.id, photo.filename || '', photos);
        }""",
        boundary_id,
    )
    page.wait_for_function(
        "photoId => window._lightboxCommittedId === photoId", arg=boundary_id
    )
    assert page.evaluate("browseLightboxSession") == original_session + 1

    page.evaluate("window.__releaseNextPage()")
    page.wait_for_function("loading === false && photos.length > 2", timeout=5000)
    page.wait_for_timeout(100)
    assert page.evaluate("window._lightboxCurrentId") == boundary_id


def test_browse_lightbox_close_selects_current_photo(live_server, page):
    """Closing after lightbox navigation focuses the last-viewed grid photo."""
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
    page.goto(f"{live_server['url']}/browse")

    cards = page.locator(".grid-card")
    cards.nth(1).wait_for(state="visible")
    second_id = int(cards.nth(1).get_attribute("data-id"))

    cards.first.dblclick()
    expect(page.locator("#lightboxOverlay")).to_have_class(
        "lightbox-overlay active"
    )
    page.locator("#lightboxNext").click()
    # Wait for the visible (committed) identity to advance, not just the
    # navigation target. `_lightboxCurrentId` flips as soon as Next is
    # pressed, while the outgoing bitmap is held on screen until the
    # incoming /full decodes; if we close on the target-only signal, a slow
    # decode races the close and returns Browse to photo 1 under the new
    # committed-identity close semantics — Codex P2 on PR #1486.
    page.wait_for_function(
        "photoId => window._lightboxCommittedId === photoId", arg=second_id
    )

    page.locator(".lightbox-close").click()

    expect(page.locator("#lightboxOverlay")).not_to_have_class(
        "lightbox-overlay active"
    )
    assert page.evaluate("selectedPhotoId") == second_id
    assert page.evaluate("selectedIndex") == 1
    assert page.evaluate("selectedPhotos.size") == 0
    expect(cards.nth(1)).to_have_class(re.compile(r"\bselected\b"))
    expect(cards.first).not_to_have_class(re.compile(r"\bselected\b"))


def test_browse_lightbox_close_during_navigation_returns_to_visible_photo(
    live_server, page,
):
    """Closing mid-navigation focuses the still-visible photo, not the loader.

    `_lightboxCurrentId` advances immediately at the start of navigation while
    the outgoing bitmap is deliberately held on screen until the incoming
    /full decodes. Capturing that internal target on close made Browse
    select and scroll to a photo the user never actually saw — Codex P2 on
    PR #1486. Track the last committed `lightbox:photochanged` identity.
    """
    ids = {"first": None, "second": None}

    def route_full(route):
        url = route.request.url
        if ids["second"] is not None and f"/photos/{ids['second']}/full" in url:
            # Park the incoming photo; the outgoing bitmap stays on screen.
            return
        route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        )

    page.route("**/photos/*/full", route_full)
    page.goto(f"{live_server['url']}/browse")

    cards = page.locator(".grid-card")
    cards.nth(1).wait_for(state="visible")
    ids["first"] = int(cards.first.get_attribute("data-id"))
    ids["second"] = int(cards.nth(1).get_attribute("data-id"))

    cards.first.dblclick()
    expect(page.locator("#lightboxOverlay")).to_have_class(
        "lightbox-overlay active"
    )
    page.wait_for_function(
        "photoId => window._lightboxCommittedId === photoId", arg=ids["first"]
    )

    page.locator("#lightboxNext").click()
    page.wait_for_function(
        "photoId => window._lightboxCurrentId === photoId"
        " && window._lbVisualTransitionPending === true",
        arg=ids["second"],
    )
    # Incoming photo has not decoded; committed identity stays on photo 1.
    assert page.evaluate("window._lightboxCommittedId") == ids["first"]

    page.locator(".lightbox-close").click()

    expect(page.locator("#lightboxOverlay")).not_to_have_class(
        "lightbox-overlay active"
    )
    assert page.evaluate("selectedPhotoId") == ids["first"]
    assert page.evaluate("selectedIndex") == 0
    assert page.evaluate("selectedPhotos.size") == 0
    expect(cards.first).to_have_class(re.compile(r"\bselected\b"))
    expect(cards.nth(1)).not_to_have_class(re.compile(r"\bselected\b"))


def test_browse_lightbox_delete_only_photo_does_not_reselect_it(
    live_server, page,
):
    """Deleting the only lightbox photo must not re-select the deleted row.

    `lightboxDelete` used to close the lightbox before removing the row from
    `photos`, so Browse's `lightbox:closed` reconciliation would still find
    the deleted photo, call `selectPhoto`/`loadDetail` on it, and then have
    its own delete cleanup null `selectedPhotoId` — leaving the sidebar
    blank or showing stale details. Codex P2 on PR #1486. Grid state must
    be updated before the lightbox transitions.
    """
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
    page.goto(f"{live_server['url']}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    first_id = int(first_card.get_attribute("data-id"))
    first_filename = first_card.get_attribute("data-filename")
    initial_count = page.evaluate("window.photos.length")

    # Open the lightbox with a single-photo list so lightboxDelete takes the
    # closeLightbox branch (rather than opening the next photo).
    page.evaluate(
        """([id, filename]) => openLightbox(id, filename, [{id: id, filename: filename}])""",
        [first_id, first_filename],
    )
    expect(page.locator("#lightboxOverlay")).to_have_class(
        "lightbox-overlay active"
    )
    page.wait_for_function(
        "photoId => window._lightboxCommittedId === photoId", arg=first_id
    )

    observed = page.evaluate(
        """() => {
            const events = [];
            document.addEventListener('lightbox:closed', event => {
                events.push({
                    photoId: event.detail && event.detail.photoId,
                    photosStillHasDeleted:
                        typeof photos !== 'undefined' &&
                        photos.some(p => p.id === event.detail.photoId),
                    selectedPhotoIdAtClose: selectedPhotoId,
                });
            }, { once: true });
            window.lightboxDelete();
            const callback = _deleteCallback;
            hideDeleteModal();
            callback({deleted: 1, failed_photo_ids: []});
            return events[0];
        }"""
    )

    expect(page.locator("#lightboxOverlay")).not_to_have_class(
        "lightbox-overlay active"
    )
    assert observed["photoId"] == first_id
    # When lightbox:closed fires, `photos` must already have the deleted
    # entry filtered out; otherwise the reconciliation re-selects it.
    assert observed["photosStillHasDeleted"] is False
    assert page.evaluate("selectedPhotoId") is None
    assert page.evaluate("window.photos.length") == initial_count - 1


def test_browse_lightbox_close_skips_reload_when_photo_unchanged(
    live_server, page,
):
    """Closing without navigating must not refetch the already-selected photo.

    The `lightbox:closed` reconciliation used to call `selectPhoto` for the
    returned photo unconditionally, which runs `loadDetail` and rerenders
    the detail panel — silently discarding an unsubmitted `#locationInput`
    draft (blur only hides suggestions; it does not save). Codex P2 on PR
    #1486. Skip the reload when `selectedPhotoId` already matches.
    """
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
    page.goto(f"{live_server['url']}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    first_id = int(first_card.get_attribute("data-id"))

    # Focus the first photo via a normal click, which runs `loadDetail` once.
    first_card.click()
    page.wait_for_function(
        "photoId => selectedPhotoId === photoId", arg=first_id
    )

    observed = page.evaluate(
        """([id, filename]) => {
            const calls = [];
            const original = window.loadDetail;
            window.loadDetail = function(photoId) {
                calls.push(photoId);
                return original.apply(this, arguments);
            };
            try {
                openLightbox(id, filename, [{id: id, filename: filename}]);
            } catch (err) {
                window.loadDetail = original;
                throw err;
            }
            // Committed identity is set synchronously inside openLightbox once
            // the image commits; capture whatever is committed at close time.
            closeLightbox();
            window.loadDetail = original;
            return { calls, selectedPhotoId, selectedIndex };
        }""",
        [first_id, first_card.get_attribute("data-filename")],
    )

    expect(page.locator("#lightboxOverlay")).not_to_have_class(
        "lightbox-overlay active"
    )
    assert observed["selectedPhotoId"] == first_id
    assert observed["selectedIndex"] == 0
    # The pre-existing focus must survive the close: no fresh loadDetail on
    # `lightbox:closed` for the already-selected photo.
    assert observed["calls"] == []


def test_lightbox_track_eye_keeps_eye_at_same_screen_position(
    live_server, page, tmp_path,
):
    """Track Eye offsets the next frame so burst inspection stays registered."""
    db = live_server["db"]
    folder_path = tmp_path / "eye-track"
    folder_path.mkdir()
    folder_id = db.add_folder(str(folder_path), name="eye-track")
    photo_ids = []
    for index, eye in enumerate(((0.25, 0.40), (0.75, 0.65)), start=1):
        filename = f"eye-{index}.png"
        Image.new("RGB", (1200, 800), (30 * index, 80, 120)).save(
            folder_path / filename
        )
        photo_id = db.add_photo(
            folder_id=folder_id,
            filename=filename,
            extension=".png",
            file_size=(folder_path / filename).stat().st_size,
            file_mtime=(folder_path / filename).stat().st_mtime,
            width=1200,
            height=800,
        )
        db.conn.execute(
            "UPDATE photos SET eye_x=?, eye_y=?, eye_conf=? WHERE id=?",
            (eye[0], eye[1], 0.98, photo_id),
        )
        photo_ids.append(photo_id)
    db.conn.commit()

    page.goto(f"{live_server['url']}/browse")
    page.evaluate("localStorage.removeItem('vireo.lb.trackEye')")
    page.evaluate(
        """photos => openLightbox(photos[0].id, photos[0].filename, photos)""",
        [
            {"id": photo_ids[0], "filename": "eye-1.png"},
            {"id": photo_ids[1], "filename": "eye-2.png"},
        ],
    )
    page.wait_for_function(
        """photoId => {
            const img = document.getElementById('lightboxImg');
            return window._lbPhotoDataByPhoto[String(photoId)] &&
                img.complete && img.naturalWidth > 0 &&
                !window._lbVisualTransitionPending;
        }""",
        arg=photo_ids[0],
    )
    page.evaluate(
        """() => window._lbApplyViewportState({
            zoom: 2,
            centerX: 0.40,
            centerY: 0.50,
            oneToOne: false,
            pending1To1: false,
        })"""
    )
    page.locator("#lightboxViewBtn").click()
    page.locator("#lightboxTrackEye").click()
    expect(page.locator("#lightboxTrackEye")).to_be_checked()
    page.locator("#lightboxViewBtn").click()
    assert page.evaluate("localStorage.getItem('vireo.lb.trackEye')") == "1"

    eye_screen_js = """() => {
        const data = window._lbPhotoDataByPhoto[String(window._lightboxCurrentId)];
        const metrics = window._lbUpdateLayoutMetrics();
        const state = window._lbViewportStateFromCurrent();
        const wrap = document.getElementById('lightboxWrap').getBoundingClientRect();
        return {
            x: wrap.left + wrap.width / 2 +
                (Number(data.eye_x) - state.centerX) * metrics.w * metrics.scale,
            y: wrap.top + wrap.height / 2 +
                (Number(data.eye_y) - state.centerY) * metrics.h * metrics.scale,
        };
    }"""
    before = page.evaluate(eye_screen_js)

    page.locator("#lightboxNext").click()
    page.wait_for_function(
        """photoId => window._lightboxCurrentId === photoId &&
            window._lbPhotoDataByPhoto[String(photoId)] &&
            !window._lbVisualTransitionPending &&
            window._lbPendingEyeTrack === null""",
        arg=photo_ids[1],
    )
    after = page.evaluate(eye_screen_js)

    assert abs(after["x"] - before["x"]) < 2
    assert abs(after["y"] - before["y"]) < 2


def test_user_pan_zoom_cancels_pending_eye_track(live_server, page):
    """Manual pan/zoom before the metadata callback lands must drop the
    armed eye alignment, otherwise _lbTryApplyPendingEyeTrack later stomps
    the user's viewport with the previous photo's eye anchor."""
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    state = page.evaluate(
        """() => {
            window._lbPendingViewportState = {
                zoom: 2, centerX: 0.5, centerY: 0.5,
                oneToOne: false, pending1To1: false,
            };
            window._lbPendingEyeTrack = {
                photoId: 'stale', offsetX: 42, offsetY: 42,
            };
            window._lbClearPendingViewportRestore();
            return {
                viewport: window._lbPendingViewportState,
                eyeTrack: window._lbPendingEyeTrack,
            };
        }"""
    )

    assert state == {"viewport": None, "eyeTrack": None}


def test_browse_lightbox_same_photo_reopen_does_not_lock_controls(live_server, page):
    """Reopening the visible photo must not wait for a same-src load event."""
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
    page.goto(f"{live_server['url']}/browse")

    page.locator(".grid-card").first.dblclick()
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth > 0;
        }"""
    )

    state = page.evaluate(
        """() => {
            const current = window._lightboxPhotoList.find(
                photo => photo.id === window._lightboxCurrentId
            );
            window.openLightbox(
                current.id,
                current.filename,
                window._lightboxPhotoList
            );
            return {
                pending: window._lbVisualTransitionPending,
                actionsInert: document.getElementById('lightboxActions').inert,
                adjustInert: document.getElementById('lightboxAdjustPanel').inert,
            };
        }"""
    )

    assert state == {
        "pending": False,
        "actionsInert": False,
        "adjustInert": False,
    }


def test_paired_source_switch_commits_after_load_and_uses_jpeg_dimensions(
    live_server, page, tmp_path,
):
    """Pair labels follow displayed pixels and failed switches preserve state."""
    db = live_server["db"]
    folder_path = tmp_path / "paired"
    folder_path.mkdir()
    folder_id = db.add_folder(str(folder_path), name="paired")
    photo_id = db.add_photo(
        folder_id=folder_id,
        filename="developed.nef",
        extension=".nef",
        file_size=1000,
        file_mtime=1.0,
        width=4000,
        height=3000,
    )
    db.conn.execute(
        "UPDATE photos SET companion_path='developed.jpg' WHERE id=?",
        (photo_id,),
    )
    db.save_detections(photo_id, [{
        "box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4},
        "confidence": 0.95,
        "category": "animal",
    }], detector_model="test-detector")
    db.conn.commit()

    jpeg = _png_bytes((200, 100), "green")
    raw = _png_bytes((100, 200), "red")
    fail_raw = {"enabled": False, "attempts": 0}
    hold_raw = {"enabled": False, "routes": []}

    def serve_pair(route):
        wants_raw = "source=raw" in route.request.url
        if wants_raw and hold_raw["enabled"]:
            hold_raw["routes"].append(route)
            return
        if wants_raw and fail_raw["enabled"]:
            fail_raw["attempts"] += 1
            route.abort()
            return
        body = raw if wants_raw else jpeg
        route.fulfill(body=body, content_type="image/png")

    pair_url = re.compile(
        rf"/(thumbnails/{photo_id}\.jpg|photos/{photo_id}/(full|original|preview))"
    )
    page.route(pair_url, serve_pair)
    page.goto(f"{live_server['url']}/browse?photo_id={photo_id}")

    card = page.locator(f'.grid-card[data-id="{photo_id}"]')
    expect(card).to_be_visible()
    jpeg_card_html = page.evaluate(
        """photoId => {
            const photo = Object.assign({}, window.photos.find(p => p.id === photoId), {
                detections: [{x: 0.1, y: 0.2, w: 0.3, h: 0.4, confidence: 0.95}]
            });
            const previous = window.showDetectionBoxes;
            window.showDetectionBoxes = true;
            const html = window.renderPhotoCard(photo, 0);
            window.showDetectionBoxes = previous;
            return html;
        }""",
        photo_id,
    )
    assert 'style="display:none;left:' in jpeg_card_html
    card.dblclick()

    control = page.locator("#lightboxSourceControl")
    image = page.locator("#lightboxImg")
    expect(control).to_be_visible()
    expect(control).to_have_text("Viewing JPEG · Show RAW")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.naturalWidth === 200 && img.naturalHeight === 100;
        }"""
    )
    assert image.evaluate("img => [img.naturalWidth, img.naturalHeight]") == [200, 100]
    assert page.evaluate(
        """() => [
            document.getElementById('lightboxTransform').style.width,
            document.getElementById('lightboxTransform').style.height
        ]"""
    ) == ["200px", "100px"]

    control.click()
    expect(control).to_have_text("Viewing RAW · Show JPEG")
    assert image.evaluate("img => [img.naturalWidth, img.naturalHeight]") == [100, 200]
    expect(page.locator("#lightboxDetections .lb-detection-box")).to_have_count(1)
    expect(card.locator(".pair-source-badge")).to_have_text("RAW · JPEG pair")
    rebuilt = page.evaluate(
        """photoId => {
            const photo = Object.assign({}, window.photos.find(p => p.id === photoId), {
                detections: [{x: 0.1, y: 0.2, w: 0.3, h: 0.4, confidence: 0.95}]
            });
            const previous = window.showDetectionBoxes;
            window.showDetectionBoxes = true;
            const html = window.renderPhotoCard(photo, 0);
            window.showDetectionBoxes = previous;
            return html;
        }""",
        photo_id,
    )
    assert "RAW · JPEG pair" in rebuilt
    assert 'class="det-box"' in rebuilt
    assert 'style="display:none;left:' not in rebuilt

    control.click()
    expect(control).to_have_text("Viewing JPEG · Show RAW")
    fail_raw["enabled"] = True
    attempts_before = fail_raw["attempts"]
    control.click()
    for _ in range(50):
        if fail_raw["attempts"] > attempts_before:
            break
        page.wait_for_timeout(100)
    assert fail_raw["attempts"] > attempts_before, (
        "Expected the RAW source request to be attempted so the abort "
        "path is exercised before asserting the state is unchanged."
    )
    expect(control).to_have_text("Viewing JPEG · Show RAW")
    assert image.evaluate("img => [img.naturalWidth, img.naturalHeight]") == [200, 100]

    fail_raw["enabled"] = False
    hold_raw["enabled"] = True
    control.click()
    expect(control).to_contain_text("Loading RAW")
    for _ in range(50):
        if hold_raw["routes"]:
            break
        page.wait_for_timeout(20)
    assert hold_raw["routes"], "expected the RAW probe request to be held"
    page.evaluate(
        """dataUrl => {
            window._lightboxCurrentId = -1;
            document.getElementById('lightboxImg').src = dataUrl;
        }""",
        "data:image/png;base64," + _PNG_1X1,
    )
    page.wait_for_function(
        """() => document.getElementById('lightboxImg').naturalWidth === 1"""
    )
    hold_raw["routes"].pop(0).fulfill(body=raw, content_type="image/png")
    page.wait_for_timeout(100)
    assert image.get_attribute("src").startswith("data:image/png;base64,")
    assert page.evaluate(
        """photoId => (
            window._vireoPairSource(photoId) === 'jpeg' &&
            !window._vireoPairPendingSourceByPhoto[String(photoId)]
        )""",
        photo_id,
    )


def test_non_raw_jpeg_companions_do_not_enable_pair_controls(
    live_server, page, tmp_path,
):
    """Sidecars and reverse pair records must not become RAW/JPEG switches."""
    db = live_server["db"]
    folder_path = tmp_path / "non-pair"
    folder_path.mkdir()
    folder_id = db.add_folder(str(folder_path), name="non-pair")
    photo_id = db.add_photo(
        folder_id=folder_id,
        filename="developed.jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=1.0,
        width=200,
        height=100,
    )
    db.conn.execute(
        "UPDATE photos SET companion_path='developed.nef' WHERE id=?",
        (photo_id,),
    )
    db.conn.commit()

    image_requests = []

    def serve_image(route):
        image_requests.append(route.request.url)
        route.fulfill(body=_png_bytes((200, 100), "green"), content_type="image/png")

    page.route(
        re.compile(
            rf"/(thumbnails/{photo_id}\.jpg|photos/{photo_id}/(full|original|preview))"
        ),
        serve_image,
    )
    page.goto(f"{live_server['url']}/browse?photo_id={photo_id}")

    card = page.locator(f'.grid-card[data-id="{photo_id}"]')
    expect(card).to_be_visible()
    expect(card.locator(".pair-source-badge")).to_have_count(0)
    assert page.evaluate(
        """() => [
            window.vireoPhotoIsRawJpegPair({
                filename: 'developed.jpg', extension: '.jpg',
                companion_path: 'developed.nef'
            }),
            window.vireoPhotoIsRawJpegPair({
                filename: 'capture.nef', extension: '.nef',
                companion_path: 'capture.xmp'
            }),
            window.vireoPhotoIsRawJpegPair({
                filename: 'capture.NEF', companion_path: 'capture.JPEG'
            })
        ]"""
    ) == [False, False, True]

    card.dblclick()
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    expect(page.locator("#lightboxSourceControl")).to_be_hidden()
    assert page.evaluate("photoId => window._vireoPairSource(photoId)", photo_id) is None
    assert image_requests
    assert all("source=" not in request_url for request_url in image_requests)


def test_browse_lightbox_filename_can_be_selected_without_resetting_zoom(
    live_server, page
):
    """Selecting the filename must not bubble into the lightbox zoom/close handlers."""
    page.goto(f"{live_server['url']}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    first_filename = first_card.get_attribute("data-filename")
    first_card.dblclick()

    overlay = page.locator("#lightboxOverlay")
    filename_display = page.locator("#lightboxFilename")
    expect(overlay).to_have_class("lightbox-overlay active")
    expect(filename_display).to_have_text(first_filename)

    page.evaluate(
        """() => {
            window._lbNativeZoom = 2;
            window._lbSetZoom(2, null, null);
        }"""
    )

    # A click is part of both double-click and drag-to-select interactions. It
    # previously reached closeLightbox(), which reset _lbZoom to fit.
    filename_display.click()

    expect(overlay).to_have_class("lightbox-overlay active")
    assert page.evaluate("window._lbZoom") == 2
    assert filename_display.evaluate(
        "el => getComputedStyle(el).userSelect"
    ) == "text"
    assert filename_display.evaluate(
        "el => getComputedStyle(el).cursor"
    ) == "text"
    assert page.evaluate(
        """() => (
            Number(getComputedStyle(document.getElementById('lightboxFlagStatus')).zIndex) >
            Number(getComputedStyle(document.querySelector('.lightbox-bottom-bar')).zIndex)
        )"""
    ) is True


def test_browse_lightbox_zoom_hud_controls_logarithmic_zoom(live_server, page):
    """The compact zoom HUD exposes fit, 1:1, steps, and a logarithmic slider."""
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="1200" '
        'viewBox="0 0 1600 1200"><rect width="1600" height="1200" fill="#274"/></svg>'
    )
    page.route(
        re.compile(r"/photos/\d+/(full|original|preview)"),
        lambda route: route.fulfill(body=svg, content_type="image/svg+xml"),
    )
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()

    overlay = page.locator("#lightboxOverlay")
    badge = page.locator("#lightboxZoomBadge")
    popover = page.locator("#lightboxZoomPopover")
    slider = page.locator("#lightboxZoomSlider")
    expect(overlay).to_have_class("lightbox-overlay active")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth === 1600 &&
                !window._lbVisualTransitionPending;
        }"""
    )

    # Make the scale deterministic: max zoom is four times native, so a
    # logarithmic slider midpoint maps sqrt(16) to the native zoom of 4.
    page.evaluate(
        """() => {
            window._lbCancelOriginalPreload();
            window._lbScheduleSourceSwap = function() {};
            window._lbRecomputeNativeZoom = function() {};
            window._lbNativeZoom = 4;
            window._lbSetZoom(1, null, null);
        }"""
    )
    expect(badge).to_be_visible()
    expect(badge).to_have_text("Fit")
    expect(badge).to_have_attribute("aria-expanded", "false")

    badge.click()
    expect(popover).to_have_class("lightbox-zoom-popover open")
    expect(badge).to_have_attribute("aria-expanded", "true")
    expect(slider).to_have_attribute("aria-valuetext", "Fit")
    expect(page.locator("#lightboxZoomNativeStop")).to_be_visible()

    # Photo navigation freezes the outgoing frame until the incoming image is
    # ready. Every zoom input, including the labelled stops, must advertise
    # that temporarily inert state instead of appearing clickable.
    page.evaluate(
        """() => {
            window._lbVisualTransitionPending = true;
            window._lbUpdateZoomControl();
        }"""
    )
    expect(page.locator(".lb-zoom-stop-fit")).to_be_disabled()
    expect(page.locator("#lightboxZoomNativeStop")).to_be_disabled()
    page.evaluate(
        """() => {
            window._lbVisualTransitionPending = false;
            window._lbUpdateZoomControl();
        }"""
    )
    expect(page.locator(".lb-zoom-stop-fit")).to_be_enabled()
    expect(page.locator("#lightboxZoomNativeStop")).to_be_enabled()

    slider.evaluate(
        """el => {
            el.value = '500';
            el.dispatchEvent(new Event('input', {bubbles: true}));
        }"""
    )
    assert abs(page.evaluate("window._lbZoom") - 4) < 0.01
    expect(badge).to_have_text("100%")
    expect(slider).to_have_attribute("aria-valuetext", "100%")

    page.locator("#lightboxZoomIn").click()
    assert abs(page.evaluate("window._lbZoom") - 5) < 0.01
    expect(badge).to_have_text("125%")

    page.locator(".lb-zoom-stop-fit").click()
    assert abs(page.evaluate("window._lbZoom") - 1) < 0.01
    expect(badge).to_have_text("Fit")

    # The labelled 1:1 stop uses the guarded high-resolution path rather than
    # merely enlarging a softer tier. Mark that tier current for this UI test.
    page.evaluate(
        """() => {
            window._lbNativeZoom = 4;
            window._lbCurrentSrcKey = window._lbPickSourceKey(window._lbNativeZoom);
            window._lbSetZoom(2, null, null);
            document.getElementById('lightboxZoomNativeStop').click();
        }"""
    )
    assert abs(page.evaluate("window._lbZoom") - 4) < 0.01
    expect(badge).to_have_text("100%")
    expect(overlay).to_have_class("lightbox-overlay active")

    page.evaluate("window.closeLightbox()")
    expect(popover).to_have_class("lightbox-zoom-popover")
    expect(badge).to_have_attribute("aria-expanded", "false")


def test_browse_lightbox_zoom_hud_keeps_near_fit_native_stop_separate(
    live_server, page
):
    """A near-fit native zoom keeps separate exact Fit and 1:1 actions."""
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="1200" '
        'viewBox="0 0 1600 1200"><rect width="1600" height="1200" fill="#274"/></svg>'
    )
    page.route(
        re.compile(r"/photos/\d+/(full|original|preview)"),
        lambda route: route.fulfill(body=svg, content_type="image/svg+xml"),
    )
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()

    overlay = page.locator("#lightboxOverlay")
    expect(overlay).to_have_class("lightbox-overlay active")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth === 1600 &&
                !window._lbVisualTransitionPending;
        }"""
    )

    # Put native zoom just above fit but below the track position where its 1:1
    # label would avoid overlapping Fit. The label is visually offset to 8%,
    # while each action retains its exact zoom target.
    page.evaluate(
        """() => {
            window._lbCancelOriginalPreload();
            window._lbScheduleSourceSwap = function() {};
            window._lbRecomputeNativeZoom = function() {};
            window._lbNativeZoom = 1.05;
            window._lbCurrentSrcKey = window._lbPickSourceKey(window._lbNativeZoom);
            window._lbSetZoom(1, null, null);
        }"""
    )
    page.locator("#lightboxZoomBadge").click()
    fit_stop = page.locator(".lb-zoom-stop-fit")
    native_stop = page.locator("#lightboxZoomNativeStop")
    expect(fit_stop).to_have_text("Fit")
    expect(native_stop).to_be_visible()
    assert native_stop.evaluate("el => el.style.left") == "8%"

    page.evaluate("() => window._lbSetZoom(1.05, null, null)")
    fit_stop.click()
    assert abs(page.evaluate("window._lbZoom") - 1.0) < 0.001
    expect(page.locator("#lightboxZoomBadge")).to_have_text("Fit")

    native_stop.click()
    assert abs(page.evaluate("window._lbZoom") - 1.05) < 0.01
    expect(page.locator("#lightboxZoomBadge")).to_have_text("100%")


def test_browse_lightbox_zoom_toggle_returns_to_fit_near_native(live_server, page):
    """The `z` toggle must return to exact fit from a nearby native zoom."""
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="1200" '
        'viewBox="0 0 1600 1200"><rect width="1600" height="1200" fill="#274"/></svg>'
    )
    page.route(
        re.compile(r"/photos/\d+/(full|original|preview)"),
        lambda route: route.fulfill(body=svg, content_type="image/svg+xml"),
    )
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()

    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth === 1600 &&
                !window._lbVisualTransitionPending;
        }"""
    )

    # A small gap between fit and native must not keep the toggle from returning
    # to exact fit.
    zoomed = page.evaluate(
        """() => {
            window._lbCancelOriginalPreload();
            window._lbScheduleSourceSwap = function() {};
            window._lbNativeZoom = 1.05;
            window._lbCurrentSrcKey = window._lbPickSourceKey(window._lbNativeZoom);
            window._lbSetZoom(window._lbNativeZoom, null, null);
            return window._lbZoom;
        }"""
    )
    assert abs(zoomed - 1.05) < 0.01

    page.evaluate("() => window.toggleLightboxZoom()")
    fit_state = page.evaluate(
        """() => ({
            zoom: window._lbZoom,
            pending: window._lbPending1To1,
        })"""
    )
    assert abs(fit_state["zoom"] - 1.0) < 0.001
    assert fit_state["pending"] is False

    # A pending 1:1 upgrade in the same range must also be cancellable via z.
    page.evaluate(
        """() => {
            window._lbPending1To1 = true;
            window._lbPending1To1Anchor = { x: 0, y: 0 };
        }"""
    )
    page.evaluate("() => window.toggleLightboxZoom()")
    cancelled = page.evaluate(
        """() => ({
            zoom: window._lbZoom,
            pending: window._lbPending1To1,
        })"""
    )
    assert abs(cancelled["zoom"] - 1.0) < 0.001
    assert cancelled["pending"] is False


def test_browse_lightbox_hide_chrome_also_hides_zoom_popover(live_server, page):
    """Hide UI must close the zoom popover and hide its badge with the rest of the chrome."""
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="1200" '
        'viewBox="0 0 1600 1200"><rect width="1600" height="1200" fill="#274"/></svg>'
    )
    page.route(
        re.compile(r"/photos/\d+/(full|original|preview)"),
        lambda route: route.fulfill(body=svg, content_type="image/svg+xml"),
    )
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()

    overlay = page.locator("#lightboxOverlay")
    badge = page.locator("#lightboxZoomBadge")
    popover = page.locator("#lightboxZoomPopover")
    control = page.locator("#lightboxZoomControl")
    expect(overlay).to_have_class("lightbox-overlay active")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth === 1600 &&
                !window._lbVisualTransitionPending;
        }"""
    )

    badge.click()
    expect(popover).to_have_class("lightbox-zoom-popover open")
    expect(badge).to_have_attribute("aria-expanded", "true")
    expect(control).to_be_visible()

    page.evaluate("() => window.toggleLightboxChrome()")
    expect(overlay).to_have_class("lightbox-overlay active lb-hide-chrome")
    # The persisted "Lightbox controls: Off" state must not leave the zoom
    # popover or its badge exposed over the image.
    expect(popover).to_have_class("lightbox-zoom-popover")
    expect(badge).to_have_attribute("aria-expanded", "false")
    expect(control).not_to_be_visible()

    page.evaluate("() => window.toggleLightboxChrome()")
    expect(overlay).to_have_class("lightbox-overlay active")
    expect(control).to_be_visible()
    # Chrome coming back must not silently reopen the popover.
    expect(popover).to_have_class("lightbox-zoom-popover")
    expect(badge).to_have_attribute("aria-expanded", "false")


def test_browse_lightbox_one_to_one_reuses_sharper_current_source(live_server, page):
    """1:1 must apply synchronously when the current source is already sharp enough."""
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="1200" '
        'viewBox="0 0 1600 1200"><rect width="1600" height="1200" fill="#274"/></svg>'
    )
    page.route(
        re.compile(r"/photos/\d+/(full|original|preview)"),
        lambda route: route.fulfill(body=svg, content_type="image/svg+xml"),
    )
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()

    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth === 1600 &&
                !window._lbVisualTransitionPending;
        }"""
    )

    # Simulate the case Codex flagged: /original is already loaded (e.g. from a
    # previous 1:1 view) but _lbPickSourceKey(_lbNativeZoom) would pick a
    # lower-rank tier. The exact-key comparison would enter the deferred path
    # (badge stuck at 'Loading 1:1'); a rank comparison sees the current
    # source is already sharp enough and applies zoom synchronously.
    result = page.evaluate(
        """() => {
            window._lbCancelOriginalPreload();
            window._lbNativeZoom = 1.5;
            window._lbSetZoom(1, null, null);
            if (window._lbSwapTimer) {
                clearTimeout(window._lbSwapTimer);
                window._lbSwapTimer = null;
            }
            window._lbCurrentSrcKey = 'original';
            window._lbDesiredSrcKey = 'original';
            window._lbPending1To1 = false;
            window._lbPending1To1Anchor = null;
            // Sanity: the picked source key for this zoom must be lower rank
            // than 'original' or the test would trivially pass.
            const picked = window._lbPickSourceKey(window._lbNativeZoom);
            const pickedRank = window._lbSrcRank(picked);
            const currentRank = window._lbSrcRank(window._lbCurrentSrcKey);
            window.setLightboxZoomToOneToOne();
            return {
                pickedLowerThanCurrent: pickedRank < currentRank,
                zoom: window._lbZoom,
                pending: window._lbPending1To1,
                badge: document.getElementById('lightboxZoomBadge').textContent,
                desiredSource: window._lbDesiredSrcKey,
                swapPending: window._lbSwapTimer !== null,
            };
        }"""
    )
    assert result["pickedLowerThanCurrent"] is True
    assert abs(result["zoom"] - 1.5) < 0.01
    assert result["pending"] is False
    assert result["badge"] != "Loading 1:1"
    assert result["desiredSource"] == "original"
    assert result["swapPending"] is False


def test_browse_lightbox_reserves_space_for_bottom_controls(live_server, page):
    """The fitted image stays above the toolbar and expands when it is hidden."""
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="1200" '
        'viewBox="0 0 1600 1200"><rect width="1600" height="1200" fill="#274"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=svg, content_type="image/svg+xml"),
    )
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()

    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth === 1600;
        }"""
    )
    page.wait_for_function(
        """() => {
            const wrapRect = document.getElementById('lightboxWrap').getBoundingClientRect();
            const imageRect = document.getElementById('lightboxTransform').getBoundingClientRect();
            return imageRect.top >= wrapRect.top - 1
                && imageRect.bottom <= wrapRect.bottom + 1;
        }"""
    )

    visible = page.evaluate(
        """() => {
            const wrap = document.getElementById('lightboxWrap');
            const bar = document.querySelector('.lightbox-bottom-bar');
            const image = document.getElementById('lightboxTransform');
            const wrapRect = wrap.getBoundingClientRect();
            const barRect = bar.getBoundingClientRect();
            const imageRect = image.getBoundingClientRect();
            return {
                wrapHeight: wrapRect.height,
                wrapBottom: wrapRect.bottom,
                barTop: barRect.top,
                imageBottom: imageRect.bottom,
                fitScale: window._lbFitScale,
            };
        }"""
    )
    assert visible["wrapBottom"] < visible["barTop"]
    assert visible["imageBottom"] <= visible["wrapBottom"] + 1

    page.locator("#lightboxViewBtn").click()
    page.locator("#lightboxToggleChrome").click()
    expect(page.locator("#lightboxOverlay")).to_have_class(
        "lightbox-overlay active lb-hide-chrome"
    )
    page.wait_for_function(
        """before => {
            const wrap = document.getElementById('lightboxWrap');
            return wrap.clientHeight > before.wrapHeight + 20
                && window._lbFitScale > before.fitScale;
        }""",
        arg=visible,
    )


def test_browse_lightbox_conditional_controls_keep_fitted_photo_size_stable(
    live_server, page,
):
    """Conditional bottom controls must not resize the fitted photo.

    A species-eligible photo shows a representative control in the lightbox.
    Rejecting that photo hides the control after the flag write settles.  The
    bottom chrome may change internally, but its reserved viewport must remain
    stable so culling does not make the photo jump in size.
    """
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="1200" '
        'viewBox="0 0 1600 1200"><rect width="1600" height="1200" fill="#274"/></svg>'
    )
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=svg, content_type="image/svg+xml"),
    )
    page.set_viewport_size({"width": 1000, "height": 800})
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()

    expect(page.locator(".lifelist-lb-panel")).to_be_visible()
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth === 1600;
        }"""
    )
    before = page.evaluate(
        """() => ({
            wrapHeight: document.getElementById('lightboxWrap').clientHeight,
            fitScale: window._lbFitScale,
        })"""
    )

    second_id = live_server["data"]["photos"][1]
    page.keyboard.press("ArrowRight")
    page.wait_for_function(
        "photoId => window._lightboxCommittedId === photoId", arg=second_id
    )
    expect(page.locator(".lifelist-lb-panel")).to_be_hidden()
    page.wait_for_timeout(250)  # allow the debounced ResizeObserver refresh
    after_navigation = page.evaluate(
        """() => ({
            wrapHeight: document.getElementById('lightboxWrap').clientHeight,
            fitScale: window._lbFitScale,
        })"""
    )
    assert after_navigation["wrapHeight"] == before["wrapHeight"]
    assert abs(after_navigation["fitScale"] - before["fitScale"]) < 0.001

    first_id = live_server["data"]["photos"][0]
    page.keyboard.press("ArrowLeft")
    page.wait_for_function(
        "photoId => window._lightboxCommittedId === photoId", arg=first_id
    )
    expect(page.locator(".lifelist-lb-panel")).to_be_visible()

    page.keyboard.press("x")
    expect(page.locator("#lightboxFlagStatus")).to_have_text("Rejected")
    expect(page.locator(".lifelist-lb-panel")).to_be_hidden()
    page.wait_for_timeout(250)  # allow the debounced ResizeObserver refresh

    after = page.evaluate(
        """() => ({
            wrapHeight: document.getElementById('lightboxWrap').clientHeight,
            fitScale: window._lbFitScale,
        })"""
    )
    assert after["wrapHeight"] == before["wrapHeight"]
    assert abs(after["fitScale"] - before["fitScale"]) < 0.001


def test_browse_lightbox_view_menu_tracks_wrapping_action_bar(live_server, page):
    """An open View menu stays anchored through viewport and control changes."""
    page.set_viewport_size({"width": 1000, "height": 700})
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    page.locator("#lightboxViewBtn").click()

    panel_tracks_button = """() => {
        const panel = document.getElementById('lightboxViewPanel');
        const button = document.getElementById('lightboxViewBtn');
        const p = panel.getBoundingClientRect();
        const b = button.getBoundingClientRect();
        const gutter = 8;
        const expectedLeft = Math.max(
            gutter,
            Math.min(b.left, Math.max(gutter, innerWidth - p.width - gutter)),
        );
        const expectedTop = Math.max(gutter, b.top - p.height - gutter);
        return panel.classList.contains('open')
            && Math.abs(p.left - expectedLeft) < 2
            && Math.abs(p.top - expectedTop) < 2
            && p.right <= innerWidth - gutter + 1;
    }"""
    page.wait_for_function(panel_tracks_button)

    # Narrowing the viewport wraps the action bar and moves the View button.
    page.set_viewport_size({"width": 640, "height": 700})
    page.wait_for_function(panel_tracks_button)

    # Conditional controls and their labels can also move the anchor without
    # a window resize (for example, paired sources or Life List actions).
    page.evaluate(
        """() => {
            const source = document.getElementById('lightboxSourceControl');
            source.style.display = '';
            source.textContent = 'Viewing developed JPEG · Show original RAW';
        }"""
    )
    page.wait_for_function(panel_tracks_button)


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
    """Open in Browse returns a bounded deep target page in one init."""
    db = live_server["db"]
    _, folder_b = live_server["data"]["folders"]
    target_id = live_server["data"]["photos"][4]  # robin2 in folder_b
    target_queries = []

    def capture_target_query(request):
        if not request.url.endswith("/api/photos/query") or request.method != "POST":
            return
        payload = json.loads(request.post_data or "{}")
        if payload.get("folder_id") == folder_b:
            target_queries.append(payload)

    page.on("request", capture_target_query)

    for idx in range(560):
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
    assert target_queries == []

    initial_ids = page.evaluate("photos.map(function(p) { return p.id; })")
    paging = page.evaluate("({earliestPage: earliestPage, perPage: perPage})")
    initial_page = paging["earliestPage"]
    assert initial_page > 1
    position_summary = page.evaluate(
        """() => {
          updateScrollPosition();
          return document.getElementById('filterSummary').textContent;
        }"""
    )
    visible_range = re.match(r"(\d+)–(\d+) of", position_summary)
    assert visible_range is not None
    offset = (initial_page - 1) * paging["perPage"]
    assert int(visible_range.group(1)) >= offset + 1

    # The truncated window must announce itself rather than pass the target's
    # page off as the whole folder, and the unloaded photos sit *before* the
    # window — they must not be counted into the downward skeleton runway.
    expect(page.locator("#loadPreviousPhotosBanner")).to_be_visible()
    expect(page.locator("#loadPreviousPhotosText")).to_contain_text(
        f"{offset:,} earlier photos"
    )
    expect(page.locator("#loadPreviousPhotosText")).to_contain_text(f"#{offset + 1:,}")
    assert page.evaluate("loadedWindowOffset()") == offset
    skeletons = page.evaluate("document.querySelectorAll('#gridTail .skel-card').length")
    assert skeletons <= max(
        0, page.evaluate("totalPhotos") - offset - len(initial_ids)
    )

    # Reaching the upper boundary extends the focused window backward without
    # moving the cards the user was looking at, even when they keep scrolling
    # while a slow preceding-page request is in flight.
    page.evaluate(
        """() => new Promise(resolve => {
          const container = document.getElementById('gridContainer');
          container.scrollTo({top: 600, behavior: 'instant'});
          requestAnimationFrame(() => requestAnimationFrame(resolve));
        })"""
    )
    page.evaluate(
        """expectedPage => {
          const originalSafeFetch = window.safeFetch;
          let release;
          window.__releasePreviousPage = () => release && release();
          window.safeFetch = async function(url, options, behavior) {
            const body = options && options.body ? JSON.parse(options.body) : null;
            if (url === '/api/photos/query' && body && body.page === expectedPage) {
              const response = originalSafeFetch.call(this, url, options, behavior);
              await new Promise(resolve => { release = resolve; });
              return response;
            }
            return originalSafeFetch.call(this, url, options, behavior);
          };
        }""",
        initial_page - 1,
    )
    old_first_id = initial_ids[0]
    page.evaluate(
        """() => {
          const container = document.getElementById('gridContainer');
          container.scrollTop = 500;
          container.dispatchEvent(new Event('scroll'));
        }"""
    )
    page.wait_for_function("loading === true && !!window.__releasePreviousPage")
    page.evaluate(
        """() => {
          const container = document.getElementById('gridContainer');
          container.scrollTop = 250;
          container.dispatchEvent(new Event('scroll'));
        }"""
    )
    response_time_top = page.locator(f'.grid-card[data-id="{old_first_id}"]').evaluate(
        "el => el.getBoundingClientRect().top"
    )
    page.evaluate("window.__releasePreviousPage()")
    page.wait_for_function(
        "expected => earliestPage === expected && loading === false",
        arg=initial_page - 1,
        timeout=5000,
    )
    prepended_ids = page.evaluate("photos.map(function(p) { return p.id; })")
    assert prepended_ids[-len(initial_ids) :] == initial_ids
    assert len(prepended_ids) == len(set(prepended_ids))
    assert target_queries[-1]["page"] == initial_page - 1
    new_first_top = page.locator(f'.grid-card[data-id="{old_first_id}"]').evaluate(
        "el => el.getBoundingClientRect().top"
    )
    assert abs(new_first_top - response_time_top) < 2

    page.locator("#loadPreviousPhotosButton").click()
    page.wait_for_function("earliestPage === 1 && browseDatasetReady", timeout=5000)
    restarted_ids = page.evaluate("photos.map(function(p) { return p.id; })")
    assert restarted_ids
    assert restarted_ids != initial_ids
    assert len(restarted_ids) == len(set(restarted_ids))
    assert target_queries[-1]["page"] == 1
    # Back to a contiguous prefix: nothing is missing, so nothing is claimed.
    expect(page.locator("#loadPreviousPhotosBanner")).to_be_hidden()


def test_browse_photo_id_deep_link_invalidates_older_workspace_load(live_server, page):
    """A focused folder claim must discard a load started during metadata fetch."""
    _, folder_b = live_server["data"]["folders"]
    target_id = live_server["data"]["photos"][4]

    page.goto(f"{live_server['url']}/browse")
    page.wait_for_function("browseDatasetReady", timeout=5000)

    result = page.evaluate(
        """async ({targetId, folderId}) => {
          const originalFetch = window.safeFetch;
          let releaseMetadata;
          let releaseOldLoad;
          let metadataRequested;
          let oldLoadRequested;
          const metadataSeen = new Promise(resolve => { metadataRequested = resolve; });
          const oldLoadSeen = new Promise(resolve => { oldLoadRequested = resolve; });
          const metadataResponse = new Promise(resolve => { releaseMetadata = resolve; });
          const oldLoadResponse = new Promise(resolve => { releaseOldLoad = resolve; });
          let interceptOldLoad = false;

          window.safeFetch = async function(url, options, behavior) {
            if (url === '/api/photos/' + targetId) {
              metadataRequested();
              return metadataResponse;
            }
            if (interceptOldLoad && url === '/api/photos/query') {
              interceptOldLoad = false;
              oldLoadRequested();
              return oldLoadResponse;
            }
            return originalFetch.call(window, url, options, behavior);
          };

          try {
            const deepLink = _runPhotoDeepLink(targetId);
            await metadataSeen;

            interceptOldLoad = true;
            const oldLoad = resetAndLoad();
            await oldLoadSeen;

            releaseMetadata({id: targetId, folder_id: folderId});
            await deepLink;
            releaseOldLoad({photos: [{id: -999, folder_id: -1}], total: 1});
            await oldLoad;

            return {
              activeFolderId,
              ids: photos.map(function(photo) { return photo.id; }),
            };
          } finally {
            window.safeFetch = originalFetch;
          }
        }""",
        {"targetId": target_id, "folderId": folder_b},
    )

    assert result["activeFolderId"] == folder_b
    assert target_id in result["ids"]
    assert -999 not in result["ids"]


# Gate two requests inside the page so the deep-link race is deterministic:
# hold /api/photos/<id> (the deep link's first await) and the first
# /api/photos/query POST (the workspace-scoped load a sort change starts while
# that await is pending). Patching window.fetch in an init script is enough —
# vireo-api.js captures window.fetch at load, which is after init scripts run.
_DEEP_LINK_REQUEST_GATE = """
(() => {
  const gate = { photoRelease: null, queryRelease: null, queryLanded: false };
  window.__vireoGate = gate;
  const targetPath = '/api/photos/__TARGET_ID__';
  const origFetch = window.fetch;
  window.fetch = function(input, init) {
    const self = this;
    const args = arguments;
    const url = String((input && input.url) || input || '');
    const method = String((init && init.method) || 'GET').toUpperCase();
    if (!gate.photoRelease && method === 'GET' && url.endsWith(targetPath)) {
      return new Promise(resolve => {
        gate.photoRelease = () => resolve(origFetch.apply(self, args));
      });
    }
    if (!gate.queryRelease && method === 'POST' && url.indexOf('/api/photos/query') !== -1) {
      return new Promise(resolve => {
        gate.queryRelease = () => resolve(
          origFetch.apply(self, args).then(async response => {
            // Body is buffered by the time a clone reads it, so this flips
            // once the app's own .text() is about to resolve too.
            try { await response.clone().text(); } catch (e) {}
            gate.queryLanded = true;
            return response;
          })
        );
      });
    }
    return origFetch.apply(self, args);
  };
})();
"""


def test_browse_photo_id_deep_link_discards_loads_started_before_folder_switch(
    live_server, page
):
    """A load started before the deep link claimed the folder must be dropped.

    Changing the sort while /api/photos/<id> is still pending runs
    applyFilters() -> resetAndLoad(), which starts a *workspace-scoped* page-1
    load. The deep link then takes over the grid for the target's folder. If it
    only snapshots the epoch that reset installed, that older load still passes
    its own guard and appends unscoped workspace rows into the folder-scoped
    window -- and the "N earlier photos aren't loaded" banner ends up counting
    against a dataset that is no longer on screen (Codex review r3792769108).
    """
    db = live_server["db"]
    folder_a, folder_b = live_server["data"]["folders"]
    target_id = live_server["data"]["photos"][4]  # robin2 in folder_b

    folder_b_ids = {live_server["data"]["photos"][3], target_id}
    for idx in range(560):
        folder_b_ids.add(
            db.add_photo(
                folder_id=folder_b,
                filename=f"yard-before-{idx:02d}.jpg",
                extension=".jpg",
                file_size=1000,
                file_mtime=10 + idx,
                timestamp=f"2024-01-{(idx % 28) + 1:02d}T00:00:00",
            )
        )
    # Photos outside the target folder that sort ahead of everything under
    # name_desc, so the workspace-scoped page 1 we hold is made entirely of
    # foreign rows: a leak is unmistakable in the grid.
    for idx in range(60):
        db.add_photo(
            folder_id=folder_a,
            filename=f"zz-park-extra-{idx:02d}.jpg",
            extension=".jpg",
            file_size=1000,
            file_mtime=900 + idx,
            timestamp=f"2025-02-{(idx % 28) + 1:02d}T00:00:00",
        )

    page.add_init_script(
        _DEEP_LINK_REQUEST_GATE.replace("__TARGET_ID__", str(target_id))
    )
    page.goto(f"{live_server['url']}/browse?photo_id={target_id}")

    # The deep link is parked on /api/photos/<id>.
    page.wait_for_function("!!(window.__vireoGate && window.__vireoGate.photoRelease)")

    # User changes the sort. activeFolderId is still null, so this reset loads
    # the unscoped workspace grid -- and we hold its response.
    # name_desc puts the 560 "yard-before-*" rows ahead of robin2, so the
    # target lands well past page 1 and the truncated-window banner applies.
    page.select_option("#sortSelect", "name_desc")
    page.wait_for_function("!!window.__vireoGate.queryRelease")

    # Let the deep link finish claiming and painting the target folder.
    page.evaluate("window.__vireoGate.photoRelease()")
    expect(page.locator(f'.grid-card[data-id="{target_id}"]')).to_be_visible(
        timeout=5000
    )
    page.wait_for_function("browseDatasetReady === true")

    # Now deliver the abandoned workspace load.
    page.evaluate("window.__vireoGate.queryRelease()")
    page.wait_for_function("window.__vireoGate.queryLanded === true")
    # Two frames: any continuation the stale response schedules has run.
    page.evaluate(
        "() => new Promise(r => requestAnimationFrame("
        "() => requestAnimationFrame(r)))"
    )

    assert page.evaluate("window.activeFolderId") == folder_b
    loaded_ids = page.evaluate("photos.map(function(p) { return p.id; })")
    assert loaded_ids
    assert len(loaded_ids) == len(set(loaded_ids))
    assert set(loaded_ids) <= folder_b_ids
    card_ids = page.evaluate(
        "Array.from(document.querySelectorAll('#grid .grid-card'))"
        ".map(function(c) { return parseInt(c.dataset.id, 10); })"
    )
    assert set(card_ids) <= folder_b_ids
    assert len(card_ids) == len(loaded_ids)

    # Transparency invariant: the banner's count must describe the window that
    # is actually on screen, not the abandoned one.
    offset = page.evaluate("loadedWindowOffset()")
    assert offset == page.evaluate("(earliestPage - 1) * perPage")
    assert offset > 0
    expect(page.locator("#loadPreviousPhotosBanner")).to_be_visible()
    expect(page.locator("#loadPreviousPhotosText")).to_contain_text(
        f"{offset:,} earlier photos"
    )
    expect(page.locator("#loadPreviousPhotosText")).to_contain_text(f"#{offset + 1:,}")
    assert page.evaluate("totalPhotos") == len(folder_b_ids)


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
    page.route("**/photos/*/original*", _hold_when_active)

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

    page.route("**/photos/*/original*", serve_original)
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
    assert any(
        f"/photos/{next_id}/original" in url and "prefetch=1" in url
        for url in original_requests
    )
    assert "prefetch=1" in page.evaluate(
        "nextId => window._lbSrcUrl(nextId, 'original')", arg=next_id,
    )
    assert page.evaluate("window._lightboxCurrentId") != next_id


def test_browse_lightbox_warms_fit_window_and_reuses_it_on_reversal(live_server, page):
    """Fast steps and a reversal reuse decoded neighbors in a bounded window."""
    page.route(
        "**/photos/*/full*",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").nth(1).dblclick()
    ids = page.evaluate("window._lightboxPhotoList.map(photo => photo.id)")
    page.wait_for_function(
        """ids => ids.every(id => Object.values(_lbAdjacentPreloads).some(
            entry => entry.photoId === id && entry.status === 'decoded'
        ))""",
        arg=[ids[0], *ids[2:5]],
    )
    assert page.evaluate("Object.keys(_lbAdjacentPreloads).length") == 4

    # Two immediate steps use the head start built while viewing photo 2.
    for photo_id in ids[2:4]:
        page.keyboard.press("ArrowRight")
        page.wait_for_function(
            "id => _lightboxCommittedId === id && !_lbVisualTransitionPending",
            arg=photo_id,
        )
        assert "prefetch=1" in page.locator("#lightboxImg").get_attribute("src")
    # The larger window still retains photo 1 after two forward steps.
    assert page.evaluate(
        "id => Object.values(_lbAdjacentPreloads).some(e => e.photoId === id && e.status === 'decoded')",
        ids[0],
    )
    assert page.evaluate("Object.keys(_lbAdjacentPreloads).length") <= 5

    page.keyboard.press("ArrowLeft")
    page.wait_for_function("id => _lightboxCommittedId === id", arg=ids[2])
    assert "prefetch=1" in page.locator("#lightboxImg").get_attribute("src")
    page.keyboard.press("Escape")
    assert page.evaluate("Object.keys(_lbAdjacentPreloads).length") == 0


def test_browse_lightbox_queues_warmups_and_continues_past_failures(live_server, page):
    """A slow warmup permits cached neighbors to load; a rejected one gets a bounded retry."""
    held = []
    requests = []
    failing_id = None

    def serve_full(route):
        photo_id = int(re.search(r"/photos/(\d+)/", route.request.url)[1])
        if "prefetch=1" in route.request.url:
            requests.append(photo_id)
            if len(requests) == 1:
                held.append(route)
                return
            if photo_id == failing_id:
                route.fulfill(status=503, body="Busy")
                return
        route.fulfill(body=base64.b64decode(_PNG_1X1), content_type="image/png")

    page.route("**/photos/*/full*", serve_full)
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").nth(1).dblclick()
    ids = page.evaluate("window._lightboxPhotoList.map(photo => photo.id)")
    failing_id = ids[2]
    page.wait_for_function(
        "Object.values(_lbAdjacentPreloads).some(entry => entry.status === 'loading')"
    )
    page.wait_for_timeout(200)
    assert requests[0] == failing_id
    assert set(requests) == {failing_id, ids[0], ids[3], ids[4]}
    assert len(held) == 1
    held.pop().fulfill(status=503, body="Busy")
    page.wait_for_function(
        """ids => ids.every(id => Object.values(_lbAdjacentPreloads).some(
            entry => entry.photoId === id && entry.status === 'decoded'
        ))""",
        arg=[ids[0], ids[3], ids[4]],
    )
    page.wait_for_function(
        "Object.values(_lbAdjacentPreloadRetry).some(retry => retry.count === 2)"
    )
    page.wait_for_timeout(800)
    assert requests.count(failing_id) == 2
    assert requests[0] == failing_id
    assert set(requests[:4]) == {failing_id, ids[0], ids[3], ids[4]}


def test_browse_lightbox_pauses_fit_warmups_during_source_upgrade(live_server, page):
    """Finishing a Fit warmup cannot queue more work ahead of a pending zoom image."""
    held_fit = []
    held_original = []
    fit_requests = []
    original_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000">'
        '<rect width="4000" height="2000" fill="#274"/></svg>'
    )

    def serve_full(route):
        """Hold the first neighboring preview until the zoom upgrade is pending."""
        if "prefetch=1" in route.request.url:
            fit_requests.append(route.request.url)
            if len(fit_requests) == 1:
                held_fit.append(route)
                return
        route.fulfill(body=base64.b64decode(_PNG_1X1), content_type="image/png")

    def serve_original(route):
        """Stall the visible upgrade while allowing later original warmups."""
        if "prefetch=1" not in route.request.url and not held_original:
            held_original.append(route)
            return
        route.fulfill(body=original_svg, content_type="image/svg+xml")

    page.route("**/photos/*/full*", serve_full)
    page.route("**/photos/*/original*", serve_original)
    page.goto(f"{live_server['url']}/browse")
    # Saturate the pool with one held request to exercise the no-spare-slot path.
    page.evaluate("_lbPreloadConcurrency = 1")
    page.locator(".grid-card").nth(1).dblclick()
    page.wait_for_function(
        """_lbFullUsesOriginal === false && Object.values(_lbAdjacentPreloads).some(
            entry => entry.status === 'loading'
        )"""
    )
    page.evaluate("_lbSetZoom(2)")
    page.wait_for_timeout(200)
    assert len(held_original) == 1
    assert page.evaluate("_lbCurrentSrcKey") == "full"
    assert page.evaluate("_lbDesiredSrcKey") == "original"
    assert len(held_fit) == 1
    held_fit.pop().fulfill(body=base64.b64decode(_PNG_1X1), content_type="image/png")
    page.wait_for_function(
        "Object.values(_lbAdjacentPreloads).some(entry => entry.status === 'decoded')"
    )
    page.wait_for_timeout(200)
    assert len(fit_requests) == 1

    held_original[0].fulfill(body=original_svg, content_type="image/svg+xml")
    page.wait_for_function(
        """_lbCurrentSrcKey === 'original' && Object.values(_lbAdjacentPreloads).some(
            entry => entry.sourceKey === 'original' && entry.status === 'decoded'
        )"""
    )


@pytest.mark.parametrize("failed_tier,needed_pixels", [("full", 100), ("2560", 2300), ("3840", 3200)])
@pytest.mark.parametrize("superseded", [False, True])
def test_browse_lightbox_resumes_warmups_after_source_failure(
    live_server, page, failed_tier, needed_pixels, superseded,
):
    """A failed tier releases the queue while retaining the usable original."""
    held_neighbor = []
    held_tier = []
    failure_enabled = False
    neighbor_requests = []
    original_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000">'
        '<rect width="4000" height="2000" fill="#274"/></svg>'
    )

    def serve_photo(route):
        """Keep one neighbor and the selected failing tier pending independently."""
        url = route.request.url
        speculative = "prefetch=1" in url
        if (failure_enabled and "/original" not in url and not speculative
                and not any(held.request.url == url for held in held_tier)):
            held_tier.append(route)
            return
        if "/original" in url:
            if speculative:
                neighbor_requests.append(url)
                if len(neighbor_requests) == 1:
                    held_neighbor.append(route)
                    return
            route.fulfill(body=original_svg, content_type="image/svg+xml")
        else:
            route.fulfill(
                body=original_svg.replace('width="4000"', 'width="1920"').replace('height="2000"', 'height="960"'),
                content_type="image/svg+xml",
            )

    page.route("**/photos/*/full*", serve_photo)
    page.route("**/photos/*/original*", serve_photo)
    page.route("**/photos/*/preview?*", serve_photo)
    page.goto(f"{live_server['url']}/browse")
    # Saturate the pool with one held request to exercise the no-spare-slot path.
    page.evaluate("_lbPreloadConcurrency = 1")
    page.locator(".grid-card").nth(1).dblclick()
    page.wait_for_function("_lbFullUsesOriginal === false && _lbFullLongEdge !== null")
    page.evaluate("_lbSetZoom(100)")
    page.wait_for_function(
        """_lbCurrentSrcKey === 'original' && Object.values(_lbAdjacentPreloads).some(
            entry => entry.sourceKey === 'original' && entry.status === 'loading'
        )"""
    )
    # Chromium serves a repeated <img> URL straight from the renderer's
    # in-memory image cache without any network request, so re-requesting the
    # /full tier the lightbox opened with would never reach the route (and so
    # could never fail). Stamp a fresh edit version on the photo so the tier
    # under test is genuinely fetched, as it is after an edit or cache eviction.
    page.evaluate("_lbEditVersionByPhoto[String(_lightboxCurrentId)] = 'retry'")
    failure_enabled = True
    target = "/full" if failed_tier == "full" else f"size={failed_tier}"
    with page.expect_request(lambda request: target in request.url and "prefetch=1" not in request.url):
        page.evaluate(
            "pixels => _lbScheduleSourceSwap(pixels / (_lbPhotoW * _lbFitScale * devicePixelRatio))",
            needed_pixels,
        )
    assert page.evaluate("_lbDesiredSrcKey") == failed_tier
    assert len(held_neighbor) == 1
    held_neighbor[0].fulfill(body=original_svg, content_type="image/svg+xml")
    page.wait_for_function(
        "Object.values(_lbAdjacentPreloads).some(entry => entry.status === 'decoded')"
    )
    page.wait_for_timeout(200)
    assert len(neighbor_requests) == 1
    assert len(held_tier) == 1
    expected_source = "original"
    if superseded:
        expected_source, next_pixels = ("2560", 2300) if failed_tier == "3840" else ("3840", 3200)
        with page.expect_request(
            lambda request: f"size={expected_source}" in request.url and "prefetch=1" not in request.url
        ):
            page.evaluate(
                "pixels => _lbScheduleSourceSwap(pixels / (_lbPhotoW * _lbFitScale * devicePixelRatio))",
                next_pixels,
            )
    held_tier[0].fulfill(status=503, body="Transient tier failure")
    if superseded:
        page.wait_for_timeout(200)
        assert page.evaluate("_lbDesiredSrcKey") == expected_source
        assert len(neighbor_requests) == 1
        assert len(held_tier) == 2
        held_tier[1].fulfill(body=original_svg, content_type="image/svg+xml")
    page.wait_for_function("_lbDesiredSrcKey === _lbCurrentSrcKey", timeout=5000)
    page.wait_for_function(
        """source => Object.values(_lbAdjacentPreloads).filter(
            entry => entry.sourceKey === source && entry.status === 'decoded'
        ).length === 2""",
        arg=expected_source,
    )
    assert page.evaluate("_lbCurrentSrcKey") == expected_source


@pytest.mark.parametrize("neighbor_fails", [False, True])
def test_browse_lightbox_original_waits_for_slow_neighbor_queue(live_server, page, neighbor_fails):
    """The original dwell cannot spend retries competing with slow Fit generation."""
    current_id = live_server["data"]["photos"][0]
    live_server["db"].conn.execute(
        "UPDATE photos SET width=4000, height=2000 WHERE id=?", (current_id,),
    )
    live_server["db"].conn.commit()
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1920" height="960">'
        '<rect width="1920" height="960" fill="#274"/></svg>'
    )
    held_fit = []
    original_requests = []

    def serve_full(route):
        """Hold the first adjacent render longer than both original dwell attempts."""
        if "prefetch=1" in route.request.url and (
            not held_fit or (neighbor_fails and len(held_fit) == 1 and route.request.url == held_fit[0].request.url)
        ):
            held_fit.append(route)
            return
        route.fulfill(body=full_svg, content_type="image/svg+xml")

    def serve_original(route):
        """Record whether the original competes with a neighboring render."""
        original_requests.append(route.request.url)
        route.fulfill(body=full_svg, content_type="image/svg+xml")

    page.route("**/photos/*/full*", serve_full)
    page.route("**/photos/*/original*", serve_original)
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()
    page.wait_for_function("_lbOriginalPreloadWaiting !== null")
    page.wait_for_timeout(1600)
    assert len(held_fit) == 1
    assert original_requests == []
    if neighbor_fails:
        with page.expect_request(lambda request: request.url == held_fit[0].request.url):
            held_fit[0].fulfill(status=503, body="Temporary neighbor failure")
        # The request event can arrive before Playwright invokes its route
        # handler. Wait for the parked retry before inspecting or releasing it.
        deadline = time.monotonic() + 5
        while len(held_fit) < 2 and time.monotonic() < deadline:
            page.wait_for_timeout(10)
        assert original_requests == []
        assert len(held_fit) == 2
        held_fit[1].fulfill(body=full_svg, content_type="image/svg+xml")
    else:
        held_fit[0].fulfill(body=full_svg, content_type="image/svg+xml")
    page.wait_for_function("_lbOriginalPreload && _lbOriginalPreload.status === 'decoded'")
    assert len(original_requests) == 1
    assert "prefetch=1" in original_requests[0]


@pytest.mark.parametrize("retired_source", ["full", "original"])
@pytest.mark.parametrize("action", ["navigate", "zoom", "reopen"])
def test_browse_lightbox_keeps_pruned_request_in_flight_until_response(
    live_server, page, retired_source, action,
):
    """Changing the navigation window cannot reuse an occupied server slot."""
    current_id = live_server["data"]["photos"][0]
    live_server["db"].conn.execute(
        "UPDATE photos SET width=4000, height=2000 WHERE id=?", (current_id,),
    )
    live_server["db"].conn.commit()
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1920" height="960">'
        '<rect width="1920" height="960" fill="#274"/></svg>'
    )
    held = []
    speculative_requests = []

    def serve_photo(route):
        """Keep one server response pending even after the viewer retires it."""
        if "prefetch=1" in route.request.url:
            speculative_requests.append(route.request.url)
            if f"/{retired_source}" in route.request.url and not held:
                held.append(route)
                return
        route.fulfill(body=svg, content_type="image/svg+xml")

    page.route("**/photos/*/full*", serve_photo)
    page.route("**/photos/*/original*", serve_photo)
    page.goto(f"{live_server['url']}/browse")
    # Saturate the pool with one held request to exercise the no-spare-slot path.
    page.evaluate("_lbPreloadConcurrency = 1")
    page.locator(".grid-card").first.dblclick()
    page.wait_for_function(
        "source => _lbSpeculativeInFlight && _lbSpeculativeInFlight.sourceKey === source",
        arg=retired_source,
    )
    page.wait_for_timeout(100)
    assert len(held) == 1
    # Routed images may be transferred again when a decoded warmup becomes
    # visible; only a new URL represents extra background work here.
    before = set(speculative_requests)
    page.evaluate("window.__retiredWarmup = _lbSpeculativeInFlight")
    if action == "zoom":
        page.evaluate("_lbSetZoom(100)")
        page.wait_for_function("_lbCurrentSrcKey === 'original'")
    else:
        if action == "reopen":
            page.keyboard.press("Escape")
        target_id = page.evaluate(
            """() => {
                var target = _lightboxPhotoList[3];
                openLightbox(target.id, target.filename, _lightboxPhotoList);
                return target.id;
            }"""
        )
        page.wait_for_function("id => _lightboxCommittedId === id", arg=target_id)
    page.evaluate("_lbClearAdjacentPreloads(); _lbScheduleAdjacentPhoto(_lbCurrentSrcKey)")
    page.wait_for_timeout(1600)
    assert set(speculative_requests) == before
    assert page.evaluate("_lbSpeculativeInFlight !== null")
    held[0].fulfill(body=svg, content_type="image/svg+xml")
    page.wait_for_function(
        """action => Object.values(_lbAdjacentPreloads).some(
            entry => entry.photoId === _lightboxPhotoList[action === 'zoom' ? 1 : 4].id &&
                entry.sourceKey === (action === 'zoom' ? 'original' : 'full') && entry.status === 'decoded'
        ) && _lbSpeculativeInFlight === null""",
        arg=action,
    )
    # The wider window may reuse URLs already fetched before the hold.
    # The decoded entries above verify that the queue resumed either way.
    if action != "zoom":
        assert page.evaluate(
            "Object.values(_lbAdjacentPreloads).every(entry => entry !== window.__retiredWarmup)"
        )


def test_browse_lightbox_close_discards_pending_warmup_queue(live_server, page):
    """A late image completion after close cannot start the remaining warmups."""
    held = []

    def serve_full(route):
        if "prefetch=1" in route.request.url:
            held.append(route)
            return
        route.fulfill(body=base64.b64decode(_PNG_1X1), content_type="image/png")

    page.route("**/photos/*/full*", serve_full)
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").nth(1).dblclick()
    page.wait_for_function(
        "Object.values(_lbAdjacentPreloads).some(entry => entry.status === 'loading')"
    )
    page.wait_for_timeout(100)
    assert len(held) == 3
    page.keyboard.press("Escape")
    for route in held:
        route.fulfill(body=base64.b64decode(_PNG_1X1), content_type="image/png")
    page.wait_for_timeout(200)
    assert len(held) == 3
    assert page.evaluate("Object.keys(_lbAdjacentPreloads).length") == 0
    assert page.evaluate("_lbAdjacentPreloadTimer") is None


def test_browse_lightbox_preloads_current_original_after_preview_settles(
    live_server, page
):
    """The current photo's 100% source is decoded after a short dwell at Fit."""
    current_id = live_server["data"]["photos"][0]
    live_server["db"].conn.execute(
        "UPDATE photos SET width=4000, height=2000 WHERE id=?",
        (current_id,),
    )
    live_server["db"].conn.commit()
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1920" height="960" '
        'viewBox="0 0 1920 960"><rect width="1920" height="960" fill="#274"/></svg>'
    )
    original_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#274"/></svg>'
    )
    original_requests = []

    def serve_original(route):
        original_requests.append(route.request.url)
        route.fulfill(body=original_svg, content_type="image/svg+xml")

    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=full_svg, content_type="image/svg+xml"),
    )
    page.route("**/photos/*/original*", serve_original)
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")

    assert page.evaluate("window._lightboxCurrentId") == current_id
    page.wait_for_timeout(800)
    assert original_requests == []
    page.wait_for_function(
        """photoId => window._lbOriginalPreload && (
            window._lbOriginalPreload.photoId === photoId &&
            window._lbOriginalPreload.status === 'decoded'
        )""",
        arg=current_id,
    )

    assert any(f"/photos/{current_id}/original" in url for url in original_requests)
    assert all("prefetch=1" in url for url in original_requests)
    assert "prefetch=1" in page.evaluate(
        "photoId => window._lbSrcUrl(photoId, 'original')", arg=current_id,
    )
    assert page.evaluate("window._lbCurrentSrcKey") == "full"


def test_browse_lightbox_waits_for_fit_image_before_preloading_original(
    live_server, page
):
    """A slow Fit render is not made slower by a competing original request."""
    current_id = live_server["data"]["photos"][0]
    db = live_server["db"]
    db.conn.execute(
        "UPDATE photos SET width=4000, height=2000 WHERE id=?",
        (current_id,),
    )
    db.conn.commit()
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1920" height="960" '
        'viewBox="0 0 1920 960"><rect width="1920" height="960" fill="#274"/></svg>'
    )
    original_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#274"/></svg>'
    )
    held_full = {}
    original_requests = []

    def hold_full(route):
        held_full["route"] = route

    def serve_original(route):
        original_requests.append(route.request.url)
        route.fulfill(body=original_svg, content_type="image/svg+xml")

    page.route("**/photos/*/full", hold_full)
    page.route("**/photos/*/original*", serve_original)
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()
    page.wait_for_function("window._lbFullUsesOriginal === false")
    page.wait_for_timeout(800)

    assert "route" in held_full
    assert original_requests == []

    held_full.pop("route").fulfill(body=full_svg, content_type="image/svg+xml")
    page.wait_for_function(
        """photoId => window._lbOriginalPreload && (
            window._lbOriginalPreload.photoId === photoId &&
            window._lbOriginalPreload.status === 'decoded'
        )""",
        arg=current_id,
    )


def test_browse_lightbox_skips_original_when_full_covers_one_to_one(
    live_server, page
):
    """Small photos stay on /full at 100%, so warming /original is wasteful."""
    current_id = live_server["data"]["photos"][0]
    db = live_server["db"]
    db.conn.execute(
        "UPDATE photos SET width=1600, height=800 WHERE id=?",
        (current_id,),
    )
    db.conn.commit()
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="800" '
        'viewBox="0 0 1600 800"><rect width="1600" height="800" fill="#274"/></svg>'
    )
    original_requests = []

    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=svg, content_type="image/svg+xml"),
    )

    def serve_original(route):
        original_requests.append(route.request.url)
        route.fulfill(body=svg, content_type="image/svg+xml")

    page.route("**/photos/*/original*", serve_original)
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()
    page.wait_for_function("window._lbNativeZoom !== null")
    page.wait_for_timeout(800)

    assert page.evaluate("window._lbPickSourceKey(window._lbNativeZoom)") == "full"
    assert original_requests == []
    assert page.evaluate("window._lbOriginalPreload") is None


def test_browse_lightbox_cancels_original_warmup_when_zoom_leaves_fit(
    live_server, page
):
    """A visible intermediate-tier upgrade takes priority over background warming."""
    current_id = live_server["data"]["photos"][0]
    db = live_server["db"]
    db.conn.execute(
        "UPDATE photos SET width=4000, height=2000 WHERE id=?",
        (current_id,),
    )
    db.conn.commit()
    full_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1920" height="960" '
        'viewBox="0 0 1920 960"><rect width="1920" height="960" fill="#274"/></svg>'
    )
    preview_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="2560" height="1280" '
        'viewBox="0 0 2560 1280"><rect width="2560" height="1280" fill="#274"/></svg>'
    )
    original_requests = []

    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=full_svg, content_type="image/svg+xml"),
    )
    page.route(
        "**/photos/*/preview?size=2560*",
        lambda route: route.fulfill(body=preview_svg, content_type="image/svg+xml"),
    )

    def serve_original(route):
        original_requests.append(route.request.url)
        route.fulfill(body=preview_svg, content_type="image/svg+xml")

    page.route("**/photos/*/original*", serve_original)
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()
    page.wait_for_function("window._lbOriginalPreloadTimer !== null")

    selected_key = page.evaluate(
        """() => {
            let zoom = 1.01;
            while (zoom < window._lbNativeZoom && window._lbPickSourceKey(zoom) === 'full') {
                zoom += 0.05;
            }
            const key = window._lbPickSourceKey(zoom);
            window._lbSetZoom(zoom);
            return key;
        }"""
    )
    assert selected_key == "2560"
    page.wait_for_timeout(800)

    assert original_requests == []
    assert page.evaluate("window._lbOriginalPreloadTimer") is None
    assert page.evaluate("window._lbOriginalPreload") is None


def test_browse_lightbox_does_not_preload_when_full_already_uses_original(
    live_server, page
):
    """Full-resolution preview mode must not request the original twice."""
    db = live_server["db"]
    db.update_workspace(
        db._active_workspace_id,
        config_overrides={"preview_max_size": 0},
    )
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#274"/></svg>'
    )
    original_requests = []

    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(body=svg, content_type="image/svg+xml"),
    )

    def serve_original(route):
        original_requests.append(route.request.url)
        route.fulfill(body=svg, content_type="image/svg+xml")

    page.route("**/photos/*/original*", serve_original)
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.dblclick()
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    page.wait_for_function("window._lbFullUsesOriginal === true")
    page.wait_for_timeout(800)

    assert original_requests == []
    assert page.evaluate("window._lbOriginalPreload") is None


def test_browse_lightbox_carries_current_viewport_to_previously_seen_photo(
    live_server, page
):
    """Arrow navigation uses the current viewport, not a target photo's old one."""
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
        "**/photos/*/original*",
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
            window._lbApplyViewportState({
                zoom: window._lbNativeZoom,
                centerX: 0.24,
                centerY: 0.70,
                oneToOne: true,
            });
            window._lbSaveViewportState(window._lightboxCurrentId);
            return window._lbViewportStateFromCurrent();
        }"""
    )
    assert first_view["oneToOne"] is True

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

    second_view = page.evaluate(
        """() => {
            window._lbApplyViewportState({zoom: 1, centerX: 0.5, centerY: 0.5});
            window._lbSaveViewportState(window._lightboxCurrentId);
            return window._lbViewportStateFromCurrent();
        }"""
    )
    page.locator("[title='Previous (←)']").click()
    expect(page.locator("#lightboxCounter")).to_contain_text("1 /")
    page.evaluate(
        """() => {
            window._lbPhotoW = 4000;
            window._lbPhotoH = 2000;
            window._lbRecomputeNativeZoom();
            window._lbTryApplyPendingViewportState();
        }"""
    )
    page.wait_for_function("window._lbPendingViewportState === null")
    returned_view = page.evaluate("window._lbViewportStateFromCurrent()")
    assert abs(returned_view["zoom"] - second_view["zoom"]) < 0.05
    assert abs(returned_view["centerX"] - second_view["centerX"]) < 0.03
    assert abs(returned_view["centerY"] - second_view["centerY"]) < 0.03
    assert abs(returned_view["zoom"] - first_view["zoom"]) > 0.5


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

    page.route("**/photos/*/original*", hold_next_original)

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
            window._lbCurrentSrcKey = 'original';
            window._lbApplyViewportState({
                zoom: window._lbNativeZoom,
                centerX: 0.24,
                centerY: 0.70,
                oneToOne: true,
            });
            window._lbSaveViewportState(window._lightboxCurrentId);
        }"""
    )
    page.evaluate(
        """() => {
            window.__photoChangedDuringNavigation = [];
            document.addEventListener('lightbox:photochanged', event => {
                window.__photoChangedDuringNavigation.push(event.detail.photoId);
            });
            document.getElementById('lightboxAdjustPanel').classList.add('open');
            const externalPanel = document.createElement('div');
            externalPanel.id = 'syncLightboxPanel';
            document.getElementById('lightboxOverlay').appendChild(externalPanel);
            window.__lightboxTransformCallsWhilePending = 0;
            window.__originalLightboxApplyTransform = window._lbApplyTransform;
            window._lbApplyTransform = function() {
                if (window._lbVisualTransitionPending) {
                    window.__lightboxTransformCallsWhilePending += 1;
                }
                return window.__originalLightboxApplyTransform.apply(this, arguments);
            };
            // Capture the outgoing bitmap at the actual navigation boundary.
            // A layout refresh queued during lightbox setup may legitimately
            // settle before this key event. Recording the baseline in the
            // capture phase keeps the snapshot and lightboxNav() in the same
            // browser task, so no timer can reflow the photo between them.
            window.__lightboxTransformAtNavigation = null;
            document.addEventListener('keydown', event => {
                if (event.key !== 'ArrowRight') return;
                const transform = document.getElementById('lightboxTransform');
                window.__lightboxTransformAtNavigation = {
                    cssTransform: transform.style.transform,
                    width: transform.style.width,
                    height: transform.style.height,
                    viewport: window._lbViewportStateFromCurrent(),
                };
            }, { capture: true, once: true });
        }"""
    )

    page.keyboard.press("ArrowRight")
    page.wait_for_function("() => window._lbVisualTransitionPending === true")
    before = page.evaluate("window.__lightboxTransformAtNavigation")
    assert before is not None
    page.wait_for_function("() => window._lightboxCurrentId === window.photos[1].id")
    page.wait_for_timeout(100)

    deadline = time.time() + 2
    while "route" not in held_original and time.time() < deadline:
        page.wait_for_timeout(10)
    assert "route" in held_original

    # Queue the delayed layout refresh only after navigation is definitely
    # pending. Scheduling it before the key press races with slow CI control
    # round-trips and can legitimately reflow the still-current photo before
    # the transition begins, invalidating the baseline captured above.
    page.evaluate(
        """() => {
            const wrap = document.getElementById('lightboxWrap');
            wrap.style.height = `${wrap.clientHeight - 42}px`;
        }"""
    )
    page.wait_for_timeout(150)
    assert page.evaluate("window.__lightboxTransformCallsWhilePending") == 0

    # The outgoing bitmap remains visible while the incoming original is
    # loading, so its filename and position must remain visible too.
    expect(page.locator("#lightboxFilename")).to_have_text(
        first_card.get_attribute("data-filename")
    )
    expect(page.locator("#lightboxCounter")).to_contain_text("1 /")
    expect(page.locator("#lightboxActions")).to_have_attribute("inert", "")
    expect(page.locator("#lightboxAdjustPanel")).to_have_attribute("inert", "")
    expect(page.locator("#syncLightboxPanel")).to_have_attribute("inert", "")
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
    page.evaluate("window.lightboxDelete()")
    expect(page.locator("#deleteModal")).not_to_have_class("modal-overlay open")

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
    page.evaluate(
        """() => {
            window._lbApplyTransform = window.__originalLightboxApplyTransform;
            delete window.__originalLightboxApplyTransform;
        }"""
    )

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
    assert page.evaluate(
        "!document.getElementById('syncLightboxPanel') || "
        "!document.getElementById('syncLightboxPanel').inert"
    )
    carried = page.evaluate("window._lbViewportStateFromCurrent()")
    assert abs(carried["centerX"] - before["viewport"]["centerX"]) < 0.03
    assert abs(carried["centerY"] - before["viewport"]["centerY"]) < 0.03


def test_browse_lightbox_resize_deferred_during_transition_reapplies_after_load(
    live_server, page
):
    """A resize queued while a navigation transition is pending must re-run
    once the incoming photo decodes; otherwise a DPR/viewport change made
    just before navigation would leave the incoming image on a stale source
    tier until the user triggers another resize or zoom.
    """
    first_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="4000" height="2000" '
        'viewBox="0 0 4000 2000"><rect width="4000" height="2000" fill="#274"/></svg>'
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

    page.route("**/photos/*/original*", hold_next_original)

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

    # Prime the outgoing viewport so navigation triggers a real transition.
    page.evaluate(
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
        }"""
    )

    # Instrument the deferred-refresh flush so we can verify it fires once the
    # transition ends, and instrument _lbSetZoom so we can assert the frozen
    # outgoing bitmap is never re-selected during the transition.
    page.evaluate(
        """() => {
            window.__flushCalls = 0;
            window.__originalFlush = window._lbFlushDeferredLightboxLayoutRefresh;
            window._lbFlushDeferredLightboxLayoutRefresh = function() {
                window.__flushCalls += 1;
                return window.__originalFlush.apply(this, arguments);
            };
            window.__setZoomCallsWhilePending = 0;
            window.__originalSetZoom = window._lbSetZoom;
            window._lbSetZoom = function() {
                if (window._lbVisualTransitionPending) {
                    window.__setZoomCallsWhilePending += 1;
                }
                return window.__originalSetZoom.apply(this, arguments);
            };
        }"""
    )

    # Kick off navigation and, while the transition is pending, queue a
    # resize-style refresh that requests a source-tier update. The 100ms
    # debounce timer will fire before the incoming image decodes.
    page.keyboard.press("ArrowRight")
    page.wait_for_function("() => window._lbVisualTransitionPending === true")
    page.evaluate(
        """() => {
            // Dispatch a window resize event; the lightbox resize handler
            // calls scheduleLightboxLayoutRefresh(true).
            window.dispatchEvent(new Event('resize'));
        }"""
    )

    # Let the debounce timer fire while the transition is still pending. The
    # early return preserves the deferred source-tier intent instead of
    # calling _lbSetZoom on the frozen outgoing bitmap.
    page.wait_for_timeout(200)
    assert page.evaluate("window.__setZoomCallsWhilePending") == 0
    assert page.evaluate("window.__flushCalls") == 0

    # Complete the incoming image load.
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

    # handleInitialImageLoad drains the deferred refresh via the flush, which
    # in turn reschedules the resize handler so the incoming photo picks up
    # the queued source-tier update instead of dropping it on the floor.
    page.wait_for_function(
        "() => window.__flushCalls >= 1",
        timeout=1000,
    )
    page.evaluate(
        """() => {
            window._lbSetZoom = window.__originalSetZoom;
            delete window.__originalSetZoom;
            window._lbFlushDeferredLightboxLayoutRefresh = window.__originalFlush;
            delete window.__originalFlush;
        }"""
    )


def test_browse_lightbox_mid_transition_save_keeps_navigation_handoff(
    live_server, page
):
    """A save while the outgoing bitmap is frozen keeps the handed-off viewport.

    Regression: while _lbVisualTransitionPending is true, _lightboxCurrentId
    already points at the incoming photo but the DOM transform still belongs
    to the outgoing bitmap. The save must use the pending navigation handoff,
    rather than re-reading that transitional DOM state.
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

    page.route("**/photos/*/original*", hold_next_original)

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

    # Prime a distinctive old viewport for the incoming photo. Navigation
    # must ignore it in favor of the current outgoing viewport.
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

    # The stored state for the incoming photo should be the pending handoff,
    # not the stale per-photo viewport primed above.
    stored = saved_during_transition["stored"]
    assert stored is not None
    assert abs(stored["zoom"] - outgoing["zoom"]) < 0.05
    assert abs(stored["centerX"] - outgoing["centerX"]) < 0.05
    assert abs(stored["centerY"] - outgoing["centerY"]) < 0.05
    assert abs(stored["centerX"] - intended["centerX"]) > 0.1

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

    page.route("**/photos/*/original*", hold_next_original)

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
        "**/photos/*/original*",
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
        "**/photos/*/original*",
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
    page.route("**/photos/*/original*", lambda route: route.fulfill(status=503, body="missing"))
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


@pytest.mark.parametrize("late_metadata", [False, True], ids=["early-metadata", "late-metadata"])
@pytest.mark.parametrize("fallback_fails", [False, True], ids=["fallback-loads", "fallback-fails"])
def test_browse_lightbox_restored_pending_one_to_one_waits_for_fallback_after_initial_original_fails(
    live_server, page, late_metadata, fallback_fails
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
    held_initial = {}
    page.route("**/photos/1/full", lambda route: held_initial.update(full=route))
    page.route("**/api/photos/1", lambda route: held_initial.update(metadata=route))

    def release_metadata():
        held_initial.pop("metadata").fulfill(
            json={
                "id": 1,
                "filename": "restored-pending.jpg",
                "width": 4000,
                "height": 2500,
                "flag": "none",
                "wildlife_excluded": False,
            }
        )
        page.wait_for_function("window._lbPhotoW === 4000")

    page.route("**/photos/*/original*", lambda route: route.abort())
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

    deadline = time.monotonic() + 5
    while len(held_initial) < 2 and time.monotonic() < deadline:
        page.wait_for_timeout(25)
    assert "full" in held_initial and "metadata" in held_initial
    if not late_metadata:
        release_metadata()
    held_initial.pop("full").fulfill(body=full_svg, content_type="image/svg+xml")

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

    # Metadata arriving while the sharper preview is held must not resolve
    # 1:1 from the small /full bitmap and cancel the fallback upgrade.
    if late_metadata:
        release_metadata()

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

    if fallback_fails:
        held_fallback.pop("route").abort()
        page.wait_for_function("window._lbPending1To1 === false", timeout=3000)
        assert page.evaluate("window._lbDesiredSrcKey") == "full"
        assert page.evaluate("window._lbCurrentSrcKey") == "full"
        assert page.evaluate("window._lbPending1To1Anchor") is None
        assert abs(page.evaluate("window._lbZoom") - 1) < 0.001
        expect(page.locator("#lightboxZoomBadge")).not_to_contain_text("Loading")
        return

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
        "**/photos/*/original*",
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

    page.route("**/photos/*/original*", hold_original)

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

    page.route("**/photos/*/original*", hold_original)

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

    page.route("**/photos/*/original*", hold_first_original)

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

    page.route("**/photos/*/original*", hold_first_original)

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
        "**/photos/*/original*",
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
    page.route("**/photos/*/original*", lambda route: route.abort())
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

    page.route("**/photos/*/original*", hold_original)

    url = live_server["url"]
    page.goto(f"{url}/browse")

    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    # This test needs to own the one /original request it later aborts. The
    # normal fit-view warmup can otherwise win the route race under a slow CI
    # runner, leaving the actual source-swap request to fail immediately and
    # turning the final abort into an unrelated preload failure.
    page.evaluate("window._lbScheduleOriginalPreload = function() {}")
    first_card.dblclick()

    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth === 8000;
        }"""
    )

    with page.expect_request("**/photos/*/original*"):
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
    with page.expect_event(
        "requestfailed",
        predicate=lambda request: "/original" in request.url,
    ):
        held_original.pop("route").abort()

    assert page.evaluate("window._lbOriginalUnavailable") is False


def test_browse_e_f_g_keyboard_modes(live_server, page):
    """Browse grid shortcuts open the image, request fullscreen, and return to grid."""
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
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
    page.wait_for_function(
        """() => {
            const img = document.getElementById('lightboxImg');
            return img && img.complete && img.naturalWidth > 0;
        }"""
    )

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
    page.route("**/photos/*/original*", lambda route: route.abort())
    held_fallback = {}

    def hold_fallback(route):
        if held_fallback.get("released"):
            route.fulfill(body=fallback_svg, content_type="image/svg+xml")
        elif "route" not in held_fallback:
            held_fallback["route"] = route
        else:
            route.fulfill(body=fallback_svg, content_type="image/svg+xml")

    page.route(
        "**/photos/*/preview?size=2560",
        hold_fallback,
    )
    page.route(
        "**/photos/*/preview?size=3840",
        hold_fallback,
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

    # The queued state above is intentionally transient in production. Keep
    # the fallback decode parked until after it has been observed so a fast
    # runner cannot complete the swap and clear _lbPending1To1 first.
    deadline = time.time() + 8
    while "route" not in held_fallback and time.time() < deadline:
        page.wait_for_timeout(25)
    assert "route" in held_fallback
    fallback_route = held_fallback.pop("route")
    held_fallback["released"] = True
    fallback_route.fulfill(body=fallback_svg, content_type="image/svg+xml")

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
    page.route("**/photos/*/original*", lambda route: route.abort())
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
