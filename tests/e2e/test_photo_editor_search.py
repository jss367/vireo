import re

import pytest
from playwright.sync_api import expect


def test_photo_editor_search_opens_matching_species(live_server, page):
    """Typing a species name on /edit opens a matching photo for editing."""
    url = live_server["url"]
    robin_id = live_server["data"]["photos"][3]

    page.goto(f"{url}/edit")
    expect(page.locator("#editorFilename")).to_have_text("No photo to edit")

    with page.expect_response("**/api/photos/ids?*"):
        page.locator("#editorSearchInput").fill("American Robin")

    expect(page).to_have_url(f"{url}/edit/{robin_id}")
    expect(page.locator("#editorFilename")).to_have_text("robin1.jpg")
    expect(page.locator("#editorSearchStatus")).to_have_text("1 match")


def test_photo_editor_clear_search_invalidates_pending_response(live_server, page):
    """Clearing search should ignore an older in-flight search response."""
    url = live_server["url"]
    held_routes = []

    def hold_photo_ids(route):
        held_routes.append(route)

    page.route("**/api/photos/ids?*", hold_photo_ids)
    page.goto(f"{url}/edit")

    page.locator("#editorSearchInput").fill("American Robin")
    for _ in range(20):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "search request was not issued"

    page.locator("#editorSearchInput").fill("")
    held_routes[0].continue_()

    expect(page).to_have_url(f"{url}/edit")
    expect(page.locator("#editorFilename")).to_have_text("No photo to edit")
    expect(page.locator("#editorSearchStatus")).to_have_text("")


def test_photo_editor_search_invalidates_pending_response_when_query_changes(live_server, page):
    """Typing a new query while a prior search is in flight discards the older response."""
    url = live_server["url"]
    hawk_id = live_server["data"]["photos"][0]
    held_routes = []

    def hold(route):
        held_routes.append(route)

    page.goto(f"{url}/edit/{hawk_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")

    page.route("**/api/photos/ids?*", hold)

    page.locator("#editorSearchInput").fill("American Robin")
    for _ in range(20):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "first search request was not issued"

    # Replace the query while the robin response is still held. The seq must
    # be bumped now, not only when the next debounced search fires 300ms later.
    page.locator("#editorSearchInput").fill("zzzz-no-match")

    held_routes[0].continue_()

    # Give the stale response a chance to (incorrectly) navigate or restatus.
    page.wait_for_timeout(300)

    expect(page).to_have_url(f"{url}/edit/{hawk_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")
    expect(page.locator("#editorSearchStatus")).not_to_have_text("1 match")


def test_photo_editor_search_trailing_space_applies_in_flight_response(live_server, page):
    """Whitespace-only input changes must not invalidate an in-flight search."""
    url = live_server["url"]
    robin_id = live_server["data"]["photos"][3]
    held_routes = []

    def hold(route):
        held_routes.append(route)

    page.goto(f"{url}/edit")
    page.route("**/api/photos/ids?*", hold)

    page.locator("#editorSearchInput").fill("American Robin")
    for _ in range(20):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "search request was not issued"

    # Add a trailing space while the response is held. The trimmed query is
    # unchanged, so the in-flight response must still apply — bumping the
    # seq here would strand the UI at "Searching..." because the debounced
    # replacement would early-return on the same-query check.
    page.locator("#editorSearchInput").fill("American Robin ")

    held_routes[0].continue_()

    expect(page).to_have_url(f"{url}/edit/{robin_id}")
    expect(page.locator("#editorFilename")).to_have_text("robin1.jpg")
    expect(page.locator("#editorSearchStatus")).to_have_text("1 match")


def test_photo_editor_search_confirms_dirty_edits_from_in_flight_search(live_server, page):
    """Edits made while a search is in flight must not be silently discarded."""
    url = live_server["url"]
    hawk_id = live_server["data"]["photos"][0]
    held_routes = []

    def hold(route):
        held_routes.append(route)

    page.goto(f"{url}/edit/{hawk_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")

    page.route("**/api/photos/ids?*", hold)

    page.locator("#editorSearchInput").fill("American Robin")
    for _ in range(20):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "search request was not issued"

    # Dirty the recipe while the search is in flight.
    exposure = page.locator("#exposureRange")
    exposure.evaluate("(el) => { el.value = '1.2'; el.dispatchEvent(new Event('input')); }")

    dialogs = []

    def on_dialog(dialog):
        dialogs.append(dialog.message)
        dialog.dismiss()

    page.once("dialog", on_dialog)

    held_routes[0].continue_()

    # The confirm must be shown, and dismissing it keeps us on the current
    # (dirty) photo instead of navigating to a search match.
    for _ in range(20):
        if dialogs:
            break
        page.wait_for_timeout(100)
    assert dialogs, "expected a discard confirmation for dirty in-flight edits"
    assert "Discard" in dialogs[0]
    expect(page).to_have_url(f"{url}/edit/{hawk_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")
    # Dismissing the discard prompt must leave the editor visibly untouched:
    # the "N matches" status and the nav position/list must NOT already
    # describe the search target, since we haven't navigated to it.
    expect(page.locator("#editorSearchStatus")).not_to_have_text("1 match")


def test_photo_editor_search_revert_to_in_flight_query_refetches(live_server, page):
    """Reverting to the in-flight query after an intermediate keystroke must refetch."""
    url = live_server["url"]
    robin_id = live_server["data"]["photos"][3]
    held_routes = []

    def hold(route):
        held_routes.append(route)

    page.goto(f"{url}/edit")
    page.route("**/api/photos/ids?*", hold)

    # First search fires the debounce and its response is held.
    page.locator("#editorSearchInput").fill("American Robin")
    for _ in range(20):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "first search request was not issued"

    # Type a different query — this bumps the seq and invalidates the held
    # response — then quickly revert to the original query before the 300ms
    # debounce fires. Without the revert-refetch fix, scheduleEditorSearch
    # would early-return on the same-query check and no replacement fetch
    # would be dispatched, leaving the UI stranded at "Searching...".
    page.locator("#editorSearchInput").fill("American Robins")
    page.locator("#editorSearchInput").fill("American Robin")

    # Wait for a second (replacement) fetch to be issued.
    for _ in range(20):
        if len(held_routes) >= 2:
            break
        page.wait_for_timeout(100)
    assert len(held_routes) >= 2, "replacement search after revert was not issued"

    # Release both responses. The first (invalidated) must be ignored; the
    # replacement must apply and navigate to the robin.
    held_routes[0].continue_()
    held_routes[1].continue_()

    expect(page).to_have_url(f"{url}/edit/{robin_id}")
    expect(page.locator("#editorFilename")).to_have_text("robin1.jpg")
    expect(page.locator("#editorSearchStatus")).to_have_text("1 match")


def test_photo_editor_100_percent_keeps_edits_panel_scrollable(live_server, page):
    """A native-size image must scroll inside the stage, not grow the grid row."""
    url = live_server["url"]
    hawk_id = live_server["data"]["photos"][0]
    preview_svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' width='6000' height='4000'>"
        "<rect width='6000' height='4000' fill='green'/>"
        "</svg>"
    )

    page.set_viewport_size({"width": 1400, "height": 800})
    page.route(
        "**/photos/*/edit-preview**",
        lambda route: route.fulfill(
            status=200,
            content_type="image/svg+xml",
            body=preview_svg,
        ),
    )
    page.goto(f"{url}/edit/{hawk_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")
    page.wait_for_function(
        "() => document.getElementById('editorImg').naturalWidth > 0"
    )

    page.locator("#actualBtn").click()
    page.wait_for_function(
        "() => document.getElementById('editorCanvasWrap')"
        ".classList.contains('zoom-actual')"
    )

    layout = page.evaluate(
        """() => {
            const shell = document.querySelector('.editor-shell');
            const panel = document.querySelector('.editor-panel');
            const canvas = document.querySelector('.editor-canvas-wrap');
            return {
                shellClientHeight: shell.clientHeight,
                shellScrollHeight: shell.scrollHeight,
                panelClientHeight: panel.clientHeight,
                panelScrollHeight: panel.scrollHeight,
                canvasClientHeight: canvas.clientHeight,
                canvasScrollHeight: canvas.scrollHeight,
            };
        }"""
    )

    assert layout["shellScrollHeight"] == layout["shellClientHeight"]
    assert layout["panelScrollHeight"] > layout["panelClientHeight"]
    assert layout["canvasScrollHeight"] > layout["canvasClientHeight"]


def test_photo_editor_crop_lock_keeps_current_aspect(live_server, page):
    """The crop ratio lock should preserve the current crop aspect while resizing."""
    url = live_server["url"]
    hawk_id = live_server["data"]["photos"][0]
    preview_svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' width='400' height='400'>"
        "<rect width='400' height='400' fill='green'/>"
        "</svg>"
    )

    page.route(
        "**/photos/*/edit-preview**",
        lambda route: route.fulfill(
            status=200,
            content_type="image/svg+xml",
            body=preview_svg,
        ),
    )
    page.goto(f"{url}/edit/{hawk_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")
    page.wait_for_function(
        "() => document.getElementById('editorImg').naturalWidth > 0"
    )

    page.evaluate(
        """() => {
            setCropField('x', '10');
            setCropField('y', '10');
            setCropField('w', '60');
            setCropField('h', '40');
        }"""
    )
    page.locator("#aspectLockBtn").click()
    assert "active" in (page.locator("#aspectLockBtn").get_attribute("class") or "")

    def crop_aspect():
        return page.evaluate(
            """() => {
                const crop = editorState.recipe.crop;
                const img = document.getElementById('editorImg');
                return (crop.w * img.clientWidth) / (crop.h * img.clientHeight);
            }"""
        )

    before = crop_aspect()
    handle = page.locator(".crop-handle.se").bounding_box()
    assert handle is not None
    page.mouse.move(handle["x"] + handle["width"] / 2, handle["y"] + handle["height"] / 2)
    page.mouse.down()
    page.mouse.move(
        handle["x"] + handle["width"] / 2 + 24,
        handle["y"] + handle["height"] / 2,
        steps=4,
    )
    page.mouse.up()

    after = crop_aspect()
    assert abs(after - before) < 0.001


def test_photo_editor_aspect_uses_current_crop(live_server, page):
    """A ratio preset should refine the latest crop instead of resetting it."""
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]
    preview_svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' width='400' height='400'>"
        "<rect width='400' height='400' fill='green'/>"
        "</svg>"
    )

    page.route(
        "**/photos/*/edit-preview**",
        lambda route: route.fulfill(
            status=200,
            content_type="image/svg+xml",
            body=preview_svg,
        ),
    )
    page.goto(f"{url}/edit/{photo_id}")
    page.wait_for_function(
        "() => document.getElementById('editorImg').naturalWidth > 0"
    )

    # Model the crop left by the user's most recent corner drag.
    page.evaluate(
        """() => {
            editorState.recipe.crop = {x: 0.1, y: 0.2, w: 0.6, h: 0.5};
            editorState.cropEditing = true;
            renderCropBox();
        }"""
    )
    page.locator("#aspect32Btn").click()

    crop = page.evaluate("() => editorState.recipe.crop")
    assert crop["x"] == pytest.approx(0.1)
    assert crop["y"] == pytest.approx(0.25)
    assert crop["w"] == pytest.approx(0.6)
    assert crop["h"] == pytest.approx(0.4)
    assert (crop["w"] / crop["h"]) == pytest.approx(1.5)

    minimum_crops = page.evaluate(
        """() => ({
            editor: cropBoxForAspect(
                {x: 0.1, y: 0.2, w: 0.02, h: 0.5}, 1.5
            ),
            shared: _cropBoxForAspect(
                {x: 0.1, y: 0.2, w: 0.02, h: 0.5}, 1.5
            ),
        })"""
    )
    for minimum_crop in minimum_crops.values():
        assert minimum_crop["w"] >= 0.02
        assert minimum_crop["h"] >= 0.02
        assert (minimum_crop["w"] / minimum_crop["h"]) == pytest.approx(1.5)


def test_photo_editor_remembers_crop_ratio_across_photos_and_reload(live_server, page):
    """Opting in restores the resizing lock without changing the next photo."""
    url = live_server["url"]
    first_id, second_id = live_server["data"]["photos"][:2]
    page.route(
        "**/photos/*/edit-preview**",
        lambda route: route.fulfill(
            content_type="image/svg+xml",
            body="<svg xmlns='http://www.w3.org/2000/svg' width='400' height='400'/>",
        ),
    )
    page.goto(f"{url}/edit/{first_id}")
    page.wait_for_function("() => document.getElementById('editorImg').naturalWidth > 0")
    remember = page.get_by_label("Remember crop ratio")
    expect(remember).not_to_be_checked()
    page.locator("#aspect32Btn").click()
    remember.check()

    # Navigation inside the editor must restore the preference, too.
    page.evaluate("photoId => loadPhoto(photoId)", second_id)
    expect(page.locator("#editorFilename")).to_have_text("hawk2.jpg")
    expect(page.locator("#aspect32Btn")).to_have_class(re.compile(r"\bactive\b"))
    expect(page.locator("#saveBtn")).to_be_disabled()
    assert page.evaluate("() => editorState.recipe.crop") == {"x": 0, "y": 0, "w": 1, "h": 1}

    page.evaluate("() => cropRatioSave")
    page.reload()
    page.wait_for_function("() => !editorState.loading && document.getElementById('editorImg').naturalWidth > 0")
    expect(remember).to_be_checked()
    expect(page.locator("#aspectLockBtn")).to_have_class(re.compile(r"\bactive\b"))
    page.locator("#cropW").fill("60")
    page.locator("#cropW").press("Tab")
    assert page.evaluate("() => currentCropAspect()") == pytest.approx(1.5)

    # Subsequent choices replace the remembered ratio, including custom locks.
    page.locator("#aspect43Btn").click()
    page.evaluate("() => cropRatioSave")
    page.reload()
    expect(page.locator("#aspect43Btn")).to_have_class(re.compile(r"\bactive\b"))
    page.wait_for_function("() => !editorState.loading && document.getElementById('editorImg').naturalWidth > 0")
    page.locator("#aspectLockBtn").click()
    page.locator("#cropW").fill("70")
    page.locator("#cropW").press("Tab")
    page.locator("#aspectLockBtn").click()
    custom_aspect = page.evaluate("() => currentCropAspect()")
    page.evaluate("() => cropRatioSave")
    page.reload()
    expect(page.locator("#editorFilename")).to_have_text("hawk2.jpg")
    assert page.evaluate("() => editorState.cropAspect") == pytest.approx(custom_aspect)

    # Explicitly unlocking is remembered; opting out keeps this photo's lock
    # but makes subsequent visits start unlocked again.
    page.locator("#aspectLockBtn").click()
    page.evaluate("() => cropRatioSave")
    page.reload()
    expect(page.locator("#editorFilename")).to_have_text("hawk2.jpg")
    expect(remember).to_be_checked()
    expect(page.locator("#aspectLockBtn")).not_to_have_class(re.compile(r"\bactive\b"))
    page.wait_for_function("() => document.getElementById('editorImg').naturalWidth > 0")
    page.locator("#aspect11Btn").click()
    remember.uncheck()
    expect(page.locator("#aspect11Btn")).to_have_class(re.compile(r"\bactive\b"))
    page.evaluate("() => cropRatioSave")
    page.reload()
    expect(page.locator("#editorFilename")).to_have_text("hawk2.jpg")
    expect(remember).not_to_be_checked()
    expect(page.locator("#aspectLockBtn")).not_to_have_class(re.compile(r"\bactive\b"))


def test_photo_editor_issues_latest_ratio_while_previous_save_is_pending(live_server, page):
    """A slow preference save cannot strand newer choices in a JS callback."""
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]
    page.route(
        "**/photos/*/edit-preview**",
        lambda route: route.fulfill(
            content_type="image/svg+xml",
            body="<svg xmlns='http://www.w3.org/2000/svg' width='400' height='400'/>",
        ),
    )
    page.goto(f"{url}/edit/{photo_id}")
    page.wait_for_function("() => document.getElementById('editorImg').naturalWidth > 0")
    pending = []

    def hold_first_save(route):
        if route.request.method == "PUT" and not pending:
            pending.append(route)
        else:
            route.continue_()

    page.route("**/api/editor/crop-ratio", hold_first_save)
    with page.expect_request(lambda request: request.method == "PUT"):
        page.get_by_label("Remember crop ratio").check()
    # The first PUT has not reached the server. The second must start now,
    # so keepalive can finish it even if the page closes immediately.
    with page.expect_response(lambda response: response.request.method == "PUT"):
        page.locator("#aspect32Btn").click()
    assert pending
    pending[0].continue_()
    page.evaluate("() => cropRatioSave")
    page.reload()
    expect(page.locator("#aspect32Btn")).to_have_class(re.compile(r"\bactive\b"))
    expect(page.get_by_label("Remember crop ratio")).to_be_checked()


@pytest.mark.parametrize("enabled", [True, False])
def test_photo_editor_restores_pending_ratio_on_immediate_reload(live_server, page, enabled):
    """Reload restores the latest choice even before its PUT reaches the server."""
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]
    page.route(
        "**/photos/*/edit-preview**",
        lambda route: route.fulfill(
            content_type="image/svg+xml",
            body="<svg xmlns='http://www.w3.org/2000/svg' width='400' height='400'/>",
        ),
    )
    if not enabled:
        assert page.request.put(
            f"{url}/api/editor/crop-ratio", data={"enabled": True, "aspect": 1.5},
        ).ok
    page.goto(f"{url}/edit/{photo_id}")
    page.wait_for_function("() => document.getElementById('editorImg').naturalWidth > 0")
    if enabled:
        page.locator("#aspect32Btn").click()
    pending = []

    def hold_first_save(route):
        if route.request.method == "PUT" and not pending:
            pending.append(route)
        else:
            route.continue_()

    page.route("**/api/editor/crop-ratio", hold_first_save)
    with page.expect_request(lambda request: request.method == "PUT"):
        page.get_by_label("Remember crop ratio").set_checked(enabled)
    # Deliberately reload without waiting for cropRatioSave. The fresh GET
    # returns the old server value; the pending local choice must win.
    page.reload()
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")
    expect(page.get_by_label("Remember crop ratio")).to_be_checked(checked=enabled)
    if enabled:
        expect(page.locator("#aspect32Btn")).to_have_class(re.compile(r"\bactive\b"))
    else:
        expect(page.locator("#aspectLockBtn")).not_to_have_class(re.compile(r"\bactive\b"))
    page.evaluate("() => cropRatioSave")
    saved = page.request.get(f"{url}/api/editor/crop-ratio").json()
    assert saved["enabled"] == enabled
    assert saved["aspect"] == (1.5 if enabled else None)


def test_photo_editor_remembered_ratio_preserves_saved_crop(live_server, page):
    """A remembered ratio must not recrop an existing edit just by opening it."""
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]
    saved_crop = {"x": 0.1, "y": 0.2, "w": 0.5, "h": 0.5}

    def photo_with_crop(route):
        response = route.fetch()
        photo = response.json()
        photo["edit_recipe"] = {"crop": saved_crop}
        route.fulfill(response=response, json=photo)

    page.route(f"**/api/photos/{photo_id}", photo_with_crop)
    response = page.request.put(
        f"{url}/api/editor/crop-ratio", data={"enabled": True, "aspect": 1.5},
    )
    assert response.ok
    page.goto(f"{url}/edit/{photo_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")
    expect(page.locator("#aspect32Btn")).to_have_class(re.compile(r"\bactive\b"))
    expect(page.locator("#saveBtn")).to_be_disabled()
    assert page.evaluate("() => editorState.recipe.crop") == saved_crop


def test_photo_editor_continuous_zoom_has_fit_and_native_stops(live_server, page):
    """The editor zoom slider scales continuously and keeps exact Fit/100% actions."""
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]
    preview_svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' width='4000' height='3000'>"
        "<rect width='4000' height='3000' fill='green'/></svg>"
    )

    page.route(
        "**/photos/*/edit-preview**",
        lambda route: route.fulfill(
            status=200,
            content_type="image/svg+xml",
            body=preview_svg,
        ),
    )
    page.set_viewport_size({"width": 2000, "height": 1000})
    page.goto(f"{url}/edit/{photo_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")
    page.wait_for_function(
        "() => document.getElementById('editorImg').naturalWidth > 0"
    )

    # Make the source dimensions deterministic independently of the fixture.
    page.evaluate(
        """() => {
            editorState.photo.width = 4000;
            editorState.photo.height = 3000;
            applyEditorZoom();
            updateEditorZoomControl();
        }"""
    )

    incremental_slider = page.evaluate(
        """() => {
            const originalSchedule = window.schedulePreview;
            let calls = 0;
            window.schedulePreview = () => { calls += 1; };
            editorState.photo.width = 20000;
            editorState.photo.height = 15000;
            editorState.zoomMode = 'fit';
            setEditorZoomFromSlider(1);
            const first = editorState.zoomPercent;
            setEditorZoomFromSlider(2);
            const second = editorState.zoomPercent;
            window.schedulePreview = originalSchedule;
            editorState.photo.width = 4000;
            editorState.photo.height = 3000;
            editorState.zoomMode = 'fit';
            applyEditorZoom();
            updateEditorZoomControl();
            return {calls, delta: second - first};
        }"""
    )
    assert 0 < incremental_slider["delta"] < 0.05
    assert incremental_slider["calls"] == 2

    capped_zoom = page.evaluate(
        """() => {
            const img = document.getElementById('editorImg');
            const originalCurrentSrc = Object.getOwnPropertyDescriptor(img, 'currentSrc');
            const originalNaturalWidth = Object.getOwnPropertyDescriptor(img, 'naturalWidth');
            const originalNaturalHeight = Object.getOwnPropertyDescriptor(img, 'naturalHeight');
            let loadedSize = 16128;
            Object.defineProperty(img, 'currentSrc', {
                configurable: true,
                get: () => '/photos/' + editorState.photoId +
                    '/edit-preview?size=' + loadedSize + '&apply_crop=0&recipe=' +
                    encodeURIComponent(JSON.stringify(previewRecipeFor(editorState.recipe)))
            });
            Object.defineProperty(img, 'naturalWidth', {
                configurable: true,
                get: () => loadedSize
            });
            Object.defineProperty(img, 'naturalHeight', {
                configurable: true,
                get: () => loadedSize * 2 / 3
            });
            editorState.photo.width = 30000;
            editorState.photo.height = 20000;
            editorState.zoomMode = 'custom';
            editorState.zoomPercent = 54;
            const before = editorNativeDisplayDimensions().width * 0.54;
            loadedSize = 16384;
            editorState.zoomPercent = 55;
            const after = editorNativeDisplayDimensions().width * 0.55;
            editorState.zoomPercent = 90;
            const later = editorNativeDisplayDimensions().width * 0.90;
            for (const [name, descriptor] of [
                ['currentSrc', originalCurrentSrc],
                ['naturalWidth', originalNaturalWidth],
                ['naturalHeight', originalNaturalHeight]
            ]) {
                if (descriptor) Object.defineProperty(img, name, descriptor);
                else delete img[name];
            }
            editorState.photo.width = 4000;
            editorState.photo.height = 3000;
            editorState.zoomMode = 'fit';
            applyEditorZoom();
            updateEditorZoomControl();
            return {before, after, later};
        }"""
    )
    assert capped_zoom["after"] >= capped_zoom["before"]
    assert capped_zoom["later"] > capped_zoom["after"]

    stale_ratio = page.evaluate(
        """() => {
            const originalRecipe = cloneRecipe(editorState.recipe);
            editorState.recipe.rotation = 90;
            const dims = editorNativeDisplayDimensions();
            editorState.recipe = originalRecipe;
            return dims;
        }"""
    )
    assert stale_ratio == {"width": 3000, "height": 4000}

    exif_oriented_crop = page.evaluate(
        """() => {
            const originalPhoto = cloneRecipe(editorState.photo);
            const originalRecipe = cloneRecipe(editorState.recipe);
            const originalCropEditing = editorState.cropEditing;
            const originalZoomMode = editorState.zoomMode;
            const originalZoomPercent = editorState.zoomPercent;
            editorState.photo.width = 6000;
            editorState.photo.height = 4000;
            editorState.photo.metadata = {EXIF: {Orientation: 6}};
            editorState.recipe = {
                crop: {x: 0, y: 0, w: 0.25, h: 1}
            };
            editorState.cropEditing = false;
            editorState.zoomMode = 'custom';
            editorState.zoomPercent = 100;
            const dims = editorNativeDisplayDimensions();
            const renderSize = previewRenderSize();
            editorState.photo = originalPhoto;
            editorState.recipe = originalRecipe;
            editorState.cropEditing = originalCropEditing;
            editorState.zoomMode = originalZoomMode;
            editorState.zoomPercent = originalZoomPercent;
            return {dims, renderSize};
        }"""
    )
    assert exif_oriented_crop == {
        "dims": {"width": 1000, "height": 6000},
        "renderSize": 6000,
    }

    fractional_crop_size = page.evaluate(
        """() => {
            const originalPhoto = cloneRecipe(editorState.photo);
            const originalRecipe = cloneRecipe(editorState.recipe);
            const originalCropEditing = editorState.cropEditing;
            const originalZoomMode = editorState.zoomMode;
            const originalZoomPercent = editorState.zoomPercent;
            editorState.photo.width = 800;
            editorState.photo.height = 600;
            editorState.photo.metadata = null;
            editorState.recipe = {
                crop: {x: 0, y: 0, w: 0.333, h: 0.333}
            };
            editorState.cropEditing = false;
            editorState.zoomMode = 'custom';
            editorState.zoomPercent = 100;
            const renderSize = previewRenderSize();
            editorState.photo = originalPhoto;
            editorState.recipe = originalRecipe;
            editorState.cropEditing = originalCropEditing;
            editorState.zoomMode = originalZoomMode;
            editorState.zoomPercent = originalZoomPercent;
            return renderSize;
        }"""
    )
    assert fractional_crop_size == 267

    page.set_viewport_size({"width": 5000, "height": 4000})
    page.wait_for_function(
        "() => document.getElementById('editorImg').src.includes('size=4000')"
    )
    native_stop = page.evaluate(
        """() => {
            updateEditorZoomControl();
            const result = {
                fit: editorFitZoomPercent(),
                renderSize: previewRenderSize(),
                actualDisplay: document.getElementById('actualBtn').style.display,
                fitLabel: document.getElementById('fitBtn').textContent
            };
            applyEditorZoom();
            updateEditorZoomControl();
            return result;
        }"""
    )
    near_native_stop = page.evaluate(
        """() => {
            const wrap = document.getElementById('editorCanvasWrap');
            const style = getComputedStyle(wrap);
            const availableW = wrap.clientWidth - parseFloat(style.paddingLeft) -
                parseFloat(style.paddingRight);
            editorState.photo.width = availableW / 0.9975;
            editorState.photo.height = editorState.photo.width * 0.75;
            updateEditorZoomControl();
            const result = {
                fit: editorFitZoomPercent(),
                actualDisplay: document.getElementById('actualBtn').style.display
            };
            editorState.photo.width = 4000;
            editorState.photo.height = 3000;
            updateEditorZoomControl();
            return result;
        }"""
    )
    page.set_viewport_size({"width": 2000, "height": 1000})
    assert native_stop["fit"] == 100
    assert native_stop["renderSize"] == 4000
    assert native_stop["actualDisplay"] == "none"
    assert native_stop["fitLabel"] == "Fit · 100%"
    assert 99.5 < near_native_stop["fit"] < 100
    assert near_native_stop["actualDisplay"] != "none"

    one_axis = page.evaluate(
        """() => {
            const wrap = document.getElementById('editorCanvasWrap');
            const style = getComputedStyle(wrap);
            const availableW = wrap.clientWidth - parseFloat(style.paddingLeft) -
                parseFloat(style.paddingRight);
            const availableH = wrap.clientHeight - parseFloat(style.paddingTop) -
                parseFloat(style.paddingBottom);
            const dims = editorNativeDisplayDimensions();
            const fitX = availableW / dims.width * 100;
            const fitY = availableH / dims.height * 100;
            setEditorZoom((Math.min(fitX, fitY) + Math.max(fitX, fitY)) / 2);
            const img = document.getElementById('editorImg').getBoundingClientRect();
            const viewport = wrap.getBoundingClientRect();
            const overflowX = img.width > availableW;
            const overflowY = img.height > availableH;
            const fittedAxisError = overflowX
                ? Math.abs(img.top + img.height / 2 - (viewport.top + wrap.clientHeight / 2))
                : Math.abs(img.left + img.width / 2 - (viewport.left + wrap.clientWidth / 2));
            return {overflowX, overflowY, fittedAxisError};
        }"""
    )
    assert one_axis["overflowX"] != one_axis["overflowY"]
    assert one_axis["fittedAxisError"] < 1

    page.evaluate(
        """() => {
            editorState.zoomMode = 'custom';
            editorState.zoomPercent = editorFitZoomPercent();
        }"""
    )
    page.set_viewport_size({"width": 3000, "height": 2000})
    page.wait_for_function(
        """() => editorState.zoomMode === 'fit' &&
            Math.abs(editorState.zoomPercent - editorFitZoomPercent()) < 0.01"""
    )
    resize_state = page.evaluate(
        """() => {
            editorState.zoomMode = 'custom';
            editorState.zoomPercent = editorFitZoomPercent();
            window.dispatchEvent(new Event('resize'));
            return {
                mode: editorState.zoomMode,
                after: editorState.zoomPercent,
                fit: editorFitZoomPercent()
            };
        }"""
    )
    # A resize that raises Fit above the retained percentage switches to Fit;
    # an exact custom selection remains custom on a subsequent resize.
    assert resize_state["mode"] == "custom"
    assert abs(resize_state["after"] - resize_state["fit"]) < 0.01
    page.set_viewport_size({"width": 2000, "height": 1000})

    page.locator("#fitBtn").click()
    page.locator("#actualBtn").click()
    expect(page.locator("#editorZoomSlider")).to_have_attribute(
        "aria-valuetext", "100%"
    )
    page.wait_for_function(
        "() => document.getElementById('editorImg').src.includes('size=4000')"
    )
    actual = page.evaluate(
        """() => ({
            mode: editorState.zoomMode,
            width: document.getElementById('editorImg').clientWidth,
            zoomed: document.getElementById('editorCanvasWrap')
                .classList.contains('zoom-custom'),
            focusX: (() => {
                const img = document.getElementById('editorImg').getBoundingClientRect();
                const wrap = document.getElementById('editorCanvasWrap');
                const viewport = wrap.getBoundingClientRect();
                return (viewport.left + wrap.clientWidth / 2 - img.left) / img.width;
            })()
        })"""
    )
    assert actual["mode"] == "custom"
    assert actual["width"] == 4000
    assert actual["zoomed"] is True
    assert abs(actual["focusX"] - 0.5) < 0.01

    page.evaluate(
        """() => {
            const slider = document.getElementById('editorZoomSlider');
            slider.value = String(Math.round(editorZoomSliderPosition(50)));
            slider.dispatchEvent(new Event('input', {bubbles: true}));
        }"""
    )
    page.wait_for_function("() => Math.abs(editorState.zoomPercent - 50) < 0.5")
    page.wait_for_function(
        "() => document.getElementById('editorImg').src.includes('size=2048')"
    )
    intermediate_width = page.evaluate(
        "() => document.getElementById('editorImg').clientWidth"
    )
    assert abs(intermediate_width - 2000) <= 4

    page.locator("#fitBtn").click()
    expect(page.locator("#editorZoomSlider")).to_have_attribute(
        "aria-valuetext", re.compile(r"Fit \(\d+%\)")
    )
    fit = page.evaluate(
        """() => ({
            mode: editorState.zoomMode,
            width: document.getElementById('editorImg').clientWidth,
            zoomed: document.getElementById('editorCanvasWrap')
                .classList.contains('zoom-custom')
        })"""
    )
    assert fit["mode"] == "fit"
    assert fit["width"] > 0
    assert fit["zoomed"] is False

    loaded_geometry_refresh = page.evaluate(
        """() => new Promise((resolve) => {
            const img = document.getElementById('editorImg');
            const originalControl = window.updateEditorZoomControl;
            const originalRenderSize = window.previewRenderSize;
            const originalSchedule = window.schedulePreview;
            let calls = 0;
            let renderSizeCalls = 0;
            let scheduled = 0;
            window.updateEditorZoomControl = function() {
                calls += 1;
                return originalControl();
            };
            window.previewRenderSize = function() {
                renderSizeCalls += 1;
                return renderSizeCalls === 1 ? 2048 : 4096;
            };
            window.schedulePreview = function() { scheduled += 1; };
            editorState.recipe.exposure += 0.013;
            updatePreview();
            const beforeLoad = calls;
            const originalLoad = img.onload;
            img.onload = function() {
                originalLoad.call(this);
                const afterLoad = calls;
                window.updateEditorZoomControl = originalControl;
                window.previewRenderSize = originalRenderSize;
                window.schedulePreview = originalSchedule;
                resolve({beforeLoad, afterLoad, scheduled});
            };
        })"""
    )
    assert loaded_geometry_refresh["afterLoad"] > loaded_geometry_refresh["beforeLoad"]
    assert loaded_geometry_refresh["scheduled"] == 1


def test_photo_editor_enter_saves_crop_after_drag_from_focused_input(
    live_server, page
):
    """Dragging a crop handle should give Enter back to the save shortcut."""
    url = live_server["url"]
    hawk_id = live_server["data"]["photos"][0]
    preview_svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' width='400' height='400'>"
        "<rect width='400' height='400' fill='green'/>"
        "</svg>"
    )

    page.route(
        "**/photos/*/edit-preview**",
        lambda route: route.fulfill(
            status=200,
            content_type="image/svg+xml",
            body=preview_svg,
        ),
    )
    page.goto(f"{url}/edit/{hawk_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")
    page.wait_for_function(
        "() => document.getElementById('editorImg').naturalWidth > 0"
    )

    # A prior interaction can leave an input focused. Because the crop drag
    # prevents pointerdown's default action, the browser will not blur that
    # input unless the crop surface explicitly takes focus.
    page.locator("#cropW").focus()
    handle = page.locator(".crop-handle.se").bounding_box()
    assert handle is not None
    page.mouse.move(
        handle["x"] + handle["width"] / 2,
        handle["y"] + handle["height"] / 2,
    )
    page.mouse.down()
    page.mouse.move(
        handle["x"] + handle["width"] / 2 - 30,
        handle["y"] + handle["height"] / 2 - 20,
        steps=4,
    )
    page.mouse.up()

    expect(page.locator("#saveBtn")).to_be_enabled()
    assert page.evaluate("() => document.activeElement.id") == "editorCropBox"
    with page.expect_response(
        f"**/api/photos/{hawk_id}/edit-recipe"
    ) as response:
        page.keyboard.press("Enter")
    assert response.value.request.method == "PUT"
    assert response.value.status == 200
    expect(page.locator("#saveBtn")).to_be_disabled()
    page.wait_for_function(
        "() => document.getElementById('editorImg').src.includes('apply_crop=1')"
    )
    committed = page.evaluate(
        """() => ({
            cropEditing: editorState.cropEditing,
            zoomMode: editorState.zoomMode,
            cropVisible: getComputedStyle(
                document.getElementById('editorCropBox')
            ).display !== 'none'
        })"""
    )
    assert committed == {
        "cropEditing": False,
        "zoomMode": "fit",
        "cropVisible": False,
    }

    recipe = page.evaluate(
        """async (photoId) => {
            const response = await fetch('/api/photos/' + photoId + '/edit-recipe');
            return (await response.json()).recipe;
        }""",
        hawk_id,
    )
    assert recipe["crop"]["w"] < 1
    assert recipe["crop"]["h"] < 1

    # Saving an unrelated adjustment on an already-cropped photo must retain
    # the user's zoom; Fit is only selected when an active crop is accepted.
    page.evaluate(
        """() => {
            editorState.zoomMode = 'custom';
            editorState.zoomPercent = 100;
            applyEditorZoom();
            updateEditorZoomControl();
        }"""
    )
    page.locator("#exposureRange").evaluate(
        """el => {
            el.value = '0.5';
            el.dispatchEvent(new Event('input', {bubbles: true}));
        }"""
    )
    expect(page.locator("#saveBtn")).to_be_enabled()
    with page.expect_response(
        f"**/api/photos/{hawk_id}/edit-recipe"
    ) as adjustment_response:
        page.locator("#saveBtn").click()
    assert adjustment_response.value.status == 200
    expect(page.locator("#saveBtn")).to_be_disabled()
    assert page.evaluate("() => editorState.zoomMode") == "custom"
    assert page.evaluate("() => editorState.zoomPercent") == 100

    page.locator("#editCropBtn").click()
    page.wait_for_function(
        "() => document.getElementById('editorImg').src.includes('apply_crop=0')"
    )
    expect(page.locator("#editorCropBox")).to_be_visible()

    # Accepting an unchanged saved crop exits crop editing without creating a
    # redundant history entry or requiring the disabled Save button.
    page.keyboard.press("Enter")
    page.wait_for_function(
        "() => document.getElementById('editorImg').src.includes('apply_crop=1')"
    )
    expect(page.locator("#editorCropBox")).to_be_hidden()

    crop_width = page.locator("#cropW")
    crop_width.focus()
    crop_width.fill("60")
    with page.expect_response(
        f"**/api/photos/{hawk_id}/edit-recipe"
    ) as numeric_response:
        page.keyboard.press("Enter")
    assert numeric_response.value.request.method == "PUT"
    assert numeric_response.value.status == 200
    page.wait_for_function(
        "() => document.getElementById('editorImg').src.includes('apply_crop=1')"
    )
    expect(page.locator("#editorCropBox")).to_be_hidden()
    assert page.evaluate("() => editorState.recipe.crop.w") == 0.6
