"""Lens controls survive save/reload, scoped resets and undo, with real previews."""

import io
import json

import numpy as np
import pytest
from PIL import Image
from playwright.sync_api import expect
from services import photo_ingest, workspace_folders


@pytest.fixture
def optics_photo(live_server, tmp_path):
    folder = tmp_path / "lens-photos"
    folder.mkdir()
    path = folder / "branches-against-sky.png"
    pixels = np.full((400, 600, 3), 220, dtype=np.uint8)
    # Dark branch and a thin purple fringe against a bright sky.
    pixels[:, 290:310] = [50, 45, 40]
    pixels[:, 310:312] = [185, 80, 220]
    for y in (70, 200, 330):
        pixels[y:y + 3] = [80, 85, 90]
    Image.fromarray(pixels).save(path)
    db = live_server["db"]
    fid = workspace_folders.add_folder(db, str(folder))
    return photo_ingest.add_photo(
        db, folder_id=fid, filename=path.name, extension=".png",
        file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
        width=600, height=400,
    )


def test_lens_controls_save_reload_reset_undo_and_server_preview(page, live_server, optics_photo):
    page.goto(f"{live_server['url']}/edit/{optics_photo}")
    page.wait_for_function('!editorState.loading && document.getElementById("editorImg").naturalWidth > 0')
    page.locator("#opticsBand").scroll_into_view_if_needed()
    expect(page.locator("#opticsBand")).to_contain_text("Matched profiles apply to decoded RAW originals")
    settings = {"distortion": 45, "vignetting": 30, "defringe_purple": 80, "defringe_green": 20}
    for key, value in settings.items():
        page.locator(f"#{key}Range").fill(str(value))
        page.locator(f"#{key}Range").dispatch_event("input")
        expect(page.locator(f"#{key}Value")).to_have_text(str(value))
    page.wait_for_function('editorState.previewTimings.some(t => !t.interactive)')
    assert page.evaluate("recipeForSave(editorState.recipe).adjustments") == settings
    assert page.evaluate("saveRecipe()") is True
    page.reload()
    page.wait_for_function('!editorState.loading && document.getElementById("editorImg").naturalWidth > 0')
    for key, value in settings.items():
        expect(page.locator(f"#{key}Range")).to_have_value(str(value))
    expect(page.locator("#saveBtn")).to_be_disabled()
    saved = live_server["db"].get_photo_edit_recipe(optics_photo)
    assert saved["adjustments"] == settings
    # Compare endpoint pixels against the shared full-resolution renderer.
    from export import load_export_image
    photo = live_server["db"].photos.get(optics_photo)
    folder = live_server["db"].folders.get(photo["folder_id"])
    expected = load_export_image(photo, str(live_server["app"].config["THUMB_CACHE_DIR"]), {photo["folder_id"]: folder["path"]}, recipe=saved)
    encoded = io.BytesIO()
    expected.save(encoded, format="JPEG", quality=90)
    response = page.request.get(
        f"{live_server['url']}/photos/{optics_photo}/edit-preview",
        params={"size": 600, "recipe": json.dumps(saved)},
    )
    assert response.ok
    assert response.body() == encoded.getvalue()
    # Basic/Detail resets retain lens settings; the lens reset is independent.
    page.evaluate("resetAdjustments(); resetDetail()")
    assert page.evaluate("recipeForSave(editorState.recipe).adjustments") == settings
    page.locator("#opticsBand button").click()
    assert page.evaluate("recipeForSave(editorState.recipe).adjustments || null") is None
    assert page.evaluate("doUndo()") is True
    assert page.evaluate("recipeForSave(editorState.recipe).adjustments") == settings
    page.wait_for_function('editorImageMatchesZoomRecipe(document.getElementById("editorImg"))')


@pytest.mark.parametrize("amount", [-100, 80])
def test_brush_inverse_lens_map_matches_renderer(page, live_server, optics_photo, amount):
    page.goto(f"{live_server['url']}/edit/{optics_photo}")
    page.wait_for_function('!editorState.loading')
    point = page.evaluate('''amount => maskBrushSourcePoint(0.25, 0.75,
      {rotation:90, flip:{horizontal:true}, adjustments:{distortion:amount}},
      false, {width:600,height:400})''', amount)
    from float_image import FloatImage
    from image_edits import _apply_geometry
    x, y = np.meshgrid((np.arange(600) + 0.5) / 600, (np.arange(400) + 0.5) / 400)
    source = FloatImage(np.stack((x, y, x * 0), axis=-1))
    rendered = np.asarray(_apply_geometry(source, {"rotation": 90, "flip": {"horizontal": True},
                                                  "adjustments": {"distortion": amount}}))
    # Rotation/flip maps (0.25,0.75) to (0.75,0.25) in the corrected frame.
    assert point == pytest.approx(rendered[450, 100, :2], abs=0.002)


@pytest.fixture
def profiled_raw(live_server, tmp_path):
    from vireo.tests.test_raw_precision import write_dng

    folder = tmp_path / "profiled-raws"
    folder.mkdir()
    path = folder / "lens-profile.dng"
    write_dng(path)
    db = live_server["db"]
    fid = workspace_folders.add_folder(db, str(folder))
    pid = photo_ingest.add_photo(db, folder_id=fid, filename=path.name, extension=".dng", width=512, height=64,
                                file_size=path.stat().st_size, file_mtime=path.stat().st_mtime)
    db.conn.execute('UPDATE photos SET camera_make=?,camera_model=?,lens=?,focal_length=?,aperture=? WHERE id=?',
                    ('NIKON CORPORATION', 'NIKON Z 8', 'Nikkor Z 180-600mm f/5.6-6.3 VR', 300, 6.3, pid))
    db.conn.commit()
    return pid


def test_automatic_profile_raw_status_persistence_and_brush_geometry(page, live_server, profiled_raw):
    page.goto(f"{live_server['url']}/edit/{profiled_raw}")
    page.wait_for_function('!editorState.loading && document.getElementById("editorImg").naturalWidth > 0')
    page.locator('#lensProfileEnabled').check()
    expect(page.locator('#lensProfileStatus')).to_contain_text('Nikkor Z 180-600mm')
    expect(page.locator('#lensProfileStatus')).to_contain_text('Applied to the decoded RAW')
    assert page.evaluate('recipeForSave(editorState.recipe).adjustments.lens_profile') is True
    # The browser's source map must include the calibrated profile/inset.
    point = page.evaluate('''() => maskBrushSourcePoint(0.25,0.75,editorState.recipe,false,{width:512,height:64})''')
    from float_image import FloatImage
    from lens_profiles import resolve_profile
    from optics import distort_image
    profile = resolve_profile(live_server['db'].photos.get(profiled_raw))
    x, y = np.meshgrid((np.arange(512) + 0.5) / 512, (np.arange(64) + 0.5) / 64)
    source = FloatImage(np.stack((x, y, x * 0), axis=-1))
    rendered = np.asarray(distort_image(source, 0, profile))
    assert point == pytest.approx(rendered[48, 128, :2], abs=0.01)
    page.evaluate('resetAdjustments(); resetDetail()')
    assert page.evaluate('recipeForSave(editorState.recipe).adjustments.lens_profile') is True
    assert page.evaluate('saveRecipe()') is True
    page.reload()
    page.wait_for_function('!editorState.loading')
    expect(page.locator('#lensProfileEnabled')).to_be_checked()
    expect(page.locator('#lensProfileStatus')).to_contain_text('Applied to the decoded RAW')
    expect(page.locator('#saveBtn')).to_be_disabled()
    page.locator('#opticsBand').scroll_into_view_if_needed()


def test_matched_profile_is_skipped_on_jpeg_fallback(page, live_server, profiled_raw, monkeypatch):
    from web.media_editing import _EditPreviewRequest

    live_server['app'].config['EDIT_PREVIEW_IN_PROCESS'] = True
    monkeypatch.setattr(_EditPreviewRequest, 'load', lambda self: Image.new('RGB', (512, 64), (80, 90, 100)))
    page.goto(f"{live_server['url']}/edit/{profiled_raw}")
    page.wait_for_function('!editorState.loading && document.getElementById("editorImg").naturalWidth > 0')
    page.locator('#lensProfileEnabled').check()
    expect(page.locator('#lensProfileStatus')).to_contain_text('Skipped: the preview is using a rendered image fallback')
    assert page.evaluate('lensProfileWarp(editorState.recipe)') is None
    assert page.evaluate('lensProfileSourcePending(editorState.recipe)') is False


def test_missing_profile_is_visible_when_recipe_requests_matching(page, live_server, optics_photo):
    page.goto(f"{live_server['url']}/edit/{optics_photo}")
    page.wait_for_function('!editorState.loading')
    page.locator('#lensProfileEnabled').check()
    expect(page.locator('#lensProfileStatus')).to_contain_text('No profile correction will be applied')
    assert page.evaluate('recipeForSave(editorState.recipe).adjustments.lens_profile') is True


def test_reused_preview_resolves_source_status_after_delay(page, live_server, profiled_raw):
    # When the editor reuses the already-displayed image for a scheduled refresh,
    # the request's seq must track the post-bump editorState.previewSeq. If it
    # didn't, a delayed source-status receipt would be rejected and the matched
    # profile would stay source-pending, blocking mask painting until a fresh
    # render ran.
    page.goto(f"{live_server['url']}/edit/{profiled_raw}")
    page.wait_for_function('!editorState.loading && document.getElementById("editorImg").naturalWidth > 0')
    page.locator('#lensProfileEnabled').check()
    page.wait_for_function('document.getElementById("editorImg").dataset.previewSourceKind === "linear"')
    # Hold the next source-status response so a potential older seq is visible
    # before the receipt lands; the reuse branch runs while the fetch is paused.
    held = []
    pattern = f"**/photos/{profiled_raw}/edit-preview?*source_status=1*"

    def _hold(route, request):
        if "source_status=1" in request.url:
            held.append(route)
        else:
            route.continue_()

    page.route(pattern, _hold)
    page.evaluate('delete document.getElementById("editorImg").dataset.previewSourceKind')
    before_seq = page.evaluate('editorState.previewSeq')
    page.evaluate('updatePreview({scheduled: true})')
    # The reuse branch bumps previewSeq synchronously, before the receipt fetch.
    page.wait_for_function(f'editorState.previewSeq > {before_seq}')
    for _ in range(100):
        if held:
            break
        page.wait_for_timeout(50)
    assert held, "source_status receipt was never requested"
    for route in held:
        route.continue_()
    page.unroute(pattern, _hold)
    page.wait_for_function('document.getElementById("editorImg").dataset.previewSourceKind === "linear"')
    assert page.evaluate('lensProfileSourcePending(editorState.recipe)') is False
