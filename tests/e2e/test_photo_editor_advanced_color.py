import pytest
from PIL import Image
from playwright.sync_api import expect


def _set_range(page, selector, value):
    page.locator(selector).evaluate(
        """(el, value) => {
            el.value = String(value);
            el.dispatchEvent(new Event('input', {bubbles: true}));
        }""",
        value,
    )


def test_photo_editor_saves_and_restores_advanced_color(live_server, page):
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]
    page.goto(f"{url}/edit/{photo_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")

    _set_range(page, "#curve_midtonesRange", 62)
    page.locator("#hslColorSelect").select_option("orange")
    _set_range(page, "#hslSaturationRange", 30)
    page.locator("#colorGradeZoneSelect").select_option("shadows")
    _set_range(page, "#colorGradeHueRange", 220)
    _set_range(page, "#colorGradeSaturationRange", 18)

    expect(page.locator("#saveBtn")).to_be_enabled()
    with page.expect_response(f"**/api/photos/{photo_id}/edit-recipe") as response:
        page.locator("#saveBtn").click()
    assert response.value.status == 200
    expect(page.locator("#saveBtn")).to_be_disabled()

    recipe = page.evaluate(
        """async (photoId) => {
            const r = await fetch('/api/photos/' + photoId + '/edit-recipe');
            return (await r.json()).recipe;
        }""",
        photo_id,
    )
    assert recipe["adjustments"]["tone_curve"] == {"midtones": 62.0}
    assert recipe["adjustments"]["hsl"] == {
        "orange": {"saturation": 30.0},
    }
    assert recipe["adjustments"]["color_grading"] == {
        "shadows": {"hue": 220.0, "saturation": 18.0},
    }

    page.reload()
    expect(page.locator("#curve_midtonesRange")).to_have_value("62")
    page.locator("#hslColorSelect").select_option("orange")
    expect(page.locator("#hslSaturationRange")).to_have_value("30")
    expect(page.locator("#colorGradeHueRange")).to_have_value("220")
    expect(page.locator("#colorGradeSaturationRange")).to_have_value("18")


def test_photo_editor_export_saves_current_edits_and_exports_current_photo(
    live_server, page,
):
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]
    page.goto(f"{url}/edit/{photo_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")
    expect(page.locator("#exportBtn")).to_be_enabled()

    _set_range(page, "#exposureRange", 1.2)
    expect(page.locator("#saveBtn")).to_be_enabled()

    with page.expect_response(
        f"**/api/photos/{photo_id}/edit-recipe"
    ) as save_response:
        page.locator("#exportBtn").click()
    assert save_response.value.status == 200
    expect(page.locator("#saveBtn")).to_be_disabled()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    expect(page.locator("#exportPreview")).to_have_text("Preview: hawk1.jpg")
    page.locator("#exportTemplate").fill("{species}")
    expect(page.locator("#exportPreview")).to_have_text(
        "Preview: Red-tailed Hawk.jpg"
    )
    page.locator("#exportTemplate").fill(
        "{folder}_{folder}_{species}_{species}"
    )
    expect(page.locator("#exportPreview")).to_have_text(
        "Preview: park_park_Red-tailed Hawk_Red-tailed Hawk.jpg"
    )
    page.locator("#exportTemplate").fill("{original}")

    page.get_by_role("button", name="Browse…", exact=True).click()
    expect(page.locator("#folderBrowser")).to_have_class(
        "folder-browser-overlay open"
    )
    page.keyboard.press("Escape")
    expect(page.locator("#folderBrowser")).not_to_have_class(
        "folder-browser-overlay open"
    )
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")

    page.evaluate(
        """() => {
          window.__editorExportRequest = null;
          window.__editorPreflightRequest = null;
          window.__resolveEditorPreflight = null;
          window.__resolveEditorExport = null;
          const originalSafeFetch = window.safeFetch;
          window.safeFetch = async function(url, options, config) {
            if (url === '/api/jobs/export/preflight') {
              window.__editorPreflightRequest = JSON.parse(options.body);
              return new Promise(function(resolve) {
                window.__resolveEditorPreflight = function() {
                  resolve({rename_count: 0, renames: []});
                };
              });
            }
            if (url === '/api/jobs/export') {
              window.__editorExportRequest = JSON.parse(options.body);
              return new Promise(function(resolve) {
                window.__resolveEditorExport = function() {
                  resolve({job_id: 'editor-export-test'});
                };
              });
            }
            return originalSafeFetch(url, options, config);
          };
        }"""
    )
    page.locator("#exportResize").select_option("custom")
    page.locator("#exportSubmitBtn").click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    assert page.evaluate("window.__editorExportRequest") is None
    expect(page.locator("#exportResizeCustom")).to_be_focused()

    page.locator("#exportResizeCustom").fill("1600")
    page.locator("#exportMetadataRating").check()
    page.locator("#exportSubmitBtn").click()
    page.wait_for_function("() => window.__resolveEditorPreflight !== null")
    expect(page.locator("#exportDest")).to_be_disabled()
    expect(page.get_by_role("button", name="Cancel", exact=True)).to_be_enabled()
    page.evaluate("window.__resolveEditorPreflight()")
    page.wait_for_function("() => window.__editorExportRequest !== null")
    request = page.evaluate("window.__editorExportRequest")
    assert request["photo_ids"] == [photo_id]
    assert request["destination"] == ""
    assert request["format"] == "jpg"
    assert request["max_size"] == 1600
    assert request["metadata_fields"] == ["rating"]
    assert page.evaluate("window.__editorPreflightRequest") == request
    expect(page.get_by_role("button", name="Cancel", exact=True)).to_be_disabled()
    page.keyboard.press("Escape")
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    page.evaluate("document.getElementById('exportOverlay').click()")
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    page.evaluate("window.__resolveEditorExport()")
    expect(page.locator("#exportOverlay")).not_to_have_class("modal-overlay open")


def test_photo_editor_export_reports_preflight_failure_before_job_start(
    live_server, page,
):
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]
    page.goto(f"{url}/edit/{photo_id}")
    page.locator("#exportBtn").click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    page.evaluate(
        """() => {
          window.__editorExportJobCalls = 0;
          window.safeFetch = async function(url) {
            if (url === '/api/jobs/export/preflight') {
              throw new Error('probe denied');
            }
            if (url === '/api/jobs/export') window.__editorExportJobCalls++;
            return {};
          };
        }"""
    )

    page.locator("#exportSubmitBtn").click()

    expect(page.locator("#toastContainer")).to_contain_text(
        "Export check failed: probe denied"
    )
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    expect(page.get_by_role("button", name="Cancel", exact=True)).to_be_enabled()
    expect(page.locator("#exportSubmitBtn")).to_be_enabled()
    assert page.evaluate("window.__editorExportJobCalls") == 0


def test_reset_adjustments_preserves_advanced_color(live_server, page):
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]
    page.goto(f"{url}/edit/{photo_id}")
    expect(page.locator("#editorFilename")).to_have_text("hawk1.jpg")

    # Basic adjustments the Reset button owns, plus advanced sections that
    # each have their own dedicated reset control.
    _set_range(page, "#exposureRange", 1.2)
    _set_range(page, "#contrastRange", 25)
    _set_range(page, "#curve_midtonesRange", 62)
    page.locator("#hslColorSelect").select_option("orange")
    _set_range(page, "#hslSaturationRange", 30)
    page.locator("#colorGradeZoneSelect").select_option("shadows")
    _set_range(page, "#colorGradeHueRange", 220)
    _set_range(page, "#colorGradeSaturationRange", 18)

    page.locator('button[onclick="resetAdjustments()"]').click()

    # Basic sliders return to neutral, but advanced sections stay intact.
    expect(page.locator("#exposureRange")).to_have_value("0")
    expect(page.locator("#contrastRange")).to_have_value("0")
    expect(page.locator("#curve_midtonesRange")).to_have_value("62")
    page.locator("#hslColorSelect").select_option("orange")
    expect(page.locator("#hslSaturationRange")).to_have_value("30")
    page.locator("#colorGradeZoneSelect").select_option("shadows")
    expect(page.locator("#colorGradeHueRange")).to_have_value("220")
    expect(page.locator("#colorGradeSaturationRange")).to_have_value("18")

    adjustments = page.evaluate("() => editorState.recipe.adjustments || {}")
    assert "exposure" not in adjustments
    assert "contrast" not in adjustments
    assert adjustments.get("tone_curve") == {"midtones": 62.0}
    assert adjustments.get("hsl") == {"orange": {"saturation": 30.0}}
    assert adjustments.get("color_grading") == {
        "shadows": {"hue": 220.0, "saturation": 18.0},
    }


def test_toolbar_history_refreshes_editor_recipe_and_next_save(live_server, page):
    photo_id = live_server['data']['photos'][0]
    page.goto(f"{live_server['url']}/edit/{photo_id}")
    expect(page.locator('#editorFilename')).to_have_text('hawk1.jpg')
    _set_range(page, '#exposureRange', 1)
    page.evaluate('saveRecipe()')
    _set_range(page, '#exposureRange', 2)
    page.evaluate('saveRecipe()')
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.locator('#historyUndoBtn').click()
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    expect(page.locator('#exposureRange')).to_have_value('1')
    assert page.evaluate('isEditorDirty()') is False
    page.locator('#historyRedoBtn').click()
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    expect(page.locator('#exposureRange')).to_have_value('2')
    page.evaluate('doUndo()')
    _set_range(page, '#contrastRange', 15)
    # Unsaved work must not be replaced by a toolbar history refresh.
    assert page.evaluate('doUndo()') is False
    expect(page.locator('#contrastRange')).to_have_value('15')
    assert page.evaluate('saveRecipe()') is True
    recipe = live_server['db'].get_photo_edit_recipe(photo_id)
    assert recipe['adjustments']['exposure'] == 1
    assert recipe['adjustments']['contrast'] == 15


def test_editor_history_failure_releases_request_freeze_but_keeps_failed_load_frozen(live_server, page):
    photo_id = live_server['data']['photos'][0]
    page.goto(f"{live_server['url']}/edit/{photo_id}")
    expect(page.locator('#editorFilename')).to_have_text('hawk1.jpg')
    _set_range(page, '#exposureRange', 1)
    page.evaluate('saveRecipe()')
    page.route('**/api/undo', lambda route: route.abort())
    assert page.evaluate('doUndo()') is False
    assert page.evaluate('editorState.loading') is False
    page.unroute('**/api/undo')
    page.route(f'**/api/photos/{photo_id}', lambda route: route.abort())
    assert page.evaluate('doUndo()') is True
    expect(page.locator('#editorFilename')).to_have_text('Photo unavailable')
    assert page.evaluate('editorState.loading') is True
    assert page.evaluate('saveRecipe()') is False


@pytest.fixture
def color_photo(live_server, tmp_path):
    folder = tmp_path / 'color-photos'
    folder.mkdir()
    path = folder / 'color-study.png'
    Image.new('RGB', (600, 400), (0, 128, 0)).save(path)
    db = live_server['db']
    folder_id = db.add_folder(str(folder))
    return db.add_photo(folder_id=folder_id, filename=path.name, extension='.png',
                        file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
                        width=600, height=400)


def _wait_color_preview(page):
    page.wait_for_function("""() => {
      const img = document.getElementById('editorImg');
      return img.complete && img.naturalWidth && editorImageMatchesZoomRecipe(img);
    }""")


def test_interactive_curve_and_point_color_persist_with_history(live_server, page, color_photo):
    photo_id = color_photo
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(f"{live_server['url']}/edit/{photo_id}")
    expect(page.locator('#editorFilename')).to_have_text('color-study.png')
    page.locator('#curveChannel').select_option('red')
    page.locator('#curveAddPoint').click()
    page.locator('#curveOutput').fill('65')
    page.locator('#curveOutput').press('Tab')
    assert page.evaluate('editorState.recipe.adjustments.point_curves.red') == [[0, 0], [50, 65], [100, 100]]
    page.locator('#pointColorCustom').evaluate("el => {el.value='#008000'; el.dispatchEvent(new Event('change'));}")
    _set_range(page, '#pointColor_hue_range', 45)
    _set_range(page, '#pointColor_saturation_range', 30)
    _set_range(page, '#pointColor_luminance_range', 25)
    _set_range(page, '#pointColor_hue', 60)
    _set_range(page, '#pointColor_saturation', -20)
    _set_range(page, '#pointColor_luminance', 10)
    assert page.evaluate('saveRecipe()') is True
    saved = live_server['db'].get_photo_edit_recipe(photo_id)
    assert saved['adjustments']['point_curves']['red'] == [[0, 0], [50, 65], [100, 100]]
    sample = saved['adjustments']['point_color'][0]
    assert sample['hue'] == 60 and sample['saturation_range'] == 30 and sample['luminance_range'] == 25
    page.reload()
    expect(page.locator('#editorFilename')).to_have_text('color-study.png')
    expect(page.locator('#saveBtn')).to_be_disabled()
    page.locator('#curveChannel').select_option('red')
    page.locator('#curvePointSelect').select_option('1')
    expect(page.locator('#curveOutput')).to_have_value('65')
    expect(page.locator('#pointColor_hue')).to_have_value('60')
    _set_range(page, '#pointColor_hue', 90)
    assert page.evaluate('saveRecipe()') is True
    assert page.evaluate('doUndo()') is True
    expect(page.locator('#pointColor_hue')).to_have_value('60')
    assert page.evaluate('doRedo()') is True
    expect(page.locator('#pointColor_hue')).to_have_value('90')
    page.locator('button[onclick="resetAdjustments()"]').click()
    assert page.evaluate('recipeForSave(editorState.recipe).adjustments.point_color[0].hue') == 90
    page.locator('button[onclick="resetCurveChannel()"]').click()
    assert page.evaluate('(recipeForSave(editorState.recipe).adjustments || {}).point_curves || null') is None
    page.locator('button[onclick="resetPointColor()"]').click()
    assert page.evaluate('recipeForSave(editorState.recipe)') == {}
    assert not errors


def test_curve_mouse_keyboard_and_legacy_promotion(live_server, page, color_photo):
    photo_id = color_photo
    page.goto(f"{live_server['url']}/edit/{photo_id}")
    expect(page.locator('#editorFilename')).to_have_text('color-study.png')
    _set_range(page, '#curve_midtonesRange', 60)
    assert page.evaluate('currentCurvePoints()')[2] == [50, 60]
    graph = page.locator('#pointCurveGraph')
    graph.scroll_into_view_if_needed()
    box = graph.bounding_box()
    def graph_xy(x, y):
        return (box['x'] + (12 + x * 2.16) * box['width'] / 240,
                box['y'] + (228 - y * 2.16) * box['height'] / 240)
    page.mouse.move(*graph_xy(50, 60))
    page.mouse.down()
    page.mouse.move(*graph_xy(58, 70), steps=6)
    page.mouse.up()
    recipe = page.evaluate('recipeForSave(editorState.recipe)')
    assert 'tone_curve' not in recipe['adjustments']
    points = recipe['adjustments']['point_curves']['rgb']
    assert abs(points[2][0] - 58) < 1 and abs(points[2][1] - 70) < 1
    graph.press('ArrowUp')
    moved = page.evaluate('currentCurvePoints()')[2]
    assert moved[1] == points[2][1] + 1
    graph.press('Delete')
    assert len(page.evaluate('currentCurvePoints()')) == 4
    page.locator('#curvePointSelect').select_option('0')
    expect(page.locator('#curveInput')).to_be_disabled()
    expect(page.locator('#curveDeletePoint')).to_be_disabled()
    page.locator('#curveOutput').fill('10')
    page.locator('#curveOutput').press('Tab')
    assert page.evaluate('currentCurvePoints()')[0] == [0, 10]
    assert page.evaluate('saveRecipe()') is True


def test_photo_color_picker_samples_before_point_color_and_cancels(live_server, page, color_photo):
    photo_id = color_photo
    page.goto(f"{live_server['url']}/edit/{photo_id}")
    expect(page.locator('#editorFilename')).to_have_text('color-study.png')
    _set_range(page, '#saturationRange', -30)
    _set_range(page, '#vibranceRange', 10)
    _wait_color_preview(page)
    page.locator('#pointColorPick').click()
    page.keyboard.press('Escape')
    expect(page.locator('#pointColorPick')).to_have_attribute('aria-pressed', 'false')
    page.locator('#pointColorPick').click()
    page.locator('#editorImg').click(force=True)
    expect(page.locator('#pointColorStatus')).to_contain_text('Color sampled')
    original = page.evaluate('pointColorSamples()[0].sample')
    assert abs(original[0] - 120) < 2
    _set_range(page, '#pointColor_hue', 90)
    _wait_color_preview(page)
    page.locator('#pointColorPick').click()
    page.locator('#editorImg').click(force=True)
    expect(page.locator('#pointColorSelect option')).to_have_count(2)
    assert page.evaluate('pointColorSamples()[1].sample') == original
    page.locator('#pointColorRemove').click()
    expect(page.locator('#pointColorSelect option')).to_have_count(1)


def test_new_color_controls_survive_presets_and_copy(live_server, page, color_photo):
    photo_id = color_photo
    page.goto(f"{live_server['url']}/edit/{photo_id}")
    expect(page.locator('#editorFilename')).to_have_text('color-study.png')
    page.locator('#curveChannel').select_option('blue')
    page.locator('#curveOutput').fill('8')
    page.locator('#curveOutput').press('Tab')
    page.locator('#pointColorCustom').evaluate("el => {el.value='#c08040'; el.dispatchEvent(new Event('change'));}")
    _set_range(page, '#pointColor_hue', -25)
    expected = page.evaluate('recipeForSave(editorState.recipe).adjustments')
    page.locator('#presetNameInput').fill('Warm wildlife colors')
    page.locator('#savePresetBtn').click()
    page.get_by_role('dialog').get_by_role('button', name='Save preset', exact=True).click()
    expect(page.locator('#presetSelect')).not_to_have_value('')
    page.locator('button[onclick="resetToneCurve()"]').click()
    page.locator('button[onclick="resetPointColor()"]').click()
    page.locator('#applyPresetBtn').click()
    page.get_by_role('dialog').get_by_role('button', name='Apply selected settings').click()
    page.wait_for_function("document.getElementById('toastContainer').textContent.includes('Applied preset')")
    assert page.evaluate('recipeForSave(editorState.recipe).adjustments') == expected
    # Clipboard settings use the canonical recipe, including both new sections.
    page.evaluate('copyEditSettings()')
    assert page.evaluate('vireoEditNav.getCopiedRecipe().recipe.adjustments') == expected
    assert page.evaluate('saveRecipe()') is True
    assert live_server['db'].get_photo_edit_recipe(photo_id)['adjustments'] == expected


@pytest.mark.parametrize('old_request_fails', [False, True])
def test_reopening_color_picker_ignores_previous_pending_sample(live_server, page, color_photo, old_request_fails):
    page.goto(f"{live_server['url']}/edit/{color_photo}")
    _wait_color_preview(page)
    page.evaluate("""() => {
      window.originalColorImageLoader = _loadImage;
      _loadImage = () => new Promise((resolve, reject) => {
        window.releaseOldColorSample = fails => fails
          ? reject(new Error('delayed sample failed'))
          : resolve(document.getElementById('editorImg'));
      });
    }""")
    page.locator('#pointColorPick').click()
    page.locator('#editorImg').click(force=True)
    expect(page.locator('#pointColorStatus')).to_have_text('Sampling color…')
    page.locator('#pointColorPick').click()
    page.evaluate('async fails => { releaseOldColorSample(fails); await Promise.resolve(); }', old_request_fails)
    assert page.evaluate('pointColorSamples()') == []
    expect(page.locator('#pointColorPick')).to_have_attribute('aria-pressed', 'true')
    expect(page.locator('#pointColorStatus')).to_have_text('Click a color in the photo. Escape cancels.')
    page.evaluate('() => { _loadImage = originalColorImageLoader; }')
    page.locator('#editorImg').click(force=True)
    expect(page.locator('#pointColorStatus')).to_contain_text('Color sampled')
    assert len(page.evaluate('pointColorSamples()')) == 1


@pytest.mark.parametrize('hex_color', ['#000000', '#808080', '#ffffff', '#808180', '#636565'])
def test_custom_point_color_rejects_neutral_samples(live_server, page, color_photo, hex_color):
    page.goto(f"{live_server['url']}/edit/{color_photo}")
    expect(page.locator('#editorFilename')).to_have_text('color-study.png')
    page.locator('#pointColorCustom').evaluate("""(el, color) => {
      el.value = color;
      el.dispatchEvent(new Event('change'));
    }""", hex_color)
    expect(page.locator('#pointColorStatus')).to_contain_text('Choose a more saturated color')
    assert page.evaluate('pointColorSamples()') == []
    expect(page.locator('#pointColor_hue')).to_be_disabled()
    expect(page.locator('#saveBtn')).to_be_disabled()
    page.locator('#pointColorCustom').evaluate("el => { el.value='#008000'; el.dispatchEvent(new Event('change')); }")
    assert len(page.evaluate('pointColorSamples()')) == 1
    expect(page.locator('#pointColor_hue')).to_be_enabled()
    expect(page.locator('#pointColorStatus')).to_have_text('')


def test_point_color_samples_the_displayed_native_resolution(live_server, page, tmp_path):
    folder = tmp_path / 'fine-color-detail'
    folder.mkdir()
    path = folder / 'fine-color-detail.png'
    image = Image.new('RGB', (2600, 400), (0, 128, 0))
    image.paste((180, 40, 40), (1298, 0, 1302, 400))
    image.save(path)
    db = live_server['db']
    folder_id = db.add_folder(str(folder))
    photo_id = db.add_photo(folder_id=folder_id, filename=path.name, extension='.png',
                            file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
                            width=2600, height=400)
    page.goto(f"{live_server['url']}/edit/{photo_id}")
    _wait_color_preview(page)
    page.evaluate("setZoomMode('actual')")
    page.wait_for_function("document.getElementById('editorImg').naturalWidth === 2600")
    _wait_color_preview(page)
    page.evaluate("""() => {
      const original = _loadImage;
      _loadImage = async url => {
        const image = await original(url);
        window.sampledRender = {url, width: image.naturalWidth};
        return image;
      };
      window.addEventListener('pointerdown', e => {
        if (!colorEditor.picking) return;
        const img = document.getElementById('editorImg');
        const r = img.getBoundingClientRect();
        const x = Math.floor((e.clientX - r.left) / r.width * img.naturalWidth);
        const y = Math.floor((e.clientY - r.top) / r.height * img.naturalHeight);
        const canvas = document.createElement('canvas');
        canvas.width = canvas.height = 1;
        const context = canvas.getContext('2d');
        context.drawImage(img, x, y, 1, 1, 0, 0, 1, 1);
        const pixel = context.getImageData(0, 0, 1, 1).data;
        window.clickedColorSample = rgbToPointSample(...pixel.slice(0, 3)).map(colorRound);
      }, true);
    }""")
    page.locator('#pointColorPick').click()
    # At 100% the image extends beyond the canvas; click its visible portion
    # rather than the full element's center, which may be behind the sidebar.
    position = page.evaluate("""() => {
      const wrap = document.getElementById('editorCanvasWrap');
      wrap.scrollLeft = (wrap.scrollWidth - wrap.clientWidth) / 2;
      const bounds = wrap.getBoundingClientRect();
      const image = document.getElementById('editorImg').getBoundingClientRect();
      return {
        x: (Math.max(bounds.left, image.left) + Math.min(bounds.right, image.right)) / 2,
        y: (Math.max(bounds.top, image.top) + Math.min(bounds.bottom, image.bottom)) / 2,
      };
    }""")
    page.mouse.click(position['x'], position['y'])
    expect(page.locator('#pointColorStatus')).to_contain_text('Color sampled')
    assert page.evaluate('sampledRender.width') == 2600
    assert page.evaluate("new URL(sampledRender.url).searchParams.get('size')") == '2600'
    assert page.evaluate('pointColorSamples()[0].sample') == page.evaluate('clickedColorSample')
