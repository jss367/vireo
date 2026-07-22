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
