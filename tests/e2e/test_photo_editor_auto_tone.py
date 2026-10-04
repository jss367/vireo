"""Auto Tone fits the Basic sliders from the server and says what it did."""

import pytest
from PIL import Image
from playwright.sync_api import expect


@pytest.fixture
def dark_photo(live_server, tmp_path):
    """A dim, muted landscape with a dark subject box."""
    folder = tmp_path / 'auto-tone-photos'
    folder.mkdir()
    image = Image.new('RGB', (300, 200))
    image.putdata([
        (20 + x // 8, 26 + x // 9, 18 + y // 10) for y in range(200) for x in range(300)
    ])
    path = folder / 'dim-meadow.png'
    image.save(path)
    db = live_server['db']
    folder_id = db.add_folder(str(folder))
    photo_id = db.add_photo(
        folder_id=folder_id, filename=path.name, extension='.png',
        file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
        width=image.width, height=image.height,
    )
    db.conn.execute(
        "INSERT INTO detections (photo_id, box_x, box_y, box_w, box_h, "
        "detector_confidence, category) VALUES (?, 0.4, 0.4, 0.2, 0.2, 0.9, 'animal')",
        (photo_id,),
    )
    db.conn.commit()
    return photo_id


def test_auto_tone_button_sets_sliders_and_reports_metering(live_server, page, dark_photo):
    page.goto(f"{live_server['url']}/edit/{dark_photo}")
    expect(page.locator('#editorFilename')).to_have_text('dim-meadow.png')
    page.wait_for_function('!editorState.loading')
    # A white balance the user chose survives Auto Tone untouched.
    page.locator('#temperatureRange').evaluate(
        "el => { el.value = '25'; el.dispatchEvent(new Event('input', {bubbles: true})); }"
    )

    with page.expect_response('**/api/photos/*/auto-tone?*') as response:
        page.locator('#autoToneBtn').click()
    fitted = response.value.json()['adjustments']

    toast = page.locator('#toastContainer')
    expect(toast).to_contain_text('Auto Tone (metered on the detected subject): Brightened')
    expect(toast).to_contain_text('White balance left unchanged.')
    assert fitted['exposure'] > 0
    expect(page.locator('#exposureValue')).to_have_text(f"{fitted['exposure']:.1f}")
    adjustments = page.evaluate('recipeForSave(editorState.recipe).adjustments')
    for key, value in fitted.items():
        assert adjustments.get(key, 0) == value
    assert adjustments['white_balance'] == {'temperature': 25}
    expect(page.locator('#saveBtn')).to_be_enabled()
    # Repeating a nonzero fit is a no-op, even though its source-based notes
    # still describe the first fit's brightening and other tonal changes.
    with page.expect_response('**/api/photos/*/auto-tone?*'):
        page.locator('#autoToneBtn').click()
    expect(toast).to_contain_text('already balanced, nothing changed')
    assert page.evaluate('recipeForSave(editorState.recipe).adjustments') == adjustments


def test_auto_tone_reports_resetting_previous_controls(live_server, page, dark_photo):
    """A zero fit can visibly clear manual edits; only a second click is a no-op."""
    page.goto(f"{live_server['url']}/edit/{dark_photo}")
    expect(page.locator('#editorFilename')).to_have_text('dim-meadow.png')
    page.wait_for_function('!editorState.loading')
    page.locator('#exposureRange').evaluate(
        "el => { el.value = '1'; el.dispatchEvent(new Event('input', {bubbles: true})); }"
    )
    page.route('**/api/photos/*/auto-tone?*', lambda route: route.fulfill(json={
        'adjustments': {key: 0 for key in (
            'exposure', 'highlights', 'shadows', 'contrast', 'whites', 'blacks', 'vibrance', 'saturation',
        )}, 'notes': [], 'metering': 'frame',
    }))
    page.locator('#autoToneBtn').click()
    toast = page.locator('#toastContainer')
    expect(toast).to_contain_text('Reset previous tone adjustments; source already balanced.')
    expect(toast).not_to_contain_text('nothing changed')
    expect(page.locator('#exposureValue')).to_have_text('0.0')
    page.locator('#autoToneBtn').click()
    expect(toast).to_contain_text('already balanced, nothing changed')
