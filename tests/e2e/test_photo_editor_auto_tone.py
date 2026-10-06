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


@pytest.mark.parametrize('reload_same_photo', [False, True])
def test_stale_auto_tone_response_does_not_edit_loaded_photo(live_server, page, dark_photo, reload_same_photo):
    """A response belongs to its photo and load, even when recipes are identical."""
    db = live_server['db']
    photo = db.get_photo(dark_photo)
    other = db.add_photo(
        folder_id=photo['folder_id'], filename='other.png', extension='.png',
        file_size=photo['file_size'], file_mtime=photo['file_mtime'], width=300, height=200,
    )
    folder = db.conn.execute('SELECT path FROM folders WHERE id=?', (photo['folder_id'],)).fetchone()[0]
    from pathlib import Path
    Image.new('RGB', (300, 200), (100, 100, 100)).save(Path(folder) / 'other.png')
    db.conn.commit()
    page.goto(f"{live_server['url']}/edit/{dark_photo}")
    page.wait_for_function('!editorState.loading')
    intercepted = []
    page.route('**/api/photos/*/auto-tone?*', lambda route: intercepted.append(route))
    with page.expect_request('**/api/photos/*/auto-tone?*'):
        page.locator('#autoToneBtn').click()
    target = dark_photo if reload_same_photo else other
    page.evaluate('id => loadPhoto(id)', target)
    page.wait_for_function('!editorState.loading')
    expect(page.locator('#editorFilename')).to_have_text('dim-meadow.png' if reload_same_photo else 'other.png')
    intercepted[0].fulfill(json={
        'adjustments': {'exposure': 2}, 'notes': ['brightened 2 EV'], 'metering': 'frame',
    })
    page.wait_for_function('!document.getElementById("autoToneBtn").disabled')
    assert page.evaluate('editorState.photoId') == target
    expect(page.locator('#exposureValue')).to_have_text('0.0')
    assert not page.evaluate('recipeForSave(editorState.recipe).adjustments')
    expect(page.locator('#saveBtn')).to_be_disabled()


@pytest.mark.parametrize('style, button, label', [
    ('subject', '#autoToneSubjectBtn', 'Auto Tone (Subject style)'),
    ('gentle', '#autoToneGentleBtn', 'Auto Tone (Gentle style)'),
])
def test_style_buttons_request_their_style_and_name_it(live_server, page, dark_photo, style, button, label):
    page.goto(f"{live_server['url']}/edit/{dark_photo}")
    expect(page.locator('#editorFilename')).to_have_text('dim-meadow.png')
    page.wait_for_function('!editorState.loading')
    held = []
    page.route('**/api/photos/*/auto-tone?*', lambda route: held.append(route))
    with page.expect_request('**/api/photos/*/auto-tone?*') as request:
        page.locator(button).click()
    assert f'style={style}' in request.value.url
    # One fit at a time: every Auto button waits for this one.
    for other in ('#autoToneBtn', '#autoToneSubjectBtn', '#autoToneGentleBtn'):
        expect(page.locator(other)).to_be_disabled()
    held[0].continue_()
    toast = page.locator('#toastContainer')
    expect(toast).to_contain_text(label + ' (metered on the detected subject):')
    expect(page.locator('#autoToneBtn')).to_be_enabled()
    expect(page.locator(button)).not_to_have_text('Analyzing...')


def test_subject_fallback_is_named_when_nothing_changes(live_server, page, dark_photo):
    page.goto(f"{live_server['url']}/edit/{dark_photo}")
    expect(page.locator('#editorFilename')).to_have_text('dim-meadow.png')
    page.wait_for_function('!editorState.loading')
    page.route('**/api/photos/*/auto-tone?*', lambda route: route.fulfill(json={
        'adjustments': {key: 0 for key in (
            'exposure', 'highlights', 'shadows', 'contrast', 'whites', 'blacks', 'vibrance', 'saturation',
        )},
        'notes': ['no subject found, so metered the whole frame as Balanced does'],
        'metering': 'frame', 'style': 'subject', 'subject_source': None,
    }))
    page.locator('#autoToneSubjectBtn').click()
    expect(page.locator('#toastContainer')).to_contain_text(
        'Auto Tone (Subject style): no subject found, so metered the whole frame as Balanced does; '
        'already balanced, nothing changed'
    )
