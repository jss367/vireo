"""Manual focus uses the upright full frame even when saved edits change framing."""
from pathlib import Path

import pytest
from PIL import Image
from playwright.sync_api import expect
from services import detection_store, photo_ingest, workspace_folders


@pytest.mark.parametrize('viewport', [(1280, 900), (800, 700), (390, 844)])
@pytest.mark.parametrize('image_size', [(600, 400), (400, 600)])
def test_manual_focus_center_and_subject_inspection_preserve_decisions(page, live_server, tmp_path, viewport, image_size):
    page.set_viewport_size({'width': viewport[0], 'height': viewport[1]})
    db = live_server['db']
    live_server['app'].config['EDIT_PREVIEW_IN_PROCESS'] = True
    folder = workspace_folders.add_folder(db, str(tmp_path), name='Focus inspection')
    pids = []
    for index in range(2):
        path = tmp_path / f'DSC_{index:04d}.jpg'
        image = Image.new('RGB', image_size, 'blue')
        width, height = image_size
        image.paste('red', (int(width * .65), int(height * .1), int(width * .85), int(height * .35)))
        image.save(path)
        pid = photo_ingest.add_photo(db, folder_id=folder, filename=path.name, extension='.jpg',
                                    file_size=path.stat().st_size, file_mtime=path.stat().st_mtime)
        detection_store.write_detection_batch(db, pid, 'test', [
            {'box': {'x': .1, 'y': .1, 'w': .2, 'h': .2}, 'confidence': .9, 'category': 'animal'},
            {'box': {'x': .65, 'y': .1, 'w': .2, 'h': .3}, 'confidence': .052, 'category': 'animal'},
        ])
        db.set_photo_edit_recipe(pid, {'crop': {'x': .1, 'y': .1, 'w': .5, 'h': .5}, 'rotation': 90})
        db.commit()
        pids.append(pid)
    before = {pid: (dict(db.photos.get(pid)), db.get_photo_edit_recipe(pid)) for pid in pids}
    db.commit()
    page.goto(live_server['url'] + f'/best-batch?photo_id={pids[0]}')
    page.locator('#compareSubjectsBtn').click()
    expect(page.locator('#inspectImg')).to_have_attribute('src', f'/photos/{pids[0]}/preview?size=1920&view=source')
    page.wait_for_function('document.getElementById("inspectImg").naturalWidth > 0')
    expect(page.locator('#focusRasterStatus')).to_contain_text('Selected subject evidence is unavailable')
    page.locator('#headFocusBtn').click()
    expect(page.locator('#focusSelectionStatus')).to_contain_text('head-centered view')
    expect(page.locator('#focusEvidenceStatus')).to_contain_text('before rejecting the photo')
    bounds = page.locator('#inspectImg').bounding_box()
    assert abs(bounds['width'] / bounds['height'] - image_size[0] / image_size[1]) < .01
    page.locator('#inspectImg').click(position={'x': bounds['width'] * .75, 'y': bounds['height'] * .25})
    expect(page.locator('#focusSelectionStatus')).to_contain_text('Manual center:')
    center_text = page.locator('#focusSelectionStatus').inner_text().split(': ', 1)[1].split('. Inspection')[0]
    x, y = map(float, center_text.split(', '))
    assert abs(x - .75) < .005 and abs(y - .25) < .005
    expect(page.locator('#focusRasterStatus')).to_contain_text('Basis: point')
    expect(page.locator('#focusEvidenceStatus')).to_contain_text('Manual inspection center')
    expect(page.locator('#focusRasterStatus')).to_contain_text('source: original')
    expect(page.locator('#focusRasterStatus')).to_contain_text(f'raster: {image_size[0]},{image_size[1]}')
    expect(page.locator('#focusCenter')).to_be_visible()
    marker = page.locator('#focusCenter').bounding_box()
    bounds = page.locator('#inspectImg').bounding_box()
    assert abs(marker['x'] + marker['width'] / 2 - (bounds['x'] + x * bounds['width'])) < 1
    assert abs(marker['y'] + marker['height'] / 2 - (bounds['y'] + y * bounds['height'])) < 1
    page.locator('#resetFocusBtn').click()
    expect(page.locator('#focusRasterStatus')).to_contain_text('Selected subject evidence is unavailable')
    page.locator('#compareSubjectsBtn').click()
    expect(page.locator('#subjectChoices option')).to_have_count(3)
    page.locator('#subjectChoices').select_option(index=2)
    expect(page.locator('#focusRasterStatus')).to_contain_text('Selected subject evidence is unavailable')
    expect(page.locator('#focusSubjectBox')).to_be_visible()
    outline = page.locator('#focusSubjectBox').bounding_box()
    bounds = page.locator('#inspectImg').bounding_box()
    assert abs(outline['x'] - (bounds['x'] + bounds['width'] * .65)) < 1
    assert abs(outline['y'] - (bounds['y'] + bounds['height'] * .1)) < 1
    assert abs(outline['width'] - bounds['width'] * .2) < 1
    assert abs(outline['height'] - bounds['height'] * .3) < 1
    page.wait_for_function('document.getElementById("chosenSubjectImg").naturalWidth > 0')
    page.locator('#manualFocusBtn').click()
    bounds = page.locator('#inspectImg').bounding_box()
    page.locator('#inspectImg').click(position={'x': bounds['width'] * .75, 'y': bounds['height'] * .25})
    expect(page.locator('#focusRasterStatus')).to_contain_text('Basis: point')
    expect(page.locator('#focusSubjectBox')).to_be_visible()
    for pid in pids:
        after = dict(db.photos.get(pid))
        assert after['flag'] == before[pid][0]['flag']
        assert db.get_photo_edit_recipe(pid) == before[pid][1]
        assert db.subjects.choice(pid) is None
    screenshot = Path(__file__).resolve().parents[2] / '.context' / 'focus-inspection.png'
    screenshot.parent.mkdir(exist_ok=True)
    page.screenshot(path=str(screenshot), full_page=True)


def test_manual_focus_uses_available_working_raster_with_offline_originals(page, live_server, tmp_path):
    db = live_server['db']
    app = live_server['app']
    app.config['EDIT_PREVIEW_IN_PROCESS'] = True
    page.set_viewport_size({'width': 800, 'height': 700})
    folder = workspace_folders.add_folder(db, str(tmp_path / 'offline-originals'), name='Offline focus')
    pids = []
    working_dir = Path(app.config['THUMB_CACHE_DIR']).parent / 'working'
    working_dir.mkdir(exist_ok=True)
    for index in range(2):
        pid = photo_ingest.add_photo(db, folder_id=folder, filename=f'DSC_{index:04d}.jpg',
                                    extension='.jpg', file_size=1000, file_mtime=1)
        image = Image.new('RGB', (300, 200), 'blue')
        image.paste('red', (200, 20, 250, 70))
        image.save(working_dir / f'{pid}.jpg')
        db.conn.execute('UPDATE photos SET working_copy_path=?,width=600,height=400 WHERE id=?',
                        (f'working/{pid}.jpg', pid))
        pids.append(pid)
    db.commit()
    before = {pid: (dict(db.photos.get(pid)), db.get_photo_edit_recipe(pid)) for pid in pids}
    page.goto(live_server['url'] + f'/best-batch?photo_id={pids[0]}')
    page.locator('#compareSubjectsBtn').click()
    page.wait_for_function('document.getElementById("inspectImg").naturalWidth > 0')
    page.locator('#manualFocusBtn').click()
    bounds = page.locator('#inspectImg').bounding_box()
    with page.expect_response(lambda response: f'/photos/{pids[0]}/focus-crop?' in response.url and 'center=' in response.url) as request:
        page.locator('#inspectImg').click(position={'x': bounds['width'] * .75, 'y': bounds['height'] * .25})
    response = request.value
    assert response.status == 200
    assert response.headers['x-vireo-focus-basis'] == 'point'
    assert response.headers['x-vireo-focus-source'] == 'working_copy'
    assert response.headers['x-vireo-focus-raster-size'] == '300,200'
    assert response.headers['x-vireo-focus-original-scale'] == '0.5000'
    expect(page.locator('#focusRasterStatus')).to_contain_text('source: working_copy')
    expect(page.locator('#focusRasterStatus')).to_contain_text('original scale: 0.5000')
    expect(page.locator('#inspectedFocusImg')).to_be_visible()
    for pid in pids:
        assert db.photos.get(pid)['flag'] == before[pid][0]['flag']
        assert db.get_photo_edit_recipe(pid) == before[pid][1]
        assert db.subjects.choice(pid) is None
