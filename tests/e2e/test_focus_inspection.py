"""Manual focus uses the upright full frame even when saved edits change framing."""
from pathlib import Path

from PIL import Image
from playwright.sync_api import expect
from services import detection_store, photo_ingest


def test_manual_focus_center_and_subject_inspection_preserve_decisions(page, live_server, tmp_path):
    db = live_server['db']
    live_server['app'].config['EDIT_PREVIEW_IN_PROCESS'] = True
    folder = db.add_folder(str(tmp_path), name='Focus inspection')
    pids = []
    for index in range(2):
        path = tmp_path / f'DSC_{index:04d}.jpg'
        image = Image.new('RGB', (600, 400), 'blue')
        image.paste('red', (400, 40, 500, 140))
        image.save(path)
        pid = photo_ingest.add_photo(db, folder_id=folder, filename=path.name, extension='.jpg',
                                    file_size=path.stat().st_size, file_mtime=path.stat().st_mtime)
        detection_store.write_detection_batch(db, pid, 'test', [
            {'box': {'x': .1, 'y': .1, 'w': .2, 'h': .2}, 'confidence': .9, 'category': 'animal'},
            {'box': {'x': .65, 'y': .1, 'w': .2, 'h': .3}, 'confidence': .052, 'category': 'animal'},
        ])
        db.set_photo_edit_recipe(pid, {'crop': {'x': .1, 'y': .1, 'w': .5, 'h': .5}, 'rotation': 90})
        pids.append(pid)
    before = {pid: (dict(db.photos.get(pid)), db.get_photo_edit_recipe(pid)) for pid in pids}
    db.commit()
    page.goto(live_server['url'] + f'/best-batch?photo_id={pids[0]}')
    page.locator('#compareSubjectsBtn').click()
    expect(page.locator('#inspectImg')).to_have_attribute('src', f'/photos/{pids[0]}/edit-preview?size=1920&recipe=%7B%7D')
    page.wait_for_function('document.getElementById("inspectImg").naturalWidth > 0')
    expect(page.locator('#focusRasterStatus')).to_contain_text('Selected subject evidence is unavailable')
    page.locator('#manualFocusBtn').click()
    bounds = page.locator('#inspectImg').bounding_box()
    page.locator('#inspectImg').click(position={'x': bounds['width'] * .75, 'y': bounds['height'] * .25})
    expect(page.locator('#focusSelectionStatus')).to_contain_text('Manual center:')
    center_text = page.locator('#focusSelectionStatus').inner_text().split(': ', 1)[1].split('. Inspection')[0]
    x, y = map(float, center_text.split(', '))
    assert abs(x - .75) < .005 and abs(y - .25) < .005
    expect(page.locator('#focusRasterStatus')).to_contain_text('Basis: point')
    expect(page.locator('#focusRasterStatus')).to_contain_text('source: original')
    expect(page.locator('#focusRasterStatus')).to_contain_text('raster: 600,400')
    expect(page.locator('#focusCenter')).to_be_visible()
    page.locator('#resetFocusBtn').click()
    expect(page.locator('#focusRasterStatus')).to_contain_text('Selected subject evidence is unavailable')
    page.locator('#compareSubjectsBtn').click()
    expect(page.locator('#subjectChoices option')).to_have_count(3)
    page.locator('#subjectChoices').select_option(index=2)
    expect(page.locator('#focusRasterStatus')).to_contain_text('Selected subject evidence is unavailable')
    expect(page.locator('#focusSubjectBox')).to_be_visible()
    page.wait_for_function('document.getElementById("chosenSubjectImg").naturalWidth > 0')
    page.locator('#manualFocusBtn').click()
    bounds = page.locator('#inspectImg').bounding_box()
    page.locator('#inspectImg').click(position={'x': bounds['width'] * .75, 'y': bounds['height'] * .25})
    expect(page.locator('#focusRasterStatus')).to_contain_text('Basis: point')
    for pid in pids:
        after = dict(db.photos.get(pid))
        assert after['flag'] == before[pid][0]['flag']
        assert db.get_photo_edit_recipe(pid) == before[pid][1]
        assert db.subjects.choice(pid) is None
    screenshot = Path(__file__).resolve().parents[2] / '.context' / 'focus-inspection.png'
    screenshot.parent.mkdir(exist_ok=True)
    page.screenshot(path=str(screenshot), full_page=True)
