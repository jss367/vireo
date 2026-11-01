"""Primary subject selection never silently edits the photo."""
from pathlib import Path

import numpy as np
from PIL import Image
from playwright.sync_api import expect
from services import detection_store, photo_ingest, workspace_folders


def test_choose_subject_preview_and_apply_suggestion(live_server, page, tmp_path):
    from subjects import analyze_photo, payload

    db = live_server['db']
    folder = tmp_path / 'subjects'
    folder.mkdir()
    path = folder / 'two-subjects.jpg'
    pixels = np.full((400, 640, 3), (25, 45, 55), dtype=np.uint8)
    pixels[80:300, 40:250] = (95, 105, 115)
    texture = np.indices((220, 210)).sum(axis=0) // 8 % 2
    pixels[80:300, 380:590] = np.where(texture[..., None], (190, 160, 80), (65, 95, 95))
    Image.fromarray(pixels).save(path)
    folder_id = workspace_folders.add_folder(db, str(folder))
    photo_id = photo_ingest.add_photo(db, folder_id, path.name, extension='.jpg',
                           file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
                           width=640, height=400)
    ids = detection_store.write_detection_batch(db, photo_id, 'megadetector-v6', [
        {'box': {'x': .06, 'y': .2, 'w': .33, 'h': .55}, 'confidence': .95, 'category': 'animal'},
        {'box': {'x': .59, 'y': .2, 'w': .33, 'h': .55}, 'confidence': .8, 'category': 'animal'},
    ])
    analyze_photo(db, photo_id, path)
    db.predictions.add(ids[0], 'American Robin', .91, 'bioclip')
    db.predictions.add(ids[1], 'Blue Jay', .87, 'bioclip')
    db.commit()
    original_recipe = db.set_photo_edit_recipe(photo_id, {'adjustments': {'exposure': .4, 'contrast': 10}})
    db.commit()
    page.goto(live_server['url'] + '/browse')
    page.evaluate("id => openLightbox(id, 'two-subjects.jpg')", photo_id)
    expect(page.locator('#lightboxSubjects')).to_be_visible()
    page.locator('#lightboxSubjectSummary').click()
    cards = page.locator('.lb-subject-card')
    expect(cards).to_have_count(2)
    expect(cards.nth(1)).to_have_attribute('aria-pressed', 'true')
    expect(page.locator('#lightboxSubjectDetails')).to_contain_text('Blue Jay')
    cards.nth(0).click()
    expect(cards.nth(0)).to_have_attribute('aria-pressed', 'true')
    expect(page.locator('#lightboxSubjectStatus')).to_contain_text('chosen by you')
    expect(page.locator('#lightboxSubjectDetails')).to_contain_text('American Robin')
    assert payload(db, photo_id)['primary_detection_id'] == ids[0]
    assert db.get_photo_edit_recipe(photo_id) == original_recipe
    # Switching correction preview is also non-destructive.
    page.locator('#lightboxSubjectCorrected').uncheck()
    assert db.get_photo_edit_recipe(photo_id) == original_recipe
    page.locator('#lightboxSubjectAutomatic').click()
    expect(cards.nth(1)).to_have_attribute('aria-pressed', 'true')
    expect(page.locator('#lightboxSubjectStatus')).to_contain_text('selected by quality')
    page.locator('#lightboxSubjectUseExposure').click()
    page.wait_for_function('!_lbEditWritePending')
    recipe = db.get_photo_edit_recipe(photo_id)
    assert recipe['adjustments']['contrast'] == 10
    assert recipe['adjustments']['exposure'] == payload(db, photo_id)['subjects'][0]['analysis']['exposure_ev']
    page.locator('#lightboxSubjectUseCrop').click()
    page.wait_for_function('!_lbEditWritePending')
    recipe = db.get_photo_edit_recipe(photo_id)
    assert recipe['crop']['x'] > .4
    assert recipe['adjustments']['contrast'] == 10
    page.wait_for_function("document.querySelector('.lb-subject-preview').complete")
    page.screenshot(path=str(Path(__file__).resolve().parents[2] / '.context' / 'subject-selection.png'))


def test_existing_photo_can_analyze_without_rerunning_models(live_server, page, tmp_path):
    db = live_server['db']
    path = tmp_path / 'existing.jpg'
    Image.new('RGB', (320, 240), (80, 95, 90)).save(path)
    folder_id = workspace_folders.add_folder(db, str(tmp_path))
    photo_id = photo_ingest.add_photo(db, folder_id, path.name, extension='.jpg',
                           file_size=path.stat().st_size, file_mtime=1, width=320, height=240)
    detection_store.write_detection_batch(db, photo_id, 'megadetector-v6', [
        {'box': {'x': .1, 'y': .1, 'w': .5, 'h': .5}, 'confidence': .9, 'category': 'animal'},
    ])
    page.goto(live_server['url'] + '/browse')
    page.evaluate("id => openLightbox(id, 'existing.jpg')", photo_id)
    expect(page.locator('#lightboxSubjects')).to_be_visible()
    page.locator('#lightboxSubjectSummary').click()
    page.locator('#lightboxSubjectAnalyze').click()
    expect(page.locator('#lightboxSubjectStatus')).to_contain_text('selected by quality', timeout=30000)
    expect(page.locator('#lightboxSubjectAnalyze')).to_be_hidden()
    assert db.get_photo_edit_recipe(photo_id) is None


def test_rejecting_a_region_as_not_an_animal_promotes_the_other_subject_and_is_reversible(live_server, page, tmp_path):
    """A stump box with a confident species leaves the subject list; Restore and Undo bring it back."""
    from subjects import analyze_photo, payload

    db = live_server['db']
    folder = tmp_path / 'verdicts'
    folder.mkdir()
    path = folder / 'stump-and-bird.jpg'
    pixels = np.full((400, 640, 3), (25, 45, 55), dtype=np.uint8)
    pixels[80:300, 40:250] = (95, 105, 115)
    texture = np.indices((220, 210)).sum(axis=0) // 8 % 2
    pixels[80:300, 380:590] = np.where(texture[..., None], (190, 160, 80), (65, 95, 95))
    Image.fromarray(pixels).save(path)
    folder_id = workspace_folders.add_folder(db, str(folder))
    photo_id = photo_ingest.add_photo(db, folder_id, path.name, extension='.jpg',
                           file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
                           width=640, height=400)
    stump, bird = detection_store.write_detection_batch(db, photo_id, 'megadetector-v6', [
        {'box': {'x': .06, 'y': .2, 'w': .33, 'h': .55}, 'confidence': .95, 'category': 'animal'},
        {'box': {'x': .59, 'y': .2, 'w': .33, 'h': .55}, 'confidence': .8, 'category': 'animal'},
    ])
    analyze_photo(db, photo_id, path)
    db.predictions.add(stump, 'Mule Deer', .93, 'bioclip')
    db.predictions.add(bird, 'Hairy Woodpecker', .61, 'bioclip')
    db.commit()
    page.goto(live_server['url'] + '/browse')
    page.evaluate("id => openLightbox(id, 'stump-and-bird.jpg')", photo_id)
    expect(page.locator('#lightboxSubjects')).to_be_visible()
    page.locator('#lightboxSubjectSummary').click()
    cards = page.locator('.lb-subject-card')
    expect(cards).to_have_count(2)
    expect(cards.nth(0)).to_contain_text('Mule Deer')
    # Reject the left-hand (stump) region.
    page.locator('.lb-subject-reject').nth(0).click()
    expect(cards).to_have_count(1)
    expect(cards.nth(0)).to_contain_text('Hairy Woodpecker')
    expect(cards.nth(0)).to_have_attribute('aria-pressed', 'true')
    expect(page.locator('#lightboxSubjectSummary')).to_contain_text('1 rejected')
    expect(page.locator('#lightboxSubjectRejected')).to_contain_text('was Mule Deer')
    state = payload(db, photo_id)
    assert state['primary_detection_id'] == bird
    assert [r['id'] for r in state['rejected']] == [stump]
    assert db.detections.row(stump)['subject_verdict'] == 'not_animal'
    assert db.edit_history.list_recent()[0]['action_type'] == 'subject_verdict'
    page.screenshot(path=str(Path(__file__).resolve().parents[2] / '.context' / 'subject-rejected.png'))
    # Restore from the panel.
    page.locator('.lb-subject-restore').click()
    expect(cards).to_have_count(2)
    expect(page.locator('#lightboxSubjectRejected')).to_be_empty()
    assert db.detections.row(stump)['subject_verdict'] is None
    # Reject again, then Undo through the app's own history control.
    page.locator('.lb-subject-reject').nth(0).click()
    expect(cards).to_have_count(1)
    assert db.detections.row(stump)['subject_verdict'] == 'not_animal'
    page.evaluate("() => safeFetch('/api/undo', {method: 'POST'})")
    page.wait_for_function(
        "id => fetch('/api/photos/' + id + '/subjects').then(r => r.json()).then(d => d.rejected.length === 0)",
        arg=photo_id,
    )
    assert db.detections.row(stump)['subject_verdict'] is None
