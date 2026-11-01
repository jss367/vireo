"""Resumable wildlife inspection shares the catalog's decisions, never replays them."""
import json
import threading
from pathlib import Path

from PIL import Image
from playwright.sync_api import expect
from services import photo_ingest, workspace_folders
from testing.waits import synchronization_timeout

from vireo.tests.wait import wait_for_job_via_runner


def seed_queue(live_server, tmp_path):
    db = live_server['db']
    live_server['app'].config['EDIT_PREVIEW_IN_PROCESS'] = True
    folder = workspace_folders.add_folder(db, str(tmp_path), name='Queue inspection')
    pids, subjects = [], []
    for index in range(2):
        path = tmp_path / f'queue-{index}.jpg'
        image = Image.new('RGB', (1000, 700), 'navy')
        image.paste('orange', (650, 100, 850, 350))
        image.save(path)
        pid = photo_ingest.add_photo(db, folder_id=folder, filename=path.name, extension='.jpg',
                                    file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
                                    width=1000, height=700)
        detections = db.detections.save(pid, [
            {'box': {'x': .1, 'y': .1, 'w': .2, 'h': .2}, 'confidence': .95, 'category': 'animal'},
            {'box': {'x': .65, 'y': .1, 'w': .2, 'h': .3}, 'confidence': .85, 'category': 'animal'},
        ], detector_model='test-detector')
        db.commit()
        for detection in detections:
            db.predictions.add(detection_id=detection, species='American Robin', confidence=.92,
                               model='BioCLIP-2', labels_fingerprint='queue-test')
            db.commit()
        pids.append(pid)
        subjects.append(detections)
    db.photo_review.set_flag(pids[0], 'flagged')
    db.commit()
    db.photo_review.set_flag(pids[1], 'rejected')
    db.commit()
    collection = db.collections.add('Inspection queue test', json.dumps([{'field': 'photo_ids', 'value': pids}]))
    db.commit()
    return pids, subjects, collection


def open_queue(page, live_server, collection):
    page.goto(live_server['url'] + '/review')
    expect(page.locator('.card')).to_have_count(5 + 4)
    page.locator('#collectionFilter').select_option(str(collection))
    expect(page.locator('.card')).to_have_count(4)
    page.locator('#rqEnabled').check()
    expect(page.locator('#rqFlag')).to_contain_text('Current catalog flag: flagged')


def test_review_queue_resumes_scope_position_subject_manual_center_and_unresolved(page, live_server, tmp_path):
    pids, subjects, collection = seed_queue(live_server, tmp_path)
    open_queue(page, live_server, collection)
    page.locator('#sortSelect').select_option('confidence_desc')
    page.locator('#confFilter').evaluate("el => { el.value = '50'; el.dispatchEvent(new Event('input', {bubbles:true})); }")
    page.locator('.vf-search input').fill('queue')
    page.wait_for_function('VireoFilter.hasFilters() && !_predictionsReloading && reviewQueue.current !== null')
    page.wait_for_function("""async () => {
      const ws = await (await fetch('/api/workspaces/active')).json();
      const saved = typeof ws.ui_state === 'string' ? JSON.parse(ws.ui_state) : ws.ui_state;
      return saved?.universal_filters?.review?.root?.rules?.length > 0;
    }""")
    expect(page.locator('#rqProgress')).to_contain_text('scope: 2 photos')
    expect(page.locator('#rqProgress')).to_contain_text('decided: 2')
    expect(page.locator('#rqProgress')).to_contain_text('inspected here: 0')
    expect(page.locator('#rqCropStatus')).to_contain_text('Source: original')
    page.locator('#rqNext').click()
    expect(page.locator('#rqFlag')).to_contain_text('Current catalog flag: rejected')
    page.locator('#rqSubject').select_option(str(subjects[1][1]))
    expect(page.locator('#rqAnalysis')).to_contain_text('No cached box analysis')
    expect(page.locator('#rqAnalysis')).to_contain_text('head or eye may be outside the crop')
    expect(page.locator('#rqAnalysis')).to_contain_text('No current subject-bound eye evidence')
    expect(page.locator('#rqSubjectBox')).to_be_visible()
    assert page.locator('#rqSubjectBox').evaluate('el => el.style.left') == '65%'
    page.locator('#rqUnresolved').check()
    page.locator('#rqManual').click()
    page.wait_for_function('document.getElementById("rqFull").naturalWidth > 0')
    bounds = page.locator('#rqFull').bounding_box()
    page.locator('#rqFull').click(position={'x': bounds['width'] * .76, 'y': bounds['height'] * .26})
    expect(page.locator('#rqAnalysis')).to_contain_text('Using the saved manual center')
    expect(page.locator('#rqCrop')).to_be_visible()
    center = page.evaluate('rqItem(reviewQueue.current.id).center')
    assert abs(center[0] - .76) < .01 and abs(center[1] - .26) < .01
    page.reload()
    expect(page.locator('#rqEnabled')).to_be_checked()
    expect(page.locator('#collectionFilter')).to_have_value(str(collection))
    expect(page.locator('#sortSelect')).to_have_value('confidence_desc')
    expect(page.locator('#confFilter')).to_have_value('50')
    page.wait_for_function('reviewQueue.current?.id === ' + str(pids[1]))
    assert page.evaluate('VireoFilter.hasFilters()')
    expect(page.locator('#rqFilename')).to_have_text('queue-1.jpg')
    expect(page.locator('#rqSubject')).to_have_value(str(subjects[1][1]))
    expect(page.locator('#rqUnresolved')).to_be_checked()
    expect(page.locator('#rqProgress')).to_contain_text('inspected here: 1')
    expect(page.locator('#rqProgress')).to_contain_text('needs follow-up here: 1')
    assert page.evaluate('rqItem(reviewQueue.current.id).center') == center
    assert [live_server['db'].photos.get(pid)['flag'] for pid in pids] == ['flagged', 'rejected']
    assert all(live_server['db'].subjects.choice(pid) is None for pid in pids)
    screenshot = Path(__file__).resolve().parents[2] / '.context' / 'review-queue.png'
    screenshot.parent.mkdir(exist_ok=True)
    page.locator('#reviewQueue').screenshot(path=str(screenshot))


def test_review_queue_missing_pixels_and_concurrent_flags_are_preserved(page, live_server, tmp_path):
    pids, _, collection = seed_queue(live_server, tmp_path)
    open_queue(page, live_server, collection)
    page.route('**/photos/*/focus-crop?*', lambda route: route.fulfill(status=404, json={'error': 'Working crop unavailable'}))
    page.locator('#rqSource').select_option('working')
    expect(page.locator('#rqCropStatus')).to_have_text('Working crop unavailable')
    expect(page.locator('#rqCrop')).to_be_hidden()
    expect(page.locator('#rqFull')).to_be_visible()
    assert live_server['db'].photos.get(pids[0])['flag'] == 'flagged'
    live_server['db'].photo_review.set_flag(pids[0], 'rejected')
    live_server["db"].commit()
    page.locator('[data-rq-flag="flagged"]').click()
    expect(page.locator('#rqFlag')).to_contain_text('flag changed elsewhere')
    assert live_server['db'].photos.get(pids[0])['flag'] == 'rejected'
    expect(page.locator('[data-rq-flag="flagged"]')).to_be_disabled()
    page.locator('#rqRefresh').click()
    expect(page.locator('#rqFlag')).to_contain_text('Current catalog flag: rejected')
    page.locator('[data-rq-flag="flagged"]').click()
    expect(page.locator('#rqFlag')).to_contain_text('Current catalog flag: flagged')
    assert live_server['db'].photos.get(pids[0])['flag'] == 'flagged'


def test_transfer_blocked_review_choice_survives_reload_until_explicit_save(page, live_server, tmp_path):
    pids, _, collection = seed_queue(live_server, tmp_path)
    open_queue(page, live_server, collection)
    runner = live_server['app']._job_runner
    release = threading.Event()
    transfer, _, _ = runner.start_singleton(
        'send-to-nas', lambda job: release.wait(synchronization_timeout(60)),
        singleton_key='archive', workspace_id=live_server['db'].active_workspace_id,
        exclusive_workspace=True,
    )
    writes = []
    page.on('request', lambda request: writes.append(request.url) if request.url.endswith('/flag') else None)
    try:
        page.locator('[data-rq-flag="rejected"]').click()
        expect(page.locator('#rqFlag')).to_contain_text(f'flag edit for photo {pids[0]}')
        expect(page.locator('#rqFlag')).to_contain_text('NAS transfer')
        expect(page.locator('#rqPendingFlag')).to_contain_text('Unsaved review choice: Reject photo')
        expect(page.locator('[data-rq-flag="rejected"]')).to_be_disabled()
        expect(page.locator('#rqProgress')).to_contain_text('inspected here: 0')
        page.reload()
        expect(page.locator('#rqFlag')).to_contain_text('Current catalog flag: flagged')
        expect(page.locator('#rqPendingFlag')).to_contain_text('Unsaved review choice: Reject photo')
        expect(page.locator('#rqDiscardPendingFlag')).to_be_visible()
        assert len(writes) == 1
        assert live_server['db'].photos.get(pids[0])['flag'] == 'flagged'
        screenshot = Path(__file__).resolve().parents[2] / '.context' / 'flag-transfer-pending.png'
        screenshot.parent.mkdir(exist_ok=True)
        page.locator('#reviewQueue').screenshot(path=str(screenshot))
    finally:
        release.set()
        assert wait_for_job_via_runner(runner, transfer)['status'] == 'completed'
    page.locator('#rqRefresh').click()
    expect(page.locator('#rqFlag')).to_contain_text('Current catalog flag: flagged')
    expect(page.locator('#rqPendingFlag')).to_contain_text('Unsaved review choice: Reject photo')
    assert len(writes) == 1
    page.locator('[data-rq-flag="rejected"]').click()
    expect(page.locator('#rqFlag')).to_contain_text('Current catalog flag: rejected')
    expect(page.locator('#rqPendingFlag')).to_be_empty()
    expect(page.locator('#rqDiscardPendingFlag')).to_be_hidden()
    assert len(writes) == 2
    assert live_server['db'].photos.get(pids[0])['flag'] == 'rejected'


def test_invalid_filter_handoff_keeps_review_closed(page, live_server):
    page.goto(live_server['url'] + '/review?filters=invalid')
    expect(page.locator('#toastContainer')).to_contain_text('Could not initialize Review')
    expect(page.locator('.card')).to_have_count(0)
    expect(page.locator('#rqContent')).to_be_hidden()


def test_review_queue_workspace_and_collection_inspection_do_not_leak(page, live_server, tmp_path):
    _, _, collection = seed_queue(live_server, tmp_path)
    open_queue(page, live_server, collection)
    page.locator('#rqInspected').click()
    expect(page.locator('#rqProgress')).to_contain_text('inspected here: 1')
    page.locator('#collectionFilter').select_option('all')
    expect(page.locator('#rqProgress')).to_contain_text('inspected here: 0')
    page.locator('#collectionFilter').select_option(str(collection))
    expect(page.locator('#rqProgress')).to_contain_text('inspected here: 1')
    other = live_server['db'].create_workspace('Other inspection scope')
    page.request.post(live_server['url'] + f'/api/workspaces/{other}/activate')
    page.reload()
    expect(page.locator('#rqEnabled')).not_to_be_checked()
    expect(page.locator('#collectionFilter')).to_have_value('all')
    page.locator('#rqEnabled').check()
    expect(page.locator('#rqProgress')).to_contain_text('inspected here: 0')
