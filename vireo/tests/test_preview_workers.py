"""Real spawned-process cancellation, isolation, bounded admission and recovery."""

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from services.preview_workers import PreviewWorkers, parse_request


def render_probe(payload, output):
    if payload.get('started'):
        Path(payload['started']).write_text(str(os.getpid()))
    if payload.get('crash'):
        os._exit(7)
    if payload.get('hang'):
        time.sleep(60)
    Path(output).write_bytes(str(payload.get('value', os.getpid())).encode())
    return 200, '', 'probe'


def wait_until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, 'condition did not become true'
        time.sleep(.01)


@pytest.fixture
def pool():
    pool = PreviewWorkers(render_probe, workers=1, timeout=10)
    yield pool
    pool.close()


def test_reuses_healthy_worker_and_rejects_out_of_order_requests(pool):
    first = pool.render({}, 'a', 1)
    assert first[0] == 200
    assert pool.render({}, 'a', 2) == first
    assert pool.render({}, 'a', 1)[0] == 409
    pool.cancel('late', 5)
    assert pool.render({}, 'late', 4)[0] == 409


def test_newer_request_terminates_stalled_native_work_and_replaces_worker(pool, tmp_path):
    marker = tmp_path / 'started'
    with ThreadPoolExecutor(2) as threads:
        old = threads.submit(pool.render, {'hang': True, 'started': str(marker)}, 'a', 1)
        wait_until(marker.exists)
        old_pid = int(marker.read_text())
        current = pool.render({}, 'a', 2)
        assert old.result()[0] == 409
        assert current[0] == 200
        assert int(current[1]) != old_pid


def test_tabs_are_independent_and_explicit_cancel_fences_late_get(tmp_path):
    pool = PreviewWorkers(render_probe, workers=2, timeout=10)
    marker = tmp_path / 'started'
    try:
        with ThreadPoolExecutor(1) as threads:
            old = threads.submit(pool.render, {'hang': True, 'started': str(marker)}, 'first-tab', 1)
            wait_until(marker.exists)
            assert pool.render({'value': 'second'}, 'second-tab', 1)[1] == b'second'
            assert not old.done()
            pool.cancel('first-tab', 3)
            assert old.result()[0] == 409
            assert pool.render({}, 'first-tab', 2)[0] == 409
    finally:
        pool.close()


def test_queue_is_bounded_and_only_latest_queued_edit_survives(tmp_path):
    pool = PreviewWorkers(render_probe, workers=1, pending=1, timeout=10)
    marker = tmp_path / 'started'
    try:
        with ThreadPoolExecutor(3) as threads:
            running = threads.submit(pool.render, {'hang': True, 'started': str(marker)}, 'running', 1)
            wait_until(marker.exists)
            stale = threads.submit(pool.render, {}, 'queued', 1)
            wait_until(lambda: len(pool._queue) == 1)
            assert pool.render({}, 'excess', 1)[0] == 503
            latest = threads.submit(pool.render, {'value': 'latest'}, 'queued', 2)
            assert stale.result()[0] == 409
            pool.cancel('running', 2)
            assert running.result()[0] == 409
            assert latest.result()[1] == b'latest'
    finally:
        pool.close()


def test_timeout_crash_and_shutdown_release_workers(tmp_path):
    pool = PreviewWorkers(render_probe, workers=1, timeout=1)
    try:
        assert pool.render({'hang': True}, 'a', 1)[0] == 504
        pool.timeout = 10
        assert pool.render({'crash': True}, 'a', 2)[0] == 500
        assert pool.render({'value': 'recovered'}, 'a', 3)[1] == b'recovered'
        marker = tmp_path / 'started'
        with ThreadPoolExecutor(1) as threads:
            active = threads.submit(pool.render, {'hang': True, 'started': str(marker)}, 'a', 4)
            wait_until(marker.exists)
            pool.close()
            assert active.result()[0] == 503
            assert all(not thread.is_alive() for thread in pool._threads)
        assert pool.render({}, 'a', 5)[0] == 503
    finally:
        pool.close()


def test_idle_worker_releases_memory_and_restarts():
    import psutil

    pool = PreviewWorkers(render_probe, workers=1, idle_timeout=.1, timeout=10)
    try:
        first_pid = int(pool.render({}, 'a', 1)[1])
        wait_until(lambda: not psutil.pid_exists(first_pid))
        assert int(pool.render({}, 'a', 2)[1]) != first_pid
    finally:
        pool.close()


def test_two_workers_keep_session_cache_affinity(tmp_path):
    pool = PreviewWorkers(render_probe, workers=2, timeout=10)
    marker = tmp_path / 'started'
    try:
        with ThreadPoolExecutor(1) as threads:
            first = threads.submit(pool.render, {'hang': True, 'started': str(marker)}, 'a', 1)
            wait_until(marker.exists)
            second_pid = pool.render({}, 'b', 1)[1]
            pool.cancel('a', 2)
            assert first.result()[0] == 409
            assert pool.render({}, 'b', 2)[1] == second_pid
    finally:
        pool.close()


def test_cancel_abandons_blocked_working_copy_guard(pool):
    from working_copy_cache import working_copy_publication_guard

    waiting = threading.Event()

    def guard(cancelled):
        waiting.set()
        return working_copy_publication_guard(cancelled=cancelled.is_set)

    with ThreadPoolExecutor(1) as threads, working_copy_publication_guard():
        blocked = threads.submit(pool.render, {}, 'a', 1, guard=guard)
        assert waiting.wait(10)
        pool.cancel('a', 2)
        assert blocked.result()[0] == 409
        # The next job can run before the cache publisher releases its lock.
        assert pool.render({'value': 'latest'}, 'a', 3)[1] == b'latest'


@pytest.mark.parametrize('session, sequence', [('x', '1'), ('a'*32, '-1'), ('a'*32, 'True'), ('a'*32, '1'*16)])
def test_bad_request_identity_is_rejected(session, sequence):
    with pytest.raises(ValueError):
        parse_request(session, sequence)


def test_real_worker_endpoint_preserves_pixels_and_checks_workspace(client_with_photo):
    app, db, photo_id = client_with_photo
    client = app.test_client()
    url = f'/photos/{photo_id}/edit-preview?size=512&recipe={{"adjustments":{{"shadows":15}}}}'
    inline = client.get(url)
    app.config['EDIT_PREVIEW_IN_PROCESS'] = False
    response = client.get(url)
    assert response.status_code == 200
    assert response.headers['X-Vireo-Preview-Source'] == 'srgb'
    assert response.data == inline.data
    assert client.get('/photos/999999/edit-preview').status_code == 404
    session = 'f'*32
    assert client.post('/api/edit-preview/cancel', json={'session': session, 'sequence': 3}).status_code == 204
    assert client.get(url + f'&preview_session={session}&preview_seq=2').status_code == 409
    assert client.get(url + '&preview_session=invalid').status_code == 400
    assert client.post('/api/edit-preview/cancel', json=['invalid']).status_code == 400
    other = db.create_workspace('Other')
    assert client.post(f'/api/workspaces/{other}/activate').status_code == 200
    assert client.get(url).status_code == 404


def test_real_worker_preserves_offline_working_copy_crop_and_local_mask(client_with_photo, tmp_path):
    import local_masks
    from PIL import Image

    app, db, photo_id = client_with_photo
    root = Path(app.config['DB_PATH']).parent
    photo = db.get_photo(photo_id)
    original = Path(db.get_folder(photo['folder_id'])['path']) / photo['filename']
    copy = root / 'working' / f'{photo_id}.jpg'
    copy.parent.mkdir()
    original.replace(copy)
    db.record_generated_original(photo_id, str(copy.relative_to(root)), tracked=True)
    mask_path = tmp_path / 'mask.png'
    mask_image = Image.new('L', (800, 600))
    mask_image.paste(255, (0, 0, 400, 600))
    mask_image.save(mask_path)
    mask = local_masks.create_snapshot(photo_id=photo_id, mask_row={'path': str(mask_path)},
                                       vireo_dir=str(root), native_size=(800, 600))
    recipe = {'crop': {'x': .1, 'y': .1, 'w': .8, 'h': .8},
              'local': {'mask': mask, 'regions': [{'region': 'subject', 'adjustments': {'exposure': 1}}]}}
    client = app.test_client()
    url = f'/photos/{photo_id}/edit-preview'
    params = {'size': 512, 'apply_crop': 1, 'recipe': json.dumps(recipe)}
    inline = client.get(url, query_string=params)
    app.config['EDIT_PREVIEW_IN_PROCESS'] = False
    response = client.get(url, query_string=params)
    assert inline.status_code == response.status_code == 200
    assert response.data == inline.data
    assert copy.exists()
