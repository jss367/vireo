"""Tests for job submission and history API routes."""

import json
import os
import sys
import threading
import time
from types import SimpleNamespace

from PIL import Image
from wait import wait_for_job_phase, wait_for_job_via_client, wait_for_job_via_runner


def _stub_classify_job(monkeypatch, sleep=0.2):
    import classify_job

    def fake_run_classify_job(*args, **kwargs):
        time.sleep(sleep)
        return {"ok": True}

    monkeypatch.setattr(classify_job, "run_classify_job", fake_run_classify_job)


def _set_onnx_providers(monkeypatch, providers):
    fake_ort = SimpleNamespace(
        __version__="1.test",
        get_available_providers=lambda: list(providers),
    )
    monkeypatch.setitem(sys.modules, "onnxruntime", fake_ort)


def _collection_with_photo_count(db, count):
    fid = db.get_folder_tree()[0]["id"]
    photo_ids = [
        r["id"]
        for r in db.conn.execute("SELECT id FROM photos ORDER BY id").fetchall()
    ]
    while len(photo_ids) < count:
        idx = len(photo_ids)
        photo_ids.append(
            db.add_photo(
                folder_id=fid,
                filename=f"runtime-warning-{idx}.jpg",
                extension=".jpg",
                file_size=1000 + idx,
                file_mtime=float(idx),
            )
        )
    return db.add_collection(
        f"runtime-warning-{count}",
        json.dumps([{"field": "photo_ids", "value": photo_ids[:count]}]),
    )


def _job_from_response(client, job_id):
    data = client.get("/api/jobs").get_json()
    return next(j for j in data["active"] if j["id"] == job_id)


def test_job_thumbnails_returns_job_id(app_and_db):
    """POST /api/jobs/thumbnails returns job_id starting with 'thumbnails-'."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/thumbnails")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("thumbnails-")


def test_large_classify_job_gets_cpu_only_runtime_warning(
    app_and_db, monkeypatch,
):
    """Large ML jobs expose a CPU-only warning with provider details."""
    _stub_classify_job(monkeypatch)
    _set_onnx_providers(monkeypatch, ["CPUExecutionProvider"])
    app, db = app_and_db
    cid = _collection_with_photo_count(db, 30)

    client = app.test_client()
    resp = client.post("/api/jobs/classify", json={"collection_id": cid})
    assert resp.status_code == 200

    job = _job_from_response(client, resp.get_json()["job_id"])
    warning = job["runtime_warning"]
    assert warning["title"] == "Using CPU only"
    assert warning["onnxruntime_providers"] == ["CPUExecutionProvider"]
    assert "Available ONNX Runtime providers: CPUExecutionProvider" in warning["detail"]


def test_large_classify_job_skips_warning_when_gpu_provider_present(
    app_and_db, monkeypatch,
):
    """A GPU-capable ONNX Runtime provider suppresses the CPU-only warning."""
    _stub_classify_job(monkeypatch)
    _set_onnx_providers(monkeypatch, ["CUDAExecutionProvider", "CPUExecutionProvider"])
    app, db = app_and_db
    cid = _collection_with_photo_count(db, 30)

    client = app.test_client()
    resp = client.post("/api/jobs/classify", json={"collection_id": cid})
    assert resp.status_code == 200

    job = _job_from_response(client, resp.get_json()["job_id"])
    assert job["runtime_warning"] is None


def test_large_classify_job_skips_warning_when_directml_provider_present(
    app_and_db, monkeypatch,
):
    """Non-CUDA/CoreML accelerated ONNX providers also suppress warnings."""
    _stub_classify_job(monkeypatch)
    _set_onnx_providers(monkeypatch, ["DmlExecutionProvider", "CPUExecutionProvider"])
    app, db = app_and_db
    cid = _collection_with_photo_count(db, 30)

    client = app.test_client()
    resp = client.post("/api/jobs/classify", json={"collection_id": cid})
    assert resp.status_code == 200

    job = _job_from_response(client, resp.get_json()["job_id"])
    assert job["runtime_warning"] is None


def test_small_classify_job_does_not_show_cpu_only_warning(
    app_and_db, monkeypatch,
):
    """Small jobs keep CPU-only execution quiet to avoid warning noise."""
    _stub_classify_job(monkeypatch)
    _set_onnx_providers(monkeypatch, ["CPUExecutionProvider"])
    app, db = app_and_db
    cid = _collection_with_photo_count(db, 3)

    client = app.test_client()
    resp = client.post("/api/jobs/classify", json={"collection_id": cid})
    assert resp.status_code == 200

    job = _job_from_response(client, resp.get_json()["job_id"])
    assert job["runtime_warning"] is None


def test_cpu_only_runtime_warning_can_be_dismissed(app_and_db, monkeypatch):
    """Dismissing a warning is client-local and does not suppress API data."""
    _stub_classify_job(monkeypatch, sleep=0.4)
    _set_onnx_providers(monkeypatch, ["CPUExecutionProvider"])
    app, db = app_and_db
    cid = _collection_with_photo_count(db, 30)

    client = app.test_client()
    resp = client.post("/api/jobs/classify", json={"collection_id": cid})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    warning = _job_from_response(client, job_id)["runtime_warning"]
    assert warning["id"] == "cpu-only-ml"

    dismiss = client.post(
        "/api/jobs/runtime-warning/dismiss",
        json={"id": warning["id"]},
    )
    assert dismiss.status_code == 200

    job = _job_from_response(client, job_id)
    assert job["runtime_warning"]["id"] == "cpu-only-ml"


def test_precompute_embedding_warning_sizing_ignores_decode_errors(
    app_and_db, monkeypatch, tmp_path,
):
    """A non-UTF-8 labels file must not make job submission fail."""
    _set_onnx_providers(monkeypatch, ["CPUExecutionProvider"])
    labels_file = tmp_path / "labels.txt"
    labels_file.write_bytes(b"\xff\xfe\x00")

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/jobs/precompute-embeddings",
        json={"model_id": "missing-model", "labels_file": str(labels_file)},
    )
    assert resp.status_code == 200
    assert resp.get_json()["job_id"].startswith("precompute-embeddings-")


def test_job_cull_returns_job_id(app_and_db):
    """POST /api/jobs/cull with empty json returns job_id starting with 'cull-'."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/cull", json={})
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("cull-")


def test_job_cull_passes_thumb_cache_parent_as_vireo_dir(tmp_path, monkeypatch):
    """api_job_cull must derive vireo_dir from THUMB_CACHE_DIR's parent,
    matching scan/classify — not from db_path's parent. Users who pass a
    custom --thumb-dir on a filesystem separate from the DB must still
    have culling find their working copies in the right place."""
    import config as cfg
    import culling as culling_module
    from app import create_app
    from db import Database

    monkeypatch.setenv("HOME", str(tmp_path))
    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    # DB and thumbnails live under DIFFERENT parents — the bug case.
    db_dir = tmp_path / "db_root"
    db_dir.mkdir()
    db_path = str(db_dir / "app.db")

    thumb_parent = tmp_path / "thumb_root"
    thumb_parent.mkdir()
    thumb_dir = str(thumb_parent / "thumbnails")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)

    captured = {}
    empty_result = {
        "species_groups": [],
        "total_photos": 0,
        "suggested_keepers": 0,
        "suggested_rejects": 0,
        "photos_missing_phash": 0,
    }

    def spy(spy_db, **kwargs):
        # culling.analyze_for_culling is monkeypatched globally, so any other
        # test's still-running cull job in the same xdist worker would race
        # into this spy and poison `captured` with its own vireo_dir. Only
        # record calls that came from this test's Database instance.
        if getattr(spy_db, "_db_path", None) == db_path:
            captured.update(kwargs)
        return empty_result

    monkeypatch.setattr(culling_module, "analyze_for_culling", spy)

    client = app.test_client()
    resp = client.post("/api/jobs/cull", json={})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    wait_for_job_via_client(client, job_id)

    assert "vireo_dir" in captured, "analyze_for_culling was never called"
    assert captured["vireo_dir"] == str(thumb_parent), (
        f"expected {thumb_parent!r}, got {captured['vireo_dir']!r}"
    )


def test_job_classify_requires_collection_id(app_and_db):
    """POST /api/jobs/classify with empty json returns 400 with 'collection_id' in error."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/classify", json={})
    assert resp.status_code == 400
    data = resp.get_json()
    assert "collection_id" in data["error"]


def test_job_history(app_and_db, tmp_path):
    """Start a scan job, wait for completion, then GET /api/jobs/history returns a list."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "historytest")
    os.makedirs(scan_dir)
    Image.new("RGB", (100, 100)).save(os.path.join(scan_dir, "photo.jpg"))

    resp = client.post("/api/jobs/scan", json={"root": scan_dir})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    # Reads /api/jobs/history below — must wait for the row to flush.
    wait_for_job_via_client(client, job_id, wait_for_history=True)

    history_resp = client.get("/api/jobs/history")
    assert history_resp.status_code == 200
    history = history_resp.get_json()
    assert isinstance(history, list)


def test_job_history_respects_limit(app_and_db, tmp_path):
    """GET /api/jobs/history?limit=1 returns list with at most 1 entry."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "limittest")
    os.makedirs(scan_dir)
    Image.new("RGB", (100, 100)).save(os.path.join(scan_dir, "photo.jpg"))

    resp = client.post("/api/jobs/scan", json={"root": scan_dir})
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]

    # Reads /api/jobs/history below — must wait for the row to flush.
    wait_for_job_via_client(client, job_id, wait_for_history=True)

    history_resp = client.get("/api/jobs/history?limit=1")
    assert history_resp.status_code == 200
    history = history_resp.get_json()
    assert isinstance(history, list)
    assert len(history) <= 1


def test_job_develop_requires_photo_ids(app_and_db):
    """POST /api/jobs/develop with empty json returns 400 with 'photo_ids' in error."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/develop", json={})
    assert resp.status_code == 400
    data = resp.get_json()
    assert "photo_ids" in data["error"]


def test_job_previews_returns_job_id(app_and_db):
    """POST /api/jobs/previews returns job_id starting with 'previews-'."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/previews")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("previews-")


def test_job_sync_returns_job_id(app_and_db):
    """POST /api/jobs/sync returns job_id starting with 'sync-'."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/sync")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("sync-")
    # Do not let this worker outlive the fixture and race into a later
    # test's process-wide monkeypatch of sync.sync_to_xmp.
    wait_for_job_via_runner(app._job_runner, data["job_id"])


def test_job_sync_accepts_selected_change_ids(app_and_db):
    """POST /api/jobs/sync accepts a checked pending-change subset."""
    app, db = app_and_db
    client = app.test_client()
    pid = db.get_photos()[0]["id"]
    db.queue_change(pid, "rating", "4")
    change_id = db.get_pending_changes()[0]["id"]

    resp = client.post("/api/jobs/sync", json={"change_ids": [change_id]})

    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("sync-")
    # The endpoint is asynchronous, but the test still owns the worker it
    # starts. Join it before monkeypatch teardown so it cannot leak into the
    # serialization tests below.
    wait_for_job_via_runner(app._job_runner, data["job_id"])


def test_job_sync_rejects_empty_selected_change_ids(app_and_db):
    """An explicitly empty checked subset is a client error."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.post("/api/jobs/sync", json={"change_ids": []})

    assert resp.status_code == 400
    assert "change_ids" in resp.get_json()["error"]


def test_job_sync_requests_serialize_xmp_work(app_and_db, monkeypatch):
    """Repeated sync submissions must not enter sync_to_xmp concurrently."""
    app, _ = app_and_db
    client = app.test_client()

    import sync as sync_module

    first_entered = threading.Event()
    second_entered = threading.Event()
    release_first = threading.Event()
    state_lock = threading.Lock()
    calls = []
    active = 0
    max_active = 0

    def fake_sync_to_xmp(db, progress_callback=None, change_ids=None, status_callback=None):
        nonlocal active, max_active
        with state_lock:
            calls.append(change_ids)
            active += 1
            max_active = max(max_active, active)
            call_number = len(calls)

        try:
            if call_number == 1:
                first_entered.set()
                assert release_first.wait(timeout=60), "first sync was not released"
            else:
                second_entered.set()

            if progress_callback:
                progress_callback(1, 1)

            return {"synced": 0, "failed": 0, "failures": []}
        finally:
            with state_lock:
                active -= 1

    monkeypatch.setattr(sync_module, "sync_to_xmp", fake_sync_to_xmp)

    first = client.post("/api/jobs/sync").get_json()["job_id"]
    assert first_entered.wait(timeout=30), "first sync did not start"

    second = client.post("/api/jobs/sync").get_json()["job_id"]

    wait_for_job_phase(app._job_runner, second, "Waiting for current XMP sync")

    assert not second_entered.is_set(), (
        "second sync entered sync_to_xmp while the first sync was still running"
    )

    release_first.set()
    wait_for_job_via_runner(app._job_runner, first)
    wait_for_job_via_runner(app._job_runner, second)

    assert second_entered.is_set()
    assert len(calls) == 2
    assert max_active == 1


def test_cancelled_waiting_sync_does_not_write_xmp(app_and_db, monkeypatch):
    """A sync cancelled while waiting for the lock must not run sync_to_xmp."""
    app, _ = app_and_db
    client = app.test_client()

    import sync as sync_module

    first_entered = threading.Event()
    release_first = threading.Event()
    state_lock = threading.Lock()
    calls = []

    def fake_sync_to_xmp(db, progress_callback=None, change_ids=None, status_callback=None):
        with state_lock:
            calls.append(change_ids)
            call_number = len(calls)

        if call_number == 1:
            first_entered.set()
            assert release_first.wait(timeout=60), "first sync was not released"

        if progress_callback:
            progress_callback(1, 1)

        return {"synced": 0, "failed": 0, "failures": []}

    monkeypatch.setattr(sync_module, "sync_to_xmp", fake_sync_to_xmp)

    first = client.post("/api/jobs/sync").get_json()["job_id"]
    assert first_entered.wait(timeout=30), "first sync did not start"

    second = client.post("/api/jobs/sync").get_json()["job_id"]
    wait_for_job_phase(app._job_runner, second, "Waiting for current XMP sync")

    cancel = client.post(f"/api/jobs/{second}/cancel")
    assert cancel.status_code == 200
    assert cancel.get_json()["cancelled"] is True

    second_job = wait_for_job_via_runner(app._job_runner, second)

    assert second_job["status"] == "cancelled"
    assert len(calls) == 1

    release_first.set()
    wait_for_job_via_runner(app._job_runner, first)


def test_sync_job_publishes_checkpoint_and_outcome_counts(app_and_db, tmp_path, monkeypatch):
    import sync

    app, db = app_and_db
    photos = db.get_photos()[:2]
    for folder in db.get_folder_tree():
        path = tmp_path / f"sync-folder-{folder['id']}"
        path.mkdir()
        db.conn.execute("UPDATE folders SET path = ? WHERE id = ?", (str(path), folder['id']))
    db.conn.commit()
    db.queue_change(photos[0]['id'], 'keyword_add', 'Osprey')
    db.queue_change(photos[1]['id'], 'rating', 'invalid')
    monkeypatch.setattr(sync, '_SYNC_CHECKPOINT_CHANGES', 1)
    client = app.test_client()
    job_id = client.post('/api/jobs/sync').get_json()['job_id']
    job = wait_for_job_via_runner(app._job_runner, job_id)
    assert job['status'] == 'failed'
    assert job['progress']['current'] == job['progress']['total'] == 2
    assert job['progress']['synced'] == 1
    assert job['progress']['failed'] == 1
    assert job['progress']['checkpoint'] == 1
    pending = client.get('/api/sync/status').get_json()
    assert pending['pending_count'] == 1
    assert pending['active_job'] is None
