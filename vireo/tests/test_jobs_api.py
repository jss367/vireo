import json
import os
import threading
import time

import pytest
from PIL import Image
from wait import wait_for_job_via_client, wait_for_job_via_runner


def _wait_for_runner_status(runner, job_id, status, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = runner.get(job_id)
        if job and job.get("status") == status:
            return job
        time.sleep(0.01)
    raise AssertionError(
        f"job {job_id} did not reach {status}; last={runner.get(job_id)!r}"
    )


def test_job_scan_returns_job_id(app_and_db, tmp_path):
    """POST /api/jobs/scan starts a background scan and returns job_id."""
    app, db = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'test.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'job_id' in data
    assert data['job_id'].startswith('scan-')


def test_job_scan_invalid_root(app_and_db):
    """POST /api/jobs/scan with invalid root returns 400."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.post('/api/jobs/scan', json={'root': '/nonexistent/path'})
    assert resp.status_code == 400


def test_job_scan_rejects_macos_other_app_bundle(app_and_db, tmp_path):
    """POST /api/jobs/scan must reject a ``.photoslibrary`` root before
    calling ``os.path.isdir`` on it. ``os.path.isdir`` against an Apple
    Photos bundle on macOS itself trips the kTCCServiceSystemPolicyAppData
    prompt this guard exists to prevent, so the rejection must happen
    before any stat.
    """
    app, _ = app_and_db
    client = app.test_client()

    bundle = tmp_path / "Photos Library.photoslibrary"
    bundle.mkdir()

    resp = client.post('/api/jobs/scan', json={'root': str(bundle)})
    assert resp.status_code == 400
    assert "macos" in resp.get_json()["error"].lower()

    # Nested paths inside the bundle (e.g. stale folder rows pointing at
    # ``.../Photos Library.photoslibrary/originals``) must be rejected too.
    nested = bundle / "originals"
    nested.mkdir()
    resp = client.post('/api/jobs/scan', json={'root': str(nested)})
    assert resp.status_code == 400


def test_job_status_endpoint(app_and_db, tmp_path):
    """GET /api/jobs/<id> returns job status."""
    app, db = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'test.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    job_id = resp.get_json()['job_id']

    data = wait_for_job_via_client(client, job_id)

    assert resp.status_code == 200
    assert data['status'] == 'completed'


def test_jobs_list(app_and_db):
    """GET /api/jobs returns active and history lists."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.get('/api/jobs')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'active' in data
    assert 'history' in data


def test_jobs_list_strips_result_payload(app_and_db):
    """Heavy ``result`` payloads must not ride along on the polling
    response — duplicate-scan can stash thousands of proposals there
    and re-shipping that on every poll freezes the browser."""
    app, _ = app_and_db
    client = app.test_client()

    runner = app._job_runner
    big_result = {"proposals": [{"i": i, "filename": f"p_{i}.jpg"} for i in range(2000)]}
    fake_job = {
        "id": "fake-duplicate-scan-1",
        "type": "duplicate-scan",
        "status": "completed",
        "started_at": "2026-04-29T20:00:00",
        "finished_at": "2026-04-29T20:01:00",
        "progress": {"current": 0, "total": 0},
        "errors": [],
        "config": {},
        "result": big_result,
    }
    with runner._lock:
        runner._jobs["fake-duplicate-scan-1"] = fake_job

    resp = client.get('/api/jobs')
    data = resp.get_json()
    listed = next(j for j in data["active"] if j["id"] == "fake-duplicate-scan-1")
    assert "result" not in listed, (
        "result must be stripped from /api/jobs polling response so "
        "huge payloads don't ship on every poll"
    )
    assert listed["has_result"] is True, (
        "has_result lets the UI distinguish 'no result yet' from "
        "'result available, fetch on demand'"
    )


def test_jobs_list_marks_has_result_false_for_running_jobs(app_and_db):
    """A running job with no result yet must report has_result=False."""
    app, _ = app_and_db
    client = app.test_client()

    runner = app._job_runner
    fake_job = {
        "id": "fake-running-1",
        "type": "pipeline",
        "status": "running",
        "started_at": "2026-04-29T20:00:00",
        "finished_at": None,
        "progress": {"current": 5, "total": 100},
        "errors": [],
        "config": {},
        "result": None,
    }
    with runner._lock:
        runner._jobs["fake-running-1"] = fake_job

    data = client.get('/api/jobs').get_json()
    listed = next(j for j in data["active"] if j["id"] == "fake-running-1")
    assert listed["has_result"] is False


def test_job_detail_endpoint_returns_full_result(app_and_db):
    """GET /api/jobs/<id> still returns the full result so callers
    that need the heavy payload (e.g. duplicates page) can fetch on
    demand instead of paying for it on every poll."""
    app, _ = app_and_db
    client = app.test_client()

    runner = app._job_runner
    big_result = {"proposals": [{"i": i} for i in range(100)]}
    with runner._lock:
        runner._jobs["fake-detail-1"] = {
            "id": "fake-detail-1",
            "type": "duplicate-scan",
            "status": "completed",
            "started_at": "2026-04-29T20:00:00",
            "finished_at": "2026-04-29T20:01:00",
            "progress": {"current": 0, "total": 0},
            "errors": [],
            "config": {},
            "result": big_result,
        }

    detail = client.get('/api/jobs/fake-detail-1').get_json()
    assert detail["result"] == big_result


def test_pipeline_slots_empty(app_and_db):
    """GET /api/pipeline/slots reports zero active and zero queued when
    the runner has no pipeline jobs, and surfaces the module's slot cap."""
    app, _ = app_and_db
    client = app.test_client()

    from jobs import SLOT_CAP

    resp = client.get('/api/pipeline/slots')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data == {"active": 0, "queued": 0, "slot_cap": SLOT_CAP}


def test_pipeline_slots_counts_only_pipeline_jobs(app_and_db):
    """``active`` and ``queued`` must count only pipeline-type jobs.
    A running scan or duplicate-scan job must not be counted as a
    pipeline slot occupant."""
    app, _ = app_and_db
    client = app.test_client()

    runner = app._job_runner

    running_pipeline = {
        "id": "pipe-running-1",
        "type": "pipeline",
        "status": "running",
        "started_at": "2026-05-27T10:00:00",
        "finished_at": None,
        "progress": {"current": 1, "total": 10},
        "errors": [],
        "config": {},
        "result": None,
    }
    finished_pipeline = {
        "id": "pipe-done-1",
        "type": "pipeline",
        "status": "completed",
        "started_at": "2026-05-27T09:00:00",
        "finished_at": "2026-05-27T09:30:00",
        "progress": {"current": 10, "total": 10},
        "errors": [],
        "config": {},
        "result": {"ok": True},
    }
    paused_pipeline = {
        "id": "pipe-paused-1",
        "type": "pipeline",
        "status": "paused",
        "started_at": "2026-05-27T09:45:00",
        "finished_at": None,
        "progress": {"current": 5, "total": 10},
        "errors": [],
        "config": {},
        "result": None,
    }
    running_scan = {
        "id": "scan-running-1",
        "type": "scan",
        "status": "running",
        "started_at": "2026-05-27T10:05:00",
        "finished_at": None,
        "progress": {"current": 0, "total": 0},
        "errors": [],
        "config": {},
        "result": None,
    }
    with runner._lock:
        runner._jobs["pipe-running-1"] = running_pipeline
        runner._jobs["pipe-done-1"] = finished_pipeline
        runner._jobs["pipe-paused-1"] = paused_pipeline
        runner._jobs["scan-running-1"] = running_scan

    from jobs import SLOT_CAP
    resp = client.get('/api/pipeline/slots')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["active"] == 2, \
        "running and paused pipelines both occupy active slots"
    assert data["queued"] == 0
    assert data["slot_cap"] == SLOT_CAP


def test_pipeline_slots_counts_queued(app_and_db):
    """Queued pipelines (surfaced via ``list_jobs()`` from
    ``_queued_pipelines``) must appear in ``queued`` but not in
    ``active``."""
    app, _ = app_and_db
    client = app.test_client()

    runner = app._job_runner

    # Synthesize a queued pipeline the same way ``enqueue_pipeline``
    # would — by registering an entry in ``_queued_pipelines``. We do
    # this directly so the test doesn't need the full pipeline plumbing.
    with runner._lock:
        runner._queued_pipelines["pipe-queued-1"] = {
            "started_at": "2026-05-27T10:10:00",
            "config": {"sources": []},
            "workspace_id": None,
            "work_fn": lambda *a, **kw: None,
        }
        runner._jobs["pipe-running-2"] = {
            "id": "pipe-running-2",
            "type": "pipeline",
            "status": "running",
            "started_at": "2026-05-27T10:00:00",
            "finished_at": None,
            "progress": {"current": 1, "total": 10},
            "errors": [],
            "config": {},
            "result": None,
        }

    from jobs import SLOT_CAP
    resp = client.get('/api/pipeline/slots')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["active"] == 1
    assert data["queued"] == 1
    assert data["slot_cap"] == SLOT_CAP


def test_scan_status_includes_extended_stats(app_and_db):
    """GET /api/scan/status includes keyword count, db_size, etc."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.get('/api/scan/status')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'photo_count' in data
    assert 'keyword_count' in data
    assert 'db_size' in data
    assert 'thumb_cache_size' in data
    assert 'accessible_photo_count' in data
    assert 'missing_folder_count' in data


def test_scan_status_photo_count_includes_missing_folders(app_and_db):
    """When a folder is flagged 'missing' (e.g. external drive unmounted),
    /api/scan/status reports the workspace's full inventory in
    ``photo_count`` and the actionable subset in ``accessible_photo_count``.

    Without this split, the dashboard's headline number collapses to 0 for
    any workspace whose photos live on an unmounted volume — even though
    the data is intact in the DB.
    """
    app, db = app_and_db
    client = app.test_client()

    resp = client.get('/api/scan/status')
    base = resp.get_json()
    assert base['photo_count'] == 3
    assert base['accessible_photo_count'] == 3
    assert base['missing_folder_count'] == 0

    # Mark every folder in the workspace as missing — simulates the
    # external-drive-not-mounted case.
    db.conn.execute("UPDATE folders SET status = 'missing'")
    db.conn.commit()

    resp = client.get('/api/scan/status')
    after = resp.get_json()
    assert after['photo_count'] == 3, \
        "headline photo_count must reflect inventory, not just accessible"
    assert after['accessible_photo_count'] == 0
    assert after['missing_folder_count'] >= 1
    # Keywords headline must agree with the dashboard's top_keywords chart
    # (which also includes photos in missing folders). The fixture tags two
    # keywords on photos that all sit in folders we just flagged 'missing',
    # so the unfiltered count should still see them.
    assert after['keyword_count'] == base['keyword_count'], \
        "keyword_count must be inventory-wide, not accessible-only"


def test_ingest_job_starts(app_and_db, tmp_path):
    """POST /api/jobs/ingest starts a background job and returns job_id."""
    app, db = app_and_db
    src = tmp_path / "sd_card"
    dst = tmp_path / "nas_dest"
    src.mkdir()
    dst.mkdir()

    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(src / "bird.jpg"))

    with app.test_client() as c:
        resp = c.post("/api/jobs/ingest", json={
            "source": str(src),
            "destination": str(dst),
        })
        assert resp.status_code == 200
        data = resp.get_json()
        assert "job_id" in data
        assert data["job_id"].startswith("ingest-")


def test_ingest_missing_params(app_and_db, tmp_path):
    """POST /api/jobs/ingest returns error when source or destination missing."""
    app, db = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/jobs/ingest", json={})
        assert resp.status_code == 400

        resp = c.post("/api/jobs/ingest", json={"source": str(tmp_path)})
        assert resp.status_code == 400

        resp = c.post("/api/jobs/ingest", json={"destination": str(tmp_path)})
        assert resp.status_code == 400


def test_ingest_nonexistent_source(app_and_db, tmp_path):
    """POST /api/jobs/ingest returns error for non-existent source directory."""
    app, db = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/jobs/ingest", json={
            "source": str(tmp_path / "does_not_exist"),
            "destination": str(tmp_path),
        })
        assert resp.status_code == 400
        assert "not found" in resp.get_json()["error"]


def test_ingest_rejects_macos_other_app_bundle(app_and_db, tmp_path):
    """POST /api/jobs/ingest must reject a ``.photoslibrary`` source before
    calling ``os.path.isdir``. See test_job_scan_rejects_macos_other_app_bundle.
    """
    app, _ = app_and_db
    bundle = tmp_path / "Photos Library.photoslibrary"
    bundle.mkdir()
    dst = tmp_path / "dst"
    dst.mkdir()
    with app.test_client() as c:
        resp = c.post("/api/jobs/ingest", json={
            "source": str(bundle),
            "destination": str(dst),
        })
        assert resp.status_code == 400
        assert "macos" in resp.get_json()["error"].lower()


def test_import_full_rejects_macos_other_app_bundle(app_and_db, tmp_path):
    """POST /api/jobs/import-full must reject a ``.photoslibrary`` source
    before calling ``os.path.isdir``."""
    app, _ = app_and_db
    bundle = tmp_path / "Photos Library.photoslibrary"
    bundle.mkdir()
    dst = tmp_path / "dst"
    dst.mkdir()
    with app.test_client() as c:
        resp = c.post("/api/jobs/import-full", json={
            "source": str(bundle),
            "destination": str(dst),
        })
        assert resp.status_code == 400
        assert "macos" in resp.get_json()["error"].lower()


def test_scan_and_ingest_reject_non_string_path_with_400(app_and_db, tmp_path):
    """JSON primitives (``{"root": 123}``, ``{"source": true}``) reach the
    excluded-bundle helper before the directory check. The helper must not
    raise ``TypeError`` on those — otherwise routes return 500 instead of
    the 400 the directory validation produced before this PR.
    """
    app, _ = app_and_db
    dst = tmp_path / "dst"
    dst.mkdir()
    with app.test_client() as c:
        resp = c.post("/api/jobs/scan", json={"root": 123})
        assert resp.status_code == 400

        resp = c.post("/api/jobs/ingest", json={
            "source": True,
            "destination": str(dst),
        })
        assert resp.status_code == 400

        resp = c.post("/api/jobs/import-full", json={
            "source": 42,
            "destination": str(dst),
        })
        assert resp.status_code == 400


def test_ingest_relative_destination(app_and_db, tmp_path):
    """POST /api/jobs/ingest validates destination is absolute."""
    app, db = app_and_db
    src = tmp_path / "src"
    src.mkdir()
    with app.test_client() as c:
        resp = c.post("/api/jobs/ingest", json={
            "source": str(src),
            "destination": "relative/path",
        })
        assert resp.status_code == 400


def test_pipeline_job_requires_source_or_collection(app_and_db):
    """Pipeline endpoint should require either source or collection_id."""
    app, _ = app_and_db
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={})
        assert resp.status_code == 400


def test_pipeline_job_rejects_macos_other_app_bundle(app_and_db, tmp_path):
    """POST /api/jobs/pipeline must reject a ``.photoslibrary`` source
    (single or in ``sources``) before calling ``os.path.isdir``.

    Like /api/jobs/scan and /api/jobs/ingest, the pipeline route stat's
    the source up front to return a clean 400 for missing dirs. On
    macOS that pre-stat against an Apple Photos bundle trips the
    kTCCServiceSystemPolicyAppData prompt this guard exists to prevent,
    so the rejection must happen before any stat.
    """
    app, _ = app_and_db
    bundle = tmp_path / "Photos Library.photoslibrary"
    bundle.mkdir()
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={"source": str(bundle)})
        assert resp.status_code == 400
        assert "macos" in resp.get_json()["error"].lower()

        # Same shape via the ``sources`` list path.
        resp = client.post(
            "/api/jobs/pipeline", json={"sources": [str(bundle)]},
        )
        assert resp.status_code == 400
        assert "macos" in resp.get_json()["error"].lower()

        # Nested paths inside the bundle must be rejected too.
        nested = bundle / "originals"
        nested.mkdir()
        resp = client.post(
            "/api/jobs/pipeline", json={"source": str(nested)},
        )
        assert resp.status_code == 400
        assert "macos" in resp.get_json()["error"].lower()


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_job_rejects_relative_destination(app_and_db, tmp_path):
    """Pipeline endpoint should reject relative destination paths."""
    app, _ = app_and_db
    src = tmp_path / "src"
    src.mkdir()
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "source": str(src),
            "destination": "relative/path",
        })
        assert resp.status_code == 400
        assert "absolute" in resp.get_json()["error"]


def test_jobs_page_returns_200(app_and_db):
    """GET /jobs returns the jobs page."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/jobs')
    assert resp.status_code == 200
    assert b'Jobs' in resp.data
    assert b'data-pause-job' in resp.data
    assert b'data-resume-job' in resp.data


def test_navbar_has_jobs_link(app_and_db):
    """Navbar on any page includes a link to /jobs."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/browse')
    assert b'href="/jobs"' in resp.data


def test_bottom_panel_has_compact_jobs(app_and_db):
    """Bottom panel Jobs tab has compact layout with View link."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get('/browse')
    html = resp.data.decode()
    assert 'bp-compact-job' in html or 'View history' in html or 'No active jobs' in html


def test_scan_job_has_steps(app_and_db, tmp_path):
    """Scan job defines steps for the jobs page tree view."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'test.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    job_id = resp.get_json()['job_id']

    data = wait_for_job_via_client(client, job_id)

    assert data['status'] == 'completed'
    assert 'steps' in data
    assert len(data['steps']) >= 2
    assert data['steps'][0]['id'] == 'scan'
    assert data['steps'][0]['status'] == 'completed'


def test_job_history_includes_parsed_tree(app_and_db, tmp_path):
    """GET /api/jobs/history returns parsed tree data for completed jobs."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'test.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    job_id = resp.get_json()['job_id']

    wait_for_job_via_client(client, job_id, wait_for_history=True)

    resp = client.get('/api/jobs/history?limit=5')
    assert resp.status_code == 200
    history = resp.get_json()
    assert len(history) > 0
    found = [h for h in history if h['id'] == job_id]
    assert len(found) > 0
    entry = found[0]
    # tree should be a parsed list, not a JSON string
    assert 'tree' in entry
    assert isinstance(entry['tree'], list)
    assert 'summary' in entry


def test_jobs_list_includes_active_workspace_id(app_and_db):
    """GET /api/jobs includes active_workspace_id."""
    app, db = app_and_db
    client = app.test_client()

    resp = client.get('/api/jobs')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'active_workspace_id' in data
    assert data['active_workspace_id'] == db._active_workspace_id


def test_jobs_list_includes_workspace_names(app_and_db):
    """GET /api/jobs includes workspace_names mapping."""
    app, _ = app_and_db
    client = app.test_client()

    resp = client.get('/api/jobs')
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'workspace_names' in data
    assert isinstance(data['workspace_names'], dict)
    # Default workspace should be present
    assert len(data['workspace_names']) >= 1


def test_readiness_includes_exiftool_status(app_and_db):
    """Readiness endpoint should report exiftool installation status."""
    app, _ = app_and_db
    with app.test_client() as client:
        resp = client.get("/api/classify/readiness")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "exiftool" in data
        assert "installed" in data["exiftool"]
        assert "brew_available" in data["exiftool"]
        assert isinstance(data["exiftool"]["installed"], bool)
        assert isinstance(data["exiftool"]["brew_available"], bool)


def test_install_exiftool_endpoint_exists(app_and_db, monkeypatch):
    """Install-exiftool endpoint should exist and return JSON."""
    import shutil
    import subprocess
    import sys

    import metadata
    monkeypatch.setattr(sys, "platform", "darwin")
    original_which = shutil.which
    monkeypatch.setattr(shutil, "which", lambda cmd: "/usr/local/bin/brew" if cmd == "brew" else (None if cmd == "exiftool" else original_which(cmd)))
    monkeypatch.setattr(metadata, "find_homebrew", lambda: "/usr/local/bin/brew")
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: type("R", (), {"returncode": 0, "stderr": ""})())
    app, _ = app_and_db
    with app.test_client() as client:
        resp = client.post("/api/system/install-exiftool")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data.get("success") is True


def test_install_exiftool_fails_without_brew(app_and_db, monkeypatch):
    """Install endpoint should fail gracefully when brew is not available."""
    import shutil
    import sys

    import metadata
    monkeypatch.setattr(sys, "platform", "darwin")
    original_which = shutil.which
    monkeypatch.setattr(shutil, "which", lambda cmd: None if cmd in ("brew", "exiftool") else original_which(cmd))
    monkeypatch.setattr(metadata, "find_homebrew", lambda: None)
    app, _ = app_and_db
    with app.test_client() as client:
        resp = client.post("/api/system/install-exiftool")
        data = resp.get_json()
        assert data.get("success") is False
        assert "brew" in data.get("error", "").lower()


def test_pipeline_job_with_collection_returns_job_id(app_and_db):
    """Pipeline with collection_id should start and return job_id."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)
    col_id = db.add_collection("Test", json.dumps([]))

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        data = resp.get_json()
        assert "job_id" in data
        assert data["job_id"].startswith("pipeline-")


def test_pipeline_job_config_includes_collection_name(app_and_db, monkeypatch):
    """Collection-scoped jobs carry the collection name for the Jobs page."""
    import json

    import pipeline_job
    from db import Database

    app, _ = app_and_db
    db = Database(app.config["DB_PATH"])
    db.set_active_workspace(db._active_workspace_id)
    col_id = db.add_collection("Costa Rica selects", json.dumps([]))

    def fake_run(job, runner, db_path, workspace_id, params, thumb_cache_dir=None,
                 **_kwargs):
        return {"ok": True}

    monkeypatch.setattr(pipeline_job, "run_pipeline_job", fake_run)

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "skip_classify": True,
            "skip_extract_masks": True,
            "skip_regroup": True,
        })
        assert resp.status_code == 200
        cfg = _job_config(client, resp.get_json()["job_id"])
        assert cfg["collection_id"] == col_id
        assert cfg["collection_name"] == "Costa Rica selects"


def test_pipeline_auto_skips_classify_when_no_model(app_and_db):
    """Pipeline should auto-skip classify/extract/regroup when no model is available."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)
    col_id = db.add_collection("Test", json.dumps([]))

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            # Don't set skip_classify — let the auto-skip kick in
        })
        assert resp.status_code == 200
        data = resp.get_json()
        assert "job_id" in data
        assert "model_warning" in data
        assert "skipped" in data["model_warning"].lower() or "no model" in data["model_warning"].lower()


def test_update_step_current_file(app_and_db):
    """update_step supports current_file field on steps."""
    from jobs import JobRunner
    runner = JobRunner.__new__(JobRunner)
    runner._jobs = {}
    runner._subscribers = {}
    runner._lock = __import__('threading').Lock()
    runner._history_db_path = None

    job_id = "test-cf"
    runner._jobs[job_id] = {
        "id": job_id,
        "steps": [
            {"id": "scan", "label": "Scan", "status": "running"},
        ],
    }
    runner.update_step(job_id, "scan", current_file="DSC_0001.NEF")
    assert runner._jobs[job_id]["steps"][0]["current_file"] == "DSC_0001.NEF"


def test_start_job_ids_unique_within_same_millisecond(app_and_db, monkeypatch):
    """start() ids carry a monotonic suffix — two same-type jobs started in
    the same millisecond previously collided, overwriting each other's
    registration and history row."""
    import time as _time

    import jobs as jobs_mod
    from jobs import JobRunner
    # Freeze the clock so every start() provably lands in the same
    # millisecond — without this a slow CI worker could space the calls
    # out and the old (suffix-less) implementation would also pass.
    frozen = _time.time()
    monkeypatch.setattr(jobs_mod.time, "time", lambda: frozen)
    runner = JobRunner()
    ids = [runner.start("scan", lambda j: None) for _ in range(10)]
    assert len(set(ids)) == 10


def test_update_step_cancelled_is_terminal(app_and_db):
    """status='cancelled' finalizes a step (finished_at + duration), same as
    completed/failed — classify/pipeline steps report it on user cancel."""
    from jobs import JobRunner
    runner = JobRunner.__new__(JobRunner)
    runner._jobs = {}
    runner._subscribers = {}
    runner._lock = __import__('threading').Lock()
    runner._history_db_path = None

    job_id = "test-cancel-step"
    runner._jobs[job_id] = {
        "id": job_id,
        "steps": [
            {"id": "classify", "label": "Classify", "status": "pending",
             "started_at": None, "finished_at": None, "duration": None},
        ],
    }
    runner.update_step(job_id, "classify", status="running")
    runner.update_step(job_id, "classify", status="cancelled",
                       summary="Cancelled (3 of 10 processed)")
    step = runner._jobs[job_id]["steps"][0]
    assert step["status"] == "cancelled"
    assert step["finished_at"] is not None
    assert step["duration"] is not None


def test_scan_step_has_progress(app_and_db, tmp_path):
    """Scan step reports step-level progress with current/total."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'a.jpg'))
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'b.jpg'))

    resp = client.post('/api/jobs/scan', json={'root': scan_dir})
    job_id = resp.get_json()['job_id']

    data = wait_for_job_via_client(client, job_id)

    assert data['status'] == 'completed'
    scan_step = data['steps'][0]
    assert scan_step['id'] == 'scan'
    # After completion, progress should have current == total
    assert 'progress' in scan_step
    assert scan_step['progress']['current'] == scan_step['progress']['total']
    assert scan_step['progress']['total'] >= 2


def test_pipeline_thumbnail_step_has_progress(app_and_db, tmp_path):
    """Thumbnail step in pipeline reports step-level progress."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'a.jpg'))

    resp = client.post('/api/jobs/pipeline', json={
        'source': scan_dir,
        'skip_classify': True,
        'skip_extract_masks': True,
        'skip_regroup': True,
    })
    job_id = resp.get_json()['job_id']

    data = wait_for_job_via_client(client, job_id)

    assert data['status'] == 'completed'
    thumb_step = next(s for s in data['steps'] if s['id'] == 'thumbnails')
    assert 'progress' in thumb_step
    assert thumb_step['progress']['current'] > 0


def test_pipeline_preview_step_has_progress(app_and_db, tmp_path):
    """Preview step in pipeline reports step-level progress."""
    app, _ = app_and_db
    client = app.test_client()

    scan_dir = str(tmp_path / "scanme")
    os.makedirs(scan_dir)
    Image.new('RGB', (100, 100)).save(os.path.join(scan_dir, 'a.jpg'))

    resp = client.post('/api/jobs/pipeline', json={
        'source': scan_dir,
        'skip_classify': True,
        'skip_extract_masks': True,
        'skip_regroup': True,
    })
    job_id = resp.get_json()['job_id']

    data = wait_for_job_via_client(client, job_id)

    assert data['status'] == 'completed'
    preview_step = next(s for s in data['steps'] if s['id'] == 'previews')
    assert 'progress' in preview_step
    assert preview_step['progress']['total'] > 0


def test_job_export_returns_job_id(app_and_db, tmp_path):
    """POST /api/jobs/export starts a background export and returns job_id."""
    app, db = app_and_db
    client = app.test_client()

    photos = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchall()
    photo_ids = [p["id"] for p in photos]
    dest = str(tmp_path / "export_out")

    resp = client.post("/api/jobs/export", json={
        "photo_ids": photo_ids,
        "destination": dest,
        "naming_template": "{original}",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data
    assert data["job_id"].startswith("export-")


def test_job_export_missing_photo_ids(app_and_db):
    """POST /api/jobs/export without photo_ids returns 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/export", json={"destination": "/tmp/out"})
    assert resp.status_code == 400


def test_job_export_missing_destination(app_and_db):
    """POST /api/jobs/export without destination returns 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/export", json={"photo_ids": [1]})
    assert resp.status_code == 400


def test_job_export_relative_destination(app_and_db):
    """POST /api/jobs/export with relative destination returns 400."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/export", json={
        "photo_ids": [1],
        "destination": "relative/path",
    })
    assert resp.status_code == 400


def test_job_export_invalid_format(app_and_db, tmp_path):
    """POST /api/jobs/export rejects unsupported output formats."""
    app, db = app_and_db
    client = app.test_client()
    photo = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()

    resp = client.post("/api/jobs/export", json={
        "photo_ids": [photo["id"]],
        "destination": str(tmp_path / "out"),
        "format": "bmp",
    })

    assert resp.status_code == 400
    assert "format must be one of" in resp.get_json()["error"]


def test_job_export_invalid_quality(app_and_db, tmp_path):
    """POST /api/jobs/export rejects JPEG quality outside Pillow's range."""
    app, db = app_and_db
    client = app.test_client()
    photo = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()

    resp = client.post("/api/jobs/export", json={
        "photo_ids": [photo["id"]],
        "destination": str(tmp_path / "out"),
        "quality": 101,
    })

    assert resp.status_code == 400
    assert "quality must be an integer" in resp.get_json()["error"]


def test_job_export_invalid_max_size(app_and_db, tmp_path):
    """POST /api/jobs/export rejects nonsensical resize settings."""
    app, db = app_and_db
    client = app.test_client()
    photo = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()

    resp = client.post("/api/jobs/export", json={
        "photo_ids": [photo["id"]],
        "destination": str(tmp_path / "out"),
        "max_size": "large",
    })

    assert resp.status_code == 400
    assert "max_size must be a positive integer" in resp.get_json()["error"]


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_ingest_saves_recent_destination(app_and_db, tmp_path, monkeypatch):
    """Starting a pipeline with a destination saves it to recent_destinations in config."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    app, db = app_and_db
    src = tmp_path / "src"
    src.mkdir()
    dst = tmp_path / "dst"
    dst.mkdir()

    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(src / "bird.jpg"))

    with app.test_client() as c:
        resp = c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": str(dst),
        })
        assert resp.status_code == 200

    config = cfg.load()
    assert config["ingest"]["recent_destinations"] == [str(dst)]


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_recent_destinations_deduplicates_and_limits(app_and_db, tmp_path, monkeypatch):
    """Recent destinations deduplicates and limits to 5 entries."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    app, db = app_and_db
    src = tmp_path / "src"
    src.mkdir()

    from PIL import Image
    Image.new("RGB", (100, 100)).save(str(src / "bird.jpg"))

    # Create 6 destination directories
    dsts = []
    for i in range(6):
        d = tmp_path / f"dst{i}"
        d.mkdir()
        dsts.append(str(d))

    with app.test_client() as c:
        # Fill up 6 destinations
        for d in dsts:
            c.post("/api/jobs/pipeline", json={
                "sources": [str(src)],
                "destination": d,
            })

    config = cfg.load()
    recents = config["ingest"]["recent_destinations"]
    assert len(recents) == 5
    # Most recent first
    assert recents[0] == dsts[5]

    # Re-use dst1 — should move to front
    with app.test_client() as c:
        c.post("/api/jobs/pipeline", json={
            "sources": [str(src)],
            "destination": dsts[1],
        })
    config = cfg.load()
    recents = config["ingest"]["recent_destinations"]
    assert recents[0] == dsts[1]
    assert len(recents) == 5


def test_job_cancel_unknown_job_returns_404(app_and_db):
    """POST /api/jobs/<id>/cancel returns 404 for unknown job."""
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/jobs/does-not-exist/cancel")
        assert resp.status_code == 404


def test_job_cancel_running_job_marks_cancelled(app_and_db):
    """POST /api/jobs/<id>/cancel signals a running job, which then finishes
    with status 'cancelled' instead of 'completed'."""
    app, _ = app_and_db
    runner = app._job_runner

    release = {"go": False}

    def slow_work(job):
        # Poll for cancellation so the work function exits promptly.
        for _ in range(200):
            if runner.is_cancelled(job["id"]):
                return {"stopped": True}
            if release["go"]:
                return {"stopped": False}
            time.sleep(0.05)
        return {"stopped": False}

    job_id = runner.start("test", slow_work)

    with app.test_client() as c:
        resp = c.post(f"/api/jobs/{job_id}/cancel")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data.get("cancelled") is True

    # Wait for the work function to observe cancellation and exit.
    job = wait_for_job_via_runner(runner, job_id)
    assert job["status"] == "cancelled"


def test_job_cancel_finished_job_returns_404(app_and_db):
    """Cancelling a job that has already finished returns 404."""
    app, _ = app_and_db
    runner = app._job_runner

    def quick_work(job):
        return {"ok": True}

    job_id = runner.start("test", quick_work)

    wait_for_job_via_runner(runner, job_id)

    with app.test_client() as c:
        resp = c.post(f"/api/jobs/{job_id}/cancel")
        assert resp.status_code == 404


def test_job_pause_and_resume_api(app_and_db):
    """The Jobs API exposes cooperative pause/resume state transitions."""
    app, _ = app_and_db
    runner = app._job_runner
    progress = {"count": 0}
    finish = threading.Event()

    def work(job):
        while not finish.is_set():
            if runner.is_cancelled(job["id"]):
                break
            progress["count"] += 1
            time.sleep(0.01)
        return {"count": progress["count"]}

    job_id = runner.start("scan", work, pausable=True)

    with app.test_client() as c:
        paused = c.post(f"/api/jobs/{job_id}/pause")
        assert paused.status_code == 200
        assert paused.get_json()["status"] == "pausing"
        _wait_for_runner_status(runner, job_id, "paused")

        before = progress["count"]
        time.sleep(0.08)
        assert progress["count"] == before

        resumed = c.post(f"/api/jobs/{job_id}/resume")
        assert resumed.status_code == 200
        assert resumed.get_json()["status"] == "running"
        finish.set()

    assert wait_for_job_via_runner(runner, job_id)["status"] == "completed"


def test_job_pause_api_rejects_unsupported_job(app_and_db):
    app, _ = app_and_db
    runner = app._job_runner
    release = threading.Event()

    def work(_job):
        release.wait(timeout=2)
        return {}

    job_id = runner.start("test", work)
    with app.test_client() as c:
        resp = c.post(f"/api/jobs/{job_id}/pause")
        assert resp.status_code == 409
        assert "does not support" in resp.get_json()["error"]
    release.set()
    wait_for_job_via_runner(runner, job_id)


# --- Pipeline metadata auto-repair tests ---

def test_find_broken_metadata_folders_returns_empty_when_healthy(app_and_db):
    """_find_broken_metadata_folders returns [] when all photos have good
    metadata."""
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    # The fixture's 3 photos all have populated timestamps and .jpg ext,
    # so none should match the detection rule.
    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    assert _find_broken_metadata_folders(db, photo_ids) == []


def test_find_broken_metadata_folders_detects_null_timestamp(app_and_db, monkeypatch):
    """_find_broken_metadata_folders flags a photo whose timestamp is NULL
    and returns its file path so the repair path can restrict the scan."""
    import pipeline_job
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    # Fixture rows point at synthetic /photos paths that don't exist on
    # disk; treat all DB rows as present for the SQL/grouping assertions.
    monkeypatch.setattr(pipeline_job.os.path, "isfile", lambda _p: True)

    # Break one photo's timestamp.
    row = db.conn.execute(
        "SELECT id, folder_id FROM photos WHERE filename='bird1.jpg'"
    ).fetchone()
    db.conn.execute("UPDATE photos SET timestamp=NULL WHERE id=?", (row["id"],))
    db.conn.commit()

    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    result = _find_broken_metadata_folders(db, photo_ids)
    folder_path = db.conn.execute(
        "SELECT path FROM folders WHERE id=?", (row["folder_id"],)
    ).fetchone()["path"]
    assert result == [(folder_path, [os.path.join(folder_path, "bird1.jpg")])]


def test_find_broken_metadata_folders_detects_raw_thumb_dims(app_and_db, monkeypatch):
    """_find_broken_metadata_folders flags a RAW photo with sub-1000px
    width (the embedded-thumbnail bug) even when timestamp is populated."""
    import pipeline_job
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    monkeypatch.setattr(pipeline_job.os.path, "isfile", lambda _p: True)

    row = db.conn.execute(
        "SELECT id, folder_id FROM photos WHERE filename='bird1.jpg'"
    ).fetchone()
    db.conn.execute(
        "UPDATE photos SET extension='.nef', width=160, height=120 "
        "WHERE id=?", (row["id"],)
    )
    db.conn.commit()

    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    result = _find_broken_metadata_folders(db, photo_ids)
    assert len(result) == 1
    # One file in the folder — bird1.jpg with the fake .nef extension.
    assert len(result[0][1]) == 1
    assert result[0][1][0].endswith("bird1.jpg")


def test_find_broken_metadata_folders_ignores_out_of_scope(app_and_db):
    """Broken photos outside the passed photo_ids list are not returned."""
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    # Break bird1, but only pass bird2's id in scope.
    bird1 = db.conn.execute(
        "SELECT id FROM photos WHERE filename='bird1.jpg'"
    ).fetchone()["id"]
    bird2 = db.conn.execute(
        "SELECT id FROM photos WHERE filename='bird2.jpg'"
    ).fetchone()["id"]
    db.conn.execute("UPDATE photos SET timestamp=NULL WHERE id=?", (bird1,))
    db.conn.commit()

    assert _find_broken_metadata_folders(db, [bird2]) == []


def test_find_broken_metadata_folders_excludes_exif_extracted(app_and_db):
    """A row whose timestamp is NULL but exif_data is populated is NOT
    returned. Such rows represent photos where ExifTool already ran and
    the source file genuinely has no DateTimeOriginal (e.g. screenshots).
    The scanner's exif_extracted guard would skip them on re-scan anyway,
    so flagging them as repairable would cause the repair path to fire
    forever without doing useful work."""
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    row = db.conn.execute(
        "SELECT id FROM photos WHERE filename='bird1.jpg'"
    ).fetchone()
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL, exif_data='{\"File\":{}}' "
        "WHERE id=?", (row["id"],)
    )
    db.conn.commit()

    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    assert _find_broken_metadata_folders(db, photo_ids) == []


def test_find_broken_metadata_folders_groups_by_folder(app_and_db, monkeypatch):
    """Multiple broken photos in the same folder are returned as one
    folder entry with a count."""
    import pipeline_job
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    monkeypatch.setattr(pipeline_job.os.path, "isfile", lambda _p: True)

    # Break both photos in folder '/photos/2024' (bird1 and bird3).
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL "
        "WHERE filename IN ('bird1.jpg', 'bird3.jpg')"
    )
    db.conn.commit()

    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    result = _find_broken_metadata_folders(db, photo_ids)
    assert len(result) == 1
    folder, paths = result[0]
    assert folder == '/photos/2024'
    assert len(paths) == 2
    assert sorted(os.path.basename(p) for p in paths) == ['bird1.jpg', 'bird3.jpg']


def test_find_broken_metadata_folders_filters_missing_files(app_and_db, tmp_path):
    """Rows whose file no longer exists on disk are filtered out. The
    scanner can only repair files it rediscovers via Path.iterdir(), so
    a missing-file row would stay broken forever and keep the collection
    stuck in repair mode on every pipeline run."""
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    # Real folder with one real file (broken) and one DB row pointing at
    # a deleted file (also broken-looking in the DB).
    real_dir = tmp_path / "photos"
    real_dir.mkdir()
    present = real_dir / "present.jpg"
    Image.new("RGB", (640, 480)).save(str(present), "JPEG")

    fid = db.add_folder(str(real_dir), name="photos")
    p_present = db.add_photo(
        folder_id=fid, filename="present.jpg", extension=".jpg",
        file_size=present.stat().st_size, file_mtime=present.stat().st_mtime,
        timestamp=None,
    )
    p_missing = db.add_photo(
        folder_id=fid, filename="ghost.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0, timestamp=None,
    )
    db.conn.commit()

    result = _find_broken_metadata_folders(db, [p_present, p_missing])
    assert len(result) == 1
    folder, paths = result[0]
    assert folder == str(real_dir)
    assert paths == [str(present)]


def test_find_broken_metadata_folders_chunks_large_id_lists(app_and_db, monkeypatch):
    """Passing more photo_ids than SQLite's default variable cap (999) must
    not raise 'too many SQL variables'. The helper chunks internally."""
    import pipeline_job
    from pipeline_job import _find_broken_metadata_folders
    _, db = app_and_db

    monkeypatch.setattr(pipeline_job.os.path, "isfile", lambda _p: True)

    folder_id = db.conn.execute(
        "SELECT id FROM folders WHERE path='/photos/2024'"
    ).fetchone()["id"]
    # Bulk-insert 1200 photos, all with populated timestamps (none broken).
    db.conn.executemany(
        "INSERT INTO photos (folder_id, filename, extension, file_size, "
        "file_mtime, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (folder_id, f"bulk_{i}.jpg", ".jpg", 1000, float(i),
             "2024-01-01T00:00:00")
            for i in range(1200)
        ],
    )
    db.conn.commit()

    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    assert len(photo_ids) > 999
    # Must not raise OperationalError: too many SQL variables.
    assert _find_broken_metadata_folders(db, photo_ids) == []

    # Break one of the bulk rows and confirm it's still found across chunks.
    target = db.conn.execute(
        "SELECT id, folder_id FROM photos WHERE filename='bulk_1100.jpg'"
    ).fetchone()
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL WHERE id=?", (target["id"],)
    )
    db.conn.commit()
    result = _find_broken_metadata_folders(db, photo_ids)
    assert len(result) == 1
    assert any(p.endswith("bulk_1100.jpg") for p in result[0][1])


def test_pipeline_with_healthy_collection_skips_scan(app_and_db):
    """A pipeline run against a collection of healthy photos reports the
    scan stage as skipped — preserving existing behavior when nothing's
    broken. The downstream thumbnail stage will fail because the fixture
    files don't exist on disk; we only care about the scan step."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)

    photo_ids = [p["id"] for p in db.conn.execute(
        "SELECT id FROM photos"
    ).fetchall()]
    col_id = db.add_collection(
        "healthy", json.dumps([{"field": "photo_ids", "value": photo_ids}])
    )

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "skip_extract_masks": True,
            "skip_regroup": True,
            "skip_classify": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]

        data = wait_for_job_via_client(client, job_id)

        scan_step = next(s for s in data["steps"] if s["id"] == "scan")
        assert scan_step.get("summary") == "Skipped (using collection)"


def test_pipeline_with_broken_collection_repairs_metadata(app_and_db, tmp_path, monkeypatch):
    """A pipeline run against a collection with broken-metadata photos
    triggers a targeted repair scan before downstream stages. Broken
    rows end with populated timestamp and corrected dimensions."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)

    # Create a real image file on disk and register a new folder+photo
    # pointing at it. The fixture's /photos/... folders don't exist on
    # disk, so we need our own.
    photos_root = tmp_path / "real_photos"
    photos_root.mkdir()
    image_file = photos_root / "broken.jpg"
    Image.new("RGB", (640, 480), color="red").save(str(image_file), "JPEG")

    fid = db.add_folder(str(photos_root), name="real_photos")
    db.add_workspace_folder(db._active_workspace_id, fid)
    pid = db.add_photo(
        folder_id=fid, filename="broken.jpg", extension=".jpg",
        file_size=image_file.stat().st_size,
        file_mtime=image_file.stat().st_mtime,
        timestamp=None, width=160, height=120,
    )
    # Force broken RAW-thumbnail state so the detection rule fires.
    db.conn.execute(
        "UPDATE photos SET extension='.nef', timestamp=NULL, "
        "exif_data=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    # Mock ExifTool so the test doesn't depend on the binary.
    import scanner
    def fake_extract(paths, restricted_tags=None, progress_callback=None,
                     checkpoint=None):
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {"DateTimeOriginal": "2024:06:15 10:00:00"},
                    "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    col_id = db.add_collection(
        "broken", json.dumps([{"field": "photo_ids", "value": [pid]}])
    )

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "skip_extract_masks": True,
            "skip_regroup": True,
            "skip_classify": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]

        data = wait_for_job_via_client(client, job_id)

        assert data["status"] == "completed", data
        scan_step = next(s for s in data["steps"] if s["id"] == "scan")
        # Scan stage should have run (not been skipped) because of repair.
        assert "repair" in (scan_step.get("summary") or "").lower()

    # Verify the broken row now has correct metadata.
    row = db.conn.execute(
        "SELECT timestamp, width, height FROM photos WHERE id=?", (pid,)
    ).fetchone()
    assert row["timestamp"] == "2024-06-15T10:00:00"
    assert row["width"] == 640
    assert row["height"] == 480


def test_pipeline_repair_does_not_ingest_untracked_files(app_and_db, tmp_path, monkeypatch):
    """When the repair path scans a folder to fix broken metadata, new
    files that were added to that folder but never scanned do NOT get
    ingested as a side effect. The repair must touch only the specific
    photos flagged as broken."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)

    # Create a real folder with two files — one in the DB (broken), one
    # untracked. The repair run should touch the broken one and leave
    # the untracked one alone.
    photos_root = tmp_path / "real_photos"
    photos_root.mkdir()
    tracked = photos_root / "tracked.jpg"
    untracked = photos_root / "untracked.jpg"
    Image.new("RGB", (640, 480), color="red").save(str(tracked), "JPEG")
    Image.new("RGB", (640, 480), color="blue").save(str(untracked), "JPEG")

    fid = db.add_folder(str(photos_root), name="real_photos")
    db.add_workspace_folder(db._active_workspace_id, fid)
    pid = db.add_photo(
        folder_id=fid, filename="tracked.jpg", extension=".jpg",
        file_size=tracked.stat().st_size,
        file_mtime=tracked.stat().st_mtime,
    )
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL, exif_data=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    import scanner
    def fake_extract(paths, restricted_tags=None, progress_callback=None,
                     checkpoint=None):
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {"DateTimeOriginal": "2024:06:15 10:00:00"},
                    "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    col_id = db.add_collection(
        "broken_tracked",
        json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "skip_extract_masks": True,
            "skip_regroup": True,
            "skip_classify": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]

        data = wait_for_job_via_client(client, job_id)

    # Tracked photo repaired: timestamp populated.
    tracked_row = db.conn.execute(
        "SELECT timestamp FROM photos WHERE filename='tracked.jpg'"
    ).fetchone()
    assert tracked_row["timestamp"] == "2024-06-15T10:00:00"
    # Untracked file NOT ingested.
    untracked_row = db.conn.execute(
        "SELECT COUNT(*) AS n FROM photos WHERE filename='untracked.jpg'"
    ).fetchone()
    assert untracked_row["n"] == 0


def test_pipeline_with_broken_collection_handles_unreachable_folder(app_and_db):
    """A pipeline run where a broken photo's underlying file no longer
    exists on disk handles the missing file gracefully — the broken row
    is filtered out of the repair scope (since the scanner couldn't
    repair it anyway), and the scan stage falls through to the normal
    'Skipped (using collection)' fast path instead of looping forever
    in repair mode."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)

    bird1 = db.conn.execute(
        "SELECT id FROM photos WHERE filename='bird1.jpg'"
    ).fetchone()["id"]
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL, exif_data=NULL WHERE id=?",
        (bird1,),
    )
    db.conn.commit()

    col_id = db.add_collection(
        "broken_unreachable",
        json.dumps([{"field": "photo_ids", "value": [bird1]}]),
    )

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "skip_extract_masks": True,
            "skip_regroup": True,
            "skip_classify": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]

        data = wait_for_job_via_client(client, job_id)

        # The scan step should have completed without error. With the
        # broken row's file missing on disk, the repair-target filter
        # drops it and the scan stage falls through to "Skipped (using
        # collection)" — this is exactly the behavior we want to keep
        # the collection from getting stuck in repair mode forever.
        scan_step = next(s for s in data["steps"] if s["id"] == "scan")
        assert scan_step["status"] == "completed"
        assert scan_step.get("summary") == "Skipped (using collection)"


def test_pipeline_repair_does_not_double_process_thumbnails(
    app_and_db, tmp_path, monkeypatch,
):
    """Repaired photos must only be processed once by the thumbnail stage.
    The collection-replay loop in thumbnail_stage() already covers every
    photo in the collection, so the scan-queue callback must not also
    enqueue repaired files — doing so would generate the thumbnail twice
    and inflate the thumbnail totals beyond the collection size."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)

    photos_root = tmp_path / "real_photos"
    photos_root.mkdir()
    image_file = photos_root / "broken.jpg"
    Image.new("RGB", (640, 480), color="red").save(str(image_file), "JPEG")

    fid = db.add_folder(str(photos_root), name="real_photos")
    db.add_workspace_folder(db._active_workspace_id, fid)
    pid = db.add_photo(
        folder_id=fid, filename="broken.jpg", extension=".jpg",
        file_size=image_file.stat().st_size,
        file_mtime=image_file.stat().st_mtime,
        timestamp=None, width=160, height=120,
    )
    db.conn.execute(
        "UPDATE photos SET extension='.nef', timestamp=NULL, "
        "exif_data=NULL WHERE id=?", (pid,)
    )
    db.conn.commit()

    import scanner
    def fake_extract(paths, restricted_tags=None, progress_callback=None,
                     checkpoint=None):
        return {p: {"File": {"ImageWidth": 640, "ImageHeight": 480},
                    "EXIF": {"DateTimeOriginal": "2024:06:15 10:00:00"},
                    "Composite": {}} for p in paths}
    monkeypatch.setattr(scanner, "extract_metadata", fake_extract)

    # Count generate_thumbnail invocations per photo_id. If the P2 bug
    # existed the repaired photo would be generated twice (once from the
    # scan queue, once from the collection replay).
    from collections import Counter
    calls = Counter()

    import thumbnails
    real_generate = thumbnails.generate_thumbnail
    def counting_generate(photo_id, *args, **kwargs):
        calls[photo_id] += 1
        return real_generate(photo_id, *args, **kwargs)
    monkeypatch.setattr(thumbnails, "generate_thumbnail", counting_generate)

    col_id = db.add_collection(
        "broken_thumb", json.dumps([{"field": "photo_ids", "value": [pid]}])
    )

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "skip_extract_masks": True,
            "skip_regroup": True,
            "skip_classify": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]

        data = wait_for_job_via_client(client, job_id)

        assert data["status"] == "completed", data

        thumb_step = next(s for s in data["steps"] if s["id"] == "thumbnails")
        # Progress total must match the collection size, not double it.
        progress = thumb_step.get("progress") or {}
        assert progress.get("total") == 1, thumb_step

    # Each repaired photo should be handed to generate_thumbnail exactly once.
    assert calls[pid] == 1, dict(calls)


def test_pipeline_repair_respects_excluded_photo_ids(
    app_and_db, monkeypatch,
):
    """Photos excluded from the run via ``exclude_photo_ids`` must not
    trigger a repair scan — the downstream stages would skip them anyway,
    so rewriting their metadata would be unexpected out-of-scope work."""
    import json

    from db import Database

    app, _ = app_and_db
    db_path = app.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(db._active_workspace_id)

    # The fixture's bird1.jpg doesn't exist on disk, but that's fine —
    # we only need to verify the scan stage takes the fast-path skip
    # instead of firing the repair scan. Force it into broken state so
    # that without the exclusion filter it WOULD be picked up for repair.
    bird1 = db.conn.execute(
        "SELECT id FROM photos WHERE filename='bird1.jpg'"
    ).fetchone()["id"]
    bird2 = db.conn.execute(
        "SELECT id FROM photos WHERE filename='bird2.jpg'"
    ).fetchone()["id"]
    db.conn.execute(
        "UPDATE photos SET timestamp=NULL, exif_data=NULL WHERE id=?",
        (bird1,),
    )
    db.conn.commit()

    # If repair ever runs for the excluded photo it would call
    # extract_metadata; fail loudly if so.
    import scanner
    def fail_extract(paths, restricted_tags=None):
        raise AssertionError(
            f"extract_metadata must not run for excluded photos: {paths}"
        )
    monkeypatch.setattr(scanner, "extract_metadata", fail_extract)

    col_id = db.add_collection(
        "with_excluded",
        json.dumps([{"field": "photo_ids", "value": [bird1, bird2]}]),
    )

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "exclude_photo_ids": [bird1],
            "skip_extract_masks": True,
            "skip_regroup": True,
            "skip_classify": True,
        })
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]

        data = wait_for_job_via_client(client, job_id)

        # The scan step should have reported the "Skipped" fast path
        # because the only broken photo was excluded from the run.
        # Downstream stages may still fail on the stub fixture files —
        # that's unrelated and does not affect the scan-stage assertion.
        scan_step = next(s for s in data["steps"] if s["id"] == "scan")
        summary = (scan_step.get("summary") or "").lower()
        assert "skipped" in summary, scan_step
        assert "repair" not in summary, scan_step

    # Excluded photo's broken metadata is untouched.
    row = db.conn.execute(
        "SELECT timestamp, exif_data FROM photos WHERE id=?", (bird1,)
    ).fetchone()
    assert row["timestamp"] is None
    assert row["exif_data"] is None


# ---------------------------------------------------------------------------
# /api/jobs/extract-masks — standalone masking route writes photo_masks rows.
#
# Phase 2 of the SAM mask history plan migrated the unified pipeline's
# masking stage to write photo_masks rows. The standalone route in
# api_job_extract_masks is a separate code path and must follow the
# same flow: write a per-variant photo_masks row, denormalize via
# set_active_mask_variant, and skip SAM when the cached row's prompt
# still matches the photo's primary detection.
# ---------------------------------------------------------------------------


def _patch_extract_masks_deps(monkeypatch, generate_mask_calls):
    """Stub out the heavy SAM2 / DINOv2 / proxy / quality modules so the
    extract-masks route runs deterministically without ONNX weights.

    `generate_mask_calls` is a list mutated in place — every fake
    generate_mask call appends a (variant, det_box_tuple) tuple. Tests
    assert against this list to check whether SAM was invoked.
    """
    import dino_embed
    import masking
    import numpy as np
    import quality

    def fake_render_proxy(image_path, longest_edge=None):
        return np.zeros((4, 4, 3), dtype=np.uint8)

    def fake_generate_mask(proxy, det_box, variant=None):
        generate_mask_calls.append(
            (variant, tuple(sorted(det_box.items())))
        )
        return np.ones((4, 4), dtype=bool)

    monkeypatch.setattr(masking, "render_proxy", fake_render_proxy)
    monkeypatch.setattr(masking, "generate_mask", fake_generate_mask)
    monkeypatch.setattr(masking, "crop_completeness", lambda m: 0.9)
    monkeypatch.setattr(
        masking, "crop_subject", lambda p, m, margin=0.15: None,
    )
    monkeypatch.setattr(
        quality, "compute_all_quality_features",
        lambda p, m: {
            "subject_tenengrad": 1.5,
            "bg_tenengrad": 0.3,
            "subject_clip_high": 0.01,
            "subject_clip_low": 0.01,
            "subject_y_median": 100.0,
            "bg_separation": 50.0,
            "phash_crop": "deadbeef",
            "noise_estimate": 5.0,
        },
    )
    monkeypatch.setattr(
        dino_embed, "embed",
        lambda p, variant=None: np.zeros(384, dtype=np.float32),
    )
    monkeypatch.setattr(
        dino_embed, "embed_batch",
        lambda imgs, variant=None: np.zeros((len(imgs), 384), dtype=np.float32),
    )
    monkeypatch.setattr(dino_embed, "embedding_to_blob", lambda e: b"")


def _seed_photo_with_detection(db, tmp_path, filename, box, model):
    """Create a folder + photo + JPEG on disk + a single detection row.
    Returns the photo_id."""
    folder_path = str(tmp_path / "photos")
    os.makedirs(folder_path, exist_ok=True)
    folder_id = None
    existing = db.conn.execute(
        "SELECT id FROM folders WHERE path=?", (folder_path,)
    ).fetchone()
    if existing:
        folder_id = existing["id"]
    else:
        folder_id = db.add_folder(folder_path)
    pid = db.add_photo(folder_id, filename, ".jpg", 1000, 1.0)
    Image.new("RGB", (16, 16), "black").save(
        os.path.join(folder_path, filename)
    )
    x, y, w, h = box
    db.save_detections(
        pid,
        [{"box": {"x": x, "y": y, "w": w, "h": h},
          "confidence": 0.9, "category": "animal"}],
        detector_model=model,
    )
    return pid


def test_extract_masks_route_writes_photo_masks_row(
    app_and_db, tmp_path, monkeypatch,
):
    """POSTing /api/jobs/extract-masks must write a photo_masks row for
    the configured variant with the right prompt, AND set
    photos.active_mask_variant. Before the Phase 2 fix-up the route
    only updated photos.mask_path and left photo_masks empty."""
    import config as cfg
    cfg.save({
        "pipeline": {
            "sam2_variant": "sam2-small",
            "dinov2_variant": "vit-b14",
        },
    })

    app, db = app_and_db
    pid = _seed_photo_with_detection(
        db, tmp_path, "bird.jpg", (10, 20, 100, 200), "MegaDetector",
    )

    calls = []
    _patch_extract_masks_deps(monkeypatch, calls)

    col_id = db.add_collection(
        "extract-masks-test",
        json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    client = app.test_client()
    resp = client.post(
        "/api/jobs/extract-masks", json={"collection_id": col_id},
    )
    assert resp.status_code == 200
    job_id = resp.get_json()["job_id"]
    data = wait_for_job_via_client(client, job_id)
    assert data["status"] == "completed", data

    # SAM was invoked exactly once.
    assert len(calls) == 1, calls

    # photo_masks row exists with the right variant + prompt.
    row = db.conn.execute(
        "SELECT variant, detector_model, prompt_x, prompt_y, "
        "prompt_w, prompt_h, path FROM photo_masks WHERE photo_id=?",
        (pid,),
    ).fetchone()
    assert row is not None, "photo_masks row must be written"
    assert row["variant"] == "sam2-small"
    assert row["detector_model"] == "MegaDetector"
    assert row["prompt_x"] == 10
    assert row["prompt_y"] == 20
    assert row["prompt_w"] == 100
    assert row["prompt_h"] == 200
    assert row["path"] and os.path.isfile(row["path"]), (
        "mask PNG must be on disk"
    )

    # photos.active_mask_variant was denormalized.
    pr = db.conn.execute(
        "SELECT active_mask_variant, mask_path FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert pr["active_mask_variant"] == "sam2-small"
    assert pr["mask_path"] == row["path"]


def test_extract_masks_route_skips_sam_when_cached(
    app_and_db, tmp_path, monkeypatch,
):
    """Re-hitting /api/jobs/extract-masks with the same configured
    variant + unchanged detection prompt must skip generate_mask: the
    cached photo_masks row is reused and active_mask_variant is
    re-applied. Before the Phase 2 fix-up the route's `mask_path
    IS NULL` gate skipped these photos entirely instead, so the cache
    short-circuit was never tested for this route."""
    import config as cfg
    cfg.save({
        "pipeline": {
            "sam2_variant": "sam2-small",
            "dinov2_variant": "vit-b14",
        },
    })

    app, db = app_and_db
    pid = _seed_photo_with_detection(
        db, tmp_path, "bird.jpg", (10, 20, 100, 200), "MegaDetector",
    )

    calls = []
    _patch_extract_masks_deps(monkeypatch, calls)

    col_id = db.add_collection(
        "extract-masks-cache-test",
        json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    client = app.test_client()

    # First run: SAM is called once.
    resp = client.post(
        "/api/jobs/extract-masks", json={"collection_id": col_id},
    )
    assert resp.status_code == 200
    data = wait_for_job_via_client(
        client, resp.get_json()["job_id"],
    )
    assert data["status"] == "completed"
    assert len(calls) == 1, (
        f"first run should call generate_mask once; got {calls}"
    )

    # Second run with the same config + same detection: SAM must NOT
    # be called again. The cache hit re-applies active_mask_variant
    # and counts the photo as masked without re-running SAM.
    calls.clear()
    resp2 = client.post(
        "/api/jobs/extract-masks", json={"collection_id": col_id},
    )
    assert resp2.status_code == 200
    data2 = wait_for_job_via_client(
        client, resp2.get_json()["job_id"],
    )
    assert data2["status"] == "completed"
    assert calls == [], (
        f"second run with cached prompt must skip generate_mask; got {calls}"
    )

    # Still exactly one row for this (photo, variant).
    n = db.conn.execute(
        "SELECT COUNT(*) FROM photo_masks "
        "WHERE photo_id=? AND variant='sam2-small'",
        (pid,),
    ).fetchone()[0]
    assert n == 1


def test_extract_masks_route_workspace_branch_respects_detector_confidence(
    app_and_db, tmp_path, monkeypatch,
):
    """Without a ``collection_id``, the workspace SQL branch must filter
    detections by the workspace-effective ``detector_confidence`` floor —
    matching the collection branch (which goes through
    ``get_detections``) and the legacy ``get_photos_missing_masks``
    path. Otherwise SAM/DINO runs on noisy below-threshold boxes and
    activates masks the user shouldn't see."""
    import config as cfg
    cfg.save({
        "pipeline": {
            "sam2_variant": "sam2-small",
            "dinov2_variant": "vit-b14",
        },
        "detector_confidence": 0.5,
    })

    app, db = app_and_db

    # Two photos in the workspace, both with a non-full-image detection.
    # Photo A's detection clears the 0.5 floor; Photo B's does not.
    pid_high = _seed_photo_with_detection(
        db, tmp_path, "above.jpg", (10, 20, 100, 200), "MegaDetector",
    )
    pid_low = _seed_photo_with_detection(
        db, tmp_path, "below.jpg", (30, 40, 80, 90), "MegaDetector",
    )
    # _seed_photo_with_detection hardcodes confidence=0.9, so override
    # the low-confidence row directly.
    db.conn.execute(
        "UPDATE detections SET detector_confidence=0.05 WHERE photo_id=?",
        (pid_low,),
    )
    db.conn.commit()

    calls = []
    _patch_extract_masks_deps(monkeypatch, calls)

    client = app.test_client()
    resp = client.post("/api/jobs/extract-masks", json={})
    assert resp.status_code == 200
    data = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert data["status"] == "completed", data

    # SAM was invoked exactly once — for the above-threshold photo only.
    assert len(calls) == 1, calls

    # photo_masks row exists for the above-threshold photo.
    high_row = db.conn.execute(
        "SELECT variant FROM photo_masks WHERE photo_id=?", (pid_high,),
    ).fetchone()
    assert high_row is not None, "above-threshold photo must be masked"

    # Below-threshold photo is skipped: no photo_masks row, no
    # active variant denormalized onto photos.
    low_row = db.conn.execute(
        "SELECT variant FROM photo_masks WHERE photo_id=?", (pid_low,),
    ).fetchone()
    assert low_row is None, (
        "below-threshold photo must not get a mask"
    )
    low_active = db.conn.execute(
        "SELECT active_mask_variant FROM photos WHERE id=?", (pid_low,),
    ).fetchone()
    assert low_active["active_mask_variant"] is None


# --- POST /api/jobs/cancel-queued (bulk queued-pipeline cancel) ---------
#
# These tests cover the bulk cancel endpoint used by the "Cancel all
# queued" button on the /jobs page. The endpoint must:
#   - cancel every queued pipeline (default: scoped to the active
#     workspace, explicit: scoped to ``workspace_id`` in the body),
#   - never touch running pipelines,
#   - never touch queued pipelines belonging to a different workspace
#     when a workspace is named.
#
# Tests below construct queued+running pipelines by reaching directly
# into ``runner.enqueue_pipeline`` so we don't depend on the full
# pipeline_job stack to build the fixture.


def _block_pipeline_until(event, result=None):
    """Build a work_fn that waits on ``event`` before returning.

    Used to keep a 'running' pipeline pinned to its slot so that
    subsequent ``enqueue_pipeline`` calls land in the queued state.
    """
    def work(job):
        event.wait(timeout=5.0)
        return result or {}
    return work


def _fill_pipeline_slots(runner, workspace_id):
    """Enqueue ``SLOT_CAP`` blocking pipelines + wait for them all to
    start, so the next enqueue lands in the queued state.

    Returns ``(occupant_ids, release_event)``. Call ``release_event.set()``
    to let them finish.
    """
    import threading

    from jobs import SLOT_CAP
    release = threading.Event()
    started = [threading.Event() for _ in range(SLOT_CAP)]
    ids = []
    for i in range(SLOT_CAP):
        evt = started[i]
        def work(job, _evt=evt, _release=release):
            _evt.set()
            _release.wait(timeout=5.0)
            return {}
        ids.append(runner.enqueue_pipeline(
            work_fn=work, config={}, workspace_id=workspace_id,
        ))
    for i, evt in enumerate(started):
        assert evt.wait(timeout=2.0), f"slot-filler {i} never started"
    return ids, release


def test_cancel_queued_endpoint_cancels_all_queued_in_active_workspace(app_and_db):
    """POST /api/jobs/cancel-queued (no body) cancels every queued
    pipeline in the active workspace. The currently-running pipeline
    is left alone.
    """
    app, db = app_and_db
    runner = app._job_runner
    client = app.test_client()
    ws_id = db._active_workspace_id

    # Fill every slot so the next two enqueues stay queued.
    occupant_ids, release = _fill_pipeline_slots(runner, ws_id)
    queued_a = runner.enqueue_pipeline(
        work_fn=lambda job: None, config={}, workspace_id=ws_id,
    )
    queued_b = runner.enqueue_pipeline(
        work_fn=lambda job: None, config={}, workspace_id=ws_id,
    )
    assert runner.get(queued_a)["status"] == "queued"
    assert runner.get(queued_b)["status"] == "queued"

    try:
        resp = client.post("/api/jobs/cancel-queued", json={})
        assert resp.status_code == 200, resp.get_data(as_text=True)
        data = resp.get_json()
        assert set(data["cancelled"]) == {queued_a, queued_b}

        # Queued rows are now cancelled.
        for jid in (queued_a, queued_b):
            row = db.conn.execute(
                "SELECT status FROM job_history WHERE id = ?", (jid,),
            ).fetchone()
            assert row is not None
            assert row["status"] == "cancelled", (
                f"queued job {jid} should be cancelled, was {row['status']}"
            )

        # Slot-filler pipelines (still running) are untouched.
        for jid in occupant_ids:
            assert runner.get(jid)["status"] == "running"
    finally:
        release.set()
        from wait import wait_for_job_via_runner
        for jid in occupant_ids:
            wait_for_job_via_runner(runner, jid)


def test_cancel_queued_endpoint_rejects_invalid_body(app_and_db):
    """Bulk queued cancel requires an object body when JSON is supplied."""
    app, _ = app_and_db
    client = app.test_client()

    for payload in (True, [], "workspace"):
        resp = client.post("/api/jobs/cancel-queued", json=payload)
        assert resp.status_code == 400


def test_cancel_queued_endpoint_rejects_malformed_json_without_cancel(app_and_db):
    """Malformed JSON must not fall back to the destructive default scope."""

    app, db = app_and_db
    runner = app._job_runner
    client = app.test_client()
    ws_id = db._active_workspace_id

    occupant_ids, release = _fill_pipeline_slots(runner, ws_id)
    queued_id = runner.enqueue_pipeline(
        work_fn=lambda job: None, config={}, workspace_id=ws_id,
    )
    assert runner.get(queued_id)["status"] == "queued"

    try:
        for body in ('{"workspace_id":', "   \n\t"):
            resp = client.post(
                "/api/jobs/cancel-queued",
                data=body,
                content_type="application/json",
            )
            assert resp.status_code == 400
            assert runner.get(queued_id)["status"] == "queued"
    finally:
        release.set()
        from wait import wait_for_job_via_runner
        for jid in occupant_ids:
            wait_for_job_via_runner(runner, jid)
        wait_for_job_via_runner(runner, queued_id)


def test_cancel_queued_endpoint_rejects_invalid_workspace_id(app_and_db):
    """``workspace_id`` must be an integer id, not bool or another type."""
    app, _ = app_and_db
    client = app.test_client()

    for workspace_id in (True, False, "1", 1.5):
        resp = client.post(
            "/api/jobs/cancel-queued", json={"workspace_id": workspace_id},
        )
        assert resp.status_code == 400


def test_cancel_queued_endpoint_leaves_other_workspaces_alone(app_and_db):
    """When ``workspace_id`` is given in the body, only queued
    pipelines in that workspace are cancelled. Queued pipelines in
    other workspaces stay queued.
    """
    app, db = app_and_db
    runner = app._job_runner
    client = app.test_client()
    ws_a = db._active_workspace_id
    # Create a second workspace so we have a meaningful "other" id.
    ws_b = db.create_workspace("scoped-other")

    # Fill every slot in ws_a so subsequent enqueues (in either
    # workspace) stay queued. Slot capacity is global, not per-workspace.
    occupant_ids, release = _fill_pipeline_slots(runner, ws_a)
    queued_a = runner.enqueue_pipeline(
        work_fn=lambda job: None, config={}, workspace_id=ws_a,
    )
    queued_b = runner.enqueue_pipeline(
        work_fn=lambda job: None, config={}, workspace_id=ws_b,
    )
    assert runner.get(queued_a)["status"] == "queued"
    assert runner.get(queued_b)["status"] == "queued"

    try:
        # Scope explicitly to workspace A.
        resp = client.post(
            "/api/jobs/cancel-queued", json={"workspace_id": ws_a},
        )
        assert resp.status_code == 200, resp.get_data(as_text=True)
        data = resp.get_json()
        assert data["cancelled"] == [queued_a], data

        # Workspace A's queued row is cancelled.
        row_a = db.conn.execute(
            "SELECT status FROM job_history WHERE id = ?", (queued_a,),
        ).fetchone()
        assert row_a["status"] == "cancelled"

        # Workspace B's queued row is still queued — the bulk cancel
        # must not cross workspace boundaries when a workspace is named.
        assert runner.get(queued_b) is not None
        assert runner.get(queued_b)["status"] == "queued"
    finally:
        release.set()
        from wait import wait_for_job_via_runner
        for jid in occupant_ids:
            wait_for_job_via_runner(runner, jid)
        # Cancel the surviving queued job so the test fixture's
        # teardown isn't waiting on a pipeline that will never run.
        runner.cancel_job(queued_b)


# ---------------------------------------------------------------------------
# Remote (SSH) archive destination for /api/jobs/pipeline — request
# validation. These must all reject BEFORE any job starts, so no SSH/rsync
# seams need faking here; end-to-end runs live in test_pipeline_job.py.
# ---------------------------------------------------------------------------

def _save_remote_target(monkeypatch, tmp_path, **overrides):
    """Save one valid remote target into the test-isolated config and make
    the POST-time GNU-rsync resolution succeed without touching the host."""
    import config as cfg
    import move as move_mod

    entry = {
        "id": "nas1", "name": "NAS", "host": "nas", "user": "me",
        "remote_path": "/volume1/Photography",
        "mount_path": str(tmp_path / "mount"),
    }
    entry.update(overrides)
    cfg.save({"remote_targets": [entry]})
    monkeypatch.setattr(
        move_mod, "resolve_rsync_bin", lambda configured="": "/usr/bin/rsync",
    )
    # resolve_rsync_bin returns any executable path an operator explicitly
    # configured, so the import/pipeline route additionally verifies the
    # candidate is GNU rsync (Apple openrsync can't drive SSH). Stub the
    # check so happy-path tests don't depend on a real GNU rsync being
    # installed in the CI environment.
    monkeypatch.setattr(move_mod, "is_gnu_rsync", lambda _rb: True)
    # The remote-archive preflight also probes for an OpenSSH client
    # (added for Windows). Stub so happy-path tests don't require ssh
    # in the CI environment.
    monkeypatch.setattr(
        move_mod, "resolve_ssh_bin", lambda configured="": "/usr/bin/ssh",
    )
    return entry


def _remote_pipeline_body(src, **overrides):
    body = {
        "sources": [str(src)],
        "remote_target_id": "nas1",
        "remote_subpath": "2026/trip",
        "local_processing": True,
        "skip_classify": True,
        "skip_extract_masks": True,
        "skip_regroup": True,
    }
    body.update(overrides)
    return body


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_remote_archive_rejects_both_destinations(
    app_and_db, tmp_path, monkeypatch,
):
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    src = tmp_path / "card"
    src.mkdir()
    client = app.test_client()
    resp = client.post("/api/jobs/pipeline", json=_remote_pipeline_body(
        src, destination=str(tmp_path / "archive"),
    ))
    assert resp.status_code == 400
    assert "mutually exclusive" in resp.get_json()["error"]


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_remote_archive_unknown_target_404(
    app_and_db, tmp_path, monkeypatch,
):
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    src = tmp_path / "card"
    src.mkdir()
    client = app.test_client()
    resp = client.post("/api/jobs/pipeline", json=_remote_pipeline_body(
        src, remote_target_id="nope",
    ))
    assert resp.status_code == 404
    assert "not found" in resp.get_json()["error"].lower()


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_remote_archive_requires_local_processing(
    app_and_db, tmp_path, monkeypatch,
):
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    src = tmp_path / "card"
    src.mkdir()
    client = app.test_client()
    resp = client.post("/api/jobs/pipeline", json=_remote_pipeline_body(
        src, local_processing=False,
    ))
    assert resp.status_code == 400
    assert "local_processing" in resp.get_json()["error"]


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_remote_archive_requires_subpath(
    app_and_db, tmp_path, monkeypatch,
):
    """No subpath means no archive-folder name: move_folder lands the staged
    folder inside a parent keeping its name, and the subpath's last segment
    IS that name. An empty subpath must 400, not silently merge the import
    into the target's base directory."""
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    src = tmp_path / "card"
    src.mkdir()
    client = app.test_client()
    resp = client.post("/api/jobs/pipeline", json=_remote_pipeline_body(
        src, remote_subpath="",
    ))
    assert resp.status_code == 400
    assert "remote_subpath" in resp.get_json()["error"]


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_remote_archive_rejects_traversal_subpath(
    app_and_db, tmp_path, monkeypatch,
):
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    src = tmp_path / "card"
    src.mkdir()
    client = app.test_client()
    for bad in ("../escape", "/absolute/path"):
        resp = client.post("/api/jobs/pipeline", json=_remote_pipeline_body(
            src, remote_subpath=bad,
        ))
        assert resp.status_code == 400, bad


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_remote_archive_requires_mount_path(
    app_and_db, tmp_path, monkeypatch,
):
    """A target with no local mount path can't keep archived photos in the
    library (the catalog is repointed at the mount path after the move) —
    mirror the move-folder endpoint's refusal."""
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path, mount_path="")
    src = tmp_path / "card"
    src.mkdir()
    client = app.test_client()
    resp = client.post("/api/jobs/pipeline", json=_remote_pipeline_body(src))
    assert resp.status_code == 400
    assert "mount path" in resp.get_json()["error"]


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_remote_archive_requires_gnu_rsync(
    app_and_db, tmp_path, monkeypatch,
):
    import move as move_mod
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    monkeypatch.setattr(
        move_mod, "resolve_rsync_bin", lambda configured="": None,
    )
    src = tmp_path / "card"
    src.mkdir()
    client = app.test_client()
    resp = client.post("/api/jobs/pipeline", json=_remote_pipeline_body(src))
    assert resp.status_code == 400
    assert "rsync" in resp.get_json()["error"].lower()


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_local_processing_requires_destination_or_remote(
    app_and_db, tmp_path,
):
    app, _ = app_and_db
    src = tmp_path / "card"
    src.mkdir()
    client = app.test_client()
    resp = client.post("/api/jobs/pipeline", json={
        "sources": [str(src)],
        "local_processing": True,
        "skip_classify": True,
        "skip_extract_masks": True,
        "skip_regroup": True,
    })
    assert resp.status_code == 400
    err = resp.get_json()["error"]
    assert "destination" in err and "remote target" in err


@pytest.mark.skip(reason="retired pipeline import/archive destination path")
def test_pipeline_remote_archive_snapshots_target_at_enqueue(
    app_and_db, tmp_path, monkeypatch,
):
    """The endpoint must capture the resolved remote target onto
    ``PipelineParams.remote_target_snapshot`` at Start-click time. Otherwise a
    queued pipeline that runs after a settings edit would archive to a
    different host/mount than the jobs panel is showing — the point of the
    move-folder endpoint's build-spec-before-enqueue pattern."""
    import pipeline_job
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    src = tmp_path / "card"
    src.mkdir()

    captured = {}

    def fake_run(job, runner, db_path, workspace_id, params,
                 thumb_cache_dir=None, **_kwargs):
        captured["params"] = params
        return {}

    monkeypatch.setattr(pipeline_job, "run_pipeline_job", fake_run)

    client = app.test_client()
    resp = client.post("/api/jobs/pipeline", json=_remote_pipeline_body(src))
    assert resp.status_code == 200, resp.get_json()

    from wait import wait_for_job_via_runner
    wait_for_job_via_runner(app._job_runner, resp.get_json()["job_id"])

    params = captured.get("params")
    assert params is not None, "fake run_pipeline_job was never invoked"
    snap = params.remote_target_snapshot
    assert snap is not None, (
        "PipelineParams must carry a snapshot of the resolved target so the "
        "queued run archives to what the user picked, not to whatever the "
        "saved target got edited to before the slot opened"
    )
    assert snap["id"] == "nas1"
    assert snap["host"] == "nas"
    assert snap["user"] == "me"
    assert snap["remote_path"] == "/volume1/Photography"
    assert snap["mount_path"] == str(tmp_path / "mount")


# ---------------------------------------------------------------------------
# strategy param on /api/jobs/pipeline (import/process split PR 1)
# ---------------------------------------------------------------------------


def _make_collection(app):
    import json as json_mod

    from db import Database

    db = Database(app.config["DB_PATH"])
    db.set_active_workspace(db._active_workspace_id)
    return db.add_collection("Strategy test", json_mod.dumps([]))


def _job_config(client, job_id):
    resp = client.get(f"/api/jobs/{job_id}")
    assert resp.status_code == 200
    return resp.get_json()["config"]


def _fake_active_model(monkeypatch):
    """Keep the route's no-model auto-skip from firing so process flags
    survive to the job config unmangled."""
    import models

    monkeypatch.setattr(models, "get_active_model", lambda: {
        "id": "fake", "model_type": "timm", "model_str": "fake",
        "weights_path": "",
    })


def _process_id(db, name):
    return next(p["id"] for p in db.get_saved_processes() if p["name"] == name)


def test_pipeline_process_id_expands_flags(app_and_db):
    app, db = app_and_db
    pid = _process_id(db, "Quick look")
    col_id = _make_collection(app)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id, "process_id": pid,
        })
        assert resp.status_code == 200
        cfg = _job_config(client, resp.get_json()["job_id"])
        assert cfg["process_id"] == pid
        assert cfg["skip_classify"] is True
        assert cfg["skip_extract_masks"] is True
        assert cfg["skip_regroup"] is True


def test_pipeline_identify_process_keeps_classify_only(app_and_db, monkeypatch):
    app, db = app_and_db
    pid = _process_id(db, "Identify birds")
    col_id = _make_collection(app)
    _fake_active_model(monkeypatch)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id, "process_id": pid,
        })
        assert resp.status_code == 200
        cfg = _job_config(client, resp.get_json()["job_id"])
        assert cfg["process_id"] == pid
        assert cfg["skip_classify"] is False
        assert cfg["skip_extract_masks"] is True
        assert cfg["skip_regroup"] is True
        assert cfg["miss_enabled"] is False
        # Only Identify birds opts into the species-only save path — the flag
        # that gates regroup_stage's ``run_species_review_pipeline`` call.
        # Without it, a Custom body posting ``skip_regroup: true`` would
        # incorrectly land there too.
        assert cfg["review_mode"] == "species"


def test_pipeline_cull_ready_pins_miss_enabled_false(app_and_db, monkeypatch):
    # Quick look alone can't prove miss_enabled reached PipelineParams:
    # it also sets skip_classify=True, and the misses stage is downstream
    # of classify, so an implementation that never wires the process's
    # miss_enabled through to params would still produce a run without
    # misses (by dint of skip_classify) and this test would go green.
    # Cull-ready has skip_classify=False + miss_enabled=False, so the
    # only way misses can be suppressed is if the process's miss_enabled
    # actually reaches PipelineParams — that's the property pinned here.
    app, db = app_and_db
    pid = _process_id(db, "Cull-ready")
    col_id = _make_collection(app)
    _fake_active_model(monkeypatch)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id, "process_id": pid,
        })
        assert resp.status_code == 200
        cfg = _job_config(client, resp.get_json()["job_id"])
        assert cfg["process_id"] == pid
        assert cfg["miss_enabled"] is False
        assert cfg["skip_classify"] is False  # Cull-ready keeps classify on


def test_pipeline_full_process_opts_into_eye_detection(app_and_db, monkeypatch):
    # A saved process with Eye Keypoints on (the "Full" seed:
    # skip_eye_keypoints=False) run by id must set eye_detect_override=True, so
    # the eye stage runs instead of deferring to the workspace's
    # eye_detect_enabled default (False) and silently skipping — mirroring what
    # checking the Eye Keypoints box on the Process page does.
    app, db = app_and_db
    pid = _process_id(db, "Full")
    col_id = _make_collection(app)
    _fake_active_model(monkeypatch)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id, "process_id": pid,
        })
        assert resp.status_code == 200
        cfg = _job_config(client, resp.get_json()["job_id"])
        assert cfg["eye_detect_override"] is True


def test_pipeline_eyes_off_process_leaves_eye_override_none(app_and_db, monkeypatch):
    # A process with Eye Keypoints off (Identify birds) must NOT force the eye
    # override — nothing to opt into, and the workspace default still governs.
    app, db = app_and_db
    pid = _process_id(db, "Identify birds")
    col_id = _make_collection(app)
    _fake_active_model(monkeypatch)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id, "process_id": pid,
        })
        assert resp.status_code == 200
        cfg = _job_config(client, resp.get_json()["job_id"])
        assert cfg["eye_detect_override"] is None


def test_pipeline_unknown_process_id_404(app_and_db):
    app, _ = app_and_db
    col_id = _make_collection(app)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id, "process_id": 999999,
        })
        assert resp.status_code == 404
        assert "unknown process id" in resp.get_json()["error"]


def test_pipeline_null_process_id_400(app_and_db):
    # The "no process" case is expressed by NOT calling /api/jobs/pipeline.
    # A present-but-null process_id must 400 so the server never silently
    # falls through to default processing when a caller thought they were
    # opting out. Distinct from "unknown process id" — null is a shape error.
    app, _ = app_and_db
    col_id = _make_collection(app)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id, "process_id": None,
        })
        assert resp.status_code == 400
        assert "process_id" in resp.get_json()["error"]


def test_pipeline_non_int_process_id_400(app_and_db):
    # A string process_id is a shape error too.
    app, _ = app_and_db
    col_id = _make_collection(app)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id, "process_id": "quick_look",
        })
        assert resp.status_code == 400


def test_pipeline_omitted_process_id_uses_body_params(app_and_db):
    # No `process_id` key at all -> the route builds PipelineParams from the
    # body as usual. Distinguishing "omitted" from "null" is exactly why the
    # route must check key presence, not truthiness.
    app, _ = app_and_db
    col_id = _make_collection(app)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={"collection_id": col_id})
        assert resp.status_code == 200
        cfg = _job_config(client, resp.get_json()["job_id"])
        assert cfg.get("process_id") is None


def test_pipeline_legacy_strategy_field_rejected(app_and_db):
    # The previous /api/jobs/pipeline shape accepted a "strategy" name
    # ("quick_look", "identify", "full", "cull_ready") and expanded it to
    # stage flags. That vocabulary was replaced by saved-process ids; a
    # caller still sending the old field must get a 400 instead of the
    # request silently falling through to a default full-pipeline run
    # (reclassifying/regrouping the whole collection unasked).
    app, _ = app_and_db
    col_id = _make_collection(app)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id, "strategy": "quick_look",
        })
        assert resp.status_code == 400
        assert "strategy" in resp.get_json()["error"]


def test_pipeline_explicit_flags_beat_process(app_and_db, monkeypatch):
    # A caller may pin one flag on top of a process; explicit wins. The
    # fake model keeps the no-model auto-skip from flipping the same flags
    # and masking a broken merge order.
    app, db = app_and_db
    pid = _process_id(db, "Full")
    col_id = _make_collection(app)
    _fake_active_model(monkeypatch)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id, "process_id": pid, "skip_regroup": True,
        })
        assert resp.status_code == 200
        cfg = _job_config(client, resp.get_json()["job_id"])
        assert cfg["skip_regroup"] is True
        assert cfg["skip_classify"] is False
        # Full's ``review_mode`` is None. Pinning ``skip_regroup=True`` on
        # top of it must NOT bleed the identify preset's species-only
        # save path in — the regroup stage should skip cleanly instead of
        # overwriting the workspace cache with all-REVIEW output.
        assert cfg.get("review_mode") is None


def test_pipeline_skip_regroup_without_strategy_has_no_review_mode(
    app_and_db, monkeypatch,
):
    # No strategy at all + ``skip_regroup: true`` — the exact shape the
    # reviewer flagged: an API client refreshing classifications without
    # touching grouping. The species-only cache write must not fire, so
    # ``review_mode`` must be None in the resulting job config.
    app, _ = app_and_db
    col_id = _make_collection(app)
    _fake_active_model(monkeypatch)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id, "skip_regroup": True,
        })
        assert resp.status_code == 200
        cfg = _job_config(client, resp.get_json()["job_id"])
        assert cfg["skip_regroup"] is True
        assert cfg.get("review_mode") is None


# ---------------------------------------------------------------------------
# folder-scoped process runs (import/process split PR 1)
# ---------------------------------------------------------------------------


def _folder_id_by_path(db, path):
    row = db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", (path,)
    ).fetchone()
    assert row is not None, f"fixture folder missing: {path}"
    return row["id"]


def _collection_photo_ids(db, collection_id):
    return sorted(
        p["id"] for p in db.get_collection_photos(collection_id, per_page=999999)
    )


def _photo_ids_in_folders(db, folder_ids):
    marks = ",".join("?" for _ in folder_ids)
    rows = db.conn.execute(
        f"SELECT id FROM photos WHERE folder_id IN ({marks})", folder_ids,
    ).fetchall()
    return sorted(r["id"] for r in rows)


def test_pipeline_folder_ids_creates_adhoc_collection(app_and_db):
    """A leaf folder scope becomes an ad-hoc collection of exactly that
    folder's photos, and the run proceeds as a collection run."""
    app, db = app_and_db
    child_id = _folder_id_by_path(db, "/photos/2024/January")
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "folder_ids": [child_id], "skip_classify": True, "skip_extract_masks": True, "skip_eye_keypoints": True, "skip_regroup": True,
        })
        assert resp.status_code == 200
        cfg = _job_config(client, resp.get_json()["job_id"])
        assert cfg["collection_id"], cfg
        assert _collection_photo_ids(db, cfg["collection_id"]) == \
            _photo_ids_in_folders(db, [child_id])


def test_pipeline_folder_ids_includes_descendants(app_and_db):
    """Scoping to a workspace root must include photos in child folders —
    the rest of the app treats a folder scope as its subtree (see
    Database.get_folder_subtree_ids). A flat folder_id IN (...) over the
    raw request would miss the bulk of a dated archive tree."""
    app, db = app_and_db
    root_id = _folder_id_by_path(db, "/photos/2024")
    child_id = _folder_id_by_path(db, "/photos/2024/January")
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "folder_ids": [root_id], "skip_classify": True, "skip_extract_masks": True, "skip_eye_keypoints": True, "skip_regroup": True,
        })
        assert resp.status_code == 200
        cfg = _job_config(client, resp.get_json()["job_id"])
        got = _collection_photo_ids(db, cfg["collection_id"])
        assert got == _photo_ids_in_folders(db, [root_id, child_id])
        # Regression tripwire: the child's photos are the recursive part.
        assert set(_photo_ids_in_folders(db, [child_id])) <= set(got)


def test_pipeline_folder_ids_unlinked_folder_404(app_and_db):
    """A folder not linked to the active workspace must 404, mirroring the
    rescan guard — otherwise a stale UI could pollute this workspace with
    another workspace's scan output."""
    app, db = app_and_db
    original_ws = db._active_workspace_id
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    foreign_id = db.add_folder("/photos/elsewhere", name="elsewhere")
    db.set_active_workspace(original_ws)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "folder_ids": [foreign_id], "skip_classify": True, "skip_extract_masks": True, "skip_eye_keypoints": True, "skip_regroup": True,
        })
        assert resp.status_code == 404


def test_pipeline_folder_ids_rejects_non_int(app_and_db):
    """Malformed ids must 400 before touching SQLite, mirroring the
    source_snapshot_id validation."""
    app, _ = app_and_db
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "folder_ids": ["../etc"], "skip_classify": True, "skip_extract_masks": True, "skip_eye_keypoints": True, "skip_regroup": True,
        })
        assert resp.status_code == 400


@pytest.mark.parametrize(
    "bad_fid",
    [
        1 << 63,          # one past SQLite's signed 64-bit max
        -(1 << 63) - 1,   # one below SQLite's signed 64-bit min
        1 << 128,         # obviously out of range
    ],
)
def test_pipeline_folder_ids_rejects_out_of_range_integer(app_and_db, bad_fid):
    """A JSON-safe integer outside SQLite's signed 64-bit range must be
    rejected with 400 before it reaches sqlite3 parameter binding.
    Without the range guard, the workspace-linked lookup binds ``bad_fid``
    directly and raises ``OverflowError``, which escapes as an opaque 500."""
    app, _ = app_and_db
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "folder_ids": [bad_fid], "skip_classify": True, "skip_extract_masks": True, "skip_eye_keypoints": True, "skip_regroup": True,
        })
        assert resp.status_code == 400, resp.get_json()
        assert "folder_ids" in resp.get_json()["error"]


def test_pipeline_folder_ids_includes_legacy_null_parent_descendants(app_and_db):
    """A workspace root's ad-hoc collection must include descendant folders
    whose ``parent_id`` is NULL even though their paths sit under the root.
    Older databases carry such rows; ``get_folder_subtree_ids`` alone walks
    ``folders.parent_id`` and drops them, so processing a workspace root
    would silently skip legacy descendant photos the rest of the app still
    treats as part of that folder subtree. Every other subtree consumer
    (folder deletion, rescan, missing-originals) already reads through
    ``_folder_subtree_ids_by_path`` — this route must too."""
    app, db = app_and_db
    root_id = _folder_id_by_path(db, "/photos/2024")
    # Insert a descendant folder whose path is under the root but whose
    # parent_id is NULL — simulating a legacy row from a pre-parent_id
    # backfill build. Link it to the active workspace so the route's
    # workspace-safety filter can find it.
    legacy_id = db.add_folder("/photos/2024/Legacy", name="Legacy")
    db.conn.execute(
        "UPDATE folders SET parent_id = NULL WHERE id = ?", (legacy_id,),
    )
    db.conn.commit()
    db.add_workspace_folder(db._active_workspace_id, legacy_id, is_root=False)
    legacy_photo = db.add_photo(
        folder_id=legacy_id, filename="legacy.jpg", extension=".jpg",
        file_size=42, file_mtime=42.0,
    )
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "folder_ids": [root_id], "skip_classify": True, "skip_extract_masks": True, "skip_eye_keypoints": True, "skip_regroup": True,
        })
        assert resp.status_code == 200, resp.get_json()
        cfg = _job_config(client, resp.get_json()["job_id"])
        got = _collection_photo_ids(db, cfg["collection_id"])
        assert legacy_photo in got, (
            "legacy NULL-parent_id descendant was omitted from the "
            "ad-hoc collection — path-prefix fallback missing"
        )


def test_pipeline_folder_ids_honors_exclude_paths(app_and_db):
    """A folder-scoped run with ``exclude_paths`` must drop the deselected
    photos from the ad-hoc collection itself. Once ``params.collection_id``
    is set, ``run_pipeline_job`` takes the collection path and its
    ``_filter_excluded`` helper only checks ``exclude_photo_ids``, so an
    excluded path would otherwise still be thumbnailed, classified, and
    regrouped. Filter at the materialization boundary so the collection
    reflects exactly the photos the user asked to process."""
    app, db = app_and_db
    root_id = _folder_id_by_path(db, "/photos/2024")
    child_id = _folder_id_by_path(db, "/photos/2024/January")
    # Photos in the fixture are keyed on folder + filename; the effective
    # "path" for exclude_paths matching is os.path.join(folder.path, filename)
    # — same shape scanner/ingest use for their skip_paths sets.
    excluded_root_file = os.path.join("/photos/2024", "bird1.jpg")
    excluded_child_file = os.path.join(
        "/photos/2024/January", "bird2.jpg",
    )
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "folder_ids": [root_id], "skip_classify": True, "skip_extract_masks": True, "skip_eye_keypoints": True, "skip_regroup": True,
            "exclude_paths": [excluded_root_file, excluded_child_file],
        })
        assert resp.status_code == 200, resp.get_json()
        cfg = _job_config(client, resp.get_json()["job_id"])
        got = set(_collection_photo_ids(db, cfg["collection_id"]))
        subtree = set(_photo_ids_in_folders(db, [root_id, child_id]))
        # Only the non-excluded photo (bird3 in the root) remains.
        remaining = {
            r["id"] for r in db.conn.execute(
                "SELECT id FROM photos WHERE folder_id = ? AND filename = ?",
                (root_id, "bird3.jpg"),
            )
        }
        assert got == remaining
        assert got < subtree, "exclusion produced no reduction — filter no-op"


def test_pipeline_folder_ids_honors_exclude_photo_ids(app_and_db):
    """The ad-hoc collection also drops photos in ``exclude_photo_ids`` so
    its membership matches what the user selected — even though
    ``_filter_excluded`` would drop them again downstream."""
    app, db = app_and_db
    root_id = _folder_id_by_path(db, "/photos/2024")
    child_id = _folder_id_by_path(db, "/photos/2024/January")
    all_ids = _photo_ids_in_folders(db, [root_id, child_id])
    excluded = all_ids[0]
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "folder_ids": [root_id], "skip_classify": True, "skip_extract_masks": True, "skip_eye_keypoints": True, "skip_regroup": True,
            "exclude_photo_ids": [excluded],
        })
        assert resp.status_code == 200, resp.get_json()
        cfg = _job_config(client, resp.get_json()["job_id"])
        got = set(_collection_photo_ids(db, cfg["collection_id"]))
        assert excluded not in got
        assert got == set(all_ids) - {excluded}


def test_pipeline_folder_ids_with_collection_id_400(app_and_db):
    """Two scopes in one request is ambiguous — reject rather than pick."""
    app, db = app_and_db
    root_id = _folder_id_by_path(db, "/photos/2024")
    col_id = _make_collection(app)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "folder_ids": [root_id], "collection_id": col_id,
        })
        assert resp.status_code == 400


@pytest.mark.parametrize(
    "extra",
    [
        {"source": "/tmp/foo"},
        {"sources": ["/tmp/foo", "/tmp/bar"]},
        {"source_snapshot_id": 999},
    ],
)
def test_pipeline_folder_ids_with_other_scope_400(app_and_db, extra):
    """Reject folder_ids combined with *any* other scope — not just
    collection_id. Otherwise run_pipeline_job silently ignores the source
    (because collection_id skips scanning) or clears the folder-derived
    collection_id when a snapshot is present, and the job processes a
    different scope than the request implied. Also verifies no stray
    ad-hoc collection was inserted before the rejection: the check must
    fire before ``add_collection`` runs."""
    app, db = app_and_db
    root_id = _folder_id_by_path(db, "/photos/2024")
    before = len(db.get_collections())
    with app.test_client() as client:
        resp = client.post(
            "/api/jobs/pipeline",
            json={"folder_ids": [root_id], **extra},
        )
        assert resp.status_code == 400
        assert "folder_ids cannot be combined with" in resp.get_json()["error"]
    assert len(db.get_collections()) == before


# os.path.abspath keeps the path shape on POSIX ("/abs/dest") but rewrites
# it to a drive-anchored form on Windows ("C:\abs\dest"), so os.path.isabs
# passes on both platforms and validation falls through to the later checks
# each parametrized case is meant to exercise.
_ABS_DEST = os.path.abspath("/abs/dest")


@pytest.mark.parametrize(
    "extra,fragment",
    [
        # Any destination is rejected outright for folder scope — the
        # scope check fires before the absolute-path guard, because
        # collection scope skips ingest and a copy destination would never
        # be written. Both a relative and an absolute path trip this.
        (
            {"destination": "relative/path", "local_processing": False},
            "import/archive fields are no longer accepted",
        ),
        (
            {"destination": _ABS_DEST, "local_processing": False},
            "import/archive fields are no longer accepted",
        ),
        # local_processing + destination + folder scope: same reject —
        # destination check runs before the local_processing scope check.
        (
            {"local_processing": True, "destination": _ABS_DEST},
            "import/archive fields are no longer accepted",
        ),
        # local_processing + folder scope without destination: this
        # trips the "requires a destination or a remote target" guard
        # (which fires before the local_processing + scope check).
        (
            {"local_processing": True},
            "import/archive fields are no longer accepted",
        ),
        # Non-boolean miss_enabled — the type-check must fire BEFORE
        # ``db.add_collection`` runs. Previously the check sat after
        # materialization, so a rejected request like
        # ``{"folder_ids": [...], "miss_enabled": "false"}`` left a stray
        # "Process …" row behind.
        (
            {"miss_enabled": "false"},
            "miss_enabled",
        ),
    ],
)
def test_pipeline_folder_ids_leaves_no_stray_collection_on_400(
    app_and_db, extra, fragment,
):
    """A rejected folder-scope request must not leave a "Process …"
    collection sitting in the workspace. The ad-hoc insert has to happen
    after every other request check has passed, otherwise the workspace
    accumulates junk every time the caller trips a later validation."""
    app, db = app_and_db
    root_id = _folder_id_by_path(db, "/photos/2024")
    before = len(db.get_collections())
    with app.test_client() as client:
        resp = client.post(
            "/api/jobs/pipeline",
            json={"folder_ids": [root_id], **extra},
        )
        assert resp.status_code == 400, resp.get_json()
        assert fragment in resp.get_json()["error"]
    assert len(db.get_collections()) == before


def test_pipeline_folder_ids_rejects_plain_destination(app_and_db):
    """Regression for the Codex review on commit 459e092: a folder-scoped
    request with ``destination`` and ``local_processing=False`` was
    previously accepted, materializing the ad-hoc collection and queuing a
    job whose ingest step could never run (``collection_id`` sets
    ``skip_scan``). Reject at request time so the user gets a clean 400
    instead of a queued-but-broken run."""
    app, db = app_and_db
    root_id = _folder_id_by_path(db, "/photos/2024")
    before = len(db.get_collections())
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "folder_ids": [root_id],
            "destination": _ABS_DEST,
            "local_processing": False,
        })
        assert resp.status_code == 400, resp.get_json()
        assert "import/archive fields" in resp.get_json()["error"]
    assert len(db.get_collections()) == before


def test_pipeline_collection_id_rejects_plain_destination(app_and_db):
    """Same reasoning as the folder-scope regression above, applied to
    the equivalent ``collection_id + destination`` case: any collection
    scope sets ``skip_scan`` in run_pipeline_job, so the ingest block
    that would copy to ``destination`` never runs."""
    app, _ = app_and_db
    col_id = _make_collection(app)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id,
            "destination": _ABS_DEST,
            "local_processing": False,
        })
        assert resp.status_code == 400, resp.get_json()
        assert "import/archive fields" in resp.get_json()["error"]


def test_pipeline_folder_ids_chunks_wide_subtree(app_and_db, monkeypatch):
    """A folder root that expands into thousands of descendant folder ids
    must not overflow SQLite's per-statement bound-variable cap
    (SQLITE_MAX_VARIABLE_NUMBER = 999 on legacy builds). Force the chunk
    size down and verify a subtree several times its width still resolves
    correctly — a single unchunked ``folder_id IN (?,...,?)`` would raise
    OperationalError before the job was queued and surface as a 500."""
    import db as db_module

    monkeypatch.setattr(db_module, "_SQLITE_PARAM_CHUNK_SIZE", 3)

    app, db = app_and_db
    parent = db.add_folder("/photos/wide", name="wide")
    photo_ids = []
    for i in range(10):
        sub = db.add_folder(
            f"/photos/wide/sub{i}", name=f"sub{i}", parent_id=parent,
        )
        pid = db.add_photo(
            folder_id=sub, filename=f"p{i}.jpg", extension=".jpg",
            file_size=100 + i, file_mtime=100.0 + i,
        )
        photo_ids.append(pid)
    db.add_workspace_folder(db._active_workspace_id, parent)

    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "folder_ids": [parent], "skip_classify": True, "skip_extract_masks": True, "skip_eye_keypoints": True, "skip_regroup": True,
        })
        assert resp.status_code == 200, resp.get_json()
        cfg = _job_config(client, resp.get_json()["job_id"])
        got = _collection_photo_ids(db, cfg["collection_id"])
        assert set(photo_ids) <= set(got)


def test_pipeline_folder_ids_persisted_in_job_config(app_and_db):
    """job_config records the caller's original folder_ids alongside the
    derived ad-hoc collection_id. Without this the Jobs page can show only
    the derived collection, not the folder subtree the user selected —
    reconstructing the selection from the collection is impossible once the
    ad-hoc collection is renamed or deleted."""
    app, db = app_and_db
    root_id = _folder_id_by_path(db, "/photos/2024")
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "folder_ids": [root_id], "skip_classify": True, "skip_extract_masks": True, "skip_eye_keypoints": True, "skip_regroup": True,
        })
        assert resp.status_code == 200
        cfg = _job_config(client, resp.get_json()["job_id"])
        assert cfg.get("folder_ids") == [root_id]
        # Sanity: the derived collection_id is still there so consumers can
        # keep using the collection-scoped code path.
        assert cfg.get("collection_id")


@pytest.mark.parametrize("bad", ["false", "true", 0, 1, "yes", [], {}])
def test_pipeline_miss_enabled_rejects_non_bool(app_and_db, bad):
    """miss_enabled is tri-state (None / True / False) — a truthy non-bool
    like the string ``"false"`` would flow through pipeline_job's
    ``params.miss_enabled is not None`` guard, then be treated as truthy in
    ``not miss_enabled``, silently turning misses ON when the caller wanted
    them OFF. Type-check at enqueue so the caller sees a clean 400."""
    app, _ = app_and_db
    col_id = _make_collection(app)
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "collection_id": col_id, "miss_enabled": bad,
        })
        assert resp.status_code == 400, resp.get_json()
        assert "miss_enabled" in resp.get_json()["error"]


def test_pipeline_miss_enabled_accepts_bools(app_and_db, monkeypatch):
    """The complement of the reject test — real bools survive to job_config
    so a strategy override or a caller explicit toggle can actually take."""
    app, _ = app_and_db
    _fake_active_model(monkeypatch)
    col_id = _make_collection(app)
    with app.test_client() as client:
        for value in (True, False):
            resp = client.post("/api/jobs/pipeline", json={
                "collection_id": col_id, "miss_enabled": value,
            })
            assert resp.status_code == 200, resp.get_json()
            cfg = _job_config(client, resp.get_json()["job_id"])
            assert cfg["miss_enabled"] is value


@pytest.mark.parametrize(
    "extra",
    [
        {"exclude_paths": None},
        {"exclude_paths": "not-a-list"},
        {"exclude_photo_ids": None},
        {"exclude_photo_ids": {"id": 3}},
    ],
)
def test_pipeline_folder_ids_bad_list_field_leaves_no_stray_collection(
    app_and_db, extra,
):
    """Regression for the Codex review on commit 5dc5a9f: a folder-scoped
    request with ``"exclude_paths": null`` (or any non-list value) passed
    every up-front validation, materialized the ad-hoc collection, then
    exploded at ``set(body.get("exclude_paths", []))`` inside the
    PipelineParams constructor. The 500 left a stray "Process …" row
    behind and never queued a job. Reject at request time and confirm
    the workspace's collection count is unchanged."""
    app, db = app_and_db
    root_id = _folder_id_by_path(db, "/photos/2024")
    before = len(db.get_collections())
    with app.test_client() as client:
        resp = client.post(
            "/api/jobs/pipeline",
            json={"folder_ids": [root_id], **extra},
        )
        assert resp.status_code == 400, resp.get_json()
        # Error message names the offending field so the caller can fix it.
        offending = next(iter(extra))
        assert offending in resp.get_json()["error"]
    assert len(db.get_collections()) == before


@pytest.mark.parametrize(
    "extra",
    [
        {"exclude_paths": [{}]},
        {"exclude_paths": [["nested"]]},
        {"exclude_paths": [None]},
        {"exclude_paths": [42]},
        {"exclude_photo_ids": [{}]},
        {"exclude_photo_ids": ["not-int"]},
        {"exclude_photo_ids": [None]},
        {"exclude_photo_ids": [True]},
    ],
)
def test_pipeline_folder_ids_bad_list_entry_leaves_no_stray_collection(
    app_and_db, extra,
):
    """Regression for the Codex review on commit bffdabd: the outer
    list-type check let a payload like ``{"exclude_paths": [{}]}`` slip
    past, whereupon ``set(body.get(...))`` inside PipelineParams raised
    ``TypeError: unhashable type`` — a 500 that left a stray "Process …"
    collection with no queued job. Reject non-string exclude_paths
    entries and non-int (or bool) exclude_photo_ids entries at the
    request boundary and confirm no collection was created."""
    app, db = app_and_db
    root_id = _folder_id_by_path(db, "/photos/2024")
    before = len(db.get_collections())
    with app.test_client() as client:
        resp = client.post(
            "/api/jobs/pipeline",
            json={"folder_ids": [root_id], **extra},
        )
        assert resp.status_code == 400, resp.get_json()
        offending = next(iter(extra))
        assert offending in resp.get_json()["error"]
        # Error message points at "entries" so the caller can distinguish
        # this from the outer-list-type reject in the sibling regression.
        assert "entries" in resp.get_json()["error"]
    assert len(db.get_collections()) == before


@pytest.mark.parametrize(
    "extra",
    [
        {"model_id": []},
        {"model_id": 5},
        {"model_id": {"id": "x"}},
        {"model_ids": 5},
        {"model_ids": "megadetector-v6"},
        {"model_ids": [5]},
        {"model_ids": [None]},
        {"model_ids": [{"id": "x"}]},
    ],
)
def test_pipeline_folder_ids_bad_model_selection_leaves_no_stray_collection(
    app_and_db, extra,
):
    """Regression for the Codex review on commit f08b72e: a folder-scoped
    request with a malformed ``model_id``/``model_ids`` (e.g. ``model_ids: 5``)
    passed every up-front validation, materialized the ad-hoc collection,
    and then blew up inside the auto-skip-classify block
    (``list(params.model_ids or [])`` on a non-list). The 500 left a stray
    "Process …" row behind. Reject at request time and confirm the
    workspace's collection count is unchanged."""
    app, db = app_and_db
    root_id = _folder_id_by_path(db, "/photos/2024")
    before = len(db.get_collections())
    with app.test_client() as client:
        resp = client.post(
            "/api/jobs/pipeline",
            json={"folder_ids": [root_id], **extra},
        )
        assert resp.status_code == 400, resp.get_json()
        offending = next(iter(extra))
        assert offending in resp.get_json()["error"]
    assert len(db.get_collections()) == before


@pytest.mark.parametrize(
    "extra",
    [
        {"source": ""},
        {"sources": []},
        {"source": "", "sources": []},
    ],
)
def test_pipeline_folder_ids_treats_empty_sources_as_omitted(
    app_and_db, extra,
):
    """A generic pipeline form can emit ``source: ""`` / ``sources: []``
    as its unfilled defaults. The rest of this endpoint treats those as
    omitted (see the ``if sources:`` / ``elif source:`` dispatch and the
    required-scope truthiness check), so the folder-scope conflict guard
    must too — otherwise every folder-scoped request from such a form
    would falsely 400 without the client having to delete unused keys."""
    app, db = app_and_db
    root_id = _folder_id_by_path(db, "/photos/2024")
    with app.test_client() as client:
        resp = client.post("/api/jobs/pipeline", json={
            "folder_ids": [root_id], "skip_classify": True, "skip_extract_masks": True, "skip_eye_keypoints": True, "skip_regroup": True, **extra,
        })
        assert resp.status_code == 200, resp.get_json()


# --- POST /api/jobs/import-photos (import job) ---------------------------

def _import_card(tmp_path, names=("DSC_0001.jpg",)):
    card = tmp_path / "import-card"
    card.mkdir(exist_ok=True)
    for name in names:
        Image.new("RGB", (16, 16), "red").save(str(card / name))
    return str(card)


@pytest.mark.parametrize("route,copy_mode", [
    ("/api/jobs/import-in-place", False),
    ("/api/jobs/import-photos", True),
])
def test_photo_import_requires_exiftool_with_explicit_advanced_override(
    app_and_db, tmp_path, monkeypatch, route, copy_mode,
):
    import metadata

    app, _ = app_and_db
    app.config["REQUIRE_EXIFTOOL_FOR_IMPORT"] = True
    monkeypatch.setattr(metadata, "exiftool_status", lambda: {
        "available": False,
        "path": "",
        "version": None,
        "error": None,
        "hint": "repair ExifTool",
    })
    card = _import_card(tmp_path)
    body = {"sources": [card], "after_import": None}
    if copy_mode:
        body["destination"] = str(tmp_path / "archive")

    with app.test_client() as client:
        blocked = client.post(route, json=body)
        assert blocked.status_code == 409
        assert blocked.get_json()["code"] == "exiftool_required"
        assert "capture dates" in blocked.get_json()["error"]

        body["allow_missing_exiftool"] = True
        allowed = client.post(route, json=body)
        assert allowed.status_code == 200, allowed.get_json()
        job = wait_for_job_via_client(client, allowed.get_json()["job_id"])
        assert job["status"] == "completed", job
        assert job["config"]["allow_missing_exiftool"] is True


def test_import_readiness_surfaces_and_starts_metadata_repair(
    app_and_db, tmp_path, monkeypatch,
):
    import metadata
    import scanner

    app, db = app_and_db
    monkeypatch.setattr(metadata, "exiftool_status", lambda: {
        "available": True,
        "path": "/bundled/exiftool",
        "version": "13.59",
        "error": None,
        "hint": "",
    })
    monkeypatch.setattr(scanner, "extract_metadata", lambda paths, **kwargs: {
        path: {"EXIF": {"Make": "Repair Camera"}, "File": {"FileType": "JPEG"}}
        for path in paths
    })
    photos = tmp_path / "repair-photos"
    photos.mkdir()
    source = photos / "repair.jpg"
    Image.new("RGB", (32, 24), "green").save(source)
    folder_id = db.add_folder(str(photos), name="repair-photos")
    photo_id = db.add_photo(
        folder_id=folder_id,
        filename=source.name,
        extension=".jpg",
        file_size=source.stat().st_size,
        file_mtime=source.stat().st_mtime,
    )
    assert db.conn.execute(
        "SELECT exif_data FROM photos WHERE id = ?", (photo_id,),
    ).fetchone()["exif_data"] is None

    with app.test_client() as client:
        ready = client.get("/api/import/readiness")
        assert ready.status_code == 200
        payload = ready.get_json()
        assert payload["exiftool"]["available"] is True
        assert payload["metadata_repair_count"] >= 1
        assert payload["metadata_repair_available"] is True

        started = client.post("/api/jobs/repair-metadata")
        assert started.status_code == 200, started.get_json()
        assert started.get_json()["photo_count"] >= 1
        job = wait_for_job_via_client(client, started.get_json()["job_id"])
        assert job["type"] == "metadata-repair"
        assert job["config"]["repair_metadata"] is True
        repaired = db.conn.execute(
            "SELECT exif_data FROM photos WHERE id = ?", (photo_id,),
        ).fetchone()["exif_data"]
        assert json.loads(repaired)["EXIF"]["Make"] == "Repair Camera"


def test_import_readiness_skips_excluded_bundle_roots(
    app_and_db, tmp_path, monkeypatch,
):
    """The readiness endpoint fires as soon as the Import page opens; it
    must never stat a workspace root that resolves inside a macOS
    ``.photoslibrary`` bundle, since that stat itself trips the TCC
    "access data from other apps" prompt for Photos Library-style
    bundles."""
    import app as app_module
    import metadata

    app, db = app_and_db
    monkeypatch.setattr(metadata, "exiftool_status", lambda: {
        "available": True,
        "path": "/bundled/exiftool",
        "version": "13.59",
        "error": None,
        "hint": "",
    })

    reachable = tmp_path / "reachable"
    reachable.mkdir()
    bundle = tmp_path / "Photos Library.photoslibrary"
    bundle.mkdir()
    db.add_folder(str(reachable), name="reachable")
    db.add_folder(str(bundle), name="legacy-library")

    original_isdir = os.path.isdir
    isdir_calls = []

    def tracking_isdir(path):
        isdir_calls.append(path)
        return original_isdir(path)

    monkeypatch.setattr(app_module.os.path, "isdir", tracking_isdir)

    with app.test_client() as client:
        resp = client.get("/api/import/readiness")
        assert resp.status_code == 200
        payload = resp.get_json()
        assert payload["reachable_root_count"] == 1

    assert str(bundle) not in isdir_calls, (
        f"os.path.isdir must not be called on excluded bundle roots; "
        f"got calls: {isdir_calls!r}"
    )


def test_lightroom_import_route_not_shadowed(app_and_db):
    """POST /api/jobs/import (Lightroom catalogs) keeps its contract —
    the photo import route is a NEW endpoint, not a rename."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/import", json={})
    assert resp.status_code == 400
    assert "catalogs" in resp.get_json()["error"]


def test_import_photos_happy_path(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    card = _import_card(tmp_path)
    dest = str(tmp_path / "archive")

    cull_ready_id = next(
        pr["id"] for pr in db.get_saved_processes() if pr["name"] == "Cull-ready")
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [card],
        "destination": dest,
        "after_import": cull_ready_id,
        "trust_likely_duplicates": True,
    })
    assert resp.status_code == 200, resp.get_json()
    job_id = resp.get_json()["job_id"]
    assert job_id.startswith("import-")

    config = _job_config(client, job_id)
    assert config["sources"] == [card]
    assert config["destination"] == dest
    assert config["folder_template"] == "%Y/%Y-%m-%d"
    assert config["after_import"] == cull_ready_id
    assert config["trust_likely_duplicates"] is True

    job = wait_for_job_via_client(client, job_id)
    assert job["status"] == "completed", job
    result = job["result"]
    assert result["discovered"] == 1
    assert result["copied"] == 1
    assert result["safe_to_format"] is True


def test_import_photos_adds_requested_tags(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    card = _import_card(tmp_path, ("DSC_0001.jpg", "DSC_0002.jpg"))
    Image.new("RGB", (16, 16), "blue").save(
        os.path.join(card, "DSC_0002.jpg")
    )

    resp = client.post("/api/jobs/import-photos", json={
        "sources": [card],
        "destination": str(tmp_path / "archive"),
        "after_import": None,
        "tags": ["Kenya trip", "Portfolio", "kenya trip"],
    })
    assert resp.status_code == 200, resp.get_json()
    job_id = resp.get_json()["job_id"]
    config = _job_config(client, job_id)
    assert config["tags"] == ["Kenya trip", "Portfolio"]

    job = wait_for_job_via_client(client, job_id)
    assert job["status"] == "completed", job
    result = job["result"]
    assert result["tagging"]["tagged_photos"] == 2
    assert result["tagging"]["errors"] == []
    for photo_id in result["photo_ids"]:
        names = {row["name"] for row in db.get_photo_keywords(photo_id)}
        assert {"Kenya trip", "Portfolio"} <= names


def test_import_pause_waits_for_tag_transaction_to_commit(
    app_and_db, tmp_path, monkeypatch,
):
    """Tag writes reach a transaction boundary before honoring Pause."""
    import import_job
    from db import Database

    app, db = app_and_db
    runner = app._job_runner
    client = app.test_client()
    photo_id = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    tag_written = threading.Event()
    release_tag_write = threading.Event()
    pause_requested = threading.Event()

    def imported_result(job, runner, db_path, workspace_id, params):
        return {
            "ok": True,
            "cancelled": False,
            "photo_ids": [photo_id],
            "discovered": 1,
            "copied": 1,
            "verified": 1,
            "skipped_duplicate": 0,
            "failed": 0,
            "safe_to_format": True,
            "unsafe_files": [],
            "folders": {},
            "errors": [],
        }

    original_tag_photo = Database.tag_photo

    def pause_after_uncommitted_tag(self, tagged_photo_id, keyword_id, _commit=True):
        original_tag_photo(self, tagged_photo_id, keyword_id, _commit=_commit)
        if not pause_requested.is_set():
            job_id = next(
                job_id for job_id, job in runner._jobs.items()
                if job["type"] == "import" and job["status"] == "running"
            )
            assert runner.pause_job(job_id) is True
            pause_requested.set()
            tag_written.set()
            assert release_tag_write.wait(timeout=2)

    monkeypatch.setattr(import_job, "run_import_job", imported_result)
    monkeypatch.setattr(Database, "tag_photo", pause_after_uncommitted_tag)
    response = client.post("/api/jobs/import-photos", json={
        "sources": [_import_card(tmp_path)],
        "destination": str(tmp_path / "archive"),
        "after_import": None,
        "tags": ["First import tag", "Second import tag"],
    })
    assert response.status_code == 200, response.get_json()
    job_id = response.get_json()["job_id"]
    assert tag_written.wait(timeout=2)
    assert runner.get(job_id)["status"] == "pausing"

    release_tag_write.set()
    _wait_for_runner_status(runner, job_id, "paused")

    # Reaching paused means the first tag transaction has committed; a pause
    # can no longer pin an uncommitted SQLite write lock indefinitely.
    first_tag = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("First import tag",),
    ).fetchone()
    assert first_tag is not None
    assert db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (photo_id, first_tag["id"]),
    ).fetchone() is not None

    assert runner.resume_job(job_id) is True
    job = wait_for_job_via_runner(runner, job_id)
    assert job["status"] == "completed", job
    assert job["result"]["tagging"]["tagged_photos"] == 1


def test_import_tag_reuses_keyword_repaired_from_legacy_peer(
    app_and_db, tmp_path, monkeypatch,
):
    import import_job

    app, db = app_and_db
    client = app.test_client()
    photo_id = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    clean_id = db.add_keyword("Import Legacy", kw_type="general")
    legacy_id = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES (?, 'general')",
        ("‘Import Legacy",),
    ).lastrowid
    db.conn.commit()
    db.tag_photo(photo_id, legacy_id)
    # Simulate the supported upgrade sequence. The normalization repair runs
    # before requests and merges the legacy spelling into the canonical row;
    # runtime import code can then rely on the stored-name invariant instead
    # of repeating normalized peer scans at every call site.
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = 'keyword_names_normalized'"
    )
    db.conn.commit()
    db.normalize_keyword_data()

    def imported_result(job, runner, db_path, workspace_id, params):
        return {
            "ok": True,
            "cancelled": False,
            "photo_ids": [photo_id],
            "discovered": 1,
            "copied": 1,
            "verified": 1,
            "skipped_duplicate": 0,
            "failed": 0,
            "safe_to_format": True,
            "unsafe_files": [],
            "folders": {},
            "errors": [],
        }

    monkeypatch.setattr(import_job, "run_import_job", imported_result)
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [_import_card(tmp_path)],
        "destination": str(tmp_path / "archive"),
        "after_import": None,
        "tags": ["Import Legacy"],
    })
    assert resp.status_code == 200, resp.get_json()
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed", job
    assert job["result"]["tagging"]["tagged_photos"] == 0
    linked = db.conn.execute(
        "SELECT keyword_id FROM photo_keywords "
        "WHERE photo_id = ? AND keyword_id IN (?, ?)",
        (photo_id, clean_id, legacy_id),
    ).fetchall()
    assert [row["keyword_id"] for row in linked] == [clean_id]


def test_duplicate_only_import_does_not_tag_existing_photos(
    app_and_db, tmp_path,
):
    app, db = app_and_db
    client = app.test_client()
    card = _import_card(tmp_path)
    destination = str(tmp_path / "archive")

    first = client.post("/api/jobs/import-photos", json={
        "sources": [card], "destination": destination, "after_import": None,
    })
    wait_for_job_via_client(client, first.get_json()["job_id"])

    second = client.post("/api/jobs/import-photos", json={
        "sources": [card],
        "destination": destination,
        "after_import": None,
        "tags": ["Do not add to duplicates"],
    })
    job = wait_for_job_via_client(client, second.get_json()["job_id"])
    result = job["result"]
    assert result["photo_ids"] == []
    assert result["tagging"]["skipped"] == "no new photos"
    assert db.conn.execute(
        "SELECT 1 FROM keywords WHERE name = ?",
        ("Do not add to duplicates",),
    ).fetchone() is None


def test_cancelled_import_does_not_apply_requested_tags(
    app_and_db, tmp_path, monkeypatch,
):
    import import_job

    app, db = app_and_db
    client = app.test_client()
    photo_id = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]

    def cancelled_result(job, runner, db_path, workspace_id, params):
        return {
            "ok": False,
            "cancelled": True,
            "photo_ids": [photo_id],
            "discovered": 1,
            "copied": 1,
            "verified": 0,
            "skipped_duplicate": 0,
            "failed": 0,
            "safe_to_format": False,
            "unsafe_files": [],
            "folders": {},
            "errors": [],
        }

    monkeypatch.setattr(import_job, "run_import_job", cancelled_result)
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [_import_card(tmp_path)],
        "destination": str(tmp_path / "archive"),
        "after_import": None,
        "tags": ["Must not be added"],
        "location_from_gps": True,
    })
    assert resp.status_code == 200, resp.get_json()
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    # The synthetic worker returns a cancelled result without setting the
    # runner's cancellation flag, so JobRunner records this fixture as failed;
    # the production cancellation path sets both. The behavior under test is
    # that the result marker alone suppresses all post-import mutations.
    assert job["status"] == "failed", job
    assert job["result"]["tagging"]["skipped"] == "import cancelled"
    assert db.conn.execute(
        "SELECT 1 FROM keywords WHERE name = ?", ("Must not be added",),
    ).fetchone() is None


def test_import_gps_tagging_stops_when_cancelled_during_resolution(
    app_and_db, tmp_path, monkeypatch,
):
    import import_job
    from db import Database

    app, db = app_and_db
    client = app.test_client()
    runner = app._job_runner
    photo_id = db.conn.execute("SELECT id FROM photos LIMIT 1").fetchone()["id"]
    db.clear_photo_location(photo_id)

    def imported_result(job, runner, db_path, workspace_id, params):
        return {
            "ok": True,
            "cancelled": False,
            "photo_ids": [photo_id],
            "discovered": 1,
            "copied": 1,
            "verified": 1,
            "skipped_duplicate": 0,
            "failed": 0,
            "safe_to_format": True,
            "unsafe_files": [],
            "folders": {},
            "errors": [],
        }

    original_get = Database.get_photos_by_ids

    def cancel_during_resolution(self, photo_ids):
        running_ids = [
            job_id for job_id, job in runner._jobs.items()
            if job["type"] == "import" and job["status"] == "running"
        ]
        assert len(running_ids) == 1
        assert runner.cancel_job(running_ids[0]) is True
        return original_get(self, photo_ids)

    monkeypatch.setattr(import_job, "run_import_job", imported_result)
    monkeypatch.setattr(Database, "get_photos_by_ids", cancel_during_resolution)
    quick_look_id = _process_id(db, "Quick look")
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [_import_card(tmp_path)],
        "destination": str(tmp_path / "archive"),
        "after_import": quick_look_id,
        "location_from_gps": True,
    })
    assert resp.status_code == 200, resp.get_json()
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "cancelled", job
    assert job["result"]["cancelled"] is True
    assert job["result"]["tagging"]["skipped"] == "import cancelled"
    assert job["result"]["after_import_skipped"] == "import cancelled"
    assert "process_job_id" not in job["result"]
    assert db.get_assigned_photo_location(photo_id) is None


@pytest.mark.parametrize("field,value,error", [
    ("tags", "Trip", "tags must be a list"),
    ("tags", ["Trip", 4], "only strings"),
    ("tags", ["   "], "must not be empty"),
    ("location_from_gps", "yes", "must be a boolean"),
])
def test_import_tag_options_are_validated_before_starting_job(
    app_and_db, tmp_path, field, value, error,
):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [_import_card(tmp_path)],
        "destination": str(tmp_path / "archive"),
        "after_import": None,
        field: value,
    })
    assert resp.status_code == 400
    assert error in resp.get_json()["error"]


def test_import_can_add_structured_locations_from_each_photos_gps(
    app_and_db, tmp_path, monkeypatch,
):
    from db import Database

    app, db = app_and_db
    client = app.test_client()
    original_get = Database.get_photos_by_ids
    details = {
        "place_id": "import-gps-place",
        "name": "Central Park",
        "lat": 40.785091,
        "lng": -73.968285,
        "types": ["park"],
        "address_components": [
            {"name": "New York", "types": ["locality"]},
            {"name": "New York", "types": ["administrative_area_level_1"]},
            {"name": "United States", "types": ["country"]},
        ],
    }

    def photos_with_gps(self, photo_ids):
        rows = original_get(self, photo_ids)
        enriched = {}
        for photo_id, row in rows.items():
            photo = dict(row)
            photo["latitude"] = details["lat"]
            photo["longitude"] = details["lng"]
            enriched[photo_id] = photo
        self.reverse_geocode_cache_put(
            details["lat"], details["lng"], details["place_id"],
            json.dumps(details),
        )
        return enriched

    monkeypatch.setattr(Database, "get_photos_by_ids", photos_with_gps)
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [_import_card(tmp_path)],
        "destination": str(tmp_path / "archive"),
        "after_import": None,
        "location_from_gps": True,
    })
    assert resp.status_code == 200, resp.get_json()
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])
    assert job["status"] == "completed", job
    result = job["result"]
    assert result["tagging"]["locations_added"] == 1
    photo_id = result["photo_ids"][0]
    location = db.get_assigned_photo_location(photo_id)
    assert location["keyword_location_name"] == "Central Park"


def test_import_in_place_no_destination_required(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    card = _import_card(tmp_path)

    resp = client.post("/api/jobs/import-in-place", json={
        "sources": [card],
        "after_import": None,
    })
    assert resp.status_code == 200, resp.get_json()
    job_id = resp.get_json()["job_id"]

    config = _job_config(client, job_id)
    assert config["sources"] == [card]
    assert config["destination"] is None
    assert config["mode"] == "in_place"
    assert config["after_import"] is None

    job = wait_for_job_via_client(client, job_id)
    assert job["status"] == "completed", job
    result = job["result"]
    assert result["mode"] == "in_place"
    assert result["indexed"] == 1
    assert result["ok"] is True
    assert result["after_import_skipped"] == "import-only"
    assert result["collection_name"].startswith("Import ")
    photos = db.get_collection_photos(
        result["collection_id"], per_page=999999,
    )
    assert [p["id"] for p in photos] == result["photo_ids"]


def test_import_in_place_can_target_new_workspace(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    old_ws = db._active_workspace_id

    resp = client.post("/api/jobs/import-in-place", json={
        "sources": [_import_card(tmp_path)],
        "after_import": None,
        "new_workspace_name": "Card Import",
    })
    assert resp.status_code == 200, resp.get_json()
    data = resp.get_json()
    assert data["workspace"]["name"] == "Card Import"

    config = _job_config(client, data["job_id"])
    assert config["workspace_id"] != old_ws
    assert config["created_workspace"]["name"] == "Card Import"
    active = client.get("/api/workspaces/active").get_json()
    assert active["id"] == config["workspace_id"]


def test_import_photos_null_after_import_is_import_only(app_and_db, tmp_path):
    """after_import: null means import-only (the chaining hook short-circuits)
    — same nullable vocabulary as pipeline.default_process_id."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [_import_card(tmp_path)],
        "destination": str(tmp_path / "archive"),
        "after_import": None,
    })
    assert resp.status_code == 200, resp.get_json()
    config = _job_config(client, resp.get_json()["job_id"])
    assert config["after_import"] is None


def test_import_photos_can_target_new_workspace(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    old_ws = db._active_workspace_id

    resp = client.post("/api/jobs/import-photos", json={
        "sources": [_import_card(tmp_path)],
        "destination": str(tmp_path / "archive"),
        "after_import": None,
        "new_workspace_name": "Archive Import",
    })
    assert resp.status_code == 200, resp.get_json()
    data = resp.get_json()
    assert data["workspace"]["name"] == "Archive Import"

    config = _job_config(client, data["job_id"])
    assert config["workspace_id"] != old_ws
    assert config["created_workspace"]["name"] == "Archive Import"
    active = client.get("/api/workspaces/active").get_json()
    assert active["id"] == config["workspace_id"]


def test_import_photos_invalid_after_import_type_400(app_and_db, tmp_path):
    """A non-int after_import fails at enqueue, not at completion — failing
    the chain hours later is the old pipeline's mistake."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [_import_card(tmp_path)],
        "destination": str(tmp_path / "archive"),
        "after_import": "yolo",
    })
    assert resp.status_code == 400
    assert "process id" in resp.get_json()["error"]


def test_import_photos_unknown_after_import_id_400(app_and_db, tmp_path):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [_import_card(tmp_path)],
        "destination": str(tmp_path / "archive"),
        "after_import": 999999,
    })
    assert resp.status_code == 400
    assert "unknown process id" in resp.get_json()["error"]


def test_import_photos_after_import_defaults_from_workspace(
        app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    ws_id = db._active_workspace_id
    pid = next(p["id"] for p in db.get_saved_processes()
               if p["name"] == "Cull-ready")
    db.update_workspace(ws_id, config_overrides={
        "pipeline": {"default_process_id": pid},
    })
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [_import_card(tmp_path)],
        "destination": str(tmp_path / "archive"),
    })
    assert resp.status_code == 200, resp.get_json()
    config = _job_config(client, resp.get_json()["job_id"])
    assert config["after_import"] == pid


def test_import_photos_new_workspace_ignores_old_default_process(
        app_and_db, tmp_path):
    """Regression: an omitted after_import must resolve against the TARGET
    workspace's effective config, not the caller's previously-active one.
    Otherwise a stale pipeline.default_process_id override on the old
    workspace silently chains onto a fresh-workspace import."""
    app, db = app_and_db
    client = app.test_client()
    old_ws = db._active_workspace_id
    pid = db.get_saved_processes()[0]["id"]
    db.update_workspace(old_ws, config_overrides={
        "pipeline": {"default_process_id": pid},
    })
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [_import_card(tmp_path)],
        "destination": str(tmp_path / "archive"),
        "new_workspace_name": "Fresh Archive",
    })
    assert resp.status_code == 200, resp.get_json()
    config = _job_config(client, resp.get_json()["job_id"])
    assert config["workspace_id"] != old_ws
    assert config["after_import"] is None


def test_import_in_place_new_workspace_ignores_old_default_process(
        app_and_db, tmp_path):
    """Same regression as above, exercised through the in-place endpoint —
    both routes call _prepare_import_workspace so both had the bug."""
    app, db = app_and_db
    client = app.test_client()
    old_ws = db._active_workspace_id
    pid = db.get_saved_processes()[0]["id"]
    db.update_workspace(old_ws, config_overrides={
        "pipeline": {"default_process_id": pid},
    })
    resp = client.post("/api/jobs/import-in-place", json={
        "sources": [_import_card(tmp_path)],
        "new_workspace_name": "Fresh In-Place",
    })
    assert resp.status_code == 200, resp.get_json()
    config = _job_config(client, resp.get_json()["job_id"])
    assert config["workspace_id"] != old_ws
    assert config["after_import"] is None


def test_import_photos_validation_400s(app_and_db, tmp_path):
    app, _ = app_and_db
    client = app.test_client()
    card = _import_card(tmp_path)
    dest = str(tmp_path / "archive")

    # Missing sources.
    resp = client.post("/api/jobs/import-photos", json={"destination": dest})
    assert resp.status_code == 400
    # Missing destination.
    resp = client.post("/api/jobs/import-photos", json={"sources": [card]})
    assert resp.status_code == 400
    # Relative destination.
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [card], "destination": "relative/archive",
    })
    assert resp.status_code == 400
    # Nonexistent source directory.
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [str(tmp_path / "nope")], "destination": dest,
    })
    assert resp.status_code == 400
    # macOS app-managed library as source (pre-stat rejection).
    bundle = tmp_path / "Photos Library.photoslibrary"
    bundle.mkdir()
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [str(bundle)], "destination": dest,
    })
    assert resp.status_code == 400
    assert "macos" in resp.get_json()["error"].lower()
    # Unsafe folder template.
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [card], "destination": dest,
        "folder_template": "../escape",
    })
    assert resp.status_code == 400


def test_import_photos_rejects_destination_inside_source(app_and_db, tmp_path):
    """Destinations equal to or nested under any source path are rejected
    at enqueue. Otherwise the importer copies card files into the same
    tree it's importing, ``safe_to_format`` still flips green once
    ``copied + skipped_duplicate == discovered``, and formatting the card
    erases the supposed archive copy — silent data loss.
    """
    app, _ = app_and_db
    client = app.test_client()
    card = _import_card(tmp_path)

    # Destination equal to source.
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [card], "destination": card,
    })
    assert resp.status_code == 400
    assert "inside a source" in resp.get_json()["error"]

    # Destination nested under source.
    nested = os.path.join(card, "archive")
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [card], "destination": nested,
    })
    assert resp.status_code == 400
    assert "inside a source" in resp.get_json()["error"]

    # A symlink that resolves back inside the source must also be
    # rejected — otherwise the rule is trivially bypassed.
    symlinked_dest = tmp_path / "symlinked-archive"
    try:
        os.symlink(card, str(symlinked_dest))
    except (OSError, NotImplementedError):
        # Windows filesystems without symlink support: skip that leg.
        pass
    else:
        resp = client.post("/api/jobs/import-photos", json={
            "sources": [card], "destination": str(symlinked_dest),
        })
        assert resp.status_code == 400
        assert "inside a source" in resp.get_json()["error"]

    # A sibling destination NOT nested under the source is fine.
    sibling = str(tmp_path / "archive")
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [card], "destination": sibling,
    })
    assert resp.status_code == 200, resp.get_json()


def test_import_photos_inconclusive_case_probe_rejects_case_collision(
    app_and_db, tmp_path,
):
    """When the case-sensitivity probe of a source cannot determine the
    filesystem's semantics (no alpha-containing entry to swap — an SD
    card whose root holds only numeric ``100``/``200``-style Nikon
    subdirectories), the containment check must fall back to case-fold.

    Otherwise, on a case-insensitive card mounted at ``/mnt/Card`` a
    destination like ``/mnt/card/archive`` differs only in case and
    resolves to the same directory as the source, but a case-sensitive
    string comparison accepts it — and ``safe_to_format`` later goes
    green even though formatting the card would erase the archive
    copy. See PR #1107 review.
    """
    import sys as _sys

    if _sys.platform in ("darwin", "win32"):
        pytest.skip(
            "Linux-only probe fallback: darwin/win32 skip the probe "
            "entirely and always case-fold."
        )

    app, _ = app_and_db
    client = app.test_client()

    # Source has only numeric-named entries: the probe has no alpha
    # character to case-swap, so it must return True (assume
    # case-insensitive) — the stricter fallback.
    source = tmp_path / "Card-BAR"
    source.mkdir()
    (source / "100").mkdir()
    (source / "200").mkdir()

    # Destination differs from the source parent only in case. On a
    # real case-insensitive card these resolve to the same directory;
    # the guard must reject the destination even though the CI
    # filesystem (ext4) treats them as distinct.
    dest = str(tmp_path / "card-bar" / "archive")

    resp = client.post("/api/jobs/import-photos", json={
        "sources": [str(source)], "destination": dest,
    })
    assert resp.status_code == 400
    assert "inside a source" in resp.get_json()["error"]


# --- POST /api/jobs/import-photos remote (SSH) archive target ------------
#
# Mirror the pipeline route's remote-target guards: accept a saved
# remote_target_id + remote_subpath, reject the bad shapes.

def _remote_import_body(card, **overrides):
    body = {
        "sources": [card],
        "remote_target_id": "nas1",
        "remote_subpath": "2026/trip",
    }
    body.update(overrides)
    return body


def test_import_photos_remote_happy_path(app_and_db, tmp_path, monkeypatch):
    """A valid remote target + subpath enqueues an import job whose config
    records the target and subpath and whose destination is the resolved
    mount path (mount_path/subpath)."""
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    client = app.test_client()
    card = _import_card(tmp_path)

    resp = client.post(
        "/api/jobs/import-photos", json=_remote_import_body(card))
    assert resp.status_code == 200, resp.get_json()
    job_id = resp.get_json()["job_id"]
    assert job_id.startswith("import-")

    config = _job_config(client, job_id)
    assert config["remote_target_id"] == "nas1"
    assert config["remote_subpath"] == "2026/trip"
    # Destination recorded as the resolved local mount path.
    expected_dest = os.path.join(str(tmp_path / "mount"), "2026", "trip")
    assert config["destination"] == expected_dest


def test_import_photos_remote_and_destination_mutually_exclusive(
        app_and_db, tmp_path, monkeypatch):
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    client = app.test_client()
    card = _import_card(tmp_path)
    resp = client.post("/api/jobs/import-photos", json=_remote_import_body(
        card, destination=str(tmp_path / "archive"),
    ))
    assert resp.status_code == 400
    assert "mutually exclusive" in resp.get_json()["error"]


def test_import_photos_remote_unknown_target_404(
        app_and_db, tmp_path, monkeypatch):
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    client = app.test_client()
    card = _import_card(tmp_path)
    resp = client.post("/api/jobs/import-photos", json=_remote_import_body(
        card, remote_target_id="nope",
    ))
    assert resp.status_code == 404
    assert "not found" in resp.get_json()["error"].lower()


def test_import_photos_remote_requires_subpath(
        app_and_db, tmp_path, monkeypatch):
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    client = app.test_client()
    card = _import_card(tmp_path)
    resp = client.post("/api/jobs/import-photos", json=_remote_import_body(
        card, remote_subpath="",
    ))
    assert resp.status_code == 400
    assert "remote_subpath" in resp.get_json()["error"]


def test_import_photos_remote_subpath_requires_target(
        app_and_db, tmp_path, monkeypatch):
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    client = app.test_client()
    card = _import_card(tmp_path)
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [card], "remote_subpath": "2026/trip",
    })
    assert resp.status_code == 400
    assert "remote_subpath requires remote_target_id" in \
        resp.get_json()["error"]


def test_import_photos_remote_rejects_traversal_subpath(
        app_and_db, tmp_path, monkeypatch):
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    client = app.test_client()
    card = _import_card(tmp_path)
    for bad in ("../escape", "/absolute/path"):
        resp = client.post(
            "/api/jobs/import-photos",
            json=_remote_import_body(card, remote_subpath=bad),
        )
        assert resp.status_code == 400, bad


def test_import_photos_remote_requires_mount_path(
        app_and_db, tmp_path, monkeypatch):
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path, mount_path="")
    client = app.test_client()
    card = _import_card(tmp_path)
    resp = client.post(
        "/api/jobs/import-photos", json=_remote_import_body(card))
    assert resp.status_code == 400
    assert "mount path" in resp.get_json()["error"]


def test_import_photos_remote_requires_gnu_rsync(
        app_and_db, tmp_path, monkeypatch):
    import move as move_mod
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    monkeypatch.setattr(
        move_mod, "resolve_rsync_bin", lambda configured="": None)
    client = app.test_client()
    card = _import_card(tmp_path)
    resp = client.post(
        "/api/jobs/import-photos", json=_remote_import_body(card))
    assert resp.status_code == 400
    assert "rsync" in resp.get_json()["error"].lower()


def test_import_photos_remote_rejects_openrsync_before_enqueue(
        app_and_db, tmp_path, monkeypatch):
    """resolve_rsync_bin returns any executable file, so a user who
    explicitly sets Settings→Paths to macOS's /usr/bin/rsync (Apple
    openrsync) passes the presence check even though openrsync can't
    drive rsync-over-SSH. The route must additionally consult
    is_gnu_rsync and fail fast at enqueue instead of starting a job that
    dies mid-transfer. See PR #1113 review."""
    import move as move_mod
    app, _ = app_and_db
    _save_remote_target(monkeypatch, tmp_path)
    monkeypatch.setattr(
        move_mod, "resolve_rsync_bin",
        lambda configured="": "/usr/bin/rsync")
    monkeypatch.setattr(move_mod, "is_gnu_rsync", lambda p: False)
    client = app.test_client()
    card = _import_card(tmp_path)
    resp = client.post(
        "/api/jobs/import-photos", json=_remote_import_body(card))
    assert resp.status_code == 400
    assert "rsync" in resp.get_json()["error"].lower()


# --- after-import chaining (import/process split PR 3) ---


def _chain_card(tmp_path, n=2, name="chain-card"):
    import datetime as _dt

    card = tmp_path / name
    card.mkdir(exist_ok=True)
    for i in range(n):
        p = card / f"DSC_{i:04d}.jpg"
        Image.new("RGB", (16, 16), "red").save(str(p))
        ts = _dt.datetime(2026, 7, 3, 10, i).timestamp()
        os.utime(str(p), (ts, ts))
    return card


def _post_import(client, card, archive, after_import="omit"):
    body = {"sources": [str(card)], "destination": str(archive)}
    if after_import != "omit":
        body["after_import"] = after_import
    resp = client.post("/api/jobs/import-photos", json=body)
    assert resp.status_code == 200, resp.get_json()
    return resp.get_json()["job_id"]


def test_import_chains_process_job(app_and_db, tmp_path):
    """A successful import with after_import set enqueues a process job
    scoped to exactly the imported photos, and links the two in history."""
    from wait import wait_for_job_via_client

    app, db = app_and_db
    card = _chain_card(tmp_path)
    with app.test_client() as client:
        quick_look_id = next(
            pr["id"] for pr in db.get_saved_processes()
            if pr["name"] == "Quick look")
        job_id = _post_import(client, card, tmp_path / "arch", quick_look_id)
        job = wait_for_job_via_client(client, job_id)
        res = job["result"]
        assert res.get("process_job_id"), res
        assert "after_import_skipped" not in res

        pj = client.get(f"/api/jobs/{res['process_job_id']}").get_json()
        assert pj["config"]["process_id"] == quick_look_id
        assert pj["config"].get("chained_from") == job_id
        col_id = pj["config"]["collection_id"]
        assert res["collection_id"] == col_id
        assert res["collection_name"].startswith("Import ")
        photos = db.get_collection_photos(col_id, per_page=999999)
        assert sorted(p["id"] for p in photos) == sorted(res["photo_ids"])


def test_import_pauses_chained_classification_when_labels_are_missing(
    app_and_db, tmp_path, monkeypatch,
):
    """A successful import must not enqueue a pipeline guaranteed to fail."""
    import models

    app, db = app_and_db
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "id": "bioclip-vit-b-16",
        "downloaded": True,
        "model_type": "bioclip",
        "model_str": "ViT-B-16",
        "weights_path": str(tmp_path / "model"),
    })
    identify_id = _process_id(db, "Identify birds")
    card = _chain_card(tmp_path)

    with app.test_client() as client:
        job_id = _post_import(client, card, tmp_path / "arch", identify_id)
        job = wait_for_job_via_client(client, job_id)
        result = job["result"]

        assert job["status"] == "completed", job
        assert result["collection_id"]
        assert "process_job_id" not in result
        assert result["after_import_skipped"].startswith("paused —")
        assert "Settings › Labels" in result["after_import_skipped"]
        history = client.get("/api/jobs/history").get_json()
        chained = [
            item for item in history
            if item.get("config", {}).get("chained_from") == job_id
        ]
        assert chained == []


def test_import_only_choice_skips_chaining(app_and_db, tmp_path):
    """Import-only still records the exact import as a Browse collection."""
    from wait import wait_for_job_via_client

    app, db = app_and_db
    card = _chain_card(tmp_path)
    with app.test_client() as client:
        job_id = _post_import(client, card, tmp_path / "arch", None)
        job = wait_for_job_via_client(client, job_id)
        res = job["result"]
        assert res.get("after_import_skipped") == "import-only"
        assert "process_job_id" not in res
        assert res["collection_name"].startswith("Import ")
        photos = db.get_collection_photos(
            res["collection_id"], per_page=999999,
        )
        assert sorted(p["id"] for p in photos) == sorted(res["photo_ids"])


def test_failed_import_does_not_chain(app_and_db, tmp_path):
    """A green processing run must not hide a failed import (rollup
    convention): any failed file suppresses chaining."""
    from wait import wait_for_job_via_client

    app, db = app_and_db
    quick_look_id = next(
        pr["id"] for pr in db.get_saved_processes() if pr["name"] == "Quick look")
    card = _chain_card(tmp_path)
    unreadable = card / "DSC_9999.jpg"
    Image.new("RGB", (16, 16), "blue").save(str(unreadable))
    os.chmod(str(unreadable), 0)
    try:
        with app.test_client() as client:
            job_id = _post_import(
                client, card, tmp_path / "arch", quick_look_id,
            )
            job = wait_for_job_via_client(client, job_id)
            res = job["result"]
            assert res["failed"] >= 1
            assert res.get("after_import_skipped") == "import failed"
            assert "process_job_id" not in res
            assert "collection_id" not in res
    finally:
        os.chmod(str(unreadable), 0o644)


def test_cancelled_import_does_not_create_collection(
        app_and_db, tmp_path, monkeypatch):
    """Partial photo ids from a cancelled run must not look like a complete
    import collection in Browse."""
    import import_job
    from wait import wait_for_job_via_client

    app, db = app_and_db
    existing_photo_id = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1"
    ).fetchone()["id"]

    def cancelled_result(*args, **kwargs):
        return {
            "ok": False,
            "cancelled": True,
            "photo_ids": [existing_photo_id],
            "copied": 1,
            "failed": 0,
        }

    monkeypatch.setattr(import_job, "run_import_job", cancelled_result)
    quick_look_id = next(
        pr["id"] for pr in db.get_saved_processes() if pr["name"] == "Quick look")
    card = _chain_card(tmp_path, n=1)
    with app.test_client() as client:
        job_id = _post_import(
            client, card, tmp_path / "arch", quick_look_id,
        )
        result = wait_for_job_via_client(client, job_id)["result"]

    assert result["after_import_skipped"] == "import failed"
    assert "collection_id" not in result
    assert "process_job_id" not in result


def test_duplicates_only_import_skips_chaining(app_and_db, tmp_path):
    """Re-importing an already-archived card chains to 'no new photos',
    not an empty process run."""
    from wait import wait_for_job_via_client

    app, db = app_and_db
    quick_look_id = next(
        pr["id"] for pr in db.get_saved_processes() if pr["name"] == "Quick look")
    card = _chain_card(tmp_path)
    with app.test_client() as client:
        first = _post_import(client, card, tmp_path / "arch", None)
        wait_for_job_via_client(client, first)
        second = _post_import(client, card, tmp_path / "arch", quick_look_id)
        job = wait_for_job_via_client(client, second)
        res = job["result"]
        assert res["skipped_duplicate"] == 2
        assert res.get("after_import_skipped") == "no new photos"
        assert "process_job_id" not in res
        assert "collection_id" not in res


def test_chained_run_surfaces_model_warning(app_and_db, tmp_path):
    """cull_ready needs a classifier; with none downloaded the chained run
    still enqueues (auto-skipped) and the import result carries the same
    model_warning the manual pipeline route surfaces."""
    from wait import wait_for_job_via_client

    app, db = app_and_db
    cull_ready_id = next(
        pr["id"] for pr in db.get_saved_processes() if pr["name"] == "Cull-ready")
    card = _chain_card(tmp_path)
    with app.test_client() as client:
        job_id = _post_import(client, card, tmp_path / "arch", cull_ready_id)
        job = wait_for_job_via_client(client, job_id)
        res = job["result"]
        assert res.get("process_job_id"), res
        assert "model_warning" in res
        pj = client.get(f"/api/jobs/{res['process_job_id']}").get_json()
        assert pj["config"]["skip_classify"] is True  # auto-skip applied


def test_chained_process_snapshot_survives_mid_import_edit(
    app_and_db, tmp_path, monkeypatch,
):
    """A saved process edited AFTER the user submits the import must not
    change the chained run — the enqueue point captures the flag snapshot.

    An archive-copy import from a full card can take many minutes; if the
    resolve happens only when the chain hook fires, a settings edit in
    that window would silently swap in different toggles than the user
    accepted at the click. This test flips skip_regroup on the chosen
    process between submit and the chained job's config read, then asserts
    the chained run reflects the ORIGINAL flags.

    The fake active model keeps the no-model auto-skip from touching
    skip_classify (which would otherwise mask an unrelated flag flip),
    so skip_regroup (which the auto-skip never touches) cleanly proves
    the snapshot survived.
    """
    from wait import wait_for_job_via_client

    app, db = app_and_db
    # Snapshot the "Full" seed and then flip skip_regroup=True after the
    # import request returns. Any live-resolve would show skip_regroup=True
    # in the chained job config; the enqueue-time snapshot keeps it False.
    full_id = next(pr["id"] for pr in db.get_saved_processes()
                   if pr["name"] == "Full")
    original = db.get_saved_process(full_id)
    assert original["skip_regroup"] is False, original

    _fake_active_model(monkeypatch)
    card = _chain_card(tmp_path)
    with app.test_client() as client:
        job_id = _post_import(client, card, tmp_path / "arch", full_id)
        # Mutate the saved process BEFORE the chain hook fires. The import
        # still runs to completion, so this is the exact window the fix
        # closes: the chain hook fires from the import-job thread after
        # ingest, and without the snapshot it would call resolve_process
        # against this mutated row.
        db.update_saved_process(full_id, skip_regroup=True)

        job = wait_for_job_via_client(client, job_id)
        res = job["result"]
        assert res.get("process_job_id"), res

        pj = client.get(f"/api/jobs/{res['process_job_id']}").get_json()
        # The chained run should reflect the snapshot at enqueue time
        # (skip_regroup=False), not the post-submit edit.
        assert pj["config"]["skip_regroup"] is False, pj["config"]
        # And the process_id link is still preserved for provenance.
        assert pj["config"]["process_id"] == full_id
