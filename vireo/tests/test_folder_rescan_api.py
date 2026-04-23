"""Tests for POST /api/folders/<id>/rescan — per-folder rescan endpoint.

The rescan endpoint queues a background scan job scoped to a single folder's
path. It re-uses the same job infrastructure as POST /api/jobs/scan.

Schema note: the job runner's job dict exposes `type` at the top level and
stores caller-supplied config (including `folder_id`) under `config`. Tests
read the folder id through `job["config"]["folder_id"]` to match the actual
schema produced by `JobRunner.start()`.
"""

import time


def _wait_for_job_listed(runner, job_id, timeout=2.0):
    """Wait until `list_jobs()` reports the given job id.

    The runner registers the job synchronously inside `start()`, so this
    should return immediately in practice; the poll loop is just a safety
    net against scheduler jitter on slow CI.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        jobs = runner.list_jobs()
        if any(j.get("id") == job_id for j in jobs):
            return jobs
        time.sleep(0.05)
    return runner.list_jobs()


def test_folder_rescan_queues_job(app_and_db, tmp_path):
    app, db = app_and_db
    # The fixture folders use fabricated paths; create a real on-disk
    # directory so the on-disk existence check passes.
    real_dir = tmp_path / "scan-me"
    real_dir.mkdir()
    folder_id = db.add_folder(str(real_dir), name="scan-me")

    with app.test_client() as c:
        resp = c.post(f"/api/folders/{folder_id}/rescan", json={})
        assert resp.status_code == 200, resp.get_json()
        body = resp.get_json()
        assert "job_id" in body
        assert body["job_id"].startswith("scan-")

    runner = app._job_runner
    jobs = _wait_for_job_listed(runner, body["job_id"])
    # The job is tagged as a scan and carries the folder id in its config.
    assert any(
        j.get("type") == "scan"
        and (j.get("config") or {}).get("folder_id") == folder_id
        for j in jobs
    ), jobs


def test_folder_rescan_unknown_folder(app_and_db):
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/folders/999999/rescan", json={})
        assert resp.status_code == 404


def test_folder_rescan_invalid_id_returns_404(app_and_db):
    """Routing restricts the id to <int:...>, so non-int paths should
    fall through to a 404 from Flask itself."""
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/folders/abc/rescan", json={})
        assert resp.status_code == 404


def test_folder_rescan_missing_path_returns_400(app_and_db, tmp_path, monkeypatch):
    app, db = app_and_db
    # Add a folder row whose on-disk path does not exist.
    fid = db.add_folder(str(tmp_path / "does-not-exist"), name="ghost")
    with app.test_client() as c:
        resp = c.post(f"/api/folders/{fid}/rescan", json={})
        assert resp.status_code == 400
        assert "no longer exists" in resp.get_json().get("error", "")


def test_folder_rescan_job_config_includes_folder_path(app_and_db, tmp_path):
    """The queued job's config carries the folder path so the work
    function can target the right directory."""
    app, db = app_and_db
    # Use a real on-disk directory so the existence check passes.
    real_dir = tmp_path / "scan-config"
    real_dir.mkdir()
    folder_path = str(real_dir)
    folder_id = db.add_folder(folder_path, name="scan-config")

    with app.test_client() as c:
        resp = c.post(f"/api/folders/{folder_id}/rescan", json={})
        assert resp.status_code == 200
        job_id = resp.get_json()["job_id"]

    runner = app._job_runner
    jobs = _wait_for_job_listed(runner, job_id)
    job = next(j for j in jobs if j.get("id") == job_id)
    cfg = job.get("config") or {}
    assert cfg.get("folder_id") == folder_id
    assert cfg.get("root") == folder_path
