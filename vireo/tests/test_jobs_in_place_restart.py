"""The Jobs-page restart control and its authoritative HTTP admission."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest
from wait import wait_for_job_via_client


def test_jobs_page_in_place_restart_controls():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for JavaScript regression tests")
    result = subprocess.run(
        [node, str(Path(__file__).with_name("jobs_in_place_restart.cjs"))],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_restart_in_place_api_uses_saved_settings(app_and_db, tmp_path):
    app, db = app_and_db
    source = tmp_path / "photos"
    source.mkdir()
    config = {"sources": [str(source)], "recursive": False, "after_import": None, "tags": ["Birding"]}
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, config, workspace_id) "
        "VALUES ('old-job', 'import-in-place', 'failed', ?, ?)",
        (json.dumps(config), db.active_workspace_id),
    )
    db.commit()
    client = app.test_client()
    response = client.post("/api/jobs/import-in-place/old-job/restart", json={
        "sources": ["/untrusted/replacement"], "new_workspace_name": "Unwanted", "recursive": True,
    })
    assert response.status_code == 200, response.get_json()
    job_id = response.get_json()["job_id"]
    job = wait_for_job_via_client(client, job_id, wait_for_history=True)
    assert job["status"] == "completed", job
    assert job["config"]["sources"] == [str(source)]
    assert job["config"]["recursive"] is False
    assert job["config"]["tags"] == ["Birding"]
    assert not any(ws["name"] == "Unwanted" for ws in db.workspaces.list_all())
    response = client.post("/api/jobs/import-in-place/old-job/restart", json={})
    assert response.status_code == 409
    assert response.get_json()["restart_job_id"] == job_id
    assert client.post("/api/jobs/import-in-place/missing/restart", json={}).status_code == 404
