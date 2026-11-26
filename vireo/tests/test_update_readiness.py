"""The restart check the desktop shell polls before installing an update."""

from services.update_readiness import job_name, restart_readiness


def _job(job_id, job_type, status, origin="user"):
    return {"id": job_id, "type": job_type, "status": status, "origin": origin}


def test_no_jobs_is_idle():
    assert restart_readiness([]) == {"idle": True, "blocking": [], "paused": []}


def test_running_queued_and_pausing_user_jobs_block():
    result = restart_readiness([
        _job("a", "import-full", "running"),
        _job("b", "pipeline", "queued"),
        _job("c", "move-folder", "pausing"),
    ])
    assert result["idle"] is False
    assert [(j["id"], j["name"], j["status"]) for j in result["blocking"]] == [
        ("a", "Import", "running"),
        ("b", "Process run", "queued"),
        ("c", "Move folder", "pausing"),
    ]


def test_system_jobs_never_block():
    # Maintenance and the periodic checks start often; waiting for a moment
    # with none running could hold the update forever.
    result = restart_readiness([
        _job("m", "preview-cache-maintenance", "running", origin="system"),
        _job("n", "missing-originals", "queued", origin="system"),
        _job("p", "pipeline", "paused", origin="system"),
    ])
    assert result == {"idle": True, "blocking": [], "paused": []}


def test_paused_user_job_is_listed_but_does_not_block():
    result = restart_readiness([_job("p", "pipeline", "paused")])
    assert result["idle"] is True
    assert result["blocking"] == []
    assert [j["name"] for j in result["paused"]] == ["Process run"]


def test_finished_jobs_are_ignored():
    result = restart_readiness([
        _job("a", "import", "completed"),
        _job("b", "pipeline", "failed"),
        _job("c", "scan", "cancelled"),
    ])
    assert result == {"idle": True, "blocking": [], "paused": []}


def test_job_without_origin_counts_as_the_users():
    result = restart_readiness([{"id": "x", "type": "scan", "status": "running"}])
    assert result["idle"] is False
    assert result["blocking"][0]["name"] == "Scan"


def test_job_names():
    assert job_name("pipeline") == "Process run"
    assert job_name("import-in-place") == "Import"
    assert job_name("send_to_nas") == "Send to NAS"
    assert job_name("") == "Job"


def test_endpoint_reports_the_runner_jobs(app_and_db, monkeypatch):
    app, _ = app_and_db
    monkeypatch.setattr(
        app._job_runner, "list_jobs",
        lambda: [_job("a", "import-full", "running"), _job("s", "scan", "running", "system")],
    )
    resp = app.test_client().get(
        "/api/v1/update-readiness", headers={"X-Vireo-Token": app.config["API_TOKEN"]},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["idle"] is False
    assert [j["id"] for j in body["blocking"]] == ["a"]


def test_endpoint_requires_the_token(app_and_db):
    app, _ = app_and_db
    assert app.test_client().get("/api/v1/update-readiness").status_code == 401
