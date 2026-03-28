import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_models_status_endpoint(app_and_db):
    """GET /api/models/status returns model readiness summary."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/models/status")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "classification" in data
    assert "ready" in data["classification"]
    assert "needs_setup" in data
    assert isinstance(data["needs_setup"], bool)


def test_models_status_no_models_needs_setup(app_and_db, monkeypatch):
    """When no classification model is downloaded, needs_setup is True."""
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: None)

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/models/status")
    data = resp.get_json()
    assert data["needs_setup"] is True
    assert data["classification"]["ready"] is False


def test_models_status_with_model_ready(app_and_db, monkeypatch):
    """When a classification model is downloaded, needs_setup is False."""
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "id": "bioclip-vit-b-16", "name": "BioCLIP", "downloaded": True
    })

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/models/status")
    data = resp.get_json()
    assert data["needs_setup"] is False
    assert data["classification"]["ready"] is True
    assert data["classification"]["model_name"] == "BioCLIP"


def test_index_redirects_to_welcome_when_no_model(app_and_db, monkeypatch):
    """GET / redirects to /welcome when no classification model is available."""
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: None)

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/")
    assert resp.status_code == 302
    assert "/welcome" in resp.headers["Location"]


def test_index_redirects_to_browse_when_model_ready(app_and_db, monkeypatch):
    """GET / redirects to /browse when a classification model is downloaded."""
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "id": "bioclip-vit-b-16", "name": "BioCLIP", "downloaded": True
    })

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/")
    assert resp.status_code == 302
    assert "/browse" in resp.headers["Location"]


def test_welcome_page_renders(app_and_db):
    """GET /welcome returns 200."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/welcome")
    assert resp.status_code == 200
    assert b"Vireo" in resp.data


def test_welcome_page_redirects_when_setup_done(app_and_db, monkeypatch):
    """GET /welcome without ?force redirects to /browse if models are ready."""
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "id": "bioclip-vit-b-16", "name": "BioCLIP", "downloaded": True
    })

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/welcome")
    assert resp.status_code == 302
    assert "/browse" in resp.headers["Location"]


def test_welcome_page_force_bypasses_redirect(app_and_db, monkeypatch):
    """GET /welcome?force=1 shows page even when models are ready."""
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "id": "bioclip-vit-b-16", "name": "BioCLIP", "downloaded": True
    })

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/welcome?force=1")
    assert resp.status_code == 200


def test_settings_page_has_setup_link(app_and_db):
    """Settings page includes a link to re-run the welcome setup."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/settings")
    assert resp.status_code == 200
    assert b"/welcome?force=1" in resp.data


def test_download_model_endpoint_exists(app_and_db):
    """POST /api/jobs/download-model returns 400 when model_id missing (not 404)."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/download-model", json={})
    assert resp.status_code == 400
    assert "model_id" in resp.get_json().get("error", "")


def test_pipeline_download_endpoint_exists(app_and_db):
    """POST /api/models/pipeline/download returns 400 when model_id missing (not 404)."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/models/pipeline/download", json={})
    assert resp.status_code == 400


def test_welcome_page_contains_download_button(app_and_db, monkeypatch):
    """Welcome page contains the download button."""
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: None)

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/welcome")
    assert resp.status_code == 200
    assert b"downloadBtn" in resp.data
    assert b"Download" in resp.data


def test_welcome_page_contains_skip_link(app_and_db, monkeypatch):
    """Welcome page contains skip link to /browse."""
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: None)

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/welcome")
    assert resp.status_code == 200
    assert b"/browse" in resp.data
    assert b"skip" in resp.data.lower() or b"Skip" in resp.data


def test_setup_complete_flag_default(tmp_path, monkeypatch):
    """setup_complete defaults to False."""
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    assert cfg.load().get("setup_complete") is False


def test_index_respects_setup_complete_flag(app_and_db, monkeypatch):
    """GET / goes to /browse when setup_complete is True, even without model."""
    import config as cfg
    import models

    monkeypatch.setattr(models, "get_active_model", lambda: None)
    cfg.set("setup_complete", True)

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/")
    assert resp.status_code == 302
    assert "/browse" in resp.headers["Location"]


def test_welcome_sets_setup_complete_on_skip(app_and_db, monkeypatch):
    """GET /welcome/complete sets the setup_complete flag."""
    import config as cfg

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/setup/complete")
    assert resp.status_code == 200
    assert cfg.load().get("setup_complete") is True
