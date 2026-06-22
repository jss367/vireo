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


def test_models_status_tol_model_ready_without_labels(app_and_db, monkeypatch):
    """A downloaded Tree-of-Life model classifies label-free, so it's ready
    even with no species list."""
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "id": "bioclip-2", "name": "BioCLIP-2", "downloaded": True,
        "model_str": "hf-hub:imageomics/bioclip-2",
    })

    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/models/status")
    data = resp.get_json()
    assert data["needs_setup"] is False
    assert data["classification"]["ready"] is True
    assert data["classification"]["labels_ready"] is True
    assert data["classification"]["model_name"] == "BioCLIP-2"


def test_models_status_label_model_without_labels_needs_setup(app_and_db, monkeypatch):
    """The default ViT-B-16 model needs a species list to classify. Downloaded
    but with no labels, it is NOT ready — this is the fresh-install state that
    previously reported ready and then failed mid-pipeline."""
    import labels
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "id": "bioclip-vit-b-16", "name": "BioCLIP", "downloaded": True,
        "model_str": "ViT-B-16",
    })
    monkeypatch.setattr(labels, "get_active_labels", lambda: [])

    app, db = app_and_db
    db.set_workspace_active_labels([])  # no active labels for the workspace
    client = app.test_client()
    resp = client.get("/api/models/status")
    data = resp.get_json()
    assert data["needs_setup"] is True
    assert data["classification"]["ready"] is False
    assert data["classification"]["model_ready"] is True
    assert data["classification"]["labels_ready"] is False


def test_models_status_label_model_with_labels_ready(app_and_db, monkeypatch, tmp_path):
    """A label-needing model becomes ready once an active species list exists."""
    import labels
    import models
    monkeypatch.setattr(models, "get_active_model", lambda: {
        "id": "bioclip-vit-b-16", "name": "BioCLIP", "downloaded": True,
        "model_str": "ViT-B-16",
    })
    labels_file = tmp_path / "region.txt"
    labels_file.write_text("Cardinalis cardinalis\n")

    app, db = app_and_db
    db.set_workspace_active_labels([str(labels_file)])
    client = app.test_client()
    resp = client.get("/api/models/status")
    data = resp.get_json()
    assert data["needs_setup"] is False
    assert data["classification"]["ready"] is True
    assert data["classification"]["labels_ready"] is True


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


def test_verify_all_models_endpoint_returns_job_id(app_and_db):
    """POST /api/jobs/verify-all-models starts a background job and returns
    its job_id so the UI can track progress via the existing SSE stream."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/jobs/verify-all-models", json={})
    assert resp.status_code == 200
    data = resp.get_json()
    assert "job_id" in data


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
