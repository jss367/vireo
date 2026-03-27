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
