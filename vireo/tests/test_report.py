"""Tests for the issue-reporting endpoint."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from db import Database
from PIL import Image


@pytest.fixture
def app_and_db(tmp_path, monkeypatch):
    """Create a test app with sample data and isolated config."""
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    from app import create_app

    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos/2024", name="2024")

    p1 = db.add_photo(
        folder_id=fid, filename="bird1.jpg", extension=".jpg",
        file_size=1000, file_mtime=1.0, timestamp="2024-01-15T10:00:00",
    )
    Image.new("RGB", (100, 100)).save(os.path.join(thumb_dir, f"{p1}.jpg"))

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    app.config["TESTING"] = True
    return app, db


@pytest.fixture
def client(app_and_db):
    app, _ = app_and_db
    with app.test_client() as c:
        yield c


class TestReportIssue:
    def test_missing_description_returns_400(self, client):
        resp = client.post("/api/report-issue", json={})
        assert resp.status_code == 400
        assert "description" in resp.get_json()["error"].lower()

    def test_empty_description_returns_400(self, client):
        resp = client.post("/api/report-issue", json={"description": "  "})
        assert resp.status_code == 400

    def test_report_without_url_returns_download(self, client):
        resp = client.post("/api/report-issue", json={"description": "Something broke"})
        data = resp.get_json()
        assert data["status"] == "download"
        diag = data["diagnostics"]
        assert diag["description"] == "Something broke"
        assert "vireo_version" in diag
        assert "system" in diag
        assert "logs" in diag
        assert "app_state" in diag
        assert "recent_jobs" in diag
        assert "config" in diag
        assert "timestamp" in diag

    def test_diagnostics_system_fields(self, client):
        """System info should include platform, python, architecture."""
        resp = client.post("/api/report-issue", json={"description": "check system"})
        system = resp.get_json()["diagnostics"]["system"]
        assert "platform" in system
        assert "python" in system
        assert "architecture" in system

    def test_sensitive_config_values_are_redacted(self, client):
        """Tokens and keys must not leak in reports."""
        import config as cfg

        current = cfg.load()
        current["hf_token"] = "hf_abc123secret"
        current["inat_token"] = "my-inat-token"
        cfg.save(current)

        resp = client.post("/api/report-issue", json={"description": "test redaction"})
        data = resp.get_json()
        config_in_report = data["diagnostics"]["config"]
        assert config_in_report.get("hf_token") == "[REDACTED]"
        assert config_in_report.get("inat_token") == "[REDACTED]"
        # Non-sensitive values should not be redacted
        assert config_in_report.get("classification_threshold") == current["classification_threshold"]

    def test_report_with_bad_url_returns_download_fallback(self, client):
        """If the report URL is unreachable, fall back to download."""
        import config as cfg

        current = cfg.load()
        current["report_url"] = "http://localhost:1/nonexistent"
        cfg.save(current)

        resp = client.post("/api/report-issue", json={"description": "test fallback"})
        data = resp.get_json()
        assert data["status"] == "download"
        assert "diagnostics" in data
