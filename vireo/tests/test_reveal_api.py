"""Tests for POST /api/files/reveal — cross-platform reveal-in-file-manager."""

from unittest.mock import MagicMock, patch


def test_reveal_macos(app_and_db):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True
        args = run.call_args[0][0]
        assert args[0] == "open"
        assert args[1] == "-R"


def test_reveal_linux_opens_parent(app_and_db):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "linux"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 200
        args = run.call_args[0][0]
        assert args[0] == "xdg-open"
        # argv[1] is the parent dir, not the file itself
        assert not args[1].endswith(".jpg")


def test_reveal_windows_select(app_and_db):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "win32"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 200
        args = run.call_args[0][0]
        assert args[0].lower() == "explorer"
        assert args[1].startswith("/select,")


def test_reveal_unknown_photo_returns_error(app_and_db):
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/files/reveal", json={"photo_id": 999999})
        assert resp.status_code == 404


def test_reveal_shell_failure_reports_reason(app_and_db):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run") as run:
        run.side_effect = FileNotFoundError("no 'open'")
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is False
        assert "reason" in body
