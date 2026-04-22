"""Tests for POST /api/files/reveal — cross-platform reveal-in-file-manager."""

import os
from unittest.mock import MagicMock, patch


def _expected_full_path(db, pid):
    """Resolve the on-disk path the endpoint will build for a given photo id."""
    photo = db.get_photo(pid)
    folder_row = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()
    return os.path.join(folder_row["path"], photo["filename"])


def test_reveal_macos(app_and_db):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    expected_path = _expected_full_path(db, pid)
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True
        args = run.call_args[0][0]
        # argv shape: ["open", "-R", "--", <path>]
        assert args[0] == "open"
        assert args[1] == "-R"
        assert args[2] == "--"
        assert args[3] == expected_path


def test_reveal_linux_opens_parent(app_and_db):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    expected_parent = os.path.dirname(_expected_full_path(db, pid))
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "linux"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 200
        args = run.call_args[0][0]
        # argv shape: ["xdg-open", "--", <parent_dir>]
        assert args[0] == "xdg-open"
        assert args[1] == "--"
        assert args[2] == expected_parent


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


def test_reveal_invalid_photo_id_returns_400(app_and_db):
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/files/reveal", json={"photo_id": "abc"})
        assert resp.status_code == 400
        body = resp.get_json()
        assert "photo_id" in (body.get("error") or "").lower()
