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
        # argv shape: ["xdg-open", <parent_dir>]. xdg-open does not accept `--`;
        # the endpoint relies on os.path.abspath to guarantee a leading slash.
        assert args[0] == "xdg-open"
        assert args[1] == os.path.abspath(expected_parent)
        assert len(args) == 2


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


def test_reveal_folder_macos(app_and_db):
    """Passing {folder_id} reveals the folder itself on macOS (open -R <dir>)."""
    app, db = app_and_db
    folder = db.get_folder_tree()[0]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/files/reveal", json={"folder_id": folder["id"]})
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True
        args = run.call_args[0][0]
        # argv shape: ["open", "-R", "--", <folder path>]
        assert args[0] == "open"
        assert args[1] == "-R"
        assert args[2] == "--"
        assert args[3] == folder["path"]


def test_reveal_folder_linux_opens_folder(app_and_db):
    """On Linux, passing {folder_id} opens the folder itself with xdg-open."""
    app, db = app_and_db
    folder = db.get_folder_tree()[0]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "linux"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/files/reveal", json={"folder_id": folder["id"]})
        assert resp.status_code == 200
        args = run.call_args[0][0]
        # argv shape: ["xdg-open", <folder path>]. xdg-open does not accept `--`;
        # the endpoint relies on os.path.abspath to guarantee a leading slash.
        assert args[0] == "xdg-open"
        assert args[1] == os.path.abspath(folder["path"])
        assert len(args) == 2


def test_reveal_folder_windows_opens_folder(app_and_db):
    """On Windows, passing {folder_id} opens the folder itself in Explorer
    (no /select, since we want to show the folder's contents, not its parent).
    """
    app, db = app_and_db
    folder = db.get_folder_tree()[0]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "win32"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/files/reveal", json={"folder_id": folder["id"]})
        assert resp.status_code == 200
        args = run.call_args[0][0]
        assert args[0].lower() == "explorer"
        # No /select, for folder reveals — open the folder itself.
        assert not args[1].startswith("/select,")
        assert args[1] == folder["path"]


def test_reveal_unknown_folder_returns_404(app_and_db):
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/files/reveal", json={"folder_id": 999999})
        assert resp.status_code == 404


def test_reveal_invalid_folder_id_returns_400(app_and_db):
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/files/reveal", json={"folder_id": "abc"})
        assert resp.status_code == 400
        body = resp.get_json()
        assert "folder_id" in (body.get("error") or "").lower()


def test_reveal_requires_photo_or_folder_id(app_and_db):
    """With neither photo_id nor folder_id, return 400."""
    app, _ = app_and_db
    with app.test_client() as c:
        resp = c.post("/api/files/reveal", json={})
        assert resp.status_code == 400
