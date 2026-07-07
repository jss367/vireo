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
    # The Linux photo-reveal path verifies the photo file exists before
    # opening its parent — the sample DB uses synthetic paths, so pretend
    # the file is on disk so subprocess still gets called.
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "linux"), \
         patch("vireo.app.os.path.isfile", return_value=True), \
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


def test_reveal_linux_missing_photo_reports_error(app_and_db):
    """On Linux, photo reveals target the parent directory, so a stale
    catalog entry whose parent still exists would silently succeed. The
    endpoint should check the photo path first and report a real failure.
    """
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "linux"), \
         patch("vireo.app.os.path.isfile", return_value=False), \
         patch("vireo.app.subprocess.run") as run:
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is False
        assert body.get("reason")
        # No xdg-open call — we bail out before invoking the file manager
        # so the user doesn't get a "revealed" toast for a missing file.
        run.assert_not_called()


def test_reveal_windows_ignores_nonzero_exit(app_and_db):
    """explorer.exe /select can return exit 1 even when it opens File
    Explorer successfully. Reporting that as a failure produces bogus
    "reveal failed" toasts for reveals that actually worked, so the
    endpoint deliberately ignores explorer.exe's returncode on Windows.
    """
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "win32"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=1, stdout="", stderr="")
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 200
        assert resp.get_json()["ok"] is True


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


def test_reveal_nonzero_exit_reports_reason(app_and_db):
    app, db = app_and_db
    pid = db.get_photos()[0]["id"]
    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="The file does not exist.",
        )
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is False
        assert body["reason"] == "The file does not exist."


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


def test_reveal_photo_outside_active_workspace_returns_404(app_and_db):
    """A photo whose folder is not linked to the active workspace must 404.

    Without this gate, a caller could reveal absolute file paths for photos
    hidden from the current workspace by guessing photo IDs.
    """
    app, db = app_and_db
    default_ws = db._active_workspace_id
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    other_fid = db.add_folder('/secret/ws-photos', name='secret')
    pid = db.add_photo(
        folder_id=other_fid, filename='hidden.jpg', extension='.jpg',
        file_size=10, file_mtime=1.0, timestamp='2024-01-01T00:00:00',
    )
    db.set_active_workspace(default_ws)
    with app.test_client() as c:
        resp = c.post("/api/files/reveal", json={"photo_id": pid})
        assert resp.status_code == 404


def test_reveal_folder_outside_active_workspace_returns_404(app_and_db):
    """A folder not linked to the active workspace must 404."""
    app, db = app_and_db
    default_ws = db._active_workspace_id
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    other_fid = db.add_folder('/secret/ws-dir', name='secret')
    db.set_active_workspace(default_ws)
    with app.test_client() as c:
        resp = c.post("/api/files/reveal", json={"folder_id": other_fid})
        assert resp.status_code == 404
