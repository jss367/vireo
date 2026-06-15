"""Tests for GET /api/volumes — cross-platform mounted-volume enumeration.

These run on any host (including Linux CI): platform detection and the
OS-specific syscalls are mocked, so each branch is exercised everywhere.
"""

from unittest.mock import MagicMock, patch


def test_volumes_macos_scans_volumes_dir(app_and_db):
    app, _ = app_and_db

    def _isdir(p):
        return p in ("/Volumes", "/Volumes/SD_CARD", "/Volumes/Backup")

    with app.test_client() as c, \
         patch("platform.system", return_value="Darwin"), \
         patch("vireo.app.os.path.isdir", side_effect=_isdir), \
         patch("vireo.app.os.listdir", return_value=["SD_CARD", "Backup"]):
        resp = c.get("/api/volumes")
        assert resp.status_code == 200
        paths = {v["path"] for v in resp.get_json()}
        assert paths == {"/Volumes/SD_CARD", "/Volumes/Backup"}


def test_volumes_linux_scans_media_mounts(app_and_db):
    app, _ = app_and_db

    def _isdir(p):
        return p in ("/media", "/media/SD_CARD")

    def _listdir(p):
        return ["SD_CARD"] if p == "/media" else []

    with app.test_client() as c, \
         patch("platform.system", return_value="Linux"), \
         patch("vireo.app.os.path.isdir", side_effect=_isdir), \
         patch("vireo.app.os.listdir", side_effect=_listdir):
        resp = c.get("/api/volumes")
        assert resp.status_code == 200
        paths = {v["path"] for v in resp.get_json()}
        assert paths == {"/media/SD_CARD"}


def test_volumes_windows_enumerates_drive_letters(app_and_db):
    """C: and D: are present; D: carries a friendly label, C: does not."""
    app, _ = app_and_db

    def _get_vol_info(root_p, buf, buflen, *rest):
        root = root_p.value if hasattr(root_p, "value") else root_p
        labels = {"D:\\": "SD_CARD"}
        if root in labels:
            buf.value = labels[root]
            return 1
        return 0

    kernel32 = MagicMock()
    # Bitmask: bit 2 -> C:, bit 3 -> D:
    kernel32.GetLogicalDrives.return_value = (1 << 2) | (1 << 3)
    kernel32.GetVolumeInformationW.side_effect = _get_vol_info
    kernel32.SetErrorMode.return_value = 0
    windll = MagicMock()
    windll.kernel32 = kernel32

    def _isdir(p):
        # Both drives are ready; everything else (incl. the empty A:/B:) is not.
        return p in ("C:\\", "D:\\")

    with app.test_client() as c, \
         patch("platform.system", return_value="Windows"), \
         patch("ctypes.windll", windll, create=True), \
         patch("vireo.app.os.path.isdir", side_effect=_isdir):
        resp = c.get("/api/volumes")
        assert resp.status_code == 200
        volumes = resp.get_json()
        by_path = {v["path"]: v["name"] for v in volumes}
        assert by_path == {"C:\\": "C:", "D:\\": "SD_CARD (D:)"}
        # The error-mode dialog suppression is restored after probing.
        assert kernel32.SetErrorMode.call_count == 2


def test_volumes_windows_skips_not_ready_drives(app_and_db):
    """A reported drive letter with no media inserted is not listed."""
    app, _ = app_and_db

    kernel32 = MagicMock()
    # Report A: (bit 0) and C: (bit 2) as existing drive letters.
    kernel32.GetLogicalDrives.return_value = (1 << 0) | (1 << 2)
    kernel32.GetVolumeInformationW.return_value = 0  # no label
    kernel32.SetErrorMode.return_value = 0
    windll = MagicMock()
    windll.kernel32 = kernel32

    def _isdir(p):
        return p == "C:\\"  # A:\ is an empty card reader, not ready

    with app.test_client() as c, \
         patch("platform.system", return_value="Windows"), \
         patch("ctypes.windll", windll, create=True), \
         patch("vireo.app.os.path.isdir", side_effect=_isdir):
        resp = c.get("/api/volumes")
        assert resp.status_code == 200
        paths = {v["path"] for v in resp.get_json()}
        assert paths == {"C:\\"}
