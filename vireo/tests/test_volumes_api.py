"""Tests for GET /api/volumes — cross-platform mounted-volume enumeration.

These run on any host (including Linux CI): platform detection and the
OS-specific syscalls are mocked, so each branch is exercised everywhere.
"""

import os
import threading
import time
from unittest.mock import MagicMock, patch


def test_volumes_macos_scans_volumes_dir(app_and_db):
    app, _ = app_and_db
    # Use os.path.join so the predicate matches whatever separator the
    # host produces (forward slash on POSIX, backslash on Windows).
    sd = os.path.join("/Volumes", "SD_CARD")
    backup = os.path.join("/Volumes", "Backup")

    def _isdir(p):
        return p in ("/Volumes", sd, backup)

    with app.test_client() as c, \
         patch("platform.system", return_value="Darwin"), \
         patch("vireo.app.os.path.isdir", side_effect=_isdir), \
         patch("vireo.app.os.listdir", return_value=["SD_CARD", "Backup"]):
        resp = c.get("/api/volumes")
        assert resp.status_code == 200
        paths = {v["path"] for v in resp.get_json()}
        assert paths == {sd, backup}


def test_volumes_linux_scans_media_mounts(app_and_db):
    app, _ = app_and_db
    sd = os.path.join("/media", "SD_CARD")

    def _isdir(p):
        return p in ("/media", sd)

    def _listdir(p):
        return ["SD_CARD"] if p == "/media" else []

    with app.test_client() as c, \
         patch("platform.system", return_value="Linux"), \
         patch("vireo.app.os.path.isdir", side_effect=_isdir), \
         patch("vireo.app.os.listdir", side_effect=_listdir):
        resp = c.get("/api/volumes")
        assert resp.status_code == 200
        paths = {v["path"] for v in resp.get_json()}
        assert paths == {sd}


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


def test_volumes_windows_serializes_set_error_mode(app_and_db):
    """Concurrent /api/volumes requests must not interleave SetErrorMode
    save/restore. The call must be ``(save, restore, save, restore)`` —
    never ``(save, save, restore, restore)`` — otherwise one request
    saves the other's temporary mode and restores the wrong value at the
    end, leaving the process in the wrong error-dialog state.
    """
    app, _ = app_and_db

    SEM_FAILCRITICALERRORS = 0x0001
    BASE_MODE = 0x0000
    events = []
    events_lock = threading.Lock()
    inside_critical = threading.Event()
    release = threading.Event()
    first_call = threading.Event()

    def _set_error_mode(new_mode):
        with events_lock:
            events.append(("set", new_mode))
            is_first = not first_call.is_set()
            if is_first:
                first_call.set()
        if is_first:
            # Hold the first thread inside the critical section so the
            # second thread has a chance to race in without the lock.
            inside_critical.set()
            release.wait(timeout=2.0)
        # Real SetErrorMode returns the previous mode. With the lock,
        # the only previous mode any caller observes is BASE_MODE.
        return BASE_MODE

    kernel32 = MagicMock()
    kernel32.GetLogicalDrives.return_value = 1 << 2  # just C:
    kernel32.GetVolumeInformationW.return_value = 0
    kernel32.SetErrorMode.side_effect = _set_error_mode
    windll = MagicMock()
    windll.kernel32 = kernel32

    def _isdir(p):
        return p == "C:\\"

    results = []

    def _hit():
        with app.test_client() as c:
            results.append(c.get("/api/volumes").status_code)

    with patch("platform.system", return_value="Windows"), \
         patch("ctypes.windll", windll, create=True), \
         patch("vireo.app.os.path.isdir", side_effect=_isdir):
        t1 = threading.Thread(target=_hit)
        t2 = threading.Thread(target=_hit)
        t1.start()
        # Wait until thread 1 is inside the protected section before
        # starting thread 2 — without the lock this is the interleaving
        # window where thread 2 would observe thread 1's temporary mode.
        assert inside_critical.wait(timeout=2.0)
        t2.start()
        # Give thread 2 a real chance to reach the critical section, then
        # assert no second save has happened *before* releasing thread 1.
        # This is what actually proves serialization: WITH the lock, thread 2
        # blocks on lock acquisition (before its save), so only thread 1's
        # save is recorded; WITHOUT the lock, thread 2 races straight through
        # and records its save here. The previous version released thread 1
        # immediately, so the expected (save, restore, save, restore) order
        # could appear even with no lock — letting a lockless regression pass.
        time.sleep(0.2)
        with events_lock:
            before_release = list(events)
        assert before_release == [("set", SEM_FAILCRITICALERRORS)], (
            "a second SetErrorMode ran before thread 1 released its lock — "
            f"the critical section is not serialized: {before_release!r}"
        )
        release.set()
        t1.join(timeout=2.0)
        t2.join(timeout=2.0)

    assert results == [200, 200]
    # Four calls total: two save-restore pairs. With the lock, the calls
    # are strictly ordered (set, restore, set, restore) — not (set, set,
    # restore, restore) which would happen if both threads raced through
    # the block. Both restores see BASE_MODE because the lock prevents
    # thread 2 from observing thread 1's temporary mode.
    assert [kind for kind, _ in events] == ["set", "set", "set", "set"]
    new_modes = [m for _, m in events]
    assert new_modes[0] == SEM_FAILCRITICALERRORS  # thread 1 save
    assert new_modes[1] == BASE_MODE               # thread 1 restore
    assert new_modes[2] == SEM_FAILCRITICALERRORS  # thread 2 save
    assert new_modes[3] == BASE_MODE               # thread 2 restore


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
