"""Tests for remote (SSH) folder moves and the remote-target plumbing.

The transport/verify/existence touchpoints shell out to ssh/rsync, so the
move_folder tests here monkeypatch those three seams and exercise the
branching, catalog-repoint, and verify-gated delete logic without a network.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import config as cfg
import move as move_mod
import pytest
from db import Database

# --------------------------------------------------------------------------
# Pure helpers — no SSH/rsync needed.
# --------------------------------------------------------------------------

def test_sanitize_subpath_basic():
    assert move_mod.sanitize_subpath("") == ""
    assert move_mod.sanitize_subpath(None) == ""
    assert move_mod.sanitize_subpath("USA/2026/") == "USA/2026"
    assert move_mod.sanitize_subpath("USA//2026") == "USA/2026"
    assert move_mod.sanitize_subpath("a/./b") == "a/b"


def test_sanitize_subpath_rejects_traversal():
    with pytest.raises(ValueError):
        move_mod.sanitize_subpath("../escape")
    with pytest.raises(ValueError):
        move_mod.sanitize_subpath("a/../../b")


def test_sanitize_subpath_rejects_absolute():
    # An absolute input must not be silently normalized into a relative segment:
    # '/foo' previously slipped through as 'foo' and landed under a different
    # base than the user typed. Reject POSIX, Windows-backslash, and drive
    # forms so the contract matches the docstring.
    with pytest.raises(ValueError):
        move_mod.sanitize_subpath("/USA/2026")
    with pytest.raises(ValueError):
        move_mod.sanitize_subpath("\\\\server\\share")
    with pytest.raises(ValueError):
        move_mod.sanitize_subpath("C:\\foo")


def test_build_remote_move_spec_joins_both_bases():
    target = {
        "host": "nas", "user": "me", "port": 22, "ssh_key": "",
        "remote_path": "/volume1/Photography",
        "mount_path": "/Volumes/Photography", "bwlimit_kbps": 0,
    }
    spec = move_mod.build_remote_move_spec(target, "USA/2026", "/usr/bin/rsync")
    assert spec["ssh_dest_base"] == "/volume1/Photography/USA/2026"
    assert spec["mount_dest_base"] == os.path.join(
        "/Volumes/Photography", "USA", "2026")
    assert spec["rsync_bin"] == "/usr/bin/rsync"
    assert spec["host"] == "nas" and spec["user"] == "me"


def test_build_remote_move_spec_no_subpath():
    target = {"host": "nas", "user": "me", "remote_path": "/volume1/Photo",
              "mount_path": "/Volumes/Photo"}
    spec = move_mod.build_remote_move_spec(target, "", "rsync")
    assert spec["ssh_dest_base"] == "/volume1/Photo"
    assert spec["mount_dest_base"] == "/Volumes/Photo"


def test_build_remote_move_spec_bad_subpath_raises():
    target = {"host": "nas", "user": "me", "remote_path": "/v", "mount_path": "/m"}
    with pytest.raises(ValueError):
        move_mod.build_remote_move_spec(target, "../x", "rsync")


# Windows has no execute bit; executability is defined by extension (PATHEXT).
_EXE_SUFFIX = ".exe" if sys.platform == "win32" else ""


def test_resolve_rsync_bin(tmp_path, monkeypatch):
    monkeypatch.setattr(move_mod, "_BUNDLED_RSYNC_CANDIDATES", ())
    monkeypatch.setattr(move_mod, "_platform_rsync_candidates", lambda: ())
    monkeypatch.delenv("VIREO_RSYNC_BIN", raising=False)
    # Nothing available -> None.
    assert move_mod.resolve_rsync_bin("") is None
    # An explicit executable path wins.
    fake = tmp_path / f"rsync{_EXE_SUFFIX}"
    fake.write_text("#!/bin/sh\n")
    fake.chmod(0o755)
    assert move_mod.resolve_rsync_bin(str(fake)) == str(fake)
    # A configured non-executable path is ignored.
    plain = tmp_path / "notexec"
    plain.write_text("x")
    plain.chmod(0o644)
    assert move_mod.resolve_rsync_bin(str(plain)) is None


def test_resolve_rsync_bin_env_override(tmp_path, monkeypatch):
    monkeypatch.setattr(move_mod, "_BUNDLED_RSYNC_CANDIDATES", ())
    monkeypatch.setattr(move_mod, "_platform_rsync_candidates", lambda: ())
    fake = tmp_path / f"rsync{_EXE_SUFFIX}"
    fake.write_text("#!/bin/sh\n")
    fake.chmod(0o755)
    monkeypatch.setenv("VIREO_RSYNC_BIN", str(fake))
    assert move_mod.resolve_rsync_bin("") == str(fake)


def test_resolve_rsync_bin_finds_linux_path_via_platform_candidates(
        tmp_path, monkeypatch):
    """On a Linux host with no config and no bundled rsync, resolve_rsync_bin
    should still find a usable GNU rsync via the platform-aware candidates
    ($PATH / /usr/bin/rsync). macOS-only candidate avoidance is intentionally
    macOS-only — every other major OS ships GNU rsync at those locations."""
    monkeypatch.setattr(move_mod, "_BUNDLED_RSYNC_CANDIDATES", ())
    monkeypatch.delenv("VIREO_RSYNC_BIN", raising=False)

    # Pretend a usable rsync is discoverable on $PATH at tmp_path/rsync.
    # Use _EXE_SUFFIX so the Windows leg of the CI matrix (which judges
    # executability by PATHEXT) treats this file as executable.
    fake = tmp_path / f"rsync{_EXE_SUFFIX}"
    fake.write_text("#!/bin/sh\n")
    fake.chmod(0o755)

    monkeypatch.setattr(
        move_mod, "_platform_rsync_candidates",
        lambda: (str(fake),))

    assert move_mod.resolve_rsync_bin("") == str(fake)


def test_platform_rsync_candidates_excludes_darwin(monkeypatch):
    """macOS's /usr/bin/rsync is openrsync (can't drive SSH), so on darwin the
    platform-aware probe must return empty — the bundled Homebrew/MacPorts
    candidates handle macOS, and including /usr/bin/rsync would silently
    auto-select an unusable binary."""
    monkeypatch.setattr(move_mod.sys, "platform", "darwin")
    assert move_mod._platform_rsync_candidates() == ()


def test_platform_rsync_candidates_includes_usr_bin_on_linux(monkeypatch):
    """On Linux/BSD/Windows, /usr/bin/rsync is GNU rsync. The probe must offer
    it as a candidate so an unconfigured Linux install has a usable default."""
    monkeypatch.setattr(move_mod.sys, "platform", "linux")
    monkeypatch.setattr(move_mod.shutil, "which", lambda name: None)
    assert "/usr/bin/rsync" in move_mod._platform_rsync_candidates()


def test_coerce_remote_target_drops_incomplete():
    assert cfg._coerce_remote_target({"host": "h", "user": "u"}) is None
    assert cfg._coerce_remote_target({"host": "h", "remote_path": "/p"}) is None
    assert cfg._coerce_remote_target("not a dict") is None


def test_coerce_remote_target_coerces_and_defaults():
    t = cfg._coerce_remote_target({
        "host": "nas", "user": "me", "remote_path": "/volume1/Photo",
        "port": "2222", "bwlimit_kbps": "2048",
    })
    assert t["port"] == 2222
    assert t["bwlimit_kbps"] == 2048
    assert t["name"] == "me@nas"  # default name
    assert t["id"]  # an id is always assigned
    assert t["mount_path"] == ""


# --------------------------------------------------------------------------
# Remote move_folder — SSH seams monkeypatched.
# --------------------------------------------------------------------------

@pytest.fixture
def remote_env(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    src = tmp_path / "src"
    src.mkdir()
    (src / "a.nef").write_bytes(b"\x00" * 50)
    (src / "b.nef").write_bytes(b"\x00" * 60)
    fid = db.add_folder(str(src), name="trip")
    db.add_photo(folder_id=fid, filename="a.nef", extension=".nef",
                 file_size=50, file_mtime=1.0)
    db.add_photo(folder_id=fid, filename="b.nef", extension=".nef",
                 file_size=60, file_mtime=2.0)

    remote = {
        "host": "nas", "user": "me", "port": 22, "ssh_key": "",
        "bwlimit_kbps": 0, "rsync_bin": "/usr/bin/rsync",
        "ssh_dest_base": "/volume1/Photography",
        "mount_dest_base": str(tmp_path / "mount"),
    }
    return {"db": db, "src": src, "fid": fid, "remote": remote,
            "tmp_path": tmp_path}


def test_remote_move_success_repoints_catalog_to_mount(remote_env, monkeypatch):
    """A verified remote move deletes originals and repoints the folder at the
    LOCAL MOUNT path (not the NAS path), so photos stay in the library."""
    env = remote_env
    calls = {}

    def fake_rsync(src_path, dest_spec, flags, total, cb, rsync_bin="rsync",
                   extra_args=None, **kw):
        calls["dest_spec"] = dest_spec
        calls["rsync_bin"] = rsync_bin
        calls["extra_args"] = list(extra_args or [])
        return (0, "", False)

    monkeypatch.setattr(move_mod, "_remote_dir_exists", lambda r, p: False)
    monkeypatch.setattr(move_mod, "_run_rsync_streamed", fake_rsync)
    monkeypatch.setattr(move_mod, "_remote_verify_complete",
                        lambda *a, **k: None)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid"], destination="",
        remote=env["remote"])

    assert result["moved"] == 2
    assert result["errors"] == []
    # rsync addressed the SSH target, with the bundled binary + ssh transport.
    assert calls["dest_spec"] == "me@nas:/volume1/Photography/trip"
    assert calls["rsync_bin"] == "/usr/bin/rsync"
    # Partials go to a dot-prefixed subdir so they don't collide with
    # --ignore-existing on retry; --partial-dir implies --partial.
    assert "-e" in calls["extra_args"]
    assert "--partial-dir=.rsync-partial" in calls["extra_args"]
    # Originals deleted.
    assert not env["src"].exists()
    # Catalog now points at the LOCAL MOUNT path, not the NAS path.
    row = env["db"].conn.execute(
        "SELECT path FROM folders WHERE id = ?", (env["fid"],)).fetchone()
    assert row["path"] == os.path.join(str(env["tmp_path"] / "mount"), "trip")


def test_remote_move_verify_failure_preserves_originals(remote_env, monkeypatch):
    """If the remote --checksum verify reports a missing/differing file, the
    move must NOT delete the originals."""
    env = remote_env
    monkeypatch.setattr(move_mod, "_remote_dir_exists", lambda r, p: False)
    monkeypatch.setattr(move_mod, "_run_rsync_streamed",
                        lambda *a, **k: (0, "", False))
    monkeypatch.setattr(move_mod, "_remote_verify_complete",
                        lambda *a, **k: ("b.nef", None))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid"], destination="",
        remote=env["remote"])

    assert result["moved"] == 0
    assert any("b.nef" in e for e in result["errors"])
    # Originals intact; catalog unchanged.
    assert (env["src"] / "a.nef").exists()
    row = env["db"].conn.execute(
        "SELECT path FROM folders WHERE id = ?", (env["fid"],)).fetchone()
    assert row["path"] == str(env["src"])


def test_remote_move_verify_error_preserves_originals(remote_env, monkeypatch):
    """A verification that couldn't run (SSH/rsync error) is treated as a
    failure — originals preserved, not deleted on an unconfirmed copy."""
    env = remote_env
    monkeypatch.setattr(move_mod, "_remote_dir_exists", lambda r, p: False)
    monkeypatch.setattr(move_mod, "_run_rsync_streamed",
                        lambda *a, **k: (0, "", False))
    monkeypatch.setattr(move_mod, "_remote_verify_complete",
                        lambda *a, **k: ("__ERROR__", "connection reset"))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid"], destination="",
        remote=env["remote"])

    assert result["moved"] == 0
    assert (env["src"] / "a.nef").exists()


def test_remote_move_missing_rsync_bin_errors(remote_env, monkeypatch):
    """No resolved GNU rsync -> clear error, nothing transferred or deleted."""
    env = remote_env
    env["remote"]["rsync_bin"] = None
    monkeypatch.setattr(move_mod, "_remote_dir_exists", lambda r, p: False)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid"], destination="",
        remote=env["remote"])

    assert result["moved"] == 0
    assert any("rsync" in e.lower() for e in result["errors"])
    assert (env["src"] / "a.nef").exists()


def test_remote_move_existing_dest_without_merge_needs_merge(remote_env, monkeypatch):
    """An existing remote destination without merge returns needs_merge, like
    the local path, so the UI can prompt to merge/resume."""
    env = remote_env
    monkeypatch.setattr(move_mod, "_remote_dir_exists", lambda r, p: True)
    monkeypatch.setattr(move_mod, "_run_rsync_streamed",
                        lambda *a, **k: (0, "", False))

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid"], destination="", merge=False,
        remote=env["remote"])

    assert result.get("needs_merge") is True
    assert result["moved"] == 0
    assert (env["src"] / "a.nef").exists()


def test_remote_move_merge_uses_ignore_existing(remote_env, monkeypatch):
    """A merge into an existing remote dest passes --ignore-existing to rsync."""
    env = remote_env
    seen = {}

    def fake_rsync(src_path, dest_spec, flags, total, cb, rsync_bin="rsync",
                   extra_args=None, **kw):
        seen["flags"] = list(flags or [])
        return (0, "", False)

    monkeypatch.setattr(move_mod, "_remote_dir_exists", lambda r, p: True)
    monkeypatch.setattr(move_mod, "_run_rsync_streamed", fake_rsync)
    monkeypatch.setattr(move_mod, "_remote_verify_complete",
                        lambda *a, **k: None)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid"], destination="", merge=True,
        remote=env["remote"])

    assert result["moved"] == 2
    assert "--ignore-existing" in seen["flags"]


def test_remote_move_merge_uses_partial_dir_for_resume(remote_env, monkeypatch):
    """A merge/resume must use --partial-dir, not bare --partial: otherwise a
    stalled file would be left at the destination filename and skipped by
    --ignore-existing on every retry, stranding it short of complete forever."""
    env = remote_env
    seen = {}

    def fake_rsync(src_path, dest_spec, flags, total, cb, rsync_bin="rsync",
                   extra_args=None, **kw):
        seen["extra_args"] = list(extra_args or [])
        return (0, "", False)

    monkeypatch.setattr(move_mod, "_remote_dir_exists", lambda r, p: True)
    monkeypatch.setattr(move_mod, "_run_rsync_streamed", fake_rsync)
    monkeypatch.setattr(move_mod, "_remote_verify_complete",
                        lambda *a, **k: None)

    move_mod.move_folder(
        db=env["db"], folder_id=env["fid"], destination="", merge=True,
        remote=env["remote"])

    assert "--partial-dir=.rsync-partial" in seen["extra_args"]
    # Bare --partial must not also appear: it would put partials back at the
    # destination filename and re-introduce the --ignore-existing collision.
    assert "--partial" not in seen["extra_args"]


def test_remote_move_refuses_relative_mount_dest_base(remote_env, monkeypatch):
    """A relative mount_dest_base would make resolve_folder_dest produce a
    relative catalog_path. After the SSH copy succeeds and originals are
    deleted, the DB row would point at a non-resolving path relative to the
    server cwd. Refuse before any transfer happens."""
    env = remote_env
    env["remote"]["mount_dest_base"] = "Photos"  # relative — not absolute

    called = {"rsync": False, "exists": False}

    def fake_rsync(*a, **k):
        called["rsync"] = True
        return (0, "", False)

    def fake_exists(*a, **k):
        called["exists"] = True
        return False

    monkeypatch.setattr(move_mod, "_remote_dir_exists", fake_exists)
    monkeypatch.setattr(move_mod, "_run_rsync_streamed", fake_rsync)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid"], destination="",
        remote=env["remote"])

    assert result["moved"] == 0
    assert any("absolute" in e.lower() for e in result["errors"])
    # No SSH calls at all — refusal happens before any probing or transfer.
    assert called["rsync"] is False
    assert called["exists"] is False
    assert (env["src"] / "a.nef").exists()


def test_remote_move_refuses_when_catalog_path_overlaps_source(
        remote_env, monkeypatch):
    """Codex P1: if the source folder is already on the configured local
    mount (src=/Volumes/Photography/trip, mount=/Volumes/Photography), the
    catalog_path the move resolves to overlaps the source. The SSH copy and
    --checksum verify would both pass against the same underlying tree, and
    the post-move rmtree(src) would wipe the only copy. The overlap guard
    must check catalog_path for remote moves, not just transfer_dest."""
    env = remote_env
    # Set the mount path to the source itself so the resolved catalog_path
    # (mount + folder name) is a child of the source — i.e. moving into a
    # subdirectory of the source via the configured local mount. After the
    # "copy" and verify, rmtree(src) would wipe both source and copy.
    env["remote"]["mount_dest_base"] = str(env["src"])

    called = {"rsync": False, "exists": False}

    def fake_rsync(*a, **k):
        called["rsync"] = True
        return (0, "", False)

    def fake_exists(*a, **k):
        called["exists"] = True
        return False

    monkeypatch.setattr(move_mod, "_remote_dir_exists", fake_exists)
    monkeypatch.setattr(move_mod, "_run_rsync_streamed", fake_rsync)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid"], destination="",
        remote=env["remote"])

    assert result["moved"] == 0
    assert any("overlap" in e.lower() for e in result["errors"])
    # No transfer happens.
    assert called["rsync"] is False
    # Source untouched.
    assert (env["src"] / "a.nef").exists()


def test_remote_move_uses_posix_join_for_transfer_dest(remote_env, monkeypatch):
    """The NAS-side rsync target must use forward slashes regardless of the
    client OS. os.path.join on Windows would build
    "/volume1/Photo\\trip"; rsync would then ship the backslash as part of
    the remote path and fail or write to the wrong directory."""
    env = remote_env
    captured = {}

    def fake_rsync(src_path, dest_spec, flags, total, cb, rsync_bin="rsync",
                   extra_args=None, **kw):
        captured["dest_spec"] = dest_spec
        return (0, "", False)

    monkeypatch.setattr(move_mod, "_remote_dir_exists", lambda r, p: False)
    monkeypatch.setattr(move_mod, "_run_rsync_streamed", fake_rsync)
    monkeypatch.setattr(move_mod, "_remote_verify_complete",
                        lambda *a, **k: None)

    move_mod.move_folder(
        db=env["db"], folder_id=env["fid"], destination="",
        remote=env["remote"])

    # No backslashes in the rsync target — even though os.path.join on
    # Windows would have inserted one. Asserts the join was POSIX-only.
    assert "\\" not in captured["dest_spec"]
    assert captured["dest_spec"] == "me@nas:/volume1/Photography/trip"


def test_remote_move_refuses_when_catalog_path_overlaps_tracked(remote_env, monkeypatch):
    """The local mount path the catalog is repointed to after a remote move
    must not overlap another tracked folder — that would copy the tree over
    SSH only to hit folders.path UNIQUE on the post-move repoint. Refuse the
    move before any SSH transfer happens."""
    env = remote_env
    # Pre-register a tracked folder at the resolved catalog_path.
    catalog_path = os.path.join(env["remote"]["mount_dest_base"], "trip")
    other_fid = env["db"].add_folder(catalog_path, name="trip")
    assert other_fid != env["fid"]

    called = {"rsync": False, "exists": False}

    def fake_rsync(*a, **k):
        called["rsync"] = True
        return (0, "", False)

    def fake_exists(*a, **k):
        called["exists"] = True
        return False

    monkeypatch.setattr(move_mod, "_remote_dir_exists", fake_exists)
    monkeypatch.setattr(move_mod, "_run_rsync_streamed", fake_rsync)
    monkeypatch.setattr(move_mod, "_remote_verify_complete",
                        lambda *a, **k: None)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid"], destination="",
        remote=env["remote"])

    assert result["moved"] == 0
    assert any("already manages" in e for e in result["errors"])
    # Nothing transferred — overlap detected before any SSH call.
    assert called["rsync"] is False
    # Source untouched.
    assert (env["src"] / "a.nef").exists()


def test_remote_move_refuses_when_ssh_probe_fails(remote_env, monkeypatch):
    """A failed SSH ``test -d`` probe (auth failure, connect timeout, ssh
    binary missing, etc.) must NOT collapse to "destination absent". Otherwise
    the move would proceed as a fresh transfer (omitting --ignore-existing)
    and overwrite same-name files on a real existing destination before the
    --checksum verify could preserve the originals. _remote_dir_exists returns
    None on probe failure; the caller must refuse the move."""
    env = remote_env
    called = {"rsync": False}

    def fake_rsync(*a, **k):
        called["rsync"] = True
        return (0, "", False)

    # None == "probe couldn't conclude" — caller must refuse, not assume False.
    monkeypatch.setattr(move_mod, "_remote_dir_exists", lambda r, p: None)
    monkeypatch.setattr(move_mod, "_run_rsync_streamed", fake_rsync)
    monkeypatch.setattr(move_mod, "_remote_verify_complete",
                        lambda *a, **k: None)

    result = move_mod.move_folder(
        db=env["db"], folder_id=env["fid"], destination="",
        remote=env["remote"])

    assert result["moved"] == 0
    assert any("ssh" in e.lower() or "probe" in e.lower()
               for e in result["errors"])
    # Crucially: rsync was NOT invoked. The transient probe failure must not
    # be silently treated as "fresh transfer ok".
    assert called["rsync"] is False
    # Source untouched.
    assert (env["src"] / "a.nef").exists()


def test_remote_dir_exists_tri_state(monkeypatch):
    """_remote_dir_exists distinguishes exists / absent / probe-failed.

    test -d exits 0 if dir, 1 if not; ssh returns 255 on its own failure. Any
    return code outside {0, 1} or an OSError must collapse to None (the caller
    refuses), not to False (the caller would proceed without --ignore-existing
    on a real existing destination).
    """
    remote = {"host": "h", "user": "u", "port": 22, "ssh_key": ""}

    class FakeCompleted:
        def __init__(self, rc):
            self.returncode = rc

    monkeypatch.setattr(move_mod.subprocess, "run",
                        lambda *a, **k: FakeCompleted(0))
    assert move_mod._remote_dir_exists(remote, "/p") is True

    monkeypatch.setattr(move_mod.subprocess, "run",
                        lambda *a, **k: FakeCompleted(1))
    assert move_mod._remote_dir_exists(remote, "/p") is False

    # SSH-level failure (returncode 255).
    monkeypatch.setattr(move_mod.subprocess, "run",
                        lambda *a, **k: FakeCompleted(255))
    assert move_mod._remote_dir_exists(remote, "/p") is None

    # subprocess raising (ssh binary missing, timeout, etc.).
    def boom(*a, **k):
        raise OSError("ssh: not found")
    monkeypatch.setattr(move_mod.subprocess, "run", boom)
    assert move_mod._remote_dir_exists(remote, "/p") is None


def test_test_remote_connection_probes_remote_rsync(monkeypatch):
    """test_remote_connection must verify rsync is available on the REMOTE
    side, not just locally. SSH + writable remote_path + a local rsync_bin
    isn't enough on its own: rsync-over-SSH invokes a `--rsync-path` program
    on the remote, and on a Synology NAS that program is gated by DSM's
    "Enable rsync service" toggle. Without this probe the test reports
    "Connection OK" and the user only finds out at first move."""
    remote = {"host": "nas", "user": "me", "port": 22, "ssh_key": "",
              "remote_path": "/volume1/Photography"}

    class FakeCompleted:
        def __init__(self, rc=0, stdout="", stderr=""):
            self.returncode = rc
            self.stdout = stdout
            self.stderr = stderr

    # Three SSH commands run in order: echo probe, writable check, rsync
    # probe. A step counter lets the fake return the appropriate response.
    calls = {"n": 0}

    def fake_run_remote_rsync_missing(cmd, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeCompleted(0, "vireo_ok\n", "")
        if calls["n"] == 2:
            return FakeCompleted(0, "WRITABLE\n", "")
        # Remote rsync probe — simulate command-not-found (exit 127).
        return FakeCompleted(127, "", "rsync: command not found\n")

    monkeypatch.setattr(move_mod.subprocess, "run",
                        fake_run_remote_rsync_missing)
    res = move_mod.test_remote_connection(remote, "/usr/bin/rsync")
    assert res["ssh"] is True
    assert res["remote_path_writable"] is True
    assert res["rsync_ok"] is True  # local rsync_bin was provided
    assert res["remote_rsync_ok"] is False
    assert res["ok"] is False
    assert "remote" in res["message"].lower()

    # Now stage the rsync probe to succeed — full green path.
    calls["n"] = 0

    def fake_run_all_ok(cmd, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeCompleted(0, "vireo_ok\n", "")
        if calls["n"] == 2:
            return FakeCompleted(0, "WRITABLE\n", "")
        return FakeCompleted(0, "rsync  version 3.2.7\n", "")

    monkeypatch.setattr(move_mod.subprocess, "run", fake_run_all_ok)
    res = move_mod.test_remote_connection(remote, "/usr/bin/rsync")
    assert res["remote_rsync_ok"] is True
    assert res["ok"] is True
