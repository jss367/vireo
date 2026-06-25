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
    assert move_mod.sanitize_subpath("/USA/2026/") == "USA/2026"
    assert move_mod.sanitize_subpath("USA//2026") == "USA/2026"
    assert move_mod.sanitize_subpath("a/./b") == "a/b"


def test_sanitize_subpath_rejects_traversal():
    with pytest.raises(ValueError):
        move_mod.sanitize_subpath("../escape")
    with pytest.raises(ValueError):
        move_mod.sanitize_subpath("a/../../b")


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


def test_resolve_rsync_bin(tmp_path, monkeypatch):
    monkeypatch.setattr(move_mod, "_BUNDLED_RSYNC_CANDIDATES", ())
    monkeypatch.delenv("VIREO_RSYNC_BIN", raising=False)
    # Nothing available -> None.
    assert move_mod.resolve_rsync_bin("") is None
    # An explicit executable path wins.
    fake = tmp_path / "rsync"
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
    fake = tmp_path / "rsync"
    fake.write_text("#!/bin/sh\n")
    fake.chmod(0o755)
    monkeypatch.setenv("VIREO_RSYNC_BIN", str(fake))
    assert move_mod.resolve_rsync_bin("") == str(fake)


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
    assert "-e" in calls["extra_args"] and "--partial" in calls["extra_args"]
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
