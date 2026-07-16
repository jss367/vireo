import os
from types import SimpleNamespace

import platform_support


def test_dependency_readiness_requires_both_remote_tools(monkeypatch):
    monkeypatch.setattr(
        platform_support,
        "exiftool_status",
        lambda: {"available": True, "path": "exiftool", "version": "13", "error": None, "hint": ""},
    )
    monkeypatch.setattr(platform_support, "find_darktable", lambda _path: None)
    monkeypatch.setattr(platform_support, "find_dng_converter", lambda _path: None)
    monkeypatch.setattr(platform_support, "find_ssh", lambda _path="": "ssh.exe")
    monkeypatch.setattr(platform_support, "_probe", lambda _binary, _args: (True, "OpenSSH_9"))
    monkeypatch.setattr(platform_support, "resolve_rsync_bin", lambda _path="": None)

    readiness = platform_support.dependency_readiness({})

    assert readiness["exiftool"]["state"] == "ready"
    assert readiness["openssh"]["state"] == "ready"
    assert readiness["rsync"]["state"] == "missing"
    assert readiness["remote_transfer"]["state"] == "unavailable"


def test_filesystem_type_is_not_probed_off_windows(monkeypatch):
    monkeypatch.setattr(platform_support.os, "name", "posix")
    monkeypatch.setattr(
        platform_support.ctypes,
        "windll",
        SimpleNamespace(kernel32=SimpleNamespace()),
        raising=False,
    )
    assert platform_support.filesystem_type("/photos") is None


def test_program_files_candidates_are_deduplicated(monkeypatch, tmp_path):
    monkeypatch.setenv("PROGRAMFILES", str(tmp_path))
    monkeypatch.setenv("PROGRAMW6432", str(tmp_path))
    monkeypatch.delenv("PROGRAMFILES(X86)", raising=False)
    assert platform_support._program_files_candidates("Adobe") == [
        os.path.join(str(tmp_path), "Adobe")
    ]


def test_windows_11_build_is_public_beta(monkeypatch):
    monkeypatch.setattr(platform_support.os, "name", "nt")
    monkeypatch.setattr(platform_support.platform, "release", lambda: "11")
    monkeypatch.setattr(platform_support.platform, "version", lambda: "10.0.26100")
    monkeypatch.setattr(platform_support.platform, "machine", lambda: "AMD64")
    monkeypatch.setattr(platform_support, "dependency_readiness", lambda _config: {})
    monkeypatch.setattr(platform_support, "webview2_version", lambda: "1.0")
    monkeypatch.setattr(
        platform_support,
        "windows_long_path_status",
        lambda: {"state": "ready", "enabled": True},
    )

    info = platform_support.platform_support_info({})

    assert info["support_tier"] == "public_beta"
    assert info["guaranteed_inference"] == "CPUExecutionProvider"
