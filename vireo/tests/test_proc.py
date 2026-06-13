"""Tests for vireo/proc.py and the Windows console-window suppression
applied to external CLI tool invocations (exiftool, darktable-cli, DNG).

On Windows the packaged GUI app has no console, so spawning a console
program flashes a console window unless CREATE_NO_WINDOW is passed. These
tests assert the flag is set on win32 and absent elsewhere.
"""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import proc


def test_no_window_kwargs_empty_on_posix(monkeypatch):
    monkeypatch.setattr(proc.sys, "platform", "linux")
    assert proc.no_window_kwargs() == {}


def test_no_window_kwargs_empty_on_macos(monkeypatch):
    monkeypatch.setattr(proc.sys, "platform", "darwin")
    assert proc.no_window_kwargs() == {}


def test_no_window_kwargs_sets_flag_on_windows(monkeypatch):
    monkeypatch.setattr(proc.sys, "platform", "win32")
    # CREATE_NO_WINDOW is absent from the POSIX stdlib, so the module
    # captures it as 0 there. Force a representative non-zero value so the
    # test is meaningful regardless of the host platform.
    monkeypatch.setattr(proc, "_CREATE_NO_WINDOW", 0x08000000)
    assert proc.no_window_kwargs() == {"creationflags": 0x08000000}


def test_run_exiftool_passes_no_window_on_windows(monkeypatch):
    import metadata

    monkeypatch.setattr(metadata.sys, "platform", "win32")
    monkeypatch.setattr(proc.sys, "platform", "win32")
    monkeypatch.setattr(proc, "_CREATE_NO_WINDOW", 0x08000000)

    with patch("metadata.subprocess.run") as run:
        run.return_value = type("R", (), {"returncode": 0, "stdout": "[]", "stderr": ""})()
        metadata._run_exiftool(["/tmp/x.jpg"])

    _args, kwargs = run.call_args
    assert kwargs.get("creationflags") == 0x08000000


def test_run_exiftool_no_flag_on_posix(monkeypatch):
    import metadata

    monkeypatch.setattr(proc.sys, "platform", "linux")

    with patch("metadata.subprocess.run") as run:
        run.return_value = type("R", (), {"returncode": 0, "stdout": "[]", "stderr": ""})()
        metadata._run_exiftool(["/tmp/x.jpg"])

    _args, kwargs = run.call_args
    assert "creationflags" not in kwargs


def test_exiftool_install_hint_per_platform(monkeypatch):
    import metadata

    monkeypatch.setattr(metadata.sys, "platform", "win32")
    assert "exiftool.org" in metadata._exiftool_install_hint()

    monkeypatch.setattr(metadata.sys, "platform", "darwin")
    assert "brew install exiftool" in metadata._exiftool_install_hint()

    monkeypatch.setattr(metadata.sys, "platform", "linux")
    hint = metadata._exiftool_install_hint()
    assert "brew" not in hint
    assert "package manager" in hint
