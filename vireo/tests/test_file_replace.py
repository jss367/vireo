"""``file_replace.replace_file``: os.replace with a Windows lock retry."""

import ast
import os
from pathlib import Path

import file_replace
import pytest


class _Win32:
    platform = "win32"


class _Posix:
    platform = "darwin"


def _flaky(real, failures, exc=None):
    exc = exc or PermissionError(5, "Access is denied")
    calls = {"n": 0}

    def replace(src, dst):
        calls["n"] += 1
        if calls["n"] <= failures:
            raise exc
        return real(src, dst)

    return replace, calls


def test_retries_a_transient_windows_lock(tmp_path, monkeypatch):
    """A destination held open for a moment (Defender scanning the last
    handoff, an editor that still has it) is replaced once released."""
    src, dst = tmp_path / "new.tmp", tmp_path / "4.jpg"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")
    replace, calls = _flaky(os.replace, failures=3)
    monkeypatch.setattr(file_replace, "sys", _Win32())
    monkeypatch.setattr(file_replace.os, "replace", replace)
    monkeypatch.setattr(file_replace.time, "sleep", lambda _s: None)

    file_replace.replace_file(str(src), str(dst))

    assert calls["n"] == 4
    assert dst.read_bytes() == b"new"
    assert not src.exists()


def test_a_lock_that_never_clears_raises_the_last_error(tmp_path, monkeypatch):
    replace, calls = _flaky(os.replace, failures=100)
    monkeypatch.setattr(file_replace, "sys", _Win32())
    monkeypatch.setattr(file_replace.os, "replace", replace)
    sleeps = []
    monkeypatch.setattr(file_replace.time, "sleep", sleeps.append)

    with pytest.raises(PermissionError):
        file_replace.replace_file(str(tmp_path / "a"), str(tmp_path / "b"))

    assert calls["n"] == len(file_replace._WINDOWS_RETRY_DELAYS)
    assert sum(sleeps) < 7


def test_other_errors_are_not_retried(tmp_path, monkeypatch):
    replace, calls = _flaky(os.replace, failures=100, exc=FileNotFoundError(2, "gone"))
    monkeypatch.setattr(file_replace, "sys", _Win32())
    monkeypatch.setattr(file_replace.os, "replace", replace)
    monkeypatch.setattr(file_replace.time, "sleep", lambda _s: None)

    with pytest.raises(FileNotFoundError):
        file_replace.replace_file(str(tmp_path / "a"), str(tmp_path / "b"))
    assert calls["n"] == 1


def test_off_windows_it_is_exactly_os_replace(tmp_path, monkeypatch):
    replace, calls = _flaky(os.replace, failures=1)
    monkeypatch.setattr(file_replace, "sys", _Posix())
    monkeypatch.setattr(file_replace.os, "replace", replace)

    with pytest.raises(PermissionError):
        file_replace.replace_file(str(tmp_path / "a"), str(tmp_path / "b"))
    assert calls["n"] == 1


def test_production_code_publishes_through_replace_file():
    """Every publish-by-rename goes through replace_file, so a Windows lock
    on the destination is retried instead of failing the request or job."""
    root = Path(__file__).resolve().parents[1]
    offenders = []
    for path in root.rglob("*.py"):
        rel = path.relative_to(root).as_posix()
        if rel.startswith("tests/") or rel == "file_replace.py":
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if (
                isinstance(node, ast.Attribute)
                and node.attr == "replace"
                and isinstance(node.value, ast.Name)
                and node.value.id == "os"
            ):
                offenders.append(f"{rel}:{node.lineno}")
    assert offenders == [], "use file_replace.replace_file instead of os.replace: " + ", ".join(offenders)
