"""Resolve the running build's version in source and frozen installations."""

import sys
import tomllib
from pathlib import Path

import pytest
from web import system


def test_frozen_version_uses_bundled_manifest(monkeypatch, tmp_path):
    (tmp_path / "pyproject.toml").write_text('[project]\nversion = "1.2.3-beta.1"\n')
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "_MEIPASS", str(tmp_path), raising=False)

    def unexpected_package_lookup(_name):
        pytest.fail("Frozen builds must not use build-environment distribution metadata")

    monkeypatch.setattr(system, "package_version", unexpected_package_lookup)
    assert system._application_version() == "1.2.3-beta.1"


@pytest.mark.parametrize("contents", [None, "invalid toml", "[project]\n", b"\xff"])
def test_unreadable_frozen_manifest_never_uses_source_or_installed_version(
    monkeypatch, tmp_path, contents,
):
    """A damaged bundle must not claim the version of a different installation."""
    if contents is not None:
        (tmp_path / "pyproject.toml").write_bytes(
            contents if isinstance(contents, bytes) else contents.encode()
        )
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "_MEIPASS", str(tmp_path), raising=False)
    monkeypatch.setattr(system, "package_version", lambda _: "9.9.9")
    assert system._application_version() == "0.0.0"


def test_source_version_uses_installed_distribution(monkeypatch):
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.setattr(system, "package_version", lambda _: "2.3.4")
    assert system._application_version() == "2.3.4"


def test_source_version_falls_back_to_checkout_manifest(monkeypatch):
    monkeypatch.setattr(sys, "frozen", False, raising=False)

    def missing(_name):
        raise system.PackageNotFoundError("vireo")

    monkeypatch.setattr(system, "package_version", missing)
    manifest = Path(__file__).resolve().parents[2] / "pyproject.toml"
    assert system._application_version() == tomllib.loads(manifest.read_text())["project"]["version"]


@pytest.mark.parametrize("contents", [None, "", "unknown", "a" * 39, "z" * 40, b"\xff"])
def test_frozen_commit_unavailable_is_explicit(monkeypatch, tmp_path, contents):
    if contents is not None:
        (tmp_path / "build-commit.txt").write_bytes(
            contents if isinstance(contents, bytes) else contents.encode()
        )
    monkeypatch.setattr(sys, "_MEIPASS", str(tmp_path), raising=False)
    assert system._application_build_commit() is None


def test_frozen_commit_uses_only_bundle_stamp(monkeypatch, tmp_path):
    commit = "abcde01234" * 4
    (tmp_path / "build-commit.txt").write_text(commit + "\n")
    monkeypatch.setattr(sys, "_MEIPASS", str(tmp_path), raising=False)
    assert system._application_build_commit() == commit


def test_source_commit_identifies_own_checkout(monkeypatch, tmp_path):
    import subprocess

    monkeypatch.delattr(sys, "_MEIPASS", raising=False)
    monkeypatch.chdir(tmp_path)
    root = Path(__file__).resolve().parents[2]
    expected = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    assert system._application_build_commit() == expected
