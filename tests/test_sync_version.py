"""Tests for scripts/sync_version.py."""
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent


@pytest.fixture()
def version_files(tmp_path):
    """Create mock version files in a temp directory."""
    tauri_conf = tmp_path / "src-tauri" / "tauri.conf.json"
    tauri_conf.parent.mkdir(parents=True)
    tauri_conf.write_text(json.dumps({"version": "0.0.0"}, indent=2) + "\n")

    pkg_json = tmp_path / "package.json"
    pkg_json.write_text(json.dumps({"version": "0.0.0"}, indent=2) + "\n")

    cargo_toml = tmp_path / "src-tauri" / "Cargo.toml"
    cargo_toml.write_text('[package]\nname = "vireo"\nversion = "0.0.0"\n')

    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[project]\nname = "vireo"\nversion = "0.0.0"\n')

    download_astro = tmp_path / "website" / "src" / "pages" / "download.astro"
    download_astro.parent.mkdir(parents=True)
    download_astro.write_text("---\nconst version = '0.0.0'\n---\n<html></html>\n")

    return tmp_path


def test_sync_version_updates_all_files(version_files, monkeypatch):
    """sync_version.py should update version in all four manifest files."""
    monkeypatch.chdir(version_files)

    script = str(REPO_ROOT / "scripts" / "sync_version.py")
    result = subprocess.run(
        [sys.executable, script, "v1.2.3"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0

    # Verify JSON files
    tauri_conf = json.loads(
        (version_files / "src-tauri" / "tauri.conf.json").read_text()
    )
    assert tauri_conf["version"] == "1.2.3"

    pkg = json.loads((version_files / "package.json").read_text())
    assert pkg["version"] == "1.2.3"

    # Verify TOML files
    cargo = (version_files / "src-tauri" / "Cargo.toml").read_text()
    assert 'version = "1.2.3"' in cargo

    pyproject = (version_files / "pyproject.toml").read_text()
    assert 'version = "1.2.3"' in pyproject


def test_sync_version_strips_v_prefix(version_files, monkeypatch):
    """The 'v' prefix should be stripped from the version string."""
    monkeypatch.chdir(version_files)

    script = str(REPO_ROOT / "scripts" / "sync_version.py")
    subprocess.run(
        [sys.executable, script, "v2.0.0-beta.1"],
        capture_output=True, text=True, check=True,
    )

    tauri_conf = json.loads(
        (version_files / "src-tauri" / "tauri.conf.json").read_text()
    )
    assert tauri_conf["version"] == "2.0.0-beta.1"
