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

    lock = {
        "name": "vireo",
        "version": "0.0.0",
        "lockfileVersion": 3,
        "requires": True,
        "packages": {
            "": {"name": "vireo", "version": "0.0.0", "devDependencies": {"acorn": "^8.0.0"}},
            "node_modules/acorn": {
                "version": "8.15.0",
                "resolved": "https://registry.npmjs.org/acorn/-/acorn-8.15.0.tgz",
                "integrity": "sha512-test",
                "dev": True,
            },
        },
    }
    (tmp_path / "package-lock.json").write_text(json.dumps(lock, indent=2) + "\n")

    cargo_toml = tmp_path / "src-tauri" / "Cargo.toml"
    cargo_toml.write_text('[package]\nname = "vireo"\nversion = "0.0.0"\n')

    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[project]\nname = "vireo"\nversion = "0.0.0"\n')

    download_astro = tmp_path / "website" / "src" / "pages" / "download.astro"
    download_astro.parent.mkdir(parents=True)
    download_astro.write_text("---\nconst version = '0.0.0'\n---\n<html></html>\n")

    return tmp_path


def test_sync_version_updates_all_files(version_files, monkeypatch):
    """sync_version.py should update every manifest, including the npm lockfile."""
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


@pytest.mark.parametrize("version", ["1.2.3", "v2.0.0-beta.1"])
def test_sync_version_changes_only_root_lockfile_versions(version_files, version):
    lock_path = version_files / "package-lock.json"
    expected = json.loads(lock_path.read_text())
    expected["version"] = version.removeprefix("v")
    expected["packages"][""]["version"] = version.removeprefix("v")

    subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts/sync_version.py"), version],
        cwd=version_files, capture_output=True, text=True, check=True,
    )

    assert json.loads(lock_path.read_text()) == expected


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


@pytest.mark.parametrize("version", ["", "v", "not-a-version"])
def test_sync_version_rejects_invalid_version_without_writing(version_files, version):
    """Invalid versions must fail before any manifest or website is changed."""
    original_contents = {
        path.relative_to(version_files): path.read_text()
        for path in version_files.rglob("*")
        if path.is_file()
    }

    script = str(REPO_ROOT / "scripts" / "sync_version.py")
    result = subprocess.run(
        [sys.executable, script, version, "--include-website"],
        cwd=version_files,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "Invalid version" in result.stderr
    assert all(
        (version_files / path).read_text() == contents
        for path, contents in original_contents.items()
    )
