"""Tests for scripts/generate_update_manifest.py."""

import json
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "generate_update_manifest.py"


def test_all_platforms(tmp_path):
    """Produces correct latest.json when all four platforms have artifacts."""
    (tmp_path / "Vireo_aarch64.app.tar.gz").write_bytes(b"fake")
    (tmp_path / "Vireo_aarch64.app.tar.gz.sig").write_text("sig-darwin-arm64")
    (tmp_path / "Vireo_x64.app.tar.gz").write_bytes(b"fake")
    (tmp_path / "Vireo_x64.app.tar.gz.sig").write_text("sig-darwin-x64")
    (tmp_path / "Vireo_0.6.27_x64-setup.exe").write_bytes(b"fake")
    (tmp_path / "Vireo_0.6.27_x64-setup.exe.sig").write_text("sig-windows")
    (tmp_path / "vireo_0.6.27_amd64.AppImage").write_bytes(b"fake")
    (tmp_path / "vireo_0.6.27_amd64.AppImage.sig").write_text("sig-linux")

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "v0.6.27", str(tmp_path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

    manifest = json.loads((tmp_path / "latest.json").read_text())
    assert manifest["version"] == "0.6.27"
    assert "pub_date" in manifest
    assert len(manifest["platforms"]) == 4

    darwin_arm = manifest["platforms"]["darwin-aarch64"]
    assert darwin_arm["signature"] == "sig-darwin-arm64"
    assert "Vireo_aarch64.app.tar.gz" in darwin_arm["url"]
    assert "v0.6.27" in darwin_arm["url"]

    assert manifest["platforms"]["darwin-x86_64"]["signature"] == "sig-darwin-x64"
    assert manifest["platforms"]["windows-x86_64"]["signature"] == "sig-windows"
    assert manifest["platforms"]["linux-x86_64"]["signature"] == "sig-linux"


def test_partial_platforms(tmp_path):
    """Handles missing platforms gracefully — only includes what's available."""
    (tmp_path / "vireo_1.0.0_amd64.AppImage").write_bytes(b"fake")
    (tmp_path / "vireo_1.0.0_amd64.AppImage.sig").write_text("sig-linux")

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "v1.0.0", str(tmp_path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0

    manifest = json.loads((tmp_path / "latest.json").read_text())
    assert manifest["version"] == "1.0.0"
    assert len(manifest["platforms"]) == 1
    assert "linux-x86_64" in manifest["platforms"]


def test_no_artifacts(tmp_path):
    """Exits cleanly and produces no latest.json when no updater artifacts exist."""
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "v0.6.27", str(tmp_path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert not (tmp_path / "latest.json").exists()


def test_missing_sig_skips_platform(tmp_path):
    """Skips a platform if its .sig file is missing."""
    (tmp_path / "Vireo_aarch64.app.tar.gz").write_bytes(b"fake")
    # No .sig file for this artifact
    (tmp_path / "vireo_0.6.27_amd64.AppImage").write_bytes(b"fake")
    (tmp_path / "vireo_0.6.27_amd64.AppImage.sig").write_text("sig-linux")

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "v0.6.27", str(tmp_path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0

    manifest = json.loads((tmp_path / "latest.json").read_text())
    assert len(manifest["platforms"]) == 1
    assert "linux-x86_64" in manifest["platforms"]
    assert "darwin-aarch64" not in manifest["platforms"]
