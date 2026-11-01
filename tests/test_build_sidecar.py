"""Release provenance must survive the frozen sidecar build."""

import json
import sys
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import build_sidecar


@pytest.mark.parametrize("platform", ["Darwin", "Windows", "Linux"])
def test_sidecar_bundles_synchronized_release_manifest(monkeypatch, tmp_path, platform):
    """Inspect the actual build command without compiling or fetching vendors."""
    monkeypatch.setattr(sys, "argv", ["build_sidecar.py", "--ci"])
    monkeypatch.setattr(build_sidecar.platform, "system", lambda: platform)
    monkeypatch.setattr(build_sidecar, "get_target_triple", lambda: "test-target")
    monkeypatch.setitem(sys.modules, "vireo._native_detail", SimpleNamespace(Filters=lambda _: None))
    monkeypatch.setitem(sys.modules, "fetch_exiftool", SimpleNamespace(fetch=lambda _: tmp_path))
    commands = []

    class BuildRequested(Exception):
        pass

    def capture(command, **kwargs):
        commands.append(command)
        raise BuildRequested

    monkeypatch.setattr(build_sidecar.subprocess, "run", capture)
    with pytest.raises(BuildRequested):
        build_sidecar.main()

    command, = commands
    separator = ";" if platform == "Windows" else ":"
    data = [command[i + 1] for i, arg in enumerate(command) if arg == "--add-data"]
    manifest, = [item for item in data if "pyproject.toml" in item]
    source, destination = manifest.rsplit(separator, 1)
    assert destination == "."  # sys._MEIPASS, regardless of platform
    source = Path(source)
    version = tomllib.loads(source.read_text())["project"]["version"]
    desktop = json.loads((source.parent / "src-tauri/tauri.conf.json").read_text())
    assert version == desktop["version"]
