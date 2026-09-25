"""The settings autosave posts a secret field only when the user edited it."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_settings_autosave_sends_only_edited_secrets():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for JavaScript regression tests")
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [node, str(Path(__file__).with_name("settings_secret_fields.cjs"))],
        cwd=root, capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
