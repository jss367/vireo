"""Run the node unit tests for pure helpers in the shared navbar scripts."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_navbar_pure_functions():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for JavaScript regression tests")
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [node, str(Path(__file__).with_name("navbar_pure_functions.cjs"))],
        cwd=root, capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
