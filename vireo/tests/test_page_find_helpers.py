"""Exercise page Find routing and text matching without a browser."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_page_find_dom_helpers():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for page Find helper tests")
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [node, str(Path(__file__).with_name("page_find_dom.cjs"))],
        cwd=root, capture_output=True, text=True, encoding="utf-8", timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
