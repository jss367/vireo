"""Run the node unit tests for vireo/static/vireo-export-job.js."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_export_job_outcome():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for JavaScript unit tests")
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [node, str(Path(__file__).with_name("export_job_outcome.cjs"))],
        cwd=root, capture_output=True, text=True, encoding="utf-8", timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
