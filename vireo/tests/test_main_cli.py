import subprocess
import sys
from pathlib import Path


def test_help_includes_headless_flag():
    repo = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, str(repo / "vireo" / "app.py"), "--help"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0
    assert "--headless" in result.stdout
