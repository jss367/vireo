"""Keep frontend validation unconditional and enforce failures at the PR gate."""

import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("workflow", ["test.yml", "test-main.yml"])
def test_frontend_checks_run_on_every_workflow_run(workflow):
    jobs = yaml.safe_load((ROOT / ".github/workflows" / workflow).read_text())["jobs"]
    frontend = jobs["frontend"]
    assert "if" not in frontend
    assert "needs" not in frontend
    commands = [step.get("run") for step in frontend["steps"]]
    assert commands.index("npm ci") < commands.index("npm run check:frontend")


@pytest.mark.parametrize("frontend_result,expected_exit", [("success", 0), ("failure", 1), ("cancelled", 1)])
@pytest.mark.skipif(sys.platform == "win32", reason="The PR aggregation gate runs Bash on Linux")
def test_pr_gate_rejects_frontend_failures(frontend_result, expected_exit):
    jobs = yaml.safe_load((ROOT / ".github/workflows/test.yml").read_text())["jobs"]
    gate = jobs["test"]
    assert "frontend" in gate["needs"]
    assert gate["if"] == "always()"
    script = next(step["run"] for step in gate["steps"] if "run" in step)
    script = re.sub(
        r"\$\{\{ needs\.([\w-]+)\.result \}\}",
        lambda match: frontend_result if match[1] == "frontend" else "success",
        script,
    )
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=10)
    assert result.returncode == expected_exit, result.stdout + result.stderr
