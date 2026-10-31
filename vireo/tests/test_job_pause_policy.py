"""New production launchers cannot silently omit pause support."""

import ast
from pathlib import Path

import pytest
from job_runner.pause_policy import PAUSABLE_JOBS, UNPAUSABLE_JOBS
from jobs import JobRunner


def test_unknown_jobs_must_declare_pause_policy():
    runner = JobRunner()
    try:
        with pytest.raises(ValueError, match="must declare a pause policy"):
            runner.start("future-operation", lambda job: None)
        with pytest.raises(ValueError, match="must declare a pause policy"):
            runner.start_singleton("future-operation", lambda job: None, singleton_key="one")
        assert runner.list_jobs() == []
    finally:
        runner.shutdown()


def test_inventory_has_no_ambiguous_or_unexplained_policies():
    assert not PAUSABLE_JOBS.intersection(UNPAUSABLE_JOBS)
    assert all(isinstance(reason, str) and reason.strip()
               for reason in UNPAUSABLE_JOBS.values())


def test_all_literal_production_job_launchers_have_audited_policies():
    root = Path(__file__).parents[1]
    missing = []
    for path in root.rglob("*.py"):
        if "tests" in path.relative_to(root).parts or "testing" in path.relative_to(root).parts:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in ("start", "start_job", "start_singleton") or not node.args:
                continue
            value = node.args[0]
            if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
                continue
            if value.value not in PAUSABLE_JOBS and value.value not in UNPAUSABLE_JOBS:
                missing.append(f"{path.relative_to(root)}:{node.lineno}: {value.value}")
    assert not missing, "Declare and test pause policies for these jobs:\n" + "\n".join(missing)
