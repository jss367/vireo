"""Run the node unit tests for the pure helpers in vireo/static/browse/."""
import shutil
import subprocess
from pathlib import Path

import pytest


def test_browse_pure_helpers():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for JavaScript unit tests")
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [node, str(Path(__file__).with_name("browse_pure_helpers.cjs"))],
        cwd=root, capture_output=True, text=True, encoding="utf-8", timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_browse_page_loads_every_browse_script_once_boot_last(app_and_db):
    """browse.html must load each file in static/browse/ exactly once, with
    boot.js last: it starts the work every other browse script defines, so a
    script listed after it (or not at all) would be missing when it runs.
    ``test_rendered_page_scripts_parse`` syntax-checks each one it loads."""
    import re

    app, _ = app_and_db
    html = app.test_client().get("/browse").get_data(as_text=True)
    loaded = re.findall(r'<script src="/static/browse/([\w.-]+\.js)"></script>', html)
    on_disk = sorted(p.name for p in (Path(__file__).resolve().parents[1] / "static" / "browse").glob("*.js"))
    assert sorted(loaded) == on_disk
    assert len(loaded) == len(set(loaded))
    assert loaded[-1] == "boot.js"
