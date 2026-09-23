"""Execute browser state transitions and parse scripts rendered by Flask."""
import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path

import pytest


@pytest.fixture
def node():
    executable = shutil.which("node")
    if executable is None:
        pytest.skip("Node.js is required for JavaScript regression tests")
    return executable


def test_selection_and_review_state(node):
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [node, str(Path(__file__).with_name("browser_state_regressions.cjs"))],
        cwd=root, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("route", ["/browse", "/pipeline", "/pipeline/review", "/settings"])
def test_rendered_page_scripts_parse(app_and_db, node, route):
    app, _ = app_and_db
    response = app.test_client().get(route)
    assert response.status_code == 200
    class Scripts(HTMLParser):
        def __init__(self):
            super().__init__()
            self.scripts = []
            self.in_script = False

        def handle_starttag(self, tag, attrs):
            if tag == "script":
                self.in_script = True

        def handle_endtag(self, tag):
            if tag == "script":
                self.in_script = False

        def handle_data(self, data):
            if self.in_script:
                self.scripts.append(data)

    parser = Scripts()
    parser.feed(response.get_data(as_text=True))
    assert parser.scripts
    for script in parser.scripts:
        if not script.strip():
            continue
        result = subprocess.run([node, "--check"], input=script, text=True, capture_output=True)
        assert result.returncode == 0, result.stderr
