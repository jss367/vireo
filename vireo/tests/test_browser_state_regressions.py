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
def test_rendered_page_scripts_parse(app_and_db, node, route, tmp_path):
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
    for index, script in enumerate(parser.scripts):
        if not script.strip():
            continue
        # Check a UTF-8 file instead of piping the script to ``node --check``
        # over stdin. On Windows the stdin form never exits: Python writes and
        # closes the pipe, then blocks forever waiting for node's stdout, until
        # pytest-timeout kills the xdist worker. ``text=True`` stdin would also
        # encode with the locale codec (cp1252 on Windows), which cannot
        # represent the arrows, check marks and emoji these pages contain. The
        # timeout keeps any future hang a test failure, not a worker crash.
        script_path = tmp_path / f"script-{index}.js"
        script_path.write_text(script, encoding="utf-8")
        result = subprocess.run(
            [node, "--check", str(script_path)],
            stdin=subprocess.DEVNULL, capture_output=True,
            encoding="utf-8", errors="replace", timeout=60,
        )
        assert result.returncode == 0, result.stderr
