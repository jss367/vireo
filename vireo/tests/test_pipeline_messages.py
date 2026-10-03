"""The Process page shows a run's benign-skip notes as notes, not failures."""
import shutil
import subprocess
from pathlib import Path

import pytest
from page_scripts import page_with_scripts


def test_pipeline_messages_helper():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for JavaScript unit tests")
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [node, str(Path(__file__).with_name("pipeline_messages.cjs"))],
        cwd=root, capture_output=True, text=True, encoding="utf-8", timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_process_page_renders_notes_apart_from_failures(app_and_db):
    """Every entry in result.errors used to land on its stage card as
    "Failed: ...", including the notes in result.notes that explain a stage
    that only skipped. The completion handler must split them through
    splitPipelineMessages and give notes their own wording and banner."""
    app, _ = app_and_db
    page = page_with_scripts(app.test_client(), "/pipeline")

    assert "function splitPipelineMessages(" in page
    handler = page[page.index("function _onPipelineComplete("):]
    handler = handler[:handler.index("\nfunction ", 1)]
    assert "splitPipelineMessages(r.errors, r.notes)" in handler
    assert "'Skipped: '" in handler
    assert "_showPipelineNotes(" in handler
    # Notes-only runs stay on the page so the notes are read.
    assert "messages.noteList.length === 0" in handler
    # The failure path reads only the errors that are not notes.
    assert "r.errors.forEach" not in handler
    assert 'id="pipelineNoteBanner"' in page
    assert "'skipped':    'Skipped'" in page


def test_workspace_job_history_prefers_the_summary(app_and_db):
    """The workspace page's Recent Jobs table dumped raw result keys, so a
    green Process run with a skipped stage read "errors: 1 items". It shows
    the server's summary, which words the skip as a skip."""
    app, _ = app_and_db
    page = page_with_scripts(app.test_client(), "/workspace")
    history = page[page.index("async function loadHistory("):]
    history = history[:history.index("\nfunction ", 1)]
    assert "if (r.summary)" in history
    assert history.index("result = r.summary") < history.index("JSON.parse(r.result)")
