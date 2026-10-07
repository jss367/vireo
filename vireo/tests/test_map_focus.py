""""View on Map" for a photo the map cannot place explains why instead of
showing the rest of the library."""
import shutil
import subprocess
from pathlib import Path

import pytest
from page_scripts import page_with_scripts


def test_map_focus_helper():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for JavaScript unit tests")
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [node, str(Path(__file__).with_name("map_focus.cjs"))],
        cwd=root, capture_output=True, text=True, encoding="utf-8", timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_map_page_shows_notice_for_unplottable_focus(app_and_db):
    """The empty-result branch hands a deep link's ``unplottable_focus`` to
    the notice, and every load clears a notice left by an earlier one."""
    app, _ = app_and_db
    page = page_with_scripts(app.test_client(), "/map")

    assert 'id="mapFocusNotice"' in page
    load = page[page.index("async function loadPhotos("):]
    load = load[:load.index("\n}\n")]
    assert "showUnplottableFocusNotice(data.unplottable_focus, loadPhotos)" in load
    assert load.index("hideUnplottableFocusNotice()") < load.index("data.photos.length === 0")
    assert "No map location found for this photo." not in page


def test_keywords_page_opens_link_place_from_url(app_and_db):
    app, _ = app_and_db
    page = page_with_scripts(app.test_client(), "/keywords")

    assert "function openLinkPlaceFromUrl(" in page
    assert "loadKeywords().then(() => openLinkPlaceFromUrl(allKeywords, openLinkPlaceModal))" in page
