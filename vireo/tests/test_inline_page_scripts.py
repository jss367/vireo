"""Page JavaScript lives in vireo/static/, not inline in templates.

Pages are moving their scripts out to classic scripts loaded with
``<script src>`` (see CLAUDE.md). These tests keep that move from sliding
back: inline script in a template may only shrink, and every page that has
been split loads each of its files once, with ``boot.js`` last.
"""
import re
from pathlib import Path

import pytest

TEMPLATES = Path(__file__).resolve().parents[1] / "templates"
STATIC = Path(__file__).resolve().parents[1] / "static"

# Non-blank lines of inline <script> per template. A template not listed here
# may have none. Lower an entry (or delete it at zero) when you move a page's
# script out; never raise one: put new page code in vireo/static/ instead.
INLINE_SCRIPT_LINE_LIMITS = {
    "_navbar.html": 1,
    "_sync_panel.html": 876,
    "audit.html": 480,
    "best_batch.html": 247,
    "browse.html": 4,
    "card_cleanup.html": 922,
    "cull.html": 820,
    "duplicates.html": 1360,
    "highlights.html": 1155,
    "id_conflicts.html": 968,
    "keywords.html": 1204,
    "lightroom.html": 135,
    "logs.html": 64,
    "map.html": 389,
    "misses.html": 1438,
    "move.html": 1660,
    "pipeline_rapid_review.html": 1614,
    "shortcuts.html": 251,
    "stats.html": 999,
    "storage.html": 885,
    "welcome.html": 374,
    "workspace.html": 375,
}

_SCRIPT_RE = re.compile(r"<script\b([^>]*)>(.*?)</script>", re.S | re.I)


def _inline_script_lines(template):
    lines = 0
    for attrs, body in _SCRIPT_RE.findall(template.read_text(encoding="utf-8")):
        if "src=" not in attrs:
            lines += sum(1 for line in body.split("\n") if line.strip())
    return lines


def test_inline_page_script_only_shrinks():
    actual = {
        str(path.relative_to(TEMPLATES)): _inline_script_lines(path)
        for path in sorted(TEMPLATES.rglob("*.html"))
    }
    grown = {
        name: (lines, INLINE_SCRIPT_LINE_LIMITS.get(name, 0))
        for name, lines in actual.items()
        if lines > INLINE_SCRIPT_LINE_LIMITS.get(name, 0)
    }
    assert not grown, (
        "Inline <script> grew past its limit (lines, limit): "
        f"{grown}. Put page code in a classic script under vireo/static/ "
        "and load it with <script src> instead."
    )
    shrunk = {
        name: (actual.get(name, 0), limit)
        for name, limit in INLINE_SCRIPT_LINE_LIMITS.items()
        if actual.get(name, 0) < limit
    }
    assert not shrunk, (
        "Inline <script> shrank below its limit (lines, limit): "
        f"{shrunk}. Lower INLINE_SCRIPT_LINE_LIMITS in "
        "vireo/tests/test_inline_page_scripts.py to match (delete entries at "
        "zero) so the move sticks."
    )


# Pages split into a per-page directory under vireo/static/, keyed by route.
# Browse has its own check in test_browse_pure_helpers.py.
SPLIT_PAGES = {
    "/settings": "settings",
    "/import": "import",
    "/life-list": "life-list",
    "/pipeline": "pipeline",
    "/pipeline/review": "pipeline-review",
    "/edit": "photo-editor",
    "/locations/review": "location-review",
    "/jobs": "jobs",
    "/review": "review",
}


@pytest.mark.parametrize("route, directory", sorted(SPLIT_PAGES.items()))
def test_split_page_loads_each_script_once_boot_last(app_and_db, route, directory):
    """Every file in the page's directory is loaded exactly once, and boot.js
    last: earlier files only declare, and boot starts the page, so a file
    listed after boot (or not at all) would be missing when it runs."""
    app, _ = app_and_db
    html = app.test_client().get(route).get_data(as_text=True)
    loaded = re.findall(
        rf'<script src="/static/{re.escape(directory)}/([\w.-]+\.js)"></script>', html
    )
    on_disk = sorted(p.name for p in (STATIC / directory).glob("*.js"))
    assert sorted(loaded) == on_disk
    assert loaded[-1] == "boot.js"
