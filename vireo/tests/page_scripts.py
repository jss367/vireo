"""Read a rendered page the way the browser assembles it.

Page and navbar JS live in ``vireo/static/`` and are loaded with
``<script src>``, so a test that checks what a page does must follow those
tags rather than grep the bare template response.
"""

import re
from pathlib import Path

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
# Paths may include a subdirectory: Browse's scripts live in static/browse/.
_STATIC_SCRIPT_RE = re.compile(r'<script src="/static/([\w./-]+\.js)"></script>')


def page_with_scripts(client, path):
    """The rendered page with its ``/static`` scripts inlined."""
    html = client.get(path).get_data(as_text=True)
    return _STATIC_SCRIPT_RE.sub(
        lambda m: "<script>\n"
        + (STATIC_DIR / m.group(1)).read_text(encoding="utf-8")
        + "</script>",
        html,
    )
