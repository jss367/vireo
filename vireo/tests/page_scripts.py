"""Read a rendered page the way the browser assembles it.

Page and navbar JS live in ``vireo/static/`` and are loaded with
``<script src>``, so a test that checks what a page does must follow those
tags rather than grep the bare template response.

A module page (``frontend-module-pages.json``) loads one entry module with
``<script type="module" src>``, and the browser loads the rest of the page
through its ``import`` statements; ``page_with_scripts`` follows those too,
inlining every module the entry reaches, each once.
"""

import json
import re
from pathlib import Path

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
MODULE_PAGES_PATH = STATIC_DIR.parents[1] / "frontend-module-pages.json"
# Paths may include a subdirectory: Browse's scripts live in static/browse/.
_STATIC_SCRIPT_RE = re.compile(
    r'<script( type="module")? src="/static/([\w./-]+\.js)"></script>'
)
# Static imports and re-exports. Module pages import only relative paths
# (``./state.js``), which test_module_pages.py checks.
_IMPORT_RE = re.compile(
    r"^\s*(?:import|export)\b[^'\";]*?\bfrom\s*['\"]([^'\"]+)['\"]"
    r"|^\s*import\s*['\"]([^'\"]+)['\"]",
    re.M,
)


def module_pages():
    """``{page: contract}`` from ``frontend-module-pages.json``."""
    return json.loads(MODULE_PAGES_PATH.read_text(encoding="utf-8"))


def module_imports(path):
    """The specifiers ``path`` imports or re-exports from, in source order."""
    return [
        m.group(1) or m.group(2)
        for m in _IMPORT_RE.finditer(path.read_text(encoding="utf-8"))
    ]


def module_graph(entry):
    """Every module ``entry`` reaches through relative imports, the entry
    included, in the order the browser evaluates them (dependencies first)."""
    ordered, seen = [], set()

    def visit(path):
        if path in seen:
            return
        seen.add(path)
        for specifier in module_imports(path):
            if specifier.startswith("."):
                visit((path.parent / specifier).resolve())
        ordered.append(path)

    visit(entry.resolve())
    return ordered


def _inline(match):
    is_module, src = match.group(1), match.group(2)
    if not is_module:
        return "<script>\n" + (STATIC_DIR / src).read_text(encoding="utf-8") + "</script>"
    return "".join(
        '<script type="module">\n' + path.read_text(encoding="utf-8") + "</script>"
        for path in module_graph(STATIC_DIR / src)
    )


def page_with_scripts(client, path):
    """The rendered page with its ``/static`` scripts inlined."""
    html = client.get(path).get_data(as_text=True)
    return _STATIC_SCRIPT_RE.sub(_inline, html)
