"""Browse's markup bindings and shared-code boundary (vireo/static/browse/).

Browse is a module page (``frontend-module-pages.json``; the general contract
is in ``test_module_pages.py``). Its markup carries no inline ``on*=``
handlers: ``browse.html``, the ``_panorama_dialog.html`` it includes, and the
markup its modules render name each control's handlers in a
``data-browse-on="event:name"`` attribute that ``markup.js`` binds. These
tests hold that table to the names the markup uses, and the comparison
overlay's shared markup (``_photo_compare.html``) to the controls
``VireoBrowseCompare.bindControls`` handles.
"""
import re
from pathlib import Path

from page_scripts import PageMarkup

REPO = Path(__file__).resolve().parents[2]
STATIC = REPO / "vireo" / "static"
TEMPLATES = REPO / "vireo" / "templates"
BROWSE = STATIC / "browse"
_SPEC = re.compile(r"data-browse-on=\\?[\"']([^\"'\\]+)\\?[\"']")


def _handler_table():
    source = (BROWSE / "markup.js").read_text(encoding="utf-8")
    start = source.index("const HANDLERS = {")
    table = source[start:source.index("\n};", start)]
    return set(re.findall(r"^  (\w+): function", table, re.M))


def _used_names():
    texts = [
        (TEMPLATES / "browse.html").read_text(encoding="utf-8"),
        (TEMPLATES / "_panorama_dialog.html").read_text(encoding="utf-8"),
    ]
    texts += [path.read_text(encoding="utf-8") for path in sorted(BROWSE.glob("*.js"))]
    used = set()
    for text in texts:
        for spec in _SPEC.findall(text):
            for pair in spec.split():
                event, _, name = pair.partition(":")
                assert re.fullmatch(r"[a-z]+", event) and name, pair
                used.add(name)
    return used


def test_markup_handlers_match_the_handler_table():
    defined = _handler_table()
    used = _used_names()
    assert used - defined == set(), "handlers the markup names but markup.js lacks"
    assert defined - used == set(), "markup.js handlers no control uses"


def test_browse_partials_carry_no_inline_handlers():
    """Browse-only partials bind through data-browse-on like the page."""
    for name in ("_panorama_dialog.html", "_photo_compare.html"):
        markup = PageMarkup((TEMPLATES / name).read_text(encoding="utf-8"))
        assert markup.handlers == [], name


def test_compare_overlay_controls_are_the_ones_bind_controls_handles():
    html = (TEMPLATES / "_photo_compare.html").read_text(encoding="utf-8")
    used = set(re.findall(r'data-compare-control="([^"]+)"', html))
    source = (STATIC / "vireo-browse-compare.js").read_text(encoding="utf-8")
    handled = set(re.findall(r"action === '([a-z]+)'", source)) | {"stop"}
    assert used == handled, (used, handled)
    # Both pages that include the overlay bind it.
    assert "VireoBrowseCompare.bindControls(" in (BROWSE / "boot.js").read_text(encoding="utf-8")
    assert "VireoBrowseCompare.bindControls(" in (STATIC / "cull-favorites.js").read_text(encoding="utf-8")


def test_shared_js_reads_globals_and_writes_only_the_listed_latches():
    """Every other Browse module reaches shared code through shared.js."""
    for path in sorted(BROWSE.glob("*.js")):
        if path.name == "shared.js":
            continue
        text = path.read_text(encoding="utf-8")
        code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith(("//", "*", "/*")))
        reads = set(re.findall(r"\bwindow\.([A-Za-z_$][\w$]*)", code))
        # Browser APIs only; shared app globals come through shared.js.
        assert reads <= {
            "location", "sessionStorage", "history", "innerWidth", "getComputedStyle",
            "addEventListener", "dispatchEvent", "confirm", "prompt", "IntersectionObserver",
            "ResizeObserver",
        }, (path.name, reads)
