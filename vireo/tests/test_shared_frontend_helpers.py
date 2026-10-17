"""Shared browser helpers have one definition: ``vireo/static/vireo-utils.js``.

``_navbar.html`` loads ``vireo-utils.js`` before every other script, so each
page already has ``escapeHtml``, ``escapeAttr`` and ``formatBytes`` in scope.
Pages used to carry their own copies (eight HTML escapers, eight byte
formatters, each slightly different: one escaped no quotes, one rendered
zero as ``0``). These tests keep the consolidation from sliding back, and
hold the helpers that are still copied per page to a count that may only
shrink.

Deadline: the entries in ``REMAINING_COPIES`` are gone by 2026-12-31
(``docs/plans/2026-10-16-frontend-schema-consolidation-plan.md``).
"""
import re
from pathlib import Path

STATIC = Path(__file__).resolve().parents[1] / "static"
TEMPLATES = Path(__file__).resolve().parents[1] / "templates"
SHARED_SOURCE = "static/vireo-utils.js"

# Helpers every page gets from vireo-utils.js. Nothing else may define them,
# under this name or an alias that used to stand in for them.
SHARED_HELPERS = {
    "escapeHtml": {"escapeHtml", "escapeAttr", "esc", "escapeHTML", "htmlEscape"},
    "formatBytes": {"formatBytes", "formatBytesNav", "formatSize", "humanBytes", "fmtBytes"},
}

# Helpers still defined once per page: definitions on disk today. Lower an
# entry when you move one into vireo-utils.js (delete it at one); never raise
# one: use the shared definition instead of adding a copy.
REMAINING_COPIES = {
    "photoThumbnailUrl": 4,
    "timeAgo": 2,
}

_DECLARATION_RE = re.compile(
    r"^\s*(?:/\*\*[^\n]*?\*/\s*)?(?:async\s+)?function\s+([A-Za-z_$][\w$]*)\s*\("
    r"|^\s*(?:/\*\*[^\n]*?\*/\s*)?(?:var|let|const)\s+([A-Za-z_$][\w$]*)\s*=\s*(?:async\s+)?(?:function\b|\([^)]*\)\s*=>)",
    re.M,
)
_SCRIPT_RE = re.compile(r"<script\b([^>]*)>(.*?)</script>", re.S | re.I)


def _frontend_sources():
    for path in sorted(STATIC.rglob("*.js")):
        if "vendor" in path.parts or path.name.endswith(".min.js"):
            continue
        yield path.relative_to(STATIC.parent).as_posix(), path.read_text(encoding="utf-8")
    for path in sorted(TEMPLATES.rglob("*.html")):
        text = path.read_text(encoding="utf-8")
        inline = "\n".join(
            body for attrs, body in _SCRIPT_RE.findall(text) if "src=" not in attrs
        )
        yield path.relative_to(TEMPLATES.parent).as_posix(), inline


def _declarations():
    """``{name: [source, ...]}`` for every top-level-looking declaration."""
    found = {}
    for source, text in _frontend_sources():
        for match in _DECLARATION_RE.finditer(text):
            name = match.group(1) or match.group(2)
            found.setdefault(name, []).append(source)
    return found


def test_shared_helpers_are_defined_only_in_vireo_utils():
    declared = _declarations()
    for canonical, names in SHARED_HELPERS.items():
        assert declared.get(canonical) == [SHARED_SOURCE], (
            f"{canonical} must be defined exactly once, in {SHARED_SOURCE}; "
            f"found {declared.get(canonical)}"
        )
        copies = {
            name: sources
            for name in names - {canonical}
            for sources in [declared.get(name, [])]
            if sources and sources != [SHARED_SOURCE]
        }
        assert not copies, (
            f"Page-local copies of {canonical} (name: files): {copies}. "
            f"Call the shared {canonical} from {SHARED_SOURCE} instead; "
            "_navbar.html loads it before every page script."
        )


def test_remaining_helper_copies_only_shrink():
    declared = _declarations()
    actual = {name: len(declared.get(name, [])) for name in REMAINING_COPIES}
    grown = {
        name: (count, REMAINING_COPIES[name])
        for name, count in actual.items()
        if count > REMAINING_COPIES[name]
    }
    assert not grown, (
        f"A per-page helper gained a copy (count, limit): {grown}. Move it to "
        f"{SHARED_SOURCE} and call that instead."
    )
    shrunk = {
        name: (count, REMAINING_COPIES[name])
        for name, count in actual.items()
        if count < REMAINING_COPIES[name]
    }
    assert not shrunk, (
        f"A per-page helper lost a copy (count, limit): {shrunk}. Lower "
        "REMAINING_COPIES in vireo/tests/test_shared_frontend_helpers.py "
        "(delete the entry at one) so the move sticks."
    )


def test_navbar_loads_shared_helpers_before_every_other_script():
    """``vireo-utils.js`` is the first script after ``vireo-api.js``: navbar
    scripts and page scripts alike may call the shared helpers at load."""
    srcs = re.findall(r'<script src="/static/([^"]+)"></script>',
                      (TEMPLATES / "_navbar.html").read_text(encoding="utf-8"))
    assert srcs[:2] == ["vireo-api.js", "vireo-utils.js"]
    assert srcs.count("vireo-utils.js") == 1


def test_navbar_persisted_controls_are_restored_after_they_are_parsed():
    """``vireo-utils.js`` restores ``[data-view-preference]`` controls when it
    loads, which on a navbar page is before any navbar markup exists. The
    navbar's own persisted controls are therefore restored by the first
    navbar script after its markup."""
    html = (TEMPLATES / "_navbar.html").read_text(encoding="utf-8")
    last_control = max(m.end() for m in re.finditer(r"data-view-preference=", html))
    scripts_after = re.findall(
        r'<script src="/static/([^"]+)"></script>', html[last_control:]
    )
    restorers = [
        name for name in scripts_after
        if "VireoViewPreferences.restoreAll(document)" in
        (STATIC / name).read_text(encoding="utf-8")
    ]
    assert restorers, (
        "no navbar script after the navbar's persisted controls restores them; "
        "navbar-shared-helpers.js used to"
    )
