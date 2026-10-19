"""The page-hook contract between the shared navbar/lightbox and pages.

Shared code reaches page functions only through ``Vireo.pageHook(name)``, and
pages hand them over with ``Vireo.registerPageHooks({...})``
(``vireo/static/vireo-page-hooks.js``). These tests hold that contract:

- the registry's ``PAGE_HOOK_NAMES`` and ``types/page-hooks.d.ts`` list the
  same hooks;
- every name shared code asks for or a page registers is one of them, and
  every hook is both registered by some page and read by shared code;
- the navbar and lightbox no longer probe page globals by name
  (``typeof getActiveSelection``, ``window.afterHistoryChange``), and no
  page publishes a hook as a ``window`` global.

Which page registers which hooks is checked in a real browser by
``tests/e2e/test_page_hooks.py``.
"""
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
STATIC = REPO / "vireo" / "static"
TEMPLATES = REPO / "vireo" / "templates"
REGISTRY = STATIC / "vireo-page-hooks.js"
HOOK_TYPES = REPO / "types" / "page-hooks.d.ts"


def _registry_names():
    source = REGISTRY.read_text(encoding="utf-8")
    block = re.search(r"PAGE_HOOK_NAMES = Object\.freeze\([^[]*\[(.*?)\]\)", source, re.S)
    assert block, "vireo-page-hooks.js must define PAGE_HOOK_NAMES"
    return re.findall(r"'(\w+)'", block.group(1))


def _typed_names():
    source = HOOK_TYPES.read_text(encoding="utf-8")
    body = re.search(r"interface VireoPageHooks \{(.*?)\n\}", source, re.S)
    assert body, "types/page-hooks.d.ts must declare interface VireoPageHooks"
    return re.findall(r"^\s*(\w+)\?\(", body.group(1), re.M)


def _sources():
    """Every first-party script and template, as (path, text)."""
    for path in sorted(STATIC.rglob("*.js")):
        if "vendor" in path.parts or path.name.endswith(".min.js"):
            continue
        yield path, path.read_text(encoding="utf-8")
    for path in sorted(TEMPLATES.rglob("*.html")):
        yield path, path.read_text(encoding="utf-8")


def _shared_scripts():
    """The shared navbar and lightbox scripts, which every page loads, and the
    shared export-modal controllers that call back into their pages."""
    return (sorted(STATIC.glob("navbar-*.js")) + sorted((STATIC / "lightbox").glob("*.js"))
            + [STATIC / "vireo-export-presets.js", STATIC / "vireo-export-collisions.js",
               STATIC / "vireo-release-originals.js"])


def _registered_names(text):
    """The keys of each ``registerPageHooks({...})`` object literal in text."""
    names = []
    for call in re.finditer(r"registerPageHooks\(\{", text):
        body = _balanced_body(text, call.end() - 1)
        names += re.findall(r"(?:^|,)\s*(\w+)\s*:", body)
    return names


def _balanced_body(text, start):
    """The text inside the brace at ``start`` and its match, with nested
    brackets, strings and comments blanked so only top-level keys remain."""
    out, depth, i = [], 0, start
    while i < len(text):
        char = text[i]
        if char in "'\"`":
            end = i + 1
            while end < len(text) and text[end] != char:
                end += 2 if text[end] == "\\" else 1
            i = end + 1
            continue
        if text.startswith("//", i):
            i = text.index("\n", i)
            continue
        if text.startswith("/*", i):
            i = text.index("*/", i) + 2
            continue
        if char in "({[":
            depth += 1
        elif char in ")}]":
            depth -= 1
            if depth == 0:
                return "".join(out)
        elif depth == 1:
            out.append(char)
        i += 1
    raise AssertionError("unbalanced registerPageHooks call")


def test_registry_and_types_list_the_same_hooks():
    names = _registry_names()
    assert names, "no page hooks listed"
    assert len(names) == len(set(names)), "a hook is listed twice"
    assert sorted(names) == sorted(_typed_names())


def test_every_hook_is_registered_by_a_page_and_read_by_shared_code():
    known = set(_registry_names())
    registered, read = set(), set()
    for path, text in _sources():
        if path == REGISTRY:
            continue
        for name in _registered_names(text):
            assert name in known, f"{path.name} registers unknown page hook {name!r}"
            registered.add(name)
        for name in re.findall(r"pageHook\('(\w+)'\)", text):
            assert name in known, f"{path.name} asks for unknown page hook {name!r}"
            read.add(name)
    assert known - registered == set(), "hooks no page registers"
    assert known - read == set(), "hooks shared code never calls"


def test_shared_code_does_not_probe_page_globals():
    """A hook name in shared code is either a pageHook('...') string or a local
    variable holding the result, never a global the page is expected to own."""
    names = _registry_names()
    probe = re.compile(
        r"typeof\s+(?:window\.)?(" + "|".join(names) + r")\b"
        r"|window\.(" + "|".join(names) + r")\b"
    )
    offenders = []
    for path in _shared_scripts():
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if probe.search(line):
                offenders.append(f"{path.relative_to(STATIC)}:{number}: {line.strip()}")
    assert not offenders, "\n".join(offenders)


def test_no_page_publishes_a_hook_as_a_window_global():
    names = _registry_names()
    write = re.compile(r"\bwindow\.(" + "|".join(names) + r")\s*=(?!=)")
    offenders = [
        f"{path.relative_to(REPO)}: window.{match.group(1)}"
        for path, text in _sources()
        for match in write.finditer(text)
    ]
    assert not offenders, offenders


def test_registry_loads_before_any_page_script():
    """Classic pages register while they parse, so the registry must be a
    classic script that _navbar.html loads ahead of them."""
    navbar = (TEMPLATES / "_navbar.html").read_text(encoding="utf-8")
    scripts = re.findall(r'<script[^>]*src="/static/([\w./-]+\.js)"', navbar)
    assert "vireo-page-hooks.js" in scripts
    assert scripts.index("vireo-page-hooks.js") < 3, scripts[:4]
    assert 'type="module" src="/static/vireo-page-hooks.js"' not in navbar


def test_registered_names_parser_reads_only_top_level_keys():
    text = (
        "Vireo.registerPageHooks({\n"
        "  getPhotos: function() { return {a: 1}; },\n"
        "  // note: not a key\n"
        "  afterHistoryChange: async function(result) {\n"
        "    safeFetch('/x', {b: 2}, {toast: false});\n"
        "    showToast('a: } b', 'info');\n"
        "  },\n"
        "  loadMisses: loadMisses,\n"
        "});"
    )
    assert _registered_names(text) == ["getPhotos", "afterHistoryChange", "loadMisses"]
