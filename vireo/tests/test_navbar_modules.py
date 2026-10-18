"""The shared navbar and lightbox are ES modules.

``_navbar.html`` loads one entry module, ``vireo/static/navbar-boot.js``,
which imports every ``navbar-*.js`` and ``lightbox/*.js`` module. The markup
carries no inline ``on*=`` handlers: each control names its handlers in a
``data-navbar-on="event:name"`` attribute that ``navbar-markup.js`` binds.
``navbar-globals.js`` is the one navbar module that writes ``window``: it
publishes the functions classic pages still call by global name. Classic page
scripts load after the navbar with ``defer``, so they run after the navbar's
deferred module entry, as they ran after its classic scripts before.

ESLint (``eslint.config.mjs``) holds the module files themselves: module
parsing, ``no-undef``/``no-unused-vars``, and no window writes or inline
handler markup outside the bridge.
"""
import re
from pathlib import Path

from page_scripts import PageMarkup, module_graph, static_scripts

REPO = Path(__file__).resolve().parents[2]
STATIC = REPO / "vireo" / "static"
TEMPLATES = REPO / "vireo" / "templates"
ENTRY = STATIC / "navbar-boot.js"
BRIDGE = STATIC / "navbar-globals.js"
MARKUP = STATIC / "navbar-markup.js"


def _navbar_modules():
    return sorted(STATIC.glob("navbar-*.js")) + sorted((STATIC / "lightbox").glob("*.js"))


def test_navbar_loads_one_module_entry_and_no_inline_script_or_handlers():
    text = (TEMPLATES / "_navbar.html").read_text(encoding="utf-8")
    markup = PageMarkup(text)
    assert not [body for attrs, body in markup.scripts if "src" not in attrs and body.strip()]
    assert markup.handlers == [], "bind navbar controls with data-navbar-on, not on*= attributes"
    modules = [src for attrs, src in static_scripts(text) if attrs.get("type") == "module"]
    assert modules == ["navbar-boot.js"]


def test_no_template_loads_a_navbar_module_as_a_classic_script():
    offenders = []
    for template in sorted(TEMPLATES.rglob("*.html")):
        for attrs, src in static_scripts(template.read_text(encoding="utf-8")):
            if re.fullmatch(r"(navbar-[\w-]+|lightbox/[\w-]+)\.js", src) and attrs.get("type") != "module":
                offenders.append(f"{template.name}: {src}")
    assert not offenders, offenders


def test_every_navbar_module_is_reached_from_the_entry():
    reached = set(module_graph(ENTRY))
    unreached = [p.relative_to(STATIC) for p in _navbar_modules() if p.resolve() not in reached]
    assert not unreached, f"not imported from navbar-boot.js: {unreached}"


def test_markup_handlers_match_the_handler_table():
    html = (TEMPLATES / "_navbar.html").read_text(encoding="utf-8")
    used = set()
    for spec in re.findall(r'data-navbar-on="([^"]+)"', html):
        for pair in spec.split():
            event, _, name = pair.partition(":")
            assert re.fullmatch(r"[a-z]+", event) and name, pair
            used.add(name)
    source = MARKUP.read_text(encoding="utf-8")
    table = source[source.index("const HANDLERS = {"):source.index("\n};", source.index("const HANDLERS = {"))]
    defined = set(re.findall(r"^  (\w+): function", table, re.M))
    assert used - defined == set(), "handlers the markup names but navbar-markup.js lacks"
    assert defined - used == set(), "navbar-markup.js handlers no control uses"


def test_only_the_bridge_writes_window():
    write = re.compile(
        r"\b(?:window|globalThis)\.[A-Za-z_$][\w$]*\s*=(?!=)"
        r"|Object\.(?:assign|defineProperty|defineProperties)\(\s*(?:window|globalThis)\b"
    )
    offenders = []
    for path in _navbar_modules():
        if path == BRIDGE:
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if write.search(line) and not line.lstrip().startswith(("//", "*", "/*")):
                offenders.append(f"{path.relative_to(STATIC)}:{number}: {line.strip()}")
    assert not offenders, "\n".join(offenders)


def _published_names():
    source = BRIDGE.read_text(encoding="utf-8")
    names = re.findall(r"^  (\w+): \w+,$", source, re.M)
    names += re.findall(r"publish(?:ReadOnly|ReadWrite)\('(\w+)'", source)
    return names


def test_bridge_publishes_only_names_classic_code_still_uses():
    """A global no classic page, template or classic shared script uses any
    more comes off the bridge."""
    names = _published_names()
    assert len(names) == len(set(names))
    consumers = []
    for path in sorted(STATIC.rglob("*.js")):
        rel = path.relative_to(STATIC).as_posix()
        if "vendor/" in rel or re.match(r"(navbar-[\w-]+|lightbox/[\w-]+)\.js$", rel):
            continue
        consumers.append(path.read_text(encoding="utf-8"))
    for path in sorted(TEMPLATES.rglob("*.html")):
        if path.name != "_navbar.html":
            consumers.append(path.read_text(encoding="utf-8"))
    corpus = "\n".join(consumers)
    unused = [name for name in names if not re.search(rf"(?<![\w$]){re.escape(name)}(?![\w$])", corpus)]
    assert not unused, f"published but unused outside the navbar: {unused}"


# Page scripts an inline script needs while the page parses. They must not
# use the navbar at load, since they run before its deferred module entry.
UNDEFERRED = {
    "card_cleanup.html": {"/static/vireo-folder-browser.js"},
}


def test_classic_page_scripts_after_the_navbar_are_deferred():
    offenders = []
    for template in sorted(TEMPLATES.glob("*.html")):
        text = template.read_text(encoding="utf-8")
        if template.name in ("_filterbar.html", "_sync_panel.html"):
            after = text
        else:
            include = re.search(r"\{%\s*include\s+'_navbar\.html'\s*%\}", text)
            if not include:
                continue
            after = text[include.end():]
        for attrs, _ in PageMarkup(after).scripts:
            src = attrs.get("src")
            if not src or attrs.get("type") == "module" or "defer" in attrs:
                continue
            if src not in UNDEFERRED.get(template.name, set()):
                offenders.append(f"{template.name}: {src}")
    assert not offenders, offenders
