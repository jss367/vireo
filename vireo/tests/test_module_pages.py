"""Module pages: pages whose JavaScript is ES modules, not shared globals.

``frontend-module-pages.json`` lists them. A module page's template loads one
entry module, ``vireo/static/<page>/boot.js``, with ``<script type="module">``;
the browser loads the rest of the page through ``import``. Its files share
state through exported bindings (``state.js``), not globals, and bind events
with ``addEventListener``, not inline ``on*=`` handlers.

These tests hold the template side of that contract. ESLint holds the file
side (``eslint.config.mjs`` reads the same list): module parsing,
``no-undef`` so shared navbar globals are reached only through the page's
``shared.js``, no inline handler markup or ``on*`` properties, and no
``window`` writes except the shared latches the list names (``sharedWrites``),
made from ``shared.js``. The functions the navbar and lightbox call on the
page are registered with ``Vireo.registerPageHooks`` (``pageHooks``), not
published on ``window``.

Converting a page: move its route out of ``SPLIT_PAGES`` in
``test_inline_page_scripts.py`` and add it here (see CLAUDE.md and
``vireo/static/misses/README.md``).
"""
import re
from pathlib import Path

import pytest
from page_scripts import PageMarkup, module_graph, module_imports, module_pages, static_scripts

TEMPLATES = Path(__file__).resolve().parents[1] / "templates"
STATIC = Path(__file__).resolve().parents[1] / "static"

PAGES = sorted(module_pages().items())
PAGE_IDS = [page for page, _ in PAGES]


def test_module_page_list_is_not_empty():
    assert PAGES, "frontend-module-pages.json lists no module pages"


@pytest.mark.parametrize("page, contract", PAGES, ids=PAGE_IDS)
def test_module_page_contract_names_its_route_template_and_hooks(page, contract):
    assert set(contract) == {"route", "template", "pageHooks", "sharedWrites"}
    assert (TEMPLATES / contract["template"]).is_file()
    assert (STATIC / page / "boot.js").is_file()
    assert (STATIC / page / "shared.js").is_file()
    assert contract["pageHooks"] == sorted(set(contract["pageHooks"]))
    assert contract["sharedWrites"] == sorted(set(contract["sharedWrites"]))


@pytest.mark.parametrize("page, contract", PAGES, ids=PAGE_IDS)
def test_module_page_loads_only_its_boot_module(app_and_db, page, contract):
    """The rendered page loads exactly one of the page's files, boot.js, as a
    module. A classic page script would put its declarations back on window."""
    app, _ = app_and_db
    html = app.test_client().get(contract["route"]).get_data(as_text=True)
    page_scripts = [
        (attrs, src) for attrs, src in static_scripts(html)
        if src.startswith(page + "/")
    ]
    assert len(page_scripts) == 1, page_scripts
    attrs, src = page_scripts[0]
    assert src == f"{page}/boot.js" and attrs.get("type") == "module", page_scripts


@pytest.mark.parametrize("page, contract", PAGES, ids=PAGE_IDS)
def test_no_template_loads_module_page_files_as_classic_scripts(page, contract):
    offenders = []
    for template in sorted(TEMPLATES.rglob("*.html")):
        for attrs, src in static_scripts(template.read_text(encoding="utf-8")):
            if src.startswith(page + "/") and attrs.get("type") != "module":
                offenders.append(f"{template.name}: {src}")
    assert not offenders, offenders


@pytest.mark.parametrize("page, contract", PAGES, ids=PAGE_IDS)
def test_module_page_template_has_no_inline_script_or_handlers(page, contract):
    text = (TEMPLATES / contract["template"]).read_text(encoding="utf-8")
    markup = PageMarkup(text)
    inline = [body for attrs, body in markup.scripts if "src" not in attrs and body.strip()]
    assert not inline, f"{contract['template']} has inline <script>; put it in a module"
    handlers = markup.handlers
    assert not handlers, (
        f"{contract['template']} has inline on*= handlers: {handlers}. Bind them "
        "with addEventListener in the page's boot.js."
    )


@pytest.mark.parametrize("page, contract", PAGES, ids=PAGE_IDS)
def test_every_page_module_is_reached_from_boot(page, contract):
    """boot.js is the entry: each file in the directory is loaded through its
    imports (a file nothing imports never runs), and nothing imports boot.js."""
    directory = (STATIC / page).resolve()
    on_disk = sorted(p.resolve() for p in directory.glob("*.js"))
    reached = module_graph(directory / "boot.js")
    assert sorted(reached) == on_disk, (
        "unreached: " + str(sorted(p.name for p in set(on_disk) - set(reached)))
    )
    assert reached[-1] == directory / "boot.js"


@pytest.mark.parametrize("page, contract", PAGES, ids=PAGE_IDS)
def test_page_modules_import_only_sibling_files(page, contract):
    """Relative ``./name.js`` imports within the page directory: shared code
    is reached through the page's shared.js, not imported from elsewhere."""
    directory = STATIC / page
    for path in sorted(directory.glob("*.js")):
        for specifier in module_imports(path):
            assert re.fullmatch(r"\./[\w.-]+\.js", specifier), (path.name, specifier)
            assert (directory / specifier).is_file(), (path.name, specifier)
            assert specifier != "./boot.js", f"{path.name} imports the entry module"


@pytest.mark.parametrize("page, contract", PAGES, ids=PAGE_IDS)
def test_only_shared_js_writes_window_and_only_its_latches(page, contract):
    """The window writes ESLint allows in shared.js are exactly the listed
    latches: a stale entry would silently allow a new global."""
    shared = (STATIC / page / "shared.js").read_text(encoding="utf-8")
    written = set(re.findall(r"\bwindow\.([A-Za-z_$][\w$]*)\s*=(?!=)", shared))
    assert written == set(contract["sharedWrites"])
    for path in sorted((STATIC / page).glob("*.js")):
        if path.name != "shared.js":
            text = path.read_text(encoding="utf-8")
            assert not re.search(r"\bwindow\.[A-Za-z_$][\w$]*\s*=(?!=)", text), path.name


@pytest.mark.parametrize("page, contract", PAGES, ids=PAGE_IDS)
def test_page_registers_exactly_its_listed_hooks(page, contract):
    """``pageHooks`` is what the page hands the navbar and lightbox through
    Vireo.registerPageHooks (vireo-page-hooks.js), each a known hook name."""
    from test_page_hooks import _registered_names, _registry_names

    registered = []
    for path in sorted((STATIC / page).glob("*.js")):
        registered += _registered_names(path.read_text(encoding="utf-8"))
    assert sorted(registered) == contract["pageHooks"]
    assert set(registered) <= set(_registry_names())


@pytest.mark.parametrize("page, contract", PAGES, ids=PAGE_IDS)
def test_shared_writes_belong_to_shared_scripts(page, contract):
    """``sharedWrites`` are globals a shared script owns and the page resets
    (e.g. the lightbox's click-to-zoom latch), not new page globals. The one
    other kind is a callback a third-party script calls by the name the page
    passes in its URL (``&callback=_gmapsCallback`` for Google Maps)."""
    shared_sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(STATIC.rglob("*.js"))
        if "vendor" not in path.parts and path.parent.name not in module_pages()
    )
    page_sources = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted((STATIC / page).glob("*.js"))
    )
    url_callbacks = set(re.findall(r"[?&]callback=([A-Za-z_$][\w$]*)", page_sources))
    for name in contract["sharedWrites"]:
        assert name in url_callbacks or re.search(
            rf"\bwindow\.{re.escape(name)}\b", shared_sources
        ), name


@pytest.mark.parametrize("text", [
    '<script src="/static/misses/boot.js"></script>',
    "<SCRIPT SRC='/static/misses/boot.js'></SCRIPT>",
    '<script defer src=/static/misses/boot.js></script>',
    '<script data-x=">" SRC=/static/misses/boot.js?v=1#entry></script>',
])
def test_static_script_guard_accepts_html_attribute_forms(text):
    [(attrs, src)] = static_scripts(text)
    assert src == "misses/boot.js"
    assert attrs.get("type") != "module"


def test_markup_guard_handles_module_and_inline_attributes():
    text = "<script SRC='/static/misses/boot.js' TYPE=module></script>"
    [(attrs, _)] = static_scripts(text)
    assert attrs["type"] == "module"
    markup = PageMarkup('<button OnClick="go()" ONDBLCLICK=x></button>'
                        '<script>const text = "<b onclick=x>";</script>'
                        '<!-- <button onclick=x> -->')
    assert markup.handlers == ["onclick", "ondblclick"]
    assert markup.scripts == [({}, 'const text = "<b onclick=x>";')]
    assert PageMarkup('<script src=x SRC=y></script>').scripts == [({"src": "x"}, "")]
