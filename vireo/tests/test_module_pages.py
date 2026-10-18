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
``window`` writes except the page hooks and shared latches the list names,
made from ``shared.js``.

Converting a page: move its route out of ``SPLIT_PAGES`` in
``test_inline_page_scripts.py`` and add it here (see CLAUDE.md and
``vireo/static/misses/README.md``).
"""
import re
from pathlib import Path

import pytest
from page_scripts import module_graph, module_imports, module_pages

TEMPLATES = Path(__file__).resolve().parents[1] / "templates"
STATIC = Path(__file__).resolve().parents[1] / "static"
STATIC_SCRIPT_RE = re.compile(r'<script\b([^>]*)\bsrc="/static/([\w./-]+\.js)"[^>]*>', re.I)
INLINE_SCRIPT_RE = re.compile(r"<script\b([^>]*)>(.*?)</script>", re.S | re.I)
# An on<event>= attribute inside a tag (not text such as "Show only").
INLINE_HANDLER_RE = re.compile(r"<[a-zA-Z][^>]*?\son[a-z]+\s*=", re.S)

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
        (attrs, src) for attrs, src in STATIC_SCRIPT_RE.findall(html)
        if src.startswith(page + "/")
    ]
    assert page_scripts == [(' type="module" ', f"{page}/boot.js")], page_scripts


@pytest.mark.parametrize("page, contract", PAGES, ids=PAGE_IDS)
def test_no_template_loads_module_page_files_as_classic_scripts(page, contract):
    offenders = []
    for template in sorted(TEMPLATES.rglob("*.html")):
        for attrs, src in STATIC_SCRIPT_RE.findall(template.read_text(encoding="utf-8")):
            if src.startswith(page + "/") and 'type="module"' not in attrs:
                offenders.append(f"{template.name}: {src}")
    assert not offenders, offenders


@pytest.mark.parametrize("page, contract", PAGES, ids=PAGE_IDS)
def test_module_page_template_has_no_inline_script_or_handlers(page, contract):
    text = (TEMPLATES / contract["template"]).read_text(encoding="utf-8")
    inline = [body for attrs, body in INLINE_SCRIPT_RE.findall(text)
              if "src=" not in attrs and body.strip()]
    assert not inline, f"{contract['template']} has inline <script>; put it in a module"
    handlers = INLINE_HANDLER_RE.findall(text)
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
def test_page_hooks_are_exposed_only_by_shared_js(page, contract):
    """The window writes ESLint allows in shared.js are exactly the listed
    hooks and latches: a stale entry would silently allow a new global."""
    shared = (STATIC / page / "shared.js").read_text(encoding="utf-8")
    written = set(re.findall(r"\bwindow\.([A-Za-z_$][\w$]*)\s*=(?!=)", shared))
    assert written == set(contract["pageHooks"]) | set(contract["sharedWrites"])
    for path in sorted((STATIC / page).glob("*.js")):
        if path.name != "shared.js":
            text = path.read_text(encoding="utf-8")
            assert not re.search(r"\bwindow\.[A-Za-z_$][\w$]*\s*=(?!=)", text), path.name


@pytest.mark.parametrize("page, contract", PAGES, ids=PAGE_IDS)
def test_shared_writes_belong_to_shared_scripts(page, contract):
    """``sharedWrites`` are globals a shared script owns and the page resets
    (e.g. the lightbox's click-to-zoom latch), not new page globals."""
    shared_sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(STATIC.rglob("*.js"))
        if "vendor" not in path.parts and path.parent.name not in module_pages()
    )
    for name in contract["sharedWrites"]:
        assert re.search(rf"\bwindow\.{re.escape(name)}\b", shared_sources), name
