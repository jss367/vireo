"""Vireo's layering holds for every import, function-local ones included.

``CLAUDE.md`` ("Architecture") describes the layers. This test holds them
against the real import graph: every production module under ``vireo/``
(outside ``tests/`` and ``testing/``) is parsed, and every ``import`` and
``from ... import`` in it, at module level or inside a function, method or
class body, becomes an edge to the first-party module it loads. Importing
``a.b`` runs ``a/__init__.py`` first, so the edge to the package ``a`` is
recorded too (unless the importer already lives inside ``a``). Imports under
``if TYPE_CHECKING:`` never run and are not edges. A function-local import is
still a dependency: it only defers the cycle to the first call.

The layers, lowest first. A module may import its own layer or any layer
below it; an import that points to a higher layer is an *upward edge*:

0. **foundation**: the root initializer (``vireo``) and ``FOUNDATION``, pure
   helpers with no disk, config, network or model I/O, which import nothing above foundation (``sql_chunks``,
   ``keyword_normalization``, ``duplicates``, ...).
1. **data**: ``db``, ``repositories/``, ``canonical_schema``, ``schema`` and
   the other ``DATA_MODULES``. They depend only on ``sqlite3``, the schema and
   foundation helpers. Global settings reach them through the
   ``settings_source`` port (``config`` installs itself there on import) and
   ``Database.effective_config()``, never by importing ``config``.
2. **application**: ``services/``, the job machinery (``jobs``,
   ``job_runner/``, ``import_job/``, ``classify_job/``), the pipeline
   (``pipeline*``, ``pipeline_stages/``) and every other top-level domain
   module (``config``, ``models``, ``classifier``, ``scanner/``, ...). These
   are peers: jobs and the pipeline call services and services start jobs,
   so the tier does not order them; the cycle rule below keeps them from
   tangling.
3. **web**: ``web/``, the blueprints and request plumbing.
4. **app**: ``app``, the composition root. Nothing imports it.

Two rules, each a two-way exact baseline in ``import_graph_baseline.json``
that may only shrink:

- ``upward_edges``: the upward edges that exist today, as ``"src -> dst"``.
  A new one fails. A removed one fails until the baseline is lowered.
- ``cycles``: the members of every import cycle (strongly connected
  component) today. A module joining a cycle, or two modules forming a new
  one, fails. A cycle that shrank or split fails until the baseline is
  lowered.

After removing an edge or breaking a cycle, run
``python vireo/tests/test_import_graph.py --write-baseline``. The writer
refuses to add an upward edge or to grow or add a cycle. A renamed or moved
module keeps its baseline entries only by hand-editing the JSON in a reviewed
change. No dynamic import (``importlib.import_module``) is analyzed.
"""

from __future__ import annotations

import ast
import json
import shutil
import subprocess
import sys
from collections.abc import Iterable, Iterator, Mapping
from functools import cache
from pathlib import Path

import pytest

VIREO = Path(__file__).resolve().parents[1]
BASELINE_PATH = Path(__file__).with_name("import_graph_baseline.json")
WRITER = "python vireo/tests/test_import_graph.py --write-baseline"

# Directories under vireo/ that are not production code.
SKIPPED_DIRS = {"tests", "testing"}
ROOT_PACKAGE = "vireo"

# Pure helpers: no disk, config, network or model I/O, and no first-party
# import above foundation. Add a module here only when that holds; the test
# reports any import it makes above foundation as an upward edge.
FOUNDATION = frozenset({
    "catalog_stamps",
    "compare",
    "config_schema",
    "detection_id",
    "duplicates",
    "edit_batch",
    "edit_recipe",
    "keyword_normalization",
    "process_strategies",
    "settings_source",
    "species_identity",
    "species_memberships",
    "sql_chunks",
})

# The data layer: the Database façade, its repositories, and the schema.
DATA_PACKAGES = ("repositories",)
DATA_MODULES = frozenset({
    "db",
    "canonical_schema",
    "schema",
    "photo_visibility_schema",
    "metadata_search",
    "new_images_cache",
    "catalog_result_files",
    "species_identity_repair",
})

WEB_PACKAGES = ("web",)
COMPOSITION_ROOTS = frozenset({"app"})

FOUNDATION_TIER, DATA_TIER, APPLICATION_TIER, WEB_TIER, APP_TIER = range(5)
TIER_NAMES = {
    FOUNDATION_TIER: "foundation",
    DATA_TIER: "data",
    APPLICATION_TIER: "application",
    WEB_TIER: "web",
    APP_TIER: "app",
}


def _in_package(module: str, packages: Iterable[str]) -> bool:
    return any(module == pkg or module.startswith(pkg + ".") for pkg in packages)


def tier(module: str) -> int:
    if module == ROOT_PACKAGE or module in FOUNDATION:
        return FOUNDATION_TIER
    if module in DATA_MODULES or _in_package(module, DATA_PACKAGES):
        return DATA_TIER
    if module in COMPOSITION_ROOTS:
        return APP_TIER
    if _in_package(module, WEB_PACKAGES):
        return WEB_TIER
    return APPLICATION_TIER


# ---------------------------------------------------------------- the graph


def _module_name(rel: Path) -> tuple[str, bool]:
    parts = list(rel.with_suffix("").parts)
    is_package = parts[-1] == "__init__"
    if is_package:
        parts = parts[:-1]
    return ".".join(parts) or ROOT_PACKAGE, is_package


def production_sources(root: Path = VIREO) -> dict[str, tuple[str, bool, str]]:
    """``{module: (source, is_package, path relative to root)}``."""
    modules = {}
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root)
        if rel.parts[0] in SKIPPED_DIRS:
            continue
        name, is_package = _module_name(rel)
        modules[name] = (path.read_text(encoding="utf-8"), is_package, rel.as_posix())
    return modules


def _is_type_checking(test: ast.expr) -> bool:
    return (
        (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING")
        or (isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING")
    )


def _runtime_imports(tree: ast.AST) -> Iterator[ast.Import | ast.ImportFrom]:
    """Every import statement that can run, at any depth."""
    stack = [tree]
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            yield node
            continue
        if isinstance(node, ast.If) and _is_type_checking(node.test):
            stack.extend(node.orelse)
            continue
        stack.extend(ast.iter_child_nodes(node))


def _ancestors(module: str) -> list[str]:
    parts = module.split(".")
    return [".".join(parts[:i]) for i in range(1, len(parts))]


def _normalize(dotted: str) -> str:
    """Share graph identities between qualified imports and legacy bare ones."""
    return dotted.removeprefix(ROOT_PACKAGE + ".")


def _resolve(dotted: str, modules: Mapping) -> str | None:
    """The longest prefix of ``dotted`` that is a first-party module."""
    parts = _normalize(dotted).split(".")
    for i in range(len(parts), 0, -1):
        candidate = ".".join(parts[:i])
        if candidate in modules:
            return candidate
    return None


def _explicit_targets(node, module: str, is_package: bool, modules: Mapping) -> list[str]:
    if isinstance(node, ast.Import):
        return [t for alias in node.names if (t := _resolve(alias.name, modules))]
    if node.level:
        # The root initializer has an explicit graph identity, while its
        # children retain the bare names used throughout the application.
        package = [] if module == ROOT_PACKAGE else (
            module.split(".") if is_package else module.split(".")[:-1]
        )
        package = package[:len(package) - (node.level - 1)] if node.level > 1 else package
        base = ".".join(package + ([node.module] if node.module else [])) or ROOT_PACKAGE
    else:
        base = node.module or ""
    targets = []
    for alias in node.names:
        full = _normalize(f"{base}.{alias.name}" if base else alias.name)
        target = full if full in modules else _resolve(base, modules) if base else None
        if target:
            targets.append(target)
    return targets


def import_edges(modules: Mapping[str, tuple[str, bool, str]]) -> dict[tuple[str, str], list[str]]:
    """``{(importer, imported): ["path:line", ...]}`` over first-party modules."""
    edges: dict[tuple[str, str], list[str]] = {}
    for module, (source, is_package, rel) in modules.items():
        own_packages = set(_ancestors(module)) | ({module} if is_package else set())
        for node in _runtime_imports(ast.parse(source, filename=rel)):
            for target in _explicit_targets(node, module, is_package, modules):
                implicit = [a for a in _ancestors(target) if a in modules and a not in own_packages]
                for imported in [*implicit, target]:
                    if imported != module:
                        edges.setdefault((module, imported), []).append(f"{rel}:{node.lineno}")
    return edges


def import_cycles(nodes: Iterable[str], edges: Iterable[tuple[str, str]]) -> list[list[str]]:
    """Strongly connected components with more than one module, sorted."""
    graph: dict[str, list[str]] = {node: [] for node in nodes}
    for src, dst in edges:
        graph.setdefault(src, []).append(dst)
        graph.setdefault(dst, [])
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    components = []
    for root in sorted(graph):
        if root in index:
            continue
        work = [(root, iter(sorted(graph[root])))]
        index[root] = low[root] = len(index)
        stack.append(root)
        on_stack.add(root)
        while work:
            node, children = work[-1]
            for child in children:
                if child not in index:
                    index[child] = low[child] = len(index)
                    stack.append(child)
                    on_stack.add(child)
                    work.append((child, iter(sorted(graph[child]))))
                    break
                if child in on_stack:
                    low[node] = min(low[node], index[child])
            else:
                work.pop()
                if work:
                    parent = work[-1][0]
                    low[parent] = min(low[parent], low[node])
                if low[node] == index[node]:
                    component = []
                    while True:
                        member = stack.pop()
                        on_stack.discard(member)
                        component.append(member)
                        if member == node:
                            break
                    if len(component) > 1:
                        components.append(sorted(component))
    return sorted(components, key=lambda c: (-len(c), c))


def upward_edges(edges: Mapping[tuple[str, str], list[str]]) -> dict[str, list[str]]:
    return {
        f"{src} -> {dst}": sites for (src, dst), sites in sorted(edges.items())
        if tier(dst) > tier(src)
    }


@cache
def current_graph(root: Path = VIREO):
    modules = production_sources(root)
    edges = import_edges(modules)
    return upward_edges(edges), import_cycles(modules, edges)


def load_baseline(path: Path = BASELINE_PATH) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------- advice


def _advice(edge: str) -> str:
    src, dst = edge.split(" -> ")
    src_tier, dst_tier = tier(src), tier(dst)
    if src == ROOT_PACKAGE:
        return (
            f"The root package initializer must not load {dst}. Keep __init__.py "
            "minimal; import domain code at its call site instead."
        )
    if src_tier == FOUNDATION_TIER:
        return (
            f"{src} is a foundation helper (FOUNDATION in this test) and may import only "
            f"other foundation modules. Move what it needs from {dst} into it (or into "
            "another foundation module), or have the caller pass the value in; if "
            f"{src} really needs {dst}, it is not a pure helper: remove it from FOUNDATION."
        )
    if src_tier == DATA_TIER:
        return (
            f"{src} is in the data layer, which depends only on sqlite3, the schema and "
            f"foundation helpers; {dst} is {TIER_NAMES[dst_tier]}. Have the caller pass in "
            "what is needed (the effective config value, a loaded taxonomy, a callback), "
            "move the method up into the service that calls it, or move a pure helper "
            "down into a foundation module (then add it to FOUNDATION)."
        )
    if src_tier == APPLICATION_TIER:
        return (
            f"{src} (services, jobs, pipeline and domain modules) must not import "
            f"{TIER_NAMES[dst_tier]} code ({dst}). Move the shared helper into services/ "
            "or a lower module and import it from both sides."
        )
    return (
        f"{src} must not import {dst}: blueprints get per-app state from their "
        "create_<domain>_blueprint factory (the injection convention), never from app."
    )


def _cycle_advice(modules: list[str]) -> str:
    return (
        f"Import cycle: {modules}. Break it rather than recording it: move the shared "
        "code into a lower module both sides import, or pass the dependency in from the "
        "caller. A function-local import does not break a cycle; it defers it to the "
        "first call."
    )


# ---------------------------------------------------------------- the ratchet


def test_no_new_upward_edges():
    baseline = set(load_baseline()["upward_edges"])
    current, _cycles = current_graph()
    new = {edge: sites for edge, sites in current.items() if edge not in baseline}
    assert not new, "New upward imports (lower layer importing a higher one):\n" + "\n".join(
        f"- {edge} at {', '.join(sites)}\n  {_advice(edge)}" for edge, sites in new.items()
    ) + (
        "\nDo not add these to vireo/tests/import_graph_baseline.json; the writer refuses."
    )


def test_removed_upward_edges_are_lowered_in_the_baseline():
    baseline = load_baseline()["upward_edges"]
    current, _cycles = current_graph()
    gone = [edge for edge in baseline if edge not in current]
    assert not gone, (
        f"These upward edges no longer exist: {gone}. Lower the baseline so they "
        f"cannot come back: run `{WRITER}`."
    )


def test_no_new_or_grown_import_cycles():
    baseline = [set(c) for c in load_baseline()["cycles"]]
    _upward, current = current_graph()
    grown = []
    for component in current:
        recorded = next((b for b in baseline if set(component) & b), set())
        if not set(component) <= recorded:
            joined = sorted(set(component) - recorded)
            grown.append(f"- {joined} joined or formed a cycle.\n  {_cycle_advice(component)}")
    assert not grown, "Import cycles grew:\n" + "\n".join(grown)


def test_shrunk_import_cycles_are_lowered_in_the_baseline():
    baseline = load_baseline()["cycles"]
    _upward, current = current_graph()
    stale = [c for c in baseline if c not in current]
    assert not stale, (
        f"These recorded cycles shrank, split or disappeared: "
        f"{[f'{len(c)} modules starting {c[:3]}' for c in stale]}. Lower the baseline "
        f"so the gain sticks: run `{WRITER}`."
    )


def test_layer_lists_name_existing_modules():
    modules = production_sources()
    named = FOUNDATION | DATA_MODULES | COMPOSITION_ROOTS | set(DATA_PACKAGES) | set(WEB_PACKAGES)
    assert not sorted(named - modules.keys()), "Layer lists name modules that do not exist"


def test_nothing_imports_the_composition_root():
    modules = production_sources()
    importers = sorted({src for src, dst in import_edges(modules) if dst in COMPOSITION_ROOTS})
    assert not importers, f"{importers} import app; it is the composition root."


# ---------------------------------------------------------------- the parser


def _graph(sources: dict[str, str]):
    modules = {}
    for name, source in sources.items():
        is_package = name.endswith("/")
        name = name.rstrip("/")
        rel = name.replace(".", "/") + ("/__init__.py" if is_package else ".py")
        modules[name] = (source, is_package, rel)
    return import_edges(modules)


@pytest.mark.parametrize(("sources", "expected"), [
    ({"a": "import b\n", "b": ""}, {("a", "b")}),
    ({"a": "def f():\n    import b\n", "b": ""}, {("a", "b")}),
    ({"a": "class C:\n    def m(self):\n        from b import x\n", "b": ""}, {("a", "b")}),
    ({"a": "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    import b\n", "b": ""}, set()),
    ({"a": "import typing\nif typing.TYPE_CHECKING:\n    import b\nelse:\n    import c\n",
      "b": "", "c": ""}, {("a", "c")}),
    ({"a": "import os\nimport json\n"}, set()),
    ({"a": "from p import m\n", "p/": "", "p.m": ""}, {("a", "p"), ("a", "p.m")}),
    ({"a": "from p.m import x\n", "p/": "", "p.m": ""}, {("a", "p"), ("a", "p.m")}),
    ({"a": "from p import helper\n", "p/": ""}, {("a", "p")}),
    ({"a": "import p.m\n", "p/": "", "p.m": ""}, {("a", "p"), ("a", "p.m")}),
    ({"p.m": "from p.n import x\n", "p/": "", "p.n": ""}, {("p.m", "p.n")}),
    ({"p.m": "from p import helper\n", "p/": ""}, {("p.m", "p")}),
    ({"a": "try:\n    from .b import x\nexcept ImportError:\n    from b import x\n", "b": ""},
     {("a", "b")}),
    ({"p.m": "from . import n\nfrom .n import x\n", "p/": "", "p.n": ""}, {("p.m", "p.n")}),
    ({"p/": "from .m import x\n", "p.m": ""}, {("p", "p.m")}),
    ({"a": "import a\n"}, set()),
])
def test_import_edges(sources, expected):
    assert set(_graph(sources)) == expected


def _fixture_modules(root: Path, sources: dict[str, str]):
    for rel, source in {"__init__.py": "", **sources}.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")
    return production_sources(root)


@pytest.mark.parametrize("statement", [
    "import vireo.app",
    "from vireo import app",
    "from vireo.app import create_app",
    "def f():\n    import vireo.app",
    "def f():\n    from vireo import app",
    "import app",
    "from . import app",
    "from .app import create_app",
])
def test_package_imports_cannot_bypass_ratchets(tmp_path, monkeypatch, statement):
    modules = _fixture_modules(tmp_path, {"db.py": statement, "app.py": ""})
    edges = import_edges(modules)
    assert ("db", "app") in edges
    monkeypatch.setattr(sys.modules[__name__], "production_sources", lambda: modules)
    monkeypatch.setattr(sys.modules[__name__], "current_graph", lambda: (upward_edges(edges), []))
    monkeypatch.setattr(sys.modules[__name__], "load_baseline", lambda: {"upward_edges": []})
    with pytest.raises(AssertionError, match="db -> app"):
        test_no_new_upward_edges()
    with pytest.raises(AssertionError, match="import app; it is the composition root"):
        test_nothing_imports_the_composition_root()


@pytest.mark.parametrize("statement", [
    "import vireo.app", "from vireo import app", "from . import app", "from .app import create_app",
])
def test_root_initializer_cannot_import_app(tmp_path, monkeypatch, statement):
    modules = _fixture_modules(tmp_path, {"__init__.py": statement, "app.py": ""})
    assert modules[ROOT_PACKAGE] == (statement, True, "__init__.py")
    edges = import_edges(modules)
    assert set(edges) == {(ROOT_PACKAGE, "app")}
    assert f"{ROOT_PACKAGE} -> app" in upward_edges(edges)
    monkeypatch.setattr(sys.modules[__name__], "production_sources", lambda: modules)
    with pytest.raises(AssertionError, match="import app; it is the composition root"):
        test_nothing_imports_the_composition_root()


@pytest.mark.parametrize("statement", [
    "import vireo.p.child",
    "from vireo.p import child",
    "def f():\n    from vireo.p.child import helper",
    "from .p import child",
    "import p.child",
])
def test_nested_package_imports_and_initializers(tmp_path, statement):
    modules = _fixture_modules(tmp_path, {
        "db.py": statement,
        "p/__init__.py": "from . import child",
        "p/child.py": "from .. import db",
    })
    edges = import_edges(modules)
    assert set(edges) == {("db", "p"), ("db", "p.child"), ("p", "p.child"), ("p.child", "db")}
    assert import_cycles(modules, edges) == [["db", "p", "p.child"]]
    assert set(upward_edges(edges)) == {"db -> p", "db -> p.child"}


@pytest.mark.parametrize(("root_import", "child_import"), [
    ("from . import child", "import vireo"),
    ("import vireo.child", "from vireo import helper"),
    ("from vireo.child import helper", "from . import helper"),
])
def test_root_child_cycle_cannot_bypass_ratchet(tmp_path, monkeypatch, root_import, child_import):
    modules = _fixture_modules(tmp_path, {"__init__.py": root_import, "child.py": child_import})
    edges = import_edges(modules)
    cycles = import_cycles(modules, edges)
    assert set(edges) == {(ROOT_PACKAGE, "child"), ("child", ROOT_PACKAGE)}
    assert cycles == [["child", ROOT_PACKAGE]]
    monkeypatch.setattr(sys.modules[__name__], "current_graph", lambda: (upward_edges(edges), cycles))
    monkeypatch.setattr(sys.modules[__name__], "load_baseline", lambda: {"cycles": []})
    with pytest.raises(AssertionError, match="Import cycles grew"):
        test_no_new_or_grown_import_cycles()


def test_import_cycles():
    edges = {("a", "b"), ("b", "c"), ("c", "a"), ("c", "d"), ("d", "e"), ("e", "d"), ("f", "a")}
    assert import_cycles("abcdef", edges) == [["a", "b", "c"], ["d", "e"]]


def test_tiers():
    assert tier(ROOT_PACKAGE) == FOUNDATION_TIER
    assert tier("sql_chunks") == FOUNDATION_TIER
    assert tier("repositories.stats") == DATA_TIER
    assert tier("services.result_files") == APPLICATION_TIER
    assert tier("pipeline") == APPLICATION_TIER
    assert tier("web.photos") == WEB_TIER
    assert tier("app") == APP_TIER


@pytest.mark.parametrize(("source", "target"), [
    ("schema", "file_replace"), ("repositories.sync", "sidecar_identity"),
])
def test_filesystem_helpers_are_above_data(source, target):
    assert tier(target) == APPLICATION_TIER
    edges = _graph({source: f"import {target}", target: ""})
    assert set(upward_edges(edges)) == {f"{source} -> {target}"}


# ---------------------------------------------------------------- the writer


def write_baseline(root: Path = VIREO, path: Path = BASELINE_PATH) -> int:
    """Rewrite the baseline from the tree; refuse to add an edge or grow a cycle."""
    current_upward, current_cycles = current_graph(root)
    if path.exists():
        previous = load_baseline(path)
        new_edges = sorted(set(current_upward) - set(previous["upward_edges"]))
        recorded = [set(c) for c in previous["cycles"]]
        grown = [
            c for c in current_cycles
            if not any(set(c) <= b for b in recorded)
        ]
        if new_edges or grown:
            print("Refusing to raise the import-graph baseline.")
            for edge in new_edges:
                print(f"  new upward edge {edge} at {', '.join(current_upward[edge])}")
                print(f"    {_advice(edge)}")
            for component in grown:
                print(f"  grown cycle: {_cycle_advice(component)}")
            print("A renamed or moved module needs a reviewed hand edit of the JSON keys.")
            return 1
    baseline = {"upward_edges": sorted(current_upward), "cycles": current_cycles}
    path.write_text(json.dumps(baseline, indent=1) + "\n", encoding="utf-8")
    print(
        f"wrote {len(current_upward)} upward edges and {len(current_cycles)} cycles "
        f"(largest {len(current_cycles[0]) if current_cycles else 0} modules) to {path}"
    )
    return 0


def test_cli_refuses_to_raise(tmp_path):
    fixture = tmp_path / "vireo"
    tests = fixture / "tests"
    tests.mkdir(parents=True)
    script = tests / Path(__file__).name
    shutil.copyfile(__file__, script)
    baseline = tests / BASELINE_PATH.name
    baseline.write_text(json.dumps({"upward_edges": [], "cycles": [["a", "b"]]}), encoding="utf-8")
    (fixture / "a.py").write_text("import b\n", encoding="utf-8")
    (fixture / "b.py").write_text("def f():\n    import a\n    import c\n", encoding="utf-8")
    (fixture / "c.py").write_text("import b\n", encoding="utf-8")
    (fixture / "db.py").write_text("def f():\n    import config\n", encoding="utf-8")
    (fixture / "config.py").write_text("", encoding="utf-8")
    previous = baseline.read_bytes()
    result = subprocess.run([sys.executable, str(script), "--write-baseline"], capture_output=True, text=True)
    assert result.returncode == 1, result.stdout + result.stderr
    assert baseline.read_bytes() == previous
    assert "new upward edge db -> config" in result.stdout
    assert "grown cycle" in result.stdout

    (fixture / "c.py").write_text("", encoding="utf-8")
    (fixture / "db.py").write_text("", encoding="utf-8")
    result = subprocess.run([sys.executable, str(script), "--write-baseline"], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(baseline.read_text()) == {"upward_edges": [], "cycles": [["a", "b"]]}


if __name__ == "__main__":
    if sys.argv[1:] != ["--write-baseline"]:
        sys.exit(f"usage: {sys.argv[0]} --write-baseline")
    sys.exit(write_baseline())
