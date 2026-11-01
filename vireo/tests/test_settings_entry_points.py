"""Every entry point that opens a ``Database`` imports ``config`` first.

The data layer reads global settings through ``settings_source`` and never
imports ``config`` (``test_import_graph.py``); importing ``config`` is what
installs it as the provider. A process that opens a ``Database`` without
having imported ``config`` fails on its first settings read with a
``RuntimeError`` saying so. The app, the jobs and the services import
``config`` on their own; the entry points checked here are the ones that may
not: ``scripts/``, the tools under ``tools/*/src/``, the test harness under
``vireo/testing/`` and the pytest ``conftest.py`` files.

The rule, per function: a call to ``Database(...)`` needs ``import config`` /
``from config import ...`` at module level, or earlier in the same function,
or an earlier call to ``configure_repo()`` (the encounter-evaluation tools'
entry point, which imports ``config``). Add the import where the test points.
"""

from __future__ import annotations

import ast
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
VIREO = REPO / "vireo"


def _entry_point_files() -> list[Path]:
    files = sorted((REPO / "scripts").rglob("*.py"))
    files += sorted((VIREO / "testing").rglob("*.py"))
    for tool in sorted((REPO / "tools").iterdir()):
        files += sorted((tool / "src").rglob("*.py"))
        files += sorted(tool.rglob("conftest.py"))
    files += sorted((REPO / "tests").rglob("conftest.py"))
    files += sorted((VIREO / "tests").glob("conftest.py"))
    return [path for path in dict.fromkeys(files) if path.is_file()]


def _imports_config(node: ast.AST) -> bool:
    if isinstance(node, ast.Import):
        return any(alias.name in ("config", "vireo.config") for alias in node.names)
    if isinstance(node, ast.ImportFrom):
        return node.level == 0 and node.module in ("config", "vireo.config")
    return False


def _bootstraps(node: ast.AST) -> bool:
    if _imports_config(node):
        return True
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "configure_repo"
    )


def _opens_database(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    return (isinstance(func, ast.Name) and func.id == "Database") or (
        isinstance(func, ast.Attribute) and func.attr == "Database"
    )


def unbootstrapped_database_opens(source: str) -> list[int]:
    """Line numbers of ``Database(...)`` calls with no earlier config import."""
    tree = ast.parse(source)
    scopes = [node for node in ast.walk(tree)
              if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    in_functions = {id(n) for scope in scopes for n in ast.walk(scope)}
    local_funcs = {
        n.name: n for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    # Earliest module-level bootstrap, as a (lineno, col_offset) pair. ``None``
    # if the module has no top-level bootstrap at all. A function body only
    # runs after module import has completed, so a late module-level import
    # still bootstraps function-level calls that are never invoked during the
    # module's own initialization; but any function entered during init runs
    # before a later import, so the bootstrap must precede the entry.
    module_bootstrap_pos = None
    for stmt in tree.body:
        if _bootstraps(stmt) or (isinstance(stmt, ast.Expr) and _bootstraps(stmt.value)):
            module_bootstrap_pos = (stmt.lineno, stmt.col_offset)
            break
    module_bootstrapped = module_bootstrap_pos is not None
    first_init_entry = _functions_invoked_during_init(tree, local_funcs)
    offenders = []
    for node in ast.walk(tree):
        if not _opens_database(node) or id(node) in in_functions:
            continue
        if not _bootstraps_in_body(tree, node.lineno):
            offenders.append(node.lineno)
    for scope in scopes:
        for node in ast.walk(scope):
            if not _opens_database(node):
                continue
            # Any enclosing function may have bootstrapped before this call,
            # but only a statement at the top level of that function's body
            # is guaranteed to execute before entering a nested scope: an
            # ``import config`` inside ``if cond:`` runs only when ``cond``
            # is true, so it is not a reliable bootstrap for a static check.
            enclosing = [s for s in scopes if s.lineno <= node.lineno <= (s.end_lineno or s.lineno)]
            if any(_bootstraps_in_body(s, node.lineno) for s in enclosing):
                continue
            outermost = min(enclosing, key=lambda s: s.lineno)
            entry_pos = first_init_entry.get(outermost.name)
            if entry_pos is not None:
                # The outermost enclosing function is reached during module
                # init at ``entry_pos``; the module-level bootstrap must
                # precede that entry in source order.
                if module_bootstrap_pos is None or module_bootstrap_pos > entry_pos:
                    offenders.append(node.lineno)
                continue
            if not module_bootstrapped:
                offenders.append(node.lineno)
    return sorted(set(offenders))


def _walk_module_level(node: ast.AST):
    """Yield ``node`` and its descendants, but stop at function/class/lambda
    bodies, which do not execute until the function is called."""
    yield node
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                              ast.ClassDef, ast.Lambda)):
            continue
        yield from _walk_module_level(child)


def _functions_invoked_during_init(
    tree: ast.Module, local_funcs: dict[str, ast.AST]
) -> dict[str, tuple[int, int]]:
    """Local functions reached during module init, keyed by earliest entry
    position. A call site at module top level enters its target then; a call
    inside an already-reached function's own body propagates the caller's
    entry position (that's when the call runs). Calls nested inside deferred
    bodies — a further ``def``, ``class`` or ``lambda`` — do not."""
    entry: dict[str, tuple[int, int]] = {}
    for stmt in tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        for child in _walk_module_level(stmt):
            if (isinstance(child, ast.Call)
                    and isinstance(child.func, ast.Name)
                    and child.func.id in local_funcs):
                pos = (child.lineno, child.col_offset)
                prev = entry.get(child.func.id)
                if prev is None or pos < prev:
                    entry[child.func.id] = pos
    changed = True
    while changed:
        changed = False
        for name in list(entry):
            caller_pos = entry[name]
            for stmt in local_funcs[name].body:
                for child in _walk_module_level(stmt):
                    if (isinstance(child, ast.Call)
                            and isinstance(child.func, ast.Name)
                            and child.func.id in local_funcs):
                        callee = child.func.id
                        prev = entry.get(callee)
                        if prev is None or caller_pos < prev:
                            entry[callee] = caller_pos
                            changed = True
    return entry


def _bootstraps_in_body(scope: ast.AST, max_lineno: int) -> bool:
    """A bootstrap sits as a top-level statement of ``scope.body`` whose line
    is at or before ``max_lineno``. Statements nested in ``if`` / ``try`` /
    ``with`` / loops are not accepted: static analysis can't prove they run."""
    body = getattr(scope, "body", None)
    if not isinstance(body, list):
        return False
    for stmt in body:
        if stmt.lineno > max_lineno:
            return False
        if _bootstraps(stmt):
            return True
        if isinstance(stmt, ast.Expr) and _bootstraps(stmt.value):
            return True
    return False


def test_entry_points_import_config_before_opening_a_database():
    offenders = []
    for path in _entry_point_files():
        lines = unbootstrapped_database_opens(path.read_text(encoding="utf-8"))
        offenders += [f"{path.relative_to(REPO)}:{line}" for line in lines]
    assert not offenders, (
        "These entry points open a Database without importing config first, so "
        "their first settings read raises 'No settings provider is installed': "
        f"{offenders}. Add `import config` (module level, or in the function "
        "before the Database call); importing config installs the settings "
        "provider the data layer reads through settings_source."
    )


@pytest.mark.parametrize(("source", "lines"), [
    ("from db import Database\ndef f(p):\n    return Database(p)\n", [3]),
    ("import config\nfrom db import Database\ndef f(p):\n    return Database(p)\n", []),
    ("def f(p):\n    import config  # noqa\n    from db import Database\n    return Database(p)\n", []),
    ("def f(p):\n    from db import Database\n    db = Database(p)\n    import config\n", [3]),
    ("def f(p):\n    configure_repo()\n    from db import Database\n    return Database(p)\n", []),
    ("def f(p):\n    from config import DEFAULTS\n    import db\n    return db.Database(p)\n", []),
    ("import db\nDB = db.Database(':memory:')\n", [2]),
    # A module-level ``Database(...)`` followed by a late ``import config``
    # runs the call before the provider is installed, so it is flagged even
    # though the file imports config elsewhere.
    ("DB = Database(':memory:')\nimport config\n", [1]),
    # The import ahead of the module-level call is the valid bootstrap.
    ("import config\nDB = Database(':memory:')\n", []),
    # A late module-level import still bootstraps function-level calls: the
    # function runs only after module initialization has imported config.
    ("def f(p):\n    return Database(p)\nimport config\n", []),
    ("def f(p):\n    import config\n    def g():\n        return Database(p)\n    return g\n", []),
    # An import inside a nested helper that is not on the path to the call
    # does not run before it; the Database(p) at line 4 is unbootstrapped.
    ("def f(p):\n    def g():\n        import config\n    return Database(p)\n", [4]),
    # An import in a sibling helper is not a bootstrap either.
    ("def f(p):\n    def g():\n        pass\n    def h():\n        import config\n    return Database(p)\n", [6]),
    # An ``import config`` inside ``if cond:`` runs only when ``cond`` is
    # true, so it isn't a reliable bootstrap; the Database(p) at line 4 is
    # unbootstrapped.
    ("def f(p, enabled):\n    if enabled:\n        import config\n    return Database(p)\n", [4]),
    # ``try: import config`` isn't guaranteed either (the import itself can
    # raise) — the except branch continues on to Database with no provider.
    ("def f(p):\n    try:\n        import config\n    except ImportError:\n        pass\n    return Database(p)\n", [6]),
    # Imports inside loops and ``with`` blocks are likewise not top-level
    # body statements.
    ("def f(p, items):\n    for _ in items:\n        import config\n    return Database(p)\n", [4]),
    ("def f(p, ctx):\n    with ctx:\n        import config\n    return Database(p)\n", [4]),
    # Invoking a local function during module init runs its body before any
    # later ``import config``: the function-level call is flagged.
    ("def f():\n    return Database(':memory:')\nf()\nimport config\n", [2]),
    # The module-level bootstrap precedes the invocation, so the entry is
    # already bootstrapped by the time the function runs.
    ("def f():\n    return Database(':memory:')\nimport config\nf()\n", []),
    # Transitive module-init invocation: a helper called during init that
    # itself calls a Database-opening local function entrains the callee at
    # the helper's invocation position.
    ("def helper():\n    f()\ndef f():\n    return Database(':memory:')\nhelper()\nimport config\n", [4]),
    ("import config\ndef helper():\n    f()\ndef f():\n    return Database(':memory:')\nhelper()\n", []),
    # A guarded invocation inside ``if __name__ == '__main__':`` still runs
    # during module init when the condition is true; a bootstrap earlier in
    # the guard counts as a module-body bootstrap (same ``if``'s own body
    # is nested, so it does not).
    ("def main():\n    return Database(':memory:')\nif __name__ == '__main__':\n    import config\n    main()\n", [2]),
    # A top-level import precedes the guarded invocation, so the entry has a
    # bootstrap in source order ahead of it.
    ("import config\ndef main():\n    return Database(':memory:')\nif __name__ == '__main__':\n    main()\n", []),
    # A function defined but never invoked during init is bootstrapped by a
    # late module-level import, since it only runs after module init.
    ("def f(p):\n    return Database(p)\ndef g(p):\n    pass\ng(1)\nimport config\n", []),
])
def test_unbootstrapped_database_opens(source, lines):
    assert unbootstrapped_database_opens(source) == lines


def _run_fresh(code: str) -> subprocess.CompletedProcess:
    """Run ``code`` in a fresh interpreter whose only path entry is vireo/."""
    return subprocess.run(
        [sys.executable, "-I", "-c", f"import sys; sys.path.insert(0, {str(VIREO)!r})\n"
         + textwrap.dedent(code)],
        capture_output=True, text=True, timeout=120,
    )


def test_settings_read_without_config_says_to_import_it(tmp_path):
    result = _run_fresh(f"""
        import sys
        from db import Database
        assert "config" not in sys.modules
        db = Database({str(tmp_path / "catalog.db")!r})
        try:
            db.get_subject_types()
        except RuntimeError as exc:
            print(exc)
        else:
            raise SystemExit("settings read succeeded without a provider")
    """)
    assert result.returncode == 0, result.stderr
    assert "import config" in result.stdout


def test_benchmark_library_seed_runs_in_a_fresh_process(tmp_path):
    """A representative script entry point opens a Database in a bare process."""
    result = _run_fresh(f"""
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "benchmark_library", {str(REPO / "scripts" / "benchmark_library.py")!r})
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        import sys
        assert "config" not in sys.modules
        module._seed({str(tmp_path / "catalog.db")!r}, 3)
        # The entry point installed the settings provider, so the Database it
        # opened (and any other in this process) can read settings.
        assert "config" in sys.modules, "the script opened a Database without importing config"
        from db import Database
        with Database({str(tmp_path / "catalog.db")!r}) as db:
            print(sorted(db.get_subject_types()))
    """)
    assert result.returncode == 0, result.stderr
