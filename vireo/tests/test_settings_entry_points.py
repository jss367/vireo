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
    # A top-level ``import config`` bootstraps every function-level call: a
    # function body only runs after module import has completed, so later
    # imports in the module are fine for calls inside functions. Module-level
    # calls, which execute during import, still need an import that precedes
    # them in source order and must be checked against the module body itself.
    module_bootstrapped = any(_imports_config(node) for node in tree.body)
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
            if module_bootstrapped:
                continue
            # Any enclosing function may have bootstrapped before this call,
            # but only a statement at the top level of that function's body
            # is guaranteed to execute before entering a nested scope: an
            # ``import config`` inside ``if cond:`` runs only when ``cond``
            # is true, so it is not a reliable bootstrap for a static check.
            enclosing = [s for s in scopes if s.lineno <= node.lineno <= (s.end_lineno or s.lineno)]
            if not any(
                _bootstraps_in_body(s, node.lineno) for s in enclosing
            ):
                offenders.append(node.lineno)
    return sorted(set(offenders))


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
