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
    # The earliest module-level bootstrap statement, in execution order; a
    # later `import config` cannot rescue a `Database(...)` call that already
    # ran at module load.
    module_bootstrap_line = next(
        (node.lineno for node in tree.body if _bootstraps(node)),
        None,
    )
    scopes = [node for node in ast.walk(tree)
              if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    in_functions = {id(n) for scope in scopes for n in ast.walk(scope)}
    offenders = []
    for scope in scopes:
        for node in ast.walk(scope):
            if not _opens_database(node):
                continue
            # Any enclosing function may have imported config before this call,
            # or a module-level bootstrap must precede the outermost enclosing
            # function's def so loading the module installs it first.
            enclosing = [s for s in scopes if s.lineno <= node.lineno <= (s.end_lineno or s.lineno)]
            outer_fn_line = min((s.lineno for s in enclosing), default=node.lineno)
            if any(
                _bootstraps(n) and n.lineno <= node.lineno
                for s in enclosing for n in ast.walk(s)
            ):
                continue
            if module_bootstrap_line is not None and module_bootstrap_line < outer_fn_line:
                continue
            offenders.append(node.lineno)
    # Module-level opens outside any function.
    for n in ast.walk(tree):
        if (
            _opens_database(n)
            and id(n) not in in_functions
            and (module_bootstrap_line is None or module_bootstrap_line > n.lineno)
        ):
            offenders.append(n.lineno)
    return sorted(set(offenders))


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
    ("def f(p):\n    import config\n    def g():\n        return Database(p)\n    return g\n", []),
    # A later module-level `import config` cannot rescue a `Database(...)` that
    # already ran at module load.
    ("from db import Database\nDB = Database(':memory:')\nimport config\n", [2]),
    ("import config\nfrom db import Database\nDB = Database(':memory:')\n", []),
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
