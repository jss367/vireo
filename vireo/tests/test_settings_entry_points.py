"""Every entry point that opens a ``Database`` imports ``config`` first.

The data layer reads global settings through ``settings_source`` and never
imports ``config`` (``test_import_graph.py``); importing ``config`` is what
installs it as the provider. A process that opens a ``Database`` without
having imported ``config`` fails on its first settings read with a
``RuntimeError`` saying so. The app, the jobs and the services import
``config`` on their own; the entry points checked here are the ones that may
not: ``scripts/``, the tools under ``tools/*/src/``, the test harness under
``vireo/testing/`` and the pytest ``conftest.py`` files.

Convention: bootstrap with a direct ``import config`` / ``from config import
...`` statement or a standalone ``configure_repo(...)`` call. Put it in the
module or an enclosing function before the statement defining the database
consumer, or in the same function before the statement containing
``Database(...)``. Imports hidden inside conditionals or sibling functions
do not count, nor does an enclosing import after the consumer's definition.

This is a syntax convention, not an execution analyzer: aliases, indirect
helper calls and dynamic initialization ordering are outside its scope.
Fresh-process tests below check the supported entry points at runtime.
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
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "configure_repo"
    )


def _opens_database(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    return (isinstance(func, ast.Name) and func.id == "Database") or (
        isinstance(func, ast.Attribute) and func.attr == "Database"
    )


def unbootstrapped_database_opens(source: str) -> list[int]:
    """Database calls missing the explicit bootstrap convention above."""
    tree = ast.parse(source)
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    offenders = []
    for call in (node for node in ast.walk(tree) if _opens_database(node)):
        scopes = []
        child = call
        while child in parents:
            parent = parents[child]
            if isinstance(parent, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef)) and child in parent.body:
                scopes.append((parent, child))
            child = parent
        # An enclosing import must precede the consumer's definition, not
        # merely its Database call's source line. No call graph is inferred.
        if not any(
            any(_bootstraps(stmt) for stmt in scope.body[:scope.body.index(statement)])
            for scope, statement in scopes
        ):
            offenders.append(call.lineno)
    return sorted(offenders)


def test_entry_points_import_config_before_opening_a_database():
    offenders = []
    for path in _entry_point_files():
        lines = unbootstrapped_database_opens(path.read_text(encoding="utf-8"))
        offenders += [f"{path.relative_to(REPO)}:{line}" for line in lines]
    assert not offenders, (
        "These Database calls do not follow the explicit bootstrap convention: "
        f"{offenders}. Add a direct `import config` or standalone `configure_repo()` "
        "statement in the module or an enclosing function before the consumer's "
        "definition, or in the same function before the statement containing the Database call. See this "
        "test module's docstring for the convention and its limits."
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
    ("def f(p):\n    def g():\n        return Database(p)\n    g()\n    import config\n", [3]),
    # Module ordering is explicit, even when a deferred function could be
    # safely called after a later import. No call graph is inferred.
    ("db = Database(p)\ndb.get_subject_types()\nimport config\n", [1]),
    ("def f(p):\n    return Database(p)\nimport config\n", [2]),
    ("configure_repo()\ndb = Database(p)\n", []),
    ("import vireo.config as cfg\nclass Consumer:\n    db = Database(p)\n", []),
    ("def f(p, flag):\n    if flag:\n        import config\n    return Database(p)\n", [4]),
    ("if flag:\n    import config\ndb = Database(p)\n", [3]),
    ("def f(p):\n    def unused():\n        import config\n    return Database(p)\n", [4]),
    ("def setup():\n    configure_repo()\ndef f(p):\n    return Database(p)\n", [4]),
    ("def f(p):\n    configure_repo(Database(p))\n", [2]),
    ("def f(p):\n    configure_repo(); return Database(p)\n", []),
    ("async def f(p):\n    import config\n    return Database(p)\n", []),
    ("def f(p=Database(p)):\n    import config\n", [1]),
])
def test_unbootstrapped_database_opens(source, lines):
    assert unbootstrapped_database_opens(source) == lines


def _run_fresh(code: str, tmp_path: Path) -> subprocess.CompletedProcess:
    """Use a fresh interpreter and temporary config without importing config."""
    config_path = tmp_path / "config.json"
    config_path.write_text('{"subject_types": ["individual"], "sync_flags_to_xmp": false}', encoding="utf-8")
    prelude = f"""
        import sys
        sys.path.insert(0, {str(VIREO)!r})
        import app_paths
        original_default_path = app_paths.default_path
        app_paths.default_path = lambda name: (
            {str(config_path)!r} if name == "config" else original_default_path(name)
        )
    """
    return subprocess.run(
        [sys.executable, "-I", "-c", textwrap.dedent(prelude) + textwrap.dedent(code)],
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
    """, tmp_path)
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
            assert db.get_subject_types() == {{"individual"}}
    """, tmp_path)
    assert result.returncode == 0, result.stderr


def test_encounter_configure_repo_runs_in_a_fresh_process(tmp_path):
    result = _run_fresh(f"""
        sys.path.insert(0, {str(REPO / "tools" / "encounter-evaluation" / "src")!r})
        from encounter_eval.common import configure_repo
        assert "config" not in sys.modules
        configure_repo({str(REPO)!r})
        assert "config" in sys.modules
        from db import Database
        with Database({str(tmp_path / "catalog.db")!r}) as db:
            assert db.get_subject_types() == {{"individual"}}
    """, tmp_path)
    assert result.returncode == 0, result.stderr


def test_userfirst_browse_seed_runs_in_a_fresh_process(tmp_path):
    result = _run_fresh(f"""
        assert "config" not in sys.modules
        from testing.userfirst.seeds import browse_seed
        assert "config" in sys.modules
        browse_seed(
            {str(tmp_path / "catalog.db")!r},
            {str(tmp_path / "thumbnails")!r},
            {str(tmp_path / "photos")!r},
        )
        from db import Database
        with Database({str(tmp_path / "catalog.db")!r}) as db:
            assert db.conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0] == 10
            assert db.get_subject_types() == {{"individual"}}
    """, tmp_path)
    assert result.returncode == 0, result.stderr
