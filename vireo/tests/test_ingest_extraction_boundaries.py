"""Inert benchmark bootstrap and fixture-call checks; no benchmark/app imports."""
import ast
import builtins
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
BENCHMARKS = {
    "benchmark_raw_previews.py": "run_worker",
    "benchmark_raw_preview_stress.py": "run_trial",
}
REMOVED = {"add_photo", "check_and_resolve_duplicates_for_hash", "apply_duplicate_resolution",
           "keep_duplicate_copy", "bulk_resolve_by_folder"}


def _inert_namespace():
    class FakePath:
        def __init__(self, value):
            self.value = value

        def resolve(self):
            return self

        @property
        def parents(self):
            return [FakePath("/invented/checkout/scripts"), FakePath("/invented/checkout")]

        def __truediv__(self, value):
            return FakePath(self.value + "/" + value)

        def __str__(self):
            return self.value

    fake_sys = SimpleNamespace(path=[])
    ingest = SimpleNamespace(add_photo=lambda *args, **kwargs: None)

    def inert_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "sys":
            return fake_sys
        if name == "pathlib":
            return SimpleNamespace(Path=FakePath)
        if name == "services":
            if "/invented/checkout/vireo" not in fake_sys.path:
                raise ModuleNotFoundError("services is unavailable before product-path bootstrap")
            return SimpleNamespace(photo_ingest=ingest, workspace_folders=object())
        if name == "scripts.benchmark_raw_previews":
            return SimpleNamespace(ROOT=FakePath("/invented/checkout"), **{
                key: object() for key in fromlist if key != "ROOT"
            })
        return SimpleNamespace(**{key: object() for key in fromlist or ()})

    return {"__builtins__": {**vars(builtins), "__import__": inert_import},
            "__file__": "/invented/checkout/scripts/benchmark.py", "ROOT": FakePath("/invented/checkout"),
            "sys": fake_sys}, ingest


def _module_prefix(tree):
    nodes = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            break
        nodes.append(node)
    return nodes


def test_benchmark_module_prefixes_bootstrap_before_product_imports():
    for filename in BENCHMARKS:
        tree = ast.parse((ROOT / "scripts" / filename).read_text(encoding="utf-8"))
        ns, ingest = _inert_namespace()
        exec(compile(ast.Module(body=_module_prefix(tree), type_ignores=[]), "<inert prefix>", "exec"), ns)
        assert ns["photo_ingest"] is ingest


def test_worker_bootstrap_precedes_all_runtime_imports():
    for filename, name in BENCHMARKS.items():
        tree = ast.parse((ROOT / "scripts" / filename).read_text(encoding="utf-8"))
        worker = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
        prefix = []
        for node in worker.body:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                break
            prefix.append(node)
        assert prefix and all(isinstance(node, ast.Expr) for node in prefix)
        ns, ingest = _inert_namespace()
        exec(compile(ast.Module(body=_module_prefix(tree), type_ignores=[]), "<inert module bootstrap>", "exec"), ns)
        # Drop the module's path: the worker must install its own before
        # importing cv2/PIL/app or doing any environment or file work.
        ns["sys"].path.clear()
        exec(compile(ast.Module(body=prefix, type_ignores=[]), "<inert worker bootstrap>", "exec"), ns)
        assert "/invented/checkout/vireo" in ns["sys"].path
        assert ns["photo_ingest"] is ingest


def _move_insert_calls(tree):
    names = {"test_move_folder_by_date_progress_counts_each_photo_once",
             "test_move_photos_reports_partial_copy_cleanup_failure",
             "test_move_photos_progress_counts_skipped_and_failed_photos"}
    return [node for fn in tree.body if isinstance(fn, ast.FunctionDef) and fn.name in names
            for node in ast.walk(fn) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == "add_photo"]


def test_move_fixture_inserts_use_ingest_without_a_database_shim():
    tree = ast.parse((ROOT / "vireo/tests/test_move.py").read_text(encoding="utf-8"))
    calls = _move_insert_calls(tree)
    assert len(calls) == 4
    db = SimpleNamespace()
    recorded = []
    def add_photo(found, **kwargs):
        assert found is db
        recorded.append(kwargs)
        return len(recorded)
    ns = {"db": db, "photo_ingest": SimpleNamespace(add_photo=add_photo), "fid": 17,
          "env": {"fid_src": 17}, "name": "invented.jpg", "stamp": "2026-07-12T09:00:00"}
    assert not hasattr(db, "add_photo")
    for node in calls:
        eval(compile(ast.Expression(body=node), "<inert insert call>", "eval"), ns)
    assert len(recorded) == 4
    assert {row["folder_id"] for row in recorded} == {17}


def test_no_removed_database_method_callers_remain():
    for directory in ("scripts", "tests", "vireo", "tools"):
        for path in (ROOT / directory).rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in REMOVED:
                    assert isinstance(node.func.value, ast.Name) and node.func.value.id in {
                        "photo_ingest", "duplicate_resolution",
                    }, f"{path.relative_to(ROOT)}:{node.lineno}: {ast.unparse(node.func)}"


def test_userfirst_seed_prefix_bootstraps_before_ingest_import():
    import posixpath

    fake_sys = SimpleNamespace(path=[])
    ingest = object()
    imports = []
    filename = "/invented/checkout/vireo/testing/userfirst/seeds.py"
    expected = posixpath.join(posixpath.dirname(filename), "..", "..")

    def inert_import(name, *args, **kwargs):
        imports.append(name)
        if name == "os":
            return SimpleNamespace(path=posixpath, pardir="..")
        if name == "sys":
            return fake_sys
        assert name in ("config", "services"), (
            f"Seed prefix unexpectedly imported a runtime dependency: {name!r}"
        )
        if expected not in fake_sys.path:
            raise ModuleNotFoundError(f"{name} unavailable before seed path bootstrap")
        if name == "config":
            return SimpleNamespace()
        return SimpleNamespace(photo_ingest=ingest, workspace_folders=object())

    tree = ast.parse((ROOT / "vireo/testing/userfirst/seeds.py").read_text(encoding="utf-8"))
    for initial in ([], [expected]):
        fake_sys.path[:] = initial
        imports.clear()
        ns = {"__file__": filename, "__builtins__": {**vars(builtins), "__import__": inert_import}}
        exec(compile(ast.Module(body=_module_prefix(tree), type_ignores=[]), "<inert seed prefix>", "exec"), ns)
        assert ns["photo_ingest"] is ingest
        assert fake_sys.path == [expected]
        assert imports == ["os", "sys", "config", "services"]
