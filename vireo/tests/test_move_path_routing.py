"""Selected path/routing methods with fake rows and relocation; no file I/O."""

import ast
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]


def _selected_function(file, name):
    tree = ast.parse((ROOT / file).read_text(encoding="utf-8"))
    node = next(n for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef) and n.name == name)
    return ast.Module(body=[node], type_ignores=[])


def _path_namespace():
    namespace = {}
    for name in ("_path_for_subtree_match", "_subtree_prefix",
                 "_subtree_relative", "_join_subtree_path"):
        exec(compile(_selected_function("db.py", name), "db path helper", "exec"), namespace)
    exec(compile(_selected_function("repositories/moves_merge.py", "folder_paths_below"),
                 "folder_paths_below", "exec"), namespace)
    return namespace


def _folders_below(base, paths):
    namespace = _path_namespace()

    def execute(sql, params):
        assert "SELECT path FROM folders" in sql
        length, prefix = params
        # The query's REPLACE is deliberately only a broad prefilter.
        rows = [{"path": path} for path in paths
                if path.replace("\\", "/")[:length] == prefix]
        return SimpleNamespace(fetchall=lambda: rows)

    repo = SimpleNamespace(conn=SimpleNamespace(execute=execute),
                           _subtree_prefix=namespace["_subtree_prefix"],
                           _subtree_relative=namespace["_subtree_relative"])
    return namespace["folder_paths_below"](repo, base)


def test_posix_trailing_backslash_is_a_literal_stem():
    base = "/archive/tail\\"
    paths = [base, base + "/child", "/archive/tail/child",
             base + "other/child", base + "/child\\name/nested"]
    expected = [(base + "/child", "child"),
                (base + "/child\\name/nested", "child\\name/nested")]
    assert _folders_below(base, paths) == expected
    assert _folders_below(base + "/", paths) == expected


def test_posix_internal_backslash_false_prefix_and_wildcards():
    for base in ("/archive/a\\b", "/archive/a_%"):
        paths = [base, base + "/child", base + "2/child",
                 base + "\\child", base.replace("\\", "/") + "/other"]
        expected = [(path, path[len(base) + 1:]) for path in paths
                    if path.startswith(base + "/")]
        assert _folders_below(base, paths) == expected
    assert _folders_below("/", ["/child", "/child/nested"]) == [
        ("/child", "child"), ("/child/nested", "child/nested")]


def test_windows_drive_unc_and_mixed_separators():
    for base in ("C:\\Archive\\Tail", "C:/Archive/Tail",
                 "\\\\invented-server\\share\\Tail"):
        forward = base.replace("\\", "/")
        paths = [base, base + "\\child", forward + "/child/deep",
                 base + "/mixed\\nested", base + "2\\outside"]
        expected = [(paths[1], "child"), (paths[2], "child/deep"),
                    (paths[3], "mixed/nested")]
        for ending in ("", "/", "\\"):
            assert _folders_below(base + ending, paths) == expected


def test_render_relocation_reconstructs_source_paths_without_filesystem():
    namespace = _path_namespace()
    method = _selected_function("move/folder_catalog.py", "relocate_developed_dirs")
    # Supply only the fake module for the method's runtime relocation import.
    calls = []

    def fake_import(name, *args, **kwargs):
        assert name == "export"
        return SimpleNamespace(relocate_developed_dir=lambda *args: calls.append(args))

    namespace["__builtins__"] = {"__import__": fake_import}
    exec(compile(method, "relocate_developed_dirs", "exec"), namespace)
    for src, dst, child, relative in (
        ("/staging/tail\\", "/archive/tail\\", "/archive/tail\\/child", "child"),
        ("/staging/tail\\", "/archive/tail\\", "/archive/tail\\/child\\name", "child\\name"),
        ("C:\\Staging\\Tail", "D:\\Archive\\Tail", "D:\\Archive\\Tail\\child", "child"),
        ("\\\\invented-server\\staging\\Tail", "\\\\invented-server\\archive\\Tail",
         "\\\\invented-server\\archive\\Tail\\child", "child"),
    ):
        calls.clear()
        for merge in (False, True):
            calls.clear()
            target = SimpleNamespace(merge_into_tracked=7 if merge else None,
                                     merge_reconcile_base=dst, catalog_path="alias" if merge else dst)
            repo = SimpleNamespace(folder_paths_below=lambda base, child=child, relative=relative, dst=dst:
                                   [(child, relative)] if base == dst else [])
            rewrite = SimpleNamespace(src_path=src, developed_dir="invented-render-root",
                                      target=target, db=SimpleNamespace(moves_merge=repo))
            namespace["relocate_developed_dirs"](rewrite)
            sep = "/" if src.startswith("/") or "/" in src else "\\"
            assert calls == [("invented-render-root", src, dst),
                             ("invented-render-root", src + sep + relative, child)]
