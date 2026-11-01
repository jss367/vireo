"""Both move import modes, with every Vireo dependency replaced by a stub.

No move function, application module, database, transfer, image loader or
filesystem workflow is executed. Runtime-import blocks are checked alone.
"""
import ast
import copy
import importlib.util
import sys
import types
from pathlib import Path

import pytest

MOVE = Path(__file__).resolve().parents[1] / "move"
MODULES = {"dates", "folder", "folder_catalog", "folder_target", "folder_transfer",
           "mounted_verify", "paths", "photos", "remote", "transfer", "verify"}
DEPENDENCIES = {
    "db": ("_chunks", "_join_subtree_path", "_subtree_prefix", "_subtree_relative"),
    "proc": ("no_window_kwargs",),
    "staged_copy": ("copy_via_temp",),
    "capture_time": ("_capture_datetime",),
    "ingest": ("build_destination_path",),
    "move_cleanup": ("finish_source",),
    "export": ("developed_folder_key", "relocate_default_developed_file", "relocate_developed_file"),
}


def _forbidden(*args, **kwargs):
    raise AssertionError("A dependency was called during import-only validation")


def _load(mode, monkeypatch):
    for name in list(sys.modules):
        if name == "move" or name.startswith("move.") or name.startswith("vireo.move"):
            monkeypatch.delitem(sys.modules, name)
    parent = types.ModuleType("vireo")
    parent.__path__ = []  # Never execute the actual vireo package initializer.
    monkeypatch.setitem(sys.modules, "vireo", parent)
    monkeypatch.setitem(sys.modules, "move" if mode == "vireo.move" else "vireo.move", None)
    stubs = {}
    for sibling, exports in DEPENDENCIES.items():
        prefix = "vireo." if mode == "vireo.move" else ""
        name = prefix + sibling
        stub = types.ModuleType(name)
        for export in exports:
            setattr(stub, export, _forbidden)
        monkeypatch.setitem(sys.modules, name, stub)
        # An incorrect fallback cannot find a real Vireo dependency.
        opposite = sibling if prefix else "vireo." + sibling
        monkeypatch.setitem(sys.modules, opposite, None)
        stubs[sibling] = stub
    spec = importlib.util.spec_from_file_location(
        mode, MOVE / "__init__.py", submodule_search_locations=[str(MOVE)],
    )
    assert spec is not None and spec.loader is not None
    package = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, mode, package)
    try:
        spec.loader.exec_module(package)
    except BaseException:
        for name in list(sys.modules):
            if name.startswith(mode + "."):
                sys.modules.pop(name, None)
        raise
    # Submodules inserted by the loader also need teardown in a shared worker.
    for module in MODULES:
        name = mode + "." + module
        loaded = sys.modules[name]
        del sys.modules[name]
        monkeypatch.setitem(sys.modules, name, loaded)
    return package, stubs


@pytest.mark.parametrize("mode", ["move", "vireo.move"])
def test_package_imports_and_exports_use_only_stubbed_vireo_dependencies(mode, monkeypatch):
    package, stubs = _load(mode, monkeypatch)
    assert package.move_photos is sys.modules[mode + ".photos"].move_photos
    assert package.move_folder is sys.modules[mode + ".folder"].move_folder
    assert package.move_folder_by_date is sys.modules[mode + ".dates"].move_folder_by_date
    for name in package.__all__:
        assert hasattr(package, name), name
        value = getattr(package, name)
        if callable(value):
            assert value.__module__.startswith(mode + "."), name
    assert sys.modules[mode + ".transfer"].copy_via_temp is stubs["staged_copy"].copy_via_temp
    assert sys.modules[mode + ".remote"].no_window_kwargs is stubs["proc"].no_window_kwargs
    assert sys.modules[mode + ".verify"].no_window_kwargs is stubs["proc"].no_window_kwargs
    assert sys.modules[mode + ".mounted_verify"].no_window_kwargs is stubs["proc"].no_window_kwargs
    assert sys.modules[mode + ".folder_catalog"]._chunks is stubs["db"]._chunks


@pytest.mark.parametrize("mode", ["move", "vireo.move"])
def test_moved_runtime_imports_resolve_at_the_vireo_sibling_depth(mode, monkeypatch):
    _package, stubs = _load(mode, monkeypatch)
    checked = 0
    for module in ["dates", "photos"]:
        tree = ast.parse((MOVE / (module + ".py")).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Try) or not node.body:
                continue
            first = node.body[0]
            if not isinstance(first, ast.ImportFrom) or first.module not in {
                "capture_time", "ingest", "move_cleanup", "export",
            }:
                continue
            assert first.level == 2
            assert all(isinstance(n, ast.ImportFrom) for n in node.body)
            assert len(node.handlers) == 1
            assert all(isinstance(n, ast.ImportFrom) for n in node.handlers[0].body)
            # Execute just the unchanged try/fallback import statements.
            namespace = {"__name__": mode + "." + module, "__package__": mode}
            imports = ast.Module(body=[copy.deepcopy(node)], type_ignores=[])
            exec(compile(imports, "<inert moved sibling imports>", "exec"), namespace)
            for alias in first.names:
                assert namespace[alias.asname or alias.name] is getattr(stubs[first.module], alias.name)
            checked += 1
    assert checked == 6
