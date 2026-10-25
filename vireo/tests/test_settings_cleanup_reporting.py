"""Selected settings definitions with fake catalogs; no app/config or I/O."""
import ast
import builtins
import contextlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import cast

SOURCE = Path(__file__).resolve().parents[1] / "web" / "settings.py"
KEY = "write_location_keywords_to_xmp"
HELPERS = {
    "queue_location_keyword_cleanup_for_workspace",
    "queue_location_keyword_cleanup_reporting",
    "location_keyword_cleanup_warning",
    "read_workspace_overrides",
    "write_workspace_overrides",
    "_with_warning",
}


def _namespace(db, body=None):
    schema = SimpleNamespace(
        SCHEMA={KEY: {"scope": "workspace"}},
        validate_value=lambda key, value: value,
        ValidationError=ValueError,
        set_dotted=lambda data, key, value: data.update({key: value}),
        delete_dotted=lambda data, key: data.pop(key, None),
        flatten=dict,
    )
    modules = {"config_schema": schema, "config": SimpleNamespace(load=lambda: {KEY: False}, DEFAULTS={KEY: False})}

    def fake_import(name, *args, **kwargs):
        assert name in modules, f"Unexpected product import: {name}"
        return modules[name]

    logs = []
    namespace = {
        "__builtins__": dict(vars(builtins), __import__=fake_import),
        "json": json, "sqlite3": sqlite3, "cast": cast, "LOCATION_KEYWORDS_SETTING": KEY,
        "log": SimpleNamespace(warning=lambda *args, **kwargs: logs.append((args, kwargs))),
        "settings_write_lock": contextlib.nullcontext(),
        "get_db": lambda: db, "jsonify": lambda payload: payload,
        "request": SimpleNamespace(get_json=lambda **kwargs: body or {}),
        "read_raw_config_file": lambda: {},
    }
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    selected = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in HELPERS]
    blueprint = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "create_settings_blueprint")
    for node in blueprint.body:
        if isinstance(node, ast.FunctionDef) and node.name in {"api_settings_workspace_patch", "api_settings_workspace_delete", "api_settings_values"}:
            node.decorator_list = []
            selected.append(node)
    exec(compile(ast.Module(body=selected, type_ignores=[]), "<fake settings catalog>", "exec"), namespace)
    return namespace, logs


class _Catalog:
    def __init__(self, *, name_error=None, queue_error=None, restore_error=None):
        self.active_workspace_id = 17
        self.overrides = {KEY: True}
        self.saved = False
        self.name_error = name_error
        self.queue_error = queue_error
        self.restore_error = restore_error
        self.switches = []
        self.workspaces = SimpleNamespace(get=self.get, update=self.update)

    def require_workspace_id(self):
        return self.active_workspace_id

    def get(self, workspace_id):
        if self.saved and self.name_error:
            raise self.name_error
        return {"name": "Invented workspace", "config_overrides": self.overrides}

    def update(self, workspace_id, *, config_overrides):
        self.overrides = dict(config_overrides or {})
        self.saved = True

    def get_effective_config(self, global_config):
        return dict(global_config, **self.overrides)

    def set_active_workspace(self, workspace_id):
        self.switches.append(workspace_id)
        if len(self.switches) == 2 and self.restore_error:
            raise self.restore_error
        self.active_workspace_id = workspace_id

    def queue_location_changes_for_tagged_photos(self):
        if self.queue_error:
            raise self.queue_error


def test_saved_workspace_patch_and_reset_warn_when_queue_and_name_read_fail():
    for route in ("api_settings_workspace_patch", "api_settings_workspace_delete"):
        db = _Catalog(name_error=sqlite3.OperationalError("invented name read failure"),
                      queue_error=sqlite3.OperationalError("invented queue failure"))
        namespace, logs = _namespace(db, {"key": KEY, "value": False})
        response = namespace[route]() if route.endswith("patch") else namespace[route](KEY)
        assert response["ok"] is True
        assert '"#17"' in response["warning"]
        assert "Queue location writes" in response["warning"]
        assert db.saved and db.get_effective_config({KEY: False})[KEY] is False
        assert db.active_workspace_id == 17 and db.switches == [17, 17]
        assert len(logs) == 2 and all(kwargs["exc_info"] for _, kwargs in logs)


def test_reporting_retains_workspace_name_when_the_name_read_succeeds():
    db = _Catalog(queue_error=sqlite3.OperationalError("invented queue failure"))
    namespace, _ = _namespace(db)
    warning = namespace["queue_location_keyword_cleanup_reporting"](db, 17)
    assert '"Invented workspace"' in warning
    assert db.active_workspace_id == 17


def test_success_does_not_read_a_name_or_add_a_warning():
    db = _Catalog(name_error=sqlite3.OperationalError("must not read"))
    db.saved = True
    namespace, logs = _namespace(db)
    assert namespace["queue_location_keyword_cleanup_reporting"](db, 17) is None
    assert namespace["_with_warning"]({"ok": True}, None) == {"ok": True}
    assert logs == []


def test_unexpected_name_read_errors_propagate_unchanged():
    error = ValueError("invented programming error")
    db = _Catalog(name_error=error, queue_error=sqlite3.OperationalError("invented queue failure"))
    db.saved = True
    namespace, _ = _namespace(db)
    try:
        namespace["queue_location_keyword_cleanup_reporting"](db, 17)
    except ValueError as caught:
        assert caught is error
    else:
        raise AssertionError("Unexpected name read errors must propagate")


def test_preexisting_workspace_restore_failure_still_propagates():
    error = sqlite3.OperationalError("invented restore failure")
    db = _Catalog(queue_error=sqlite3.OperationalError("invented queue failure"), restore_error=error)
    namespace, _ = _namespace(db)
    try:
        namespace["queue_location_keyword_cleanup_for_workspace"](db, 17)
    except sqlite3.Error as caught:
        assert caught is error
    else:
        raise AssertionError("Restoration failures must not be hidden")


def test_singular_cleanup_retry_names_the_workspace_to_activate():
    namespace, _ = _namespace(_Catalog())
    warning = namespace["location_keyword_cleanup_warning"](["Invented non-active workspace"])
    retry = warning.split("To retry,", 1)[1]
    assert 'workspace "Invented non-active workspace"' in retry


def test_bulk_workspace_update_propagates_failed_cleanup_warning():
    db = _Catalog(queue_error=sqlite3.OperationalError("invented queue failure"))
    db.active_workspace_id = 99
    namespace, _ = _namespace(db, {"config_overrides": {KEY: False}})
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    effective = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "workspace_effective_setting")
    workspace_source = SOURCE.with_name("workspaces.py")
    tree = ast.parse(workspace_source.read_text(encoding="utf-8"))
    blueprint = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "create_workspace_blueprint")
    update = next(node for node in blueprint.body if isinstance(node, ast.FunctionDef) and node.name == "api_update_workspace")
    update.decorator_list = []
    namespace["_validate_workspace_config_overrides"] = lambda *args: None
    exec(compile(ast.Module(body=[effective, update], type_ignores=[]), "<fake bulk workspace update>", "exec"), namespace)
    response = namespace["api_update_workspace"](17)
    assert db.saved and db.overrides[KEY] is False
    assert db.active_workspace_id == 99 and db.switches == [17, 99]
    assert '"Invented workspace"' in response["warning"]


def test_values_identify_the_workspace_whose_warnings_the_ui_renders():
    db = _Catalog()
    namespace, _ = _namespace(db)
    for workspace_id in (17, 18):
        db.active_workspace_id = workspace_id
        response = namespace["api_settings_values"]()
        assert response["workspace_id"] == workspace_id
        assert response["workspace"][KEY] is True
