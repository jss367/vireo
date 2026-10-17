"""Selected AST and invented in-memory history/fake collaborators only."""
import ast
import json
import sqlite3
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load(file, names=(), classes=()):
    nodes = [n for n in ast.parse((ROOT / file).read_text(encoding="utf-8")).body
             if (isinstance(n, ast.FunctionDef) and n.name in names)
             or (isinstance(n, ast.ClassDef) and n.name in classes)]
    ns = {"json": json}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<inert>", "exec",
                 flags=__import__("__future__").annotations.compiler_flag), ns)
    return ns


def history():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""CREATE TABLE edit_history (
        id INTEGER PRIMARY KEY, workspace_id INTEGER, action_type TEXT,
        description TEXT, new_value TEXT, source TEXT, reason TEXT,
        is_batch INTEGER DEFAULT 0, undone INTEGER DEFAULT 0,
        created_at TEXT DEFAULT 'invented-time');
        CREATE TABLE edit_history_items(id INTEGER PRIMARY KEY, edit_id INTEGER,
        photo_id INTEGER, old_value TEXT, new_value TEXT);""")
    ws = [1]
    cls = load("repositories/edit_history.py", classes={"EditHistoryRepository"})["EditHistoryRepository"]
    repo = cls(conn, lambda: ws[0], non_undoable=[], restore_species_representative=lambda *a: None)
    return conn, ws, repo


def record(repo, flag, action="flag", payload=None):
    return repo.record(action, "invented", payload or flag,
                       [dict(photo_id=7, old_value="none", new_value=flag)],
                       source="invented-agent", reason="invented-reason")


def test_cross_workspace_cycles_do_not_revive_provenance():
    conn, ws, repo = history()
    first = record(repo, "flagged")
    ws[0] = 2
    record(repo, "rejected")
    last = record(repo, "flagged")
    ws[0] = 1
    assert repo.latest_flag_note(7) is None
    conn.execute("UPDATE edit_history SET undone=1 WHERE id > ?", (first,))
    assert repo.latest_flag_note(7)["edit_id"] == first
    ws[0] = 2
    conn.execute("UPDATE edit_history SET undone=0 WHERE id=?", (last,))
    assert repo.latest_flag_note(7)["edit_id"] == last
    conn.close()


def test_pipeline_flag_history_blocks_old_note_but_rating_does_not():
    conn, _, repo = history()
    first = record(repo, "flagged")
    record(repo, "4", action="rating")
    assert repo.latest_flag_note(7)["edit_id"] == first
    payload = json.dumps({"photo_edit": {"action_type": "flag", "new_value": "pipeline_group_apply"}})
    barrier = record(repo, "flagged", action="pipeline_grouping", payload=payload)
    assert repo.latest_flag_note(7) is None
    conn.execute("UPDATE edit_history SET undone=1 WHERE id=?", (barrier,))
    assert repo.latest_flag_note(7)["edit_id"] == first
    conn.close()


def test_no_matching_batch_ids_preserves_redo_and_avoids_writes():
    ns = load("services/photo_review.py", {"flag_description"}, {"PhotoReviewService"})
    writes = []
    db = types.SimpleNamespace(get_photos_by_ids=lambda ids: {},
        photo_review=types.SimpleNamespace(set_flags=lambda *a: writes.append("flags")),
        queue_flag_change_if_enabled=lambda *a, **kw: writes.append("queue"),
        record_edit=lambda *a, **kw: writes.append("history"))
    assert ns["PhotoReviewService"](db).set_flags([999], "flagged") == 0
    assert writes == []


def test_batch_boolean_ids_rejected_before_service():
    ns = load("web/photo_review.py", {"flag_provenance", "create_photo_review_blueprint"})
    routes, calls = {}, []
    class Blueprint:
        def __init__(self, *a):
            pass
        def post(self, path):
            def register(fn):
                routes[path] = fn
                return fn
            return register
    ns.update(Blueprint=Blueprint, VALID_FLAGS=("none", "flagged", "rejected"),
        FLAG_SOURCE_MAX_CHARS=60, FLAG_REASON_MAX_CHARS=300, jsonify=lambda x: x,
        PhotoReviewService=lambda db: types.SimpleNamespace(set_flags=lambda *a, **kw: calls.append(a)))
    ns["create_photo_review_blueprint"](lambda: object(), lambda msg, status=400: (msg, status))
    for ids in ([True], [False], [1, True], "1", [1.0], [-1]):
        ns["request"] = types.SimpleNamespace(get_json=lambda ids=ids, **kw: {"photo_ids": ids, "flag": "rejected"})
        assert routes["/api/batch/flag"]()[1] == 400
    assert calls == []



def test_scalar_json_rejected_on_both_flag_aliases():
    ns = load("web/app_hooks.py", {"_reject_non_object_json_body"})
    ns["json_error"] = lambda msg, status, **kw: (msg, status)
    for path in ("/api/batch/flag", "/api/v1/batch/flag", "/api/v1/photos/7/flag"):
        for body in (True, 7, "invented", [7], False, []):
            ns["request"] = types.SimpleNamespace(path=path, method="POST", is_json=True,
                get_json=lambda body=body, **kw: body)
            assert ns["_reject_non_object_json_body"]()[1] == 400
    tree = ast.parse((ROOT / "web/app_hooks.py").read_text(encoding="utf-8"))
    assert any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
               and n.func.attr == "before_request" and n.args
               and isinstance(n.args[0], ast.Name) and n.args[0].id == "_reject_non_object_json_body"
               for n in ast.walk(tree))


if __name__ == "__main__":
    failed = []
    for name, check in list(globals().items()):
        if name.startswith("test_"):
            try:
                check()
                print("PASS", name)
            except (AssertionError, KeyError) as exc:
                failed.append(name)
                print("FAIL", name, str(exc))
    assert not failed, failed
