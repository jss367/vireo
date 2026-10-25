"""Selected AST and invented in-memory history/fake collaborators only."""
import ast
import contextlib
import json
import sqlite3
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load(file, names=(), classes=()):
    nodes = [n for n in ast.parse((ROOT / file).read_text(encoding="utf-8")).body
             if (isinstance(n, ast.FunctionDef) and n.name in names)
             or (isinstance(n, ast.ClassDef) and n.name in classes)]
    ns = {"json": json, "contextlib": contextlib, "contextmanager": contextlib.contextmanager}
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
        photo_id INTEGER, old_value TEXT, new_value TEXT, species_provenance TEXT);""")
    ws = [1]
    cls = load("repositories/edit_history.py", classes={"EditHistoryRepository"})["EditHistoryRepository"]
    repo = cls(conn, lambda: ws[0], non_undoable=[], restore_species_representative=lambda *a: None)
    return conn, ws, repo


def test_no_matching_batch_ids_preserves_redo_and_avoids_writes():
    ns = load("services/photo_review.py", {"flag_description"}, {"PhotoReviewService"})
    writes = []
    db = types.SimpleNamespace(photos=types.SimpleNamespace(get_by_ids=lambda ids: {}),
        photo_review=types.SimpleNamespace(set_flags=lambda *a: writes.append("flags"),
                                          flag_write=lambda: contextlib.nullcontext(False)),
        queue_flag_change_if_enabled=lambda *a, **kw: writes.append("queue"),
        record_edit=lambda *a, **kw: writes.append("history"))
    assert ns["PhotoReviewService"](db).set_flags([999], "flagged") == 0
    assert writes == []


def test_batch_boolean_ids_rejected_before_service():
    ns = load("web/photo_review.py", {"flag_provenance", "create_photo_review_blueprint", "_valid_photo_id"})
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
    for ids in ([True], [False], [1, True], "1", [1.0], [-1], [2**63], [2**64]):
        ns["request"] = types.SimpleNamespace(get_json=lambda ids=ids, **kw: {"photo_ids": ids, "flag": "rejected"})
        assert routes["/api/batch/flag"]()[1] == 400
    assert calls == []
    ns["request"] = types.SimpleNamespace(get_json=lambda **kw: {"flag": "rejected"})
    for pid in (0, -1, True, 2**63, 2**64):
        assert routes["/api/photos/<int:photo_id>/flag"](pid)[1] == 400
    assert ns["_valid_photo_id"](1)
    assert ns["_valid_photo_id"](2**63 - 1)



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



def durable_fixture():
    conn, ws, history_repo = history()
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript("""CREATE TABLE workspaces(id INTEGER PRIMARY KEY);
        CREATE TABLE photos(id INTEGER PRIMARY KEY, flag TEXT);
        CREATE TABLE photo_workspace_visibility(photo_id INTEGER, workspace_id INTEGER);
        CREATE TABLE invented_queue(photo_id INTEGER, flag TEXT);
        INSERT INTO workspaces VALUES(1),(2);
        INSERT INTO photos VALUES(7,'none');
        INSERT INTO photo_workspace_visibility VALUES(7,1),(7,2);""")
    tree = ast.parse((ROOT / "canonical_schema.py").read_text(encoding="utf-8"))
    ddl = ast.literal_eval(next(n.value for n in tree.body if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "CURRENT_FLAG_ATTRIBUTION_OBJECTS" for t in n.targets)))
    canonical = load("canonical_schema.py", {"create_current_flag_attribution"})
    canonical["CURRENT_FLAG_ATTRIBUTION_OBJECTS"] = ddl
    canonical["create_current_flag_attribution"](conn)
    canonical["create_current_flag_attribution"](conn)
    load("schema.py", {"_validate_current_flag_attribution"})["_validate_current_flag_attribution"](conn)
    review_cls = load("repositories/photo_review.py", classes={"PhotoReviewRepository"})["PhotoReviewRepository"]
    class FakeDB:
        failed = False
        prunes = 0
        @property
        def photo_review(self):
            return review_cls(conn, ws[0])
        @property
        def photos(self):
            return types.SimpleNamespace(get_by_ids=self._photos_by_ids)
        def _photos_by_ids(self, ids):
            return {r["id"]: dict(r) for r in conn.execute(
                "SELECT p.id,p.flag FROM photos p JOIN photo_workspace_visibility v "
                "ON v.photo_id=p.id WHERE v.workspace_id=?", (ws[0],)) if r["id"] in ids}
        def queue_flag_change_if_enabled(self, pid, flag, _commit=True):
            conn.execute("INSERT INTO invented_queue VALUES(?,?)", (pid, flag))
            if _commit:
                conn.commit()
        def record_edit(self, *a, **kw):
            assert kw["_commit"] is False
            if self.failed:
                raise ValueError("invented history failure")
            return history_repo.record(*a, **kw)
        def prune_edit_history(self):
            self.prunes += 1
    db = FakeDB()
    service_ns = load("services/photo_review.py", {"flag_description", "flag_note"}, {"PhotoReviewService"})
    conn.commit()
    return conn, ws, db, service_ns["PhotoReviewService"](db), service_ns["flag_note"]


def test_durable_note_survives_pruning_but_never_revives_after_other_writers():
    conn, ws, db, service, note = durable_fixture()
    assert note(db, 7, "none") is None
    service.set_flag(7, "flagged", source="first", reason="invented")
    conn.execute("DELETE FROM edit_history_items")
    conn.execute("DELETE FROM edit_history")
    conn.commit()
    assert note(db, 7, "flagged")["source"] == "first"
    # Same-value writes invalidate too; no surviving history can refill the note.
    conn.execute("UPDATE photos SET flag='flagged' WHERE id=7")
    assert note(db, 7, "flagged") is None
    conn.commit()
    service.set_flag(7, "flagged", source="first")
    ws[0] = 2
    service.set_flag(7, "rejected", source="second")
    service.set_flag(7, "flagged", source="second")
    assert note(db, 7, "flagged")["source"] == "second"
    ws[0] = 1
    assert note(db, 7, "flagged") is None
    conn.execute("DELETE FROM edit_history WHERE workspace_id=2")
    conn.execute("DELETE FROM workspaces WHERE id=2")
    conn.commit()
    assert note(db, 7, "flagged") is None
    conn.execute("UPDATE photos SET flag='rejected' WHERE id=7")
    conn.execute("UPDATE photos SET flag='flagged' WHERE id=7")
    assert note(db, 7, "flagged") is None
    conn.close()


def test_flag_history_queue_and_note_are_atomic_and_preserve_outer_transaction():
    conn, _, db, service, note = durable_fixture()
    service.set_flag(7, "flagged", source="first")
    old_counts = tuple(conn.execute("SELECT (SELECT COUNT(*) FROM edit_history),"
                                   "(SELECT COUNT(*) FROM invented_queue)").fetchone())
    db.failed = True
    try:
        service.set_flag(7, "rejected", source="failed")
    except ValueError:
        pass
    else:
        raise AssertionError("fake failure was swallowed")
    assert not conn.in_transaction
    assert conn.execute("SELECT flag FROM photos WHERE id=7").fetchone()[0] == "flagged"
    assert note(db, 7, "flagged")["source"] == "first"
    assert tuple(conn.execute("SELECT (SELECT COUNT(*) FROM edit_history),"
                              "(SELECT COUNT(*) FROM invented_queue)").fetchone()) == old_counts
    db.failed = False
    before_prunes = db.prunes
    conn.execute("BEGIN")
    service.set_flags([7], "rejected", source="outer")
    assert conn.in_transaction and db.prunes == before_prunes
    assert note(db, 7, "rejected")["source"] == "outer"
    conn.rollback()
    assert note(db, 7, "flagged")["source"] == "first"
    conn.close()


def test_replay_and_manual_writes_clear_current_note_without_history_guessing():
    conn, _, db, service, note = durable_fixture()
    service.set_flag(7, "rejected", source="agent")
    # Execute the actual selected undo/redo flag handler, never Database.__init__.
    tree = ast.parse((ROOT / "db.py").read_text(encoding="utf-8"))
    fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_edit_set_flag")
    ns = {}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "<inert replay>", "exec"), ns)
    ns["_edit_set_flag"](db, 7, "none")
    ns["_edit_set_flag"](db, 7, "rejected")
    assert note(db, 7, "rejected") is None
    service.set_flag(7, "flagged", source="agent")
    service.set_flag(7, "flagged")
    assert note(db, 7, "flagged") is None
    conn.close()



def test_migration_and_canonical_ddl_match_without_backfilling_legacy_history():
    conn, _, db, _, note = durable_fixture()
    conn.execute("DROP TRIGGER invalidate_current_flag_attribution")
    conn.execute("DROP TABLE current_flag_attribution")
    conn.execute("INSERT INTO edit_history(workspace_id,action_type,description,new_value,source) "
                 "VALUES(1,'flag','legacy','flagged','legacy-agent')")
    conn.execute("UPDATE photos SET flag='flagged' WHERE id=7")
    canonical = ast.parse((ROOT / "canonical_schema.py").read_text(encoding="utf-8"))
    ddl = ast.literal_eval(next(n.value for n in canonical.body if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "CURRENT_FLAG_ATTRIBUTION_OBJECTS" for t in n.targets)))
    ns = load("canonical_schema.py", {"create_current_flag_attribution"})
    ns["CURRENT_FLAG_ATTRIBUTION_OBJECTS"] = ddl
    ns["create_current_flag_attribution"](conn)
    validate = load("schema.py", {"_validate_current_flag_attribution"})["_validate_current_flag_attribution"]
    validate(conn)
    assert note(db, 7, "flagged") is None
    assert conn.execute("SELECT source FROM edit_history").fetchone()[0] == "legacy-agent"
    actual = [list(r) for r in conn.execute("SELECT type,name,tbl_name,sql FROM sqlite_master "
        "WHERE name IN ('current_flag_attribution','invalidate_current_flag_attribution') ORDER BY rowid")]
    snapshot = json.loads((ROOT / "tests/fixtures/canonical_schema_snapshot.json").read_text(encoding="utf-8"))
    expected = [r for r in snapshot["sqlite_master"]
                if r[1] in {"current_flag_attribution", "invalidate_current_flag_attribution"}]
    assert actual == expected
    conn.execute("DROP TRIGGER invalidate_current_flag_attribution")
    try:
        validate(conn)
    except RuntimeError:
        pass
    else:
        raise AssertionError("missing invalidation trigger accepted")
    conn.close()



def test_note_publication_failure_and_photo_replacement_cannot_leave_false_attribution():
    conn, _, db, service, note = durable_fixture()
    service.set_flag(7, "flagged", source="first")
    repo_cls = type(db.photo_review)
    original = repo_cls.set_current_flag_note
    def fail(*a, **kw):
        raise sqlite3.IntegrityError("invented note publication failure")
    repo_cls.set_current_flag_note = fail
    try:
        try:
            service.set_flag(7, "rejected", source="failed")
        except sqlite3.IntegrityError:
            pass
        else:
            raise AssertionError("publication failure swallowed")
    finally:
        repo_cls.set_current_flag_note = original
    assert not conn.in_transaction
    assert note(db, 7, "flagged")["source"] == "first"
    conn.execute("DELETE FROM photos WHERE id=7")
    conn.execute("INSERT INTO photos(id,flag) VALUES(7,'flagged')")
    assert note(db, 7, "flagged") is None
    conn.close()




def test_global_hook_leaves_malformed_and_object_validation_to_flag_routes():
    ns = load("web/app_hooks.py", {"_reject_non_object_json_body"})
    for body in (None, {}):
        ns["request"] = types.SimpleNamespace(path="/api/v1/photos/7/flag", method="POST", is_json=True,
            get_json=lambda body=body, **kw: body)
        assert ns["_reject_non_object_json_body"]() is None


def test_v1_flag_routes_require_an_object_and_explicit_flag_before_any_write():
    conn, _, db, service, note = durable_fixture()
    service.set_flag(7, "flagged", source="invented-agent")
    ns = load("web/photo_review.py", {"flag_provenance", "create_photo_review_blueprint", "_valid_photo_id"})
    routes, accesses = {}, []
    class Blueprint:
        def __init__(self, *a):
            pass
        def post(self, path):
            def register(fn):
                routes[fn.__name__] = fn
                return fn
            return register
    ns.update(Blueprint=Blueprint, VALID_FLAGS=("none", "flagged", "rejected"),
        FLAG_SOURCE_MAX_CHARS=60, FLAG_REASON_MAX_CHARS=300, jsonify=lambda x: x,
        PhotoReviewService=lambda ignored: service)
    ns["create_photo_review_blueprint"](lambda: accesses.append("db") or db,
                                         lambda msg, status=400: (msg, status))
    tree = ast.parse((ROOT / "app.py").read_text(encoding="utf-8"))
    aliases = ast.literal_eval(next(n.value for n in tree.body if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "_V1_ALIASES" for t in n.targets)))
    views = {endpoint: routes.get(endpoint.split(".")[-1], object()) for _, endpoint, _ in aliases}
    registered = {}
    app = types.SimpleNamespace(view_functions=views,
        add_url_rule=lambda path, **kw: registered.__setitem__(path, kw["view_func"]))
    aliases_ns = load("app.py", {"_register_v1_aliases"})
    aliases_ns["_V1_ALIASES"] = aliases
    aliases_ns["_register_v1_aliases"](app, set())
    single = registered["/api/v1/photos/<int:photo_id>/flag"]
    batch = registered["/api/v1/batch/flag"]
    assert single is routes["set_flag"] and batch is routes["set_flags"]
    raw_inputs = ("", "{", '{"flag":"none"', "null", "false", "7", '"x"', "[]",
                  "{}", '{"photo_ids":[7]}', '{"flag":null}', '{"flag":false}', '{"flag":"invalid"}')
    before = tuple(conn.execute("SELECT (SELECT flag FROM photos WHERE id=7),"
        "(SELECT COUNT(*) FROM edit_history),(SELECT COUNT(*) FROM invented_queue)").fetchone())
    def decode(raw, kwargs):
        assert kwargs == {"silent": True}
        try:
            return json.loads(raw)
        except ValueError:
            return None
    for raw in raw_inputs:
        ns["request"] = types.SimpleNamespace(get_json=lambda raw=raw, **kw: decode(raw, kw))
        for fn, args in ((single, (7,)), (batch, ())):
            assert fn(*args)[1] == 400
            assert tuple(conn.execute("SELECT (SELECT flag FROM photos WHERE id=7),"
                "(SELECT COUNT(*) FROM edit_history),(SELECT COUNT(*) FROM invented_queue)").fetchone()) == before
            assert note(db, 7, "flagged")["source"] == "invented-agent"
    assert accesses == []
    ns["request"] = types.SimpleNamespace(get_json=lambda **kw: {"flag": "none"})
    assert single(7) == {"ok": True}
    assert conn.execute("SELECT flag FROM photos WHERE id=7").fetchone()[0] == "none"
    assert note(db, 7, "none") is None
    ns["request"] = types.SimpleNamespace(get_json=lambda **kw: {"photo_ids": [7], "flag": "none"})
    assert batch() == {"ok": True, "updated": 1}
    assert conn.execute("SELECT COUNT(*) FROM edit_history").fetchone()[0] == before[1] + 2
    conn.close()


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
