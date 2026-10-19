"""Actual history SQL and response AST on invented memory rows; no app imports."""

import __future__

import ast
import contextlib
import copy
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]


def _execute(nodes, namespace):
    exec(compile(
        ast.Module(body=copy.deepcopy(nodes), type_ignores=[]), "<inert history identity>", "exec",
        flags=__future__.annotations.compiler_flag,
    ), namespace)


@contextlib.contextmanager
def _history():
    namespace = {}
    tree = ast.parse((ROOT / "repositories" / "edit_history.py").read_text(encoding="utf-8"))
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "EditHistoryRepository")
    cls.body = [node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name in {
        "__init__", "workspace_id", "record", "next_undo", "next_redo", "mark_undone", "mark_redone",
    }]
    _execute([cls], namespace)
    with contextlib.closing(sqlite3.connect(":memory:")) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        snapshot = json.loads((ROOT / "tests" / "fixtures" / "canonical_schema_snapshot.json").read_text(encoding="utf-8"))
        for kind, name, _table, sql in snapshot["sqlite_master"]:
            if kind == "table" and name in {"workspaces", "folders", "photos", "edit_history", "edit_history_items"}:
                conn.execute(sql)
        conn.execute("INSERT INTO workspaces(id,name) VALUES(17,'Invented workspace')")
        conn.execute("INSERT INTO folders(id,path) VALUES(1,'/invented')")
        conn.execute("INSERT INTO photos(id,folder_id,filename,extension) VALUES(42,1,'invented.jpg','jpg')")
        conn.commit()
        repo = namespace["EditHistoryRepository"](
            conn, lambda: 17, non_undoable=("invented-nonundoable",),
            restore_species_representative=lambda *args: None,
        )
        response_tree = ast.parse((ROOT / "web" / "history.py").read_text(encoding="utf-8"))
        response = next(node for node in ast.walk(response_tree)
                        if isinstance(node, ast.FunctionDef) and node.name == "_history_response")
        failure = [False]
        response_namespace = {
            "_refresh_pipeline_cache_species_from_edit": lambda *args: not failure[0],
            "_history_flags_changed": lambda *args: False,
        }
        _execute([response], response_namespace)

        def record(action="species_replace", description="invented species edit"):
            return repo.record(action, description, None, [
                {"photo_id": 42, "old_value": "invented-old", "new_value": "invented-new"},
            ])

        def step(operation, stale=False):
            entry, _items = getattr(repo, "next_" + operation)()
            getattr(repo, "mark_undone" if operation == "undo" else "mark_redone")(entry["id"])
            failure[0] = stale
            return response_namespace["_history_response"](
                SimpleNamespace(), entry, "Undo" if operation == "undo" else "Redo", None,
            )

        yield conn, record, step, repo


def history_sequences():
    """Response/visibility sequences also consumed by the fake DOM validation."""
    cases = []
    for replacement in ("flag", "species_replace"):
        with _history() as (_conn, record, step, repo):
            original = record()
            failed = step("undo", True)
            reused = record(replacement, "invented species edit")
            assert reused == original
            assert repo.next_redo() is None
            replacement_undo = step("undo")
            replacement_redo = step("redo")
            assert all(event["entry_id"] == original for event in (failed, replacement_undo, replacement_redo))
            cases.append(("recycled ID for " + replacement, [
                (failed, True), (replacement_undo, True), (replacement_redo, True),
            ]))
    with _history() as (_conn, record, step, _repo):
        original = record()
        failed = step("undo", True)
        assert record("species_replace") == original
        failed_replacement = step("undo", True)
        repaired_replacement = step("redo")
        cases.append(("recycled ID with another failed undo", [
            (failed, True), (failed_replacement, True), (repaired_replacement, True),
        ]))
    with _history() as (_conn, record, step, _repo):
        original = record()
        healthy_undo = step("undo")
        failed_redo = step("redo", True)
        assert record("flag", "unrelated flag") != original
        flag_undo = step("undo")
        flag_redo = step("redo")
        flag_undo_again = step("undo")
        actual_inverse = step("undo")
        assert actual_inverse["entry_id"] == original
        cases.append(("failed redo with unrelated edit and actual inverse", [
            (healthy_undo, False), (failed_redo, True), (flag_undo, True),
            (flag_redo, True), (flag_undo_again, True), (actual_inverse, False),
        ]))
    with _history() as (_conn, record, step, _repo):
        record()
        failed_undo = step("undo", True)
        failed_inverse = step("redo", True)
        actual_inverse = step("undo")
        cases.append(("failed undo and failed redo repaired by actual inverse", [
            (failed_undo, True), (failed_inverse, True), (actual_inverse, False),
        ]))
    with _history() as (_conn, record, step, _repo):
        record()
        record()
        failed_high = step("undo", True)
        failed_low = step("undo", True)
        repaired_low = step("redo")
        repaired_high = step("redo")
        assert failed_high["entry_id"] != failed_low["entry_id"]
        cases.append(("multiple retained history entries", [
            (failed_high, True), (failed_low, True), (repaired_low, True), (repaired_high, False),
        ]))
    with _history() as (_conn, record, step, _repo):
        record()
        failed = step("undo", True)
        failed.pop("entry_id")
        repaired = step("redo")
        cases.append(("unidentified failure cannot be proven repaired", [(failed, True), (repaired, True)]))
    return cases


def test_sqlite_history_reuses_discarded_ids_and_responses_keep_their_directions():
    cases = history_sequences()
    assert len(cases) == 7
    for _name, sequence in cases:
        for response, _visible in sequence:
            assert response["ok"] is True
            assert ("undone" in response) != ("redone" in response)
            assert bool(response.get("pipeline_cache_stale")) == ("warning" in response)
