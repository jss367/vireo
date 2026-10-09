"""Behavior pins for the saved-processes domain of ``Database``.

These tests exercise listing, reading and creating saved processes through
the ``db.processes`` accessor, and updating, deleting and resolving them
through the ``Database`` methods that stay on the façade. They pin return shapes and ordering, field
validation and coercion, error messages, commit boundaries, and how
``delete_saved_process`` rewrites workspace ``config_overrides`` (including
rows it must skip). ``test_saved_processes.py`` covers seeding and the
legacy ``default_strategy`` migration.
"""

import ast
import inspect
import json
import sqlite3
import textwrap

import process_strategies as ps
import pytest
from db import Database
from repositories.processes import ProcessesRepository

_DICT_KEYS = {
    "id", "name", "skip_classify", "skip_extract_masks", "skip_eye_keypoints",
    "skip_regroup", "miss_enabled", "review_mode", "is_seed", "sort_order",
}


def _other_conn(db):
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _raw_row(db, process_id):
    conn = _other_conn(db)
    try:
        return conn.execute(
            "SELECT * FROM saved_processes WHERE id = ?", (process_id,)
        ).fetchone()
    finally:
        conn.close()


def _clear_processes(db):
    db.conn.execute("DELETE FROM saved_processes")
    db.conn.commit()


def _set_overrides_raw(db, workspace_id, raw):
    db.conn.execute(
        "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
        (raw, workspace_id),
    )
    db.conn.commit()


def _overrides_raw(db, workspace_id):
    return db.conn.execute(
        "SELECT config_overrides FROM workspaces WHERE id = ?", (workspace_id,)
    ).fetchone()[0]


# -- reads -------------------------------------------------------------------


def test_get_saved_process_returns_dict_with_python_types(db):
    pid = db.processes.create(
        "Typed", skip_classify=True, miss_enabled=False, review_mode="species",
    )
    proc = db.processes.get(pid)
    assert type(proc) is dict
    assert set(proc) == _DICT_KEYS
    assert proc["id"] == pid
    for key in ("skip_classify", "skip_extract_masks", "skip_eye_keypoints",
                "skip_regroup", "miss_enabled", "is_seed"):
        assert type(proc[key]) is bool, key
    assert proc["skip_classify"] is True
    assert proc["miss_enabled"] is False
    assert proc["is_seed"] is False
    assert proc["review_mode"] == "species"
    assert type(proc["sort_order"]) is int


def test_get_saved_process_missing_returns_none(db):
    assert db.processes.get(987654) is None


def test_get_saved_process_coerces_nonzero_integers_to_true(db):
    pid = db.processes.create("Raw")
    db.conn.execute(
        "UPDATE saved_processes SET skip_regroup = 7, is_seed = 2 WHERE id = ?",
        (pid,),
    )
    db.conn.commit()
    proc = db.processes.get(pid)
    assert proc["skip_regroup"] is True
    assert proc["is_seed"] is True


def test_get_saved_processes_orders_by_sort_order_then_id(db):
    _clear_processes(db)
    a = db.processes.create("A")
    b = db.processes.create("B")
    c = db.processes.create("C")
    # Tie a and c on sort_order 5 and put b first: id breaks the tie.
    db.conn.execute("UPDATE saved_processes SET sort_order = 5 WHERE id IN (?, ?)", (a, c))
    db.conn.execute("UPDATE saved_processes SET sort_order = 1 WHERE id = ?", (b,))
    db.conn.commit()
    procs = db.processes.list_all()
    assert type(procs) is list
    assert [p["id"] for p in procs] == [b, a, c]
    assert all(set(p) == _DICT_KEYS for p in procs)


def test_get_saved_processes_empty_table_returns_empty_list(db):
    _clear_processes(db)
    assert db.processes.list_all() == []


# -- resolve_process ---------------------------------------------------------


def test_resolve_process_layers_flags_over_base(db):
    pid = db.processes.create(
        "Resolve", skip_classify=True, skip_regroup=True, review_mode="species",
    )
    flags = db.resolve_process(pid)
    assert flags == {
        **ps._BASE,
        "skip_classify": True,
        "skip_regroup": True,
        "review_mode": "species",
    }
    assert set(flags) == set(ps._BASE)
    # Not the stored dict: no id/name/is_seed/sort_order leak through.
    assert "id" not in flags and "name" not in flags


def test_resolve_process_unknown_id_message(db):
    with pytest.raises(ValueError, match=r"unknown process id: 424242"):
        db.resolve_process(424242)
    with pytest.raises(ValueError, match=r"unknown process id: 'abc'"):
        db.resolve_process("abc")


# -- create ------------------------------------------------------------------


def test_create_commits_and_is_visible_to_another_connection(db):
    pid = db.processes.create("Visible")
    assert isinstance(pid, int)
    assert not db.conn.in_transaction
    row = _raw_row(db, pid)
    assert row["name"] == "Visible"
    assert row["is_seed"] == 0


def test_create_defaults(db):
    pid = db.processes.create("Defaults")
    row = _raw_row(db, pid)
    assert (row["skip_classify"], row["skip_extract_masks"],
            row["skip_eye_keypoints"], row["skip_regroup"],
            row["miss_enabled"]) == (0, 0, 0, 0, 1)
    assert row["review_mode"] is None


def test_create_strips_name_and_stores_flags_as_0_or_1(db):
    pid = db.processes.create(
        "  Padded  ", skip_classify="yes", skip_extract_masks=5,
        skip_eye_keypoints=[1], skip_regroup=0, miss_enabled="",
    )
    row = _raw_row(db, pid)
    assert row["name"] == "Padded"
    assert (row["skip_classify"], row["skip_extract_masks"],
            row["skip_eye_keypoints"], row["skip_regroup"],
            row["miss_enabled"]) == (1, 1, 1, 0, 0)


def test_create_appends_after_max_sort_order(db):
    procs = db.processes.list_all()
    top = max(p["sort_order"] for p in procs)
    db.conn.execute(
        "UPDATE saved_processes SET sort_order = 40 WHERE id = ?", (procs[0]["id"],)
    )
    db.conn.commit()
    pid = db.processes.create("After")
    assert db.processes.get(pid)["sort_order"] == max(top, 40) + 1


def test_create_into_empty_table_starts_sort_order_at_zero(db):
    _clear_processes(db)
    first = db.processes.create("First")
    second = db.processes.create("Second")
    assert db.processes.get(first)["sort_order"] == 0
    assert db.processes.get(second)["sort_order"] == 1


@pytest.mark.parametrize("name", ["", "   ", None, 7])
def test_create_rejects_blank_or_non_string_name(db, name):
    before = db.processes.list_all()
    with pytest.raises(ValueError, match="process name is required"):
        db.processes.create(name)
    assert db.processes.list_all() == before


@pytest.mark.parametrize("mode", ["Species", "whatever", "", 0])
def test_create_rejects_bad_review_mode(db, mode):
    with pytest.raises(ValueError, match=r"review_mode must be 'species' or null"):
        db.processes.create("Bad mode", review_mode=mode)
    assert all(p["name"] != "Bad mode" for p in db.processes.list_all())


def test_create_duplicate_name_message_uses_stripped_name(db):
    db.processes.create("Dup")
    with pytest.raises(ValueError, match=r"a process named 'Dup' already exists") as exc:
        db.processes.create("  Dup ")
    assert isinstance(exc.value.__cause__, sqlite3.IntegrityError)
    assert [p["name"] for p in db.processes.list_all()].count("Dup") == 1


# -- update ------------------------------------------------------------------


def test_update_commits_and_returns_true(db):
    pid = db.processes.create("Before")
    assert db.update_saved_process(pid, name="  After  ", skip_eye_keypoints=True) is True
    assert not db.conn.in_transaction
    row = _raw_row(db, pid)
    assert row["name"] == "After"
    assert row["skip_eye_keypoints"] == 1


def test_update_missing_returns_false_without_opening_transaction(db):
    assert db.update_saved_process(55555, name="Nope") is False
    assert not db.conn.in_transaction


def test_update_with_no_fields_rewrites_same_values(db):
    pid = db.processes.create(
        "Same", skip_classify=True, miss_enabled=False, review_mode="species",
    )
    before = db.processes.get(pid)
    assert db.update_saved_process(pid) is True
    assert db.processes.get(pid) == before


def test_update_each_flag_independently(db):
    pid = db.processes.create("Flags")
    db.update_saved_process(pid, skip_classify=True)
    db.update_saved_process(pid, skip_extract_masks=True)
    db.update_saved_process(pid, skip_regroup=True)
    db.update_saved_process(pid, miss_enabled=False)
    proc = db.processes.get(pid)
    assert proc["skip_classify"] is True
    assert proc["skip_extract_masks"] is True
    assert proc["skip_eye_keypoints"] is False
    assert proc["skip_regroup"] is True
    assert proc["miss_enabled"] is False
    # False is a real value, not "leave unchanged" (only None is).
    db.update_saved_process(pid, skip_classify=False)
    assert db.processes.get(pid)["skip_classify"] is False


def test_update_preserves_is_seed_and_sort_order(db):
    seed = db.processes.list_all()[0]
    assert db.update_saved_process(seed["id"], name="Renamed seed")
    after = db.processes.get(seed["id"])
    assert after["is_seed"] is True
    assert after["sort_order"] == seed["sort_order"]


def test_update_review_mode_sentinel_vs_explicit_none(db):
    pid = db.processes.create("Review", review_mode="species")
    db.update_saved_process(pid, name="Review2")
    assert db.processes.get(pid)["review_mode"] == "species"
    db.update_saved_process(pid, review_mode=None)
    assert db.processes.get(pid)["review_mode"] is None


def test_update_bad_review_mode_leaves_row_untouched(db):
    pid = db.processes.create("Keep")
    before = db.processes.get(pid)
    with pytest.raises(ValueError, match=r"review_mode must be 'species' or null"):
        db.update_saved_process(pid, name="Changed", review_mode="nope")
    assert db.processes.get(pid) == before


def test_update_blank_name_rejected(db):
    pid = db.processes.create("Named")
    with pytest.raises(ValueError, match="process name is required"):
        db.update_saved_process(pid, name="   ")
    assert db.processes.get(pid)["name"] == "Named"


def test_update_duplicate_name_message_uses_stripped_name(db):
    db.processes.create("Taken")
    pid = db.processes.create("Mine")
    with pytest.raises(ValueError, match=r"a process named 'Taken' already exists") as exc:
        db.update_saved_process(pid, name=" Taken  ")
    assert isinstance(exc.value.__cause__, sqlite3.IntegrityError)
    assert db.processes.get(pid)["name"] == "Mine"


# -- delete ------------------------------------------------------------------


def test_delete_commits_and_is_visible_to_another_connection(db):
    pid = db.processes.create("Gone")
    assert db.delete_saved_process(pid) is True
    assert not db.conn.in_transaction
    assert _raw_row(db, pid) is None


def test_delete_missing_returns_false_and_leaves_workspaces_alone(db):
    ws = db.create_workspace(
        "WS", config_overrides={"pipeline": {"default_process_id": 31337}},
    )
    before = _overrides_raw(db, ws)
    assert db.delete_saved_process(31337) is False
    assert not db.conn.in_transaction
    assert _overrides_raw(db, ws) == before


def test_delete_rewrites_only_the_matching_pointer(db):
    pid = db.processes.create("Target")
    ws = db.create_workspace(
        "WS",
        config_overrides={
            "pipeline": {"default_process_id": pid, "w_focus": 0.5},
            "other": {"x": 1},
        },
    )
    db.delete_saved_process(pid)
    conn = _other_conn(db)
    try:
        raw = conn.execute(
            "SELECT config_overrides FROM workspaces WHERE id = ?", (ws,)
        ).fetchone()[0]
    finally:
        conn.close()
    assert json.loads(raw) == {
        "pipeline": {"default_process_id": None, "w_focus": 0.5},
        "other": {"x": 1},
    }


def test_delete_rewrites_every_referencing_workspace(db):
    pid = db.processes.create("Shared")
    ws_ids = [
        db.create_workspace(
            f"WS{i}", config_overrides={"pipeline": {"default_process_id": pid}},
        )
        for i in range(3)
    ]
    db.delete_saved_process(pid)
    for ws in ws_ids:
        assert json.loads(_overrides_raw(db, ws))["pipeline"]["default_process_id"] is None


@pytest.mark.parametrize(
    "raw",
    [
        "{",                                   # malformed JSON
        "",                                    # empty string
        "[1, 2]",                              # JSON but not an object
        '"pipeline"',                          # JSON scalar
        '{"pipeline": 5}',                     # pipeline not a dict
        '{"pipeline": [1]}',                   # pipeline a list
        '{"other": {"default_process_id": 1}}',  # no pipeline key
        '{"pipeline": {"w_focus": 0.5}}',      # pipeline without the pointer
    ],
)
def test_delete_skips_workspaces_it_cannot_or_need_not_rewrite(db, raw):
    pid = db.processes.create("Skip")
    ws = db.create_workspace("WS")
    _set_overrides_raw(db, ws, raw)
    assert db.delete_saved_process(pid) is True
    assert _overrides_raw(db, ws) == raw
    assert db.processes.get(pid) is None


def test_delete_skips_null_overrides(db):
    pid = db.processes.create("Nulls")
    ws = db.create_workspace("WS")
    assert _overrides_raw(db, ws) is None
    assert db.delete_saved_process(pid) is True
    assert _overrides_raw(db, ws) is None


def test_delete_compares_pointer_by_equality(db):
    # A string pointer "N" is not the integer id N; only == matches rewrite.
    pid = db.processes.create("Typed pointer")
    ws_str = db.create_workspace(
        "WSstr", config_overrides={"pipeline": {"default_process_id": str(pid)}},
    )
    db.delete_saved_process(pid)
    assert json.loads(_overrides_raw(db, ws_str))["pipeline"]["default_process_id"] == str(pid)


def test_delete_mixed_rows_rewrites_matches_and_skips_malformed(db):
    pid = db.processes.create("Mixed")
    bad = db.create_workspace("Bad")
    good = db.create_workspace(
        "Good", config_overrides={"pipeline": {"default_process_id": pid}},
    )
    _set_overrides_raw(db, bad, "{not json")
    assert db.delete_saved_process(pid) is True
    assert _overrides_raw(db, bad) == "{not json"
    assert json.loads(_overrides_raw(db, good))["pipeline"]["default_process_id"] is None


def test_static_helpers_stay_callable_on_database(db):
    """The private static helpers remain on ``Database`` for any caller."""
    pid = db.processes.create("Static", skip_regroup=True)
    row = db.conn.execute(
        "SELECT * FROM saved_processes WHERE id = ?", (pid,)
    ).fetchone()
    assert Database._saved_process_row_to_dict(row) == db.processes.get(pid)
    assert db._saved_process_row_to_dict(row)["skip_regroup"] is True
    assert Database._normalize_process_fields(
        " N ", 1, 0, "x", None, True, "species",
    ) == ("N", 1, 0, 1, 0, 1, "species")
    with pytest.raises(ValueError, match="process name is required"):
        Database._normalize_process_fields("", 0, 0, 0, 0, 0, None)
    with pytest.raises(ValueError, match=r"review_mode must be 'species' or null"):
        Database._normalize_process_fields("ok", 0, 0, 0, 0, 0, "x")


def test_saved_processes_do_not_require_an_active_workspace(db):
    db.set_active_workspace(None)
    pid = db.processes.create("Global")
    assert db.processes.get(pid)["name"] == "Global"
    assert any(p["id"] == pid for p in db.processes.list_all())
    assert db.resolve_process(pid)["skip_classify"] is False
    assert db.update_saved_process(pid, skip_classify=True) is True
    assert db.delete_saved_process(pid) is True


# -- structure: the saved-process SQL lives in the repository ------------------

# Coordinated Database methods over repositories/processes.py: each checks
# the row exists before writing, so it stays on the façade; none may reach the
# connection directly again. ``resolve_process`` has no SQL and composes
# ``db.processes.get``, so it is not listed.
_DELEGATING_PROCESS_METHODS = (
    "update_saved_process",
    "delete_saved_process",
)

# The forwarding wrappers ``db.processes`` replaced.
_REMOVED_PROCESS_WRAPPERS = (
    "get_saved_processes",
    "get_saved_process",
    "create_saved_process",
)

# Pure static helpers that forward to the repository's static helpers.
_DELEGATING_PROCESS_STATICMETHODS = (
    "_saved_process_row_to_dict",
    "_normalize_process_fields",
)


def _parsed(name):
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    return ast.parse(source).body[0]


@pytest.mark.parametrize("name", _DELEGATING_PROCESS_METHODS)
def test_process_method_delegates_to_repository(name):
    attrs = {
        node.attr
        for node in ast.walk(_parsed(name))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to ProcessesRepository"
    )
    assert "_processes_repository" in attrs, (
        f"Database.{name} no longer delegates to ProcessesRepository"
    )


@pytest.mark.parametrize("name", _DELEGATING_PROCESS_STATICMETHODS)
def test_process_staticmethod_delegates_to_repository(name):
    assert isinstance(inspect.getattr_static(Database, name), staticmethod)
    fn = _parsed(name)
    names = {n.id for n in ast.walk(fn) if isinstance(n, ast.Name)}
    assert "ProcessesRepository" in names, (
        f"Database.{name} no longer forwards to ProcessesRepository"
    )
    assert "bool" not in names and "int" not in names, (
        f"Database.{name} coerces fields itself; keep that in ProcessesRepository"
    )


def test_update_saved_process_shares_the_unset_sentinel_with_the_repository():
    import db as db_module
    from repositories import UNSET
    from repositories.processes import ProcessesRepository

    assert db_module._UNSET is UNSET
    facade = inspect.signature(Database.update_saved_process).parameters
    repo = inspect.signature(ProcessesRepository.update).parameters
    assert facade["review_mode"].default is UNSET
    assert repo["review_mode"].default is UNSET


def test_process_wrapper_signatures_are_unchanged():
    sig = inspect.signature(ProcessesRepository.create)
    assert [(p.name, p.kind.name, p.default) for p in sig.parameters.values()] == [
        ("self", "POSITIONAL_OR_KEYWORD", inspect.Parameter.empty),
        ("name", "POSITIONAL_OR_KEYWORD", inspect.Parameter.empty),
        ("skip_classify", "KEYWORD_ONLY", False),
        ("skip_extract_masks", "KEYWORD_ONLY", False),
        ("skip_eye_keypoints", "KEYWORD_ONLY", False),
        ("skip_regroup", "KEYWORD_ONLY", False),
        ("miss_enabled", "KEYWORD_ONLY", True),
        ("review_mode", "KEYWORD_ONLY", None),
    ]
    sig = inspect.signature(Database.update_saved_process)
    assert list(sig.parameters) == [
        "self", "process_id", "name", "skip_classify", "skip_extract_masks",
        "skip_eye_keypoints", "skip_regroup", "miss_enabled", "review_mode",
    ]
    assert all(
        sig.parameters[k].default is None
        for k in ("name", "skip_classify", "skip_extract_masks",
                  "skip_eye_keypoints", "skip_regroup", "miss_enabled")
    )


def test_existence_checks_go_through_the_repository_get(db, monkeypatch):
    """update/delete ask ``ProcessesRepository.get`` whether the row exists.

    The patch goes on the class: ``db.processes`` builds a fresh repository on
    every access, so a class-level patch reaches every existence check.
    """
    pid = db.processes.create("Patched")
    monkeypatch.setattr(ProcessesRepository, "get", lambda self, process_id: None)
    assert db.update_saved_process(pid, name="Nope") is False
    assert db.delete_saved_process(pid) is False
    with pytest.raises(ValueError, match="unknown process id"):
        db.resolve_process(pid)
    monkeypatch.undo()
    assert db.processes.get(pid)["name"] == "Patched"


def test_processes_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.processes`` builds a new repository each time, never a cached one."""
    first, second = db.processes, db.processes
    assert isinstance(first, ProcessesRepository)
    assert first is not second
    assert first.conn is db.conn


def test_processes_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.processes``; Database keeps no aliases."""
    for name in _REMOVED_PROCESS_WRAPPERS:
        assert not hasattr(Database, name), f"Database.{name} came back; call db.processes"
    accessor = Database.__dict__["processes"]
    assert isinstance(accessor, property)
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(textwrap.dedent(inspect.getsource(accessor.fget))))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_processes_repository" in attrs
    assert "conn" not in attrs


def test_production_code_updates_and_deletes_through_the_facade():
    """Nothing outside the data layer skips the existence check.

    ``ProcessesRepository.update`` / ``delete`` assume the row exists (update
    needs the current row to merge over); ``Database.update_saved_process`` /
    ``delete_saved_process`` check first.
    """
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    pattern = re.compile(r"\.processes\.(update|delete)\(")
    offenders = []
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root)
        if rel.parts[0] in ("tests", "repositories") or rel.name == "db.py":
            continue
        text = path.read_text(encoding="utf-8")
        offenders += [f"{rel}: {m.group(0)}" for m in pattern.finditer(text)]
    assert offenders == [], (
        "Call db.update_saved_process / db.delete_saved_process instead: "
        f"{offenders}"
    )
