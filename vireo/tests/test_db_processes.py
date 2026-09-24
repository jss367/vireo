"""Behavior pins for the saved-processes domain of ``Database``.

These tests exercise the saved-process methods only through the public
``Database`` façade, so they hold whether the SQL lives in ``db.py`` or in
``repositories/processes.py``. They pin return shapes and ordering, field
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
    pid = db.create_saved_process(
        "Typed", skip_classify=True, miss_enabled=False, review_mode="species",
    )
    proc = db.get_saved_process(pid)
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
    assert db.get_saved_process(987654) is None


def test_get_saved_process_coerces_nonzero_integers_to_true(db):
    pid = db.create_saved_process("Raw")
    db.conn.execute(
        "UPDATE saved_processes SET skip_regroup = 7, is_seed = 2 WHERE id = ?",
        (pid,),
    )
    db.conn.commit()
    proc = db.get_saved_process(pid)
    assert proc["skip_regroup"] is True
    assert proc["is_seed"] is True


def test_get_saved_processes_orders_by_sort_order_then_id(db):
    _clear_processes(db)
    a = db.create_saved_process("A")
    b = db.create_saved_process("B")
    c = db.create_saved_process("C")
    # Tie a and c on sort_order 5 and put b first: id breaks the tie.
    db.conn.execute("UPDATE saved_processes SET sort_order = 5 WHERE id IN (?, ?)", (a, c))
    db.conn.execute("UPDATE saved_processes SET sort_order = 1 WHERE id = ?", (b,))
    db.conn.commit()
    procs = db.get_saved_processes()
    assert type(procs) is list
    assert [p["id"] for p in procs] == [b, a, c]
    assert all(set(p) == _DICT_KEYS for p in procs)


def test_get_saved_processes_empty_table_returns_empty_list(db):
    _clear_processes(db)
    assert db.get_saved_processes() == []


# -- resolve_process ---------------------------------------------------------


def test_resolve_process_layers_flags_over_base(db):
    pid = db.create_saved_process(
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
    pid = db.create_saved_process("Visible")
    assert isinstance(pid, int)
    assert not db.conn.in_transaction
    row = _raw_row(db, pid)
    assert row["name"] == "Visible"
    assert row["is_seed"] == 0


def test_create_defaults(db):
    pid = db.create_saved_process("Defaults")
    row = _raw_row(db, pid)
    assert (row["skip_classify"], row["skip_extract_masks"],
            row["skip_eye_keypoints"], row["skip_regroup"],
            row["miss_enabled"]) == (0, 0, 0, 0, 1)
    assert row["review_mode"] is None


def test_create_strips_name_and_stores_flags_as_0_or_1(db):
    pid = db.create_saved_process(
        "  Padded  ", skip_classify="yes", skip_extract_masks=5,
        skip_eye_keypoints=[1], skip_regroup=0, miss_enabled="",
    )
    row = _raw_row(db, pid)
    assert row["name"] == "Padded"
    assert (row["skip_classify"], row["skip_extract_masks"],
            row["skip_eye_keypoints"], row["skip_regroup"],
            row["miss_enabled"]) == (1, 1, 1, 0, 0)


def test_create_appends_after_max_sort_order(db):
    procs = db.get_saved_processes()
    top = max(p["sort_order"] for p in procs)
    db.conn.execute(
        "UPDATE saved_processes SET sort_order = 40 WHERE id = ?", (procs[0]["id"],)
    )
    db.conn.commit()
    pid = db.create_saved_process("After")
    assert db.get_saved_process(pid)["sort_order"] == max(top, 40) + 1


def test_create_into_empty_table_starts_sort_order_at_zero(db):
    _clear_processes(db)
    first = db.create_saved_process("First")
    second = db.create_saved_process("Second")
    assert db.get_saved_process(first)["sort_order"] == 0
    assert db.get_saved_process(second)["sort_order"] == 1


@pytest.mark.parametrize("name", ["", "   ", None, 7])
def test_create_rejects_blank_or_non_string_name(db, name):
    before = db.get_saved_processes()
    with pytest.raises(ValueError, match="process name is required"):
        db.create_saved_process(name)
    assert db.get_saved_processes() == before


@pytest.mark.parametrize("mode", ["Species", "whatever", "", 0])
def test_create_rejects_bad_review_mode(db, mode):
    with pytest.raises(ValueError, match=r"review_mode must be 'species' or null"):
        db.create_saved_process("Bad mode", review_mode=mode)
    assert all(p["name"] != "Bad mode" for p in db.get_saved_processes())


def test_create_duplicate_name_message_uses_stripped_name(db):
    db.create_saved_process("Dup")
    with pytest.raises(ValueError, match=r"a process named 'Dup' already exists") as exc:
        db.create_saved_process("  Dup ")
    assert isinstance(exc.value.__cause__, sqlite3.IntegrityError)
    assert [p["name"] for p in db.get_saved_processes()].count("Dup") == 1


# -- update ------------------------------------------------------------------


def test_update_commits_and_returns_true(db):
    pid = db.create_saved_process("Before")
    assert db.update_saved_process(pid, name="  After  ", skip_eye_keypoints=True) is True
    assert not db.conn.in_transaction
    row = _raw_row(db, pid)
    assert row["name"] == "After"
    assert row["skip_eye_keypoints"] == 1


def test_update_missing_returns_false_without_opening_transaction(db):
    assert db.update_saved_process(55555, name="Nope") is False
    assert not db.conn.in_transaction


def test_update_with_no_fields_rewrites_same_values(db):
    pid = db.create_saved_process(
        "Same", skip_classify=True, miss_enabled=False, review_mode="species",
    )
    before = db.get_saved_process(pid)
    assert db.update_saved_process(pid) is True
    assert db.get_saved_process(pid) == before


def test_update_each_flag_independently(db):
    pid = db.create_saved_process("Flags")
    db.update_saved_process(pid, skip_classify=True)
    db.update_saved_process(pid, skip_extract_masks=True)
    db.update_saved_process(pid, skip_regroup=True)
    db.update_saved_process(pid, miss_enabled=False)
    proc = db.get_saved_process(pid)
    assert proc["skip_classify"] is True
    assert proc["skip_extract_masks"] is True
    assert proc["skip_eye_keypoints"] is False
    assert proc["skip_regroup"] is True
    assert proc["miss_enabled"] is False
    # False is a real value, not "leave unchanged" (only None is).
    db.update_saved_process(pid, skip_classify=False)
    assert db.get_saved_process(pid)["skip_classify"] is False


def test_update_preserves_is_seed_and_sort_order(db):
    seed = db.get_saved_processes()[0]
    assert db.update_saved_process(seed["id"], name="Renamed seed")
    after = db.get_saved_process(seed["id"])
    assert after["is_seed"] is True
    assert after["sort_order"] == seed["sort_order"]


def test_update_review_mode_sentinel_vs_explicit_none(db):
    pid = db.create_saved_process("Review", review_mode="species")
    db.update_saved_process(pid, name="Review2")
    assert db.get_saved_process(pid)["review_mode"] == "species"
    db.update_saved_process(pid, review_mode=None)
    assert db.get_saved_process(pid)["review_mode"] is None


def test_update_bad_review_mode_leaves_row_untouched(db):
    pid = db.create_saved_process("Keep")
    before = db.get_saved_process(pid)
    with pytest.raises(ValueError, match=r"review_mode must be 'species' or null"):
        db.update_saved_process(pid, name="Changed", review_mode="nope")
    assert db.get_saved_process(pid) == before


def test_update_blank_name_rejected(db):
    pid = db.create_saved_process("Named")
    with pytest.raises(ValueError, match="process name is required"):
        db.update_saved_process(pid, name="   ")
    assert db.get_saved_process(pid)["name"] == "Named"


def test_update_duplicate_name_message_uses_stripped_name(db):
    db.create_saved_process("Taken")
    pid = db.create_saved_process("Mine")
    with pytest.raises(ValueError, match=r"a process named 'Taken' already exists") as exc:
        db.update_saved_process(pid, name=" Taken  ")
    assert isinstance(exc.value.__cause__, sqlite3.IntegrityError)
    assert db.get_saved_process(pid)["name"] == "Mine"


# -- delete ------------------------------------------------------------------


def test_delete_commits_and_is_visible_to_another_connection(db):
    pid = db.create_saved_process("Gone")
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
    pid = db.create_saved_process("Target")
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
    pid = db.create_saved_process("Shared")
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
    pid = db.create_saved_process("Skip")
    ws = db.create_workspace("WS")
    _set_overrides_raw(db, ws, raw)
    assert db.delete_saved_process(pid) is True
    assert _overrides_raw(db, ws) == raw
    assert db.get_saved_process(pid) is None


def test_delete_skips_null_overrides(db):
    pid = db.create_saved_process("Nulls")
    ws = db.create_workspace("WS")
    assert _overrides_raw(db, ws) is None
    assert db.delete_saved_process(pid) is True
    assert _overrides_raw(db, ws) is None


def test_delete_compares_pointer_by_equality(db):
    # A string pointer "N" is not the integer id N; only == matches rewrite.
    pid = db.create_saved_process("Typed pointer")
    ws_str = db.create_workspace(
        "WSstr", config_overrides={"pipeline": {"default_process_id": str(pid)}},
    )
    db.delete_saved_process(pid)
    assert json.loads(_overrides_raw(db, ws_str))["pipeline"]["default_process_id"] == str(pid)


def test_delete_mixed_rows_rewrites_matches_and_skips_malformed(db):
    pid = db.create_saved_process("Mixed")
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
    pid = db.create_saved_process("Static", skip_regroup=True)
    row = db.conn.execute(
        "SELECT * FROM saved_processes WHERE id = ?", (pid,)
    ).fetchone()
    assert Database._saved_process_row_to_dict(row) == db.get_saved_process(pid)
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
    pid = db.create_saved_process("Global")
    assert db.get_saved_process(pid)["name"] == "Global"
    assert any(p["id"] == pid for p in db.get_saved_processes())
    assert db.resolve_process(pid)["skip_classify"] is False
    assert db.update_saved_process(pid, skip_classify=True) is True
    assert db.delete_saved_process(pid) is True


# -- structure: the saved-process SQL lives in the repository ------------------

# Database methods whose SQL moved to repositories/processes.py. Each stays on
# Database as a thin wrapper so existing call sites keep working; none may
# reach the connection directly again. ``resolve_process`` has no SQL and
# composes ``get_saved_process`` on the façade, so it is not listed.
_DELEGATING_PROCESS_METHODS = (
    "get_saved_processes",
    "get_saved_process",
    "create_saved_process",
    "update_saved_process",
    "delete_saved_process",
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
    sig = inspect.signature(Database.create_saved_process)
    assert str(sig) == (
        "(self, name, *, skip_classify=False, skip_extract_masks=False, "
        "skip_eye_keypoints=False, skip_regroup=False, miss_enabled=True, "
        "review_mode=None)"
    )
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


def test_existence_checks_go_through_the_facade(db, monkeypatch):
    """update/delete ask ``Database.get_saved_process`` whether the row exists."""
    pid = db.create_saved_process("Patched")
    monkeypatch.setattr(db, "get_saved_process", lambda process_id: None)
    assert db.update_saved_process(pid, name="Nope") is False
    assert db.delete_saved_process(pid) is False
    monkeypatch.undo()
    assert db.get_saved_process(pid)["name"] == "Patched"
