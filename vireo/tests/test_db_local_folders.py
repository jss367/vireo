"""Behavior pins for the ``local_folders`` reads on ``Database``.

The tests go through the public ``Database`` façade; the structural test at
the end keeps the SQL in ``repositories/local_folders.py``.
"""

import ast
import inspect
import textwrap

import pytest
from db import Database


def _folder(db, path):
    cur = db.conn.execute("INSERT INTO folders (path, name) VALUES (?, ?)", (path, path))
    db.conn.commit()
    return cur.lastrowid


def _local(db, root_id, state, *, created_at=None, activated_at=None):
    db.conn.execute(
        "INSERT INTO local_folders (root_folder_id, state, created_at, activated_at) "
        "VALUES (?, ?, ?, ?)",
        (root_id, state, created_at, activated_at),
    )
    db.conn.commit()


def test_get_local_folder_states_reads_the_requested_roots_in_id_order(db):
    a, b, c, d = (_folder(db, f"/lf/{name}") for name in "abcd")
    _local(db, c, "active", created_at=3.0, activated_at=4.0)
    _local(db, a, "staging", created_at=1.0)
    _local(db, d, "syncing", created_at=5.0, activated_at=6.0)
    db.set_active_workspace(None)  # catalog-wide: no workspace needed

    rows = db.get_local_folder_states([c, b, a])
    assert [dict(r) for r in rows] == [
        {"root_folder_id": a, "state": "staging", "activated_at": None, "created_at": 1.0},
        {"root_folder_id": c, "state": "active", "activated_at": 4.0, "created_at": 3.0},
    ]


def test_get_local_folder_states_is_one_statement(db):
    roots = [_folder(db, f"/lf/many{i}") for i in range(5)]
    for root in roots:
        _local(db, root, "active")
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        rows = db.get_local_folder_states(reversed(roots))
    finally:
        db.conn.set_trace_callback(None)
    assert [r["root_folder_id"] for r in rows] == sorted(roots)
    assert len([s for s in statements if "FROM local_folders" in s]) == 1


def test_get_local_folder_states_with_no_ids_reads_nothing(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        assert db.get_local_folder_states([]) == []
    finally:
        db.conn.set_trace_callback(None)
    assert statements == []


# -- structure ------------------------------------------------------------------


@pytest.mark.parametrize("name", ["get_local_folder_states"])
def test_local_folder_method_delegates_to_repository(name):
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    fn = ast.parse(source).body[0]
    attrs = {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to LocalFolderRepository"
    )
    assert "_local_folder_repository" in attrs, (
        f"Database.{name} no longer delegates to LocalFolderRepository"
    )
