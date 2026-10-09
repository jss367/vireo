"""Behavior pins for the ``local_folders`` reads on ``Database``.

The tests go through the ``db.local_folders`` accessor; the structural tests
at the end pin the accessor's shape and keep the old forwarding wrapper gone.
"""

import ast
import inspect
import textwrap

from db import Database
from repositories.local_folders import LocalFolderRepository


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


def test_state_rows_reads_the_requested_roots_in_id_order(db):
    a, b, c, d = (_folder(db, f"/lf/{name}") for name in "abcd")
    _local(db, c, "active", created_at=3.0, activated_at=4.0)
    _local(db, a, "staging", created_at=1.0)
    _local(db, d, "syncing", created_at=5.0, activated_at=6.0)
    db.set_active_workspace(None)  # catalog-wide: no workspace needed

    rows = db.local_folders.state_rows([c, b, a])
    assert [dict(r) for r in rows] == [
        {"root_folder_id": a, "state": "staging", "activated_at": None, "created_at": 1.0},
        {"root_folder_id": c, "state": "active", "activated_at": 4.0, "created_at": 3.0},
    ]


def test_state_rows_is_one_statement(db):
    roots = [_folder(db, f"/lf/many{i}") for i in range(5)]
    for root in roots:
        _local(db, root, "active")
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        rows = db.local_folders.state_rows(reversed(roots))
    finally:
        db.conn.set_trace_callback(None)
    assert [r["root_folder_id"] for r in rows] == sorted(roots)
    assert len([s for s in statements if "FROM local_folders" in s]) == 1


def test_state_rows_with_no_ids_reads_nothing(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        assert db.local_folders.state_rows([]) == []
    finally:
        db.conn.set_trace_callback(None)
    assert statements == []


# -- structure ------------------------------------------------------------------


def test_local_folders_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.local_folders`` builds a new repository each time, never a cached one."""
    first, second = db.local_folders, db.local_folders
    assert isinstance(first, LocalFolderRepository)
    assert first is not second
    assert first.conn is db.conn


def test_local_folders_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.local_folders``; Database keeps no aliases."""
    assert not hasattr(Database, "get_local_folder_states"), (
        "Database.get_local_folder_states came back; call db.local_folders"
    )
    accessor = Database.__dict__["local_folders"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_local_folder_repository" in attrs
    assert "conn" not in attrs
