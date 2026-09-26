"""Structural guard for the finished ``db.py`` split.

Every domain's SQL lives in ``vireo/repositories/`` (or, for the canonical
schema, ``vireo/canonical_schema.py``), and ``Database`` is the façade over
them: one-line wrappers, cross-domain composition, the active-workspace state
and process caches. The per-domain ``test_db_<domain>.py`` files each pin
their own moved methods; this test pins the whole class, so a new method that
queries ``self.conn`` directly fails here even if no domain test lists it.

A ``Database`` method may still *hand* its connection to something that runs
SQL — a ``_<domain>_repository()`` factory, ``canonical_schema.create_tables``,
``repositories.collections.remap_collection_photo_ids`` — so ``self.conn``
passed as a call argument is allowed. Anything else (``self.conn.execute``,
``self.conn.commit()``, ``with self.conn:``, aliasing it to a local) is SQL or
transaction control on the façade and belongs in a repository, except in the
connection-lifecycle methods listed below.
"""

import ast
import inspect

import pytest
from db import Database

# Methods that own the connection itself rather than a domain's SQL.
CONNECTION_LIFECYCLE = {
    # Opens the connection and sets its PRAGMAs and SQL functions.
    "__init__",
    "close",
    # Holds the connection's commits so an undo/redo replay is one
    # transaction; it commits or rolls back the connection as a whole.
    "_commits_held",
}


def _database_methods():
    with open(inspect.getsourcefile(Database), encoding="utf-8") as handle:
        tree = ast.parse(handle.read())
    cls = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "Database"
    )
    return [
        node for node in cls.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]


def _is_self_conn(node):
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "conn"
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    )


def _direct_conn_uses(fn):
    """Line numbers where ``fn`` uses ``self.conn`` other than as a call argument."""
    passed = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            passed.update(id(arg) for arg in node.args if _is_self_conn(arg))
            passed.update(
                id(kw.value) for kw in node.keywords if _is_self_conn(kw.value)
            )
    return [
        node.lineno for node in ast.walk(fn)
        if _is_self_conn(node) and id(node) not in passed
    ]


def test_database_methods_run_no_sql_of_their_own():
    offenders = {
        fn.name: _direct_conn_uses(fn)
        for fn in _database_methods()
        if fn.name not in CONNECTION_LIFECYCLE and _direct_conn_uses(fn)
    }
    assert not offenders, (
        "These Database methods use self.conn directly. Move the SQL into the "
        "domain's repository (vireo/repositories/) and leave a one-line "
        "wrapper; see the CLAUDE.md repositories conventions.\n"
        + "\n".join(
            f"  {name}: {len(lines)} uses, first at db.py:{min(lines)}"
            for name, lines in sorted(offenders.items())
        )
    )


@pytest.mark.parametrize("name", sorted(CONNECTION_LIFECYCLE))
def test_connection_lifecycle_allowlist_is_not_stale(name):
    fn = next(f for f in _database_methods() if f.name == name)
    assert _direct_conn_uses(fn), (
        f"Database.{name} no longer uses self.conn; drop it from "
        "CONNECTION_LIFECYCLE so the guard covers it."
    )
