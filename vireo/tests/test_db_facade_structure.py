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
connection-lifecycle methods listed below. The tests at the end pin the
public transaction-control methods among them (``commit``, ``rollback``,
``in_transaction``, ``begin_immediate``) to the connection calls they replace,
and ``commit_with_retry`` (which only hands the connection to
``db.commit_with_retry``, so the guard needs no exception for it) to that
helper.
"""

import ast
import contextlib
import inspect
import sqlite3

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
    # Public transaction control, so callers outside the data layer never
    # reach for the connection. Each is the one sqlite3.Connection call it
    # names:
    # Reports whether the connection has a transaction open.
    "in_transaction",
    # Commits through _Connection.commit, so held commits stay no-ops.
    "commit",
    # Rolls the connection's open transaction back.
    "rollback",
    # Takes SQLite's writer lock up front (the prediction-decision lock).
    "begin_immediate",
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


# -- transaction control ------------------------------------------------------------------


def _committed_markers(db):
    """Test rows in ``db_meta`` as a second connection sees them."""
    with contextlib.closing(sqlite3.connect(db._db_path)) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM db_meta WHERE key LIKE 'tx-test-%'"
        ).fetchone()[0]


def _write(db, name):
    db.conn.execute(
        "INSERT INTO db_meta (key, value) VALUES (?, '1')", (f"tx-test-{name}",),
    )


def test_commit_rollback_and_in_transaction(db):
    assert not db.in_transaction
    _write(db, "a")
    assert db.in_transaction
    db.rollback()
    assert not db.in_transaction
    assert _committed_markers(db) == 0
    _write(db, "b")
    db.commit()
    assert not db.in_transaction
    assert _committed_markers(db) == 1


def test_commit_is_a_no_op_while_commits_are_held(db):
    with db._commits_held():
        _write(db, "a")
        db.commit()
        assert db.in_transaction
        assert _committed_markers(db) == 0
    assert not db.in_transaction
    assert _committed_markers(db) == 1


def test_begin_immediate_holds_the_writer_lock(db):
    db.begin_immediate()
    assert db.in_transaction
    with contextlib.closing(sqlite3.connect(db._db_path, timeout=0)) as other:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            other.execute("BEGIN IMMEDIATE")
    # BEGIN does not nest: a caller must commit or roll back first.
    with pytest.raises(sqlite3.OperationalError):
        db.begin_immediate()
    db.rollback()
    assert not db.in_transaction


def test_commit_with_retry_commits_through_the_module_helper(db, monkeypatch):
    import db as db_module

    calls = []
    real = db_module.commit_with_retry

    def recording(conn, *args, **kwargs):
        calls.append(conn)
        return real(conn, *args, **kwargs)

    monkeypatch.setattr(db_module, "commit_with_retry", recording)
    _write(db, "a")
    db.commit_with_retry()
    assert calls == [db.conn]
    assert not db.in_transaction
    assert _committed_markers(db) == 1


def test_commit_with_retry_retries_a_transient_lock(db, monkeypatch):
    import db as db_module

    attempts = []
    real_commit = db.conn.commit

    def flaky_commit():
        attempts.append(1)
        if len(attempts) == 1:
            raise sqlite3.OperationalError("database is locked")
        real_commit()

    monkeypatch.setattr(db_module.time, "sleep", lambda _s: None)
    monkeypatch.setattr(db.conn, "commit", flaky_commit)
    _write(db, "a")
    db.commit_with_retry()
    assert len(attempts) == 2
    assert _committed_markers(db) == 1
