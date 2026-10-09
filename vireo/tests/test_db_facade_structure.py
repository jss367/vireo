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
``in_transaction``, ``begin``, ``begin_immediate``, ``transaction``) and
``set_progress_handler`` to the connection calls they replace, and
``commit_with_retry`` (which only hands the connection to
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
    # Opens a deferred transaction (a read snapshot for multi-query reads).
    "begin",
    # sqlite3's own context manager (``with conn:``): commits on exit and
    # rolls back on error, exactly as the ``with db.conn:`` blocks it replaces.
    "transaction",
    # Installs a SQLite progress handler (search-lane cancellation), a
    # connection setting rather than a statement.
    "set_progress_handler",
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


def test_begin_holds_one_read_snapshot_without_the_writer_lock(db):
    db.begin()
    assert db.in_transaction
    assert _committed_markers(db) == 0
    # The first read fixes the snapshot.
    before = db.conn.execute(
        "SELECT COUNT(*) FROM db_meta WHERE key LIKE 'tx-test-%'"
    ).fetchone()[0]
    with contextlib.closing(sqlite3.connect(db._db_path, timeout=0)) as other:
        # Deferred: another connection can still write and commit.
        other.execute("INSERT INTO db_meta (key, value) VALUES ('tx-test-other', '1')")
        other.commit()
    after = db.conn.execute(
        "SELECT COUNT(*) FROM db_meta WHERE key LIKE 'tx-test-%'"
    ).fetchone()[0]
    assert after == before == 0
    # BEGIN does not nest.
    with pytest.raises(sqlite3.OperationalError):
        db.begin()
    db.rollback()
    assert not db.in_transaction
    assert _committed_markers(db) == 1


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


def test_transaction_commits_on_exit(db):
    with db.transaction():
        db.begin_immediate()
        _write(db, "a")
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


def test_transaction_rolls_back_when_the_block_raises(db):
    with pytest.raises(RuntimeError, match="boom"), db.transaction():
        db.begin_immediate()
        _write(db, "a")
        raise RuntimeError("boom")
    assert not db.in_transaction
    assert _committed_markers(db) == 0


def test_transaction_commits_even_while_commits_are_held(db):
    # ``with conn:`` commits through sqlite3's C-level commit, which
    # ``_Connection.commit``'s hold never sees; ``transaction()`` keeps that.
    with db._commits_held():
        with db.transaction():
            _write(db, "a")
        assert not db.in_transaction
        assert _committed_markers(db) == 1


def test_set_progress_handler_interrupts_and_clears(db):
    db.set_progress_handler(lambda: 1, 1)
    with pytest.raises(sqlite3.OperationalError, match="interrupted"):
        db.conn.execute("SELECT COUNT(*) FROM db_meta").fetchone()
    db.set_progress_handler(None, 0)
    assert db.conn.execute("SELECT 1").fetchone()[0] == 1


# -- forwarding wrappers ---------------------------------------------------------

# How many ``Database`` methods are pure forwarders: one statement that builds
# a repository through ``self._<domain>_repository(...)`` and calls one method
# on it. New persistence operations are reached through a domain accessor
# (``db.job_history.get(...)``, a property that builds a fresh repository per
# access) instead of another alias, so this count may only shrink: lower it
# when you move a domain's callers onto its accessor and delete the wrappers,
# never raise it. Methods that coordinate more than one call (``add_photo``
# resolving duplicates after the insert, cross-domain composition) are not
# forwarders and are not counted.
FORWARDING_WRAPPER_LIMIT = 294


def _is_forwarding_wrapper(fn):
    body = [
        stmt for stmt in fn.body
        if not (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant)
                and isinstance(stmt.value.value, str))
    ]
    if len(body) != 1 or not isinstance(body[0], (ast.Return, ast.Expr)):
        return False
    call = body[0].value
    if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Attribute):
        return False
    factory = call.func.value
    return (
        isinstance(factory, ast.Call)
        and isinstance(factory.func, ast.Attribute)
        and isinstance(factory.func.value, ast.Name)
        and factory.func.value.id == "self"
        and factory.func.attr.startswith("_")
        and factory.func.attr.endswith("_repository")
    )


def test_forwarding_wrappers_only_shrink():
    wrappers = sorted(fn.name for fn in _database_methods() if _is_forwarding_wrapper(fn))
    assert len(wrappers) <= FORWARDING_WRAPPER_LIMIT, (
        f"{len(wrappers)} forwarding wrappers on Database, limit "
        f"{FORWARDING_WRAPPER_LIMIT}. Reach the repository through its domain "
        "accessor (a property such as Database.job_history) instead of adding "
        "another one-line alias."
    )
    assert len(wrappers) >= FORWARDING_WRAPPER_LIMIT, (
        f"Forwarding wrappers fell to {len(wrappers)}; lower "
        "FORWARDING_WRAPPER_LIMIT in vireo/tests/test_db_facade_structure.py to "
        "match so the migration sticks."
    )


def test_forwarding_wrapper_detector_matches_known_shapes():
    """Pin the detector so the cap counts what it claims to count."""
    def parse(src):
        return ast.parse(src).body[0]

    assert _is_forwarding_wrapper(parse(
        'def f(self, x):\n    """Doc."""\n    return self._photos_repository().get(x)'
    ))
    assert _is_forwarding_wrapper(parse(
        "def f(self, x):\n    self._folder_repository(scoped=False).delete(x)"
    ))
    # A domain accessor returns the repository itself; it is not a forwarder.
    assert not _is_forwarding_wrapper(parse(
        "def job_history(self):\n    return self._job_history_repository()"
    ))
    # Coordinated work (more than one statement) is not a forwarder either.
    assert not _is_forwarding_wrapper(parse(
        "def f(self, x):\n    pid = self._photos_repository().add(x)\n    self.resolve(pid)\n    return pid"
    ))
