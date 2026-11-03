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
``in_transaction``, ``begin``, ``begin_immediate``) and
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
from new_images import get_new_images_for_workspace

# Methods that own the connection itself rather than a domain's SQL.
CONNECTION_LIFECYCLE = {
    # Opens the connection and sets its PRAGMAs and SQL functions.
    "__init__",
    "close",
    # Public transaction control, so callers outside the data layer never
    # reach for the connection. Each is the one sqlite3.Connection call it
    # names:
    # Reports whether the connection has a transaction open.
    "in_transaction",
    # Commits the open transaction; refused inside a unit of work
    # (``unit_of_work.GuardedConnection``).
    "commit",
    # Rolls the connection's open transaction back.
    "rollback",
    # Takes SQLite's writer lock up front (the prediction-decision lock).
    "begin_immediate",
    # Opens a deferred transaction (a read snapshot for multi-query reads).
    "begin",
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


def test_pathlike_catalog_shares_cache_key_with_string_path(tmp_path):
    path = tmp_path / "catalog.db"
    with Database(path) as path_db, Database(str(path)) as string_db:
        assert path_db._db_path == string_db._db_path == str(path)
        workspace_id = path_db.require_workspace_id()
        expected = {"new_count": 7}
        path_db._new_images_cache.set(path_db._db_path, workspace_id, expected)
        assert get_new_images_for_workspace(string_db, workspace_id) == expected


def test_execute_with_retry_returns_a_cursor_or_raises(db):
    from db import execute_with_retry

    cursor = execute_with_retry(db.conn, "SELECT :value", {"value": 7}, max_retries=0)
    assert isinstance(cursor, sqlite3.Cursor)
    assert cursor.fetchone()[0] == 7
    with pytest.raises(ValueError, match="max_retries"):
        execute_with_retry(db.conn, "SELECT 1", max_retries=-1)
    with pytest.raises(sqlite3.OperationalError):
        execute_with_retry(db.conn, "SELECT * FROM nonexistent", max_retries=0)


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


def test_commit_is_refused_inside_a_unit_of_work(db):
    from unit_of_work import TransactionOwnershipError

    with db.transaction():
        _write(db, "a")
        with pytest.raises(TransactionOwnershipError):
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
    with db.transaction(immediate=True):
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
    with pytest.raises(RuntimeError, match="boom"), db.transaction(immediate=True):
        _write(db, "a")
        raise RuntimeError("boom")
    assert not db.in_transaction
    assert _committed_markers(db) == 0


def test_nested_transaction_commits_with_its_owner(db):
    # A nested unit is a savepoint: nothing is durable until the outermost
    # unit commits (``test_unit_of_work.py`` covers the rest).
    with db.transaction():
        with db.transaction():
            _write(db, "a")
        assert db.in_transaction
        assert _committed_markers(db) == 0
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
FORWARDING_WRAPPER_LIMIT = 11


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


# -- repositories depend only downward ----------------------------------------------

# A repository takes its connection, the workspace resolver and the pure
# helpers or constants it needs; it never receives the ``Database`` or calls
# back into it. Logic that needs another domain composes that domain's
# repository on the same connection, or moves to a service. Two kinds of
# back-binding remain, each an exact inventory that may only shrink:
#
# - ``FACADE_BINDING_MODULES``: repository modules that copy ``Database``
#   methods onto themselves by name (``FACADE_METHODS``) or take the
#   ``Database`` itself as a ``facade`` argument.
# - ``FACTORY_CALLBACKS``: the ``Database`` methods each ``Database`` method
#   still hands to a repository constructor, keyed by the handing method.
#   Domain accessors (properties that build another repository) and class
#   constants are not callbacks and are not listed.
FACADE_BINDING_MODULES = {
    "keyword_provenance",
    "predictions",
}

FACTORY_CALLBACKS = {
    "_keyword_provenance_repository": {"<Database>"},
    "_location_repository": {"_merge_keyword_into"},
    "_moves_merge_repository": {"_carry_keyword_associations_for_merge"},
    "_prediction_repository": {"<Database>"},
}

# The active-workspace resolver a factory may hand over: a repository takes the
# workspace id or this resolver, never anything else of the façade.
WORKSPACE_RESOLVERS = {"_ws_id"}


def _repository_modules():
    from pathlib import Path

    root = Path(inspect.getsourcefile(Database)).parent / "repositories"
    for path in sorted(root.glob("*.py")):
        yield path.stem, ast.parse(path.read_text(encoding="utf-8"))


def _binds_the_facade(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "FACADE_METHODS"
            for target in node.targets
        ):
            return True
        if isinstance(node, ast.arg) and node.arg == "facade":
            return True
    return False


def test_facade_binding_repositories_only_shrink():
    binding = {name for name, tree in _repository_modules() if _binds_the_facade(tree)}
    assert binding == FACADE_BINDING_MODULES, (
        "Repository modules that bind Database methods by name (FACADE_METHODS) "
        "or take the Database as `facade` changed. A repository composes other "
        "repositories on its own connection, or the cross-domain logic moves to "
        "a service; never add one. When you remove one, drop it from "
        "FACADE_BINDING_MODULES.\n"
        f"  found:    {sorted(binding)}\n"
        f"  expected: {sorted(FACADE_BINDING_MODULES)}"
    )


def _factory_callbacks():
    methods = {
        fn.name for fn in _database_methods()
        if not any(
            isinstance(d, ast.Name) and d.id in ("property", "staticmethod")
            for d in fn.decorator_list
        )
    }
    found = {}
    for fn in _database_methods():
        for node in ast.walk(fn):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id.endswith("Repository")):
                continue
            for arg in [*node.args, *(kw.value for kw in node.keywords)]:
                found_in = _callbacks_in(arg, methods)
                if found_in:
                    found.setdefault(fn.name, set()).update(found_in)
    return found


def _callbacks_in(expr, methods):
    """The ``Database`` callbacks an argument expression hands over.

    A bare ``self`` anywhere in it (passed directly, or closed over by a
    lambda) is ``<Database>``; ``self.<method>`` is that method unless it is
    the workspace resolver. ``self.conn``, domain accessors (properties),
    class constants and the value a method call returns are not callbacks.
    """
    parents = {child: node for node in ast.walk(expr) for child in ast.iter_child_nodes(node)}
    found = set()
    for sub in ast.walk(expr):
        if not (isinstance(sub, ast.Name) and sub.id == "self"):
            continue
        parent = parents.get(sub)
        if isinstance(parent, ast.Attribute) and parent.value is sub:
            called = isinstance(parents.get(parent), ast.Call) and parents[parent].func is parent
            if parent.attr in methods and parent.attr not in WORKSPACE_RESOLVERS and not called:
                found.add(parent.attr)
        else:
            found.add("<Database>")
    return found


def test_repository_factories_hand_over_no_database_callbacks():
    found = _factory_callbacks()
    assert found == FACTORY_CALLBACKS, (
        "The Database methods handed to repository constructors changed. A "
        "repository must not call back into Database: import the helper it "
        "needs, build the other domain's repository on its own connection, or "
        "move the composition to a service. When you remove a callback, drop "
        "it from FACTORY_CALLBACKS.\n"
        f"  found:    {found}\n"
        f"  expected: {FACTORY_CALLBACKS}"
    )


def test_backbinding_detectors_match_known_shapes():
    """Pin both detectors so the inventories count what they claim to."""
    assert _binds_the_facade(ast.parse("FACADE_METHODS = ('a',)"))
    assert _binds_the_facade(ast.parse("class R:\n    def __init__(self, conn, *, facade):\n        pass"))
    assert not _binds_the_facade(ast.parse("class R:\n    def __init__(self, conn):\n        pass"))
    methods = {"effective_config", "_ws_id"}

    def callbacks(src):
        return _callbacks_in(ast.parse(src, mode="eval").body, methods)

    assert callbacks("self") == {"<Database>"}
    assert callbacks("lambda a: helper(self, a)") == {"<Database>"}
    assert callbacks("self.effective_config") == {"effective_config"}
    assert callbacks("self._ws_id") == set()
    assert callbacks("self.conn") == set()
    assert callbacks("self.moves_merge.transfer_gps_review") == set()
    assert callbacks("self.PHOTO_COLS") == set()
    assert callbacks("self.effective_config()") == set()
