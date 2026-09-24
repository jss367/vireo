"""Behavior pins for the ``db_meta`` key/value domain of ``Database``.

The tests exercise ``get_meta`` and ``set_meta`` only through the public
``Database`` façade, so they hold regardless of whether the SQL lives in
``db.py`` or in a repository.
"""

import inspect
import sqlite3
from contextlib import closing

import pytest
from db import Database


def _read_other(db, key):
    """Read ``key`` through a second connection (sees only committed data)."""
    with closing(sqlite3.connect(db._db_path)) as other:
        row = other.execute(
            "SELECT value FROM db_meta WHERE key = ?", (key,)
        ).fetchone()
    return row[0] if row else None


# -- get_meta -----------------------------------------------------------------


def test_get_missing_key_returns_none(db):
    assert db.get_meta("meta_test_never_set") is None


def test_get_returns_stored_string(db):
    db.conn.execute(
        "INSERT INTO db_meta (key, value) VALUES ('meta_test_raw', 'hello')"
    )
    db.conn.commit()
    value = db.get_meta("meta_test_raw")
    assert value == "hello"
    assert type(value) is str


def test_get_returns_none_for_null_value(db):
    """A row with a NULL value is indistinguishable from a missing key."""
    db.conn.execute(
        "INSERT INTO db_meta (key, value) VALUES ('meta_test_null', NULL)"
    )
    db.conn.commit()
    assert db.get_meta("meta_test_null") is None


def test_get_returns_empty_string_not_none(db):
    db.conn.execute(
        "INSERT INTO db_meta (key, value) VALUES ('meta_test_empty', '')"
    )
    db.conn.commit()
    assert db.get_meta("meta_test_empty") == ""


def test_get_is_exact_key_match(db):
    db.set_meta("meta_test_key", "v")
    assert db.get_meta("META_TEST_KEY") is None
    assert db.get_meta("meta_test_ke") is None
    assert db.get_meta("meta_test_%") is None


def test_get_issues_one_parameterized_select(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        db.get_meta("meta_test_trace")
    finally:
        db.conn.set_trace_callback(None)
    assert statements == [
        "SELECT value FROM db_meta WHERE key = 'meta_test_trace'"
    ]


def test_get_does_not_open_a_transaction(db):
    db.set_meta("meta_test_tx", "1")
    db.get_meta("meta_test_tx")
    assert not db.conn.in_transaction


def test_get_sees_uncommitted_write_on_same_connection(db):
    db.set_meta("meta_test_pending", "x", _commit=False)
    assert db.get_meta("meta_test_pending") == "x"
    db.conn.rollback()
    assert db.get_meta("meta_test_pending") is None


def test_get_needs_no_active_workspace(db):
    db.set_meta("meta_test_ws", "1")
    db.set_active_workspace(None)
    assert db.get_meta("meta_test_ws") == "1"


# -- set_meta -----------------------------------------------------------------


def test_set_returns_none_and_commits(db):
    assert db.set_meta("meta_test_commit", "yes") is None
    assert not db.conn.in_transaction
    assert _read_other(db, "meta_test_commit") == "yes"


def test_set_overwrites_existing_value(db):
    db.set_meta("meta_test_upsert", "first")
    db.set_meta("meta_test_upsert", "second")
    assert db.get_meta("meta_test_upsert") == "second"
    count = db.conn.execute(
        "SELECT COUNT(*) FROM db_meta WHERE key = 'meta_test_upsert'"
    ).fetchone()[0]
    assert count == 1


def test_set_upsert_keeps_rowid(db):
    """ON CONFLICT DO UPDATE updates in place; INSERT OR REPLACE would not."""
    db.set_meta("meta_test_rowid", "a")
    before = db.conn.execute(
        "SELECT rowid FROM db_meta WHERE key = 'meta_test_rowid'"
    ).fetchone()[0]
    db.set_meta("meta_test_rowid", "b")
    after = db.conn.execute(
        "SELECT rowid FROM db_meta WHERE key = 'meta_test_rowid'"
    ).fetchone()[0]
    assert before == after


@pytest.mark.parametrize(
    "value, stored",
    [
        (1, "1"),
        (0, "0"),
        (3.5, "3.5"),
        (True, "True"),
        (None, "None"),
        ("", ""),
        ("text", "text"),
        (["a"], "['a']"),
    ],
)
def test_set_coerces_value_with_str(db, value, stored):
    db.set_meta("meta_test_coerce", value)
    got = db.get_meta("meta_test_coerce")
    assert got == stored
    assert type(got) is str


def test_set_binds_key_and_str_value_as_parameters(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        db.set_meta("meta_test_sql", 7)
    finally:
        db.conn.set_trace_callback(None)
    assert [s.strip() for s in statements] == [
        "BEGIN",
        "INSERT INTO db_meta (key, value) VALUES ('meta_test_sql', '7') "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        "COMMIT",
    ]


def test_set_without_commit_leaves_transaction_open(db):
    db.set_meta("meta_test_nocommit", "1", _commit=False)
    assert db.conn.in_transaction
    assert _read_other(db, "meta_test_nocommit") is None
    db.conn.commit()
    assert _read_other(db, "meta_test_nocommit") == "1"


def test_set_without_commit_rolls_back_with_caller(db):
    db.set_meta("meta_test_rb", "old")
    db.set_meta("meta_test_rb", "new", _commit=False)
    db.conn.rollback()
    assert db.get_meta("meta_test_rb") == "old"


def test_set_without_commit_issues_no_commit(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        db.set_meta("meta_test_nocommit_sql", "1", _commit=False)
    finally:
        db.conn.set_trace_callback(None)
    assert "COMMIT" not in statements
    db.conn.rollback()


def test_set_commit_flushes_pending_caller_writes(db):
    """The default commit also commits writes the caller left open."""
    db.set_meta("meta_test_a", "1", _commit=False)
    db.set_meta("meta_test_b", "2")
    assert _read_other(db, "meta_test_a") == "1"
    assert _read_other(db, "meta_test_b") == "2"


def test_set_needs_no_active_workspace(db):
    db.set_active_workspace(None)
    db.set_meta("meta_test_ws_set", "1")
    assert _read_other(db, "meta_test_ws_set") == "1"


def test_set_persists_across_reopen(tmp_path):
    path = str(tmp_path / "meta.db")
    first = Database(path)
    first.set_meta("meta_test_reopen", "kept")
    first.close()
    second = Database(path)
    try:
        assert second.get_meta("meta_test_reopen") == "kept"
    finally:
        second.close()


def test_meta_signatures():
    assert list(inspect.signature(Database.get_meta).parameters) == ["self", "key"]
    params = inspect.signature(Database.set_meta).parameters
    assert list(params) == ["self", "key", "value", "_commit"]
    assert params["_commit"].default is True
