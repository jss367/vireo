"""Behavior pins for the pending-NAS-transfer domain of ``Database``.

The behavior tests go through the ``db.pending_archives`` accessor; the
structural tests at the end pin the accessor's shape and keep the old
forwarding wrappers gone.
"""

import ast
import contextlib
import inspect
import sqlite3
import textwrap

import pytest
from db import Database
from repositories.pending_archives import PendingArchiveRepository


def _archive(db, archive_id, *, workspace_id=None, state="pending",
             collection_id=None, created_at="2026-01-01 00:00:00"):
    db.conn.execute(
        "INSERT INTO pending_archives (id, workspace_id, collection_id, destination, "
        "staging_destination, target_json, state, created_at) "
        "VALUES (?, ?, ?, ?, ?, '{}', ?, ?)",
        (archive_id, workspace_id or db.require_workspace_id(), collection_id,
         f"/nas/{archive_id}", f"/staging/{archive_id}", state, created_at),
    )
    db.conn.commit()


def _committed(db, archive_id):
    with contextlib.closing(sqlite3.connect(db._db_path)) as conn:
        return conn.execute(
            "SELECT state, error FROM pending_archives WHERE id = ?", (archive_id,),
        ).fetchone()


def test_open_with_review_collection(db):
    ws = db.require_workspace_id()
    other = db.create_workspace("Other")
    review = db.add_collection("Review", "[]")
    _archive(db, "late", created_at="2026-02-01 00:00:00", collection_id=review)
    _archive(db, "early", created_at="2026-01-01 00:00:00", state="sending")
    _archive(db, "done", state="complete")
    _archive(db, "elsewhere", workspace_id=other)
    db.set_active_workspace(other)
    foreign_collection = db.add_collection("Foreign", "[]")
    db.set_active_workspace(ws)
    _archive(db, "foreign-collection", created_at="2026-03-01 00:00:00",
             collection_id=foreign_collection)

    rows = db.pending_archives.open_with_review_collection()
    # Not complete, this workspace only, oldest first.
    assert [r["id"] for r in rows] == ["early", "late", "foreign-collection"]
    by_id = {r["id"]: r for r in rows}
    assert by_id["late"]["review_collection_id"] == review
    assert by_id["late"]["collection_name"] == "Review"
    assert by_id["early"]["review_collection_id"] is None
    # A collection id from another workspace is not joined.
    assert by_id["foreign-collection"]["review_collection_id"] is None
    assert by_id["foreign-collection"]["collection_name"] is None
    # Every table column rides along.
    assert by_id["early"]["staging_destination"] == "/staging/early"
    assert by_id["early"]["state"] == "sending"

    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.pending_archives.open_with_review_collection()


def test_delete_is_workspace_scoped_and_commits(db):
    ws = db.require_workspace_id()
    other = db.create_workspace("Other")
    _archive(db, "mine")
    _archive(db, "theirs", workspace_id=other)
    db.pending_archives.delete("mine")
    db.pending_archives.delete("theirs")
    assert not db.in_transaction
    assert _committed(db, "mine") is None
    assert _committed(db, "theirs") is not None
    db.set_active_workspace(other)
    db.pending_archives.delete("theirs")
    assert _committed(db, "theirs") is None
    db.set_active_workspace(ws)


def test_set_state_commits_by_id_in_any_workspace(db):
    other = db.create_workspace("Other")
    _archive(db, "a", workspace_id=other)
    db.pending_archives.set_state("a", "pending", "disk full")
    assert not db.in_transaction
    assert tuple(_committed(db, "a")) == ("pending", "disk full")
    db.pending_archives.set_state("a", "complete")
    assert tuple(_committed(db, "a")) == ("complete", "")
    # Needs no active workspace: the send job addresses the transfer by id.
    db.set_active_workspace(None)
    db.pending_archives.set_state("a", "sending")
    assert tuple(_committed(db, "a")) == ("sending", "")


def test_scoped_operations_raise_before_any_sql_without_a_workspace(db):
    """The listing and discard need a workspace, exactly where the wrappers did.

    The workspace is resolved lazily, so reaching ``db.pending_archives`` with
    none active is fine; the scoped call itself raises ``RuntimeError`` before
    touching the table, as the wrapper's eager ``_ws_id()`` did.
    """
    _archive(db, "kept")
    db.set_active_workspace(None)
    repo = db.pending_archives
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        with pytest.raises(RuntimeError):
            repo.open_with_review_collection()
        with pytest.raises(RuntimeError):
            repo.delete("kept")
    finally:
        db.conn.set_trace_callback(None)
    assert statements == []
    assert _committed(db, "kept") is not None


def test_scoped_operations_resolve_the_workspace_active_at_the_call(db):
    """A fresh repository per access, and each call reads the current workspace."""
    ws = db.require_workspace_id()
    other = db.create_workspace("Other")
    _archive(db, "mine")
    _archive(db, "theirs", workspace_id=other)
    db.set_active_workspace(other)
    assert [r["id"] for r in db.pending_archives.open_with_review_collection()] == ["theirs"]
    db.set_active_workspace(ws)
    assert [r["id"] for r in db.pending_archives.open_with_review_collection()] == ["mine"]


# -- structure ------------------------------------------------------------------


def test_pending_archives_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.pending_archives`` builds a new repository each time, never a cached one.

    The workspace is passed as ``Database._ws_id`` itself, uncalled, so building
    the repository resolves nothing and every scoped method resolves the
    active workspace when it runs.
    """
    first, second = db.pending_archives, db.pending_archives
    assert isinstance(first, PendingArchiveRepository)
    assert first is not second
    assert first.conn is db.conn
    assert first.workspace_id_fn == db._ws_id


def test_pending_archives_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.pending_archives``; Database keeps no aliases."""
    for name in ("get_open_pending_archives", "delete_pending_archive", "set_pending_archive_state"):
        assert not hasattr(Database, name), f"Database.{name} came back; call db.pending_archives"
    accessor = Database.__dict__["pending_archives"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_pending_archive_repository" in attrs
    assert "conn" not in attrs
