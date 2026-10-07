"""Behavior pins for the pending-NAS-transfer domain of ``Database``.

The behavior tests go through the public ``Database`` façade; the structural
test at the end keeps the SQL in ``repositories/pending_archives.py``.
"""

import ast
import contextlib
import inspect
import sqlite3
import textwrap

import pytest
from db import Database


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


def test_get_open_pending_archives(db):
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

    rows = db.get_open_pending_archives()
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
        db.get_open_pending_archives()


def test_delete_pending_archive_is_workspace_scoped_and_commits(db):
    ws = db.require_workspace_id()
    other = db.create_workspace("Other")
    _archive(db, "mine")
    _archive(db, "theirs", workspace_id=other)
    db.delete_pending_archive("mine")
    db.delete_pending_archive("theirs")
    assert not db.in_transaction
    assert _committed(db, "mine") is None
    assert _committed(db, "theirs") is not None
    db.set_active_workspace(other)
    db.delete_pending_archive("theirs")
    assert _committed(db, "theirs") is None
    db.set_active_workspace(ws)


def test_set_pending_archive_state_commits_by_id_in_any_workspace(db):
    other = db.create_workspace("Other")
    _archive(db, "a", workspace_id=other)
    db.set_pending_archive_state("a", "pending", "disk full")
    assert not db.in_transaction
    assert tuple(_committed(db, "a")) == ("pending", "disk full")
    db.set_pending_archive_state("a", "complete")
    assert tuple(_committed(db, "a")) == ("complete", "")
    # Needs no active workspace: the send job addresses the transfer by id.
    db.set_active_workspace(None)
    db.set_pending_archive_state("a", "sending")
    assert tuple(_committed(db, "a")) == ("sending", "")


# -- structure: the pending-transfer SQL lives in the repository ---------------

_DELEGATING_PENDING_ARCHIVE_METHODS = (
    "get_open_pending_archives",
    "delete_pending_archive",
    "set_pending_archive_state",
)


@pytest.mark.parametrize("name", _DELEGATING_PENDING_ARCHIVE_METHODS)
def test_pending_archive_method_delegates_to_repository(name):
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
        f"Database.{name} touches self.conn; move the SQL to PendingArchiveRepository"
    )
    assert "_pending_archive_repository" in attrs, (
        f"Database.{name} no longer delegates to PendingArchiveRepository"
    )
