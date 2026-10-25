"""Behavior pins for the move-source-cleanup domain of ``Database``.

``move_cleanup`` reaches its catalog SQL through ``db.source_cleanup``: the
cataloged-photo check, the Work Locally paths, other workspaces' recursive
roots, every folder's path, and retiring an emptied folder's rows. The
structural tests at the end keep that SQL in ``repositories/source_cleanup.py``.
"""

import ast
import inspect
import textwrap

from db import Database
from repositories.source_cleanup import SourceCleanupRepository
from services import photo_ingest, workspace_folders


def test_folder_has_photos_sees_photos_in_any_workspace(db):
    empty = workspace_folders.add_folder(db, "/library/empty")
    full = workspace_folders.add_folder(db, "/library/full")
    photo_ingest.add_photo(db, full, "a.jpg", ".jpg", 10, 1.0)
    other = db.create_workspace("Other")
    db.set_active_workspace(other)

    assert db.source_cleanup.folder_has_photos(full) is True
    assert db.source_cleanup.folder_has_photos(empty) is False


def test_local_copy_paths_unions_workspace_and_folder_copies(db):
    ws = db.active_workspace_id
    a = workspace_folders.add_folder(db, "/library/a")
    b = workspace_folders.add_folder(db, "/library/b")
    db.conn.execute(
        "INSERT INTO local_workspace_folders (workspace_id, folder_id, source_path, local_path) "
        "VALUES (?, ?, ?, ?)", (ws, a, "/library/a", "/local/a"),
    )
    db.conn.execute(
        "INSERT INTO local_folders (root_folder_id, state) VALUES (?, 'active')", (b,),
    )
    db.conn.execute(
        "INSERT INTO local_folder_mappings (root_folder_id, folder_id, source_path, local_path) "
        "VALUES (?, ?, ?, ?)", (b, b, "/library/b", "/local/b"),
    )
    db.commit()

    rows = {(r["source_path"], r["local_path"]) for r in db.source_cleanup.local_copy_paths()}
    assert rows == {("/library/a", "/local/a"), ("/library/b", "/local/b")}


def test_other_workspace_roots_excludes_the_given_workspace(db):
    mine = db.active_workspace_id
    other = db.create_workspace("Other")
    root = workspace_folders.add_folder(db, "/library/root")
    workspace_folders.add_workspace_folder(db, other, root)
    workspace_folders.add_workspace_folder(db, mine, root)

    rows = [(r["workspace_id"], r["path"]) for r in db.source_cleanup.other_workspace_roots(mine)]
    assert rows == [(other, "/library/root")]
    every = {(r["workspace_id"], r["path"]) for r in db.source_cleanup.other_workspace_roots(None)}
    assert every == {(mine, "/library/root"), (other, "/library/root")}


def test_folder_paths_lists_every_folder(db):
    a = workspace_folders.add_folder(db, "/library/a")
    b = workspace_folders.add_folder(db, "/library/b", link_to_workspace=False)

    rows = {(r["id"], r["path"]) for r in db.source_cleanup.folder_paths()}
    assert rows == {(a, "/library/a"), (b, "/library/b")}


def test_retire_folder_drops_row_links_and_move_source_marker_without_committing(db):
    source = workspace_folders.add_folder(db, "/library/source")
    dest = workspace_folders.add_folder(db, "/library/dest")
    pid = photo_ingest.add_photo(db, dest, "a.jpg", ".jpg", 10, 1.0)
    db.conn.execute(
        "UPDATE photos SET last_move_source_folder_path = ? WHERE id = ?",
        ("/library/source", pid),
    )
    db.commit()

    db.begin_immediate()
    db.source_cleanup.retire_folder(source, "/library/source")
    assert db.in_transaction
    assert db.conn.execute("SELECT 1 FROM folders WHERE id = ?", (source,)).fetchone() is None
    assert db.conn.execute(
        "SELECT 1 FROM workspace_folders WHERE folder_id = ?", (source,)
    ).fetchone() is None
    assert db.conn.execute(
        "SELECT last_move_source_folder_path FROM photos WHERE id = ?", (pid,)
    ).fetchone()[0] is None
    db.rollback()
    assert db.conn.execute("SELECT 1 FROM folders WHERE id = ?", (source,)).fetchone()


# -- structure ------------------------------------------------------------------


def test_source_cleanup_is_a_fresh_repository_on_the_connection_per_access(db):
    first, second = db.source_cleanup, db.source_cleanup
    assert isinstance(first, SourceCleanupRepository)
    assert first is not second
    assert first.conn is db.conn


def test_source_cleanup_accessor_builds_through_the_factory():
    accessor = Database.__dict__["source_cleanup"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_source_cleanup_repository" in attrs
    assert "conn" not in attrs
