"""Behavior and accessor pins for the photo-visibility domain of ``Database``.

Moves, imports and catalog reads that depend on photo-only grants are covered
end to end in ``test_photo_workspace_visibility.py``. This file pins the
``db.photo_visibility`` accessor itself: a fresh repository per access, which
methods resolve the active workspace (and where they raise without one), and
that the old forwarding wrappers stay gone.
"""

import ast
import inspect
import textwrap

import pytest
from db import Database
from repositories.photo_visibility import PhotoVisibilityRepository


def _photo(db, name="bird.jpg"):
    fid = db.add_folder(f"/photos/{name}", name=name)
    return fid, db.add_photo(
        folder_id=fid, filename=name, extension=".jpg",
        file_size=1000, file_mtime=1.0,
    )


def test_visible_photo_ids_keeps_order_drops_unknown_and_dedupes(db):
    _, a = _photo(db, "a.jpg")
    _, b = _photo(db, "b.jpg")
    assert db.photo_visibility.visible_photo_ids([b, 999_999_999, a, b]) == [b, a]


def test_workspace_scoped_methods_raise_before_any_sql_without_a_workspace(db):
    """The visibility filter and the move helpers need a workspace, as before.

    The workspace is resolved lazily, so reaching ``db.photo_visibility`` with
    none active is fine; the scoped call itself raises ``RuntimeError`` before
    touching a table, as the wrappers' eager ``_ws_id()`` did.
    """
    _, pid = _photo(db)
    db.set_active_workspace(None)
    repo = db.photo_visibility
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        for call in (
            lambda: repo.visible_photo_ids([]),
            lambda: repo.affected_workspaces([pid]),
            lambda: repo.preserve_for_move(pid, True),
        ):
            with pytest.raises(RuntimeError, match="No active workspace set"):
                call()
    finally:
        db.conn.set_trace_callback(None)
    assert statements == []


def test_grants_take_their_workspace_explicitly_and_need_none_active(db):
    """The grant and revoke writes never resolve the active workspace."""
    other = db.create_workspace("Other")
    _, pid = _photo(db)
    db.set_active_workspace(None)
    db.photo_visibility.grant(other, [pid])
    db.commit()
    db.set_active_workspace(other)
    assert db.photo_visibility.visible_photo_ids([pid]) == [pid]
    db.set_active_workspace(None)
    db.photo_visibility.revoke_grants(other, [pid])
    db.commit()
    db.set_active_workspace(other)
    assert db.photo_visibility.visible_photo_ids([pid]) == []


def test_scoped_methods_read_the_workspace_active_at_the_call(db):
    """A fresh repository per access, and each call reads the current workspace."""
    home = db.require_workspace_id()
    other = db.create_workspace("Other")
    folder, pid = _photo(db)
    db.add_workspace_folder(other, folder)
    db.set_active_workspace(home)
    assert [w["id"] for w in db.photo_visibility.affected_workspaces([pid])] == [other]
    db.set_active_workspace(other)
    assert [w["id"] for w in db.photo_visibility.affected_workspaces([pid])] == [home]


# -- structure ------------------------------------------------------------------

_REMOVED_PHOTO_VISIBILITY_WRAPPERS = (
    "grant_workspace_photos",
    "revoke_workspace_photo_grants_for_folders",
    "grant_verified_twin_photos",
    "grant_verified_twin_photos_tracked",
    "revoke_photo_grants",
    "demote_folders_to_missing",
    "photo_move_affected_workspaces",
    "preserve_photo_visibility_for_move",
    "filter_photo_ids_in_workspace",
)


def test_photo_visibility_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.photo_visibility`` builds a new repository each time, never a cached one.

    The workspace is passed as ``Database._ws_id`` itself, uncalled, so building
    the repository resolves nothing.
    """
    first, second = db.photo_visibility, db.photo_visibility
    assert isinstance(first, PhotoVisibilityRepository)
    assert first is not second
    assert first.conn is db.conn
    assert first.workspace_id_fn == db._ws_id


def test_photo_visibility_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.photo_visibility``; Database keeps no aliases."""
    for name in _REMOVED_PHOTO_VISIBILITY_WRAPPERS:
        assert not hasattr(Database, name), f"Database.{name} came back; call db.photo_visibility"
    accessor = Database.__dict__["photo_visibility"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_photo_visibility_repository" in attrs
    assert "conn" not in attrs
