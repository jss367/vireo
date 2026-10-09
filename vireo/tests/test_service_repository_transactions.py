"""Service persistence uses one live connection and leaves commits to callers."""

import sqlite3

import pytest


@pytest.mark.parametrize("name", ["local_workspaces", "keywords", "collections"])
def test_service_domain_accessors_are_fresh_on_the_live_connection(db, name):
    repo = getattr(db, name)
    assert repo is not getattr(db, name)
    assert repo.conn is db.conn


def test_collection_service_reads_resolve_workspace_at_use(db):
    first = db.active_workspace_id
    collection_id = db.add_collection("First workspace collection", "[]")
    repo = db.collections
    assert repo.visual_scope_row(collection_id)["id"] == collection_id
    second = db.create_workspace("Second workspace")
    db.set_active_workspace(second)
    assert repo.visual_scope_row(collection_id) is None
    assert repo.visual_source_row(collection_id) is None
    db.set_active_workspace(first)
    assert repo.visual_source_row(collection_id)["id"] == collection_id
    db._active_workspace_id = None
    with pytest.raises(RuntimeError):
        repo.visual_scope_row(collection_id)


@pytest.mark.parametrize("kind", ["folder", "workspace"])
def test_local_copy_activation_can_be_rolled_back_as_one_transaction(db, tmp_path, kind):
    workspace_id = db.active_workspace_id
    folder_id = db.add_folder(str(tmp_path / "source"))
    original_path = db.get_folder(folder_id)["path"]
    folder = {
        "folder_id": folder_id,
        "source_path": original_path,
        "local_path": str(tmp_path / "local"),
        "status": "ok",
        "is_root": True,
    }
    repo = db.local_folders if kind == "folder" else db.local_workspaces
    owner = folder_id if kind == "folder" else workspace_id
    assert repo is not (db.local_folders if kind == "folder" else db.local_workspaces)
    assert repo.conn is db.conn

    db.begin_immediate()
    repo.create_staging(owner, 1.0)
    if kind == "folder":
        repo.add_mapping(owner, folder)
    else:
        repo.add_mapping(owner, folder, {folder_id: 0})
    db.local_folders.rebase_folder_if_unchanged(folder)
    assert db.local_folders.last_change_count() == 1
    repo.activate(owner, 2.0)
    assert repo.get_state(owner)["state"] == "active"
    assert db.get_folder(folder_id)["path"] == folder["local_path"]

    # A second reader sees none of the activation until the service commits.
    other = sqlite3.connect(db._db_path)
    try:
        assert other.execute("SELECT path FROM folders WHERE id=?", (folder_id,)).fetchone()[0] == original_path
    finally:
        other.close()
    db.rollback()
    assert repo.get_state(owner) is None
    assert repo.mappings(owner) == []
    assert db.get_folder(folder_id)["path"] == original_path


def test_explicit_local_workspace_ids_do_not_follow_the_active_workspace(db):
    first = db.active_workspace_id
    second = db.create_workspace("Other workspace")
    repo = db.local_workspaces
    db.set_active_workspace(second)
    repo.create_staging(first, 1.0)
    assert repo.get_state(first)["state"] == "staging"
    assert repo.get_state(second) is None
    db.rollback()


def test_staging_rebase_detects_a_changed_catalog_path(db, tmp_path):
    folder_id = db.add_folder(str(tmp_path / "new-source"))
    folder = {"folder_id": folder_id, "source_path": str(tmp_path / "old-source"),
              "local_path": str(tmp_path / "local")}
    db.local_folders.rebase_folder_if_unchanged(folder)
    assert db.local_folders.last_change_count() == 0
    assert db.get_folder(folder_id)["path"] == str(tmp_path / "new-source")
    db.rollback()


def test_grouping_payload_and_history_change_share_the_service_transaction(db):
    edit_id = db.record_edit("photo_flag", "Edit", "{}", [], _commit=False)
    db.commit()
    db.edit_history.store_grouping_payload(edit_id, '{"before": [], "after": []}')
    db.edit_history.convert_to_grouping(edit_id, '{"label_edit": true}')
    assert db.edit_history.grouping_payload(edit_id)[0] == '{"before": [], "after": []}'
    db.rollback()
    assert db.edit_history.grouping_payload(edit_id) is None
    row = db.conn.execute("SELECT action_type, new_value FROM edit_history WHERE id=?", (edit_id,)).fetchone()
    assert tuple(row) == ("photo_flag", "{}")
