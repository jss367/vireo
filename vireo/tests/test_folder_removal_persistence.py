"""Explicit removals must survive automatic workspace folder discovery."""

import sqlite3

import pytest
from db import Database


@pytest.fixture
def shared_tree(tmp_path):
    db = Database(str(tmp_path / "catalog.db"))
    workspace = db._ws_id()
    parent_path = tmp_path / "photos"
    parent_path.mkdir()
    parent = db.add_folder(str(parent_path))
    missing = db.add_folder(str(parent_path / "missing"), parent_id=parent,
                            workspace_root=False)
    child = db.add_folder(str(parent_path / "missing" / "child"), parent_id=missing,
                          workspace_root=False)
    other = db.create_workspace("All")
    db.add_workspace_folder(other, parent)
    db.check_folder_health()
    yield db, workspace, other, parent, missing, child
    db.close()


@pytest.mark.parametrize("operation", ["delete", "unlink_tree", "unlink_single"])
def test_removed_shared_folders_stay_removed_after_refresh_and_restart(shared_tree, operation):
    db, workspace, other, parent, missing, child = shared_tree
    if operation == "delete":
        assert db.delete_folder(missing)["deleted_photos"] == 0
    elif operation == "unlink_tree":
        db.remove_workspace_folder_tree(workspace, missing)
    else:
        db.remove_workspace_folder(workspace, child)
        db.remove_workspace_folder(workspace, missing)

    for _ in range(2):
        assert {f["id"] for f in db.get_workspace_folders(workspace)} == {parent}
        db.get_workspace_root_folder_ids(workspace)
        db.get_workspace_folder_roots(workspace)
        db.check_folder_health()
        assert db.get_missing_folders() == []
        assert {w["id"] for w in db.get_folder_workspaces(missing)} == {other}
        assert {f["id"] for f in db.get_workspace_folders(other)} == {parent, missing, child}
        if _ == 0:
            db_path = db._db_path
            db.close()
            db = Database(db_path)
            db.set_active_workspace(workspace)
    db.close()


def test_scanning_parent_does_not_restore_removed_missing_descendants(shared_tree):
    from scanner import scan

    db, workspace, other, parent, missing, child = shared_tree
    db.delete_folder(missing)

    scan(db.get_folder(parent)["path"], db, extract_full_metadata=False)
    assert {f["id"] for f in db.get_workspace_folders(workspace)} == {parent}
    assert db.get_missing_folders() == []


def test_explicit_add_restores_removed_subtree(shared_tree):
    db, workspace, other, parent, missing, child = shared_tree
    db.delete_folder(missing)
    db.add_workspace_folder(workspace, missing)
    assert {f["id"] for f in db.get_workspace_folders(workspace)} == {parent, missing, child}
    assert {w["id"] for w in db.get_folder_workspaces(child)} == {workspace, other}


def test_exact_import_restores_only_the_selected_folder(shared_tree):
    db, workspace, other, parent, missing, child = shared_tree
    db.delete_folder(missing)
    db.add_workspace_folder_exact(workspace, missing)
    assert {f["id"] for f in db.get_workspace_folders(workspace)} == {parent, missing}


def test_removing_shared_folder_preserves_other_workspaces_photos(shared_tree):
    db, workspace, other, parent, missing, child = shared_tree
    photo = db.add_photo(folder_id=child, filename="bird.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    assert db.delete_folder(missing) == {"deleted_photos": 0, "files": []}
    assert {f["id"] for f in db.get_workspace_folders(workspace)} == {parent}
    assert db.get_photo(photo) is not None
    assert {f["id"] for f in db.get_workspace_folders(other)} == {parent, missing, child}


def test_new_descendants_still_materialize(shared_tree):
    db, workspace, other, parent, missing, child = shared_tree
    db.delete_folder(missing)
    db.set_active_workspace(other)
    new_folder = db.add_folder(db.get_folder(parent)["path"] + "/new",
                               parent_id=parent, workspace_root=False)
    assert {f["id"] for f in db.get_workspace_folders(workspace)} == {parent, new_folder}


@pytest.mark.parametrize("operation", ["delete", "unlink_tree"])
def test_new_folders_under_removed_subtree_stay_hidden(shared_tree, operation):
    db, workspace, other, parent, missing, child = shared_tree
    if operation == "delete":
        db.delete_folder(missing)
    else:
        db.remove_workspace_folder_tree(workspace, missing)
    db.set_active_workspace(other)
    removed_path = db.get_folder(missing)["path"]
    new_folder = db.add_folder(removed_path + "/new", parent_id=missing, workspace_root=False)
    sibling = db.add_folder(removed_path + "-sibling", parent_id=parent, workspace_root=False)
    assert {w["id"] for w in db.get_folder_workspaces(new_folder)} == {other}
    assert {f["id"] for f in db.get_workspace_folders(workspace)} == {parent, sibling}

    # Registration during a parent rescan must use the same subtree rule.
    db.set_active_workspace(workspace)
    db.add_folder(db.get_folder(parent)["path"])
    assert {f["id"] for f in db.get_workspace_folders(workspace)} == {parent, sibling}
    db.add_workspace_folder(workspace, missing)
    assert {f["id"] for f in db.get_workspace_folders(workspace)} == {
        parent, missing, child, new_folder, sibling,
    }


def test_single_folder_unlink_does_not_remove_descendants(shared_tree):
    db, workspace, other, parent, missing, child = shared_tree
    db.remove_workspace_folder(workspace, missing)
    db.set_active_workspace(other)
    new_folder = db.add_folder(db.get_folder(missing)["path"] + "/new",
                               parent_id=missing, workspace_root=False)
    assert {f["id"] for f in db.get_workspace_folders(workspace)} == {parent, child, new_folder}
    assert {w["id"] for w in db.get_folder_workspaces(child)} == {workspace, other}


@pytest.mark.parametrize("legacy_records", [False, True])
def test_refresh_after_large_subtree_removal_has_bounded_query_work(shared_tree, legacy_records):
    db, workspace, other, parent, missing, child = shared_tree
    path = db.get_folder(missing)["path"]
    db.conn.executemany(
        "INSERT INTO folders (path, parent_id) VALUES (?, ?)",
        [(f"{path}/folder-{i}", missing) for i in range(3000)],
    )
    db.conn.commit()
    db.add_workspace_folder(workspace, parent)
    db.add_workspace_folder(other, parent)
    db.remove_workspace_folder_tree(workspace, missing)
    if legacy_records:
        db.conn.execute("UPDATE workspace_folder_removals SET recursive = 1")
        db.conn.execute("DELETE FROM db_meta WHERE key = 'workspace_folder_removal_scope_version'")
        db.conn.commit()
        with Database(db._db_path):
            pass
    ticks = 0

    def limit_query_work():
        nonlocal ticks
        ticks += 1
        return ticks > 5000

    # Count SQLite VM work instead of wall time: a redundant recursive
    # record for every descendant used to scan the catalog quadratically.
    db.conn.set_progress_handler(limit_query_work, 1000)
    try:
        assert {f["id"] for f in db.get_workspace_folders(workspace)} == {parent}
    finally:
        db.conn.set_progress_handler(None, 0)


def test_explicit_subfolder_root_can_override_removed_ancestor(shared_tree):
    db, workspace, other, parent, missing, child = shared_tree
    db.delete_folder(missing)
    db.add_workspace_folder(workspace, child)
    db.set_active_workspace(other)
    new_folder = db.add_folder(db.get_folder(child)["path"] + "/new",
                               parent_id=child, workspace_root=False)
    assert {f["id"] for f in db.get_workspace_folders(workspace)} == {parent, child, new_folder}
    assert {w["id"] for w in db.get_folder_workspaces(child)} == {workspace, other}


def test_background_discovery_cannot_restore_a_concurrent_removal(shared_tree, monkeypatch):
    db, workspace, other, parent, missing, child = shared_tree
    # Mimic a legacy descendant that discovery is just about to relink.
    db.conn.execute("DELETE FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
                    (workspace, missing))
    db.conn.commit()
    original = db._removed_workspace_folder_ids

    def remove_after_discovery_snapshot(workspace_id):
        snapshot = original(workspace_id)
        with Database(db._db_path, initialize_schema=False) as writer:
            writer.set_active_workspace(workspace)
            writer.delete_folder(missing)
        return snapshot

    monkeypatch.setattr(db, "_removed_workspace_folder_ids", remove_after_discovery_snapshot)
    assert {f["id"] for f in db.get_workspace_folders(workspace)} == {parent}
    assert {w["id"] for w in db.get_folder_workspaces(missing)} == {other}


def test_failed_delete_rolls_back_removal_records(shared_tree):
    db, workspace, other, parent, missing, child = shared_tree
    db.conn.execute("""CREATE TRIGGER reject_unlink BEFORE DELETE ON workspace_folders
                       BEGIN SELECT RAISE(ABORT, 'test unlink failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="test unlink failure"):
        db.delete_folder(missing)
    assert {w["id"] for w in db.get_folder_workspaces(missing)} == {workspace, other}
    assert db.conn.execute("SELECT * FROM workspace_folder_removals").fetchall() == []


def test_existing_catalog_gains_removal_tracking(shared_tree):
    db, workspace, other, parent, missing, child = shared_tree
    db.conn.execute("DROP TRIGGER workspace_folder_restore_on_link")
    db.conn.execute("DROP TABLE workspace_folder_removals")
    db.conn.commit()
    with Database(db._db_path) as upgraded:
        upgraded.set_active_workspace(workspace)
        upgraded.delete_folder(missing)
        assert {f["id"] for f in upgraded.get_workspace_folders(workspace)} == {parent}


@pytest.mark.parametrize("operation", ["tree", "single", "restore_child"])
def test_catalog_upgrade_preserves_legacy_exact_scope(shared_tree, operation):
    db, workspace, other, parent, missing, child = shared_tree
    if operation != "single":
        db.delete_folder(missing)
        if operation == "restore_child":
            db.add_workspace_folder(workspace, child)
    else:
        db.remove_workspace_folder(workspace, missing)
    db.conn.execute("DROP VIEW workspace_removed_folders")
    db.conn.execute("ALTER TABLE workspace_folder_removals DROP COLUMN recursive")
    db.conn.execute("DELETE FROM db_meta WHERE key = 'workspace_folder_removal_scope_version'")
    db.conn.commit()
    with Database(db._db_path) as upgraded:
        upgraded.set_active_workspace(other)
        new_folder = upgraded.add_folder(upgraded.get_folder(missing)["path"] + "/new",
                                         parent_id=missing, workspace_root=False)
        # Pre-recursive catalogs excluded known IDs, not unknown future
        # paths. Upgrading must preserve that scope without inventing intent.
        expected = {parent, new_folder} if operation == "tree" else {parent, child, new_folder}
        assert {f["id"] for f in upgraded.get_workspace_folders(workspace)} == expected


def test_upgrade_preserves_recorded_recursive_scope_with_restored_child(shared_tree):
    db, workspace, other, parent, missing, child = shared_tree
    db.delete_folder(missing)
    db.add_workspace_folder(workspace, child)
    db.conn.execute("DELETE FROM db_meta WHERE key = 'workspace_folder_removal_scope_version'")
    db.conn.commit()
    with Database(db._db_path) as upgraded:
        upgraded.set_active_workspace(other)
        upgraded.add_folder(upgraded.get_folder(missing)["path"] + "/sibling",
                            parent_id=missing, workspace_root=False)
        new_child = upgraded.add_folder(upgraded.get_folder(child)["path"] + "/new",
                                        parent_id=child, workspace_root=False)
        assert {f["id"] for f in upgraded.get_workspace_folders(workspace)} == {parent, child, new_child}


def test_upgrade_from_intermediate_default_one_column_preserves_exact_scope(shared_tree):
    """An intermediate branch build set every legacy exact tombstone to
    recursive on schema upgrade because it added the column with
    ``DEFAULT 1``. Reopening such a catalog must roll those accidental
    recursive marks back to exact — otherwise a single-folder unlink
    starts hiding newly discovered descendants that used to stay
    visible.
    """
    db, workspace, other, parent, missing, child = shared_tree
    # Original catalog: single-folder unlink recorded as exact.
    db.remove_workspace_folder(workspace, child)
    # Simulate the ``1141b93`` intermediate schema: drop the column and
    # re-add it with ``DEFAULT 1``, so the surviving exact row gets
    # bumped to recursive, and clear the scope-version marker so the
    # newer build's compaction pass runs from scratch.
    db.conn.execute("DROP VIEW workspace_removed_folders")
    db.conn.execute("ALTER TABLE workspace_folder_removals DROP COLUMN recursive")
    db.conn.execute(
        "ALTER TABLE workspace_folder_removals "
        "ADD COLUMN recursive INTEGER NOT NULL DEFAULT 1"
    )
    db.conn.execute(
        "DELETE FROM db_meta WHERE key = 'workspace_folder_removal_scope_version'"
    )
    db.conn.commit()
    with Database(db._db_path) as upgraded:
        upgraded.set_active_workspace(other)
        new_descendant = upgraded.add_folder(
            upgraded.get_folder(child)["path"] + "/deeper",
            parent_id=child, workspace_root=False,
        )
        upgraded.set_active_workspace(workspace)
        visible = {f["id"] for f in upgraded.get_workspace_folders(workspace)}
        # A single-folder unlink of `child` still leaves the fresh
        # descendant visible; the intermediate DEFAULT-1 mark did not
        # promote the unlink to a recursive scope.
        assert new_descendant in visible


def test_global_folder_delete_cleans_up_removal_records(shared_tree):
    db, workspace, other, parent, missing, child = shared_tree
    db.delete_folder(missing)
    db.set_active_workspace(other)
    db.delete_folder(missing)
    assert db.get_folder(missing) is None
    assert db.conn.execute("SELECT * FROM workspace_folder_removals").fetchall() == []
    assert db.conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_missing_folder_delete_survives_background_polls(app_and_db, tmp_path):
    app, db = app_and_db
    workspace = db._ws_id()
    parent = db.add_folder(str(tmp_path))
    missing = db.add_folder(str(tmp_path / "missing"), parent_id=parent,
                            workspace_root=False)
    other = db.create_workspace("All")
    db.add_workspace_folder(other, parent)
    client = app.test_client()

    before = client.post("/api/folders/check-health").get_json()["missing"]
    assert missing in {f["id"] for f in before}
    response = client.delete(f"/api/folders/{missing}")
    assert response.status_code == 200
    assert response.get_json() == {"deleted_photos": 0}

    # These ordinary background reads used to recreate the removed link.
    for url in ("/api/workspaces/active/local-folders/blocker",
                "/api/workspaces/active/local-folders",
                f"/api/workspaces/{workspace}/folders"):
        assert client.get(url).status_code == 200
    after = client.post("/api/folders/check-health").get_json()["missing"]
    assert missing not in {f["id"] for f in after}
    assert missing not in {f["id"] for f in client.get("/api/folders/missing").get_json()}
    assert {w["id"] for w in db.get_folder_workspaces(missing)} == {other}

    response = client.post(f"/api/workspaces/{workspace}/folders", json={"folder_id": missing})
    assert response.status_code == 200
    assert missing in {f["id"] for f in client.get("/api/folders/missing").get_json()}
