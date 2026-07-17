"""Folder-scoped Work Locally behavior and shared-workspace integration."""

from pathlib import Path

import pytest
from db import Database
from services.local_folder import (
    LocalWorkspaceConflict,
    LocalWorkspaceError,
    discard_folder,
    folder_status,
    local_root_under_folder,
    local_roots_under_folder,
    stage_folder,
    sync_folder,
    workspace_status,
)
from wait import wait_for_job_via_client


def _shared_environment(tmp_path):
    source = tmp_path / "nas" / "photos"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "vireo.db"))
    first = db.create_workspace("First")
    second = db.create_workspace("Second")
    folder_id = db.add_folder(str(source), name="photos", link_to_workspace=False)
    db.add_workspace_folder(first, folder_id)
    db.add_workspace_folder(second, folder_id)
    return db, vireo_dir, source, first, second, folder_id


def test_shared_folder_uses_one_local_copy_in_every_workspace(tmp_path):
    db, vireo_dir, source, first, second, folder_id = _shared_environment(tmp_path)
    try:
        result = stage_folder(db, folder_id, str(vireo_dir))

        first_status = workspace_status(db, first, str(vireo_dir))
        second_status = workspace_status(db, second, str(vireo_dir))
        assert first_status["state"] == "active"
        assert second_status["state"] == "active"
        assert first_status["folders"][0]["local_path"] == second_status["folders"][0]["local_path"]
        assert first_status["folders"][0]["workspace_ids"] == [first, second]
        assert result["local_path"].startswith(str(vireo_dir / "local-folders"))

        local_root = Path(db.get_folder(folder_id)["path"])
        (local_root / "bird.jpg").write_bytes(b"edited through either workspace")
        assert workspace_status(db, second, str(vireo_dir))["folders"][0]["changes"] == {
            "created": 0,
            "modified": 1,
            "deleted": 0,
        }

        sync_folder(db, folder_id, str(vireo_dir))
        assert (source / "bird.jpg").read_bytes() == b"edited through either workspace"
        assert db.get_folder(folder_id)["path"] == str(source)
        assert workspace_status(db, first, str(vireo_dir))["state"] == "remote"
        assert workspace_status(db, second, str(vireo_dir))["state"] == "remote"
    finally:
        db.close()


def test_workspace_status_is_derived_from_independent_root_folders(tmp_path):
    db = Database(str(tmp_path / "vireo.db"))
    workspace_id = db.create_workspace("Mixed")
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    (first / "one.jpg").write_bytes(b"one")
    (second / "two.jpg").write_bytes(b"two")
    first_id = db.add_folder(str(first), name="first", link_to_workspace=False)
    second_id = db.add_folder(str(second), name="second", link_to_workspace=False)
    db.add_workspace_folder(workspace_id, first_id)
    db.add_workspace_folder(workspace_id, second_id)
    try:
        stage_folder(db, first_id, str(tmp_path / "data"))
        status = workspace_status(db, workspace_id, str(tmp_path / "data"))
        assert status["state"] == "mixed"
        assert status["local_folder_count"] == 1
        assert status["folder_count"] == 2
        assert {item["state"] for item in status["folders"]} == {"active", "remote"}

        stage_folder(db, second_id, str(tmp_path / "data"))
        status = workspace_status(db, workspace_id, str(tmp_path / "data"))
        assert status["state"] == "active"
        assert status["local_folder_count"] == 2

        discard_folder(db, first_id, str(tmp_path / "data"))
        assert workspace_status(db, workspace_id, str(tmp_path / "data"))["state"] == "mixed"
    finally:
        db.close()


def test_shared_sync_refuses_source_conflict_and_preserves_local_copy(tmp_path):
    db, vireo_dir, source, first, second, folder_id = _shared_environment(tmp_path)
    try:
        stage_folder(db, folder_id, str(vireo_dir))
        local_root = Path(db.get_folder(folder_id)["path"])
        (local_root / "bird.jpg").write_bytes(b"local edit")
        (source / "bird.jpg").write_bytes(b"outside source edit")

        with pytest.raises(LocalWorkspaceConflict):
            sync_folder(db, folder_id, str(vireo_dir))

        assert (local_root / "bird.jpg").read_bytes() == b"local edit"
        assert (source / "bird.jpg").read_bytes() == b"outside source edit"
        assert workspace_status(db, first, str(vireo_dir))["state"] == "active"
        assert workspace_status(db, second, str(vireo_dir))["state"] == "active"
    finally:
        db.close()


def test_shared_sync_requires_count_bound_deletion_confirmation(tmp_path):
    db, vireo_dir, source, _first, _second, folder_id = _shared_environment(tmp_path)
    try:
        stage_folder(db, folder_id, str(vireo_dir))
        local_root = Path(db.get_folder(folder_id)["path"])
        (local_root / "bird.jpg").unlink()

        with pytest.raises(LocalWorkspaceError, match="confirm deletions"):
            sync_folder(db, folder_id, str(vireo_dir))
        assert (source / "bird.jpg").exists()

        result = sync_folder(
            db,
            folder_id,
            str(vireo_dir),
            allow_deletions=True,
            confirmed_deletions=1,
        )
        assert result["deleted"] == 1
        assert not (source / "bird.jpg").exists()
    finally:
        db.close()


def test_discard_restores_every_workspace_without_touching_source(tmp_path):
    db, vireo_dir, source, first, second, folder_id = _shared_environment(tmp_path)
    try:
        stage_folder(db, folder_id, str(vireo_dir))
        local_root = Path(db.get_folder(folder_id)["path"])
        (local_root / "bird.jpg").write_bytes(b"throw this away")

        discard_folder(db, folder_id, str(vireo_dir))

        assert (source / "bird.jpg").read_bytes() == b"original"
        assert db.get_folder(folder_id)["path"] == str(source)
        assert workspace_status(db, first, str(vireo_dir))["state"] == "remote"
        assert workspace_status(db, second, str(vireo_dir))["state"] == "remote"
    finally:
        db.close()


def test_folder_can_be_linked_to_another_workspace_while_local(tmp_path):
    db, vireo_dir, _source, first, second, folder_id = _shared_environment(tmp_path)
    third = db.create_workspace("Third")
    try:
        # Remove the fixture's second link so the stage begins unshared, then
        # link it elsewhere after activation. The new workspace should reuse
        # the existing managed copy rather than being rejected or duplicated.
        db.remove_workspace_folder_tree(second, folder_id)
        stage_folder(db, folder_id, str(vireo_dir))
        db.add_workspace_folder(third, folder_id)
        third_status = workspace_status(db, third, str(vireo_dir))
        assert third_status["state"] == "active"
        assert folder_status(db, folder_id, str(vireo_dir))["workspace_ids"] == [first, third]
    finally:
        db.close()


def test_folder_scoped_http_cycle_and_shared_status(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    source = tmp_path / "nas" / "photos"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")
    db = Database(db_path)
    first = db.create_workspace("First")
    second = db.create_workspace("Second")
    folder_id = db.add_folder(str(source), name="photos", link_to_workspace=False)
    db.add_workspace_folder(first, folder_id)
    db.add_workspace_folder(second, folder_id)
    db.set_active_workspace(first)
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(f"/api/workspaces/{first}/activate", json={}).status_code == 200
        before = client.get("/api/workspaces/active/local-folders").get_json()
        assert before["state"] == "remote"

        response = client.post(
            "/api/workspaces/active/local-folders/stage", json={"folder_ids": [folder_id]}
        )
        assert response.status_code == 202
        duplicate = client.post(
            "/api/workspaces/active/local-folders/stage", json={"folder_ids": [folder_id]}
        )
        assert duplicate.status_code == 409
        assert wait_for_job_via_client(client, response.get_json()["job_id"])["status"] == "completed"

        assert client.post(f"/api/workspaces/{second}/activate", json={}).status_code == 200
        shared = client.get("/api/workspaces/active/local-folders").get_json()
        assert shared["state"] == "active"
        assert shared["folders"][0]["workspace_ids"] == [first, second]
        assert client.delete(f"/api/workspaces/{first}/folders/{folder_id}").status_code == 200
        last_link = client.delete(f"/api/workspaces/{second}/folders/{folder_id}")
        assert last_link.status_code == 409
        assert "last workspace" in last_link.get_json()["error"]

        check_db = Database(db_path)
        Path(check_db.get_folder(folder_id)["path"], "bird.jpg").write_bytes(b"edited")
        check_db.close()
        response = client.post(
            "/api/workspaces/active/local-folders/sync",
            json={"folder_ids": [folder_id], "confirmed_deletion_counts": {str(folder_id): 0}},
        )
        assert response.status_code == 202
        assert wait_for_job_via_client(client, response.get_json()["job_id"])["status"] == "completed"
        assert (source / "bird.jpg").read_bytes() == b"edited"


def test_local_root_under_folder_finds_descendant_session(tmp_path):
    db = Database(str(tmp_path / "vireo.db"))
    workspace_id = db.create_workspace("Ancestor")
    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"content")
    parent_id = db.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_id = db.add_folder(str(child), name="child", parent_id=parent_id, link_to_workspace=False)
    db.add_workspace_folder(workspace_id, parent_id)
    db.add_workspace_folder(workspace_id, child_id)
    try:
        assert local_root_under_folder(db, parent_id) is None
        stage_folder(db, child_id, str(tmp_path / "vireo"))
        # Child row has been rebased under local-folders/, so a folders.path
        # subtree scan from the parent would miss it — the guard has to
        # consult local_folder_mappings.source_path directly.
        assert local_root_under_folder(db, parent_id) == child_id
        assert local_root_under_folder(db, child_id) is None
    finally:
        db.close()


def test_delete_ancestor_of_local_folder_refuses_with_409(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")
    db = Database(db_path)
    workspace_id = db.create_workspace("Ancestor")
    parent_id = db.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_id = db.add_folder(str(child), name="child", parent_id=parent_id, link_to_workspace=False)
    db.add_workspace_folder(workspace_id, parent_id)
    db.add_workspace_folder(workspace_id, child_id)
    db.set_active_workspace(workspace_id)
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(f"/api/workspaces/{workspace_id}/activate", json={}).status_code == 200
        response = client.post(
            "/api/workspaces/active/local-folders/stage", json={"folder_ids": [child_id]}
        )
        assert response.status_code == 202
        assert wait_for_job_via_client(client, response.get_json()["job_id"])["status"] == "completed"

        blocked = client.delete(f"/api/folders/{parent_id}")
        assert blocked.status_code == 409
        assert "subfolder" in blocked.get_json()["error"]

        # The exact-folder guard still catches deletes of the staged child itself.
        blocked_child = client.delete(f"/api/folders/{child_id}")
        assert blocked_child.status_code == 409
        assert "shared local copy" in blocked_child.get_json()["error"]


def _stage_child_under_parent(tmp_path):
    """Build a parent/child folder tree with the child staged locally."""
    from app import create_app

    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")
    db = Database(db_path)
    workspace_id = db.create_workspace("Ancestor")
    parent_id = db.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_id = db.add_folder(
        str(child), name="child", parent_id=parent_id, link_to_workspace=False,
    )
    db.add_workspace_folder(workspace_id, parent_id)
    db.add_workspace_folder(workspace_id, child_id)
    db.set_active_workspace(workspace_id)
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    client = app.test_client()
    assert (
        client.post(f"/api/workspaces/{workspace_id}/activate", json={}).status_code == 200
    )
    response = client.post(
        "/api/workspaces/active/local-folders/stage", json={"folder_ids": [child_id]},
    )
    assert response.status_code == 202
    assert wait_for_job_via_client(client, response.get_json()["job_id"])["status"] == "completed"
    return client, parent, parent_id, child_id


def test_relocate_ancestor_of_local_folder_refuses_with_409(tmp_path, monkeypatch):
    """POST /api/folders/<ancestor>/relocate must refuse when a descendant
    has a shared local copy. Without the guard, ``db.relocate_folder`` walks
    the parent's ``folders.path`` subtree — which no longer includes the
    rebased child — and rewrites the parent while
    ``local_folder_mappings.source_path`` and the manifest keep pointing at
    the old descendant location, so a later sync/discard cannot land at the
    new source path."""
    monkeypatch.setenv("HOME", str(tmp_path))
    client, parent, parent_id, child_id = _stage_child_under_parent(tmp_path)

    new_parent = tmp_path / "nas" / "renamed"
    new_parent.mkdir()
    blocked = client.post(
        f"/api/folders/{parent_id}/relocate", json={"path": str(new_parent)},
    )
    assert blocked.status_code == 409
    assert "subfolder" in blocked.get_json()["error"]

    # The exact-folder guard still catches relocates of the staged child itself.
    other = tmp_path / "nas" / "renamed_child"
    other.mkdir()
    blocked_child = client.post(
        f"/api/folders/{child_id}/relocate", json={"path": str(other)},
    )
    assert blocked_child.status_code == 409
    assert "shared local copy" in blocked_child.get_json()["error"]


def test_local_roots_under_folder_enumerates_every_descendant_session(tmp_path):
    """The plural variant must return every descendant session so the ancestor
    unlink guard can check each session's linked-workspace list independently
    and the workspace status aggregator can surface all of them at once."""
    db = Database(str(tmp_path / "vireo.db"))
    workspace_id = db.create_workspace("Ancestor")
    parent = tmp_path / "nas" / "parent"
    child_a = parent / "childA"
    child_b = parent / "childB"
    child_a.mkdir(parents=True)
    child_b.mkdir(parents=True)
    (child_a / "one.jpg").write_bytes(b"a")
    (child_b / "two.jpg").write_bytes(b"b")
    parent_id = db.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_a_id = db.add_folder(str(child_a), name="childA", parent_id=parent_id, link_to_workspace=False)
    child_b_id = db.add_folder(str(child_b), name="childB", parent_id=parent_id, link_to_workspace=False)
    db.add_workspace_folder(workspace_id, parent_id)
    try:
        assert local_roots_under_folder(db, parent_id) == []
        stage_folder(db, child_a_id, str(tmp_path / "vireo"))
        stage_folder(db, child_b_id, str(tmp_path / "vireo"))
        assert local_roots_under_folder(db, parent_id) == sorted([child_a_id, child_b_id])
        # Singular variant keeps returning a match (any one) for the guards
        # that only need to know whether any descendant session exists.
        assert local_root_under_folder(db, parent_id) in {child_a_id, child_b_id}
    finally:
        db.close()


def test_workspace_status_surfaces_descendant_local_session(tmp_path):
    """A workspace linking an ancestor sees the descendant session as its own
    status item. Without this, /api/workspaces/active/local-folders reports
    the workspace as fully remote and would offer Work Locally instead of
    sync/discard controls while another workspace's session is active."""
    db = Database(str(tmp_path / "vireo.db"))
    parent_ws = db.create_workspace("Parent")
    child_ws = db.create_workspace("Child")
    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    parent_id = db.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_id = db.add_folder(str(child), name="child", parent_id=parent_id, link_to_workspace=False)
    db.add_workspace_folder(parent_ws, parent_id)
    db.add_workspace_folder(child_ws, child_id)
    try:
        before = workspace_status(db, parent_ws, str(tmp_path / "vireo"))
        assert before["state"] == "remote"
        assert before["local_folder_count"] == 0

        stage_folder(db, child_id, str(tmp_path / "vireo"))

        after = workspace_status(db, parent_ws, str(tmp_path / "vireo"))
        assert after["state"] == "mixed"
        assert after["local_folder_count"] == 1
        assert after["folder_count"] == 2  # /parent (remote) + descendant session
        descendant_items = [item for item in after["folders"] if item["state"] != "remote"]
        assert len(descendant_items) == 1
        assert descendant_items[0]["root_folder_id"] == child_id
        assert descendant_items[0]["source_path"] == str(child)
        # The owning workspace still sees its own session directly.
        owning = workspace_status(db, child_ws, str(tmp_path / "vireo"))
        assert owning["state"] == "active"
    finally:
        db.close()


def test_ancestor_workspace_materialized_before_stage_rebase(tmp_path):
    """When a workspace links an ancestor of the staged folder and the child
    row was inserted after that link, staging must materialize the workspace's
    link to the child BEFORE rebasing folders.path. Otherwise
    _materialize_workspace_descendants can no longer find the child under the
    ancestor path (it's been moved under local-folders/), so
    affected_workspace_ids/workspace_local_root_ids omit the workspace and
    workspace_status still reports the folder as remote even though its
    catalog is partly rebased under the shared local copy."""
    from services.local_folder import affected_workspace_ids, workspace_local_root_ids

    db = Database(str(tmp_path / "vireo.db"))
    parent_ws = db.create_workspace("Parent")
    child_ws = db.create_workspace("Child")
    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    parent_id = db.add_folder(str(parent), name="parent", link_to_workspace=False)
    # Link the parent BEFORE the child row exists. add_workspace_folder only
    # materializes descendants present at link time, so parent_ws gets no
    # explicit workspace_folders row for any child yet — this is the gap the
    # fix has to close before rebasing catalog paths.
    db.add_workspace_folder(parent_ws, parent_id)
    child_id = db.add_folder(str(child), name="child", parent_id=parent_id, link_to_workspace=False)
    db.add_workspace_folder(child_ws, child_id)

    pre_row = db.conn.execute(
        "SELECT 1 FROM workspace_folders WHERE workspace_id=? AND folder_id=?",
        (parent_ws, child_id),
    ).fetchone()
    assert pre_row is None

    try:
        stage_folder(db, child_id, str(tmp_path / "vireo"))

        assert parent_ws in affected_workspace_ids(db, child_id)
        assert child_id in workspace_local_root_ids(db, parent_ws)

        status = workspace_status(db, parent_ws, str(tmp_path / "vireo"))
        assert status["state"] == "mixed"
        local_items = [item for item in status["folders"] if item["state"] != "remote"]
        assert local_items and local_items[0]["root_folder_id"] == child_id
    finally:
        db.close()


def test_future_ancestor_link_materializes_local_descendants(tmp_path):
    """Linking an ancestor root AFTER a descendant has been staged must still
    discover the rebased descendant. Once staging moves the child's
    folders.path under local-folders/, ``_folder_subtree_ids_by_path`` and
    ``_materialize_workspace_descendants`` walking pure ``folders.path``
    prefixes would miss it, leaving the newly linked workspace without a
    workspace_folders row for the child and hiding it from
    ``affected_workspace_ids``/``workspace_local_root_ids``.
    """
    from services.local_folder import affected_workspace_ids, workspace_local_root_ids

    db = Database(str(tmp_path / "vireo.db"))
    staging_ws = db.create_workspace("Staging")
    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    parent_id = db.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_id = db.add_folder(str(child), name="child", parent_id=parent_id, link_to_workspace=False)
    db.add_workspace_folder(staging_ws, child_id)

    try:
        stage_folder(db, child_id, str(tmp_path / "vireo"))

        # Link the ancestor AFTER the rebase. Before the fix,
        # add_workspace_folder walked only folders.path under /parent (empty
        # for the rebased child) and never materialized a link to child_id.
        late_ws = db.create_workspace("Late")
        db.add_workspace_folder(late_ws, parent_id)

        link = db.conn.execute(
            "SELECT 1 FROM workspace_folders WHERE workspace_id=? AND folder_id=?",
            (late_ws, child_id),
        ).fetchone()
        assert link is not None
        assert late_ws in affected_workspace_ids(db, child_id)
        assert child_id in workspace_local_root_ids(db, late_ws)

        status = workspace_status(db, late_ws, str(tmp_path / "vireo"))
        assert status["state"] == "mixed"
        local_items = [item for item in status["folders"] if item["state"] != "remote"]
        assert local_items and local_items[0]["root_folder_id"] == child_id
    finally:
        db.close()


def test_ancestor_workspace_can_sync_descendant_local_session(tmp_path, monkeypatch):
    """A workspace linked to an ancestor of a staged folder must be able to
    sync/discard the descendant session — the workspace-status surface makes
    the session actionable in that workspace's UI, so the HTTP layer has to
    accept the descendant's root_folder_id as a valid folder_ids target."""
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")
    db = Database(db_path)
    parent_ws = db.create_workspace("Parent")
    child_ws = db.create_workspace("Child")
    parent_id = db.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_id = db.add_folder(str(child), name="child", parent_id=parent_id, link_to_workspace=False)
    db.add_workspace_folder(parent_ws, parent_id)
    db.add_workspace_folder(child_ws, child_id)
    db.set_active_workspace(child_ws)
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(f"/api/workspaces/{child_ws}/activate", json={}).status_code == 200
        stage = client.post(
            "/api/workspaces/active/local-folders/stage",
            json={"folder_ids": [child_id]},
        )
        assert stage.status_code == 202
        assert wait_for_job_via_client(client, stage.get_json()["job_id"])["status"] == "completed"

        assert client.post(f"/api/workspaces/{parent_ws}/activate", json={}).status_code == 200
        status = client.get("/api/workspaces/active/local-folders").get_json()
        assert status["state"] == "mixed"
        descendant_items = [item for item in status["folders"] if item["state"] != "remote"]
        assert descendant_items and descendant_items[0]["root_folder_id"] == child_id

        # Edit the local copy from the ancestor workspace's side and sync.
        check_db = Database(db_path)
        local_path = check_db.get_folder(child_id)["path"]
        check_db.close()
        Path(local_path, "bird.jpg").write_bytes(b"edited from parent workspace")

        response = client.post(
            "/api/workspaces/active/local-folders/sync",
            json={"folder_ids": [child_id], "confirmed_deletion_counts": {str(child_id): 0}},
        )
        assert response.status_code == 202
        assert wait_for_job_via_client(client, response.get_json()["job_id"])["status"] == "completed"
        assert (child / "bird.jpg").read_bytes() == b"edited from parent workspace"


def test_ancestor_workspace_photo_count_survives_descendant_rebase(tmp_path):
    """When a workspace links an ancestor of a descendant that is then staged
    locally, the ancestor root's ``workspace_photo_count`` must still include
    the rebased descendant's photos. ``get_workspace_folder_roots`` counts
    photos by matching ``folders.path`` against the root's path, but staging
    moves the descendant's ``folders.path`` under ``local-folders/`` while
    ``workspace_folders`` membership still makes those photos visible. Before
    the fix the ancestor row reported 0 (or too few) photos, so the workspace
    page underreported the images affected by remove/move confirmations."""
    db = Database(str(tmp_path / "vireo.db"))
    parent_ws = db.create_workspace("Parent")
    child_ws = db.create_workspace("Child")
    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    (child / "fox.jpg").write_bytes(b"original")
    parent_id = db.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_id = db.add_folder(str(child), name="child", parent_id=parent_id, link_to_workspace=False)
    db.add_photo(child_id, "bird.jpg", ".jpg", 1000, 1.0)
    db.add_photo(child_id, "fox.jpg", ".jpg", 1000, 1.0)
    db.add_workspace_folder(parent_ws, parent_id)
    db.add_workspace_folder(child_ws, child_id)
    # get_workspace_folder_roots materializes descendants itself, so the
    # ancestor workspace's link to the child folder exists before staging.
    try:
        before = {
            row["path"]: row["workspace_photo_count"]
            for row in db.get_workspace_folder_roots(parent_ws)
        }
        assert before[str(parent)] == 2

        stage_folder(db, child_id, str(tmp_path / "vireo"))

        after = {
            row["path"]: row["workspace_photo_count"]
            for row in db.get_workspace_folder_roots(parent_ws)
        }
        # The ancestor's user-facing path is unchanged (only the descendant
        # was rebased under local-folders/), so its count should still match
        # the pre-stage total.
        assert after.get(str(parent)) == 2, (
            "ancestor root undercounts photos after descendant rebase: "
            f"{after!r}"
        )

        # The staging workspace's own root count is unaffected — its root is
        # the rebased folder itself and its ``cf.path == f.path`` predicate
        # still matches.
        child_roots = {
            row["id"]: row["workspace_photo_count"]
            for row in db.get_workspace_folder_roots(child_ws)
        }
        assert child_roots[child_id] == 2
    finally:
        db.close()


def test_unlink_ancestor_of_shared_local_session_cleans_phantom_rows(tmp_path):
    """Unlinking an ancestor while a descendant has a shared local copy must
    also drop the workspace_folders row that materialization created for the
    rebased descendant. Without the sweep, remove_workspace_folder_tree()'s
    folders.path subtree walk misses the descendant (its path was rebased
    under local-folders/) and leaves a hidden non-root row that still counts
    toward affected_workspace_ids but no longer appears in the folder UI."""
    from app import create_app
    from services.local_folder import affected_workspace_ids

    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    thumbs = tmp_path / "thumbs"
    thumbs.mkdir()
    db_path = str(tmp_path / "vireo.db")

    setup = Database(db_path)
    parent_ws = setup.create_workspace("Parent")
    child_ws = setup.create_workspace("Child")
    parent_id = setup.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_id = setup.add_folder(str(child), name="child", parent_id=parent_id, link_to_workspace=False)
    setup.add_workspace_folder(parent_ws, parent_id)
    setup.add_workspace_folder(child_ws, child_id)
    # Materialize descendants so the parent workspace inherits the child row
    # the same way get_workspace_folders/materialize would have from the UI.
    setup._materialize_workspace_descendants(parent_ws)
    stage_folder(setup, child_id, str(tmp_path / "vireo"))
    assert affected_workspace_ids(setup, child_id) == sorted([parent_ws, child_ws])
    setup.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(f"/api/workspaces/{parent_ws}/activate", json={}).status_code == 200
        response = client.delete(f"/api/workspaces/{parent_ws}/folders/{parent_id}")
        assert response.status_code == 200

    check_db = Database(db_path)
    try:
        row = check_db.conn.execute(
            "SELECT 1 FROM workspace_folders WHERE workspace_id=? AND folder_id=?",
            (parent_ws, child_id),
        ).fetchone()
        assert row is None, "phantom workspace_folders row survived ancestor unlink"
        assert affected_workspace_ids(check_db, child_id) == [child_ws]
    finally:
        check_db.close()


def test_unlink_ancestor_refuses_when_last_link_to_descendant_session(tmp_path):
    """When only one workspace is linked to a descendant local session, the
    ancestor-unlink guard must refuse just like the exact-folder branch.
    Otherwise the unlink would orphan the local session (no workspaces
    linked, but the manifest and folder rebasing still in place)."""
    from app import create_app

    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    thumbs = tmp_path / "thumbs"
    thumbs.mkdir()
    db_path = str(tmp_path / "vireo.db")

    setup = Database(db_path)
    workspace_id = setup.create_workspace("Only")
    parent_id = setup.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_id = setup.add_folder(str(child), name="child", parent_id=parent_id, link_to_workspace=False)
    setup.add_workspace_folder(workspace_id, parent_id)
    setup._materialize_workspace_descendants(workspace_id)
    stage_folder(setup, child_id, str(tmp_path / "vireo"))
    setup.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(f"/api/workspaces/{workspace_id}/activate", json={}).status_code == 200
        response = client.delete(f"/api/workspaces/{workspace_id}/folders/{parent_id}")
        assert response.status_code == 409
        assert "subfolder" in response.get_json()["error"]


def test_move_folder_job_rejects_ancestor_of_local_folder(tmp_path, monkeypatch):
    """POST /api/jobs/move-folder must refuse when a descendant has a shared
    local copy. Without the guard the job would move the parent source
    directory on disk (physically moving the original child location too)
    while ``local_folder_mappings.source_path`` still records the old path,
    so sync/discard would have no destination to publish or restore to."""
    monkeypatch.setenv("HOME", str(tmp_path))
    client, parent, parent_id, child_id = _stage_child_under_parent(tmp_path)

    destination = tmp_path / "moved"
    destination.mkdir()
    blocked = client.post(
        "/api/jobs/move-folder",
        json={"folder_id": parent_id, "destination": str(destination)},
    )
    assert blocked.status_code == 409
    assert "subfolder" in blocked.get_json()["error"]

    # The exact-folder guard still catches moves of the staged child itself.
    destination_child = tmp_path / "moved_child"
    destination_child.mkdir()
    blocked_child = client.post(
        "/api/jobs/move-folder",
        json={"folder_id": child_id, "destination": str(destination_child)},
    )
    assert blocked_child.status_code == 409
    assert "shared local copy" in blocked_child.get_json()["error"]


def test_move_folders_ancestor_sweeps_descendant_local_rows(tmp_path):
    """POST /api/workspaces/<id>/move-folders on an ancestor with a shared
    descendant local root must sweep the rebased descendant's workspace_folders
    rows from source to target. db.move_folders_to_workspace uses a folders.path
    subtree walk that misses the rebased descendant, so without the sweep the
    source keeps a hidden non-root link and the target never gains access to
    the shared local session."""
    from app import create_app
    from services.local_folder import affected_workspace_ids

    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    thumbs = tmp_path / "thumbs"
    thumbs.mkdir()
    db_path = str(tmp_path / "vireo.db")

    setup = Database(db_path)
    parent_ws = setup.create_workspace("Parent")
    child_ws = setup.create_workspace("Child")
    target_ws = setup.create_workspace("Target")
    parent_id = setup.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_id = setup.add_folder(str(child), name="child", parent_id=parent_id, link_to_workspace=False)
    setup.add_workspace_folder(parent_ws, parent_id)
    setup.add_workspace_folder(child_ws, child_id)
    setup._materialize_workspace_descendants(parent_ws)
    stage_folder(setup, child_id, str(tmp_path / "vireo"))
    assert affected_workspace_ids(setup, child_id) == sorted([parent_ws, child_ws])
    setup.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(f"/api/workspaces/{parent_ws}/activate", json={}).status_code == 200
        response = client.post(
            f"/api/workspaces/{parent_ws}/move-folders",
            json={"folder_ids": [parent_id], "target_workspace_id": target_ws},
        )
        assert response.status_code == 200, response.get_json()

    check_db = Database(db_path)
    try:
        source_row = check_db.conn.execute(
            "SELECT 1 FROM workspace_folders WHERE workspace_id=? AND folder_id=?",
            (parent_ws, child_id),
        ).fetchone()
        assert source_row is None, "descendant row survived on source after move"
        target_row = check_db.conn.execute(
            "SELECT 1 FROM workspace_folders WHERE workspace_id=? AND folder_id=?",
            (target_ws, child_id),
        ).fetchone()
        assert target_row is not None, "target workspace never received descendant row"
        assert affected_workspace_ids(check_db, child_id) == sorted([child_ws, target_ws])
    finally:
        check_db.close()


def test_move_folders_ancestor_refuses_when_last_link_to_descendant(tmp_path):
    """When only the source workspace is linked to a descendant local session,
    moving the ancestor must refuse rather than silently transfer the local
    session to the target — the user didn't name the descendant folder, so a
    silent transfer of a shared local copy is surprising. Matches the ancestor
    branch of the folder-unlink route."""
    from app import create_app

    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    thumbs = tmp_path / "thumbs"
    thumbs.mkdir()
    db_path = str(tmp_path / "vireo.db")

    setup = Database(db_path)
    only_ws = setup.create_workspace("Only")
    target_ws = setup.create_workspace("Target")
    parent_id = setup.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_id = setup.add_folder(str(child), name="child", parent_id=parent_id, link_to_workspace=False)
    setup.add_workspace_folder(only_ws, parent_id)
    setup._materialize_workspace_descendants(only_ws)
    stage_folder(setup, child_id, str(tmp_path / "vireo"))
    setup.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(f"/api/workspaces/{only_ws}/activate", json={}).status_code == 200
        response = client.post(
            f"/api/workspaces/{only_ws}/move-folders",
            json={"folder_ids": [parent_id], "target_workspace_id": target_ws},
        )
        assert response.status_code == 409
        assert "subfolder" in response.get_json()["error"]


def test_workspace_ids_for_folder_tree_includes_ancestor_linked_workspaces(tmp_path):
    """workspace_ids_for_folder_tree feeds _busy_job and
    _pending_local_workspace_transition, which need to see workspaces whose
    root sits above the proposed local root as well as ones nested inside it.
    Missing the ancestor direction let jobs enqueue in the ancestor workspace
    during another workspace's staging window."""
    from services.local_folder import workspace_ids_for_folder_tree

    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)

    db = Database(str(tmp_path / "vireo.db"))
    try:
        parent_ws = db.create_workspace("Parent")
        child_ws = db.create_workspace("Child")
        parent_id = db.add_folder(str(parent), name="parent", link_to_workspace=False)
        child_id = db.add_folder(str(child), name="child", parent_id=parent_id, link_to_workspace=False)
        db.add_workspace_folder(parent_ws, parent_id)
        db.add_workspace_folder(child_ws, child_id)
        # Before staging, no local_folder_mappings exist. The lookup must
        # still see parent_ws even though `/parent` is not "within" `/parent/child`.
        result = workspace_ids_for_folder_tree(db, child_id)
        assert sorted(result) == sorted([parent_ws, child_ws])
    finally:
        db.close()


def test_bulk_stage_skips_ancestor_of_descendant_local_session(tmp_path):
    """Bulk "Make All Folders Local" from the workspace UI derives its root list
    from the workspace's user-facing roots. If one of those roots already
    contains a descendant local session (workspace A links ``/parent`` and
    another workspace has ``/parent/child`` staged), staging ``/parent`` would
    deterministically fail with an "overlaps existing local copy" error and
    take the whole job — including the sibling remote roots — down with it.
    The endpoint must filter out ancestor-of-descendant roots the same way it
    filters exact-match local roots, so sibling remote roots still get staged
    and the reason the ancestor was skipped is reported synchronously."""
    from app import create_app

    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    other = tmp_path / "nas" / "other"
    child.mkdir(parents=True)
    other.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    (other / "fox.jpg").write_bytes(b"original")
    thumbs = tmp_path / "thumbs"
    thumbs.mkdir()
    db_path = str(tmp_path / "vireo.db")

    setup = Database(db_path)
    workspace_id = setup.create_workspace("Mixed")
    parent_id = setup.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_id = setup.add_folder(
        str(child), name="child", parent_id=parent_id, link_to_workspace=False,
    )
    other_id = setup.add_folder(str(other), name="other", link_to_workspace=False)
    setup.add_workspace_folder(workspace_id, parent_id)
    setup.add_workspace_folder(workspace_id, other_id)
    setup._materialize_workspace_descendants(workspace_id)
    # Stage the child from a second workspace so this workspace reaches the
    # local session through its ancestor root.
    other_ws = setup.create_workspace("Staging")
    setup.add_workspace_folder(other_ws, child_id)
    stage_folder(setup, child_id, str(tmp_path / "vireo"))
    setup.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(f"/api/workspaces/{workspace_id}/activate", json={}).status_code == 200

        # Explicit stage of the ancestor root alone: must 409 synchronously.
        blocked = client.post(
            "/api/workspaces/active/local-folders/stage",
            json={"folder_ids": [parent_id]},
        )
        assert blocked.status_code == 409, blocked.get_json()
        assert "already local" in blocked.get_json()["error"] or "working locally" in blocked.get_json()["error"]

        # Implicit bulk stage (no folder_ids): must skip the ancestor and
        # stage only the sibling remote root, not enqueue a job that fails.
        response = client.post(
            "/api/workspaces/active/local-folders/stage", json={},
        )
        assert response.status_code == 202, response.get_json()
        body = response.get_json()
        assert body["folder_ids"] == [other_id]
        assert wait_for_job_via_client(client, body["job_id"])["status"] == "completed"

    check_db = Database(db_path)
    try:
        # Sibling got staged; ancestor's user-facing path is unchanged.
        # ``create_app`` derives ``vireo_dir`` from the thumbnail cache's
        # parent, so the staged local-folders/ tree lives alongside it.
        assert check_db.get_folder(other_id)["path"].startswith(
            str(tmp_path / "local-folders")
        )
        assert check_db.get_folder(parent_id)["path"] == str(parent)
    finally:
        check_db.close()


def test_folder_stage_endpoint_refuses_while_scan_is_paused(tmp_path, monkeypatch):
    """A paused scan/import on the same workspace still blocks folder stage.

    ``pause_job`` moves a pausable scan/import out of ``running``, but the
    worker retains its workspace and root assumptions in memory and resumes
    them later. If ``_busy_job`` stopped treating those states as live, a
    folder-scoped stage could rebase folders while the paused job's plan
    still points at the pre-transition layout.
    """
    import time

    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    source = tmp_path / "nas" / "photos"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")

    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    workspace_id = db.create_workspace("Owner")
    folder_id = db.add_folder(str(source), name="photos", link_to_workspace=False)
    db.add_workspace_folder(workspace_id, folder_id)
    db.set_active_workspace(workspace_id)
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True

    runner = app._job_runner

    def pausable_scan(job):
        while not runner.is_cancelled(job["id"]):
            time.sleep(0.01)
        return {"stopped": True}

    with app.test_client() as client:
        assert client.post(f"/api/workspaces/{workspace_id}/activate", json={}).status_code == 200

        job_id = runner.start(
            "scan", pausable_scan, workspace_id=workspace_id, pausable=True
        )
        try:
            assert runner.pause_job(job_id) is True
            deadline = time.monotonic() + 2
            while runner.get(job_id)["status"] != "paused" and time.monotonic() < deadline:
                time.sleep(0.01)
            assert runner.get(job_id)["status"] == "paused"

            blocked = client.post(
                "/api/workspaces/active/local-folders/stage",
                json={"folder_ids": [folder_id]},
            )
            assert blocked.status_code == 409
            body = blocked.get_json()["error"].lower()
            assert "scan" in body
        finally:
            runner.cancel_job(job_id)
        wait_for_job_via_client(client, job_id)
