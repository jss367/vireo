"""Folder-scoped Work Locally behavior and shared-workspace integration."""

from pathlib import Path

import pytest
from db import Database
from services.local_folder import (
    LocalWorkspaceConflict,
    LocalWorkspaceError,
    discard_folder,
    folder_status,
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
