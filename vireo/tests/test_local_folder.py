"""Folder-scoped Work Locally behavior and shared-workspace integration."""

import threading
from pathlib import Path

import pytest
from db import Database
from services.local_folder import (
    LocalWorkspaceConflict,
    LocalWorkspaceError,
    discard_folder,
    folder_status,
    local_copy_preflight,
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
        remote_status = workspace_status(db, first, str(vireo_dir))
        assert remote_status["folders"][0]["workspace_ids"] == [first, second]
        assert remote_status["linked_workspaces"] == [
            {"id": first, "name": "First"},
            {"id": second, "name": "Second"},
        ]

        result = stage_folder(db, folder_id, str(vireo_dir))

        first_status = workspace_status(db, first, str(vireo_dir))
        second_status = workspace_status(db, second, str(vireo_dir))
        assert first_status["state"] == "active"
        assert second_status["state"] == "active"
        assert first_status["folders"][0]["local_path"] == second_status["folders"][0]["local_path"]
        assert first_status["folders"][0]["workspace_ids"] == [first, second]
        assert first_status["linked_workspaces"] == [
            {"id": first, "name": "First"},
            {"id": second, "name": "Second"},
        ]
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


def test_custom_local_destination_is_used_and_removed_after_sync(tmp_path):
    db, vireo_dir, source, _first, _second, folder_id = _shared_environment(tmp_path)
    local_parent = tmp_path / "fast-storage"
    try:
        result = stage_folder(
            db, folder_id, str(vireo_dir), local_base=str(local_parent)
        )

        local_root = local_parent / "photos"
        assert result["local_path"] == str(local_root)
        assert db.get_folder(folder_id)["path"] == str(local_root)
        assert (local_root / "bird.jpg").read_bytes() == b"original"
        assert (vireo_dir / "local-folders" / str(folder_id) / "manifest.json").is_file()

        (local_root / "bird.jpg").write_bytes(b"edited locally")
        sync_folder(db, folder_id, str(vireo_dir))

        assert (source / "bird.jpg").read_bytes() == b"edited locally"
        assert not local_root.exists()
        assert local_parent.is_dir()
        assert not (vireo_dir / "local-folders" / str(folder_id)).exists()
    finally:
        db.close()


def test_custom_local_destination_refuses_existing_folder(tmp_path):
    db, vireo_dir, _source, _first, _second, folder_id = _shared_environment(tmp_path)
    local_parent = tmp_path / "fast-storage"
    existing = local_parent / "photos"
    existing.mkdir(parents=True)
    (existing / "keep.txt").write_text("do not overwrite")
    try:
        with pytest.raises(LocalWorkspaceError, match="already exists"):
            stage_folder(
                db, folder_id, str(vireo_dir), local_base=str(local_parent)
            )
        assert (existing / "keep.txt").read_text() == "do not overwrite"
        assert Path(db.get_folder(folder_id)["path"]) == _source
    finally:
        db.close()


def test_local_copy_preflight_aggregates_folders_on_the_same_disk(
    tmp_path, monkeypatch
):
    from services import local_folder as service

    db = Database(str(tmp_path / "vireo.db"))
    workspace_id = db.create_workspace("Trip")
    first = tmp_path / "nas" / "first"
    second = tmp_path / "nas" / "second"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    (first / "one.raw").write_bytes(b"a" * 30)
    (second / "two.raw").write_bytes(b"b" * 60)
    first_id = db.add_folder(str(first), name="first", link_to_workspace=False)
    second_id = db.add_folder(str(second), name="second", link_to_workspace=False)
    db.add_workspace_folder(workspace_id, first_id)
    db.add_workspace_folder(workspace_id, second_id)
    monkeypatch.setattr(
        service,
        "destination_disk_space",
        lambda _path, **_kwargs: {
            "total_bytes": 100_000,
            "free_bytes": 18_000,
            "reserve_bytes": 2_000,
            "device": 7,
            "probe_path": str(tmp_path),
        },
    )
    try:
        result = local_copy_preflight(
            db, [first_id, second_id], str(tmp_path / "vireo")
        )

        assert result["total_bytes"] == 90
        assert [folder["total_bytes"] for folder in result["folders"]] == [30, 60]
        assert [folder["estimated_bytes"] for folder in result["folders"]] == [8192, 8192]
        assert len(result["volumes"]) == 1
        assert result["volumes"][0]["copy_bytes"] == 16384
        assert result["volumes"][0]["after_copy_bytes"] == 1616
        assert result["volumes"][0]["can_copy"] is False
        assert result["can_copy"] is False
    finally:
        db.close()


def test_stage_refuses_copy_that_would_use_scaled_safety_reserve(
    tmp_path, monkeypatch
):
    from services import local_workspace as workspace_service

    db, vireo_dir, source, _first, _second, folder_id = _shared_environment(tmp_path)
    large = source / "large.raw"
    with large.open("wb") as handle:
        handle.truncate(2 * 1024**3)
    usage_type = workspace_service.shutil._ntuple_diskusage
    monkeypatch.setattr(
        workspace_service.shutil,
        "disk_usage",
        lambda _path: usage_type(100 * 1024**3, 94 * 1024**3, 6 * 1024**3),
    )
    try:
        # A 100 GiB destination keeps 5 GiB free. The old fixed 1 GiB
        # reserve would have allowed this 2 GiB copy with only 6 GiB free.
        with pytest.raises(LocalWorkspaceError, match="safety reserve"):
            stage_folder(db, folder_id, str(vireo_dir))
        assert db.get_folder(folder_id)["path"] == str(source)
    finally:
        db.close()


def test_custom_local_destination_refuses_other_catalog_source(tmp_path):
    db, vireo_dir, _source, _first, _second, folder_id = _shared_environment(tmp_path)
    other_source = tmp_path / "nas" / "other-source"
    other_source.mkdir()
    db.add_folder(str(other_source), name="other-source", link_to_workspace=False)
    try:
        with pytest.raises(LocalWorkspaceError, match="already manages"):
            stage_folder(
                db, folder_id, str(vireo_dir), local_base=str(other_source)
            )
        assert not (other_source / "photos").exists()
    finally:
        db.close()


def test_custom_local_destination_refuses_other_session_directory(tmp_path):
    db, vireo_dir, _source, _first, _second, folder_id = _shared_environment(tmp_path)
    other_source = tmp_path / "nas" / "other-source"
    other_source.mkdir()
    other_id = db.add_folder(
        str(other_source), name="other-source", link_to_workspace=False
    )
    other_session_base = vireo_dir / "local-folders" / str(other_id) / "files"
    try:
        with pytest.raises(LocalWorkspaceError, match="session storage"):
            stage_folder(
                db,
                folder_id,
                str(vireo_dir),
                local_base=str(other_session_base),
            )
        assert not (other_session_base / "photos").exists()
    finally:
        db.close()


def test_custom_local_destination_refuses_session_metadata_directory(tmp_path):
    source = tmp_path / "nas" / "1"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "vireo.db"))
    workspace_id = db.create_workspace("Numeric folder")
    folder_id = db.add_folder(str(source), name="1", link_to_workspace=False)
    assert folder_id == 1
    db.add_workspace_folder(workspace_id, folder_id)
    try:
        with pytest.raises(LocalWorkspaceError, match="session storage"):
            stage_folder(
                db,
                folder_id,
                str(vireo_dir),
                local_base=str(vireo_dir / "local-folders"),
            )
        assert not (vireo_dir / "local-folders" / "1").exists()
        assert db.get_folder(folder_id)["path"] == str(source)
    finally:
        db.close()


def test_custom_local_cleanup_failure_preserves_copy_after_catalog_restore(
    tmp_path, monkeypatch
):
    from services import local_folder as service

    db, vireo_dir, source, first, _second, folder_id = _shared_environment(tmp_path)
    local_parent = tmp_path / "fast-storage"
    local_root = local_parent / "photos"
    stage_folder(db, folder_id, str(vireo_dir), local_base=str(local_parent))
    real_rmtree = service.shutil.rmtree

    def refuse_custom_cleanup(path, *args, **kwargs):
        if Path(path) == local_root:
            raise PermissionError("destination is busy")
        return real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(service.shutil, "rmtree", refuse_custom_cleanup)
    try:
        with pytest.raises(LocalWorkspaceError, match="Could not remove local data"):
            discard_folder(db, folder_id, str(vireo_dir))

        assert local_root.is_dir()
        assert Path(db.get_folder(folder_id)["path"]) == source
        assert folder_status(db, folder_id, str(vireo_dir))["state"] == "remote"
        assert workspace_status(db, first, str(vireo_dir))["state"] == "remote"
        assert source.is_dir()
    finally:
        db.close()


@pytest.mark.parametrize("operation", ["discard", "sync"])
def test_catalog_restore_failure_keeps_local_copy_and_session(
    tmp_path, monkeypatch, operation
):
    from services import local_folder as service

    db, vireo_dir, source, first, _second, folder_id = _shared_environment(tmp_path)
    local_parent = tmp_path / "fast-storage"
    local_root = local_parent / "photos"
    stage_folder(db, folder_id, str(vireo_dir), local_base=str(local_parent))
    (local_root / "bird.jpg").write_bytes(b"edited locally")

    def refuse_catalog_restore(_db, _root_folder_id):
        raise OSError("catalog is busy")

    monkeypatch.setattr(service, "_restore_catalog", refuse_catalog_restore)
    try:
        with pytest.raises(OSError, match="catalog is busy"):
            if operation == "sync":
                sync_folder(db, folder_id, str(vireo_dir))
            else:
                discard_folder(db, folder_id, str(vireo_dir))

        assert local_root.is_dir()
        assert (local_root / "bird.jpg").read_bytes() == b"edited locally"
        assert Path(db.get_folder(folder_id)["path"]) == local_root
        expected_state = "recovery" if operation == "sync" else "active"
        assert folder_status(db, folder_id, str(vireo_dir))["state"] == expected_state
        workspace = workspace_status(db, first, str(vireo_dir))
        assert workspace["state"] == "active"
        assert workspace["folders"][0]["state"] == expected_state
        assert service.folder_state(db, folder_id)["state"] == (
            "syncing" if operation == "sync" else "active"
        )
        expected_source = b"edited locally" if operation == "sync" else b"original"
        assert (source / "bird.jpg").read_bytes() == expected_source
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


def test_folder_stage_endpoint_accepts_destination_and_uses_folder_name_in_job(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    source = tmp_path / "nas" / "104NCZ_8"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")
    db = Database(db_path)
    workspace_id = db.create_workspace("Trip")
    folder_id = db.add_folder(str(source), name="104NCZ_8", link_to_workspace=False)
    db.add_workspace_folder(workspace_id, folder_id)
    db.set_active_workspace(workspace_id)
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    destination = tmp_path / "chosen-local-storage"
    with app.test_client() as client:
        assert client.post(f"/api/workspaces/{workspace_id}/activate", json={}).status_code == 200
        before = client.get("/api/workspaces/active/local-folders").get_json()
        item = before["folders"][0]
        assert item["default_local_base"] == str(
            vireo_dir / "local-folders" / str(folder_id) / "files"
        )
        assert item["local_folder_name"] == "104NCZ_8"
        assert Path(item["default_local_path"]).name == "104NCZ_8"

        preflight = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [folder_id],
                "destination_bases": {str(folder_id): str(destination)},
            },
        )
        assert preflight.status_code == 200, preflight.get_json()
        capacity = preflight.get_json()
        assert capacity["can_copy"] is True
        assert capacity["total_bytes"] == len(b"original")
        assert capacity["folders"][0]["total_files"] == 1
        assert capacity["folders"][0]["destination_path"] == str(
            destination / "104NCZ_8"
        )
        assert capacity["volumes"][0]["free_bytes"] > capacity["total_bytes"]
        assert capacity["volumes"][0]["reserve_bytes"] >= 1024**3

        response = client.post(
            "/api/workspaces/active/local-folders/stage",
            json={
                "folder_ids": [folder_id],
                "destination_bases": {str(folder_id): str(destination)},
            },
        )
        assert response.status_code == 202, response.get_json()
        job = wait_for_job_via_client(client, response.get_json()["job_id"])
        assert job["status"] == "completed"
        assert job["steps"][0]["label"] == "Copy 104NCZ_8 locally"
        assert job["progress"]["phase"] == "Copying 104NCZ_8 locally"

    check_db = Database(db_path)
    try:
        assert check_db.get_folder(folder_id)["path"] == str(destination / "104NCZ_8")
    finally:
        check_db.close()


def test_stage_endpoint_rejects_case_folded_destination_collisions(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app
    from web import local_folder as local_folder_web

    first = tmp_path / "nas" / "Photos"
    second = tmp_path / "other-nas" / "PHOTOS"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    (first / "one.jpg").write_bytes(b"one")
    (second / "two.jpg").write_bytes(b"two")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")
    db = Database(db_path)
    workspace_id = db.create_workspace("Case collision")
    first_id = db.add_folder(str(first), name="Photos", link_to_workspace=False)
    second_id = db.add_folder(str(second), name="PHOTOS", link_to_workspace=False)
    db.add_workspace_folder(workspace_id, first_id)
    db.add_workspace_folder(workspace_id, second_id)
    db.set_active_workspace(workspace_id)
    db.close()

    monkeypatch.setattr(
        local_folder_web,
        "destination_case_insensitive",
        lambda _path, **_kwargs: True,
    )
    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    destination = tmp_path / "local"
    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{workspace_id}/activate", json={}
        ).status_code == 200
        response = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [first_id, second_id],
                "destination_bases": {
                    str(first_id): str(destination),
                    str(second_id): str(destination),
                },
            },
        )

    assert response.status_code == 400
    assert "overlap" in response.get_json()["error"]


def test_preflight_endpoint_returns_destination_probe_error(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app
    from web import local_folder as local_folder_web

    source = tmp_path / "nas" / "Photos"
    source.mkdir(parents=True)
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")
    db = Database(db_path)
    workspace_id = db.create_workspace("Probe failure")
    folder_id = db.add_folder(str(source), name="Photos", link_to_workspace=False)
    db.add_workspace_folder(workspace_id, folder_id)
    db.set_active_workspace(workspace_id)
    db.close()

    def fail_probe(_path, **_kwargs):
        raise LocalWorkspaceError("Could not inspect destination filesystem")

    monkeypatch.setattr(
        local_folder_web, "destination_case_insensitive", fail_probe
    )
    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{workspace_id}/activate", json={}
        ).status_code == 200
        response = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [folder_id],
                "destination_bases": {str(folder_id): str(tmp_path / "local")},
            },
        )

    assert response.status_code == 409
    assert response.get_json()["error"] == "Could not inspect destination filesystem"


def _cancellable_preflight_app(tmp_path, monkeypatch, fake_preflight):
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app
    from web import local_folder as local_folder_web

    source = tmp_path / "nas" / "Photos"
    source.mkdir(parents=True)
    (source / "bird.raw").write_bytes(b"raw")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")
    db = Database(db_path)
    workspace_id = db.create_workspace("Cancellable preflight")
    folder_id = db.add_folder(str(source), name="Photos", link_to_workspace=False)
    db.add_workspace_folder(workspace_id, folder_id)
    db.set_active_workspace(workspace_id)
    db.close()

    monkeypatch.setattr(local_folder_web, "local_copy_preflight", fake_preflight)
    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{workspace_id}/activate", json={}
        ).status_code == 200
    return app, folder_id


def test_preflight_cancel_endpoint_stops_matching_scan(tmp_path, monkeypatch):
    from services.local_folder import LocalWorkspaceCancelled

    started = threading.Event()
    release = threading.Event()

    def fake_preflight(
        _db, _root_ids, _vireo_dir, *, destination_bases=None, cancel_check=None
    ):
        started.set()
        assert release.wait(5), "test did not release the fake preflight"
        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("cancelled")
        return {"folder_count": 1, "total_bytes": 0, "can_copy": True,
                "folders": [], "volumes": []}

    app, folder_id = _cancellable_preflight_app(
        tmp_path, monkeypatch, fake_preflight
    )
    result = {}
    with app.test_client() as client:
        origin_workspace_id = client.get(
            "/api/workspaces/active/local-folders"
        ).get_json()["workspace_id"]
        other_workspace_id = client.post(
            "/api/workspaces", json={"name": "Other workspace"}
        ).get_json()["id"]

    def request_preflight():
        with app.test_client() as client:
            result["response"] = client.post(
                "/api/workspaces/active/local-folders/preflight",
                json={"folder_ids": [folder_id], "preflight_id": "dialog-one"},
            )

    thread = threading.Thread(target=request_preflight)
    thread.start()
    assert started.wait(5), "preflight did not start"
    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{other_workspace_id}/activate", json={}
        ).status_code == 200
        cancelled = client.post(
            "/api/workspaces/active/local-folders/preflight/cancel",
            json={
                "preflight_id": "dialog-one",
                "workspace_id": origin_workspace_id,
            },
        )
    release.set()
    thread.join(5)

    assert not thread.is_alive()
    assert cancelled.status_code == 200
    assert cancelled.get_json() == {"cancelled": True}
    assert result["response"].status_code == 409
    assert result["response"].get_json()["error"] == (
        "Folder size calculation was cancelled"
    )


def test_preflight_cancel_can_arrive_before_scan_starts(tmp_path, monkeypatch):
    from services.local_folder import LocalWorkspaceCancelled

    def fake_preflight(
        _db, _root_ids, _vireo_dir, *, destination_bases=None, cancel_check=None
    ):
        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("cancelled before start")
        return {"folder_count": 1, "total_bytes": 0, "can_copy": True,
                "folders": [], "volumes": []}

    app, folder_id = _cancellable_preflight_app(
        tmp_path, monkeypatch, fake_preflight
    )
    with app.test_client() as client:
        cancel = client.post(
            "/api/workspaces/active/local-folders/preflight/cancel",
            json={"preflight_id": "late-request"},
        )
        preflight = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [folder_id],
                "preflight_id": "late-request",
                "preflight_client_id": "browser-one",
                "preflight_seq": 5,
            },
        )
        older = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [folder_id],
                "preflight_id": "older-request",
                "preflight_client_id": "browser-one",
                "preflight_seq": 3,
            },
        )

    assert cancel.status_code == 200
    assert cancel.get_json() == {"cancelled": False}
    assert preflight.status_code == 409
    assert preflight.get_json()["error"] == "Folder size calculation was cancelled"
    assert older.status_code == 409
    assert older.get_json()["error"] == "Folder size calculation was cancelled"


def test_queued_preflight_stays_bound_to_origin_workspace(tmp_path, monkeypatch):
    """A delayed POST must consume the origin workspace's cancel tombstone."""
    from services.local_folder import LocalWorkspaceCancelled

    def fake_preflight(
        _db, _root_ids, _vireo_dir, *, destination_bases=None, cancel_check=None
    ):
        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("cancelled before queued request ran")
        return {"folder_count": 1, "total_bytes": 0, "can_copy": True,
                "folders": [], "volumes": []}

    app, folder_id = _cancellable_preflight_app(
        tmp_path, monkeypatch, fake_preflight
    )
    with app.test_client() as client:
        origin_workspace_id = client.get(
            "/api/workspaces/active/local-folders"
        ).get_json()["workspace_id"]
        other_workspace_id = client.post(
            "/api/workspaces",
            json={"name": "Shared workspace", "folder_ids": [folder_id]},
        ).get_json()["id"]
        assert client.post(
            f"/api/workspaces/{other_workspace_id}/activate", json={}
        ).status_code == 200

        cancel = client.post(
            "/api/workspaces/active/local-folders/preflight/cancel",
            json={
                "preflight_id": "queued-origin-request",
                "workspace_id": origin_workspace_id,
            },
        )
        preflight = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [folder_id],
                "preflight_id": "queued-origin-request",
                "preflight_client_id": "origin-tab",
                "preflight_seq": 1,
                "workspace_id": origin_workspace_id,
            },
        )

    assert cancel.status_code == 200
    assert cancel.get_json() == {"cancelled": False}
    assert preflight.status_code == 409
    assert preflight.get_json()["error"] == "Folder size calculation was cancelled"


def test_preflight_precancelled_newer_request_stops_intervening_older_scan(
    tmp_path, monkeypatch
):
    from services.local_folder import LocalWorkspaceCancelled

    older_started = threading.Event()
    release_older = threading.Event()
    calls_lock = threading.Lock()
    calls = 0

    def fake_preflight(
        _db, _root_ids, _vireo_dir, *, destination_bases=None, cancel_check=None
    ):
        nonlocal calls
        with calls_lock:
            calls += 1
            call_number = calls
        if call_number == 1:
            older_started.set()
            assert release_older.wait(5), "test did not release the older scan"
        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("cancelled")
        return {"folder_count": 1, "total_bytes": 1, "can_copy": True,
                "folders": [], "volumes": []}

    app, folder_id = _cancellable_preflight_app(
        tmp_path, monkeypatch, fake_preflight
    )
    older_result = {}

    def request_older():
        with app.test_client() as client:
            older_result["response"] = client.post(
                "/api/workspaces/active/local-folders/preflight",
                json={
                    "folder_ids": [folder_id],
                    "preflight_id": "older",
                    "preflight_client_id": "browser-one",
                    "preflight_seq": 3,
                },
            )

    thread = threading.Thread(target=request_older)
    thread.start()
    assert older_started.wait(5), "older preflight did not start"
    with app.test_client() as client:
        cancel = client.post(
            "/api/workspaces/active/local-folders/preflight/cancel",
            json={"preflight_id": "newer"},
        )
        newer = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [folder_id],
                "preflight_id": "newer",
                "preflight_client_id": "browser-one",
                "preflight_seq": 5,
            },
        )
    release_older.set()
    thread.join(5)

    assert not thread.is_alive()
    assert cancel.get_json() == {"cancelled": False}
    assert newer.status_code == 409
    assert older_result["response"].status_code == 409


def test_preflight_cancel_stops_destination_probing(tmp_path, monkeypatch):
    """A cancel request must break the destination-validation loop so a
    dismissed dialog does not continue probing every remaining destination
    volume — each ``destination_case_insensitive`` call can hang for seconds
    on an unavailable network mount."""
    from web import local_folder as local_folder_web

    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    source_a = tmp_path / "nas" / "PhotosA"
    source_b = tmp_path / "nas" / "PhotosB"
    source_a.mkdir(parents=True)
    source_b.mkdir(parents=True)
    (source_a / "bird.raw").write_bytes(b"raw")
    (source_b / "bird.raw").write_bytes(b"raw")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")
    db = Database(db_path)
    workspace_id = db.create_workspace("Cancellable destination probe")
    folder_a = db.add_folder(str(source_a), name="PhotosA", link_to_workspace=False)
    folder_b = db.add_folder(str(source_b), name="PhotosB", link_to_workspace=False)
    db.add_workspace_folder(workspace_id, folder_a)
    db.add_workspace_folder(workspace_id, folder_b)
    db.set_active_workspace(workspace_id)
    db.close()

    first_probe_started = threading.Event()
    release_probe = threading.Event()
    probe_paths = []

    def slow_case_insensitive(final_path, *, cancel_check=None, **_kwargs):
        probe_paths.append(final_path)
        if len(probe_paths) == 1:
            first_probe_started.set()
            assert release_probe.wait(5), "test did not release the destination probe"
        # Emulate the real probe honouring the cancellation callback so we
        # exercise ``LocalWorkspaceCancelled`` propagating out of the probe
        # (as opposed to only checking the loop's between-iteration check).
        # ``LocalWorkspaceCancelled`` subclasses ``LocalWorkspaceError``, so a
        # naive ``except LocalWorkspaceError`` around the probe would swallow
        # this into the generic destination-error 409 instead of the
        # cancellation error text.
        if cancel_check and cancel_check():
            from services.local_folder import LocalWorkspaceCancelled

            raise LocalWorkspaceCancelled("Folder scan cancelled")
        return False

    def fake_preflight(
        _db, _root_ids, _vireo_dir, *, destination_bases=None, cancel_check=None
    ):
        raise AssertionError(
            "preflight should not run after cancellation during destination validation"
        )

    monkeypatch.setattr(
        local_folder_web, "destination_case_insensitive", slow_case_insensitive
    )
    monkeypatch.setattr(local_folder_web, "local_copy_preflight", fake_preflight)
    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{workspace_id}/activate", json={}
        ).status_code == 200

    destination_a = str(tmp_path / "local" / "a")
    destination_b = str(tmp_path / "local" / "b")
    result = {}

    def request_preflight():
        with app.test_client() as client:
            result["response"] = client.post(
                "/api/workspaces/active/local-folders/preflight",
                json={
                    "folder_ids": [folder_a, folder_b],
                    "preflight_id": "probe-cancel",
                    "destination_bases": {
                        str(folder_a): destination_a,
                        str(folder_b): destination_b,
                    },
                },
            )

    thread = threading.Thread(target=request_preflight)
    thread.start()
    assert first_probe_started.wait(5), "first destination probe did not start"
    with app.test_client() as client:
        cancelled = client.post(
            "/api/workspaces/active/local-folders/preflight/cancel",
            json={"preflight_id": "probe-cancel"},
        )
    release_probe.set()
    thread.join(5)

    assert not thread.is_alive()
    assert cancelled.status_code == 200
    assert cancelled.get_json() == {"cancelled": True}
    assert result["response"].status_code == 409
    assert result["response"].get_json()["error"] == (
        "Folder size calculation was cancelled"
    )
    # The second destination is never probed — cancellation propagates
    # through the loop between filesystem calls instead of forcing the
    # obsolete handler to walk every remaining volume.
    assert len(probe_paths) == 1


def test_preflight_cancel_after_workspace_switch_stops_scan(tmp_path, monkeypatch):
    """A cancel POSTed after switching to another workspace must still stop
    the scan started under the previous workspace.

    ``switchWorkspace`` activates the new workspace before ``pagehide`` fires
    the best-effort cancel, so the cancel handler sees the new workspace as
    the active one. Scoping cancellation by that active workspace would leave
    the old workspace's SMB scan running; the ``preflight_id`` UUID is
    globally unique, so cancellation resolves by that instead."""
    from app import create_app
    from services.local_folder import LocalWorkspaceCancelled
    from web import local_folder as local_folder_web

    monkeypatch.setenv("HOME", str(tmp_path))
    source = tmp_path / "nas" / "Photos"
    source.mkdir(parents=True)
    (source / "bird.raw").write_bytes(b"raw")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")
    db = Database(db_path)
    first_workspace = db.create_workspace("First")
    second_workspace = db.create_workspace("Second")
    folder_id = db.add_folder(str(source), name="Photos", link_to_workspace=False)
    db.add_workspace_folder(first_workspace, folder_id)
    db.close()

    started = threading.Event()
    release = threading.Event()

    def fake_preflight(
        _db, _root_ids, _vireo_dir, *, destination_bases=None, cancel_check=None
    ):
        started.set()
        assert release.wait(5), "test did not release the fake preflight"
        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("cancelled after workspace switch")
        return {"folder_count": 1, "total_bytes": 0, "can_copy": True,
                "folders": [], "volumes": []}

    monkeypatch.setattr(local_folder_web, "local_copy_preflight", fake_preflight)
    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{first_workspace}/activate", json={}
        ).status_code == 200

    result = {}

    def request_preflight():
        with app.test_client() as client:
            result["response"] = client.post(
                "/api/workspaces/active/local-folders/preflight",
                json={"folder_ids": [folder_id], "preflight_id": "scan-in-first"},
            )

    thread = threading.Thread(target=request_preflight)
    thread.start()
    assert started.wait(5), "preflight did not start under first workspace"

    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{second_workspace}/activate", json={}
        ).status_code == 200
        cancelled = client.post(
            "/api/workspaces/active/local-folders/preflight/cancel",
            json={"preflight_id": "scan-in-first"},
        )
    release.set()
    thread.join(5)

    assert not thread.is_alive()
    assert cancelled.status_code == 200
    assert cancelled.get_json() == {"cancelled": True}
    assert result["response"].status_code == 409
    assert result["response"].get_json()["error"] == (
        "Folder size calculation was cancelled"
    )


def test_older_seq_arrival_cannot_supersede_newer_registered_scan(tmp_path, monkeypatch):
    """A delayed request with an older ``preflight_seq`` must not cancel a
    newer scan that already registered.

    Waitress can deliver requests out of order (an older one waited longer for
    a free worker, or its cancel signal was dropped). Without the ordering
    token the older arrival unconditionally cancels the newer scan and takes
    over as active — a subsequent cancel for the older id then cancels it
    too, and the client is stuck at 409 while its latest inputs never get a
    size back."""
    from services.local_folder import LocalWorkspaceCancelled

    newer_started = threading.Event()
    release_newer = threading.Event()
    calls_lock = threading.Lock()
    calls = 0

    def fake_preflight(
        _db, _root_ids, _vireo_dir, *, destination_bases=None, cancel_check=None
    ):
        # A stale arrival rejected by the ordering token gets a pre-set
        # ``cancel_check``; short-circuit here rather than doing any scan
        # work so the test can distinguish rejection from a full scan.
        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("stale seq arrival was rejected")
        nonlocal calls
        with calls_lock:
            calls += 1
            call_number = calls
        assert call_number == 1, "older seq arrival should not run a scan"
        newer_started.set()
        assert release_newer.wait(5), "test did not release the newer preflight"
        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("newer was superseded")
        return {"folder_count": 1, "total_bytes": 999, "can_copy": True,
                "folders": [], "volumes": []}

    app, folder_id = _cancellable_preflight_app(
        tmp_path, monkeypatch, fake_preflight
    )
    newer_result = {}

    def request_newer():
        with app.test_client() as client:
            newer_result["response"] = client.post(
                "/api/workspaces/active/local-folders/preflight",
                json={
                    "folder_ids": [folder_id],
                    "preflight_id": "newer",
                    "preflight_client_id": "browser-one",
                    "preflight_seq": 5,
                },
            )

    thread = threading.Thread(target=request_newer)
    thread.start()
    assert newer_started.wait(5), "newer preflight did not start"
    with app.test_client() as client:
        older = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [folder_id],
                "preflight_id": "older",
                "preflight_client_id": "browser-one",
                "preflight_seq": 3,
            },
        )
    release_newer.set()
    thread.join(5)

    assert not thread.is_alive()
    # Older arrival is rejected with the same 409 the client already knows
    # about; it never touched active_preflights.
    assert older.status_code == 409
    assert older.get_json()["error"] == "Folder size calculation was cancelled"
    # Newer scan ran to completion — was not cancelled by the stale arrival.
    assert newer_result["response"].status_code == 200
    assert newer_result["response"].get_json()["total_bytes"] == 999


def test_sequence_cache_does_not_evict_active_client(tmp_path, monkeypatch):
    """Cache pressure must not let an older request replace an active scan."""
    from services.local_folder import LocalWorkspaceCancelled

    active_started = threading.Event()
    release_active = threading.Event()
    calls_lock = threading.Lock()
    calls = 0

    def fake_preflight(
        _db, _root_ids, _vireo_dir, *, destination_bases=None, cancel_check=None
    ):
        if cancel_check and cancel_check():
            raise LocalWorkspaceCancelled("stale sequence")
        nonlocal calls
        with calls_lock:
            calls += 1
            call_number = calls
        if call_number == 1:
            active_started.set()
            assert release_active.wait(5), "test did not release the active scan"
            if cancel_check and cancel_check():
                raise LocalWorkspaceCancelled("active scan was superseded")
        return {"folder_count": 1, "total_bytes": call_number, "can_copy": True,
                "folders": [], "volumes": []}

    app, folder_id = _cancellable_preflight_app(
        tmp_path, monkeypatch, fake_preflight
    )
    active_result = {}

    def request_active():
        with app.test_client() as client:
            active_result["response"] = client.post(
                "/api/workspaces/active/local-folders/preflight",
                json={
                    "folder_ids": [folder_id],
                    "preflight_id": "active-newer",
                    "preflight_client_id": "active-client",
                    "preflight_seq": 5,
                },
            )

    thread = threading.Thread(target=request_active)
    thread.start()
    assert active_started.wait(5), "active preflight did not start"
    with app.test_client() as client:
        for index in range(64):
            response = client.post(
                "/api/workspaces/active/local-folders/preflight",
                json={
                    "folder_ids": [folder_id],
                    "preflight_id": f"cache-pressure-{index}",
                    "preflight_client_id": f"other-client-{index}",
                    "preflight_seq": 1,
                },
            )
            assert response.status_code == 200
        older = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [folder_id],
                "preflight_id": "active-older",
                "preflight_client_id": "active-client",
                "preflight_seq": 3,
            },
        )
    release_active.set()
    thread.join(5)

    assert not thread.is_alive()
    assert older.status_code == 409
    assert older.get_json()["error"] == "Folder size calculation was cancelled"
    assert active_result["response"].status_code == 200


def test_new_browser_client_can_restart_preflight_sequence(tmp_path, monkeypatch):
    def fake_preflight(
        _db, _root_ids, _vireo_dir, *, destination_bases=None, cancel_check=None
    ):
        return {"folder_count": 1, "total_bytes": 1, "can_copy": True,
                "folders": [], "volumes": []}

    app, folder_id = _cancellable_preflight_app(
        tmp_path, monkeypatch, fake_preflight
    )
    with app.test_client() as client:
        old_page = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [folder_id],
                "preflight_id": "old-page",
                "preflight_client_id": "browser-one",
                "preflight_seq": 5,
            },
        )
        refreshed_page = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [folder_id],
                "preflight_id": "refreshed-page",
                "preflight_client_id": "browser-two",
                "preflight_seq": 1,
            },
        )

    assert old_page.status_code == 200
    assert refreshed_page.status_code == 200


def test_preflights_from_different_browser_clients_do_not_cancel_each_other(
    tmp_path, monkeypatch
):
    first_started = threading.Event()
    release_first = threading.Event()
    calls_lock = threading.Lock()
    calls = 0

    def fake_preflight(
        _db, _root_ids, _vireo_dir, *, destination_bases=None, cancel_check=None
    ):
        nonlocal calls
        with calls_lock:
            calls += 1
            call_number = calls
        if call_number == 1:
            first_started.set()
            assert release_first.wait(5), "test did not release the first page"
        assert not (cancel_check and cancel_check())
        return {"folder_count": 1, "total_bytes": call_number, "can_copy": True,
                "folders": [], "volumes": []}

    app, folder_id = _cancellable_preflight_app(
        tmp_path, monkeypatch, fake_preflight
    )
    first_result = {}

    def request_first_page():
        with app.test_client() as client:
            first_result["response"] = client.post(
                "/api/workspaces/active/local-folders/preflight",
                json={
                    "folder_ids": [folder_id],
                    "preflight_id": "first-page",
                    "preflight_client_id": "browser-one",
                    "preflight_seq": 1,
                },
            )

    thread = threading.Thread(target=request_first_page)
    thread.start()
    assert first_started.wait(5), "first page preflight did not start"
    with app.test_client() as client:
        second = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [folder_id],
                "preflight_id": "second-page",
                "preflight_client_id": "browser-two",
                "preflight_seq": 1,
            },
        )
    release_first.set()
    thread.join(5)

    assert not thread.is_alive()
    assert second.status_code == 200
    assert first_result["response"].status_code == 200


def test_preflight_sequence_cache_is_bounded(tmp_path, monkeypatch):
    def fake_preflight(
        _db, _root_ids, _vireo_dir, *, destination_bases=None, cancel_check=None
    ):
        return {"folder_count": 1, "total_bytes": 1, "can_copy": True,
                "folders": [], "volumes": []}

    app, folder_id = _cancellable_preflight_app(
        tmp_path, monkeypatch, fake_preflight
    )
    with app.test_client() as client:
        for index in range(65):
            response = client.post(
                "/api/workspaces/active/local-folders/preflight",
                json={
                    "folder_ids": [folder_id],
                    "preflight_id": f"request-{index}",
                    "preflight_client_id": f"browser-{index}",
                    "preflight_seq": 5,
                },
            )
            assert response.status_code == 200

        # The oldest of 65 page entries was evicted from the 64-entry cache,
        # so its restarted sequence is accepted rather than rejected as stale.
        evicted_client = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [folder_id],
                "preflight_id": "evicted-client",
                "preflight_client_id": "browser-0",
                "preflight_seq": 1,
            },
        )

    assert evicted_client.status_code == 200


def test_new_preflight_supersedes_old_scan(tmp_path, monkeypatch):
    from services.local_folder import LocalWorkspaceCancelled

    first_started = threading.Event()
    release_first = threading.Event()
    calls_lock = threading.Lock()
    calls = 0

    def fake_preflight(
        _db, _root_ids, _vireo_dir, *, destination_bases=None, cancel_check=None
    ):
        nonlocal calls
        with calls_lock:
            calls += 1
            call_number = calls
        if call_number == 1:
            first_started.set()
            assert release_first.wait(5), "test did not release the first preflight"
            if cancel_check and cancel_check():
                raise LocalWorkspaceCancelled("superseded")
        return {"folder_count": 1, "total_bytes": call_number, "can_copy": True,
                "folders": [], "volumes": []}

    app, folder_id = _cancellable_preflight_app(
        tmp_path, monkeypatch, fake_preflight
    )
    first_result = {}

    def request_first():
        with app.test_client() as client:
            first_result["response"] = client.post(
                "/api/workspaces/active/local-folders/preflight",
                json={"folder_ids": [folder_id], "preflight_id": "first"},
            )

    thread = threading.Thread(target=request_first)
    thread.start()
    assert first_started.wait(5), "first preflight did not start"
    with app.test_client() as client:
        second = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={"folder_ids": [folder_id], "preflight_id": "second"},
        )
    release_first.set()
    thread.join(5)

    assert not thread.is_alive()
    assert second.status_code == 200
    assert second.get_json()["total_bytes"] == 2
    assert first_result["response"].status_code == 409
    assert first_result["response"].get_json()["error"] == (
        "Folder size calculation was cancelled"
    )


def test_reloaded_page_supersedes_obsolete_scan_under_same_client_id(
    tmp_path, monkeypatch
):
    """A page reload that reuses its persisted client id (as the browser now
    does via ``sessionStorage``) has to supersede the obsolete scan still
    running under that same key. A different client id would create a fresh
    ``(workspace_id, preflight_client_id)`` slot and let the old scan keep
    walking the filesystem in parallel with the reloaded page's scan."""
    from services.local_folder import LocalWorkspaceCancelled

    first_started = threading.Event()
    release_first = threading.Event()
    calls_lock = threading.Lock()
    calls = 0

    def fake_preflight(
        _db, _root_ids, _vireo_dir, *, destination_bases=None, cancel_check=None
    ):
        nonlocal calls
        with calls_lock:
            calls += 1
            call_number = calls
        if call_number == 1:
            first_started.set()
            assert release_first.wait(5), "test did not release the first preflight"
            if cancel_check and cancel_check():
                raise LocalWorkspaceCancelled("superseded by reload")
        return {"folder_count": 1, "total_bytes": call_number, "can_copy": True,
                "folders": [], "volumes": []}

    app, folder_id = _cancellable_preflight_app(
        tmp_path, monkeypatch, fake_preflight
    )
    original_result = {}

    def request_original():
        with app.test_client() as client:
            original_result["response"] = client.post(
                "/api/workspaces/active/local-folders/preflight",
                json={
                    "folder_ids": [folder_id],
                    "preflight_id": "original-page",
                    "preflight_client_id": "browser-persisted",
                    "preflight_seq": 5,
                },
            )

    thread = threading.Thread(target=request_original)
    thread.start()
    assert first_started.wait(5), "original preflight did not start"
    with app.test_client() as client:
        reloaded = client.post(
            "/api/workspaces/active/local-folders/preflight",
            json={
                "folder_ids": [folder_id],
                "preflight_id": "reloaded-page",
                "preflight_client_id": "browser-persisted",
                "preflight_seq": 6,
            },
        )
    release_first.set()
    thread.join(5)

    assert not thread.is_alive()
    assert reloaded.status_code == 200
    assert reloaded.get_json()["total_bytes"] == 2
    assert original_result["response"].status_code == 409
    assert original_result["response"].get_json()["error"] == (
        "Folder size calculation was cancelled"
    )


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


@pytest.mark.parametrize("remove_root", [True, False])
def test_staging_in_another_workspace_preserves_removed_folders(tmp_path, remove_root):
    """Local-copy preparation must not undo another workspace's removal."""
    from services.local_folder import affected_workspace_ids

    with Database(str(tmp_path / "vireo.db")) as db:
        parent_ws = db.create_workspace("Parent")
        child_ws = db.create_workspace("Child")
        observer_ws = db.create_workspace("Observer")
        parent = tmp_path / "nas" / "parent"
        child = parent / "child"
        descendant = child / "descendant"
        descendant.mkdir(parents=True)
        (descendant / "bird.jpg").write_bytes(b"original")
        parent_id = db.add_folder(str(parent), link_to_workspace=False)
        child_id = db.add_folder(str(child), parent_id=parent_id, link_to_workspace=False)
        descendant_id = db.add_folder(str(descendant), parent_id=child_id, link_to_workspace=False)
        db.add_workspace_folder(parent_ws, parent_id)
        db.add_workspace_folder(child_ws, child_id)
        db.add_workspace_folder(observer_ws, parent_id)
        db.set_active_workspace(parent_ws)
        db.delete_folder(child_id if remove_root else descendant_id)
        expected = {parent_id} if remove_root else {parent_id, child_id}
        db.set_active_workspace(child_ws)
        vireo_dir = str(tmp_path / "vireo")

        stage_folder(db, child_id, vireo_dir)
        assert {f["id"] for f in db.get_workspace_folders(parent_ws)} == expected
        assert {w["id"] for w in db.get_folder_workspaces(descendant_id)} == {child_ws, observer_ws}
        assert observer_ws in affected_workspace_ids(db, child_id)
        assert {f["id"] for f in db.get_workspace_folders(observer_ws)} == {
            parent_id, child_id, descendant_id,
        }

        discard_folder(db, child_id, vireo_dir)
        assert {f["id"] for f in db.get_workspace_folders(parent_ws)} == expected
        assert (descendant / "bird.jpg").read_bytes() == b"original"


def test_workspace_status_exposes_visible_ancestor_for_missing_local_root(tmp_path):
    """When a local session's rebased folders.path goes missing (managed local
    directory unmounted or deleted, so check_folder_health marks it 'missing'
    and /api/folders drops it), workspace_status must still hand the Browse
    folder tree an anchor to render the LOCAL ISSUE badge on. The nearest
    visible ancestor is that anchor; without it the badge disappears exactly
    when the local copy needs recovery (Codex review r3791982618)."""
    db = Database(str(tmp_path / "vireo.db"))
    ws = db.create_workspace("Reviewer")
    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    parent_id = db.add_folder(str(parent), name="parent", link_to_workspace=False)
    child_id = db.add_folder(str(child), name="child", parent_id=parent_id, link_to_workspace=False)
    db.add_workspace_folder(ws, parent_id)
    db.add_workspace_folder(ws, child_id)
    try:
        stage_folder(db, child_id, str(tmp_path / "vireo"))

        # Simulate check_folder_health flipping the rebased child's status to
        # 'missing' after its managed local dir disappears — get_folder_tree
        # then excludes the row (/api/folders no longer lists it).
        db.conn.execute(
            "UPDATE folders SET status='missing' WHERE id=?", (child_id,)
        )
        db.conn.commit()

        db.set_active_workspace(ws)
        visible_ids = {row["id"] for row in db.get_folder_tree()}
        assert child_id not in visible_ids, "test premise: missing row is dropped"
        assert parent_id in visible_ids, "test premise: ancestor still visible"

        status = workspace_status(db, ws, str(tmp_path / "vireo"))
        descendant = next(
            item for item in status["folders"]
            if item["requested_folder_id"] == child_id
        )
        assert descendant["visible_ancestor_folder_id"] == parent_id
        # folder_name gives the frontend something to render if it ever needs
        # to synthesize a tree entry for a top-level missing root.
        assert descendant["folder_name"] == "child"
    finally:
        db.close()


def test_workspace_status_ancestor_is_none_for_top_level_missing_root(tmp_path):
    """A workspace root that goes missing has no ancestor linked to the
    workspace. workspace_status must expose visible_ancestor_folder_id=None
    in that case so the frontend knows the badge can't attach to an ancestor
    and can fall back to whatever it likes (global banner, workspace panel)."""
    db = Database(str(tmp_path / "vireo.db"))
    ws = db.create_workspace("Reviewer")
    source = tmp_path / "nas" / "photos"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")
    folder_id = db.add_folder(str(source), name="photos", link_to_workspace=False)
    db.add_workspace_folder(ws, folder_id)
    try:
        stage_folder(db, folder_id, str(tmp_path / "vireo"))
        db.conn.execute(
            "UPDATE folders SET status='missing' WHERE id=?", (folder_id,)
        )
        db.conn.commit()

        status = workspace_status(db, ws, str(tmp_path / "vireo"))
        item = next(
            entry for entry in status["folders"]
            if entry["requested_folder_id"] == folder_id
        )
        assert item["visible_ancestor_folder_id"] is None
        assert item["folder_name"] == "photos"
    finally:
        db.close()


def test_workspace_status_counts_unanchored_descendant_session(tmp_path):
    """A hidden descendant session keeps its count if its ancestor vanishes.

    A workspace can link a remote ancestor while sharing a descendant local
    session. If both catalog rows go missing, Browse cannot attach the local
    recovery state to the ancestor and instead synthesizes the descendant as
    the only visible row. That row needs the session's workspace photo count
    rather than the usual zero used while an ancestor owns the tally.
    """
    db = Database(str(tmp_path / "vireo.db"))
    parent_ws = db.create_workspace("Parent")
    child_ws = db.create_workspace("Child")
    parent = tmp_path / "nas" / "parent"
    child = parent / "child"
    child.mkdir(parents=True)
    (child / "bird.jpg").write_bytes(b"original")
    (child / "fox.jpg").write_bytes(b"original")
    parent_id = db.add_folder(
        str(parent), name="parent", link_to_workspace=False
    )
    child_id = db.add_folder(
        str(child), name="child", parent_id=parent_id, link_to_workspace=False
    )
    db.add_photo(child_id, "bird.jpg", ".jpg", 1000, 1.0)
    db.add_photo(child_id, "fox.jpg", ".jpg", 1000, 1.0)
    db.add_workspace_folder(parent_ws, parent_id)
    db.add_workspace_folder(child_ws, child_id)
    try:
        stage_folder(db, child_id, str(tmp_path / "vireo"))
        local_path = Path(db.get_folder(child_id)["path"])
        (local_path / "bird.jpg").unlink()
        (local_path / "fox.jpg").unlink()
        local_path.rmdir()
        db.conn.execute(
            "UPDATE folders SET status='missing' WHERE id IN (?, ?)",
            (parent_id, child_id),
        )
        db.conn.commit()

        status = workspace_status(db, parent_ws, str(tmp_path / "vireo"))
        descendant = next(
            item
            for item in status["folders"]
            if item["requested_folder_id"] == child_id
        )
        assert descendant["state"] == "recovery"
        assert descendant["visible_ancestor_folder_id"] is None
        assert descendant["workspace_photo_count"] == 2
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


def test_local_folder_status_exposes_blocking_job_and_preflight_stops_early(
    tmp_path, monkeypatch
):
    """The UI should know a transition is unavailable before scanning storage.

    The capacity preflight can take a long time on a network volume.  If a
    pipeline already makes Work Locally unsafe, report that job in status and
    reject preflight without walking the source tree.
    """
    import threading

    monkeypatch.setenv("HOME", str(tmp_path))
    import web.local_folder as local_folder_web
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
    started = threading.Event()
    release = threading.Event()

    def processing(_job):
        started.set()
        assert release.wait(timeout=10)
        return {"ok": True}

    def unexpected_preflight(*_args, **_kwargs):
        raise AssertionError("blocked preflight must not scan source storage")

    monkeypatch.setattr(local_folder_web, "local_copy_preflight", unexpected_preflight)

    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{workspace_id}/activate", json={}
        ).status_code == 200
        job_id = app._job_runner.start(
            "pipeline", processing, workspace_id=workspace_id
        )
        try:
            assert started.wait(timeout=2)
            status = client.get(
                "/api/workspaces/active/local-folders"
            ).get_json()
            assert status["blocking_job"] == {
                "id": job_id,
                "type": "pipeline",
                "status": "running",
            }

            preflight = client.post(
                "/api/workspaces/active/local-folders/preflight",
                json={"folder_ids": [folder_id]},
            )
            assert preflight.status_code == 409
            assert "pipeline" in preflight.get_json()["error"]
        finally:
            release.set()
        assert wait_for_job_via_client(client, job_id)["status"] == "completed"


def test_local_folder_blocker_endpoint_avoids_source_walk(tmp_path, monkeypatch):
    """The lightweight blocker endpoint must not walk managed local trees.

    The Work Locally panel polls this endpoint on a keep-alive timer to notice
    scans/pipelines started from other tabs. The full ``/local-folders``
    payload recursively walks every managed local tree via ``folder_status``
    to compute a change summary, so hitting it every few seconds on a large
    library would keep the disk continuously busy.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    import web.local_folder as local_folder_web
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

    def unexpected_status(*_args, **_kwargs):
        raise AssertionError("blocker endpoint must not walk local trees")

    # web.local_folder rebinds workspace_status via `from services.local_folder
    # import workspace_status`, so patch that name on the consuming module.
    monkeypatch.setattr(local_folder_web, "workspace_status", unexpected_status)

    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{workspace_id}/activate", json={}
        ).status_code == 200
        # No blocker → empty payload, without walking storage.
        response = client.get("/api/workspaces/active/local-folders/blocker")
        assert response.status_code == 200
        assert response.get_json() == {
            "blocking_job": None,
            "folder_blocking_jobs": {},
            "residency_fingerprint": "",
        }

    import threading

    started = threading.Event()
    release = threading.Event()

    def processing(_job):
        started.set()
        assert release.wait(timeout=10)
        return {"ok": True}

    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{workspace_id}/activate", json={}
        ).status_code == 200
        job_id = app._job_runner.start(
            "pipeline", processing, workspace_id=workspace_id
        )
        try:
            assert started.wait(timeout=2)
            response = client.get("/api/workspaces/active/local-folders/blocker")
            assert response.status_code == 200
            assert response.get_json() == {
                "blocking_job": {
                    "id": job_id,
                    "type": "pipeline",
                    "status": "running",
                },
                "folder_blocking_jobs": {
                    str(folder_id): {
                        "id": job_id,
                        "type": "pipeline",
                        "status": "running",
                    }
                },
                "residency_fingerprint": "",
            }
        finally:
            release.set()
        assert wait_for_job_via_client(client, job_id)["status"] == "completed"

    # Once the pipeline finishes the endpoint should report an empty blocker
    # again, still without hitting workspace_status.
    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{workspace_id}/activate", json={}
        ).status_code == 200
        response = client.get("/api/workspaces/active/local-folders/blocker")
        assert response.status_code == 200
        assert response.get_json() == {
            "blocking_job": None,
            "folder_blocking_jobs": {},
            "residency_fingerprint": "",
        }
    # Silence unused-variable warning when folder_id isn't referenced.
    assert folder_id


def test_local_folder_blockers_are_scoped_to_affected_roots(tmp_path, monkeypatch):
    """A shared-root job must not disable an unrelated root in the workspace."""
    import threading

    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    shared_source = tmp_path / "nas" / "shared"
    unrelated_source = tmp_path / "nas" / "unrelated"
    shared_source.mkdir(parents=True)
    unrelated_source.mkdir(parents=True)
    (shared_source / "bird.jpg").write_bytes(b"shared")
    (unrelated_source / "fox.jpg").write_bytes(b"unrelated")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    first_workspace = db.create_workspace("First")
    second_workspace = db.create_workspace("Second")
    shared_id = db.add_folder(
        str(shared_source), name="shared", link_to_workspace=False
    )
    unrelated_id = db.add_folder(
        str(unrelated_source), name="unrelated", link_to_workspace=False
    )
    db.add_workspace_folder(first_workspace, shared_id)
    db.add_workspace_folder(first_workspace, unrelated_id)
    db.add_workspace_folder(second_workspace, shared_id)
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    started = threading.Event()
    release = threading.Event()

    def processing(_job):
        started.set()
        assert release.wait(timeout=10)
        return {"ok": True}

    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{first_workspace}/activate", json={}
        ).status_code == 200
        job_id = app._job_runner.start(
            "pipeline", processing, workspace_id=second_workspace
        )
        try:
            assert started.wait(timeout=2)
            blocker = client.get(
                "/api/workspaces/active/local-folders/blocker"
            ).get_json()
            assert blocker["blocking_job"]["id"] == job_id
            assert blocker["folder_blocking_jobs"] == {
                str(shared_id): blocker["blocking_job"]
            }

            stage = client.post(
                "/api/workspaces/active/local-folders/stage",
                json={"folder_ids": [unrelated_id]},
            )
            assert stage.status_code == 202, stage.get_json()
            assert wait_for_job_via_client(
                client, stage.get_json()["job_id"]
            )["status"] == "completed"
        finally:
            release.set()
        assert wait_for_job_via_client(client, job_id)["status"] == "completed"


def test_local_folder_blockers_include_descendant_sessions(tmp_path, monkeypatch):
    """Every local-session row surfaced by workspace status gets a blocker."""
    import threading

    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    parent_source = tmp_path / "nas" / "parent"
    child_source = parent_source / "child"
    child_source.mkdir(parents=True)
    (child_source / "bird.jpg").write_bytes(b"original")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")

    db = Database(db_path)
    parent_workspace = db.create_workspace("Parent")
    child_workspace = db.create_workspace("Child")
    parent_id = db.add_folder(
        str(parent_source), name="parent", link_to_workspace=False
    )
    child_id = db.add_folder(
        str(child_source),
        name="child",
        parent_id=parent_id,
        link_to_workspace=False,
    )
    db.add_workspace_folder(parent_workspace, parent_id)
    db.add_workspace_folder(child_workspace, child_id)
    stage_folder(db, child_id, str(vireo_dir))
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    started = threading.Event()
    release = threading.Event()

    def processing(_job):
        started.set()
        assert release.wait(timeout=10)
        return {"ok": True}

    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{parent_workspace}/activate", json={}
        ).status_code == 200
        job_id = app._job_runner.start(
            "pipeline", processing, workspace_id=child_workspace
        )
        try:
            assert started.wait(timeout=2)
            blocker = client.get(
                "/api/workspaces/active/local-folders/blocker"
            ).get_json()
            assert blocker["blocking_job"]["id"] == job_id
            assert blocker["folder_blocking_jobs"] == {
                str(child_id): blocker["blocking_job"],
            }
        finally:
            release.set()
        assert wait_for_job_via_client(client, job_id)["status"] == "completed"


def test_folder_sync_proceeds_while_observational_job_runs(tmp_path, monkeypatch):
    """An automatic read-only probe must not hold local work hostage.

    ``new_images_walk`` can take minutes on a large network library. Its
    result is cache-generation guarded, so the sync's path invalidation makes
    a result from the old local layout harmless; waiting for the walk only
    turns an ambient navbar probe into a user-visible sync failure.
    """
    import threading

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
    folder_id = db.add_folder(
        str(source), name="photos", link_to_workspace=False,
    )
    db.add_workspace_folder(workspace_id, folder_id)
    db.set_active_workspace(workspace_id)
    stage_folder(db, folder_id, str(vireo_dir))
    local_path = Path(db.get_folder(folder_id)["path"])
    (local_path / "bird.jpg").write_bytes(b"edited")
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    release = threading.Event()
    started = threading.Event()

    def observational_probe(_job):
        started.set()
        assert release.wait(timeout=10)
        return {"ok": True}

    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{workspace_id}/activate", json={},
        ).status_code == 200
        probe_id = app._job_runner.start(
            "new_images_walk",
            observational_probe,
            workspace_id=workspace_id,
            blocks_local_transitions=False,
        )
        try:
            assert started.wait(timeout=2)
            assert app._job_runner.get(probe_id)["status"] == "running"
            response = client.post(
                "/api/workspaces/active/local-folders/sync",
                json={"folder_ids": [folder_id]},
            )
            assert response.status_code == 202, response.get_json()
            sync_job = wait_for_job_via_client(
                client, response.get_json()["job_id"],
            )
            assert sync_job["status"] == "completed"
            assert (source / "bird.jpg").read_bytes() == b"edited"
        finally:
            release.set()
        wait_for_job_via_client(client, probe_id)


def test_local_folder_blocker_fingerprint_tracks_residency_transitions(tmp_path, monkeypatch):
    """Residency fingerprint changes across stage → active → discard.

    The blocker poller compares the endpoint's payload signature to decide
    when to run a full ``load()``. A stage/sync/discard fired from another
    tab that starts *and* finishes between two polls leaves no active job
    behind: both polls report ``blocking_job: None``. Without residency
    fingerprint the signatures compare equal and the folder-tree badge
    keeps its pre-job state. The endpoint has to expose a residency
    fingerprint that changes across every residency transition so cross-tab
    short jobs still trip a refresh.
    """
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
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True

    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{workspace_id}/activate", json={}
        ).status_code == 200

        remote = client.get(
            "/api/workspaces/active/local-folders/blocker"
        ).get_json()
        assert remote["blocking_job"] is None
        assert remote["residency_fingerprint"] == ""

        stage = client.post(
            "/api/workspaces/active/local-folders/stage",
            json={"folder_ids": [folder_id]},
        )
        assert stage.status_code == 202, stage.get_json()
        assert wait_for_job_via_client(
            client, stage.get_json()["job_id"]
        )["status"] == "completed"

        active = client.get(
            "/api/workspaces/active/local-folders/blocker"
        ).get_json()
        assert active["blocking_job"] is None
        # A new local_folders row makes the fingerprint non-empty, so a
        # blocker poll landing right after another tab's stage completes
        # sees a signature change and reruns load(); the folder tree can
        # transition from REMOTE to LOCAL without waiting for a reload.
        assert active["residency_fingerprint"] != remote["residency_fingerprint"]
        assert active["residency_fingerprint"] != ""

        discard = client.post(
            "/api/workspaces/active/local-folders/discard",
            json={"folder_ids": [folder_id], "confirm": True},
        )
        assert discard.status_code == 202, discard.get_json()
        assert wait_for_job_via_client(
            client, discard.get_json()["job_id"]
        )["status"] == "completed"

        cleared = client.get(
            "/api/workspaces/active/local-folders/blocker"
        ).get_json()
        assert cleared["blocking_job"] is None
        # Discard removed the row; fingerprint returns to empty, which
        # still differs from the active-state fingerprint so a poller that
        # missed the job window still refreshes.
        assert cleared["residency_fingerprint"] == ""
        assert cleared["residency_fingerprint"] != active["residency_fingerprint"]


def test_catalog_independent_job_does_not_block_work_locally(tmp_path, monkeypatch):
    """A model/label download in the same workspace must not gate staging.

    The block protects against a stage rebasing ``folders.path`` underneath a
    job that captured pre-rebase paths. A label-embedding precompute holds no
    such paths — it reads a labels file and writes the embedding cache — so
    blocking it only stranded the user behind work unrelated to the folder.
    """
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
    workspace_id = db.create_workspace("Only")
    folder_id = db.add_folder(str(source), name="photos", link_to_workspace=False)
    db.add_workspace_folder(workspace_id, folder_id)
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    started = threading.Event()
    release = threading.Event()

    def embedding_work(_job):
        started.set()
        assert release.wait(timeout=10)
        return {"labels": 3}

    with app.test_client() as client:
        assert client.post(
            f"/api/workspaces/{workspace_id}/activate", json={}
        ).status_code == 200
        job_id = app._job_runner.start(
            "precompute-embeddings", embedding_work, workspace_id=workspace_id,
        )
        try:
            assert started.wait(timeout=2)
            blocker = client.get(
                "/api/workspaces/active/local-folders/blocker"
            ).get_json()
            assert blocker["blocking_job"] is None
            assert blocker["folder_blocking_jobs"] == {}

            stage = client.post(
                "/api/workspaces/active/local-folders/stage",
                json={"folder_ids": [folder_id]},
            )
            assert stage.status_code == 202, stage.get_json()
            assert wait_for_job_via_client(
                client, stage.get_json()["job_id"]
            )["status"] == "completed"
        finally:
            release.set()
        assert wait_for_job_via_client(client, job_id)["status"] == "completed"


def test_sync_reports_the_source_check_before_publishing(tmp_path):
    """The conflict scan is the slow half of a sync and must report progress.

    It re-reads every at-risk source file over the network before anything is
    published, so without its own counter the job shows a bare spinner for the
    bulk of its run.
    """
    db, vireo_dir, source, _first, _second, folder_id = _shared_environment(tmp_path)
    try:
        (source / "kept.jpg").write_bytes(b"kept")
        (source / "culled.jpg").write_bytes(b"culled")
        stage_folder(db, folder_id, str(vireo_dir))
        local_root = Path(db.get_folder(folder_id)["path"])
        (local_root / "bird.jpg").write_bytes(b"local edit")
        (local_root / "culled.jpg").unlink()

        scanned = []
        published = []
        result = sync_folder(
            db,
            folder_id,
            str(vireo_dir),
            allow_deletions=True,
            confirmed_deletions=1,
            progress=lambda current, total, path: published.append((current, total, path)),
            scan_progress=lambda current, total, path: scanned.append((current, total, path)),
        )

        assert result["created_or_modified"] == 1
        assert result["deleted"] == 1
        # One modified file plus one deletion to verify against the source.
        # Each entry is named before it is read and counted only once the
        # comparison is done, so the count never runs ahead of the work.
        assert scanned == [
            (0, 2, "bird.jpg"),
            (1, 2, "culled.jpg"),
            (2, 2, ""),
        ]
        # Checking finishes before publishing starts; the two never interleave.
        assert [item[0] for item in published] == [1, 2]
    finally:
        db.close()


def test_sync_scan_progress_counts_files_with_no_source_counterpart(tmp_path):
    """Files created locally are checked against the source too, and counted."""
    db, vireo_dir, _source, _first, _second, folder_id = _shared_environment(tmp_path)
    try:
        stage_folder(db, folder_id, str(vireo_dir))
        local_root = Path(db.get_folder(folder_id)["path"])
        (local_root / "new.jpg").write_bytes(b"new")

        scanned = []
        result = sync_folder(
            db,
            folder_id,
            str(vireo_dir),
            scan_progress=lambda current, total, path: scanned.append((current, total, path)),
        )

        assert result["created_or_modified"] == 1
        assert scanned == [(0, 1, "new.jpg"), (1, 1, "")]
    finally:
        db.close()


def test_sync_job_shows_the_source_check_as_its_own_step(tmp_path, monkeypatch):
    """The job card names both phases, so neither runs as a silent spinner."""
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    source = tmp_path / "nas" / "09"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")
    (source / "culled.jpg").write_bytes(b"culled")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")
    db = Database(db_path)
    workspace_id = db.create_workspace("Trip")
    folder_id = db.add_folder(str(source), name="09", link_to_workspace=False)
    db.add_workspace_folder(workspace_id, folder_id)
    db.set_active_workspace(workspace_id)
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(f"/api/workspaces/{workspace_id}/activate", json={}).status_code == 200
        staged = client.post(
            "/api/workspaces/active/local-folders/stage", json={"folder_ids": [folder_id]}
        )
        assert staged.status_code == 202, staged.get_json()
        assert wait_for_job_via_client(
            client, staged.get_json()["job_id"]
        )["status"] == "completed"

        check_db = Database(db_path)
        local_root = Path(check_db.get_folder(folder_id)["path"])
        check_db.close()
        (local_root / "bird.jpg").write_bytes(b"edited")
        (local_root / "culled.jpg").unlink()

        response = client.post(
            "/api/workspaces/active/local-folders/sync",
            json={"folder_ids": [folder_id], "confirmed_deletion_counts": {str(folder_id): 1}},
        )
        assert response.status_code == 202, response.get_json()
        job = wait_for_job_via_client(client, response.get_json()["job_id"])
        assert job["status"] == "completed"

        check_step, sync_step = job["steps"]
        assert check_step["id"] == f"check-{folder_id}"
        assert check_step["label"] == "Check 09 against source"
        assert check_step["status"] == "completed"
        assert check_step["progress"] == {"current": 2, "total": 2}
        assert check_step["summary"] == "2 checked, no conflicts"
        assert sync_step["id"] == f"folder-{folder_id}"
        assert sync_step["label"] == "Sync 09 to source"
        assert sync_step["status"] == "completed"
        assert sync_step["summary"] == "1 published, 1 deleted"
        assert job["progress"]["phase"] == "Syncing 09 to source"

    assert (source / "bird.jpg").read_bytes() == b"edited"
    assert not (source / "culled.jpg").exists()


def test_sync_conflict_marks_the_check_step_not_the_publish_step(tmp_path, monkeypatch):
    """A conflict stops the job during checking, before anything is written."""
    monkeypatch.setenv("HOME", str(tmp_path))
    from app import create_app

    source = tmp_path / "nas" / "09"
    source.mkdir(parents=True)
    (source / "bird.jpg").write_bytes(b"original")
    vireo_dir = tmp_path / "vireo"
    thumbs = vireo_dir / "thumbnails"
    thumbs.mkdir(parents=True)
    db_path = str(vireo_dir / "vireo.db")
    db = Database(db_path)
    workspace_id = db.create_workspace("Trip")
    folder_id = db.add_folder(str(source), name="09", link_to_workspace=False)
    db.add_workspace_folder(workspace_id, folder_id)
    db.set_active_workspace(workspace_id)
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.post(f"/api/workspaces/{workspace_id}/activate", json={}).status_code == 200
        staged = client.post(
            "/api/workspaces/active/local-folders/stage", json={"folder_ids": [folder_id]}
        )
        assert staged.status_code == 202, staged.get_json()
        assert wait_for_job_via_client(
            client, staged.get_json()["job_id"]
        )["status"] == "completed"

        check_db = Database(db_path)
        local_root = Path(check_db.get_folder(folder_id)["path"])
        check_db.close()
        (local_root / "bird.jpg").write_bytes(b"local edit")
        (source / "bird.jpg").write_bytes(b"changed on the source since staging")

        response = client.post(
            "/api/workspaces/active/local-folders/sync",
            json={"folder_ids": [folder_id], "confirmed_deletion_counts": {str(folder_id): 0}},
        )
        assert response.status_code == 202, response.get_json()
        job = wait_for_job_via_client(client, response.get_json()["job_id"])
        assert job["status"] == "failed"

        check_step, sync_step = job["steps"]
        assert check_step["status"] == "failed"
        assert sync_step["status"] == "pending"

    assert (source / "bird.jpg").read_bytes() == b"changed on the source since staging"


def test_scan_counter_waits_for_the_read_it_is_reporting(tmp_path, monkeypatch):
    """An entry is counted after it is compared, not when it is picked up.

    Throughput and ETA are derived from the count, so counting on entry would
    show a file as finished for the whole time it is being hashed — minutes,
    for one large original on a network share.
    """
    import services.local_folder as local_folder_service

    db, vireo_dir, _source, _first, _second, folder_id = _shared_environment(tmp_path)
    try:
        stage_folder(db, folder_id, str(vireo_dir))
        local_root = Path(db.get_folder(folder_id)["path"])
        (local_root / "bird.jpg").write_bytes(b"local edit")

        reports = []
        seen_while_reading = []
        real_source_state = local_folder_service._source_state

        def watched_source_state(*args, **kwargs):
            seen_while_reading.append(reports[-1])
            return real_source_state(*args, **kwargs)

        monkeypatch.setattr(local_folder_service, "_source_state", watched_source_state)
        sync_folder(
            db,
            folder_id,
            str(vireo_dir),
            scan_progress=lambda current, total, path: reports.append((current, total, path)),
        )

        assert seen_while_reading == [(0, 1, "bird.jpg")]
        assert reports[-1] == (1, 1, "")
    finally:
        db.close()
