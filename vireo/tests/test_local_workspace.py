import os
from pathlib import Path

import pytest
from db import Database
from services.local_workspace import (
    LocalWorkspaceConflict,
    LocalWorkspaceError,
    discard_local,
    stage_workspace,
    status,
    sync_back,
    workspace_dir,
)
from wait import wait_for_job_via_client


@pytest.fixture
def local_workspace_env(tmp_path):
    source = tmp_path / "nas" / "photos"
    child = source / "2026"
    child.mkdir(parents=True)
    (source / "empty-folder").mkdir()
    (source / "root.jpg").write_bytes(b"root-original")
    (child / "bird.jpg").write_bytes(b"bird-original")
    (child / "bird.xmp").write_text("original metadata", encoding="utf-8")

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "vireo.db"))
    workspace_id = db._active_workspace_id
    root_id = db.add_folder(str(source), name="photos")
    child_id = db.add_folder(
        str(child),
        name="2026",
        parent_id=root_id,
        workspace_root=False,
    )
    db.add_photo(
        child_id,
        "bird.jpg",
        ".jpg",
        (child / "bird.jpg").stat().st_size,
        (child / "bird.jpg").stat().st_mtime,
    )
    yield {
        "db": db,
        "workspace_id": workspace_id,
        "root_id": root_id,
        "child_id": child_id,
        "source": source,
        "child": child,
        "vireo_dir": vireo_dir,
    }
    db.close()


def _folder_path(db, folder_id):
    return db.conn.execute("SELECT path FROM folders WHERE id=?", (folder_id,)).fetchone()["path"]


def test_stage_modify_and_sync_back(local_workspace_env):
    env = local_workspace_env
    result = stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert result["files"] == 3
    local_root = Path(_folder_path(env["db"], env["root_id"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    assert local_root != env["source"]
    assert local_child == local_root / "2026"
    assert (local_root / "empty-folder").is_dir()
    assert (local_child / "bird.jpg").read_bytes() == b"bird-original"

    (local_child / "bird.jpg").write_bytes(b"bird-locally-edited")
    (local_child / "new.xmp").write_text("new metadata", encoding="utf-8")
    os.unlink(local_child / "bird.xmp")

    current = status(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    assert current["state"] == "active"
    assert current["changes"] == {"created": 1, "modified": 1, "deleted": 1}

    synced = sync_back(
        env["db"],
        env["workspace_id"],
        str(env["vireo_dir"]),
        allow_deletions=True,
    )

    assert synced["created_or_modified"] == 2
    assert synced["deleted"] == 1
    assert (env["child"] / "bird.jpg").read_bytes() == b"bird-locally-edited"
    assert (env["child"] / "new.xmp").read_text(encoding="utf-8") == "new metadata"
    assert not (env["child"] / "bird.xmp").exists()
    assert _folder_path(env["db"], env["root_id"]) == str(env["source"])
    assert _folder_path(env["db"], env["child_id"]) == str(env["child"])
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"])) == {
        "state": "remote",
        "workspace_id": env["workspace_id"],
    }
    assert not workspace_dir(str(env["vireo_dir"]), env["workspace_id"]).exists()


def test_sync_refuses_source_changes_and_preserves_local_workspace(local_workspace_env):
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "bird.jpg").write_bytes(b"local edit")
    (env["child"] / "bird.jpg").write_bytes(b"changed on nas")

    with pytest.raises(LocalWorkspaceConflict) as exc_info:
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert str(env["child"] / "bird.jpg") in exc_info.value.paths
    assert (local_child / "bird.jpg").read_bytes() == b"local edit"
    assert status(env["db"], env["workspace_id"], str(env["vireo_dir"]))["state"] == "active"


def test_sync_detects_source_change_with_preserved_size_and_mtime(local_workspace_env):
    env = local_workspace_env
    source_file = env["child"] / "bird.jpg"
    original_stat = source_file.stat()
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    source_file.write_bytes(b"BIRD-EXTERNAL")
    os.utime(source_file, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))

    with pytest.raises(LocalWorkspaceConflict) as exc_info:
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert str(source_file) in exc_info.value.paths


def test_sync_requires_explicit_confirmation_for_deletions(local_workspace_env):
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    os.unlink(local_child / "bird.xmp")

    with pytest.raises(LocalWorkspaceError, match="confirm deletions"):
        sync_back(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert (env["child"] / "bird.xmp").exists()


def test_discard_restores_catalog_without_changing_source(local_workspace_env):
    env = local_workspace_env
    stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))
    local_child = Path(_folder_path(env["db"], env["child_id"]))
    (local_child / "bird.jpg").write_bytes(b"discard me")

    result = discard_local(env["db"], env["workspace_id"], str(env["vireo_dir"]))

    assert result == {"ok": True, "discarded": True}
    assert (env["child"] / "bird.jpg").read_bytes() == b"bird-original"
    assert _folder_path(env["db"], env["child_id"]) == str(env["child"])


def test_stage_rejects_folders_shared_with_another_workspace(local_workspace_env):
    env = local_workspace_env
    other_workspace = env["db"].create_workspace("Shared")
    env["db"].add_workspace_folder(other_workspace, env["root_id"])

    with pytest.raises(LocalWorkspaceError, match="also used by another workspace"):
        stage_workspace(env["db"], env["workspace_id"], str(env["vireo_dir"]))


def test_stage_rejects_folder_covered_by_another_workspace_root(tmp_path):
    source_root = tmp_path / "nas" / "photos"
    source_root.mkdir(parents=True)
    db = Database(str(tmp_path / "vireo.db"))
    first_workspace = db._active_workspace_id
    db.set_active_workspace(None)
    root_id = db.add_folder(str(source_root), name="photos")
    db.set_active_workspace(first_workspace)
    db.add_workspace_folder(first_workspace, root_id)

    nested = source_root / "2026"
    nested.mkdir()
    (nested / "bird.jpg").write_bytes(b"bird")
    second_workspace = db.create_workspace("Nested")
    db.set_active_workspace(second_workspace)
    nested_id = db.add_folder(str(nested), name="2026")

    # The ancestor workspace was linked before the nested folder existed, so
    # it has no exact workspace_folders row for nested_id.
    exact_link = db.conn.execute(
        "SELECT 1 FROM workspace_folders WHERE workspace_id=? AND folder_id=?",
        (first_workspace, nested_id),
    ).fetchone()
    assert exact_link is None

    with pytest.raises(LocalWorkspaceError, match="overlaps a root used by another workspace"):
        stage_workspace(db, second_workspace, str(tmp_path / "local-data"))
    db.close()


def test_work_locally_http_job_flow(tmp_path, monkeypatch):
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
    folder_id = db.add_folder(str(source), name="photos")
    workspace_id = db._active_workspace_id
    db.close()

    app = create_app(db_path, thumb_cache_dir=str(thumbs))
    app.config["TESTING"] = True
    with app.test_client() as client:
        assert client.get("/api/workspaces/active/local-workspace").get_json()["state"] == "remote"

        response = client.post("/api/workspaces/active/local-workspace/stage", json={})
        assert response.status_code == 202
        stage_job = wait_for_job_via_client(client, response.get_json()["job_id"])
        assert stage_job["status"] == "completed"

        local_status = client.get("/api/workspaces/active/local-workspace").get_json()
        assert local_status["state"] == "active"
        other_workspace = client.post("/api/workspaces", json={"name": "Other"}).get_json()["id"]
        assert client.post(f"/api/workspaces/{other_workspace}/activate").status_code == 200
        blocked_delete = client.delete(f"/api/workspaces/{workspace_id}")
        assert blocked_delete.status_code == 400
        assert "sync or discard" in blocked_delete.get_json()["error"]
        assert client.post(f"/api/workspaces/{workspace_id}/activate").status_code == 200

        check_db = Database(db_path)
        local_path = _folder_path(check_db, folder_id)
        check_db.close()
        Path(local_path, "bird.jpg").write_bytes(b"edited")

        response = client.post(
            "/api/workspaces/active/local-workspace/sync",
            json={"confirm_deletions": False},
        )
        assert response.status_code == 202
        sync_job = wait_for_job_via_client(client, response.get_json()["job_id"])
        assert sync_job["status"] == "completed"
        assert (source / "bird.jpg").read_bytes() == b"edited"
        assert client.get("/api/workspaces/active/local-workspace").get_json()["state"] == "remote"

    final_db = Database(db_path)
    assert _folder_path(final_db, folder_id) == str(source)
    assert final_db._active_workspace_id == workspace_id
    final_db.close()
