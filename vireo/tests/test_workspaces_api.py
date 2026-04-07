"""Tests for workspace API routes (/api/workspaces/*)."""
import json


def test_list_workspaces(app_and_db):
    """GET /api/workspaces returns at least the Default workspace."""
    app, _db = app_and_db
    client = app.test_client()
    resp = client.get("/api/workspaces")
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list)
    assert len(data) >= 1
    names = {ws["name"] for ws in data}
    assert "Default" in names


def test_get_active_workspace(app_and_db):
    """GET /api/workspaces/active returns active workspace with folders list."""
    app, _db = app_and_db
    client = app.test_client()
    resp = client.get("/api/workspaces/active")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "id" in data
    assert "name" in data
    assert "folders" in data
    assert isinstance(data["folders"], list)


def test_create_workspace(app_and_db):
    """POST /api/workspaces with name creates a new workspace."""
    app, _db = app_and_db
    client = app.test_client()
    resp = client.post("/api/workspaces", json={"name": "Test WS"})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["name"] == "Test WS"
    assert "id" in data

    # Verify it appears in the listing
    listing = client.get("/api/workspaces").get_json()
    names = {ws["name"] for ws in listing}
    assert "Test WS" in names


def test_create_workspace_empty_name(app_and_db):
    """POST /api/workspaces with empty name returns 400."""
    app, _db = app_and_db
    client = app.test_client()

    resp = client.post("/api/workspaces", json={"name": ""})
    assert resp.status_code == 400
    assert "error" in resp.get_json()

    resp2 = client.post("/api/workspaces", json={"name": "   "})
    assert resp2.status_code == 400


def test_create_workspace_with_folders(app_and_db):
    """POST /api/workspaces with folder_ids links folders to the workspace."""
    app, db = app_and_db
    client = app.test_client()

    # Get the existing folder IDs from the DB
    folders = db.conn.execute("SELECT id FROM folders").fetchall()
    folder_ids = [f["id"] for f in folders]

    resp = client.post("/api/workspaces", json={
        "name": "Folder WS",
        "folder_ids": folder_ids,
    })
    assert resp.status_code == 200
    ws_id = resp.get_json()["id"]

    # Verify folders are linked
    folder_resp = client.get(f"/api/workspaces/{ws_id}/folders")
    assert folder_resp.status_code == 200
    linked = folder_resp.get_json()
    assert len(linked) == len(folder_ids)
    linked_ids = {f["id"] for f in linked}
    assert linked_ids == set(folder_ids)


def test_update_workspace(app_and_db):
    """PUT /api/workspaces/<id> updates the workspace name."""
    app, _db = app_and_db
    client = app.test_client()

    # Create a workspace to update
    create_resp = client.post("/api/workspaces", json={"name": "OldName"})
    ws_id = create_resp.get_json()["id"]

    resp = client.put(f"/api/workspaces/{ws_id}", json={"name": "NewName"})
    assert resp.status_code == 200
    assert resp.get_json()["name"] == "NewName"


def test_delete_workspace(app_and_db):
    """DELETE /api/workspaces/<id> removes the workspace."""
    app, _db = app_and_db
    client = app.test_client()

    # Create a second workspace so we can delete it
    create_resp = client.post("/api/workspaces", json={"name": "ToDelete"})
    ws_id = create_resp.get_json()["id"]

    resp = client.delete(f"/api/workspaces/{ws_id}")
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True

    # Verify it's gone from the listing
    listing = client.get("/api/workspaces").get_json()
    ids = {ws["id"] for ws in listing}
    assert ws_id not in ids


def test_delete_only_workspace_fails(app_and_db):
    """Cannot delete the last remaining workspace (400)."""
    app, db = app_and_db
    client = app.test_client()

    # Only the Default workspace exists; trying to delete it should fail
    ws_id = db._active_workspace_id
    resp = client.delete(f"/api/workspaces/{ws_id}")
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_delete_active_workspace_fails(app_and_db):
    """Cannot delete the active workspace (400), error mentions 'active'."""
    app, db = app_and_db
    client = app.test_client()

    # Create a second workspace so the "only workspace" guard doesn't fire
    client.post("/api/workspaces", json={"name": "Extra"})

    active_id = db._active_workspace_id
    resp = client.delete(f"/api/workspaces/{active_id}")
    assert resp.status_code == 400
    error_msg = resp.get_json()["error"]
    assert "active" in error_msg.lower()


def test_activate_workspace(app_and_db):
    """POST /api/workspaces/<id>/activate switches the active workspace."""
    app, db = app_and_db
    client = app.test_client()

    old_active = db._active_workspace_id

    # Create a new workspace and activate it
    create_resp = client.post("/api/workspaces", json={"name": "Activate Me"})
    new_id = create_resp.get_json()["id"]

    resp = client.post(f"/api/workspaces/{new_id}/activate", json={})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["workspace"]["id"] == new_id

    # Active workspace endpoint should now reflect the new workspace
    active_resp = client.get("/api/workspaces/active")
    assert active_resp.get_json()["id"] == new_id
    assert new_id != old_active


def test_activate_saves_and_restores_path(app_and_db):
    """Activation saves current_path on old WS, restores from new WS ui_state."""
    app, db = app_and_db
    client = app.test_client()

    # Create a second workspace with a saved last_path in ui_state
    create_resp = client.post("/api/workspaces", json={"name": "WS-B"})
    ws_b_id = create_resp.get_json()["id"]

    # Set ui_state on WS-B with a last_path
    db.update_workspace(ws_b_id, ui_state={"last_path": "/browse?folder=5"})

    # Activate WS-B while sending current_path for the old workspace
    resp = client.post(
        f"/api/workspaces/{ws_b_id}/activate",
        json={"current_path": "/review"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["restore_path"] == "/browse?folder=5"

    # Verify the old workspace had /review saved
    old_ws = db.get_workspace(db.get_workspaces()[-1]["id"])
    # Find the Default workspace (the one we just left)
    all_ws = db.get_workspaces()
    default_ws = [w for w in all_ws if w["name"] == "Default"][0]
    ui = json.loads(default_ws["ui_state"]) if default_ws["ui_state"] else {}
    assert ui.get("last_path") == "/review"


def test_activate_nonexistent_workspace(app_and_db):
    """POST /api/workspaces/<id>/activate returns 404 for nonexistent id."""
    app, _db = app_and_db
    client = app.test_client()
    resp = client.post("/api/workspaces/999999/activate", json={})
    assert resp.status_code == 404
    assert "error" in resp.get_json()


def test_workspace_folder_management(app_and_db):
    """Add folder to workspace, verify listed, remove it, verify gone."""
    app, db = app_and_db
    client = app.test_client()

    # Create a workspace
    create_resp = client.post("/api/workspaces", json={"name": "FolderTest"})
    ws_id = create_resp.get_json()["id"]

    # Grab a folder id
    folder = db.conn.execute("SELECT id FROM folders LIMIT 1").fetchone()
    fid = folder["id"]

    # Add folder
    add_resp = client.post(f"/api/workspaces/{ws_id}/folders", json={"folder_id": fid})
    assert add_resp.status_code == 200
    assert add_resp.get_json()["ok"] is True

    # Verify it's listed
    list_resp = client.get(f"/api/workspaces/{ws_id}/folders")
    assert list_resp.status_code == 200
    assert any(f["id"] == fid for f in list_resp.get_json())

    # Remove folder
    rm_resp = client.delete(f"/api/workspaces/{ws_id}/folders/{fid}")
    assert rm_resp.status_code == 200
    assert rm_resp.get_json()["ok"] is True

    # Verify it's gone
    list_resp2 = client.get(f"/api/workspaces/{ws_id}/folders")
    assert not any(f["id"] == fid for f in list_resp2.get_json())


def test_add_folder_missing_id(app_and_db):
    """POST folders without folder_id returns 400."""
    app, db = app_and_db
    client = app.test_client()

    ws_id = db._active_workspace_id

    resp = client.post(f"/api/workspaces/{ws_id}/folders", json={})
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_workspace_config_get_and_set(app_and_db):
    """GET config initially empty, POST sets overrides, GET reads them back."""
    app, _db = app_and_db
    client = app.test_client()

    # Initial config should be empty (no overrides)
    resp = client.get("/api/workspaces/active/config")
    assert resp.status_code == 200
    initial = resp.get_json()
    assert isinstance(initial, dict)
    # Default workspace has no overrides set
    assert len(initial) == 0

    # Set some overrides
    overrides = {
        "classification_threshold": 0.75,
        "grouping_window_seconds": 120,
    }
    set_resp = client.post("/api/workspaces/active/config", json=overrides)
    assert set_resp.status_code == 200
    set_data = set_resp.get_json()
    assert set_data["ok"] is True
    assert set_data["overrides"]["classification_threshold"] == 0.75
    assert set_data["overrides"]["grouping_window_seconds"] == 120

    # Read them back
    get_resp = client.get("/api/workspaces/active/config")
    assert get_resp.status_code == 200
    config = get_resp.get_json()
    assert config["classification_threshold"] == 0.75
    assert config["grouping_window_seconds"] == 120


def test_workspace_config_ignores_unknown_keys(app_and_db):
    """Only allowed keys are stored; unknown keys are silently ignored."""
    app, _db = app_and_db
    client = app.test_client()

    payload = {
        "classification_threshold": 0.5,
        "similarity_threshold": 0.8,
        "grouping_window_seconds": 60,
        "unknown_key": "should_be_dropped",
        "api_key": "secret",
    }
    resp = client.post("/api/workspaces/active/config", json=payload)
    assert resp.status_code == 200
    stored = resp.get_json()["overrides"]

    # Only allowed keys present
    assert "classification_threshold" in stored
    assert "similarity_threshold" in stored
    assert "grouping_window_seconds" in stored
    assert "unknown_key" not in stored
    assert "api_key" not in stored

    # Verify via GET as well
    get_resp = client.get("/api/workspaces/active/config")
    config = get_resp.get_json()
    assert "unknown_key" not in config
    assert "api_key" not in config
    assert config["classification_threshold"] == 0.5


def test_move_folders_to_existing_workspace(app_and_db):
    """POST move-folders moves folders to an existing workspace."""
    app, db = app_and_db
    client = app.test_client()

    active = client.get("/api/workspaces/active").get_json()
    source_ws_id = active["id"]
    folder_ids = [f["id"] for f in active["folders"]]
    assert len(folder_ids) > 0

    target_resp = client.post("/api/workspaces", json={"name": "Target WS"})
    target_ws_id = target_resp.get_json()["id"]

    resp = client.post(f"/api/workspaces/{source_ws_id}/move-folders", json={
        "folder_ids": folder_ids,
        "target_workspace_id": target_ws_id,
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["folders_moved"] == len(folder_ids)
    assert data["target_workspace_id"] == target_ws_id

    target_folders = client.get(f"/api/workspaces/{target_ws_id}/folders").get_json()
    assert len(target_folders) == len(folder_ids)
    source_folders = client.get(f"/api/workspaces/{source_ws_id}/folders").get_json()
    assert len(source_folders) == 0


def test_move_folders_to_new_workspace(app_and_db):
    """POST move-folders with new_workspace_name creates WS and moves folders."""
    app, db = app_and_db
    client = app.test_client()

    active = client.get("/api/workspaces/active").get_json()
    source_ws_id = active["id"]
    folder_ids = [f["id"] for f in active["folders"]]

    resp = client.post(f"/api/workspaces/{source_ws_id}/move-folders", json={
        "folder_ids": folder_ids,
        "new_workspace_name": "Brand New WS",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["folders_moved"] == len(folder_ids)
    assert data["target_workspace_id"] is not None

    new_ws_folders = client.get(f"/api/workspaces/{data['target_workspace_id']}/folders").get_json()
    assert len(new_ws_folders) == len(folder_ids)


def test_move_folders_no_folder_ids_returns_400(app_and_db):
    """POST move-folders with empty folder_ids returns 400."""
    app, db = app_and_db
    client = app.test_client()

    active = client.get("/api/workspaces/active").get_json()
    resp = client.post(f"/api/workspaces/{active['id']}/move-folders", json={
        "folder_ids": [],
        "target_workspace_id": 1,
    })
    assert resp.status_code == 400


def test_move_folders_no_target_returns_400(app_and_db):
    """POST move-folders without target or name returns 400."""
    app, db = app_and_db
    client = app.test_client()

    active = client.get("/api/workspaces/active").get_json()
    resp = client.post(f"/api/workspaces/{active['id']}/move-folders", json={
        "folder_ids": [1],
    })
    assert resp.status_code == 400
