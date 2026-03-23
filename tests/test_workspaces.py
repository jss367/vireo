import json

import pytest


def test_database_creates_tables(db):
    tables = [r[0] for r in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    assert "photos" in tables
    assert "workspaces" in tables


def test_workspace_tables_exist(db):
    tables = [r[0] for r in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    assert "workspaces" in tables
    assert "workspace_folders" in tables


def test_predictions_has_workspace_id(db):
    cols = [r[1] for r in db.conn.execute("PRAGMA table_info(predictions)").fetchall()]
    assert "workspace_id" in cols


def test_collections_has_workspace_id(db):
    cols = [r[1] for r in db.conn.execute("PRAGMA table_info(collections)").fetchall()]
    assert "workspace_id" in cols


def test_pending_changes_has_workspace_id(db):
    cols = [r[1] for r in db.conn.execute("PRAGMA table_info(pending_changes)").fetchall()]
    assert "workspace_id" in cols


# -- Task 3: Workspace CRUD and active workspace --


def test_create_workspace(db):
    ws_id = db.create_workspace("Kenya 2025")
    assert ws_id is not None
    ws = db.get_workspace(ws_id)
    assert ws["name"] == "Kenya 2025"


def test_create_workspace_duplicate_name_raises(db):
    db.create_workspace("Kenya")
    with pytest.raises(Exception):
        db.create_workspace("Kenya")


def test_get_workspaces(db):
    db.create_workspace("A")
    db.create_workspace("B")
    workspaces = db.get_workspaces()
    names = [w["name"] for w in workspaces]
    assert "A" in names
    assert "B" in names


def test_update_workspace(db):
    ws_id = db.create_workspace("Old Name")
    db.update_workspace(ws_id, name="New Name")
    ws = db.get_workspace(ws_id)
    assert ws["name"] == "New Name"


def test_delete_workspace(db):
    ws_id = db.create_workspace("Temp")
    db.delete_workspace(ws_id)
    assert db.get_workspace(ws_id) is None


def test_workspace_folders(db):
    ws_id = db.create_workspace("Test")
    folder_id = db.add_folder("/photos/kenya", name="kenya")
    db.add_workspace_folder(ws_id, folder_id)
    folders = db.get_workspace_folders(ws_id)
    assert len(folders) == 1
    assert folders[0]["id"] == folder_id


def test_remove_workspace_folder(db):
    ws_id = db.create_workspace("Test")
    folder_id = db.add_folder("/photos/kenya", name="kenya")
    db.add_workspace_folder(ws_id, folder_id)
    db.remove_workspace_folder(ws_id, folder_id)
    assert len(db.get_workspace_folders(ws_id)) == 0


def test_set_active_workspace(db):
    ws_id = db.create_workspace("Active")
    db.set_active_workspace(ws_id)
    assert db._active_workspace_id == ws_id


def test_ensure_default_workspace(db):
    ws_id = db.ensure_default_workspace()
    ws = db.get_workspace(ws_id)
    assert ws["name"] == "Default"
    # Calling again returns same id
    assert db.ensure_default_workspace() == ws_id


# -- Task 4: Workspace-scoped predictions --


@pytest.fixture
def db_with_workspace(db):
    """DB with a workspace, folder, and a photo ready for predictions."""
    ws_id = db.create_workspace("Test WS")
    folder_id = db.add_folder("/photos", name="photos")
    db.add_workspace_folder(ws_id, folder_id)
    photo_id = db.add_photo(folder_id, "bird.jpg", ".jpg", 1000, 1.0)
    db.set_active_workspace(ws_id)
    return db, ws_id, folder_id, photo_id


def test_add_prediction_uses_workspace(db_with_workspace):
    db, ws_id, _, photo_id = db_with_workspace
    db.add_prediction(photo_id, "Robin", 0.95, "bioclip")
    row = db.conn.execute(
        "SELECT workspace_id FROM predictions WHERE photo_id = ?", (photo_id,)
    ).fetchone()
    assert row["workspace_id"] == ws_id


def test_get_predictions_scoped_to_workspace(db_with_workspace):
    db, ws_id, folder_id, photo_id = db_with_workspace
    db.add_prediction(photo_id, "Robin", 0.95, "bioclip")
    # Create second workspace with same photo, different prediction
    ws2 = db.create_workspace("Other")
    db.add_workspace_folder(ws2, folder_id)
    db.set_active_workspace(ws2)
    db.add_prediction(photo_id, "Sparrow", 0.8, "bioclip")
    # Each workspace sees only its own predictions
    preds_ws2 = db.get_predictions()
    assert len(preds_ws2) == 1
    assert preds_ws2[0]["species"] == "Sparrow"
    db.set_active_workspace(ws_id)
    preds_ws1 = db.get_predictions()
    assert len(preds_ws1) == 1
    assert preds_ws1[0]["species"] == "Robin"


def test_clear_predictions_scoped_to_workspace(db_with_workspace):
    db, ws_id, folder_id, photo_id = db_with_workspace
    db.add_prediction(photo_id, "Robin", 0.95, "bioclip")
    ws2 = db.create_workspace("Other")
    db.add_workspace_folder(ws2, folder_id)
    db.set_active_workspace(ws2)
    db.add_prediction(photo_id, "Sparrow", 0.8, "bioclip")
    # Clear ws2 predictions only
    db.clear_predictions()
    assert len(db.get_predictions()) == 0
    # ws1 predictions untouched
    db.set_active_workspace(ws_id)
    assert len(db.get_predictions()) == 1


def test_cascade_delete_removes_predictions(db_with_workspace):
    db, ws_id, _, photo_id = db_with_workspace
    db.add_prediction(photo_id, "Robin", 0.95, "bioclip")
    db.delete_workspace(ws_id)
    count = db.conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
    assert count == 0


def test_add_prediction_without_active_workspace_raises(db):
    folder_id = db.add_folder("/photos", name="photos")
    photo_id = db.add_photo(folder_id, "bird.jpg", ".jpg", 1000, 1.0)
    with pytest.raises(RuntimeError):
        db.add_prediction(photo_id, "Robin", 0.95, "bioclip")


# -- Task 5: Workspace-scoped collections --


def test_add_collection_uses_workspace(db_with_workspace):
    db, ws_id, _, _ = db_with_workspace
    cid = db.add_collection("Flagged", '[{"field":"flag","op":"equals","value":"flagged"}]')
    row = db.conn.execute(
        "SELECT workspace_id FROM collections WHERE id = ?", (cid,)
    ).fetchone()
    assert row["workspace_id"] == ws_id


def test_get_collections_scoped(db_with_workspace):
    db, ws_id, _, _ = db_with_workspace
    db.add_collection("WS1 Collection", "[]")
    ws2 = db.create_workspace("Other")
    db.set_active_workspace(ws2)
    db.add_collection("WS2 Collection", "[]")
    # Each workspace sees only its own
    assert len(db.get_collections()) == 1
    assert db.get_collections()[0]["name"] == "WS2 Collection"
    db.set_active_workspace(ws_id)
    assert len(db.get_collections()) == 1
    assert db.get_collections()[0]["name"] == "WS1 Collection"


def test_create_default_collections_per_workspace(db_with_workspace):
    db, ws_id, _, _ = db_with_workspace
    db.create_default_collections()
    count_ws1 = len(db.get_collections())
    ws2 = db.create_workspace("Other")
    db.set_active_workspace(ws2)
    db.create_default_collections()
    count_ws2 = len(db.get_collections())
    assert count_ws1 == count_ws2
    # Both workspaces have their own copies
    total = db.conn.execute("SELECT COUNT(*) FROM collections").fetchone()[0]
    assert total == count_ws1 + count_ws2


def test_cascade_delete_removes_collections(db_with_workspace):
    db, ws_id, _, _ = db_with_workspace
    db.add_collection("Test", "[]")
    db.delete_workspace(ws_id)
    count = db.conn.execute("SELECT COUNT(*) FROM collections").fetchone()[0]
    assert count == 0


def test_get_collection_photos_prediction_join_scoped(db_with_workspace):
    """Taxonomy rules in collection filter should use workspace-scoped predictions."""
    db, ws_id, folder_id, photo_id = db_with_workspace
    db.add_prediction(photo_id, "Robin", 0.95, "bioclip",
                      taxonomy={"order": "Passeriformes"})
    rules = json.dumps([{"field": "taxonomy_order", "op": "equals", "value": "Passeriformes"}])
    cid = db.add_collection("Passerines", rules)
    photos = db.get_collection_photos(cid, per_page=100)
    assert len(photos) == 1
    # Switch workspace — same collection rule should find nothing
    ws2 = db.create_workspace("Empty")
    db.set_active_workspace(ws2)
    cid2 = db.add_collection("Passerines", rules)
    photos2 = db.get_collection_photos(cid2, per_page=100)
    assert len(photos2) == 0


# -- Task 6: Workspace-scoped pending changes --


def test_queue_change_uses_workspace(db_with_workspace):
    db, ws_id, _, photo_id = db_with_workspace
    db.queue_change(photo_id, "keyword_add", "Robin")
    row = db.conn.execute(
        "SELECT workspace_id FROM pending_changes WHERE photo_id = ?", (photo_id,)
    ).fetchone()
    assert row["workspace_id"] == ws_id


def test_get_pending_changes_scoped(db_with_workspace):
    db, ws_id, _, photo_id = db_with_workspace
    db.queue_change(photo_id, "keyword_add", "Robin")
    ws2 = db.create_workspace("Other")
    db.set_active_workspace(ws2)
    assert len(db.get_pending_changes()) == 0
    db.set_active_workspace(ws_id)
    assert len(db.get_pending_changes()) == 1


def test_count_pending_changes_scoped(db_with_workspace):
    db, ws_id, _, photo_id = db_with_workspace
    db.queue_change(photo_id, "keyword_add", "Robin")
    assert db.count_pending_changes() == 1
    ws2 = db.create_workspace("Other")
    db.set_active_workspace(ws2)
    assert db.count_pending_changes() == 0


def test_queue_change_dedup_within_workspace(db_with_workspace):
    db, ws_id, _, photo_id = db_with_workspace
    db.queue_change(photo_id, "keyword_add", "Robin")
    db.queue_change(photo_id, "keyword_add", "Robin")
    assert db.count_pending_changes() == 1


def test_cascade_delete_removes_pending_changes(db_with_workspace):
    db, ws_id, _, photo_id = db_with_workspace
    db.queue_change(photo_id, "keyword_add", "Robin")
    db.delete_workspace(ws_id)
    count = db.conn.execute("SELECT COUNT(*) FROM pending_changes").fetchone()[0]
    assert count == 0
