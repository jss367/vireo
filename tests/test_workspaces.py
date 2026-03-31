import json

import pytest
from db import Database


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


def test_detections_has_workspace_id(db):
    cols = [r[1] for r in db.conn.execute("PRAGMA table_info(detections)").fetchall()]
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
    det_ids = db.save_detections(photo_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], "Robin", 0.95, "bioclip")
    row = db.conn.execute(
        "SELECT workspace_id FROM detections WHERE id = ?", (det_ids[0],)
    ).fetchone()
    assert row["workspace_id"] == ws_id


def test_get_predictions_scoped_to_workspace(db_with_workspace):
    db, ws_id, folder_id, photo_id = db_with_workspace
    det_ids1 = db.save_detections(photo_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids1[0], "Robin", 0.95, "bioclip")
    # Create second workspace with same photo, different prediction
    ws2 = db.create_workspace("Other")
    db.add_workspace_folder(ws2, folder_id)
    db.set_active_workspace(ws2)
    det_ids2 = db.save_detections(photo_id, [
        {"box": {"x": 0.2, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.8, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids2[0], "Sparrow", 0.8, "bioclip")
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
    det_ids1 = db.save_detections(photo_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids1[0], "Robin", 0.95, "bioclip")
    ws2 = db.create_workspace("Other")
    db.add_workspace_folder(ws2, folder_id)
    db.set_active_workspace(ws2)
    det_ids2 = db.save_detections(photo_id, [
        {"box": {"x": 0.2, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.8, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids2[0], "Sparrow", 0.8, "bioclip")
    # Clear ws2 predictions only
    db.clear_predictions()
    assert len(db.get_predictions()) == 0
    # ws1 predictions untouched
    db.set_active_workspace(ws_id)
    assert len(db.get_predictions()) == 1


def test_cascade_delete_removes_predictions(db_with_workspace):
    db, ws_id, _, photo_id = db_with_workspace
    det_ids = db.save_detections(photo_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], "Robin", 0.95, "bioclip")
    db.delete_workspace(ws_id)
    # Cascade: workspace -> detections -> predictions
    count = db.conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
    assert count == 0
    det_count = db.conn.execute("SELECT COUNT(*) FROM detections").fetchone()[0]
    assert det_count == 0


def test_save_detections_without_active_workspace_raises(db):
    folder_id = db.add_folder("/photos", name="photos")
    photo_id = db.add_photo(folder_id, "bird.jpg", ".jpg", 1000, 1.0)
    db._active_workspace_id = None  # Clear auto-set workspace
    with pytest.raises(RuntimeError):
        db.save_detections(photo_id, [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
        ], detector_model="MDV6")


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
    det_ids = db.save_detections(photo_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], "Robin", 0.95, "bioclip",
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


# -- Task 7: JobRunner workspace_id in job_history --


def test_job_history_workspace_id(db_with_workspace):
    from jobs import JobRunner
    db, ws_id, _, _ = db_with_workspace
    runner = JobRunner(db=db)

    # Start a job with workspace_id
    def noop(job):
        return {"ok": True}

    job_id = runner.start("test-job", noop, workspace_id=ws_id)

    # Wait for job to complete
    import sqlite3
    import time
    for _ in range(100):
        job = runner.get(job_id)
        if job and job["status"] in ("completed", "failed"):
            break
        time.sleep(0.05)
    assert job["status"] == "completed", f"Job did not complete: {job}"

    # Poll for persistence (written by background thread via separate connection)
    db_path = db.conn.execute("PRAGMA database_list").fetchone()[2]
    row = None
    for _ in range(50):
        conn = sqlite3.connect(db_path, timeout=5)
        row = conn.execute(
            "SELECT workspace_id FROM job_history WHERE id = ?", (job_id,)
        ).fetchone()
        conn.close()
        if row is not None:
            break
        time.sleep(0.05)

    # Verify workspace_id was persisted
    assert row is not None, "Job was not persisted to job_history"
    assert row[0] == ws_id, f"Expected workspace_id={ws_id}, got {row[0]}"


def test_job_history_filtered_by_workspace(db_with_workspace):
    import sqlite3
    import time

    from jobs import JobRunner

    db, ws_id, _, _ = db_with_workspace
    ws2_id = db.create_workspace("Other WS")
    runner = JobRunner(db=db)

    def noop(job):
        return {"ok": True}

    # Start jobs in two different workspaces
    job1_id = runner.start("test-ws1", noop, workspace_id=ws_id)
    job2_id = runner.start("test-ws2", noop, workspace_id=ws2_id)

    # Wait for both to complete
    for _ in range(100):
        j1 = runner.get(job1_id)
        j2 = runner.get(job2_id)
        if (j1 and j1["status"] in ("completed", "failed") and
                j2 and j2["status"] in ("completed", "failed")):
            break
        time.sleep(0.05)
    assert j1["status"] == "completed", f"Job 1 did not complete: {j1}"
    assert j2["status"] == "completed", f"Job 2 did not complete: {j2}"

    # Poll for persistence (written by background thread via separate connection)
    db_path = db.conn.execute("PRAGMA database_list").fetchone()[2]
    for _ in range(50):
        conn = sqlite3.connect(db_path, timeout=5)
        count = conn.execute("SELECT COUNT(*) FROM job_history").fetchone()[0]
        conn.close()
        if count >= 2:
            break
        time.sleep(0.05)

    # Query history scoped to ws_id
    db.set_active_workspace(ws_id)
    history = runner.get_history(db, limit=10)
    job_ids = [h["id"] for h in history]
    assert job1_id in job_ids
    assert job2_id not in job_ids


# -- Task 8: Migration test with pre-existing data --


def test_migration_from_legacy_db(tmp_path):
    """Simulate opening an existing Vireo DB that has no workspace tables."""
    import sqlite3
    db_path = str(tmp_path / "legacy.db")
    # Create legacy schema manually (no workspaces table)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript("""
        CREATE TABLE folders (id INTEGER PRIMARY KEY, path TEXT UNIQUE,
                              parent_id INTEGER, name TEXT, photo_count INTEGER DEFAULT 0);
        CREATE TABLE photos (id INTEGER PRIMARY KEY, folder_id INTEGER REFERENCES folders(id),
                             filename TEXT, extension TEXT, file_size INTEGER, file_mtime REAL,
                             xmp_mtime REAL, timestamp TEXT, width INTEGER, height INTEGER,
                             rating INTEGER DEFAULT 0, flag TEXT DEFAULT 'none',
                             thumb_path TEXT, sharpness REAL, detection_box TEXT,
                             detection_conf REAL, subject_sharpness REAL, subject_size REAL,
                             quality_score REAL, embedding BLOB, latitude REAL,
                             longitude REAL, phash TEXT, UNIQUE(folder_id, filename));
        CREATE TABLE keywords (id INTEGER PRIMARY KEY, name TEXT, parent_id INTEGER,
                               is_species INTEGER DEFAULT 0, UNIQUE(name, parent_id));
        CREATE TABLE photo_keywords (photo_id INTEGER, keyword_id INTEGER,
                                     PRIMARY KEY (photo_id, keyword_id));
        CREATE TABLE collections (id INTEGER PRIMARY KEY, name TEXT, rules TEXT);
        CREATE TABLE pending_changes (id INTEGER PRIMARY KEY, photo_id INTEGER,
                                      change_type TEXT, value TEXT,
                                      created_at TEXT DEFAULT (datetime('now')));
        CREATE TABLE predictions (id INTEGER PRIMARY KEY, photo_id INTEGER,
                                  species TEXT, confidence REAL, model TEXT, category TEXT,
                                  status TEXT DEFAULT 'pending', group_id TEXT,
                                  vote_count INTEGER, total_votes INTEGER, individual TEXT,
                                  taxonomy_kingdom TEXT, taxonomy_phylum TEXT,
                                  taxonomy_class TEXT, taxonomy_order TEXT,
                                  taxonomy_family TEXT, taxonomy_genus TEXT,
                                  scientific_name TEXT,
                                  created_at TEXT DEFAULT (datetime('now')),
                                  UNIQUE(photo_id, model));
    """)
    # Insert legacy data
    conn.execute("INSERT INTO folders (path, name) VALUES ('/photos', 'photos')")
    conn.execute("INSERT INTO photos (folder_id, filename, extension, file_size, file_mtime) "
                 "VALUES (1, 'bird.jpg', '.jpg', 1000, 1.0)")
    conn.execute("INSERT INTO predictions (photo_id, species, confidence, model) "
                 "VALUES (1, 'Robin', 0.95, 'bioclip')")
    conn.execute("INSERT INTO collections (name, rules) VALUES ('Test', '[]')")
    conn.execute("INSERT INTO pending_changes (photo_id, change_type, value) "
                 "VALUES (1, 'keyword_add', 'Robin')")
    conn.commit()
    conn.close()

    # Opening with Database triggers migration
    db = Database(db_path)

    # Default workspace created
    ws = db.conn.execute("SELECT * FROM workspaces WHERE name = 'Default'").fetchone()
    assert ws is not None
    default_id = ws[0]

    # Folder linked to default workspace
    wf = db.conn.execute(
        "SELECT * FROM workspace_folders WHERE workspace_id = ?", (default_id,)
    ).fetchall()
    assert len(wf) == 1

    # Predictions table was dropped and recreated with detection_id schema.
    # Legacy predictions are lost (accepted keywords survive in photo_keywords).
    pred_count = db.conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
    assert pred_count == 0

    # Detections table exists with workspace_id
    det_cols = [r[1] for r in db.conn.execute("PRAGMA table_info(detections)").fetchall()]
    assert "workspace_id" in det_cols
    assert "photo_id" in det_cols

    # predictions table now uses detection_id
    pred_cols = [r[1] for r in db.conn.execute("PRAGMA table_info(predictions)").fetchall()]
    assert "detection_id" in pred_cols
    assert "photo_id" not in pred_cols

    # Collections and pending_changes backfilled with workspace_id
    coll = db.conn.execute("SELECT workspace_id FROM collections").fetchone()
    assert coll[0] == default_id
    pc = db.conn.execute("SELECT workspace_id FROM pending_changes").fetchone()
    assert pc[0] == default_id


# -- Workspace API route tests --


@pytest.fixture
def client(tmp_path):
    """Flask test client with a fresh DB."""
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "vireo"))
    from app import create_app
    app = create_app(str(tmp_path / "test.db"))
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def test_api_get_workspaces(client):
    resp = client.get("/api/workspaces")
    assert resp.status_code == 200
    data = resp.get_json()
    # Default workspace exists
    assert any(w["name"] == "Default" for w in data)


def test_api_create_workspace(client):
    resp = client.post("/api/workspaces",
        data=json.dumps({"name": "Kenya 2025"}),
        content_type="application/json")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["name"] == "Kenya 2025"
    assert "id" in data


def test_api_activate_workspace(client):
    # Create a workspace
    resp = client.post("/api/workspaces",
        data=json.dumps({"name": "Test"}),
        content_type="application/json")
    ws_id = resp.get_json()["id"]
    # Activate it
    resp = client.post(f"/api/workspaces/{ws_id}/activate")
    assert resp.status_code == 200
    # Verify it's active
    resp = client.get("/api/workspaces/active")
    assert resp.get_json()["id"] == ws_id


def test_api_delete_workspace(client):
    resp = client.post("/api/workspaces",
        data=json.dumps({"name": "ToDelete"}),
        content_type="application/json")
    ws_id = resp.get_json()["id"]
    resp = client.delete(f"/api/workspaces/{ws_id}")
    assert resp.status_code == 200


def test_api_rename_workspace(client):
    resp = client.post("/api/workspaces",
        data=json.dumps({"name": "Old"}),
        content_type="application/json")
    ws_id = resp.get_json()["id"]
    resp = client.put(f"/api/workspaces/{ws_id}",
        data=json.dumps({"name": "New"}),
        content_type="application/json")
    assert resp.status_code == 200
    assert resp.get_json()["name"] == "New"


def test_api_workspace_folders(client):
    # Get folder tree first (need at least one folder)
    resp = client.get("/api/workspaces/active")
    ws_id = resp.get_json()["id"]
    resp = client.get(f"/api/workspaces/{ws_id}/folders")
    assert resp.status_code == 200


# -- Workspace-scoped photo and folder queries --


def test_get_photos_scoped_to_workspace(db):
    """Photos from folders not in workspace are not returned."""
    ws = db.create_workspace("Test")
    f1 = db.add_folder("/photos/kenya", name="kenya")
    f2 = db.add_folder("/photos/usa", name="usa")
    db.add_workspace_folder(ws, f1)
    # f2 NOT linked to workspace
    db.add_photo(f1, "lion.jpg", ".jpg", 1000, 1.0)
    db.add_photo(f2, "eagle.jpg", ".jpg", 1000, 1.0)
    db.set_active_workspace(ws)
    photos = db.get_photos(per_page=100)
    assert len(photos) == 1
    assert photos[0]["filename"] == "lion.jpg"


def test_count_photos_scoped_to_workspace(db):
    ws = db.create_workspace("Test")
    f1 = db.add_folder("/photos/kenya", name="kenya")
    f2 = db.add_folder("/photos/usa", name="usa")
    db.add_workspace_folder(ws, f1)
    db.add_photo(f1, "lion.jpg", ".jpg", 1000, 1.0)
    db.add_photo(f2, "eagle.jpg", ".jpg", 1000, 1.0)
    db.set_active_workspace(ws)
    assert db.count_photos() == 1


def test_count_folders_scoped_to_workspace(db):
    ws = db.create_workspace("Test")
    f1 = db.add_folder("/photos/kenya", name="kenya")
    f2 = db.add_folder("/photos/usa", name="usa")
    db.add_workspace_folder(ws, f1)
    db.set_active_workspace(ws)
    assert db.count_folders() == 1


def test_get_folder_tree_scoped_to_workspace(db):
    ws = db.create_workspace("Test")
    f1 = db.add_folder("/photos/kenya", name="kenya")
    f2 = db.add_folder("/photos/usa", name="usa")
    db.add_workspace_folder(ws, f1)
    db.set_active_workspace(ws)
    folders = db.get_folder_tree()
    assert len(folders) == 1
    assert folders[0]["path"] == "/photos/kenya"


def test_count_keywords_scoped_by_workspace(db):
    """count_keywords only counts keywords used by photos in the active workspace."""
    # Workspace A with a folder and photo
    ws_a = db.create_workspace("A")
    fid_a = db.add_folder("/photos/a", name="a")
    db.add_workspace_folder(ws_a, fid_a)
    pid_a = db.add_photo(folder_id=fid_a, filename="a.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    k1 = db.add_keyword("Robin")
    k2 = db.add_keyword("Jay")
    db.tag_photo(pid_a, k1)
    db.tag_photo(pid_a, k2)

    # Workspace B with a different folder and photo
    ws_b = db.create_workspace("B")
    fid_b = db.add_folder("/photos/b", name="b")
    db.add_workspace_folder(ws_b, fid_b)
    pid_b = db.add_photo(folder_id=fid_b, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    k3 = db.add_keyword("Hawk")
    db.tag_photo(pid_b, k3)

    db.set_active_workspace(ws_a)
    assert db.count_keywords() == 2

    db.set_active_workspace(ws_b)
    assert db.count_keywords() == 1


def test_count_keywords_empty_workspace(db):
    """A workspace with no photos returns 0 keywords."""
    ws = db.create_workspace("Empty")
    db.add_keyword("Robin")  # global keyword, no photos in this workspace
    db.set_active_workspace(ws)
    assert db.count_keywords() == 0


def test_dashboard_top_keywords_scoped_by_workspace(db):
    """get_dashboard_stats top_keywords only includes current workspace's keywords."""
    ws_a = db.create_workspace("A")
    fid_a = db.add_folder("/photos/a", name="a")
    db.add_workspace_folder(ws_a, fid_a)
    pid_a = db.add_photo(folder_id=fid_a, filename="a.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0, timestamp="2024-01-01T00:00:00")
    k1 = db.add_keyword("Robin")
    db.tag_photo(pid_a, k1)

    ws_b = db.create_workspace("B")
    fid_b = db.add_folder("/photos/b", name="b")
    db.add_workspace_folder(ws_b, fid_b)
    pid_b = db.add_photo(folder_id=fid_b, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0, timestamp="2024-01-01T00:00:00")
    k2 = db.add_keyword("Hawk")
    db.tag_photo(pid_b, k2)

    db.set_active_workspace(ws_a)
    stats = db.get_dashboard_stats()
    kw_names = [kw["name"] for kw in stats["top_keywords"]]
    assert "Robin" in kw_names
    assert "Hawk" not in kw_names

    db.set_active_workspace(ws_b)
    stats = db.get_dashboard_stats()
    kw_names = [kw["name"] for kw in stats["top_keywords"]]
    assert "Hawk" in kw_names
    assert "Robin" not in kw_names


def test_keyword_tree_scoped_by_workspace(db):
    """get_keyword_tree returns only keywords used by photos in the active workspace."""
    ws_a = db.create_workspace("A")
    fid_a = db.add_folder("/photos/a", name="a")
    db.add_workspace_folder(ws_a, fid_a)
    pid_a = db.add_photo(folder_id=fid_a, filename="a.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    k1 = db.add_keyword("Robin")
    k2 = db.add_keyword("Jay")
    db.tag_photo(pid_a, k1)
    db.tag_photo(pid_a, k2)

    ws_b = db.create_workspace("B")
    fid_b = db.add_folder("/photos/b", name="b")
    db.add_workspace_folder(ws_b, fid_b)
    pid_b = db.add_photo(folder_id=fid_b, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    k3 = db.add_keyword("Hawk")
    db.tag_photo(pid_b, k3)

    db.set_active_workspace(ws_a)
    tree = db.get_keyword_tree()
    names = [kw["name"] for kw in tree]
    assert "Robin" in names
    assert "Jay" in names
    assert "Hawk" not in names

    db.set_active_workspace(ws_b)
    tree = db.get_keyword_tree()
    names = [kw["name"] for kw in tree]
    assert "Hawk" in names
    assert "Robin" not in names
    assert "Jay" not in names


def test_keyword_tree_empty_workspace(db):
    """Empty workspace returns no keywords even if keywords exist globally."""
    ws = db.create_workspace("Empty")
    db.add_keyword("Robin")
    db.set_active_workspace(ws)
    tree = db.get_keyword_tree()
    assert len(tree) == 0


def test_get_collection_photos_scoped_to_workspace_folders(db):
    """Collection should only return photos from workspace folders."""
    ws = db.create_workspace("Test")
    f1 = db.add_folder("/photos/kenya", name="kenya")
    f2 = db.add_folder("/photos/usa", name="usa")
    db.add_workspace_folder(ws, f1)
    p1 = db.add_photo(f1, "lion.jpg", ".jpg", 1000, 1.0)
    p2 = db.add_photo(f2, "eagle.jpg", ".jpg", 1000, 1.0)
    db.update_photo_rating(p1, 5)
    db.update_photo_rating(p2, 5)
    db.set_active_workspace(ws)
    cid = db.add_collection("High Rated", json.dumps([{"field": "rating", "op": ">=", "value": 4}]))
    photos = db.get_collection_photos(cid, per_page=100)
    assert len(photos) == 1
    assert photos[0]["filename"] == "lion.jpg"


# -- Task 5: Workspace-scoped active labels helpers --


def test_get_workspace_active_labels_default_empty(db):
    """Workspace with no active_labels in config_overrides returns None."""
    ws = db.create_workspace("Fresh")
    db.set_active_workspace(ws)
    assert db.get_workspace_active_labels() is None


def test_set_and_get_workspace_active_labels(db):
    """set/get workspace active labels round-trips through config_overrides."""
    ws = db.create_workspace("Labeled")
    db.set_active_workspace(ws)
    paths = ["/home/user/.vireo/labels/ca-birds.txt", "/home/user/.vireo/labels/ca-reptiles.txt"]
    db.set_workspace_active_labels(paths)
    assert db.get_workspace_active_labels() == paths


def test_set_workspace_active_labels_preserves_other_overrides(db):
    """Setting active labels doesn't clobber other config_overrides."""
    ws = db.create_workspace("WithConfig", config_overrides={"threshold": 0.5})
    db.set_active_workspace(ws)
    db.set_workspace_active_labels(["/path/to/labels.txt"])
    result = db.get_workspace_active_labels()
    assert result == ["/path/to/labels.txt"]
    # Check threshold is still there
    overrides = json.loads(db.get_workspace(ws)["config_overrides"])
    assert overrides["threshold"] == 0.5


def test_merge_duplicate_keywords_scoped_by_workspace(db):
    """merge_duplicate_keywords only merges duplicates used in the active workspace."""
    ws_a = db.create_workspace("A")
    fid_a = db.add_folder("/photos/a", name="a")
    db.add_workspace_folder(ws_a, fid_a)
    pid_a = db.add_photo(folder_id=fid_a, filename="a.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)

    ws_b = db.create_workspace("B")
    fid_b = db.add_folder("/photos/b", name="b")
    db.add_workspace_folder(ws_b, fid_b)
    pid_b = db.add_photo(folder_id=fid_b, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)

    # Create case-variant duplicates via raw SQL (add_keyword dedupes)
    db.conn.execute("INSERT INTO keywords (name) VALUES ('Cardinal')")
    db.conn.execute("INSERT INTO keywords (name) VALUES ('cardinal')")
    db.conn.execute("INSERT INTO keywords (name) VALUES ('Sparrow')")
    db.conn.execute("INSERT INTO keywords (name) VALUES ('sparrow')")
    db.conn.commit()

    k_cardinal = db.conn.execute("SELECT id FROM keywords WHERE name='Cardinal'").fetchone()[0]
    k_cardinal_lc = db.conn.execute("SELECT id FROM keywords WHERE name='cardinal'").fetchone()[0]
    k_sparrow = db.conn.execute("SELECT id FROM keywords WHERE name='Sparrow'").fetchone()[0]
    k_sparrow_lc = db.conn.execute("SELECT id FROM keywords WHERE name='sparrow'").fetchone()[0]

    # Tag Cardinal/cardinal on photos in workspace A
    db.tag_photo(pid_a, k_cardinal)
    db.tag_photo(pid_a, k_cardinal_lc)
    # Tag Sparrow/sparrow on photos in workspace B
    db.tag_photo(pid_b, k_sparrow)
    db.tag_photo(pid_b, k_sparrow_lc)

    # Merge in workspace A — should only merge Cardinal pair
    db.set_active_workspace(ws_a)
    merged = db.merge_duplicate_keywords()
    assert merged == 1  # only Cardinal/cardinal

    # Sparrow/sparrow should still exist as two separate keywords
    sparrows = db.conn.execute(
        "SELECT COUNT(*) FROM keywords WHERE name IN ('Sparrow', 'sparrow')"
    ).fetchone()[0]
    assert sparrows == 2


def test_empty_workspace_labels_does_not_fallback_to_global(db):
    """An explicit empty active_labels [] should NOT fall back to global labels."""
    ws = db.create_workspace("Empty Labels")
    db.set_active_workspace(ws)
    db.set_workspace_active_labels([])  # explicitly no labels

    result = db.get_workspace_active_labels()
    assert result == []  # should be empty list, not None

    # Simulate what _load_labels does: ws_labels is not None should be True
    ws_labels = db.get_workspace_active_labels()
    assert ws_labels is not None  # must distinguish [] from None


def test_keyword_tree_includes_ancestors(db):
    """get_keyword_tree includes untagged ancestor keywords so hierarchy is navigable."""
    ws = db.create_workspace("Hier")
    fid = db.add_folder("/photos/h", name="h")
    db.add_workspace_folder(ws, fid)
    pid = db.add_photo(folder_id=fid, filename="h.jpg", extension=".jpg",
                       file_size=100, file_mtime=1.0)

    # Create hierarchy: Birds > Raptors > Red-tailed Hawk
    birds = db.add_keyword("Birds")
    raptors = db.add_keyword("Raptors", parent_id=birds)
    hawk = db.add_keyword("Red-tailed Hawk", parent_id=raptors)

    # Only tag the leaf (mimics scanner behavior)
    db.tag_photo(pid, hawk)

    db.set_active_workspace(ws)
    tree = db.get_keyword_tree()
    names = {kw["name"] for kw in tree}
    assert "Red-tailed Hawk" in names
    assert "Raptors" in names  # ancestor must be included
    assert "Birds" in names    # root ancestor must be included
