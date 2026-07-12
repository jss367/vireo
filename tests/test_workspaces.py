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


def test_workspace_folder_roots_hide_recursive_descendants(db):
    ws_id = db._active_workspace_id
    db.set_active_workspace(None)
    root_id = db.add_folder("/photos/usa/2026", name="2026")
    child_id = db.add_folder(
        "/photos/usa/2026/2026-05-01",
        name="2026-05-01",
        parent_id=root_id,
    )
    db.set_active_workspace(ws_id)

    db.add_workspace_folder(ws_id, root_id)

    folders = {f["id"] for f in db.get_workspace_folders(ws_id)}
    roots = db.get_workspace_folder_roots(ws_id)

    assert root_id in folders
    assert child_id in folders
    assert [f["id"] for f in roots] == [root_id]


def test_workspace_root_materializes_existing_path_descendants(db):
    ws_id = db._active_workspace_id
    db.set_active_workspace(None)
    root_id = db.add_folder("/photos/usa/2026", name="2026")
    # Historical DBs can have path-descendant folders without parent_id.
    child_id = db.add_folder("/photos/usa/2026/2026-05-01", name="2026-05-01")
    db.add_photo(child_id, "bird.jpg", ".jpg", 1000, 1.0)
    db.set_active_workspace(ws_id)

    db.add_workspace_folder(ws_id, root_id)

    folders = {f["id"] for f in db.get_workspace_folders(ws_id)}
    roots = db.get_workspace_folder_roots(ws_id)
    photos = db.get_photos()

    assert root_id in folders
    assert child_id in folders
    assert [f["id"] for f in roots] == [root_id]
    assert [p["filename"] for p in photos] == ["bird.jpg"]


def test_existing_folder_is_reparented_when_surrounding_root_is_added(db):
    """A standalone folder root should nest once its parent is known."""
    ws_id = db._active_workspace_id
    db.set_active_workspace(None)
    usa_id = db.add_folder("/photos/USA", name="USA")
    year_id = db.add_folder("/photos/USA/2026", name="2026")
    date_id = db.add_folder(
        "/photos/USA/2026/2026-01-19",
        name="2026-01-19",
        parent_id=year_id,
    )
    db.set_active_workspace(ws_id)

    db.add_workspace_folder(ws_id, year_id)
    db.add_workspace_folder(ws_id, usa_id)

    # A later scan/import walks the full parent chain and now knows 2026's
    # parent. Reusing the row must fill in the missing parent_id.
    assert db.add_folder(
        "/photos/USA/2026",
        name="2026",
        parent_id=usa_id,
        workspace_root=False,
    ) == year_id

    tree = {f["id"]: f for f in db.get_folder_tree()}
    roots = [f for f in tree.values() if f["parent_id"] is None]

    assert [f["id"] for f in roots] == [usa_id]
    assert tree[year_id]["parent_id"] == usa_id
    assert tree[date_id]["parent_id"] == year_id


def test_database_repairs_parentless_folder_rows_on_open(tmp_path):
    """Existing databases with parentless path-children should self-heal."""
    from db import Database

    db_path = str(tmp_path / "test.db")
    seed = Database(db_path)
    ws_id = seed._active_workspace_id
    seed.conn.executemany(
        "INSERT INTO folders (id, path, name, parent_id) VALUES (?, ?, ?, ?)",
        [
            (101, "/photos/USA", "USA", None),
            (102, "/photos/USA/2026", "2026", None),
            (103, "/photos/USA/2026/2026-01-19", "2026-01-19", 102),
        ],
    )
    seed.conn.executemany(
        "INSERT INTO workspace_folders (workspace_id, folder_id, is_root) "
        "VALUES (?, ?, ?)",
        [
            (ws_id, 101, 1),
            (ws_id, 102, 0),
            (ws_id, 103, 0),
        ],
    )
    seed.conn.commit()
    seed.close()

    reopened = Database(db_path)
    reopened.set_active_workspace(ws_id)
    try:
        tree = {f["id"]: f for f in reopened.get_folder_tree()}
    finally:
        reopened.close()

    assert tree[101]["parent_id"] is None
    assert tree[102]["parent_id"] == 101
    assert tree[103]["parent_id"] == 102


def test_workspace_root_hides_materialized_descendant_when_parent_has_photos(db):
    ws_id = db._active_workspace_id
    db.set_active_workspace(None)
    root_id = db.add_folder("/photos/usa/2026", name="2026")
    child_id = db.add_folder(
        "/photos/usa/2026/2026-05-01",
        name="2026-05-01",
        parent_id=root_id,
    )
    db.add_photo(root_id, "root.jpg", ".jpg", 1000, 1.0)
    db.add_photo(child_id, "child.jpg", ".jpg", 1000, 1.0)
    db.set_active_workspace(ws_id)

    db.add_workspace_folder(ws_id, root_id)

    assert [f["id"] for f in db.get_workspace_folder_roots(ws_id)] == [root_id]
    assert {p["filename"] for p in db.get_photos()} == {"root.jpg", "child.jpg"}


def test_workspace_folder_roots_report_subtree_photo_counts(db):
    ws_id = db._active_workspace_id
    db.set_active_workspace(None)
    # Root with no direct photos — all images live in subfolders. The
    # direct-only folders.photo_count would read 0 here.
    root_id = db.add_folder("/photos/usa", name="usa")
    child_a = db.add_folder("/photos/usa/2026", name="2026", parent_id=root_id)
    child_b = db.add_folder("/photos/usa/2025", name="2025", parent_id=root_id)
    db.add_photo(child_a, "a1.jpg", ".jpg", 1000, 1.0)
    db.add_photo(child_a, "a2.jpg", ".jpg", 1000, 1.0)
    db.add_photo(child_b, "b1.jpg", ".jpg", 1000, 1.0)
    # A separate sibling root — its photos must not bleed into usa's count.
    other_root = db.add_folder("/photos/canada", name="canada")
    db.add_photo(other_root, "c1.jpg", ".jpg", 1000, 1.0)
    db.set_active_workspace(ws_id)

    db.add_workspace_folder(ws_id, root_id)
    db.add_workspace_folder(ws_id, other_root)

    counts = {
        f["path"]: f["workspace_photo_count"]
        for f in db.get_workspace_folder_roots(ws_id)
    }
    assert counts["/photos/usa"] == 3
    assert counts["/photos/canada"] == 1


def test_workspace_root_count_is_scoped_to_its_workspace(db):
    # The same folder tree lives in two workspaces; each root's count must
    # reflect only photos and folders linked to that workspace.
    db.set_active_workspace(None)
    root_id = db.add_folder("/photos/usa", name="usa")
    child_id = db.add_folder("/photos/usa/2026", name="2026", parent_id=root_id)
    db.add_photo(child_id, "a.jpg", ".jpg", 1000, 1.0)
    db.add_photo(child_id, "b.jpg", ".jpg", 1000, 1.0)

    ws_a = db.create_workspace("A")
    ws_b = db.create_workspace("B")
    db.set_active_workspace(ws_a)
    db.add_workspace_folder(ws_a, root_id)

    counts_a = {
        f["path"]: f["workspace_photo_count"]
        for f in db.get_workspace_folder_roots(ws_a)
    }
    assert counts_a["/photos/usa"] == 2
    # ws_b never linked the folder — it has no roots to report.
    assert db.get_workspace_folder_roots(ws_b) == []


def test_workspace_root_materializes_windows_style_descendants(db):
    ws_id = db._active_workspace_id
    db.set_active_workspace(None)
    root_id = db.add_folder(r"C:\photos\usa\2026", name="2026")
    child_id = db.add_folder(
        r"C:\photos\usa\2026\2026-05-01",
        name="2026-05-01",
    )
    db.add_photo(child_id, "bird.jpg", ".jpg", 1000, 1.0)
    db.set_active_workspace(ws_id)

    db.add_workspace_folder(ws_id, root_id)

    folders = {f["id"] for f in db.get_workspace_folders(ws_id)}
    roots = db.get_workspace_folder_roots(ws_id)
    photos = db.get_photos()

    assert root_id in folders
    assert child_id in folders
    assert [f["id"] for f in roots] == [root_id]
    assert [p["filename"] for p in photos] == ["bird.jpg"]


def test_workspace_root_materialization_is_case_sensitive(db):
    ws_id = db._active_workspace_id
    db.set_active_workspace(None)
    root_id = db.add_folder("/photos/USA/2026", name="2026")
    child_id = db.add_folder("/photos/USA/2026/2026-05-01", name="2026-05-01")
    other_id = db.add_folder("/photos/usa/2026/2026-05-02", name="2026-05-02")
    db.add_photo(child_id, "bird.jpg", ".jpg", 1000, 1.0)
    db.add_photo(other_id, "wrong-case.jpg", ".jpg", 1000, 1.0)
    db.set_active_workspace(ws_id)

    db.add_workspace_folder(ws_id, root_id)

    folders = {f["id"] for f in db.get_workspace_folders(ws_id)}
    assert root_id in folders
    assert child_id in folders
    assert other_id not in folders
    assert {p["filename"] for p in db.get_photos()} == {"bird.jpg"}


def test_workspace_large_recursive_root_operations_chunk_sql_params(db):
    ws_id = db._active_workspace_id
    db.set_active_workspace(None)
    root_id = db.add_folder("/photos/big", name="big")
    for idx in range(1005):
        db.add_folder(f"/photos/big/day-{idx:04d}", name=f"day-{idx:04d}")
    db.set_active_workspace(ws_id)

    db.add_workspace_folder(ws_id, root_id)

    assert len(db.get_workspace_folders(ws_id)) == 1006
    assert [f["id"] for f in db.get_workspace_folder_roots(ws_id)] == [root_id]

    target_ws_id = db.create_workspace("Target")
    result = db.move_folders_to_workspace(ws_id, target_ws_id, [root_id])

    assert result["folders_moved"] == 1
    assert len(db.get_workspace_folders(ws_id)) == 0
    assert len(db.get_workspace_folders(target_ws_id)) == 1006
    assert [f["id"] for f in db.get_workspace_folder_roots(target_ws_id)] == [root_id]

    db.remove_workspace_folder_tree(target_ws_id, root_id)

    assert len(db.get_workspace_folders(target_ws_id)) == 0


def test_materializing_workspace_descendants_invalidates_new_images_cache(db):
    ws_id = db.create_workspace("USA 2026")
    root_id = db.add_folder("/photos/usa/2026", name="2026")
    child_id = db.add_folder("/photos/usa/2026/day-1", name="day-1")

    db.conn.execute(
        """INSERT INTO workspace_folders (workspace_id, folder_id, is_root)
           VALUES (?, ?, 1)""",
        (ws_id, root_id),
    )
    db.conn.commit()
    db._new_images_cache.set(
        db._db_path, ws_id, {"new_count": 0, "per_root": [], "sample": []}
    )

    folders = {f["id"] for f in db.get_workspace_folders(ws_id)}

    assert child_id in folders
    assert db._new_images_cache.get(db._db_path, ws_id) is None


def test_remove_workspace_folder(db):
    ws_id = db.create_workspace("Test")
    folder_id = db.add_folder("/photos/kenya", name="kenya")
    db.add_workspace_folder(ws_id, folder_id)
    db.remove_workspace_folder(ws_id, folder_id)
    assert len(db.get_workspace_folders(ws_id)) == 0


def test_remove_workspace_root_unlinks_descendants(db):
    ws_id = db._active_workspace_id
    root_id = db.add_folder("/photos/usa/2026", name="2026")
    child_id = db.add_folder(
        "/photos/usa/2026/2026-05-01",
        name="2026-05-01",
        parent_id=root_id,
    )
    db.add_photo(child_id, "bird.jpg", ".jpg", 1000, 1.0)

    db.remove_workspace_folder_tree(ws_id, root_id)

    assert len(db.get_workspace_folders(ws_id)) == 0
    assert db.get_photos() == []


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


def test_get_predictions_scoped_to_workspace(db_with_workspace):
    """Predictions are global, but get_predictions() scopes visibility through
    workspace_folders. A prediction on a photo whose folder is linked only to
    ws1 is invisible to ws2."""
    db, ws_id, folder_id, photo_id = db_with_workspace
    det_ids1 = db.save_detections(photo_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids1[0], "Robin", 0.95, "bioclip")

    # Second workspace with a different folder + photo.
    ws2 = db.create_workspace("Other")
    db.set_active_workspace(ws2)
    folder2 = db.add_folder("/photos2", name="photos2")
    db.add_workspace_folder(ws2, folder2)
    photo2 = db.add_photo(folder2, "bird2.jpg", ".jpg", 1000, 1.0)
    det_ids2 = db.save_detections(photo2, [
        {"box": {"x": 0.2, "y": 0.2, "w": 0.3, "h": 0.4}, "confidence": 0.8, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids2[0], "Sparrow", 0.8, "bioclip")

    # Each workspace sees only predictions on its own folders' photos.
    preds_ws2 = db.get_predictions()
    assert len(preds_ws2) == 1
    assert preds_ws2[0]["species"] == "Sparrow"
    db.set_active_workspace(ws_id)
    preds_ws1 = db.get_predictions()
    assert len(preds_ws1) == 1
    assert preds_ws1[0]["species"] == "Robin"


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


# -- Workspace API route tests --


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Flask test client with a fresh DB.

    Isolates $HOME and config paths to tmp_path so create_app's
    ``_mark_species_and_maybe_backfill`` doesn't read the developer's real
    ``~/.vireo/taxonomy.json``. Without this, the lazy
    ``from taxonomy import ...`` inside that helper freezes
    ``taxonomy.TAXONOMY_JSON_PATH`` to the real path on first import; later
    tests in ``vireo/tests/`` then load the real 554MB taxonomy via the
    cached module, retype "Cardinal" as a species, and trigger the
    auto-Wildlife backfill — breaking ``test_remove_keyword_from_photo`` and
    ``test_undo_keyword_remove_clears_pending_change`` with a phantom
    Wildlife tag.
    """
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "vireo"))
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    import models
    from app import create_app

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))

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


def test_add_keyword_normalizes_stray_edge_quotes(db):
    """New keywords should not preserve accidental leading/trailing quote marks."""
    keyword_id = db.add_keyword("\u2018apapane'")

    row = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (keyword_id,)
    ).fetchone()
    assert row["name"] == "apapane"
    assert db.add_keyword("apapane") == keyword_id


def test_add_keyword_preserves_leading_okina(db):
    """Modifier-letter okinas (U+02BB/U+02BC) are letters inside legitimate
    species names like \u02bbApapane and must not be stripped."""
    for name in ("\u02bbApapane", "\u02bcHawai\u02bbi", "\u02bbI\u02bbiwi"):
        keyword_id = db.add_keyword(name)
        row = db.conn.execute(
            "SELECT name FROM keywords WHERE id = ?", (keyword_id,)
        ).fetchone()
        assert row["name"] == name, (
            f"okina stripped from {name!r} -> {row['name']!r}"
        )


def test_add_keyword_normalizes_acute_accent_edge_variant(db):
    """U+00B4 (ACUTE ACCENT) NFKC-decomposes to U+0020 U+0301, so unless it
    is stripped before NFKC runs, ``´apapane`` normalizes to a
    stranded leading combining acute (`́apapane`) that survives the
    post-NFKC strip and creates a nearly-invisible distinct keyword."""
    kid = db.add_keyword("´apapane")

    row = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "apapane"
    assert db.add_keyword("apapane") == kid


def test_add_keyword_normalizes_acute_accent_with_leading_whitespace(db):
    """When an imported/synced XMP keyword has whitespace before U+00B4
    (e.g. `` ´apapane`` from an XMP element with pretty-printed text),
    the pre-NFKC edge-quote strip must still see the acute. Otherwise
    NFKC decomposes it to a leading combining mark U+0301 that survives
    the later strip and produces a nearly-invisible variant."""
    kid = db.add_keyword(" ´apapane")

    row = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (kid,)
    ).fetchone()
    assert row["name"] == "apapane"
    assert db.add_keyword("apapane") == kid


def test_add_keyword_preserves_internal_acute_accent(db):
    """U+00B4 should be stripped only at the edges, not inside a keyword."""
    keyword_id = db.add_keyword("O\u00b4Brien")

    row = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (keyword_id,)
    ).fetchone()
    assert row["name"] == "O\u00b4Brien"


def test_add_keyword_preserves_private_use_area_character(db):
    """U+E000 is a valid Private Use Area code point that users may include
    in a keyword. Prior implementations reserved it as an internal sentinel
    for U+00B4 protection, which meant a lone U+E000 in the input round-tripped
    as U+00B4 (data corruption)."""
    keyword_id = db.add_keyword("a\ue000b")
    row = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (keyword_id,)
    ).fetchone()
    assert row["name"] == "a\ue000b"

    # A lone U+E000 keyword should also survive intact (no accidental
    # substitution to U+00B4 by the sentinel round-trip).
    keyword_id = db.add_keyword("\ue000")
    row = db.conn.execute(
        "SELECT name FROM keywords WHERE id = ?", (keyword_id,)
    ).fetchone()
    assert row["name"] == "\ue000"


def test_merge_keyword_rewrites_pending_changes_to_dst_name(db):
    """When keyword rename/dedupe collapses a legacy variant into the clean
    spelling, any still-unsynced ``keyword_add``/``keyword_remove`` queued
    under the source name must be rewritten to the destination name so the
    next XMP sync writes/removes the canonical text instead of leaking the
    stray-quote spelling back into the sidecar."""
    ws = db.create_workspace("A")
    fid = db.add_folder("/photos/a", name="a")
    db.add_workspace_folder(ws, fid)
    pid_a = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    pid_b = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    db.set_active_workspace(ws)

    db.conn.execute("INSERT INTO keywords (name) VALUES (?)", ("\u2018apapane",))
    db.conn.execute("INSERT INTO keywords (name) VALUES ('apapane')")
    db.conn.commit()
    quoted = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("\u2018apapane",)
    ).fetchone()[0]
    clean = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'apapane'"
    ).fetchone()[0]
    db.tag_photo(pid_a, quoted)
    db.tag_photo(pid_b, clean)

    # Queue an unsynced keyword_add under the legacy spelling. Simulates a
    # tag applied before this normalization pass landed.
    db.queue_change(pid_a, "keyword_add", "\u2018apapane")
    # Also queue a duplicate under the destination name so we exercise the
    # dedupe branch (the merge must drop the source row without producing
    # two identical pending entries).
    db.queue_change(pid_b, "keyword_add", "apapane")
    db.queue_change(pid_b, "keyword_add", "\u2018apapane")

    db.merge_duplicate_keywords()

    add_values = sorted(
        (row["photo_id"], row["value"]) for row in db.conn.execute(
            """SELECT photo_id, value FROM pending_changes
               WHERE change_type = 'keyword_add'"""
        ).fetchall()
    )
    assert add_values == [(pid_a, "apapane"), (pid_b, "apapane")]


def test_add_keyword_dedupes_pre_existing_edge_quote_variant(db):
    """When an upgraded database already carries a tagged edge-quote variant
    like '\u2018apapane', a later `add_keyword('apapane')` must reuse that row
    instead of inserting a duplicate."""
    cur = db.conn.execute(
        "INSERT INTO keywords (name) VALUES (?)", ("\u2018apapane",)
    )
    db.conn.commit()
    stored_id = cur.lastrowid

    assert db.add_keyword("apapane") == stored_id

    rows = db.conn.execute(
        "SELECT id, name FROM keywords WHERE name LIKE '%apapane'"
    ).fetchall()
    assert [(row["id"], row["name"]) for row in rows] == [
        (stored_id, "\u2018apapane"),
    ]


def test_add_keyword_dedupes_pre_existing_edge_quote_variant_with_parent(db):
    """Same as above, but scoped under a parent keyword \u2014 the fallback must
    respect the parent filter and not merge across parents."""
    birds = db.add_keyword("Birds")
    other = db.add_keyword("Other")
    cur = db.conn.execute(
        "INSERT INTO keywords (name, parent_id) VALUES (?, ?)",
        ("\u2018apapane", birds),
    )
    db.conn.commit()
    stored_id = cur.lastrowid

    assert db.add_keyword("apapane", parent_id=birds) == stored_id
    # A different parent must not reuse the row \u2014 it should insert a new one.
    other_id = db.add_keyword("apapane", parent_id=other)
    assert other_id != stored_id


def test_add_keyword_rejects_name_that_normalizes_to_empty(db):
    """Input like `'` or `\"\"` is non-empty as raw text but normalizes to '',
    which would otherwise insert an invisible keyword row."""
    for empty in ("'", '"', "\u2018", "\u201c\u201d", "'\"'"):
        try:
            db.add_keyword(empty)
        except ValueError:
            pass
        else:
            raise AssertionError(
                f"expected ValueError for name={empty!r}, none raised"
            )
    # No rows should have been inserted from the failed attempts.
    assert (
        db.conn.execute("SELECT COUNT(*) FROM keywords WHERE name = ''").fetchone()[0]
        == 0
    )


def test_merge_duplicate_keywords_normalizes_stray_edge_quotes(db):
    """Cleanup should collapse existing edge-quote variants to a clean spelling."""
    ws = db.create_workspace("A")
    fid = db.add_folder("/photos/a", name="a")
    db.add_workspace_folder(ws, fid)
    pid_a = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    pid_b = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    db.set_active_workspace(ws)

    db.conn.execute("INSERT INTO keywords (name) VALUES (?)", ("\u2018apapane",))
    db.conn.execute("INSERT INTO keywords (name) VALUES ('apapane')")
    db.conn.commit()
    quoted = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("\u2018apapane",)
    ).fetchone()[0]
    clean = db.conn.execute(
        "SELECT id FROM keywords WHERE name = 'apapane'"
    ).fetchone()[0]
    db.tag_photo(pid_a, quoted)
    db.tag_photo(pid_b, clean)

    merged = db.merge_duplicate_keywords()

    assert merged == 1
    rows = db.conn.execute(
        "SELECT id, name FROM keywords WHERE name LIKE '%apapane'"
    ).fetchall()
    assert [(row["id"], row["name"]) for row in rows] == [(clean, "apapane")]
    tagged = {
        row["keyword_id"]
        for row in db.conn.execute(
            "SELECT keyword_id FROM photo_keywords WHERE photo_id IN (?, ?)",
            (pid_a, pid_b),
        ).fetchall()
    }
    assert tagged == {clean}


def test_merge_duplicate_keywords_retargets_species_curation_for_survivor_rename(db):
    """When cleanup canonicalizes the surviving keyword row from a legacy
    spelling to a clean one, ``species_highlights``, ``photo_preferences``,
    and ``species_representatives`` rows keyed on the legacy string must
    follow. Otherwise a highlighted or life-list representative photo under
    the kept spelling silently drops out of the eligible/highlight queries
    (which compare ``sh.species``/``sr.species`` exact against ``k.name``)
    even though the tag itself was retained.
    """
    ws = db.create_workspace("A")
    fid = db.add_folder("/photos/a", name="a")
    db.add_workspace_folder(ws, fid)
    pid_a = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    pid_b = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    db.set_active_workspace(ws)

    # Legacy variant is the only surviving row (no clean sibling), so
    # ``_normalize_keyword_row_name`` rewrites its ``name`` in place.
    db.conn.execute("INSERT INTO keywords (name) VALUES (?)", ("‘apapane",))
    db.conn.execute("INSERT INTO keywords (name) VALUES (?)", ("'apapane",))
    db.conn.commit()
    quoted = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("‘apapane",)
    ).fetchone()[0]
    ascii_variant = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("'apapane",)
    ).fetchone()[0]
    db.tag_photo(pid_a, quoted)
    db.tag_photo(pid_b, ascii_variant)

    # Seed curation under the legacy spelling on both variants: a
    # highlight rank, a life-list preference, and a global representative
    # pick. The keep_id will end up being one of these rows and must
    # carry its curation with it when the name is canonicalized.
    db.conn.execute(
        """INSERT INTO species_highlights
              (workspace_id, species, photo_id, rank,
               created_at, updated_at)
           VALUES (?, ?, ?, 1, datetime('now'), datetime('now'))""",
        (ws, "‘apapane", pid_a),
    )
    db.conn.execute(
        """INSERT INTO photo_preferences
              (workspace_id, purpose, species, photo_id,
               created_at, updated_at)
           VALUES (?, 'representative', ?, ?, datetime('now'), datetime('now'))""",
        (ws, "‘apapane", pid_a),
    )
    db.conn.execute(
        """INSERT INTO species_representatives
              (species, photo_id, selected_order,
               created_at, updated_at)
           VALUES (?, ?, 1, datetime('now'), datetime('now'))""",
        ("‘apapane", pid_a),
    )
    db.conn.commit()

    merged = db.merge_duplicate_keywords()

    assert merged == 1
    # Surviving keyword row was canonicalized to the clean spelling.
    remaining = db.conn.execute(
        "SELECT name FROM keywords WHERE name LIKE '%apapane'"
    ).fetchall()
    assert [row["name"] for row in remaining] == ["apapane"]

    # All three curation tables must now key the row on the clean
    # spelling — no rows left under the legacy spelling, and each
    # migrated entry preserved its photo association.
    hl = db.conn.execute(
        "SELECT species, photo_id FROM species_highlights"
    ).fetchall()
    assert [(row["species"], row["photo_id"]) for row in hl] == [
        ("apapane", pid_a),
    ]
    pref = db.conn.execute(
        "SELECT species, purpose, photo_id FROM photo_preferences"
    ).fetchall()
    assert [(row["species"], row["purpose"], row["photo_id"]) for row in pref] == [
        ("apapane", "representative", pid_a),
    ]
    rep = db.conn.execute(
        "SELECT species, photo_id FROM species_representatives"
    ).fetchall()
    assert [(row["species"], row["photo_id"]) for row in rep] == [
        ("apapane", pid_a),
    ]


def test_merge_duplicate_keywords_retargets_species_curation_when_source_merges_into_clean_survivor(db):
    """When the SURVIVOR is already clean (so ``_normalize_keyword_row_name``
    no-ops) and a legacy source with its own species curation gets merged
    into it, ``_merge_keyword_into`` must retarget ``species_highlights``,
    ``photo_preferences``, and ``species_representatives`` from the source
    spelling onto the survivor's spelling. Without this the tag itself
    moves to the surviving row but the curation rows stay keyed to a
    keyword name the DB no longer has -- the eligible highlight/life-list
    queries compare ``sh.species``/``sr.species`` exact against
    ``keywords.name``, so the user's curated picks drop out of the UI
    even though the tag was retained.
    """
    ws = db.create_workspace("A")
    fid = db.add_folder("/photos/a", name="a")
    db.add_workspace_folder(ws, fid)
    pid_survivor = db.add_photo(folder_id=fid, filename="s.jpg", extension=".jpg",
                                file_size=100, file_mtime=1.0)
    pid_legacy = db.add_photo(folder_id=fid, filename="l.jpg", extension=".jpg",
                              file_size=100, file_mtime=1.0)
    db.set_active_workspace(ws)

    # Survivor is already clean; a separate legacy taxonomy row will be
    # merged into it. The pass sorts (is_dirty, id) so ``apapane`` wins as
    # the keep row while ``‘apapane`` becomes the source of the merge.
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'taxonomy', 1)",
        ("apapane",),
    )
    db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES (?, 'taxonomy', 1)",
        ("‘apapane",),
    )
    db.conn.commit()
    survivor_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("apapane",)
    ).fetchone()[0]
    legacy_id = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("‘apapane",)
    ).fetchone()[0]
    db.tag_photo(pid_survivor, survivor_id)
    db.tag_photo(pid_legacy, legacy_id)

    # Curation keyed on the legacy source spelling for the photo tagged with
    # the legacy row. After the merge the tag moves to ``survivor_id`` /
    # ``apapane`` but the highlight/life-list queries only match rows whose
    # species text equals the surviving ``keywords.name`` -- so without the
    # retarget these rows silently drop out of the UI.
    db.conn.execute(
        """INSERT INTO species_highlights
              (workspace_id, species, photo_id, rank,
               created_at, updated_at)
           VALUES (?, ?, ?, 1, datetime('now'), datetime('now'))""",
        (ws, "‘apapane", pid_legacy),
    )
    db.conn.execute(
        """INSERT INTO photo_preferences
              (workspace_id, purpose, species, photo_id,
               created_at, updated_at)
           VALUES (?, 'life_list', ?, ?, datetime('now'), datetime('now'))""",
        (ws, "‘apapane", pid_legacy),
    )
    db.conn.execute(
        """INSERT INTO species_representatives
              (species, photo_id, selected_order,
               created_at, updated_at)
           VALUES (?, ?, 1, datetime('now'), datetime('now'))""",
        ("‘apapane", pid_legacy),
    )
    db.conn.commit()

    merged = db.merge_duplicate_keywords()

    assert merged == 1
    # Survivor keeps its clean name; the legacy source row was deleted.
    kw_rows = db.conn.execute(
        "SELECT id, name FROM keywords WHERE name LIKE '%apapane' ORDER BY id"
    ).fetchall()
    assert [(row["id"], row["name"]) for row in kw_rows] == [
        (survivor_id, "apapane"),
    ]
    # The legacy photo's species tag moved onto the surviving id. The photo
    # also carries an auto-added Wildlife genre from tag_photo's
    # auto-Wildlife trigger (only-species-on-photo path); check membership
    # rather than equality so that unrelated tag isn't hard-coded.
    tag_ids = {
        row["keyword_id"]
        for row in db.conn.execute(
            "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?",
            (pid_legacy,),
        ).fetchall()
    }
    assert survivor_id in tag_ids
    assert legacy_id not in tag_ids

    # All three curation tables are now keyed on the surviving clean
    # spelling. Nothing left under the legacy source spelling.
    hl = db.conn.execute(
        "SELECT species, photo_id FROM species_highlights"
    ).fetchall()
    assert [(row["species"], row["photo_id"]) for row in hl] == [
        ("apapane", pid_legacy),
    ]
    pref = db.conn.execute(
        "SELECT species, purpose, photo_id FROM photo_preferences"
    ).fetchall()
    assert [(row["species"], row["purpose"], row["photo_id"]) for row in pref] == [
        ("apapane", "life_list", pid_legacy),
    ]
    rep = db.conn.execute(
        "SELECT species, photo_id FROM species_representatives"
    ).fetchall()
    assert [(row["species"], row["photo_id"]) for row in rep] == [
        ("apapane", pid_legacy),
    ]


def test_merge_duplicate_keywords_scopes_curation_to_tagged_pairs(db):
    """When one workspace runs keyword cleanup, curation rows keyed on the
    legacy spelling in a DIFFERENT workspace whose photos are not tagged with
    the merged/canonicalized keyword must NOT be rewritten. A separate legacy
    keyword row can carry the same species string across workspaces, and a
    global rename by species text would silently retarget the second
    workspace's highlights/preferences onto a canonical name it has no tag
    for — the eligible highlight/preference queries JOIN back to
    ``keywords.name`` exactly, so those rows would then drop out of the UI
    even though the tag itself is still present.
    """
    # Workspace A owns folder A with the duplicate keyword rows to clean up.
    ws_a = db.create_workspace("A")
    fid_a = db.add_folder("/photos/a", name="a")
    db.add_workspace_folder(ws_a, fid_a)
    pid_a = db.add_photo(folder_id=fid_a, filename="a.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    # Workspace B owns a separate folder and has its own legacy keyword row
    # that is NOT part of workspace A's cleanup scope.
    ws_b = db.create_workspace("B")
    fid_b = db.add_folder("/photos/b", name="b")
    db.add_workspace_folder(ws_b, fid_b)
    pid_b = db.add_photo(folder_id=fid_b, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)

    # Two normalized-equal legacy rows in workspace A -- cleanup will merge
    # them and canonicalize the survivor's spelling.
    db.set_active_workspace(ws_a)
    db.conn.execute("INSERT INTO keywords (name) VALUES (?)", ("‘apapane",))
    db.conn.execute("INSERT INTO keywords (name) VALUES (?)", ("'apapane",))
    db.conn.commit()
    a_quoted = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("‘apapane",)
    ).fetchone()[0]
    a_ascii = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ?", ("'apapane",)
    ).fetchone()[0]
    db.tag_photo(pid_a, a_quoted)
    db.tag_photo(pid_a, a_ascii)

    # A SEPARATE legacy keyword row that is only tagged in workspace B. Cleanup
    # in A must not touch it -- keywords are global but B's photo is not
    # tagged with either of A's duplicate rows, so curation and pending
    # changes referencing this independent row must survive untouched.
    db.conn.execute("INSERT INTO keywords (name) VALUES (?)", ("‘apapane",))
    db.conn.commit()
    b_quoted = db.conn.execute(
        "SELECT id FROM keywords WHERE name = ? ORDER BY id DESC LIMIT 1",
        ("‘apapane",),
    ).fetchone()[0]
    assert b_quoted not in (a_quoted, a_ascii)
    db.tag_photo(pid_b, b_quoted)

    # Seed curation in BOTH workspaces under the legacy spelling and a
    # pending keyword_add in each workspace.
    db.conn.execute(
        """INSERT INTO species_highlights
              (workspace_id, species, photo_id, rank,
               created_at, updated_at)
           VALUES (?, ?, ?, 1, datetime('now'), datetime('now'))""",
        (ws_a, "‘apapane", pid_a),
    )
    db.conn.execute(
        """INSERT INTO species_highlights
              (workspace_id, species, photo_id, rank,
               created_at, updated_at)
           VALUES (?, ?, ?, 1, datetime('now'), datetime('now'))""",
        (ws_b, "‘apapane", pid_b),
    )
    db.conn.execute(
        """INSERT INTO photo_preferences
              (workspace_id, purpose, species, photo_id,
               created_at, updated_at)
           VALUES (?, 'life_list', ?, ?, datetime('now'), datetime('now'))""",
        (ws_b, "‘apapane", pid_b),
    )
    db.conn.commit()
    db.queue_change(pid_a, "keyword_add", "‘apapane")

    # Switch to workspace B briefly to queue a pending change scoped to it,
    # then hop back to A for the cleanup.
    db.set_active_workspace(ws_b)
    db.queue_change(pid_b, "keyword_add", "‘apapane")
    db.set_active_workspace(ws_a)

    db.merge_duplicate_keywords()

    # Workspace A's curation and pending change should be canonicalized.
    a_hl = db.conn.execute(
        "SELECT species FROM species_highlights WHERE workspace_id = ?",
        (ws_a,),
    ).fetchone()
    assert a_hl["species"] == "apapane"
    a_pending = db.conn.execute(
        """SELECT value FROM pending_changes
           WHERE photo_id = ? AND change_type = 'keyword_add'""",
        (pid_a,),
    ).fetchone()
    assert a_pending["value"] == "apapane"

    # Workspace B's curation and pending change must NOT be rewritten -- its
    # keyword row is a separate row not touched by A's cleanup, and the
    # eligible-highlight/preferences queries join by species text to that
    # row's stored name.
    b_hl = db.conn.execute(
        "SELECT species FROM species_highlights WHERE workspace_id = ?",
        (ws_b,),
    ).fetchone()
    assert b_hl["species"] == "‘apapane"
    b_pref = db.conn.execute(
        "SELECT species FROM photo_preferences WHERE workspace_id = ?",
        (ws_b,),
    ).fetchone()
    assert b_pref["species"] == "‘apapane"
    b_pending = db.conn.execute(
        """SELECT value FROM pending_changes
           WHERE photo_id = ? AND change_type = 'keyword_add'""",
        (pid_b,),
    ).fetchone()
    assert b_pending["value"] == "‘apapane"


def test_merge_duplicate_keywords_does_not_fold_distinct_non_ascii(db):
    """``keyword_match_key`` must use ``str.lower()``, not ``str.casefold()``.

    ``str.casefold()`` folds ``ß`` to ``ss``, so ``"Maße".casefold() ==
    "Masse".casefold() == "masse"``. If cleanup grouped on that key, it
    would silently retag and delete one of two distinct German keywords
    even though ``add_keyword()`` and the table constraints treat them as
    distinct — a data-loss regression. Using ``str.lower()`` (which
    leaves ``ß`` alone, matching SQLite's ASCII ``COLLATE NOCASE``) keeps
    them as separate keywords.
    """
    ws = db.create_workspace("A")
    fid = db.add_folder("/photos/a", name="a")
    db.add_workspace_folder(ws, fid)
    pid_a = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    pid_b = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    db.set_active_workspace(ws)

    masse_id = db.add_keyword("Masse")
    masze_id = db.add_keyword("Maße")
    assert masse_id != masze_id
    db.tag_photo(pid_a, masse_id)
    db.tag_photo(pid_b, masze_id)

    merged = db.merge_duplicate_keywords()

    assert merged == 0
    remaining = {
        row["id"]: row["name"] for row in db.conn.execute(
            "SELECT id, name FROM keywords WHERE id IN (?, ?)",
            (masse_id, masze_id),
        ).fetchall()
    }
    assert remaining == {masse_id: "Masse", masze_id: "Maße"}
    tagged = {
        (row["photo_id"], row["keyword_id"])
        for row in db.conn.execute(
            "SELECT photo_id, keyword_id FROM photo_keywords "
            "WHERE photo_id IN (?, ?)",
            (pid_a, pid_b),
        ).fetchall()
    }
    assert tagged == {(pid_a, masse_id), (pid_b, masze_id)}


def test_merge_duplicate_keywords_does_not_fold_non_ascii_case_pairs(db):
    """``keyword_match_key`` must ignore non-ASCII case pairs.

    ``"Éclair".lower() == "éclair"`` in Python, but SQLite's
    ``LOWER()``/``COLLATE NOCASE`` used by ``add_keyword()`` is
    ASCII-only and treats them as distinct. If cleanup grouped on
    ``str.lower()``, the merge path would silently retag and delete one
    of two distinct keywords that ``add_keyword()`` kept separate.
    """
    ws = db.create_workspace("A")
    fid = db.add_folder("/photos/a", name="a")
    db.add_workspace_folder(ws, fid)
    pid_a = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    pid_b = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                         file_size=100, file_mtime=1.0)
    db.set_active_workspace(ws)

    upper_id = db.add_keyword("Éclair")
    lower_id = db.add_keyword("éclair")
    assert upper_id != lower_id
    db.tag_photo(pid_a, upper_id)
    db.tag_photo(pid_b, lower_id)

    merged = db.merge_duplicate_keywords()

    assert merged == 0
    remaining = {
        row["id"]: row["name"] for row in db.conn.execute(
            "SELECT id, name FROM keywords WHERE id IN (?, ?)",
            (upper_id, lower_id),
        ).fetchall()
    }
    assert remaining == {upper_id: "Éclair", lower_id: "éclair"}
    tagged = {
        (row["photo_id"], row["keyword_id"])
        for row in db.conn.execute(
            "SELECT photo_id, keyword_id FROM photo_keywords "
            "WHERE photo_id IN (?, ?)",
            (pid_a, pid_b),
        ).fetchall()
    }
    assert tagged == {(pid_a, upper_id), (pid_b, lower_id)}


def test_merge_duplicate_keywords_respects_parent_and_type(db):
    """Same-name keywords under different parents (Springfield, IL vs MO) or
    with different types are distinct by design and must NOT merge; only
    case-variants in the same (parent, type) slot are duplicates."""
    ws = db.create_workspace("A")
    fid = db.add_folder("/photos", name="photos")
    db.add_workspace_folder(ws, fid)
    pid = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                       file_size=100, file_mtime=1.0)
    db.set_active_workspace(ws)

    db.conn.execute("INSERT INTO keywords (name, type) VALUES ('Illinois', 'location')")
    db.conn.execute("INSERT INTO keywords (name, type) VALUES ('Missouri', 'location')")
    il = db.conn.execute("SELECT id FROM keywords WHERE name='Illinois'").fetchone()[0]
    mo = db.conn.execute("SELECT id FROM keywords WHERE name='Missouri'").fetchone()[0]
    db.conn.execute(
        "INSERT INTO keywords (name, parent_id, type) VALUES ('Springfield', ?, 'location')", (il,))
    db.conn.execute(
        "INSERT INTO keywords (name, parent_id, type) VALUES ('Springfield', ?, 'location')", (mo,))
    # Same name, same NULL parent, different type — also distinct
    db.conn.execute("INSERT INTO keywords (name, type) VALUES ('Macro', 'genre')")
    db.conn.execute("INSERT INTO keywords (name, type) VALUES ('macro', 'general')")
    db.conn.commit()

    for row in db.conn.execute(
        "SELECT id FROM keywords WHERE name IN ('Springfield', 'Macro', 'macro')"
    ).fetchall():
        db.tag_photo(pid, row[0])

    merged = db.merge_duplicate_keywords()
    assert merged == 0

    assert db.conn.execute(
        "SELECT COUNT(*) FROM keywords WHERE name = 'Springfield'"
    ).fetchone()[0] == 2
    assert db.conn.execute(
        "SELECT COUNT(*) FROM keywords WHERE LOWER(name) = 'macro'"
    ).fetchone()[0] == 2


def test_merge_duplicate_keywords_reparents_children(db):
    """A duplicate with child keywords merges without tripping the
    keywords.parent_id FK; children move to the survivor, and a follow-up
    pass collapses children that became same-parent case-duplicates.
    Only the leaves are photo-tagged — XMP import never tags ancestors —
    so the untagged Birds/birds parents must still be found via the
    descendant walk."""
    ws = db.create_workspace("A")
    fid = db.add_folder("/photos", name="photos")
    db.add_workspace_folder(ws, fid)
    pid = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                       file_size=100, file_mtime=1.0)
    db.set_active_workspace(ws)

    # Case-duplicate parents, each with a case-duplicate child chain
    db.conn.execute("INSERT INTO keywords (name) VALUES ('Birds')")
    db.conn.execute("INSERT INTO keywords (name) VALUES ('birds')")
    upper = db.conn.execute("SELECT id FROM keywords WHERE name='Birds'").fetchone()[0]
    lower = db.conn.execute("SELECT id FROM keywords WHERE name='birds'").fetchone()[0]
    db.conn.execute("INSERT INTO keywords (name, parent_id) VALUES ('Heron', ?)", (upper,))
    db.conn.execute("INSERT INTO keywords (name, parent_id) VALUES ('heron', ?)", (lower,))
    db.conn.commit()

    # Tag only the leaves, mirroring _import_keywords_for_photo
    for row in db.conn.execute(
        "SELECT id FROM keywords WHERE LOWER(name) = 'heron'"
    ).fetchall():
        db.tag_photo(pid, row[0])

    merged = db.merge_duplicate_keywords()
    assert merged == 2  # Birds/birds, then Heron/heron once same-parent

    birds = db.conn.execute(
        "SELECT id FROM keywords WHERE LOWER(name) = 'birds'"
    ).fetchall()
    herons = db.conn.execute(
        "SELECT id, parent_id FROM keywords WHERE LOWER(name) = 'heron'"
    ).fetchall()
    assert len(birds) == 1 and len(herons) == 1
    assert herons[0]["parent_id"] == birds[0]["id"]
    # Photo keeps its leaf association, now on the surviving heron
    tagged = {r[0] for r in db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (pid,)
    ).fetchall()}
    assert tagged == {herons[0]["id"]}


def test_merge_duplicate_keywords_merges_exact_name_children(db):
    """When duplicate parents both have a child with the exact same name,
    the UNIQUE(name, parent_id) clash on reparenting means the child is a
    duplicate of the survivor's sibling — it must merge into it (keeping
    its photo associations), not get renamed to 'Heron (id-N)'."""
    ws = db.create_workspace("A")
    fid = db.add_folder("/photos", name="photos")
    db.add_workspace_folder(ws, fid)
    pid1 = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                        file_size=100, file_mtime=1.0)
    pid2 = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                        file_size=100, file_mtime=1.0)
    db.set_active_workspace(ws)

    # Case-duplicate parents, each with an exact-same-name child
    db.conn.execute("INSERT INTO keywords (name) VALUES ('Birds')")
    db.conn.execute("INSERT INTO keywords (name) VALUES ('birds')")
    upper = db.conn.execute("SELECT id FROM keywords WHERE name='Birds'").fetchone()[0]
    lower = db.conn.execute("SELECT id FROM keywords WHERE name='birds'").fetchone()[0]
    db.conn.execute("INSERT INTO keywords (name, parent_id) VALUES ('Heron', ?)", (upper,))
    db.conn.execute("INSERT INTO keywords (name, parent_id) VALUES ('Heron', ?)", (lower,))
    h1 = db.conn.execute(
        "SELECT id FROM keywords WHERE name='Heron' AND parent_id=?", (upper,)).fetchone()[0]
    h2 = db.conn.execute(
        "SELECT id FROM keywords WHERE name='Heron' AND parent_id=?", (lower,)).fetchone()[0]
    db.conn.commit()

    db.tag_photo(pid1, h1)
    db.tag_photo(pid2, h2)

    merged = db.merge_duplicate_keywords()
    assert merged == 2  # birds into Birds, then its Heron into Birds' Heron

    herons = db.conn.execute(
        "SELECT id, name, parent_id FROM keywords WHERE name LIKE 'Heron%'"
    ).fetchall()
    assert len(herons) == 1
    assert herons[0]["name"] == "Heron"  # no 'Heron (id-N)' mangling
    assert herons[0]["parent_id"] == upper
    # Both photos' associations converge on the surviving Heron
    for pid in (pid1, pid2):
        tagged = {r[0] for r in db.conn.execute(
            "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (pid,)
        ).fetchall()}
        assert tagged == {herons[0]["id"]}


def test_merge_duplicate_keywords_handles_stale_group_after_parent_merge(db):
    """A pass that contains both duplicate parents AND duplicate children
    under those parents must not crash when the parent merge recursively
    deletes one of the child group's ids. Without checking that each
    group's keep_id still exists, the loop would UPDATE photo_keywords
    toward a non-existent FK target and trip an IntegrityError."""
    ws = db.create_workspace("A")
    fid = db.add_folder("/photos", name="photos")
    db.add_workspace_folder(ws, fid)
    pid = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                       file_size=100, file_mtime=1.0)
    db.set_active_workspace(ws)

    # Parents: Birds (keep) and birds (dup). Birds already has a Heron
    # child; birds has both 'Heron' and 'heron' children — these form
    # their own duplicate group under birds, and the parent merge will
    # collapse 'Heron@birds' into 'Heron@Birds' before that group is
    # processed.
    db.conn.execute("INSERT INTO keywords (name) VALUES ('Birds')")
    db.conn.execute("INSERT INTO keywords (name) VALUES ('birds')")
    upper = db.conn.execute("SELECT id FROM keywords WHERE name='Birds'").fetchone()[0]
    lower = db.conn.execute("SELECT id FROM keywords WHERE name='birds'").fetchone()[0]
    db.conn.execute("INSERT INTO keywords (name, parent_id) VALUES ('Heron', ?)", (upper,))
    db.conn.execute("INSERT INTO keywords (name, parent_id) VALUES ('Heron', ?)", (lower,))
    db.conn.execute("INSERT INTO keywords (name, parent_id) VALUES ('heron', ?)", (lower,))
    db.conn.commit()

    for row in db.conn.execute(
        "SELECT id FROM keywords WHERE LOWER(name) = 'heron'"
    ).fetchall():
        db.tag_photo(pid, row[0])

    # Species/location metadata carried only by the duplicate must fold
    # into the survivor instead of being deleted with it. (Set after
    # tagging so the auto-Wildlife rule doesn't muddy the tag assertions.)
    db.conn.execute(
        "UPDATE keywords SET is_species = 1, latitude = -33.9, longitude = 18.4 "
        "WHERE name = 'heron' AND parent_id = ?", (lower,))
    db.conn.commit()

    # Must not raise; converges to one Birds with one Heron child.
    db.merge_duplicate_keywords()

    birds = db.conn.execute(
        "SELECT id FROM keywords WHERE LOWER(name) = 'birds'"
    ).fetchall()
    herons = db.conn.execute(
        "SELECT id, parent_id, is_species, latitude, longitude "
        "FROM keywords WHERE LOWER(name) = 'heron'"
    ).fetchall()
    assert len(birds) == 1
    assert len(herons) == 1
    assert herons[0]["parent_id"] == birds[0]["id"]
    assert herons[0]["is_species"] == 1
    assert herons[0]["latitude"] == -33.9
    assert herons[0]["longitude"] == 18.4
    tagged = {r[0] for r in db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (pid,)
    ).fetchall()}
    assert tagged == {herons[0]["id"]}


def test_merge_duplicate_keywords_preserves_differently_typed_children(db):
    """A child name-collision on reparent is only a duplicate when both
    children share a type. With 'Birds > Macro' (general) under one parent
    and 'birds > Macro' (genre) under its case-duplicate, the parent merge
    triggers a UNIQUE(name, parent_id) clash on the migrating Macro. The
    dedup boundary is (LOWER(name), parent_id, type), so these aren't
    duplicates; the migrating one must be preserved (disambiguated by
    name), not silently merged into a different-type sibling — that would
    retag photos across the type boundary and drop one typed keyword."""
    ws = db.create_workspace("A")
    fid = db.add_folder("/photos", name="photos")
    db.add_workspace_folder(ws, fid)
    pid_general = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                               file_size=100, file_mtime=1.0)
    pid_genre = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                             file_size=100, file_mtime=1.0)
    db.set_active_workspace(ws)

    db.conn.execute("INSERT INTO keywords (name) VALUES ('Birds')")
    db.conn.execute("INSERT INTO keywords (name) VALUES ('birds')")
    upper = db.conn.execute("SELECT id FROM keywords WHERE name='Birds'").fetchone()[0]
    lower = db.conn.execute("SELECT id FROM keywords WHERE name='birds'").fetchone()[0]
    db.conn.execute(
        "INSERT INTO keywords (name, parent_id, type) VALUES ('Macro', ?, 'general')",
        (upper,),
    )
    db.conn.execute(
        "INSERT INTO keywords (name, parent_id, type) VALUES ('Macro', ?, 'genre')",
        (lower,),
    )
    m_general = db.conn.execute(
        "SELECT id FROM keywords WHERE name='Macro' AND parent_id=?", (upper,)
    ).fetchone()[0]
    m_genre = db.conn.execute(
        "SELECT id FROM keywords WHERE name='Macro' AND parent_id=?", (lower,)
    ).fetchone()[0]
    db.conn.commit()

    db.tag_photo(pid_general, m_general)
    db.tag_photo(pid_genre, m_genre)

    db.merge_duplicate_keywords()

    # Birds/birds collapsed to one parent.
    birds = db.conn.execute(
        "SELECT id FROM keywords WHERE LOWER(name) = 'birds'"
    ).fetchall()
    assert len(birds) == 1
    assert birds[0]["id"] == upper

    # Both typed Macros survive under the survivor parent; the migrating
    # one was disambiguated rather than merged across the type boundary.
    macros = db.conn.execute(
        "SELECT id, name, parent_id, type FROM keywords WHERE LOWER(name) LIKE 'macro%' "
        "ORDER BY type"
    ).fetchall()
    assert len(macros) == 2
    assert {m["parent_id"] for m in macros} == {upper}
    assert {m["type"] for m in macros} == {"general", "genre"}
    # The original general Macro keeps its name; the migrating genre Macro
    # was disambiguated with an id suffix.
    by_type = {m["type"]: m for m in macros}
    assert by_type["general"]["id"] == m_general
    assert by_type["general"]["name"] == "Macro"
    assert by_type["genre"]["id"] == m_genre
    assert by_type["genre"]["name"] == f"Macro (id-{m_genre})"

    # Each photo's association sticks to its own type — neither got
    # silently retagged onto the other-type Macro.
    general_tags = {r[0] for r in db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (pid_general,)
    ).fetchall()}
    genre_tags = {r[0] for r in db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (pid_genre,)
    ).fetchall()}
    assert general_tags == {m_general}
    assert genre_tags == {m_genre}


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


# -- Move folders between workspaces --


def test_move_folders_moves_pending_changes(db_with_workspace):
    """move_folders_to_workspace moves folders and pending_changes.

    Detections are now global (no workspace_id), so predictions follow the
    folder via workspace_folders membership rather than being reassigned.
    """
    db, ws1, folder_id, photo_id = db_with_workspace
    det_ids = db.save_detections(photo_id, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9, "category": "animal"}
    ], detector_model="MDV6")
    db.add_prediction(det_ids[0], "Robin", 0.95, "bioclip")
    db.queue_change(photo_id, "keyword_add", "Robin")

    ws2 = db.create_workspace("Target")

    result = db.move_folders_to_workspace(ws1, ws2, [folder_id])
    assert result["folders_moved"] == 1
    assert result["pending_changes_moved"] == 1

    # Folder moved: ws2 has it, ws1 does not
    assert len(db.get_workspace_folders(ws2)) == 1
    assert len(db.get_workspace_folders(ws1)) == 0

    # Predictions follow the folder's workspace membership (detections are global).
    db.set_active_workspace(ws2)
    preds = db.get_predictions()
    assert len(preds) == 1
    assert preds[0]["species"] == "Robin"

    db.set_active_workspace(ws1)
    assert len(db.get_predictions()) == 0

    # Pending changes moved to ws2
    db.set_active_workspace(ws2)
    assert len(db.get_pending_changes()) == 1
    db.set_active_workspace(ws1)
    assert len(db.get_pending_changes()) == 0


def test_move_folders_moves_photo_preferences(db_with_workspace):
    """Representative choices and ordered highlights follow moved photos."""
    db, ws1, folder_id, photo_id = db_with_workspace
    db.set_photo_preference("life_list", "Robin", photo_id)
    db.set_photo_preference("highlights", "Robin", photo_id)
    db.add_species_highlight("Robin", photo_id)

    ws2 = db.create_workspace("Target")
    result = db.move_folders_to_workspace(ws1, ws2, [folder_id])

    assert result["photo_preferences_moved"] == 2
    assert result["species_highlights_moved"] == 1
    db.set_active_workspace(ws2)
    assert db.get_photo_preferences("life_list") == {"Robin": photo_id}
    assert db.get_photo_preferences("highlights") == {"Robin": photo_id}
    assert db.get_species_highlights() == {"Robin": {photo_id: 1}}

    db.set_active_workspace(ws1)
    assert db.get_photo_preferences("life_list") == {}
    assert db.get_photo_preferences("highlights") == {}
    assert db.get_species_highlights() == {}


def test_move_folders_reranks_species_highlights_on_collision(db):
    """Moving highlights into a target that already has ranks for the same
    species must append after the target's max rank so `ORDER BY rank`
    keeps the target's curated order first."""
    ws1 = db.create_workspace("Source")
    ws2 = db.create_workspace("Target")

    src_folder = db.add_folder("/src", name="src")
    tgt_folder = db.add_folder("/tgt", name="tgt")
    db.add_workspace_folder(ws1, src_folder)
    db.add_workspace_folder(ws2, tgt_folder)

    src_photo = db.add_photo(folder_id=src_folder, filename="src.jpg",
                             extension=".jpg", file_size=100, file_mtime=1.0)
    tgt_photo = db.add_photo(folder_id=tgt_folder, filename="tgt.jpg",
                             extension=".jpg", file_size=100, file_mtime=2.0)

    db.set_active_workspace(ws1)
    db.add_species_highlight("Robin", src_photo)
    db.set_active_workspace(ws2)
    db.add_species_highlight("Robin", tgt_photo)

    result = db.move_folders_to_workspace(ws1, ws2, [src_folder])
    assert result["species_highlights_moved"] == 1

    db.set_active_workspace(ws2)
    # Both photos are in the Robin bucket, with distinct ranks; the target's
    # original rank-1 stays first because moved rows append after it.
    assert db.get_species_highlights("Robin") == {
        "Robin": {tgt_photo: 1, src_photo: 2},
    }


def test_move_folders_collections_stay_behind(db_with_workspace):
    """Collections in the source workspace are NOT moved."""
    db, ws1, folder_id, photo_id = db_with_workspace
    db.add_collection("Flagged", "[]")

    ws2 = db.create_workspace("Target")
    db.move_folders_to_workspace(ws1, ws2, [folder_id])

    db.set_active_workspace(ws1)
    assert len(db.get_collections()) == 1
    db.set_active_workspace(ws2)
    assert len(db.get_collections()) == 0


def test_move_folders_blocks_descendant_when_source_root_remains(db):
    ws1 = db.create_workspace("Source")
    ws2 = db.create_workspace("Target")
    root_id = db.add_folder("/photos/usa/2026", name="2026")
    child_id = db.add_folder(
        "/photos/usa/2026/day-1",
        name="day-1",
        parent_id=root_id,
    )
    db.add_workspace_folder(ws1, root_id)

    with pytest.raises(ValueError, match="covered by another source workspace folder"):
        db.move_folders_to_workspace(ws1, ws2, [child_id])

    assert {f["id"] for f in db.get_workspace_folders(ws1)} == {root_id, child_id}
    assert db.get_workspace_folders(ws2) == []


def test_archive_merge_preserves_narrower_workspace_root(db):
    """Merging into a broad archive must not widen an already-scoped workspace."""
    ws_id = db.create_workspace("USA2026")

    # Existing archive rows are globally known from another workspace/history,
    # but this workspace is intentionally rooted only at the 2026 subtree.
    db.set_active_workspace(None)
    archive_id = db.add_folder("/archive/USA", name="USA")
    year_id = db.add_folder("/archive/USA/2026", name="2026")
    old_year_id = db.add_folder(
        "/archive/USA/2020",
        name="2020",
        parent_id=archive_id,
    )
    db.add_photo(old_year_id, "old.jpg", ".jpg", 1000, 1.0)

    db.set_active_workspace(ws_id)
    db.add_workspace_folder(ws_id, year_id)
    db.conn.executemany(
        """INSERT INTO workspace_folders (workspace_id, folder_id, is_root)
           VALUES (?, ?, 0)""",
        [(ws_id, archive_id), (ws_id, old_year_id)],
    )
    db.conn.commit()

    # Staging mirrors local-processing imports: the broad staging folder is
    # just the archive shell; the touched dated leaf is the user-facing import
    # root while it is still in staging.
    staged_archive_id = db.add_folder(
        "/staging/job/USA",
        name="USA",
        workspace_root=False,
    )
    staged_year_id = db.add_folder(
        "/staging/job/USA/2026",
        name="2026",
        parent_id=staged_archive_id,
        workspace_root=False,
    )
    staged_leaf_id = db.add_folder(
        "/staging/job/USA/2026/2026-07-02",
        name="2026-07-02",
        parent_id=staged_year_id,
        workspace_root=True,
    )
    db.add_photo(staged_leaf_id, "new.jpg", ".jpg", 1000, 2.0)
    staged_other_year_id = db.add_folder(
        "/staging/job/USA/2027",
        name="2027",
        parent_id=staged_archive_id,
        workspace_root=False,
    )
    staged_other_leaf_id = db.add_folder(
        "/staging/job/USA/2027/2027-01-01",
        name="2027-01-01",
        parent_id=staged_other_year_id,
        workspace_root=True,
    )
    db.add_photo(staged_other_leaf_id, "future.jpg", ".jpg", 1000, 3.0)
    db._new_images_cache.set(
        db._db_path,
        ws_id,
        {"new_count": 99, "files": []},
    )

    result = db.merge_staged_tree_into_archive(staged_archive_id, "/archive/USA")

    assert result["new_photos"] == 2
    assert db._new_images_cache.get(db._db_path, ws_id) is None
    root_paths = {
        r["path"] for r in db.get_workspace_folder_roots(ws_id)
    }
    assert "/archive/USA/2026" in root_paths
    assert "/archive/USA" not in root_paths
    assert "/archive/USA/2027/2027-01-01" not in root_paths

    archive_link = db.conn.execute(
        """SELECT is_root FROM workspace_folders
           WHERE workspace_id = ? AND folder_id = ?""",
        (ws_id, archive_id),
    ).fetchone()
    assert archive_link is None

    old_year_link = db.conn.execute(
        """SELECT 1 FROM workspace_folders
           WHERE workspace_id = ? AND folder_id = ?""",
        (ws_id, old_year_id),
    ).fetchone()
    assert old_year_link is None

    new_photo = db.conn.execute(
        """SELECT p.filename
           FROM photos p
           JOIN folders f ON f.id = p.folder_id
           JOIN workspace_folders wf ON wf.folder_id = f.id
          WHERE wf.workspace_id = ? AND f.path = ?""",
        (ws_id, "/archive/USA/2026/2026-07-02"),
    ).fetchone()
    assert new_photo["filename"] == "new.jpg"

    old_photo = db.conn.execute(
        """SELECT p.filename
           FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
          WHERE wf.workspace_id = ? AND p.filename = ?""",
        (ws_id, "old.jpg"),
    ).fetchone()
    assert old_photo is None

    future_photo = db.conn.execute(
        """SELECT p.filename
           FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
          WHERE wf.workspace_id = ? AND p.filename = ?""",
        (ws_id, "future.jpg"),
    ).fetchone()
    assert future_photo is None


def test_archive_merge_scoped_root_invalidates_new_images_cache(db):
    """Scoped-root merges must flush a stale new-images cache after commit.

    The scoped-root branch skips ``add_workspace_folder`` for the broad
    archive base; ``_prune_...`` and ``_materialize_...`` only invalidate
    when they actually change rows. In the shape below both are no-ops, so
    the only workspace-membership writes come from the reconciliation
    loop's ``_add_workspace_folder_no_commit`` — which does not invalidate.
    Without the post-commit invalidation a cache entry set between the
    rsync copy and reconciliation would keep reporting the just-imported
    files as "new" until the TTL expired.
    """
    ws_id = db.create_workspace("USA2026")

    db.set_active_workspace(None)
    archive_id = db.add_folder("/archive/USA", name="USA")
    year_id = db.add_folder("/archive/USA/2026", name="2026")

    db.set_active_workspace(ws_id)
    db.add_workspace_folder(ws_id, year_id)

    staged_archive_id = db.add_folder(
        "/staging/job/USA",
        name="USA",
        workspace_root=False,
    )
    staged_year_id = db.add_folder(
        "/staging/job/USA/2026",
        name="2026",
        parent_id=staged_archive_id,
        workspace_root=False,
    )
    staged_leaf_id = db.add_folder(
        "/staging/job/USA/2026/2026-07-02",
        name="2026-07-02",
        parent_id=staged_year_id,
        workspace_root=True,
    )
    db.add_photo(staged_leaf_id, "new.jpg", ".jpg", 1000, 2.0)

    db._new_images_cache.set(
        db._db_path, ws_id, {"new_count": 0, "per_root": [], "sample": []}
    )

    db.merge_staged_tree_into_archive(staged_archive_id, "/archive/USA")

    assert db._new_images_cache.get(db._db_path, ws_id) is None


def test_archive_merge_new_path_preserves_narrower_workspace_root(db):
    """Merging to a new archive path must not root a broader known ancestor."""
    ws_id = db.create_workspace("USA2026")

    db.set_active_workspace(None)
    archive_id = db.add_folder("/archive/USA", name="USA")
    year_id = db.add_folder(
        "/archive/USA/2026",
        name="2026",
        parent_id=archive_id,
    )
    db.add_folder(
        "/archive/USA/2020",
        name="2020",
        parent_id=archive_id,
    )

    db.set_active_workspace(ws_id)
    db.add_workspace_folder(ws_id, year_id)

    staged_year_id = db.add_folder(
        "/staging/job/2027",
        name="2027",
        workspace_root=False,
    )
    staged_leaf_id = db.add_folder(
        "/staging/job/2027/2027-01-01",
        name="2027-01-01",
        parent_id=staged_year_id,
        workspace_root=True,
    )
    db.add_photo(staged_leaf_id, "future.jpg", ".jpg", 1000, 3.0)

    result = db.merge_staged_tree_into_archive(
        staged_year_id,
        "/archive/USA/2027",
    )

    assert result["new_photos"] == 1
    root_paths = {
        r["path"] for r in db.get_workspace_folder_roots(ws_id)
    }
    assert root_paths == {"/archive/USA/2026"}

    archive_link = db.conn.execute(
        """SELECT 1 FROM workspace_folders
           WHERE workspace_id = ? AND folder_id = ?""",
        (ws_id, archive_id),
    ).fetchone()
    assert archive_link is None

    future_photo = db.conn.execute(
        """SELECT p.filename
           FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
          WHERE wf.workspace_id = ? AND p.filename = ?""",
        (ws_id, "future.jpg"),
    ).fetchone()
    assert future_photo is None


def test_archive_merge_nested_new_path_does_not_link_out_of_scope_intermediate(db):
    """A missing intermediate under a broad archive must not leak siblings.

    Workspace root ``/archive/USA/2026`` is scoped narrower than the archive
    base ``/archive/USA``. Merging into a nested new path ``/archive/USA/2027/
    Trip`` requires materializing ``/archive/USA/2027`` as a folder row for
    ``parent_id`` chaining. That intermediate must not be linked to the
    workspace: no workspace root covers it, and any non-root link would let
    ``_materialize_workspace_descendants`` (called by ``get_workspace_folders``)
    pull the merged ``/archive/USA/2027/Trip`` subtree into the workspace.
    """
    ws_id = db.create_workspace("USA2026")

    db.set_active_workspace(None)
    archive_id = db.add_folder("/archive/USA", name="USA")
    year_id = db.add_folder(
        "/archive/USA/2026",
        name="2026",
        parent_id=archive_id,
    )

    db.set_active_workspace(ws_id)
    db.add_workspace_folder(ws_id, year_id)

    staged_trip_id = db.add_folder(
        "/staging/job/Trip",
        name="Trip",
        workspace_root=True,
    )
    db.add_photo(staged_trip_id, "future.jpg", ".jpg", 1000, 3.0)

    result = db.merge_staged_tree_into_archive(
        staged_trip_id,
        "/archive/USA/2027/Trip",
    )

    assert result["new_photos"] == 1

    root_paths = {
        r["path"] for r in db.get_workspace_folder_roots(ws_id)
    }
    assert root_paths == {"/archive/USA/2026"}

    intermediate_row = db.conn.execute(
        "SELECT id FROM folders WHERE path = ?",
        ("/archive/USA/2027",),
    ).fetchone()
    assert intermediate_row is not None
    intermediate_link = db.conn.execute(
        """SELECT 1 FROM workspace_folders
           WHERE workspace_id = ? AND folder_id = ?""",
        (ws_id, intermediate_row["id"]),
    ).fetchone()
    assert intermediate_link is None

    # get_workspace_folders() runs _materialize_workspace_descendants; the
    # merged Trip subtree must remain invisible to this scoped workspace.
    linked_paths = {
        r["path"] for r in db.get_workspace_folders(ws_id)
    }
    assert "/archive/USA/2027" not in linked_paths
    assert "/archive/USA/2027/Trip" not in linked_paths

    future_photo = db.conn.execute(
        """SELECT p.filename
           FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
          WHERE wf.workspace_id = ? AND p.filename = ?""",
        (ws_id, "future.jpg"),
    ).fetchone()
    assert future_photo is None


def test_archive_merge_new_path_prunes_stale_broad_nonroot_link(db):
    """No-row merge must prune a stale broad ``is_root=0`` ancestor link.

    Workspace root ``/archive/USA/2026`` is scoped narrower than the archive
    base ``/archive/USA``. If the workspace already carries a stale
    ``is_root=0`` link on ``/archive/USA`` (e.g. from a prior broader merge
    or a legacy layout), merging into a brand-new sibling like
    ``/archive/USA/2027/Trip`` (no folder row for the archive destination
    yet) walks up to ``/archive/USA``, hits the descendant-root guard, and
    skips rooting it. Without also pruning that stale non-root link the
    next ``get_workspace_folders()`` runs
    ``_materialize_workspace_descendants`` from ``/archive/USA`` and pulls
    the freshly-inserted ``/archive/USA/2027`` (and its ``Trip`` subtree)
    into the workspace, defeating the scoped merge. The elif branch must
    prune uncovered non-root links matching the existing-row cleanup.
    """
    ws_id = db.create_workspace("USA2026")

    db.set_active_workspace(None)
    archive_id = db.add_folder("/archive/USA", name="USA")
    year_id = db.add_folder(
        "/archive/USA/2026",
        name="2026",
        parent_id=archive_id,
    )

    db.set_active_workspace(ws_id)
    db.add_workspace_folder(ws_id, year_id)
    # Seed the stale broad non-root link that this fix must clean up.
    db.add_workspace_folder(ws_id, archive_id, is_root=False)

    archive_link_before = db.conn.execute(
        """SELECT is_root FROM workspace_folders
           WHERE workspace_id = ? AND folder_id = ?""",
        (ws_id, archive_id),
    ).fetchone()
    assert archive_link_before is not None
    assert archive_link_before["is_root"] == 0

    staged_trip_id = db.add_folder(
        "/staging/job/Trip",
        name="Trip",
        workspace_root=True,
    )
    db.add_photo(staged_trip_id, "future.jpg", ".jpg", 1000, 3.0)

    result = db.merge_staged_tree_into_archive(
        staged_trip_id,
        "/archive/USA/2027/Trip",
    )

    assert result["new_photos"] == 1

    # The narrower root survives untouched.
    root_paths = {
        r["path"] for r in db.get_workspace_folder_roots(ws_id)
    }
    assert root_paths == {"/archive/USA/2026"}

    # The stale ``/archive/USA`` non-root link is gone.
    archive_link_after = db.conn.execute(
        """SELECT 1 FROM workspace_folders
           WHERE workspace_id = ? AND folder_id = ?""",
        (ws_id, archive_id),
    ).fetchone()
    assert archive_link_after is None

    # ``get_workspace_folders`` runs ``_materialize_workspace_descendants``;
    # the merged Trip subtree and the new 2027 intermediate must remain
    # invisible to this scoped workspace.
    linked_paths = {
        r["path"] for r in db.get_workspace_folders(ws_id)
    }
    assert "/archive/USA" not in linked_paths
    assert "/archive/USA/2027" not in linked_paths
    assert "/archive/USA/2027/Trip" not in linked_paths

    future_photo = db.conn.execute(
        """SELECT p.filename
           FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
          WHERE wf.workspace_id = ? AND p.filename = ?""",
        (ws_id, "future.jpg"),
    ).fetchone()
    assert future_photo is None


def test_archive_merge_prunes_stale_nonroot_ancestor_above_scoped_root(db):
    """Existing-row merge must prune non-root ancestors of the merge target.

    A restricted scan (see ``scanner.py`` ``_restrict_root_paths``) links the
    broad scan base as ``is_root=0`` above the user-facing scoped root — for
    example, ``/archive`` non-root and ``/archive/USA/2026`` as the only
    root. When a later merge into ``/archive/USA`` prunes non-root links at
    or below ``/archive/USA`` and then calls
    ``_materialize_workspace_descendants``, the surviving ``/archive``
    non-root ancestor would immediately re-insert ``/archive/USA``,
    ``/archive/USA/2020``, and any newly-merged sibling like
    ``/archive/USA/2027``, defeating the scoped merge. The prune must also
    remove uncovered non-root ancestors of the merge target.
    """
    ws_id = db.create_workspace("USA2026")

    db.set_active_workspace(None)
    archive_base_id = db.add_folder("/archive", name="archive")
    usa_id = db.add_folder(
        "/archive/USA",
        name="USA",
        parent_id=archive_base_id,
    )
    year_id = db.add_folder(
        "/archive/USA/2026",
        name="2026",
        parent_id=usa_id,
    )
    old_year_id = db.add_folder(
        "/archive/USA/2020",
        name="2020",
        parent_id=usa_id,
    )
    db.add_photo(old_year_id, "old.jpg", ".jpg", 1000, 1.0)

    db.set_active_workspace(ws_id)
    db.add_workspace_folder(ws_id, year_id)
    # Seed the stale broad non-root ancestor link. This mirrors what a
    # restricted scan leaves behind on the scan base above the scoped root.
    db.add_workspace_folder(ws_id, archive_base_id, is_root=False)

    staged_archive_id = db.add_folder(
        "/staging/job/USA",
        name="USA",
        workspace_root=False,
    )
    staged_year_id = db.add_folder(
        "/staging/job/USA/2026",
        name="2026",
        parent_id=staged_archive_id,
        workspace_root=False,
    )
    staged_leaf_id = db.add_folder(
        "/staging/job/USA/2026/2026-07-02",
        name="2026-07-02",
        parent_id=staged_year_id,
        workspace_root=True,
    )
    db.add_photo(staged_leaf_id, "new.jpg", ".jpg", 1000, 2.0)
    staged_other_year_id = db.add_folder(
        "/staging/job/USA/2027",
        name="2027",
        parent_id=staged_archive_id,
        workspace_root=False,
    )
    staged_other_leaf_id = db.add_folder(
        "/staging/job/USA/2027/2027-01-01",
        name="2027-01-01",
        parent_id=staged_other_year_id,
        workspace_root=True,
    )
    db.add_photo(staged_other_leaf_id, "future.jpg", ".jpg", 1000, 3.0)

    db.merge_staged_tree_into_archive(staged_archive_id, "/archive/USA")

    # Scoped root survives untouched.
    root_paths = {
        r["path"] for r in db.get_workspace_folder_roots(ws_id)
    }
    assert root_paths == {"/archive/USA/2026"}

    # The stale ``/archive`` non-root ancestor link is gone, so a later
    # ``_materialize_workspace_descendants`` cannot pull the whole archive
    # subtree back into the workspace.
    archive_base_link = db.conn.execute(
        """SELECT 1 FROM workspace_folders
           WHERE workspace_id = ? AND folder_id = ?""",
        (ws_id, archive_base_id),
    ).fetchone()
    assert archive_base_link is None

    # ``get_workspace_folders`` invokes ``_materialize_workspace_descendants``;
    # verify neither the broad archive base nor sibling years surface.
    linked_paths = {
        r["path"] for r in db.get_workspace_folders(ws_id)
    }
    assert "/archive" not in linked_paths
    assert "/archive/USA" not in linked_paths
    assert "/archive/USA/2020" not in linked_paths
    assert "/archive/USA/2027" not in linked_paths
    assert "/archive/USA/2027/2027-01-01" not in linked_paths

    # Out-of-scope sibling photos stay hidden.
    old_photo = db.conn.execute(
        """SELECT p.filename
           FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
          WHERE wf.workspace_id = ? AND p.filename = ?""",
        (ws_id, "old.jpg"),
    ).fetchone()
    assert old_photo is None

    future_photo = db.conn.execute(
        """SELECT p.filename
           FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
          WHERE wf.workspace_id = ? AND p.filename = ?""",
        (ws_id, "future.jpg"),
    ).fetchone()
    assert future_photo is None


def test_move_folders_validates_source_workspace(db):
    """Raises ValueError if source workspace doesn't exist."""
    ws = db.create_workspace("A")
    with pytest.raises(ValueError, match="Source workspace"):
        db.move_folders_to_workspace(9999, ws, [1])


def test_move_folders_validates_target_workspace(db):
    """Raises ValueError if target workspace doesn't exist."""
    ws = db.create_workspace("A")
    with pytest.raises(ValueError, match="Target workspace"):
        db.move_folders_to_workspace(ws, 9999, [1])


def test_move_folders_validates_same_workspace(db):
    """Raises ValueError when source == target."""
    ws = db.create_workspace("A")
    with pytest.raises(ValueError, match="same"):
        db.move_folders_to_workspace(ws, ws, [1])


def test_move_folders_validates_folder_belongs_to_source(db):
    """Raises ValueError if a folder doesn't belong to source workspace."""
    ws1 = db.create_workspace("Source")
    ws2 = db.create_workspace("Target")
    db.set_active_workspace(ws1)
    fid = db.add_folder("/photos/a", name="a")
    with pytest.raises(ValueError, match="does not belong"):
        db.move_folders_to_workspace(ws1, ws2, [fid, 9999])


def test_move_folders_empty_list(db):
    """Moving an empty list returns zeros without error."""
    ws1 = db.create_workspace("A")
    ws2 = db.create_workspace("B")
    result = db.move_folders_to_workspace(ws1, ws2, [])
    assert result["folders_moved"] == 0


def test_workspaces_has_tabs_column(db):
    cols = [r["name"] for r in db.conn.execute("PRAGMA table_info(workspaces)")]
    assert "tabs" in cols


def test_legacy_workspaces_get_default_tabs_on_migration(tmp_path):
    """A pre-existing workspaces table without `tabs` should be backfilled with DEFAULT_TABS."""
    import json as _json
    import sqlite3
    db_path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE workspaces (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL UNIQUE,
              config_overrides TEXT,
              ui_state TEXT,
              open_tabs TEXT,
              created_at TEXT DEFAULT (datetime('now')),
              last_opened_at TEXT)"""
    )
    conn.execute(
        "INSERT INTO workspaces (name, open_tabs) VALUES (?, ?)",
        ("Legacy", _json.dumps(["settings", "workspace"])),
    )
    conn.commit()
    conn.close()

    from db import DEFAULT_TABS, Database
    db = Database(db_path)
    cols = [r["name"] for r in db.conn.execute("PRAGMA table_info(workspaces)")]
    assert "tabs" in cols
    row = db.conn.execute("SELECT tabs FROM workspaces WHERE name = 'Legacy'").fetchone()
    assert _json.loads(row["tabs"]) == DEFAULT_TABS


def test_new_workspace_gets_default_tabs(db):
    import json as _json

    from db import DEFAULT_TABS
    ws_id = db.create_workspace("Fresh")
    row = db.conn.execute("SELECT tabs FROM workspaces WHERE id = ?", (ws_id,)).fetchone()
    assert row["tabs"] is not None
    assert _json.loads(row["tabs"]) == DEFAULT_TABS


def test_get_tabs_returns_default_for_new_workspace(db):
    from db import DEFAULT_TABS
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    assert db.get_tabs() == DEFAULT_TABS


def test_pin_tab_appends(db):
    from db import DEFAULT_TABS
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    result = db.pin_tab("logs")
    assert result == DEFAULT_TABS + ["logs"]
    assert db.get_tabs() == DEFAULT_TABS + ["logs"]


def test_pin_tab_idempotent(db):
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    db.pin_tab("logs")
    db.pin_tab("logs")
    assert db.get_tabs().count("logs") == 1


def test_pin_tab_rejects_unknown_id(db):
    import pytest
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    with pytest.raises(ValueError):
        db.pin_tab("not_a_real_page")


def test_all_registered_pages_are_valid_tab_ids():
    from app import ALL_PAGES
    from db import ALL_NAV_IDS

    registered_ids = {page["id"] for page in ALL_PAGES}
    assert registered_ids <= ALL_NAV_IDS


def test_unpin_tab_removes(db):
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    db.unpin_tab("settings")
    assert "settings" not in db.get_tabs()


def test_unpin_tab_idempotent_when_not_pinned(db):
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    db.unpin_tab("logs")  # not in defaults
    db.unpin_tab("logs")  # again
    assert "logs" not in db.get_tabs()


def test_set_tabs_replaces_full_list(db):
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    new_order = ["cull", "review", "browse"]
    result = db.set_tabs(new_order)
    assert result == new_order
    assert db.get_tabs() == new_order


def test_set_tabs_rejects_unknown_id(db):
    import pytest
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    with pytest.raises(ValueError):
        db.set_tabs(["browse", "not_a_real_page"])


def test_set_tabs_rejects_duplicates(db):
    import pytest
    ws_id = db.create_workspace("Fresh")
    db.set_active_workspace(ws_id)
    with pytest.raises(ValueError):
        db.set_tabs(["browse", "browse", "review"])


def test_tabs_are_per_workspace(db):
    from db import DEFAULT_TABS
    ws1 = db.create_workspace("WS1")
    ws2 = db.create_workspace("WS2")
    db.set_active_workspace(ws1)
    db.pin_tab("logs")
    assert "logs" in db.get_tabs()
    db.set_active_workspace(ws2)
    assert db.get_tabs() == DEFAULT_TABS
    assert "logs" not in db.get_tabs()


# ---------------------------------------------------------------------------
# per-workspace default process strategy (import/process split PR 1)
# ---------------------------------------------------------------------------


def _a_process_id(client, name):
    procs = client.get("/api/processes").get_json()
    return next(p["id"] for p in procs if p["name"] == name)


def _put_default_process(client, ws_id, value):
    return client.put(
        f"/api/workspaces/{ws_id}",
        data=json.dumps(
            {"config_overrides": {"pipeline": {"default_process_id": value}}}
        ),
        content_type="application/json",
    )


def test_workspace_default_process_saved_and_effective(client):
    resp = client.post(
        "/api/workspaces",
        data=json.dumps({"name": "Strat"}),
        content_type="application/json",
    )
    ws_id = resp.get_json()["id"]
    pid = _a_process_id(client, "Cull-ready")
    resp = _put_default_process(client, ws_id, pid)
    assert resp.status_code == 200

    import config as cfg
    from db import Database

    # Read through get_effective_config exactly like the pipeline does.
    db_path = None
    with client.application.app_context():
        db_path = client.application.config["DB_PATH"]
    db = Database(db_path)
    db.set_active_workspace(ws_id)
    effective = db.get_effective_config(cfg.load())
    assert effective["pipeline"]["default_process_id"] == pid


def test_workspace_default_process_null_means_import_only(client):
    """None is the "no automatic processing after import" sentinel that the
    chaining hook short-circuits on. Saving it must succeed — if this 400s,
    the "import only" user flow is unreachable."""
    resp = client.post(
        "/api/workspaces",
        data=json.dumps({"name": "StratNull"}),
        content_type="application/json",
    )
    ws_id = resp.get_json()["id"]
    resp = _put_default_process(client, ws_id, None)
    assert resp.status_code == 200

    import config as cfg
    from db import Database

    db = Database(client.application.config["DB_PATH"])
    db.set_active_workspace(ws_id)
    effective = db.get_effective_config(cfg.load())
    assert effective["pipeline"]["default_process_id"] is None


def test_workspace_default_process_unknown_400(client):
    resp = client.post(
        "/api/workspaces",
        data=json.dumps({"name": "StratBad"}),
        content_type="application/json",
    )
    ws_id = resp.get_json()["id"]
    resp = _put_default_process(client, ws_id, 999999)
    assert resp.status_code == 400
    assert "unknown process id" in resp.get_json()["error"]


@pytest.mark.parametrize("bad", [True, "identify", ["1"], {"id": 1}])
def test_workspace_default_process_non_int_400(client, bad):
    """A JSON client sending a bool, string, list, or dict for
    ``pipeline.default_process_id`` must get a 400 validation error, not a
    500. (bool is a subclass of int and must be rejected explicitly.)"""
    resp = client.post(
        "/api/workspaces",
        data=json.dumps({"name": f"StratBadType-{type(bad).__name__}"}),
        content_type="application/json",
    )
    ws_id = resp.get_json()["id"]
    resp = _put_default_process(client, ws_id, bad)
    assert resp.status_code == 400
    assert "integer" in resp.get_json()["error"]


def test_config_default_process_id_default_is_none():
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "vireo"))
    from config import DEFAULTS

    assert DEFAULTS["pipeline"]["default_process_id"] is None


# ---------------------------------------------------------------------------
# Create-workspace config_overrides validation (mirror of PUT validation)
#
# Regression tripwires for a validation gap on POST /api/workspaces: the
# create path used to pass ``body.get("config_overrides")`` straight into
# db.create_workspace without checking pipeline.default_process_id, so a
# client could seed a workspace with a dangling process id that PUT and
# /api/jobs/pipeline would both reject — later feeding the chaining hook an
# invalid id that only surfaces as a job failure.
# ---------------------------------------------------------------------------


def test_create_workspace_rejects_unknown_default_process(client):
    resp = client.post(
        "/api/workspaces",
        data=json.dumps({
            "name": "StratBadCreate",
            "config_overrides": {"pipeline": {"default_process_id": 999999}},
        }),
        content_type="application/json",
    )
    assert resp.status_code == 400
    assert "unknown process id" in resp.get_json()["error"]


@pytest.mark.parametrize("bad", [True, "identify", ["1"], {"id": 1}])
def test_create_workspace_rejects_non_int_default_process(client, bad):
    resp = client.post(
        "/api/workspaces",
        data=json.dumps({
            "name": f"StratBadTypeCreate-{type(bad).__name__}",
            "config_overrides": {"pipeline": {"default_process_id": bad}},
        }),
        content_type="application/json",
    )
    assert resp.status_code == 400
    assert "integer" in resp.get_json()["error"]


def test_create_workspace_accepts_valid_default_process(client):
    pid = _a_process_id(client, "Cull-ready")
    resp = client.post(
        "/api/workspaces",
        data=json.dumps({
            "name": "StratCreateOK",
            "config_overrides": {"pipeline": {"default_process_id": pid}},
        }),
        content_type="application/json",
    )
    assert resp.status_code == 200
    ws_id = resp.get_json()["id"]

    import config as cfg
    from db import Database

    db = Database(client.application.config["DB_PATH"])
    db.set_active_workspace(ws_id)
    effective = db.get_effective_config(cfg.load())
    assert effective["pipeline"]["default_process_id"] == pid


def test_create_workspace_accepts_null_default_process(client):
    """Explicit null on create must round-trip — it is the "import only"
    sentinel and mirrors the PUT path's null acceptance."""
    resp = client.post(
        "/api/workspaces",
        data=json.dumps({
            "name": "StratCreateNull",
            "config_overrides": {"pipeline": {"default_process_id": None}},
        }),
        content_type="application/json",
    )
    assert resp.status_code == 200


def test_create_workspace_rejects_non_object_config_overrides(client):
    """``config_overrides`` must be a JSON object or null; anything else
    would be persisted as-is and break the labels accessors."""
    resp = client.post(
        "/api/workspaces",
        data=json.dumps({
            "name": "StratBadOverrides",
            "config_overrides": "not-a-dict",
        }),
        content_type="application/json",
    )
    assert resp.status_code == 400
    assert "config_overrides" in resp.get_json()["error"]


# ---------------------------------------------------------------------------
# Legacy ``pipeline.default_strategy`` translation on workspace overrides.
#
# An older client (or a payload restored from a pre-migration backup) may
# still send the hardcoded strategy name. The workspace endpoints must
# translate it to ``default_process_id`` up front — otherwise it falls
# through as an inert non-schema key and the effective default_process_id
# stays null, silently downgrading the workspace to import-only.
# ---------------------------------------------------------------------------


def _effective_pipeline(client, ws_id):
    import config as cfg
    from db import Database

    db = Database(client.application.config["DB_PATH"])
    db.set_active_workspace(ws_id)
    return db.get_effective_config(cfg.load())["pipeline"]


def test_put_workspace_translates_legacy_default_strategy(client):
    """PUT with the legacy ``default_strategy: "identify"`` must land on the
    matching seed's ``default_process_id`` in the effective config, not fall
    through as an inert override."""
    resp = client.post(
        "/api/workspaces",
        data=json.dumps({"name": "LegacyPut"}),
        content_type="application/json",
    )
    ws_id = resp.get_json()["id"]

    resp = client.put(
        f"/api/workspaces/{ws_id}",
        data=json.dumps(
            {"config_overrides": {"pipeline": {"default_strategy": "identify"}}}
        ),
        content_type="application/json",
    )
    assert resp.status_code == 200

    expected_pid = _a_process_id(client, "Identify birds")
    effective = _effective_pipeline(client, ws_id)
    assert effective["default_process_id"] == expected_pid
    assert "default_strategy" not in effective


def test_put_workspace_unknown_legacy_strategy_maps_to_null(client):
    """An unrecognized legacy name must translate to ``None`` (import only)
    rather than falling through as an inert override that leaves
    ``default_process_id`` null-by-omission on the workspace but subject to
    the global default via effective-config merging."""
    resp = client.post(
        "/api/workspaces",
        data=json.dumps({"name": "LegacyPutUnknown"}),
        content_type="application/json",
    )
    ws_id = resp.get_json()["id"]

    resp = client.put(
        f"/api/workspaces/{ws_id}",
        data=json.dumps(
            {"config_overrides": {"pipeline": {"default_strategy": "not-a-strategy"}}}
        ),
        content_type="application/json",
    )
    assert resp.status_code == 200

    # Read the persisted override directly from the workspaces list — the
    # per-workspace effective config would fall back to the global default
    # if the workspace had NO override, so we need to see what was actually
    # stored to distinguish "translated to explicit null" from "silently
    # dropped".
    ws = next(
        w for w in client.get("/api/workspaces").get_json() if w["id"] == ws_id
    )
    raw_overrides = ws.get("config_overrides")
    overrides = (
        json.loads(raw_overrides) if isinstance(raw_overrides, str) else (raw_overrides or {})
    )
    pipeline = overrides.get("pipeline") or {}
    assert "default_strategy" not in pipeline
    assert pipeline.get("default_process_id") is None


def test_put_workspace_both_keys_new_wins(client):
    """If a caller sends both the legacy and the new key, the new one wins
    and the legacy key is dropped so it can't resurface on a later read."""
    resp = client.post(
        "/api/workspaces",
        data=json.dumps({"name": "LegacyPutBoth"}),
        content_type="application/json",
    )
    ws_id = resp.get_json()["id"]

    cull_ready = _a_process_id(client, "Cull-ready")
    resp = client.put(
        f"/api/workspaces/{ws_id}",
        data=json.dumps({
            "config_overrides": {
                "pipeline": {
                    # legacy names "identify" (would map to "Identify birds"),
                    # but the explicit new key must take precedence.
                    "default_strategy": "identify",
                    "default_process_id": cull_ready,
                }
            }
        }),
        content_type="application/json",
    )
    assert resp.status_code == 200

    effective = _effective_pipeline(client, ws_id)
    assert effective["default_process_id"] == cull_ready
    assert "default_strategy" not in effective


def test_create_workspace_translates_legacy_default_strategy(client):
    """The create path shares the validator, so the same translation must
    apply — otherwise a workspace can be spawned with an inert legacy key."""
    expected_pid = _a_process_id(client, "Cull-ready")
    resp = client.post(
        "/api/workspaces",
        data=json.dumps({
            "name": "LegacyCreate",
            "config_overrides": {"pipeline": {"default_strategy": "cull_ready"}},
        }),
        content_type="application/json",
    )
    assert resp.status_code == 200
    ws_id = resp.get_json()["id"]

    effective = _effective_pipeline(client, ws_id)
    assert effective["default_process_id"] == expected_pid
