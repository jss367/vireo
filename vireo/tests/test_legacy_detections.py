"""Upgrade discovery uses unfiltered legacy predictions in visible folders."""

from services import moves_merge, photo_ingest, workspace_folders


def _predict(db, photo_id, *, fingerprint="legacy", model="old-model"):
    detection = db.detections.save(photo_id, [{
        "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
        "confidence": 0.01, "category": "animal",
    }], "old-detector")[0]
    db.predictions.add(detection, "Robin", 0.01, model,
                       labels_fingerprint=fingerprint, status="rejected")


def test_legacy_folders_count_photos_once_without_visibility_filters(app_and_db):
    app, db = app_and_db
    photos = db.conn.execute("SELECT id, folder_id FROM photos ORDER BY id").fetchall()
    first, modern, last = photos
    _predict(db, first["id"])
    _predict(db, first["id"], model="another-model")
    _predict(db, last["id"])
    _predict(db, modern["id"], fingerprint="current-labels")

    response = app.test_client().get("/api/pipeline/legacy-detections")
    assert response.status_code == 200
    assert response.json == {"folders": [{
        "id": first["folder_id"], "path": "/photos/2024", "photo_count": 2,
    }]}

    # Redetection retires old detections and their sentinel predictions.
    for photo in (first, last):
        db.detections.clear(photo["id"])
        _predict(db, photo["id"], fingerprint="current-labels")
    assert app.test_client().get("/api/pipeline/legacy-detections").json == {"folders": []}


def test_legacy_folders_follow_active_workspace(app_and_db):
    app, db = app_and_db
    original_ws = db.active_workspace_id
    first = db.conn.execute("SELECT id, folder_id FROM photos ORDER BY id LIMIT 1").fetchone()
    _predict(db, first["id"])
    other_ws = db.create_workspace("Separate library")
    db.set_active_workspace(other_ws)
    folder = workspace_folders.add_folder(db, "/other/legacy")
    photo = photo_ingest.add_photo(db, folder, "other.jpg", ".jpg", 10, 1.0)
    _predict(db, photo)
    db.set_active_workspace(original_ws)

    client = app.test_client()
    assert client.post(f"/api/workspaces/{other_ws}/activate").status_code == 200
    assert client.get("/api/pipeline/legacy-detections").json == {"folders": [{
        "id": folder, "path": "/other/legacy", "photo_count": 1,
    }]}
    assert client.post(f"/api/workspaces/{original_ws}/activate").status_code == 200
    assert client.get("/api/pipeline/legacy-detections").json["folders"][0]["id"] == first["folder_id"]


def test_fresh_library_has_no_legacy_folders(app_and_db):
    app, _ = app_and_db
    assert app.test_client().get("/api/pipeline/legacy-detections").json == {"folders": []}


def test_legacy_scan_keeps_moved_photo_only_grants_without_exposing_siblings(db):
    owner = db.active_workspace_id
    sharing = db.create_workspace("Sharing")
    empty = db.create_workspace("Empty")
    db.set_active_workspace(owner)
    archive = workspace_folders.add_folder(db, "/archive")
    sibling = photo_ingest.add_photo(db, archive, "sibling.jpg", ".jpg", 10, 1.0)
    stage = workspace_folders.add_folder(db, "/stage")
    moved = photo_ingest.add_photo(db, stage, "moved.jpg", ".jpg", 10, 1.0)
    workspace_folders.add_workspace_folder(db, sharing, stage)
    _predict(db, sibling)
    _predict(db, moved)
    _predict(db, moved, model="another-model")

    moves_merge.merge_staged_tree_into_archive(db, stage, "/archive")
    assert db.conn.execute(
        "SELECT 1 FROM workspace_photos WHERE workspace_id = ? AND photo_id = ?",
        (sharing, moved),
    ).fetchone() is not None
    assert db.conn.execute(
        "SELECT 1 FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
        (sharing, archive),
    ).fetchone() is None

    db.set_active_workspace(sharing)
    assert db.predictions.legacy_detection_folders() == [{
        "id": archive, "path": "/archive", "photo_count": 1,
    }]
    # Overlapping folder and photo grants must not count the moved photo twice.
    workspace_folders.add_workspace_folder(db, sharing, archive)
    assert db.predictions.legacy_detection_folders() == [{
        "id": archive, "path": "/archive", "photo_count": 2,
    }]
    db.set_active_workspace(empty)
    assert db.predictions.legacy_detection_folders() == []
