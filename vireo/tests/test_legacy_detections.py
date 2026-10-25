"""Upgrade discovery uses unfiltered legacy predictions in visible folders."""

import pytest
from services import detection_store, moves_merge, photo_ingest, workspace_folders


def _predict(db, photo_id, *, fingerprint="legacy", model="old-model", runtime="legacy"):
    detection = db.detections.save(photo_id, [{
        "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
        "confidence": 0.01, "category": "animal",
    }], "megadetector-v6", runtime_fingerprint=runtime)[0]
    db.predictions.add(detection, "Robin", 0.01, model,
                       labels_fingerprint=fingerprint, status="rejected")
    return detection


def test_legacy_folders_count_photos_once_without_visibility_filters(app_and_db):
    app, db = app_and_db
    photos = db.conn.execute("SELECT id, folder_id FROM photos ORDER BY id").fetchall()
    first, modern, last = photos
    _predict(db, first["id"])
    _predict(db, first["id"], model="another-model")
    _predict(db, last["id"])
    _predict(db, modern["id"], fingerprint="current-labels", runtime="raw-detector-runtime")

    response = app.test_client().get("/api/pipeline/legacy-detections")
    assert response.status_code == 200
    assert response.json == {"folders": [{
        "id": first["folder_id"], "path": "/photos/2024", "photo_count": 2,
    }]}

    # Redetection retires old detections and their sentinel predictions.
    for photo in (first, last):
        db.detections.clear(photo["id"])
        _predict(db, photo["id"], fingerprint="current-labels", runtime="raw-detector-runtime")
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


@pytest.mark.parametrize("empty_scene", [False, True])
def test_raw_detector_run_hides_surviving_legacy_classifier_rows(db, empty_scene):
    folder = workspace_folders.add_folder(db, "/photos/legacy")
    photo = photo_ingest.add_photo(db, folder, "old.jpg", ".jpg", 10, 1.0)
    old_id = _predict(db, photo)
    # A synthetic full-image subject may also survive an empty-scene rerun.
    anchor = db.detections.save(photo, [{
        "box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 1,
    }], "full-image")[0]
    db.predictions.add(anchor, "Robin", 0.9, "old-model")
    assert db.predictions.legacy_detection_folders()[0]["photo_count"] == 1

    ids = detection_store.write_detection_batch(db, photo, "megadetector-v6", [] if empty_scene else [{
        "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
        "confidence": 0.01, "category": "animal",
    }], runtime_fingerprint="raw-detector-runtime")
    if not empty_scene:
        assert ids == [old_id]
        assert db.conn.execute(
            "SELECT 1 FROM predictions WHERE detection_id = ? AND labels_fingerprint = 'legacy'",
            (old_id,),
        ).fetchone()
        # Reclassify clears only its selected label fingerprint, not legacy rows.
        db.predictions.clear(model="old-model", collection_photo_ids=[photo], labels_fingerprint="current-labels")
    assert db.conn.execute(
        "SELECT 1 FROM predictions WHERE detection_id = ? AND labels_fingerprint = 'legacy'", (anchor,),
    ).fetchone()
    assert db.predictions.legacy_detection_folders() == []


def test_legacy_detector_run_still_needs_raw_detection(db):
    folder = workspace_folders.add_folder(db, "/photos/legacy")
    photo = photo_ingest.add_photo(db, folder, "old.jpg", ".jpg", 10, 1.0)
    _predict(db, photo)
    db.model_runs.record_detector_run(photo, "megadetector-v6", 1, runtime_fingerprint="legacy")
    assert db.predictions.legacy_detection_folders() == [{
        "id": folder, "path": "/photos/legacy", "photo_count": 1,
    }]


@pytest.mark.parametrize("evidence", ["boxes", "run", "empty-run", "synthetic-only", "none"])
def test_unclassified_legacy_detector_output_is_found(db, evidence):
    folder = workspace_folders.add_folder(db, "/photos/legacy")
    photo = photo_ingest.add_photo(db, folder, "old.jpg", ".jpg", 10, 1.0)
    if evidence in ("boxes", "synthetic-only"):
        db.detections.save(photo, [{
            "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
            "confidence": 0.01, "category": "animal",
        }], "megadetector-v6" if evidence == "boxes" else "full-image")
    elif evidence in ("run", "empty-run"):
        db.model_runs.record_detector_run(photo, "megadetector-v6", 0 if evidence == "empty-run" else 1)
    assert db.conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
    expected = [] if evidence in ("none", "synthetic-only") else [{
        "id": folder, "path": "/photos/legacy", "photo_count": 1,
    }]
    assert db.predictions.legacy_detection_folders() == expected


@pytest.mark.parametrize("runtime", [None, "legacy", "other-runtime", "raw-runtime"])
def test_incomplete_raw_run_does_not_hide_legacy_evidence(db, runtime):
    folder = workspace_folders.add_folder(db, "/photos/legacy")
    photo = photo_ingest.add_photo(db, folder, "old.jpg", ".jpg", 10, 1.0)
    # A surviving synthetic prediction is evidence of the old library, but
    # cannot satisfy a MegaDetector run claiming that animal boxes were saved.
    anchor = db.detections.save(photo, [{
        "box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 1,
    }], "full-image")[0]
    db.predictions.add(anchor, "Robin", 0.9, "old-model")
    if runtime is not None:
        db.detections.save(photo, [{
            "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
            "confidence": 0.01, "category": "animal",
        }], "megadetector-v6", runtime_fingerprint=runtime)
    db.model_runs.record_detector_run(photo, "megadetector-v6", 1, runtime_fingerprint="raw-runtime")
    expected = [] if runtime == "raw-runtime" else [{
        "id": folder, "path": "/photos/legacy", "photo_count": 1,
    }]
    assert db.predictions.legacy_detection_folders() == expected


@pytest.mark.parametrize("status", ["pending", "accepted", "rejected"])
def test_legacy_sentinel_discovery_is_independent_of_workspace_reviews(db, status):
    folder = workspace_folders.add_folder(db, "/photos/legacy")
    photo = photo_ingest.add_photo(db, folder, "old.jpg", ".jpg", 10, 1.0)
    # Only a sentinel prediction provides upgrade evidence here: no legacy
    # MegaDetector rows/runs can mask an accidental review-filtered read.
    anchor = db.detections.save(photo, [{
        "box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 1,
    }], "full-image")[0]
    db.predictions.add(anchor, "Robin", 0.01, "old-model")
    prediction = db.predictions.id_for_key(anchor, "old-model", "legacy", "Robin")
    db.predictions.set_review_status(prediction, db.active_workspace_id, status)
    other = db.create_workspace("Other reviews")
    db.predictions.set_review_status(prediction, other, "rejected" if status != "rejected" else "accepted")
    assert db.predictions.legacy_detection_folders() == [{
        "id": folder, "path": "/photos/legacy", "photo_count": 1,
    }]
