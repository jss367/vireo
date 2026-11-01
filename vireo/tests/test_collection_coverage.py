"""Import history is stable; current membership and decisions are independent."""

import json
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest
import schema
from db import AUTO_MATCH_REVIEW_MARKER, Database
from services import photo_ingest, workspace_folders
from services.collection_coverage import collection_coverage
from services.imports import ImportService


def _photos(db, tmp_path, count=3):
    folder = workspace_folders.add_folder(db, str(tmp_path / "originals"), name="originals")
    photos = [photo_ingest.add_photo(
        db, folder_id=folder, filename=f"{i}.jpg", extension=".jpg",
        file_size=10, file_mtime=1,
    ) for i in range(count)]
    return folder, photos


def _collection(db, photos, original=None):
    if original is not None:
        # Admission can include records that are no longer in the catalog.
        absent = [max(photos, default=0) + i + 1 for i in range(original - len(photos))]
        with db.transaction():
            return db.collections.add_import("Imported photos", [*photos, *absent])
    with db.transaction():
        return db.collections.add(
            "Imported photos", json.dumps([{"field": "photo_ids", "value": photos}]),
        )


def _prediction(db, photo, status, individual=None):
    detection = db.detections.save(photo, [{
        "box": {"x": 0, "y": 0, "w": 1, "h": 1},
        "confidence": 0.9, "category": "animal",
    }], "test-detector")[0]
    return db.predictions.add(
        detection, "Unidentified bird", 0.9, "test-classifier",
        status=status, individual=individual,
    )


def test_import_total_survives_removed_members_and_rule_changes(db, tmp_path):
    _, photos = _photos(db, tmp_path)
    cid = _collection(db, photos, original=3)
    original = collection_coverage(db, cid, {})["original_import"]
    assert original["count"] == 3
    assert original["recorded_at"]
    assert collection_coverage(db, cid, {})["current_membership"]["catalog_count"] == 3

    # A record absent from the catalog is outside the current denominator.
    db.conn.execute("DELETE FROM photos WHERE id = ?", (photos[-1],))
    db.commit()
    removed = collection_coverage(db, cid, {})
    assert removed["original_import"] == original
    assert removed["current_membership"]["catalog_count"] == 2
    assert removed["review"]["denominator"] == 2

    db.collections.update(cid, rules_json=json.dumps([
        {"field": "photo_ids", "value": [photos[0]]},
    ]))
    db.commit()
    narrowed = collection_coverage(db, cid, {})
    assert narrowed["original_import"] == original
    assert narrowed["review"]["denominator"] == 1
    copied = db.collections.duplicate(cid)
    db.commit()
    assert collection_coverage(db, copied, {})["original_import"]["count"] is None


def test_reported_example_keeps_3303_original_and_3159_current(db, tmp_path):
    folder = workspace_folders.add_folder(db, str(tmp_path / "originals"), name="originals")
    db.conn.executemany(
        "INSERT INTO photos (folder_id, filename, extension) VALUES (?, ?, '.jpg')",
        [(folder, f"{i}.jpg") for i in range(3159)],
    )
    db.commit()
    photos = [r["id"] for r in db.conn.execute("SELECT id FROM photos")]
    cid = _collection(db, photos, original=3303)
    result = collection_coverage(db, cid, {})
    assert result["original_import"]["count"] == 3303
    assert result["current_membership"]["catalog_count"] == 3159
    assert result["review"]["denominator"] == 3159
    assert result["review"]["inspected_count"] is None
    assert "lost" not in result


def test_offline_records_and_missing_originals_are_different(db, tmp_path):
    folder, photos = _photos(db, tmp_path)
    cid = _collection(db, photos, original=3)
    # No files were created, but folder status cannot establish missing files.
    initial = collection_coverage(db, cid, {})
    assert initial["current_membership"]["online_folder_count"] == 3
    assert initial["original_files"]["known_missing_count"] is None
    snapshot = {"status": "ready", "stale": False, "checked_at": "2026-10-20T12:00:00Z",
                "photos": [{"id": photos[0]}, {"id": 999999}]}
    checked = collection_coverage(db, cid, snapshot)
    assert checked["original_files"]["known_missing_count"] == 1
    assert checked["original_files"]["accessible_count"] is None
    assert checked["current_membership"]["catalog_count"] == 3

    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (folder,))
    db.commit()
    offline = collection_coverage(db, cid, {})
    assert offline["current_membership"] == {
        "catalog_count": 3, "online_folder_count": 0, "offline_folder_count": 3,
    }
    assert offline["review"]["denominator"] == 3
    assert offline["original_import"]["count"] == 3


@pytest.mark.parametrize("snapshot", [
    {"status": "ready", "stale": True},
    {"status": "pending", "stale": False},
    {"status": "error", "stale": False},
])
def test_stale_or_failed_file_checks_do_not_claim_known_availability(db, tmp_path, snapshot):
    _, photos = _photos(db, tmp_path)
    cid = _collection(db, photos)
    snapshot["photos"] = [{"id": photos[0]}]
    assert collection_coverage(db, cid, snapshot)["original_files"]["known_missing_count"] is None


def test_prediction_decisions_do_not_imply_inspection_or_species_resolution(db, tmp_path):
    _, photos = _photos(db, tmp_path)
    cid = _collection(db, photos, original=3)
    _prediction(db, photos[0], "rejected")
    _prediction(db, photos[1], "accepted", AUTO_MATCH_REVIEW_MARKER)
    _prediction(db, photos[2], "pending")
    species = db.add_keyword("Confirmed bird", is_species=True)
    db.tag_photo(photos[1], species)
    db.commit()
    review = collection_coverage(db, cid, {})["review"]
    assert review["denominator"] == 3
    assert review["prediction_decision_count"] == 1
    assert review["with_species_count"] == 1
    assert review["without_species_count"] == 2
    assert review["inspected_count"] is None


def test_import_collection_creation_records_admitted_count(db, tmp_path):
    _, photos = _photos(db, tmp_path)
    service = object.__new__(ImportService)
    cid, _ = service._create_import_collection(db, photos)
    assert collection_coverage(db, cid, {})["original_import"]["count"] == 3


def test_explicitly_unresolved_is_independent_of_existing_species_and_scope(db, tmp_path, monkeypatch):
    from types import SimpleNamespace

    _, photos = _photos(db, tmp_path)
    cid = _collection(db, photos[:2])
    species = db.add_keyword("Confirmed bird", is_species=True)
    db.tag_photo(photos[0], species)
    db.commit()
    monkeypatch.setattr(Database, "identity_alternatives", property(lambda _db: None), raising=False)
    assert collection_coverage(db, cid, {})["review"]["unresolved_identity_count"] is None
    # The identity-alternatives workflow ships separately. Exercise its
    # public read contract without substituting species presence for it.
    monkeypatch.setattr(Database, "identity_alternatives", property(
        lambda _db: SimpleNamespace(photo_ids=lambda: {photos[0], photos[2]}),
    ), raising=False)
    review = collection_coverage(db, cid, {})["review"]
    assert review["unresolved_identity_count"] == 1
    assert review["with_species_count"] == 1
    assert review["without_species_count"] == 1


def test_coverage_endpoint_rejects_cross_workspace_and_visual_scope(app_and_db):
    app, db = app_and_db
    cid = db.collections.add("Saved", "[]")
    db.commit()
    client = app.test_client()
    result = client.get(f"/api/collections/{cid}/coverage")
    assert result.status_code == 200
    assert result.json["original_import"]["count"] is None
    visual = db.collections.add("Visual", "[]", visual_json='{"prompt":"bird"}')
    db.commit()
    assert client.get(f"/api/collections/{visual}/coverage").status_code == 400
    assert client.get("/api/collections/999999/coverage").status_code == 404
    assert client.get("/api/collections/9223372036854775808/coverage").status_code == 400
    workspace = db.create_workspace("Other")
    assert client.post(f"/api/workspaces/{workspace}/activate").status_code == 200
    assert client.get(f"/api/collections/{cid}/coverage").status_code == 404


def test_migration_does_not_guess_historical_total_from_rules(tmp_path):
    path = str(tmp_path / "historical.db")
    db = Database(path)
    cid = db.collections.add("Import 2026-10-17", '[{"field":"photo_ids","value":[1,2,3]}]')
    db.commit()
    db.close()
    with sqlite3.connect(path) as conn:
        conn.execute("ALTER TABLE collections DROP COLUMN original_import_count")
        conn.execute("ALTER TABLE collections DROP COLUMN import_recorded_at")
        conn.execute("PRAGMA user_version = 22")
    schema.ensure_schema(path)
    db = Database(path)
    try:
        row = db.collections.import_provenance(cid)
        assert row["original_import_count"] is None
        assert row["import_recorded_at"] is None
    finally:
        db.close()


def test_browser_coverage_drops_stale_scope_and_explains_unknown_counts():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for browser coverage regressions")
    result = subprocess.run(
        [node, str(Path(__file__).with_name("collection_coverage.cjs"))],
        cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_prediction_decisions_require_decision_in_every_any_rule_branch(db, tmp_path):
    _, photos = _photos(db, tmp_path)
    cid = db.collections.add("Either photo", json.dumps({
        "mode": "any", "rules": [
            {"field": "photo_ids", "value": [photos[0]]},
            {"field": "photo_ids", "value": [photos[1]]},
        ],
    }))
    db.commit()
    # Neither a pending prediction nor an automatic taxonomy match proves
    # an explicit decision, regardless of which OR branch includes the photo.
    _prediction(db, photos[0], "pending")
    _prediction(db, photos[1], "accepted", AUTO_MATCH_REVIEW_MARKER)
    assert collection_coverage(db, cid, {})["review"]["prediction_decision_count"] == 0
    _prediction(db, photos[0], "reviewed")
    assert collection_coverage(db, cid, {})["review"]["prediction_decision_count"] == 1
