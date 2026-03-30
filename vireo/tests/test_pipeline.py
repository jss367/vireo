# vireo/tests/test_pipeline.py
"""Tests for the pipeline orchestration module.

Uses a real SQLite database with synthetic photo data to verify the
full pipeline from feature loading through triage labeling.
"""
import json
import os
import sys
from datetime import datetime, timedelta

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _setup_db_with_photos(tmp_path, n_encounters=2, photos_per_encounter=3):
    """Create a test DB with photos that have pipeline features populated.

    Returns (db, photo_ids_by_encounter).
    """
    from db import Database
    from dino_embed import embedding_to_blob

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    base_time = datetime(2026, 3, 20, 10, 0, 0)
    all_ids = []

    for enc_idx in range(n_encounters):
        enc_ids = []
        # Each encounter has a distinct embedding direction
        emb_base = np.zeros(768, dtype=np.float32)
        emb_base[enc_idx * 100: enc_idx * 100 + 100] = 1.0
        emb_base = emb_base / np.linalg.norm(emb_base)

        enc_offset = enc_idx * 300  # 300s between encounters (hard time cut)

        for i in range(photos_per_encounter):
            ts = base_time + timedelta(seconds=enc_offset + i * 2)
            pid = db.add_photo(
                fid,
                f"enc{enc_idx}_photo{i}.jpg",
                ".jpg",
                1000,
                1.0,
                timestamp=ts.isoformat(),
                width=4000,
                height=3000,
            )

            # Add slightly varied embedding per photo
            emb = emb_base + np.random.RandomState(pid).randn(768).astype(np.float32) * 0.01
            emb = emb / np.linalg.norm(emb)

            db.update_photo_mask(pid, f"/masks/{pid}.png")
            db.update_photo_pipeline_features(
                pid,
                mask_path=f"/masks/{pid}.png",
                subject_tenengrad=200 + i * 50 + enc_idx * 10,
                bg_tenengrad=30 + i * 5,
                crop_complete=0.85 + i * 0.03,
                bg_separation=50.0 - i * 10,
                subject_clip_high=0.01,
                subject_clip_low=0.01,
                subject_y_median=120.0,
                phash_crop=f"{pid:016x}",
            )
            db.update_photo_embeddings(
                pid,
                dino_subject_embedding=embedding_to_blob(emb),
                dino_global_embedding=embedding_to_blob(emb),
            )

            # Update subject_size via the existing quality method
            db.update_photo_quality(
                pid,
                subject_size=0.08 + i * 0.02,
            )

            # Add a detection in the detections table
            det_ids = db.save_detections(pid, [
                {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.9},
            ], detector_model="megadetector")

            # Add a species prediction (references detection, not photo)
            species = "robin" if enc_idx == 0 else "eagle"
            db.add_prediction(
                det_ids[0], species, 0.9 - i * 0.05, "bioclip",
                category="match",
            )

            enc_ids.append(pid)
        all_ids.append(enc_ids)

    return db, all_ids


# -- load_photo_features --


def test_load_photo_features(tmp_path):
    """load_photo_features returns photos with all expected fields."""
    from pipeline import load_photo_features

    db, ids = _setup_db_with_photos(tmp_path)
    photos = load_photo_features(db)

    assert len(photos) == 6  # 2 encounters × 3 photos

    p = photos[0]
    assert p["id"] is not None
    assert p["timestamp"] is not None
    assert p["mask_path"] is not None
    assert p["subject_tenengrad"] is not None
    assert p["dino_subject_embedding"] is not None
    assert isinstance(p["dino_subject_embedding"], np.ndarray)
    assert p["dino_subject_embedding"].dtype == np.float32
    assert len(p["species_top5"]) >= 1
    assert p["species_top5"][0][0] in ("robin", "eagle")


def test_load_photo_features_empty_db(tmp_path):
    """Empty DB returns empty list."""
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    photos = load_photo_features(db)
    assert photos == []


def test_load_photo_features_no_predictions(tmp_path):
    """Photos without predictions get empty species_top5."""
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="root")
    db.add_photo(fid, "a.jpg", ".jpg", 100, 1.0)

    photos = load_photo_features(db)
    assert len(photos) == 1
    assert photos[0]["species_top5"] == []


# -- run_grouping --


def test_run_grouping_separates_encounters(tmp_path):
    """Photos 300s apart should end up in different encounters."""
    from pipeline import load_photo_features, run_grouping

    db, ids = _setup_db_with_photos(tmp_path, n_encounters=2, photos_per_encounter=3)
    photos = load_photo_features(db)

    encounters = run_grouping(photos)
    assert len(encounters) == 2
    assert encounters[0]["photo_count"] == 3
    assert encounters[1]["photo_count"] == 3

    # Each encounter should have bursts
    for enc in encounters:
        assert "bursts" in enc
        assert enc["burst_count"] >= 1


def test_run_grouping_single_encounter(tmp_path):
    """All photos close together → one encounter."""
    from pipeline import load_photo_features, run_grouping

    db, ids = _setup_db_with_photos(tmp_path, n_encounters=1, photos_per_encounter=5)
    photos = load_photo_features(db)

    encounters = run_grouping(photos)
    assert len(encounters) == 1
    assert encounters[0]["photo_count"] == 5


# -- run_triage --


def test_run_triage_labels_all_photos(tmp_path):
    """Every photo should get a KEEP, REVIEW, or REJECT label."""
    from pipeline import load_photo_features, run_grouping, run_triage

    db, ids = _setup_db_with_photos(tmp_path)
    photos = load_photo_features(db)
    encounters = run_grouping(photos)
    _, all_photos = run_triage(encounters)

    assert len(all_photos) == 6
    for p in all_photos:
        assert p["label"] in ("KEEP", "REVIEW", "REJECT"), f"Photo {p['id']} has label {p.get('label')}"
        assert "quality_composite" in p


def test_run_triage_has_scores(tmp_path):
    """Scored photos should have all sub-scores."""
    from pipeline import load_photo_features, run_grouping, run_triage

    db, ids = _setup_db_with_photos(tmp_path)
    photos = load_photo_features(db)
    encounters = run_grouping(photos)
    _, all_photos = run_triage(encounters)

    for p in all_photos:
        for key in ("focus_score", "exposure_score", "composition_score", "area_score", "noise_score"):
            assert key in p, f"Photo {p['id']} missing {key}"


# -- run_full_pipeline --


def test_run_full_pipeline(tmp_path):
    """Full pipeline returns encounters, labeled photos, and summary."""
    from pipeline import load_photo_features, run_full_pipeline

    db, ids = _setup_db_with_photos(tmp_path)
    photos = load_photo_features(db)

    results = run_full_pipeline(photos)

    assert "encounters" in results
    assert "photos" in results
    assert "summary" in results

    s = results["summary"]
    assert s["total_photos"] == 6
    assert s["encounter_count"] == 2
    assert s["keep_count"] + s["review_count"] + s["reject_count"] == 6


# -- serialize_results + save/load --


def test_serialize_results_is_json_safe(tmp_path):
    """Serialized results should be JSON-serializable (no numpy)."""
    from pipeline import load_photo_features, run_full_pipeline, serialize_results

    db, ids = _setup_db_with_photos(tmp_path)
    photos = load_photo_features(db)
    results = run_full_pipeline(photos)

    serialized = serialize_results(results)
    # Should not raise
    json_str = json.dumps(serialized)
    assert len(json_str) > 0

    # Round-trip
    parsed = json.loads(json_str)
    assert parsed["summary"]["total_photos"] == 6


def test_save_load_results_roundtrip(tmp_path):
    """Results survive save → load cycle."""
    from pipeline import (
        load_photo_features,
        load_results,
        run_full_pipeline,
        save_results,
    )

    db, ids = _setup_db_with_photos(tmp_path)
    photos = load_photo_features(db)
    results = run_full_pipeline(photos)

    cache_dir = str(tmp_path)
    path = save_results(results, cache_dir, workspace_id=1)
    assert os.path.exists(path)

    loaded = load_results(cache_dir, workspace_id=1)
    assert loaded is not None
    assert loaded["summary"]["total_photos"] == 6
    assert len(loaded["photos"]) == 6
    assert len(loaded["encounters"]) == 2


def test_load_results_missing(tmp_path):
    """load_results returns None when no cache exists."""
    from pipeline import load_results

    assert load_results(str(tmp_path), workspace_id=999) is None


# -- Species encounter labels --


def test_encounters_have_species(tmp_path):
    """Each encounter should have a species label."""
    from pipeline import load_photo_features, run_grouping

    db, ids = _setup_db_with_photos(tmp_path)
    photos = load_photo_features(db)
    encounters = run_grouping(photos)

    species_names = {enc["species"][0] for enc in encounters}
    assert "robin" in species_names
    assert "eagle" in species_names


def test_load_photo_features_confirmed_species(tmp_path):
    """Photos with species keywords get confirmed_species set."""
    from pipeline import load_photo_features

    db, ids = _setup_db_with_photos(tmp_path)
    # Confirm species for first encounter's photos
    kid = db.add_keyword("Robin", is_species=True)
    for pid in ids[0]:
        db.tag_photo(pid, kid)

    photos = load_photo_features(db)
    confirmed = [p for p in photos if p["confirmed_species"] is not None]
    assert len(confirmed) == len(ids[0])
    for p in confirmed:
        assert p["confirmed_species"] == "Robin"

    # Second encounter should have no confirmed species
    unconfirmed = [p for p in photos if p["confirmed_species"] is None]
    assert len(unconfirmed) == len(ids[1])


def test_confirmed_species_deterministic_with_multiple_tags(tmp_path):
    """When a photo has multiple species tags, confirmed_species is deterministic (alphabetically first)."""
    from pipeline import load_photo_features

    db, ids = _setup_db_with_photos(tmp_path)
    pid = ids[0][0]

    # Tag with two species in non-alphabetical order
    k_zebra = db.add_keyword("Zebra Finch", is_species=True)
    k_blue = db.add_keyword("Blue Jay", is_species=True)
    db.tag_photo(pid, k_zebra)
    db.tag_photo(pid, k_blue)

    # Run twice to confirm determinism
    for _ in range(2):
        photos = load_photo_features(db)
        photo = next(p for p in photos if p["id"] == pid)
        assert photo["confirmed_species"] == "Blue Jay"


def test_serialize_results_includes_species_predictions(tmp_path):
    """serialize_results includes species_predictions and confirmed_species."""
    from pipeline import load_photo_features, run_full_pipeline, serialize_results

    db, ids = _setup_db_with_photos(tmp_path)
    # Confirm species for first encounter
    kid = db.add_keyword("Robin", is_species=True)
    for pid in ids[0]:
        db.tag_photo(pid, kid)

    photos = load_photo_features(db)
    results = run_full_pipeline(photos)
    serialized = serialize_results(results)

    for enc in serialized["encounters"]:
        assert "species_predictions" in enc
        assert "confirmed_species" in enc
        assert isinstance(enc["species_predictions"], list)
        assert "species_confirmed" in enc
        assert isinstance(enc["species_confirmed"], bool)

    # At least one encounter should have confirmed species
    confirmed_encs = [e for e in serialized["encounters"]
                      if e["confirmed_species"] is not None]
    assert len(confirmed_encs) >= 1
    assert confirmed_encs[0]["confirmed_species"] == "Robin"
    assert confirmed_encs[0]["species_confirmed"] is True

    # Unconfirmed encounters should have species_confirmed=False
    unconfirmed_encs = [e for e in serialized["encounters"]
                        if e["confirmed_species"] is None]
    for e in unconfirmed_encs:
        assert e["species_confirmed"] is False

    # Species predictions should have species/count/models
    for enc in serialized["encounters"]:
        for pred in enc["species_predictions"]:
            assert "species" in pred
            assert "count" in pred
            assert "models" in pred
            assert "avg_confidence" in pred


def test_load_photo_features_includes_model_in_species(tmp_path):
    """species_top5 entries include the model name."""
    from pipeline import load_photo_features

    db, ids = _setup_db_with_photos(tmp_path)
    photos = load_photo_features(db)

    # _setup_db_with_photos adds predictions — check model is present
    for p in photos:
        for entry in p.get("species_top5", []):
            assert len(entry) == 3, f"Expected (species, confidence, model), got {entry}"
            assert isinstance(entry[2], str), f"Model should be a string, got {type(entry[2])}"


def test_load_photo_features_collection_scoped(tmp_path):
    """load_photo_features with collection_id returns only collection photos."""
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    p1 = db.add_photo(fid, "a.jpg", ".jpg", 1000, 1.0, timestamp="2026-01-01T10:00:00")
    p2 = db.add_photo(fid, "b.jpg", ".jpg", 1000, 1.0, timestamp="2026-01-01T11:00:00")

    # Create a static collection with only p1
    rules = json.dumps([{"field": "photo_ids", "value": [p1]}])
    cid = db.add_collection("test-coll", rules)

    # Without collection_id — returns both
    all_photos = load_photo_features(db)
    assert len(all_photos) == 2

    # With collection_id — returns only p1
    scoped = load_photo_features(db, collection_id=cid)
    assert len(scoped) == 1
    assert scoped[0]["id"] == p1


def test_serialize_results_has_species_predictions(tmp_path):
    """Serialized encounters include species_predictions with model info."""
    from pipeline import load_photo_features, run_full_pipeline, serialize_results

    db, ids = _setup_db_with_photos(tmp_path)
    photos = load_photo_features(db)
    results = run_full_pipeline(photos)
    serialized = serialize_results(results)

    for enc in serialized["encounters"]:
        assert "species_predictions" in enc
        assert "species_confirmed" in enc
        assert isinstance(enc["species_confirmed"], bool)
        # species_predictions should have model breakdown
        for sp in enc["species_predictions"]:
            assert "species" in sp
            assert "models" in sp
            for m in sp["models"]:
                assert "model" in m
                assert "confidence" in m
                assert "photo_count" in m


def test_serialize_results_has_burst_species_predictions(tmp_path):
    """Serialized bursts include species_predictions scoped to burst photos."""
    from pipeline import load_photo_features, run_full_pipeline, serialize_results

    db, ids = _setup_db_with_photos(tmp_path)
    photos = load_photo_features(db)
    results = run_full_pipeline(photos)
    serialized = serialize_results(results)

    for enc in serialized["encounters"]:
        if "bursts" not in enc:
            continue
        for burst in enc["bursts"]:
            assert isinstance(burst, dict), "Bursts should be dicts, not lists of IDs"
            assert "photo_ids" in burst
            assert "species_predictions" in burst
            assert "species_override" in burst
