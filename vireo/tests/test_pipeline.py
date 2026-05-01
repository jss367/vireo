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


def test_save_results_preserves_miss_computed_at_across_reflow(tmp_path):
    """save_results must preserve an existing miss_computed_at marker
    when the caller's results dict doesn't carry one. reflow and
    regroup-live don't recompute misses, so overwriting the marker
    would make the pipeline_review "Review misses" shortcut hide itself
    after every threshold tweak even though miss flags are still valid."""
    from pipeline import (
        load_photo_features,
        load_results,
        run_full_pipeline,
        save_results,
        save_results_raw,
    )

    db, ids = _setup_db_with_photos(tmp_path)
    photos = load_photo_features(db)
    results = run_full_pipeline(photos)

    cache_dir = str(tmp_path)
    save_results(results, cache_dir, workspace_id=1)

    # Simulate pipeline_job's miss_stage stamping the marker.
    raw = load_results(cache_dir, workspace_id=1)
    raw["miss_computed_at"] = "2026-04-22T12:00:00.000000+00:00"
    save_results_raw(raw, cache_dir, workspace_id=1)

    # Reflow: save new results (without marker) and confirm the marker
    # survives, matching what the UI needs.
    fresh = run_full_pipeline(photos)
    assert "miss_computed_at" not in fresh
    save_results(fresh, cache_dir, workspace_id=1)

    loaded = load_results(cache_dir, workspace_id=1)
    assert loaded["miss_computed_at"] == "2026-04-22T12:00:00.000000+00:00"


def test_save_results_new_marker_overrides_existing(tmp_path):
    """When the caller's results dict does carry miss_computed_at (e.g.
    a fresh full pipeline run restamping the marker), it must win over
    any marker already in the cache — otherwise reruns couldn't
    advance the /misses?since= review window."""
    from pipeline import (
        load_photo_features,
        load_results,
        run_full_pipeline,
        save_results,
        save_results_raw,
    )

    db, ids = _setup_db_with_photos(tmp_path)
    photos = load_photo_features(db)
    results = run_full_pipeline(photos)

    cache_dir = str(tmp_path)
    save_results(results, cache_dir, workspace_id=1)

    raw = load_results(cache_dir, workspace_id=1)
    raw["miss_computed_at"] = "2026-04-22T12:00:00.000000+00:00"
    save_results_raw(raw, cache_dir, workspace_id=1)

    fresh = run_full_pipeline(photos)
    fresh["miss_computed_at"] = "2026-04-23T15:00:00.000000+00:00"
    save_results(fresh, cache_dir, workspace_id=1)

    loaded = load_results(cache_dir, workspace_id=1)
    assert loaded["miss_computed_at"] == "2026-04-23T15:00:00.000000+00:00"


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


def test_load_photo_features_filters_to_latest_fingerprint(tmp_path):
    """With multiple fingerprints cached for a (detection, model), only the
    latest one surfaces — otherwise stale species from an old label set
    would leak into the pipeline top-k.
    """
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    folder_id = db.add_folder("/tmp/p")
    db.add_workspace_folder(ws, folder_id)
    pid = db.add_photo(
        folder_id, "a.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_ids = db.save_detections(
        pid,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.95, "category": "animal"}],
        detector_model="MDV6",
    )
    # Older fingerprint row (stale label set) — Robin.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, "
        "species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-old', 'Robin', 0.9, '2026-01-01T00:00:00')",
        (det_ids[0],),
    )
    # Newer fingerprint row (current label set) — Blue Jay.
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, "
        "species, confidence, created_at) "
        "VALUES (?, 'bioclip-2', 'fp-new', 'Blue Jay', 0.85, '2026-04-24T00:00:00')",
        (det_ids[0],),
    )
    db.conn.commit()

    # Default behavior: most recent fingerprint only.
    photos = load_photo_features(db)
    species = [e[0] for e in photos[0]["species_top5"]]
    assert species == ["Blue Jay"], "should surface only the latest fingerprint's species"

    # Explicit override: pin to the stale fingerprint.
    photos = load_photo_features(db, labels_fingerprint="fp-old")
    species = [e[0] for e in photos[0]["species_top5"]]
    assert species == ["Robin"]


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


# -- DINOv2 variant filtering --


def _add_photo_with_embedding(db, fid, filename, emb, variant=None, ts=None):
    """Helper: insert a photo and attach a subject+global embedding."""
    from dino_embed import embedding_to_blob
    pid = db.add_photo(
        fid, filename, ".jpg", 1000, 1.0,
        timestamp=(ts or datetime(2026, 3, 20, 10, 0, 0)).isoformat(),
        width=4000, height=3000,
    )
    db.update_photo_embeddings(
        pid,
        dino_subject_embedding=embedding_to_blob(emb),
        dino_global_embedding=embedding_to_blob(emb),
        variant=variant,
    )
    return pid


def test_load_photo_features_filters_variant_mismatch(tmp_path):
    """load_photo_features drops embeddings whose stored variant differs from configured variant."""
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    # Photo A: stored as vit-b14 (768-dim)
    emb_b = np.ones(768, dtype=np.float32) / np.sqrt(768)
    pid_b = _add_photo_with_embedding(db, fid, "b.jpg", emb_b, variant="vit-b14")

    # Photo L: stored as vit-l14 (1024-dim)
    emb_l = np.ones(1024, dtype=np.float32) / np.sqrt(1024)
    pid_l = _add_photo_with_embedding(db, fid, "l.jpg", emb_l, variant="vit-l14")

    # Configure pipeline to use vit-b14
    cfg = {"pipeline": {"dinov2_variant": "vit-b14"}}
    photos = load_photo_features(db, config=cfg)
    by_id = {p["id"]: p for p in photos}

    assert by_id[pid_b]["dino_subject_embedding"] is not None
    assert len(by_id[pid_b]["dino_subject_embedding"]) == 768
    # Mismatched variant must be dropped so it can't feed a dot product of wrong dim
    assert by_id[pid_l]["dino_subject_embedding"] is None
    assert by_id[pid_l]["dino_global_embedding"] is None


def test_load_photo_features_legacy_null_variant_dim_fallback(tmp_path):
    """Photos with NULL variant (pre-migration) are kept only if embedding dim matches."""
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    # Legacy 768-dim embedding, NULL variant
    emb_768 = np.ones(768, dtype=np.float32) / np.sqrt(768)
    pid_match = _add_photo_with_embedding(db, fid, "m.jpg", emb_768, variant=None)
    # Legacy 1024-dim embedding, NULL variant
    emb_1024 = np.ones(1024, dtype=np.float32) / np.sqrt(1024)
    pid_mismatch = _add_photo_with_embedding(db, fid, "x.jpg", emb_1024, variant=None)

    cfg = {"pipeline": {"dinov2_variant": "vit-b14"}}
    photos = load_photo_features(db, config=cfg)
    by_id = {p["id"]: p for p in photos}

    assert by_id[pid_match]["dino_subject_embedding"] is not None
    assert by_id[pid_mismatch]["dino_subject_embedding"] is None


def test_load_photo_features_no_variant_configured_keeps_embeddings(tmp_path):
    """If config doesn't specify a variant, behavior is unchanged (backward compat for tests)."""
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    emb = np.ones(768, dtype=np.float32) / np.sqrt(768)
    pid = _add_photo_with_embedding(db, fid, "a.jpg", emb, variant=None)

    photos = load_photo_features(db)  # no config
    assert len(photos) == 1
    assert photos[0]["dino_subject_embedding"] is not None


# ---------------------------------------------------------------------------
# Eye-focus detection stage
# ---------------------------------------------------------------------------

def _setup_eligible_mammal_photo(tmp_path, taxonomy_class="Mammalia"):
    """Create one photo that passes gate 1 + has a mask + has a detection.

    Returns (db, photo_id). Caller controls whether weights exist (gate 2)
    via monkeypatching keypoints.MODELS_DIR.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id  # auto-created Default workspace
    fid = db.add_folder(str(tmp_path), name="photos")
    db.add_workspace_folder(ws_id, fid)

    pid = db.add_photo(
        fid,
        "mammal.jpg",
        ".jpg",
        1000,
        1.0,
        timestamp="2026-04-16T10:00:00",
        width=800,
        height=600,
    )
    db.update_photo_pipeline_features(pid, mask_path=str(tmp_path / "mask.png"))

    det_ids = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8}, "confidence": 0.95}],
        detector_model="MegaDetector",
    )
    db.add_prediction(
        det_ids[0],
        species="Vulpes vulpes",
        confidence=0.92,
        model="bioclip-2.5",
        category="match",
        taxonomy={
            "kingdom": "Animalia",
            "phylum": "Chordata",
            "class": taxonomy_class,
            "order": "Carnivora",
            "family": "Canidae",
            "genus": "Vulpes",
            "scientific_name": "Vulpes vulpes",
        },
    )
    return db, pid


def test_eye_keypoint_stage_preflight_disabled(tmp_path, monkeypatch):
    """Preflight returns a skip reason when eye detection is disabled."""
    from pipeline import eye_keypoint_stage_preflight

    assert eye_keypoint_stage_preflight({"eye_detect_enabled": False}) \
        == "Disabled in config"


def test_eye_keypoint_stage_preflight_enabled(tmp_path, monkeypatch):
    """Preflight returns None when eye detection is enabled. Weights presence
    is no longer a preflight gate — pipeline_job.eye_keypoints_stage
    auto-downloads them at run time, so detect_eye_keypoints_stage relies
    on the per-photo defensive check rather than a stage-level guard.
    """
    from pipeline import eye_keypoint_stage_preflight

    assert eye_keypoint_stage_preflight({"eye_detect_enabled": True}) is None
    assert eye_keypoint_stage_preflight({}) is None


def test_eye_keypoint_stage_writes_nothing_when_weights_absent(
    tmp_path, monkeypatch,
):
    """When detect_eye_keypoints_stage is invoked without weights on disk
    (e.g. someone bypasses pipeline_job's auto-download), the per-photo
    Gate 2 check skips writing eye_* — no records get partial state.
    """
    import keypoints as kp
    from pipeline import detect_eye_keypoints_stage

    empty_models_dir = tmp_path / "empty_models"
    empty_models_dir.mkdir()
    monkeypatch.setattr(kp, "MODELS_DIR", str(empty_models_dir))

    db, pid = _setup_eligible_mammal_photo(tmp_path)

    detect_eye_keypoints_stage(db, config={"eye_detect_enabled": True})

    row = db.conn.execute(
        "SELECT eye_x, eye_y, eye_conf, eye_tenengrad FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert (row[0], row[1], row[2], row[3]) == (None, None, None, None)


def test_eye_keypoint_stage_respects_eye_detect_enabled_flag(tmp_path, monkeypatch):
    """When config disables eye detection, the stage does not enumerate photos."""
    from pipeline import detect_eye_keypoints_stage

    db, pid = _setup_eligible_mammal_photo(tmp_path)

    # Any call through to list_photos_for_eye_keypoint_stage would be a bug —
    # the disabled flag must short-circuit before the DB helper.
    called = {"listed": False}
    orig = db.list_photos_for_eye_keypoint_stage

    def _spy(*args, **kwargs):
        called["listed"] = True
        return orig(*args, **kwargs)

    monkeypatch.setattr(db, "list_photos_for_eye_keypoint_stage", _spy)

    detect_eye_keypoints_stage(db, config={"eye_detect_enabled": False})

    assert called["listed"] is False
    row = db.conn.execute(
        "SELECT eye_x, eye_y, eye_conf, eye_tenengrad FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert (row[0], row[1], row[2], row[3]) == (None, None, None, None)


def test_eye_keypoint_stage_scopes_to_collection(tmp_path, monkeypatch):
    """When ``collection_id`` is provided, only photos in that collection
    are passed to ``list_photos_for_eye_keypoint_stage``. A pipeline run
    targeted at one collection must not touch eye fields on unrelated
    photos elsewhere in the workspace.
    """
    import keypoints as kp
    from pipeline import detect_eye_keypoints_stage

    # Install fake routable weights so the stage-level preflight doesn't
    # short-circuit before the scoping check.
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    _make_fake_weights(str(models_dir), "superanimal-quadruped")
    _make_fake_weights(str(models_dir), "superanimal-bird")
    monkeypatch.setattr(kp, "MODELS_DIR", str(models_dir))

    db, pid = _setup_eligible_mammal_photo(tmp_path)

    # Add a second eligible photo not in the collection.
    fid = db.add_folder(str(tmp_path / "other"), name="other")
    db.add_workspace_folder(db._active_workspace_id, fid)
    other_pid = db.add_photo(
        fid, "mammal2.jpg", ".jpg", 1000, 2.0,
        timestamp="2026-04-16T11:00:00", width=800, height=600,
    )
    db.update_photo_pipeline_features(other_pid, mask_path=str(tmp_path / "mask.png"))
    det_ids = db.save_detections(
        other_pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8}, "confidence": 0.9}],
        detector_model="MegaDetector",
    )
    db.add_prediction(
        det_ids[0], species="Vulpes vulpes", confidence=0.9,
        model="bioclip-2.5", category="match",
        taxonomy={"class": "Mammalia", "scientific_name": "Vulpes vulpes"},
    )

    cid = db.add_collection(
        "fox set",
        json.dumps([{"field": "photo_ids", "value": [pid]}]),
    )

    captured = {}
    orig = db.list_photos_for_eye_keypoint_stage

    def _spy(photo_ids=None):
        captured["photo_ids"] = (
            set(photo_ids) if photo_ids is not None else None
        )
        return orig(photo_ids=photo_ids)

    monkeypatch.setattr(db, "list_photos_for_eye_keypoint_stage", _spy)

    detect_eye_keypoints_stage(
        db, config={"eye_detect_enabled": True}, collection_id=cid,
    )

    assert captured["photo_ids"] == {pid}


def test_eye_keypoint_stage_honors_exclude_photo_ids(tmp_path, monkeypatch):
    """When ``exclude_photo_ids`` is provided, excluded photos are filtered
    out before the eligibility query so the stage doesn't mutate eye_*
    fields for photos the user deselected in preview.
    """
    import keypoints as kp
    from pipeline import detect_eye_keypoints_stage

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    _make_fake_weights(str(models_dir), "superanimal-quadruped")
    _make_fake_weights(str(models_dir), "superanimal-bird")
    monkeypatch.setattr(kp, "MODELS_DIR", str(models_dir))

    db, pid = _setup_eligible_mammal_photo(tmp_path)

    fid = db.add_folder(str(tmp_path / "other"), name="other")
    db.add_workspace_folder(db._active_workspace_id, fid)
    other_pid = db.add_photo(
        fid, "mammal2.jpg", ".jpg", 1000, 2.0,
        timestamp="2026-04-16T11:00:00", width=800, height=600,
    )
    db.update_photo_pipeline_features(other_pid, mask_path=str(tmp_path / "mask.png"))
    det_ids = db.save_detections(
        other_pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8}, "confidence": 0.9}],
        detector_model="MegaDetector",
    )
    db.add_prediction(
        det_ids[0], species="Vulpes vulpes", confidence=0.9,
        model="bioclip-2.5", category="match",
        taxonomy={"class": "Mammalia", "scientific_name": "Vulpes vulpes"},
    )

    cid = db.add_collection(
        "both",
        json.dumps([{"field": "photo_ids", "value": [pid, other_pid]}]),
    )

    captured = {}
    orig = db.list_photos_for_eye_keypoint_stage

    def _spy(photo_ids=None):
        captured["photo_ids"] = (
            set(photo_ids) if photo_ids is not None else None
        )
        return orig(photo_ids=photo_ids)

    monkeypatch.setattr(db, "list_photos_for_eye_keypoint_stage", _spy)

    detect_eye_keypoints_stage(
        db, config={"eye_detect_enabled": True}, collection_id=cid,
        exclude_photo_ids={other_pid},
    )

    assert captured["photo_ids"] == {pid}


# --- Four-gate matrix tests for detect_eye_keypoints_stage ---


def _make_fake_weights(models_dir, model_name):
    """Create model.onnx + config.json under MODELS_DIR/<name>/.

    Contents are placeholders; _load_session is monkeypatched elsewhere
    so the files only exist to pass the gate-2 presence check.
    """
    target = os.path.join(models_dir, model_name)
    os.makedirs(target, exist_ok=True)
    with open(os.path.join(target, "model.onnx"), "wb") as f:
        f.write(b"fake")
    with open(os.path.join(target, "config.json"), "w") as f:
        f.write("{}")


def _setup_eligible_mammal_with_files(tmp_path, *, classifier_conf=0.92,
                                       taxonomy_class="Mammalia",
                                       img_size=(800, 600)):
    """Extended fixture: writes a real image and mask PNG to disk.

    Fake SuperAnimal-Quadruped weights are placed under a tmp MODELS_DIR
    so gate 2 passes. Caller monkeypatches kp.MODELS_DIR to that location
    and kp.detect_keypoints to control the eye the stage will evaluate.
    """
    from db import Database
    from PIL import Image

    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id
    fid = db.add_folder(str(tmp_path), name="photos")
    db.add_workspace_folder(ws_id, fid)

    img_w, img_h = img_size
    img = Image.new("RGB", (img_w, img_h), color=(128, 128, 128))
    # High-contrast edge block so compute_eye_tenengrad returns >0 when it
    # runs on a window inside the block.
    arr = np.array(img)
    arr[200:400, 200:400] = np.tile([0, 255] * 100, (200, 1)).astype(np.uint8)[:, :, None]
    Image.fromarray(arr).save(tmp_path / "mammal.jpg")

    # Mask PNG — white where the subject is (center region), black elsewhere.
    mask = np.zeros((img_h, img_w), dtype=np.uint8)
    mask[100:500, 100:700] = 255
    Image.fromarray(mask).save(tmp_path / "mask.png")

    pid = db.add_photo(
        fid, "mammal.jpg", ".jpg", 1000, 1.0,
        timestamp="2026-04-16T10:00:00",
        width=img_w, height=img_h,
    )
    db.update_photo_pipeline_features(pid, mask_path=str(tmp_path / "mask.png"))

    det_ids = db.save_detections(
        pid,
        [{"box": {"x": 0.125, "y": 0.167, "w": 0.75, "h": 0.667},
          "confidence": 0.95}],
        detector_model="MegaDetector",
    )
    db.add_prediction(
        det_ids[0],
        species="Vulpes vulpes",
        confidence=classifier_conf,
        model="bioclip-2.5",
        category="match",
        taxonomy={
            "kingdom": "Animalia", "phylum": "Chordata",
            "class": taxonomy_class, "order": "Carnivora",
            "family": "Canidae", "genus": "Vulpes",
            "scientific_name": "Vulpes vulpes",
        },
    )

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    _make_fake_weights(str(models_dir), "superanimal-quadruped")
    _make_fake_weights(str(models_dir), "superanimal-bird")

    return db, pid, str(models_dir)


def _read_eye_fields(db, pid):
    row = db.conn.execute(
        "SELECT eye_x, eye_y, eye_conf, eye_tenengrad FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    return (row[0], row[1], row[2], row[3])


def test_eye_stage_gate1_low_classifier_conf_no_write(tmp_path, monkeypatch):
    """Gate 1: classifier conf below threshold → no keypoint model is even loaded."""
    import keypoints as kp
    from pipeline import detect_eye_keypoints_stage

    db, pid, models_dir = _setup_eligible_mammal_with_files(
        tmp_path, classifier_conf=0.3,
    )
    monkeypatch.setattr(kp, "MODELS_DIR", models_dir)

    # If the stage reaches keypoint detection, the test setup is broken.
    def _boom(*a, **kw):
        raise AssertionError("detect_keypoints should not run when gate 1 fails")

    monkeypatch.setattr(kp, "detect_keypoints", _boom)

    detect_eye_keypoints_stage(
        db, config={"eye_detect_enabled": True, "eye_classifier_conf_gate": 0.5},
    )
    assert _read_eye_fields(db, pid) == (None, None, None, None)


def test_eye_stage_gate1_out_of_scope_species_no_write(tmp_path, monkeypatch):
    """Gate 1: species class not in {Aves, Mammalia} → no write."""
    import keypoints as kp
    from pipeline import detect_eye_keypoints_stage

    db, pid, models_dir = _setup_eligible_mammal_with_files(
        tmp_path, taxonomy_class="Actinopterygii",  # ray-finned fish
    )
    monkeypatch.setattr(kp, "MODELS_DIR", models_dir)

    def _boom(*a, **kw):
        raise AssertionError("detect_keypoints should not run for fish")

    monkeypatch.setattr(kp, "detect_keypoints", _boom)

    detect_eye_keypoints_stage(db, config={"eye_detect_enabled": True})
    assert _read_eye_fields(db, pid) == (None, None, None, None)


def test_eye_stage_gate3_low_eye_conf_no_write(tmp_path, monkeypatch):
    """Gate 3: both eye keypoints below eye_detection_conf_gate → no write."""
    import keypoints as kp
    from pipeline import detect_eye_keypoints_stage

    db, pid, models_dir = _setup_eligible_mammal_with_files(tmp_path)
    monkeypatch.setattr(kp, "MODELS_DIR", models_dir)

    low = [
        {"name": "left_eye", "x": 300.0, "y": 300.0, "conf": 0.2},
        {"name": "right_eye", "x": 350.0, "y": 300.0, "conf": 0.15},
        {"name": "nose", "x": 325.0, "y": 350.0, "conf": 0.9},
    ]
    monkeypatch.setattr(kp, "detect_keypoints", lambda *a, **kw: low)

    detect_eye_keypoints_stage(
        db, config={"eye_detect_enabled": True, "eye_detection_conf_gate": 0.5},
    )
    assert _read_eye_fields(db, pid) == (None, None, None, None)


def test_eye_stage_gate4_eye_outside_mask_no_write(tmp_path, monkeypatch):
    """Gate 4: eye keypoint falls outside the subject mask → no write."""
    import keypoints as kp
    from pipeline import detect_eye_keypoints_stage

    db, pid, models_dir = _setup_eligible_mammal_with_files(tmp_path)
    monkeypatch.setattr(kp, "MODELS_DIR", models_dir)

    # Mask covers [100:500, 100:700]; (50, 50) is well outside it.
    outside = [
        {"name": "left_eye", "x": 50.0, "y": 50.0, "conf": 0.9},
        {"name": "right_eye", "x": 60.0, "y": 50.0, "conf": 0.9},
    ]
    monkeypatch.setattr(kp, "detect_keypoints", lambda *a, **kw: outside)

    detect_eye_keypoints_stage(db, config={"eye_detect_enabled": True})
    assert _read_eye_fields(db, pid) == (None, None, None, None)


def test_eye_stage_all_gates_pass_writes_eye_fields(tmp_path, monkeypatch):
    """All four gates pass → eye_x, eye_y, eye_conf, eye_tenengrad populated."""
    import keypoints as kp
    from pipeline import detect_eye_keypoints_stage

    db, pid, models_dir = _setup_eligible_mammal_with_files(tmp_path)
    monkeypatch.setattr(kp, "MODELS_DIR", models_dir)

    # Eye at (300, 300) is inside mask [100:500, 100:700] and inside the
    # high-contrast block [200:400, 200:400] so tenengrad > 0.
    good = [
        {"name": "left_eye", "x": 300.0, "y": 300.0, "conf": 0.88},
        {"name": "right_eye", "x": 350.0, "y": 300.0, "conf": 0.85},
        {"name": "nose", "x": 325.0, "y": 350.0, "conf": 0.9},
    ]
    monkeypatch.setattr(kp, "detect_keypoints", lambda *a, **kw: good)

    detect_eye_keypoints_stage(db, config={"eye_detect_enabled": True})

    eye_x, eye_y, eye_conf, eye_teng = _read_eye_fields(db, pid)
    assert eye_x is not None
    assert eye_y is not None
    assert eye_conf is not None and eye_conf >= 0.5
    assert eye_teng is not None and eye_teng > 0.0


def test_eye_stage_uses_image_loader_and_tolerates_none(tmp_path, monkeypatch):
    """Stage calls image_loader.load_image (which handles RAW + EXIF
    orientation) rather than plain PIL.Image.open. If the loader returns
    None (unsupported format, decode failure), the stage skips the photo
    without raising.
    """
    import image_loader
    import keypoints as kp
    from pipeline import detect_eye_keypoints_stage

    db, pid, models_dir = _setup_eligible_mammal_with_files(tmp_path)
    monkeypatch.setattr(kp, "MODELS_DIR", models_dir)

    calls = {"n": 0}

    def _fake_loader(path, max_size=1024):
        calls["n"] += 1
        return None

    monkeypatch.setattr("pipeline.load_image", _fake_loader, raising=False)
    # Also patch in the module where pipeline imports it, in case the
    # function binds at call time via `from image_loader import load_image`.
    monkeypatch.setattr(image_loader, "load_image", _fake_loader)

    def _boom(*a, **kw):
        raise AssertionError("detect_keypoints should not run when image load fails")

    monkeypatch.setattr(kp, "detect_keypoints", _boom)

    detect_eye_keypoints_stage(db, config={"eye_detect_enabled": True})

    assert calls["n"] >= 1
    assert _read_eye_fields(db, pid) == (None, None, None, None)


def test_eye_stage_picks_eye_with_higher_tenengrad(tmp_path, monkeypatch):
    """Two valid eyes → pick the one with the higher windowed tenengrad.

    The high-contrast block covers [200:400, 200:400]. A window around
    (300, 300) lands fully inside it; a window around (600, 300) lands on
    flat gray. The stage must persist the (300, 300) eye.
    """
    import keypoints as kp
    from pipeline import detect_eye_keypoints_stage

    db, pid, models_dir = _setup_eligible_mammal_with_files(tmp_path)
    monkeypatch.setattr(kp, "MODELS_DIR", models_dir)

    two_eyes = [
        {"name": "left_eye", "x": 300.0, "y": 300.0, "conf": 0.80},  # sharp region
        {"name": "right_eye", "x": 600.0, "y": 300.0, "conf": 0.90}, # flat region (but higher conf)
    ]
    monkeypatch.setattr(kp, "detect_keypoints", lambda *a, **kw: two_eyes)

    detect_eye_keypoints_stage(db, config={"eye_detect_enabled": True})

    eye_x, eye_y, eye_conf, eye_teng = _read_eye_fields(db, pid)
    # Winner is chosen by tenengrad, not conf, so the sharp-region eye
    # wins. Coords are normalized 0-1 against the 800x600 fixture image.
    assert abs(eye_x - 300.0 / 800.0) < 1.0 / 800.0
    assert abs(eye_y - 300.0 / 600.0) < 1.0 / 600.0
    assert eye_conf == 0.80
    assert eye_teng > 0.0


# ---------------------------------------------------------------------------
# Read-time detection threshold filtering (Task 24)
# ---------------------------------------------------------------------------


def test_load_photo_features_honors_workspace_detector_threshold(tmp_path):
    """Lowering workspace `detector_confidence` surfaces more cached boxes at
    read time, without rewriting detection rows.

    Exercises the global-detections design: boxes are stored once globally;
    each workspace's view of subject crops / primary detections is filtered
    by its effective `detector_confidence` override at read time.
    """
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    ws_id = db._active_workspace_id
    fid = db.add_folder(str(tmp_path), name="photos")
    db.add_workspace_folder(ws_id, fid)

    pid = db.add_photo(
        fid, "bird.jpg", ".jpg", 1000, 1.0,
        timestamp="2026-04-23T10:00:00",
        width=4000, height=3000,
    )

    # Save two boxes globally: one high-conf (surfaces at default threshold),
    # one low-conf (only surfaces when the workspace lowers its threshold).
    db.save_detections(pid, [
        {"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
         "confidence": 0.05, "category": "animal"},
        {"box": {"x": 0.5, "y": 0.5, "w": 0.3, "h": 0.3},
         "confidence": 0.95, "category": "animal"},
    ], detector_model="MDV6")

    # At the default workspace threshold (0.2), only the high-conf box wins
    # as primary detection.
    photos = load_photo_features(db)
    assert len(photos) == 1
    primary = photos[0]["detection_box"]
    assert primary is not None
    assert primary["x"] == 0.5
    assert photos[0]["detection_conf"] == 0.95

    # Lower the workspace threshold via per-workspace config override so the
    # low-conf box becomes visible. Primary is still the high-conf one
    # (helper sorts by confidence DESC), but if we drop the high-conf box
    # the low-conf one now surfaces — proving the read-time filter actually
    # changed behavior without any detection-row writes.
    db.update_workspace(ws_id,
                        config_overrides={"detector_confidence": 0.01})

    # Delete just the high-conf detection to confirm the low-conf one now
    # becomes primary under the lowered threshold.
    db.conn.execute(
        "DELETE FROM detections WHERE photo_id = ? AND detector_confidence = ?",
        (pid, 0.95),
    )
    db.conn.commit()

    photos = load_photo_features(db)
    assert len(photos) == 1
    primary = photos[0]["detection_box"]
    assert primary is not None, (
        "lowering detector_confidence should surface the cached low-conf box"
    )
    assert primary["x"] == 0.1
    assert photos[0]["detection_conf"] == 0.05

    # Raise the threshold back above 0.05 — the low-conf box disappears
    # again, purely through read-time filtering.
    db.update_workspace(ws_id,
                        config_overrides={"detector_confidence": 0.5})
    photos = load_photo_features(db)
    assert photos[0]["detection_box"] is None
    assert photos[0]["detection_conf"] is None

    # And the raw row count never changed (no rewrites).
    raw = db.conn.execute(
        "SELECT COUNT(*) FROM detections WHERE photo_id = ?", (pid,),
    ).fetchone()[0]
    assert raw == 1
