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

            # Add a species prediction (references detection, not photo).
            # Stamp taxonomy_class so the prediction routes through the
            # eye-keypoint stage's primary path — without it, attemptable
            # photo counts collapse to zero and readiness assertions about
            # the eye_keypoints enhancing gap stop firing.
            species = "robin" if enc_idx == 0 else "eagle"
            db.add_prediction(
                det_ids[0], species, 0.9 - i * 0.05, "bioclip",
                category="match",
                taxonomy={"class": "Aves"},
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


def test_load_photo_features_subject_absent_from_current_detections(tmp_path):
    """subject_absent must be derived from this run's detections vs the
    current detector_confidence threshold — NOT from the stored
    miss_no_subject column. Regroup runs before miss in the pipeline, so
    miss_no_subject reflects the *previous* run (or NULL on first run);
    relying on it would make the asymmetric-no-subject penalty lag by a
    run and go stale on threshold changes.
    """
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    # Photo A: real detection at conf 0.9 (well above default 0.2 threshold).
    # write_detection_batch records both the detection rows AND a
    # detector_runs row, marking the detector as "having run" on this photo.
    pid_a = db.add_photo(fid, "a.jpg", ".jpg", 100, 1.0)
    db.write_detection_batch(pid_a, "megadetector-v6", [
        {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.9},
    ])
    # Stamp stale miss flags from a hypothetical prior run that would
    # WRONGLY mark this photo as no-subject. The current-detection
    # derivation must ignore them.
    db.conn.execute(
        "UPDATE photos SET miss_no_subject=1, "
        "miss_computed_at='2026-01-01T00:00:00' WHERE id=?",
        (pid_a,),
    )

    # Photo B: detector ran (detector_runs row written) but only produced
    # sub-threshold detections (the 1761 case from apr2026). Empty boxes
    # would also work, but a sub-threshold row matches the actual apr2026
    # state where the detector emitted low-confidence garbage.
    pid_b = db.add_photo(fid, "b.jpg", ".jpg", 100, 1.0)
    db.write_detection_batch(pid_b, "megadetector-v6", [
        {"box": {"x": 0.2, "y": 0.2, "w": 0.05, "h": 0.05}, "confidence": 0.03},
    ])
    # Conversely, leave miss flags NULL on B — this is what the very first
    # pipeline run on a new workspace looks like. The new derivation must
    # still mark B subject_absent based on the live detection check.
    db.conn.commit()

    photos = load_photo_features(db)
    by_id = {p["id"]: p for p in photos}

    assert by_id[pid_a]["subject_absent"] is False, (
        "stale miss_no_subject=1 must not override a live high-conf detection"
    )
    assert by_id[pid_b]["subject_absent"] is True, (
        "no detection above threshold must mark subject_absent=True even "
        "when miss_computed_at IS NULL"
    )


def test_load_photo_features_subject_absent_false_when_detector_never_ran(tmp_path):
    """A photo with no detector_runs row hasn't had the detector run yet
    (e.g. first-time regroup with skip_classify=True, or newly imported
    photos before classify/detect). Its subject state is *unknown*, not
    "detector confirmed empty". subject_absent must be False so
    compute_s_enc drops the signal and renormalizes — matching the
    uncomputed-features semantics. Gating on subject_absent=True here
    would create false hard cuts and fragment encounters.
    """
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    # Photo C: no detections, no detector_runs row — pristine "never run".
    pid_c = db.add_photo(fid, "c.jpg", ".jpg", 100, 1.0)
    db.conn.commit()

    photos = load_photo_features(db)
    by_id = {p["id"]: p for p in photos}

    assert by_id[pid_c]["subject_absent"] is False, (
        "photo with no detector_runs row must not be marked subject_absent — "
        "the detector hasn't run yet, so we have no evidence either way"
    )


def test_load_photo_features_subject_absent_ignores_non_mdv6_detections(tmp_path):
    """`subject_absent` must be derived against `megadetector-v6` only.
    Synthetic `full-image` fallback rows are written at confidence 0 by
    classify_job when MDV6 finds nothing, and the user can lower
    `detector_confidence` to 0.0 (allowed by schema). If load_photo_features
    matches *any* detector model when checking "has a passing detection",
    those full-image rows would mask MDV6's true empty-scene verdict and
    suppress the no-subject cut signal.
    """
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    # Lower the workspace's detector_confidence to 0.0 so a confidence=0
    # synthetic full-image row WOULD pass the threshold if not filtered.
    ws_id = db._active_workspace_id
    db.update_workspace(ws_id, config_overrides={"detector_confidence": 0.0})

    pid = db.add_photo(fid, "x.jpg", ".jpg", 100, 1.0)
    # MDV6: empty-scene run (canonical "detector confirmed empty")
    db.write_detection_batch(pid, "megadetector-v6", [])
    # Synthetic full-image fallback at conf 0 (mirrors classify_job)
    db.write_detection_batch(pid, "full-image", [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.0},
    ])
    db.conn.commit()

    photos = load_photo_features(db)
    assert photos[0]["subject_absent"] is True, (
        "MDV6's empty-scene verdict must not be masked by synthetic "
        "full-image fallback rows from a different detector model"
    )
    assert photos[0]["subject_present"] is False


def test_load_photo_features_subject_present_when_mdv6_passes_threshold(tmp_path):
    """`subject_present=True` iff MDV6 has a detection passing the
    workspace's effective threshold. Used by compute_s_enc to gate the
    asymmetric-no-subject penalty so it only fires when the non-absent
    side has affirmative subject evidence.
    """
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    pid = db.add_photo(fid, "duck.jpg", ".jpg", 100, 1.0)
    db.write_detection_batch(pid, "megadetector-v6", [
        {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.9},
    ])
    db.conn.commit()

    photos = load_photo_features(db)
    assert photos[0]["subject_present"] is True
    assert photos[0]["subject_absent"] is False


def test_load_photo_features_subject_present_false_when_detector_unrun(tmp_path):
    """A photo the detector hasn't seen has subject_present=False — the
    state is "unknown", neither absent nor present. compute_s_enc uses
    this to drop subj/species rather than penalize.
    """
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    pid = db.add_photo(fid, "fresh.jpg", ".jpg", 100, 1.0)
    db.conn.commit()

    photos = load_photo_features(db)
    assert photos[0]["subject_present"] is False
    assert photos[0]["subject_absent"] is False


def test_load_photo_features_subject_absent_true_for_empty_scene_run(tmp_path):
    """Empty-scene runs (detector ran, found zero boxes) must be
    subject_absent=True. write_detection_batch records a detector_runs
    row with box_count=0 even when no boxes are passed — that's the
    canonical "ran and confirmed empty" state.
    """
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    pid = db.add_photo(fid, "empty.jpg", ".jpg", 100, 1.0)
    db.write_detection_batch(pid, "megadetector-v6", [])  # empty scene
    db.conn.commit()

    photos = load_photo_features(db)
    assert photos[0]["subject_absent"] is True, (
        "empty-scene detector run (box_count=0) is the canonical "
        "subject_absent=True signal"
    )


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


def test_run_species_review_pipeline_labels_all_review_without_scores(tmp_path):
    """Identify-only results support review without pretending to cull."""
    from pipeline import (
        load_photo_features,
        run_species_review_pipeline,
        serialize_results,
    )

    db, ids = _setup_db_with_photos(tmp_path)
    photos = load_photo_features(db)

    results = run_species_review_pipeline(photos)
    serialized = serialize_results(results)

    assert serialized["review_mode"] == "species"
    assert serialized["summary"]["total_photos"] == 6
    assert serialized["summary"]["review_count"] == 6
    assert serialized["summary"]["keep_count"] == 0
    assert serialized["summary"]["reject_count"] == 0
    assert {p["label"] for p in serialized["photos"]} == {"REVIEW"}
    assert all("quality_composite" not in p for p in serialized["photos"])
    assert serialized["encounters"][0]["species_predictions"]


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


def test_load_results_quarantines_corrupt_cache(tmp_path):
    """A malformed cache should not crash request handlers that read it."""
    from pipeline import load_results, load_results_raw

    path = tmp_path / "pipeline_results_ws1.json"
    path.write_text('{"photos": [], "encounters": [{"photo_ids"')

    assert load_results(str(tmp_path), workspace_id=1) is None
    assert not path.exists()
    backups = list(tmp_path.glob("pipeline_results_ws1.json.corrupt-*"))
    assert len(backups) == 1
    assert backups[0].read_text().startswith('{"photos"')

    # Subsequent raw reads see a missing cache, not the original JSON error.
    assert load_results_raw(str(tmp_path), workspace_id=1) is None


def test_save_results_raw_replaces_corrupt_cache_atomically(tmp_path):
    """Saving raw results should replace an unusable cache with valid JSON."""
    from pipeline import load_results_raw, save_results_raw

    path = tmp_path / "pipeline_results_ws1.json"
    path.write_text('{"photos": [')

    saved = save_results_raw({"photos": [], "encounters": []}, str(tmp_path), 1)

    assert saved == str(path)
    loaded = load_results_raw(str(tmp_path), workspace_id=1)
    assert loaded["photos"] == []
    assert loaded["encounters"] == []
    assert loaded["summary"]["total_photos"] == 0
    assert loaded["summary"]["encounter_count"] == 0
    assert loaded["summary"]["confirmed_count"] == 0
    assert loaded["summary"]["unconfirmed_count"] == 0
    assert list(tmp_path.glob("*.tmp")) == []


# -- prune_results --


def _write_cache(cache_dir, workspace_id, data):
    path = os.path.join(cache_dir, f"pipeline_results_ws{workspace_id}.json")
    with open(path, "w") as f:
        json.dump(data, f)
    return path


def _sample_cache():
    return {
        "encounters": [
            {
                "species": "American Robin",
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 3,
                "burst_count": 2,
                "time_range": ["2026-03-20T10:00:00", "2026-03-20T10:00:04"],
                "photo_ids": [1, 2, 3],
                "bursts": [
                    {"photo_ids": [1, 2], "species_predictions": [], "species_override": {"species": "American Robin", "confirmed": True}},
                    {"photo_ids": [3], "species_predictions": [], "species_override": None},
                ],
            },
            {
                "species": "Blue Jay",
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": 2,
                "burst_count": 1,
                "time_range": ["2026-03-20T10:05:00", "2026-03-20T10:05:02"],
                "photo_ids": [4, 5],
                "bursts": [
                    {"photo_ids": [4, 5], "species_predictions": [], "species_override": None},
                ],
            },
        ],
        "photos": [
            {"id": 1, "label": "KEEP", "rarity_protected": False},
            {"id": 2, "label": "REJECT", "rarity_protected": False},
            {"id": 3, "label": "REVIEW", "rarity_protected": True},
            {"id": 4, "label": "KEEP", "rarity_protected": False},
            {"id": 5, "label": "REJECT", "rarity_protected": False},
        ],
        "summary": {
            "total_photos": 5,
            "encounter_count": 2,
            "burst_count": 3,
            "keep_count": 2,
            "review_count": 1,
            "reject_count": 2,
            "rarity_protected": 1,
            "confirmed_count": 1,
            "unconfirmed_count": 2,
        },
    }


def test_prune_results_removes_deleted_ids(tmp_path):
    """Deleted photo IDs are stripped from photos and encounter/burst photo_ids."""
    from pipeline import load_results, prune_results

    _write_cache(str(tmp_path), 1, _sample_cache())
    changed = prune_results(str(tmp_path), 1, [2, 5])

    assert changed is True
    loaded = load_results(str(tmp_path), 1)
    assert [p["id"] for p in loaded["photos"]] == [1, 3, 4]
    assert loaded["encounters"][0]["photo_ids"] == [1, 3]
    assert loaded["encounters"][1]["photo_ids"] == [4]
    assert loaded["encounters"][0]["bursts"][0]["photo_ids"] == [1]
    assert loaded["encounters"][0]["bursts"][1]["photo_ids"] == [3]
    assert loaded["encounters"][1]["bursts"][0]["photo_ids"] == [4]


def test_prune_results_drops_empty_encounters_and_bursts(tmp_path):
    """An encounter (or burst) whose every photo was deleted is dropped."""
    from pipeline import load_results, prune_results

    _write_cache(str(tmp_path), 1, _sample_cache())
    # Delete the entire second encounter and the second burst of the first.
    prune_results(str(tmp_path), 1, [3, 4, 5])

    loaded = load_results(str(tmp_path), 1)
    assert len(loaded["encounters"]) == 1
    assert loaded["encounters"][0]["photo_ids"] == [1, 2]
    assert len(loaded["encounters"][0]["bursts"]) == 1
    assert loaded["encounters"][0]["bursts"][0]["photo_ids"] == [1, 2]


def test_prune_results_recomputes_counts(tmp_path):
    """photo_count, burst_count, and summary are recomputed from survivors."""
    from pipeline import load_results, prune_results

    _write_cache(str(tmp_path), 1, _sample_cache())
    prune_results(str(tmp_path), 1, [2, 5])

    loaded = load_results(str(tmp_path), 1)
    assert loaded["encounters"][0]["photo_count"] == 2
    assert loaded["encounters"][0]["burst_count"] == 2
    assert loaded["encounters"][1]["photo_count"] == 1
    assert loaded["encounters"][1]["burst_count"] == 1

    summary = loaded["summary"]
    assert summary["total_photos"] == 3
    assert summary["encounter_count"] == 2
    assert summary["burst_count"] == 3
    assert summary["keep_count"] == 2
    assert summary["review_count"] == 1
    assert summary["reject_count"] == 0
    assert summary["rarity_protected"] == 1
    assert summary["confirmed_count"] == 1
    assert summary["unconfirmed_count"] == 2


def test_prune_results_no_overlap_returns_false(tmp_path):
    """If no deleted IDs intersect the cache, the file is unchanged."""
    from pipeline import prune_results

    path = _write_cache(str(tmp_path), 1, _sample_cache())
    mtime = os.path.getmtime(path)

    changed = prune_results(str(tmp_path), 1, [99, 100])

    assert changed is False
    assert os.path.getmtime(path) == mtime


def test_prune_results_missing_cache_is_noop(tmp_path):
    """No cache file → prune_results returns False, does not raise."""
    from pipeline import prune_results

    assert prune_results(str(tmp_path), 1, [1, 2, 3]) is False


def test_prune_results_empty_id_list(tmp_path):
    """Empty deleted-id list → no-op."""
    from pipeline import prune_results

    path = _write_cache(str(tmp_path), 1, _sample_cache())
    mtime = os.path.getmtime(path)

    assert prune_results(str(tmp_path), 1, []) is False
    assert os.path.getmtime(path) == mtime


# -- prune_missing_photos --


def _make_db_with_ids(tmp_path, ids, workspace_id=1, link_folder=True):
    """Create a minimal Database with photo rows whose ids match ``ids``.

    ``link_folder`` controls whether the photo folder is registered with
    ``workspace_id`` via ``workspace_folders``. ``add_folder`` auto-links
    to the active workspace, so when False we explicitly remove the link
    to simulate a folder that exists in the photos table but is not
    visible in the workspace.
    """
    from db import Database

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    if not link_folder:
        db.remove_workspace_folder(workspace_id, fid)
    for pid in ids:
        # add_photo auto-assigns id, so we override via raw SQL after
        # insertion to pin the row to the desired id.
        actual = db.add_photo(fid, f"p{pid}.jpg", ".jpg", 1000, 1.0)
        if actual != pid:
            db.conn.execute("UPDATE photos SET id=? WHERE id=?", (pid, actual))
    db.conn.commit()
    return db


def test_prune_missing_photos_drops_deleted_ids(tmp_path):
    """IDs in the cache but absent from the photos table are pruned."""
    from pipeline import load_results, prune_missing_photos

    _write_cache(str(tmp_path), 1, _sample_cache())
    # photos table has only ids {1, 3}; cache references {1..5}
    db = _make_db_with_ids(tmp_path, [1, 3])

    changed = prune_missing_photos(str(tmp_path), 1, db)
    assert changed is True

    loaded = load_results(str(tmp_path), 1)
    assert [p["id"] for p in loaded["photos"]] == [1, 3]
    assert loaded["encounters"][0]["photo_ids"] == [1, 3]
    # second encounter (ids 4, 5) is fully gone
    assert len(loaded["encounters"]) == 1


def test_prune_missing_photos_noop_when_all_present(tmp_path):
    """If every cached id exists in the DB, the cache is untouched."""
    from pipeline import prune_missing_photos

    path = _write_cache(str(tmp_path), 1, _sample_cache())
    mtime = os.path.getmtime(path)
    db = _make_db_with_ids(tmp_path, [1, 2, 3, 4, 5])

    changed = prune_missing_photos(str(tmp_path), 1, db)
    assert changed is False
    assert os.path.getmtime(path) == mtime


def test_prune_missing_photos_no_cache_is_noop(tmp_path):
    """No cache file → returns False, does not raise."""
    from pipeline import prune_missing_photos

    db = _make_db_with_ids(tmp_path, [1])
    assert prune_missing_photos(str(tmp_path), 999, db) is False


def test_prune_missing_photos_drops_ids_in_unlinked_folder(tmp_path):
    """Photos that exist in the table but whose folder isn't linked to
    the workspace are pruned just like hard-deleted rows. Without this,
    unlinking a folder leaves orphan cards on the review page and the
    thumbnail self-heal route still 404s because thumbnail source
    resolution is itself workspace-scoped."""
    from pipeline import load_results, prune_missing_photos

    _write_cache(str(tmp_path), 1, _sample_cache())
    # Photos 1..5 exist in the photos table, but their folder is NOT
    # linked to workspace 1 (link_folder=False), so the workspace can't
    # actually see any of them.
    db = _make_db_with_ids(tmp_path, [1, 2, 3, 4, 5], link_folder=False)

    changed = prune_missing_photos(str(tmp_path), 1, db)
    assert changed is True

    loaded = load_results(str(tmp_path), 1)
    assert loaded["photos"] == []
    assert loaded["encounters"] == []


def test_prune_missing_photos_chunks_large_id_lists(tmp_path):
    """Cache with more entries than SQLite's bound-parameter cap
    (``SQLITE_LIMIT_VARIABLE_NUMBER``, default 999 in production builds)
    must not raise ``OperationalError: too many SQL variables``. Without
    chunking, a large workspace would regress from a recoverable
    stale-cache state to a hard 500 on ``GET /api/pipeline/results``.

    The local ``sqlite3`` build often ships with a much higher default
    (e.g. 250k), which would mask the bug. Force the limit down with
    ``Connection.setlimit`` so the chunking path is actually exercised.
    """
    import sqlite3

    from pipeline import load_results, prune_missing_photos

    # 1500 cached ids — half present, half not.
    n = 1500
    present_ids = list(range(1, n // 2 + 1))
    missing_ids = list(range(n // 2 + 1, n + 1))
    cache = {
        "encounters": [
            {
                "species": "Robin",
                "confirmed_species": None,
                "species_predictions": [],
                "species_confirmed": False,
                "photo_count": n,
                "burst_count": 1,
                "time_range": ["2026-03-20T10:00:00", "2026-03-20T10:00:01"],
                "photo_ids": present_ids + missing_ids,
                "bursts": [
                    {"photo_ids": present_ids + missing_ids, "species_predictions": [], "species_override": None},
                ],
            },
        ],
        "photos": [
            {"id": pid, "label": "KEEP", "rarity_protected": False}
            for pid in present_ids + missing_ids
        ],
        "summary": {
            "total_photos": n,
            "encounter_count": 1,
            "burst_count": 1,
            "keep_count": n,
            "review_count": 0,
            "reject_count": 0,
            "rarity_protected": 0,
        },
    }
    _write_cache(str(tmp_path), 1, cache)
    db = _make_db_with_ids(tmp_path, present_ids)

    # Force the bound-parameter limit below n so the unchunked code path
    # would raise OperationalError. The chunking implementation uses 900
    # per query, so a cap of 999 lets each chunk through.
    db.conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)

    changed = prune_missing_photos(str(tmp_path), 1, db)
    assert changed is True

    loaded = load_results(str(tmp_path), 1)
    assert [p["id"] for p in loaded["photos"]] == present_ids


# -- compute_review_readiness --


def test_compute_review_readiness_empty_workspace(tmp_path):
    """No photos in the workspace → state='empty'."""
    from db import Database
    from pipeline import compute_review_readiness

    db = Database(str(tmp_path / "test.db"))
    out = compute_review_readiness(db)
    assert out["state"] == "empty"
    assert out["total_photos"] == 0
    assert out["missing_required"] == []
    assert out["enhancing_missing"] == []


def test_compute_review_readiness_no_masks(tmp_path):
    """Photos exist but none have masks → state='insufficient',
    'masks' listed in missing_required."""
    from db import Database
    from pipeline import compute_review_readiness

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    base_time = datetime(2026, 3, 20, 10, 0, 0)
    for i in range(5):
        ts = base_time + timedelta(seconds=i * 2)
        db.add_photo(
            fid, f"photo{i}.jpg", ".jpg", 1000, 1.0,
            timestamp=ts.isoformat(), width=4000, height=3000,
        )

    out = compute_review_readiness(db)
    assert out["state"] == "insufficient"
    assert "masks" in out["missing_required"]
    assert out["total_photos"] == 5
    assert out["with_masks"] == 0


def test_compute_review_readiness_masks_present_no_eye(tmp_path):
    """Masks present for all photos, no eye keypoints →
    state='computable', 'eye_keypoints' in enhancing_missing."""
    import config as cfg
    from pipeline import compute_review_readiness

    # Eye readiness gaps are only surfaced when the workspace opts into
    # eye detection (the default flipped off). Enable it explicitly so
    # this pins the "computable but enhancing inputs missing" contract
    # rather than the default-off suppression path.
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({"pipeline": {"eye_detect_enabled": True}})

    # _setup_db_with_photos populates masks, embeddings, and predictions
    # for every photo but does NOT set eye_x — exactly the "computable
    # but enhancing inputs missing" state we want to assert against.
    db, _ids = _setup_db_with_photos(tmp_path)
    out = compute_review_readiness(db)
    assert out["state"] == "computable"
    assert out["with_masks"] > 0
    assert out["with_masks"] == out["total_photos"]
    assert "eye_keypoints" in out["enhancing_missing"]


def test_compute_review_readiness_full_features(tmp_path):
    """All upstream features present including eye keypoints →
    state='computable', enhancing_missing empty."""
    from pipeline import compute_review_readiness

    db, ids = _setup_db_with_photos(tmp_path)
    # Backfill eye keypoints for every photo so coverage is 100%.
    for enc_ids in ids:
        for pid in enc_ids:
            db.update_photo_pipeline_features(
                pid,
                eye_x=0.5,
                eye_y=0.5,
                eye_conf=0.9,
                eye_tenengrad=200.0,
            )

    out = compute_review_readiness(db)
    assert out["state"] == "computable"
    assert out["enhancing_missing"] == []


def test_compute_review_readiness_at_mask_threshold_boundary(tmp_path):
    """Mask coverage exactly at the 25% threshold → state='computable'.

    Pins the contract that the comparison is strict ``<`` (not ``<=``):
    with 4 photos and 1 having a mask, coverage is 1/4 = 25%, which is
    *at* threshold and must classify as computable. Drop one mask (0/4)
    and the same workspace must classify as insufficient. If the
    comparison flips to ``<=``, the boundary case becomes insufficient
    and the first assertion below fails.
    """
    from db import Database
    from pipeline import compute_review_readiness

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    base_time = datetime(2026, 3, 20, 10, 0, 0)
    pids = []
    for i in range(4):
        ts = base_time + timedelta(seconds=i * 2)
        pid = db.add_photo(
            fid, f"photo{i}.jpg", ".jpg", 1000, 1.0,
            timestamp=ts.isoformat(), width=4000, height=3000,
        )
        pids.append(pid)

    # Exactly one mask out of four → 25% coverage, at the threshold.
    db.update_photo_pipeline_features(pids[0], mask_path=f"/masks/{pids[0]}.png")

    out = compute_review_readiness(db)
    assert out["state"] == "computable"
    assert out["total_photos"] == 4
    assert out["with_masks"] == 1
    assert "masks" not in out["missing_required"]
    # Fix 1 contract: when masks-partial would be redundant with a
    # required-masks block we suppress it, but here masks is NOT required
    # (we're at threshold) so the partial signal is informative and present.
    assert "masks_partial" in out["enhancing_missing"]

    # Asymmetric companion: drop the only mask → 0/4, below threshold.
    db.update_photo_pipeline_features(pids[0], mask_path=None)
    out_below = compute_review_readiness(db)
    assert out_below["state"] == "insufficient"
    assert out_below["with_masks"] == 0
    assert "masks" in out_below["missing_required"]
    # Fix 1 contract: when masks is in missing_required we must NOT
    # also surface the redundant masks_partial enhancing signal.
    assert "masks_partial" not in out_below["enhancing_missing"]


def test_compute_review_readiness_below_threshold_uses_ceiling(tmp_path):
    """Coverage strictly below the threshold must classify as insufficient
    even when ``int(total * threshold)`` would floor the required count
    down to a value the actual coverage happens to meet.

    Concrete case: 5 photos, 1 with a mask = 20% coverage. Against the
    default 25% threshold, ``int(5 * 0.25) == 1`` would let 1 mask pass;
    the ceiling form ``ceil(5 * 0.25) == 2`` correctly requires 2 and
    classifies the workspace as insufficient.
    """
    from db import Database
    from pipeline import compute_review_readiness

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    base_time = datetime(2026, 3, 20, 10, 0, 0)
    pids = []
    for i in range(5):
        ts = base_time + timedelta(seconds=i * 2)
        pid = db.add_photo(
            fid, f"photo{i}.jpg", ".jpg", 1000, 1.0,
            timestamp=ts.isoformat(), width=4000, height=3000,
        )
        pids.append(pid)

    # 1 mask out of 5 → 20% coverage, strictly below the 25% threshold.
    db.update_photo_pipeline_features(pids[0], mask_path=f"/masks/{pids[0]}.png")

    out = compute_review_readiness(db)
    assert out["state"] == "insufficient"
    assert out["with_masks"] == 1
    assert "masks" in out["missing_required"]


def test_compute_review_readiness_variant_aware_embeddings(tmp_path):
    """Switching DINOv2 variants must drop stale embeddings from coverage.

    Pins the contract that embedding readiness mirrors the variant rule
    in ``load_photo_features._embedding_usable``: a workspace whose
    embeddings were all written under ``vit-b14`` (768-dim) must report
    zero usable embeddings when readiness is asked for under ``vit-l14``
    (1024-dim), and the user-facing ``enhancing_missing`` must surface
    the gap so the degraded banner explains the silent quality drop.

    Without this, after a variant switch the readiness pane would claim
    full embedding coverage while ``/api/pipeline/regroup-live`` actually
    runs without a single usable embedding.
    """
    from pipeline import compute_review_readiness

    # _setup_db_with_photos writes 768-dim embeddings (vit-b14 shape) but
    # leaves dino_embedding_variant NULL via update_photo_embeddings.
    db, _ids = _setup_db_with_photos(tmp_path)

    # Same variant as the stored byte-length — NULL stored variant slips
    # through the dim-match branch, so coverage is full.
    out_match = compute_review_readiness(db, dinov2_variant="vit-b14")
    assert out_match["with_embeddings"] == out_match["total_photos"]
    assert "embeddings" not in out_match["enhancing_missing"]

    # Mismatched variant — 768-dim embeddings can't be reused as 1024-dim,
    # so they must NOT count as usable and must surface in the gap list.
    out_mismatch = compute_review_readiness(db, dinov2_variant="vit-l14")
    assert out_mismatch["with_embeddings"] == 0
    assert "embeddings" in out_mismatch["enhancing_missing"]

    # Stamp the variant explicitly: a row marked vit-b14 must NOT count
    # under a vit-l14 readiness check even though byte-length doesn't
    # match either — the explicit-variant branch wins.
    pid = _ids[0][0]
    db.conn.execute(
        "UPDATE photos SET dino_embedding_variant=? WHERE id=?",
        ("vit-b14", pid),
    )
    db.conn.commit()
    out_explicit_mismatch = compute_review_readiness(db, dinov2_variant="vit-l14")
    assert out_explicit_mismatch["with_embeddings"] == 0


def test_compute_review_readiness_ignores_no_subject_photos_for_enhancers(tmp_path):
    """No-subject photos should not make the review page claim masks,
    embeddings, or species predictions are incomplete.

    The detector can legitimately evaluate a workspace photo and find no
    above-threshold subject. Those photos are reviewable as subject-absent
    rejects, but rerunning SAM/DINO/species stages will not produce subject
    features for them.
    """
    from db import Database
    from dino_embed import embedding_to_blob
    from pipeline import compute_review_readiness

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    pids = [
        db.add_photo(
            fid, f"photo{i}.jpg", ".jpg", 1000, 1.0,
            timestamp=datetime(2026, 3, 20, 10, 0, i).isoformat(),
            width=4000, height=3000,
        )
        for i in range(4)
    ]
    emb = np.ones(768, dtype=np.float32)
    emb = emb / np.linalg.norm(emb)

    for pid in pids[:2]:
        det_ids = db.write_detection_batch(pid, "megadetector-v6", [
            {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.9},
        ])
        db.add_prediction(det_ids[0], "robin", 0.9, "bioclip", category="match")
        db.update_photo_pipeline_features(pid, mask_path=f"/masks/{pid}.png")
        db.update_photo_embeddings(
            pid,
            dino_subject_embedding=embedding_to_blob(emb),
            dino_global_embedding=embedding_to_blob(emb),
        )

    for pid in pids[2:]:
        db.write_detection_batch(pid, "megadetector-v6", [])

    out = compute_review_readiness(db)
    assert out["state"] == "computable"
    assert out["total_photos"] == 4
    assert out["mask_target_photos"] == 2
    assert out["embedding_target_photos"] == 2
    assert out["prediction_target_photos"] == 2
    assert "masks_partial" not in out["enhancing_missing"]
    assert "embeddings" not in out["enhancing_missing"]
    assert "species_predictions" not in out["enhancing_missing"]


def test_compute_review_readiness_eye_attempts_clear_eye_gap(tmp_path):
    """A current eye-keypoint attempt should satisfy readiness even when
    no trustworthy eye point was written."""
    from db import Database
    from dino_embed import embedding_to_blob
    from pipeline import EYE_KP_FINGERPRINT_VERSION, compute_review_readiness

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    pid = db.add_photo(
        fid, "photo.jpg", ".jpg", 1000, 1.0,
        timestamp=datetime(2026, 3, 20, 10, 0, 0).isoformat(),
        width=4000, height=3000,
    )
    det_ids = db.write_detection_batch(pid, "megadetector-v6", [
        {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.9},
    ])
    db.add_prediction(
        det_ids[0], "robin", 0.9, "bioclip", category="match",
        taxonomy={"class": "Aves"},
    )
    emb = np.ones(768, dtype=np.float32)
    emb = emb / np.linalg.norm(emb)
    db.update_photo_pipeline_features(
        pid,
        mask_path=f"/masks/{pid}.png",
        eye_x=None,
        eye_y=None,
        eye_conf=None,
        eye_tenengrad=None,
        eye_kp_fingerprint=EYE_KP_FINGERPRINT_VERSION,
    )
    db.update_photo_embeddings(
        pid,
        dino_subject_embedding=embedding_to_blob(emb),
        dino_global_embedding=embedding_to_blob(emb),
    )

    out = compute_review_readiness(db)
    assert out["with_eye_keypoint_attempts"] == 1
    assert out["eye_keypoint_target_photos"] == 1
    assert "eye_keypoints" not in out["enhancing_missing"]


def _add_eligible_photo(db, fid, filename, species_conf, *, taxonomy_class):
    """Insert one photo+detection+prediction that the eye stage would route
    on if ``taxonomy_class`` is Aves/Mammalia and ``species_conf`` clears
    the classifier confidence gate. Returns the photo id."""
    pid = db.add_photo(
        fid, filename, ".jpg", 1000, 1.0,
        timestamp=datetime(2026, 3, 20, 10, 0, 0).isoformat(),
        width=4000, height=3000,
    )
    det_ids = db.write_detection_batch(pid, "megadetector-v6", [
        {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.9},
    ])
    db.add_prediction(
        det_ids[0], "subject", species_conf, "bioclip", category="match",
        taxonomy={"class": taxonomy_class} if taxonomy_class else None,
    )
    db.update_photo_pipeline_features(pid, mask_path=f"/masks/{pid}.png")
    return pid


def test_compute_review_readiness_eye_target_excludes_out_of_scope_taxonomy(tmp_path):
    """A photo whose top prediction routes to no keypoint model (taxonomy
    outside Aves/Mammalia) must not inflate ``eye_keypoint_target_photos``.

    Pre-fix: the loose mask+detection+prediction eligibility join counted
    these photos, so ``eye_attempts < eye_target`` stayed true forever and
    the "results were computed without eye keypoints" banner showed after
    every full run for any workspace that contained, say, an insect.
    """
    from db import Database
    from pipeline import compute_review_readiness

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    # Out-of-scope species: dragonflies aren't birds or mammals, so the
    # stage's _resolve_keypoint_model returns None and the photo is skipped
    # without stamping a fingerprint. It must not appear in the target.
    _add_eligible_photo(db, fid, "bug.jpg", 0.9, taxonomy_class="Insecta")

    out = compute_review_readiness(db)
    assert out["eye_keypoint_target_photos"] == 0
    assert "eye_keypoints" not in out["enhancing_missing"]


def test_compute_review_readiness_eye_target_excludes_low_confidence(tmp_path):
    """A routable photo whose top prediction sits below the eye-stage
    classifier confidence gate must not inflate the target.

    Pre-fix: the loose count included this photo, so the banner appeared
    after every run even though the stage will keep skipping it at Gate 1.
    """
    import config as cfg
    from db import Database
    from pipeline import compute_review_readiness

    # The stage gate defaults to 0.5; pin the test to a known threshold
    # so a future default tweak doesn't quietly break the assertion.
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({"pipeline": {"eye_classifier_conf_gate": 0.5}})

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    _add_eligible_photo(db, fid, "shy.jpg", 0.3, taxonomy_class="Aves")

    out = compute_review_readiness(db)
    assert out["eye_keypoint_target_photos"] == 0
    assert "eye_keypoints" not in out["enhancing_missing"]


def test_compute_review_readiness_eye_target_includes_routable_above_gate(tmp_path):
    """A routable photo above the confidence gate must still appear in the
    target so a missing eye-keypoint attempt continues to surface."""
    import config as cfg
    from db import Database
    from pipeline import compute_review_readiness

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    # ``eye_detect_enabled`` gates whether the eye-keypoint gap surfaces
    # in ``enhancing_missing``; the target count is orthogonal but the
    # banner check below requires the feature to be turned on.
    cfg.save({
        "pipeline": {
            "eye_classifier_conf_gate": 0.5,
            "eye_detect_enabled": True,
        }
    })

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    _add_eligible_photo(db, fid, "bird.jpg", 0.9, taxonomy_class="Aves")

    out = compute_review_readiness(db)
    assert out["eye_keypoint_target_photos"] == 1
    assert "eye_keypoints" in out["enhancing_missing"]


def test_compute_review_readiness_eye_target_follows_gate_changes(tmp_path):
    """Lowering ``eye_classifier_conf_gate`` must expand the target, the
    same way switching DINOv2 variants reshapes ``with_embeddings``.

    This pins the principle that the readiness counter reflects the
    *current* config — re-running the stage with a lower gate would now
    legitimately produce new attempts, and the banner should reappear.
    """
    import config as cfg
    from db import Database
    from pipeline import compute_review_readiness

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    # Feature must be on for the ``enhancing_missing`` banner to appear
    # at all; the gate/target checks are the point of this test.
    cfg.save({
        "pipeline": {
            "eye_classifier_conf_gate": 0.5,
            "eye_detect_enabled": True,
        }
    })

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    _add_eligible_photo(db, fid, "shy.jpg", 0.3, taxonomy_class="Aves")

    out_strict = compute_review_readiness(db)
    assert out_strict["eye_keypoint_target_photos"] == 0

    cfg.save({
        "pipeline": {
            "eye_classifier_conf_gate": 0.2,
            "eye_detect_enabled": True,
        }
    })
    out_loose = compute_review_readiness(db)
    assert out_loose["eye_keypoint_target_photos"] == 1
    assert "eye_keypoints" in out_loose["enhancing_missing"]


def test_compute_review_readiness_eye_target_honors_workspace_pipeline_override(tmp_path):
    """A workspace-level ``pipeline.eye_classifier_conf_gate`` override
    must reshape the eye-keypoint target.

    Codex's PR #900 review called out *workspace* settings alongside
    global ones. The earlier commit verified the nested read against
    global config; this pins the workspace-overrides path explicitly so
    a future refactor of ``get_effective_config`` (e.g. forgetting to
    deep-merge nested dicts) can't silently sever the gate at the
    workspace level while leaving the global path working.
    """
    import config as cfg
    from db import Database
    from pipeline import compute_review_readiness

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    # Global default gate stays at 0.5; only the active workspace lowers
    # it, mirroring a user with a per-workspace looser threshold. Enable
    # eye detection at the global level so the ``enhancing_missing`` gap
    # can surface (the default is off).
    cfg.save({
        "pipeline": {
            "eye_classifier_conf_gate": 0.5,
            "eye_detect_enabled": True,
        }
    })

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.create_workspace(
        "loose-gate",
        config_overrides={"pipeline": {"eye_classifier_conf_gate": 0.2}},
    )
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(tmp_path), name="photos")
    # Species confidence 0.3 sits between the global gate (0.5) and the
    # workspace override (0.2): excluded if the workspace override is
    # ignored, included if it's honored.
    _add_eligible_photo(db, fid, "shy.jpg", 0.3, taxonomy_class="Aves")

    out = compute_review_readiness(db)
    assert out["eye_keypoint_target_photos"] == 1
    assert "eye_keypoints" in out["enhancing_missing"]


def test_compute_review_readiness_suppresses_eye_gap_when_detection_disabled(tmp_path):
    """When ``pipeline.eye_detect_enabled`` is False (the new default), the
    readiness diagnostic must not flag ``eye_keypoints`` in
    ``enhancing_missing`` even if eligible photos have no eye attempts.

    Otherwise a default-off workspace's pipeline_review page reports the
    result as "computed without eye keypoints — should re-run that stage"
    for a stage the workspace has intentionally disabled, contradicting
    the visible Settings state and the default Process flow (which
    skips the stage). Regression for Codex thread PRRT_kwDORn8c-s6QOH4L.
    """
    import config as cfg
    from db import Database
    from pipeline import compute_review_readiness

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({
        "pipeline": {
            "eye_classifier_conf_gate": 0.5,
            "eye_detect_enabled": False,
        }
    })

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")
    # An Aves photo above the confidence gate would count toward the eye
    # target if the feature were on — it's the same shape that
    # ``test_..._eye_target_includes_routable_above_gate`` uses to
    # deliberately surface the banner.
    _add_eligible_photo(db, fid, "bird.jpg", 0.9, taxonomy_class="Aves")

    out = compute_review_readiness(db)
    assert "eye_keypoints" not in out["enhancing_missing"], (
        "eye_keypoints must not appear in enhancing_missing when the "
        "workspace has eye detection disabled — otherwise the review "
        "page warns about a stage the user chose not to run"
    )


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


def test_save_results_preserve_miss_marker_false_drops_existing(tmp_path):
    """When called with preserve_miss_marker=False, save_results must
    drop any prior miss_computed_at marker instead of carrying it
    forward. The identify/species-only pipeline uses this so Pipeline
    Review doesn't render misses from an earlier full run as if this
    pass produced them."""
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
    assert "miss_computed_at" not in fresh
    save_results(fresh, cache_dir, workspace_id=1, preserve_miss_marker=False)

    loaded = load_results(cache_dir, workspace_id=1)
    assert "miss_computed_at" not in loaded


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


def test_load_photo_features_includes_taxonomy_type_without_species_flag(tmp_path):
    """Taxonomy-typed legacy rows remain confirmed species even when their
    redundant is_species flag is unset."""
    from pipeline import load_photo_features

    db, ids = _setup_db_with_photos(tmp_path)
    pid = ids[0][0]
    keyword_id = db.add_keyword("Verdin")
    db.conn.execute(
        "UPDATE keywords SET type = 'taxonomy', is_species = 0 "
        "WHERE id = ?",
        (keyword_id,),
    )
    db.conn.commit()
    db.tag_photo(pid, keyword_id)

    photo = next(p for p in load_photo_features(db) if p["id"] == pid)
    assert photo["confirmed_species"] == "Verdin"


def test_load_photo_features_ignores_taxonomy_ancestors_as_species(tmp_path):
    """A family keyword may be taxonomy-typed, but only the species-rank
    hierarchy leaf should become confirmed_species."""
    from pipeline import load_photo_features

    db, ids = _setup_db_with_photos(tmp_path)
    db.conn.execute(
        "INSERT INTO taxa (id, name, common_name, rank) VALUES "
        "(38595, 'Remizidae', 'Penduline tits', 'family'), "
        "(2912, 'Auriparus flaviceps', 'Verdin', 'species')"
    )
    db.conn.commit()
    birds = db.add_keyword("1Birds")
    family = db.add_keyword("Penduline tits", parent_id=birds)
    species = db.add_keyword("Verdin", parent_id=family)
    pid = ids[0][0]
    db.tag_photo(pid, family)
    db.tag_photo(pid, species)

    photo = next(p for p in load_photo_features(db) if p["id"] == pid)
    assert photo["confirmed_species"] == "Verdin"


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


def test_confirmed_species_canonicalizes_same_taxon_across_encounter(tmp_path):
    """A photo carrying only a hierarchy leaf and a sibling still on the
    canonical root — same taxon — must resolve to the same confirmed
    species string, so the encounter stays confirmed after regroup.

    Regression for the Codex feedback on pipeline.py:395: without
    canonicalizing hierarchy leaves to the shared taxon root before
    building confirmed_by_photo, ``serialize_pipeline_results`` sees a
    mixed ``confirmed_set`` (e.g. ``{"verdin", "Verdin"}``) and marks
    the encounter unconfirmed, so already-reviewed groups reappear.
    """
    from pipeline import (
        load_photo_features,
        run_full_pipeline,
        serialize_results,
    )

    db, ids = _setup_db_with_photos(tmp_path)
    db.conn.execute(
        "INSERT INTO taxa (id, name, common_name, rank) VALUES "
        "(2912, 'Auriparus flaviceps', 'Verdin', 'species')"
    )
    db.conn.commit()

    # Canonical top-level root, linked to the species taxon.
    root_kid = db.add_keyword("Verdin", is_species=True)
    db.conn.execute(
        "UPDATE keywords SET taxon_id = 2912 WHERE id = ?",
        (root_kid,),
    )
    # Hierarchy leaf under a family container, same taxon, differently
    # cased stored name — the shape duplicate-repair leaves behind when
    # it detaches the redundant root association from that photo.
    birds = db.add_keyword("Birds")
    leaf_kid = db.add_keyword("verdin", parent_id=birds)
    db.conn.execute(
        "UPDATE keywords SET is_species = 1, type = 'taxonomy', "
        "taxon_id = 2912 WHERE id = ?",
        (leaf_kid,),
    )
    db.conn.commit()

    enc_ids = ids[0]
    assert len(enc_ids) >= 2
    # Half the encounter still carries the root; the other half was
    # migrated to the hierarchy leaf spelling.
    db.tag_photo(enc_ids[0], root_kid)
    for pid in enc_ids[1:]:
        db.tag_photo(pid, leaf_kid)

    photos = load_photo_features(db)
    tagged = {p["id"]: p for p in photos if p["id"] in enc_ids}
    assert set(tagged) == set(enc_ids)
    # Every photo in the encounter reports the canonical root spelling,
    # regardless of which keyword row is actually attached.
    assert {p["confirmed_species"] for p in tagged.values()} == {"Verdin"}

    results = run_full_pipeline(photos)
    serialized = serialize_results(results)
    matching = [
        e for e in serialized["encounters"]
        if set(e["photo_ids"]) == set(enc_ids)
    ]
    assert len(matching) == 1, (
        "expected a single encounter matching the tagged photo group"
    )
    enc = matching[0]
    assert enc["confirmed_species"] == "Verdin"
    assert enc["species_confirmed"] is True


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


def test_load_photo_features_preserves_subject_boxes_and_predictions(tmp_path):
    """Each qualifying detection remains a distinct review subject.

    A detected second bird must still surface when it has no prediction, and
    later predictions must attach to that bird rather than being flattened
    without spatial provenance.
    """
    from db import Database
    from pipeline import load_photo_features

    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder(str(tmp_path), name="photos")
    photo_id = db.add_photo(
        folder_id, "two-birds.jpg", ".jpg", 1000, 1.0,
        width=4000, height=3000,
    )
    detection_ids = db.save_detections(
        photo_id,
        [
            {
                "box": {"x": 0.05, "y": 0.4, "w": 0.2, "h": 0.3},
                "confidence": 0.9,
                "category": "animal",
            },
            {
                "box": {"x": 0.55, "y": 0.35, "w": 0.2, "h": 0.3},
                "confidence": 0.4,
                "category": "animal",
            },
        ],
        detector_model="megadetector-v6",
    )
    db.add_prediction(
        detection_ids[0], "American Wigeon", 0.98, "bioclip",
        category="match",
    )

    photo = load_photo_features(db)[0]
    assert [s["detection_id"] for s in photo["subjects"]] == detection_ids
    assert photo["subjects"][0]["box"] == {
        "x": 0.05, "y": 0.4, "w": 0.2, "h": 0.3,
    }
    assert photo["subjects"][0]["predictions"] == [
        ("American Wigeon", 0.98, "bioclip"),
    ]
    assert photo["subjects"][1]["predictions"] == []

    db.add_prediction(
        detection_ids[1], "Blue-winged Teal", 0.91, "bioclip",
        category="match",
    )
    photo = load_photo_features(db)[0]
    assert photo["subjects"][1]["predictions"] == [
        ("Blue-winged Teal", 0.91, "bioclip"),
    ]
    assert {row[0] for row in photo["species_top5"]} == {
        "American Wigeon", "Blue-winged Teal",
    }


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


def test_serialize_results_partial_confirmation_not_marked_confirmed(tmp_path):
    """An encounter with a mix of confirmed and unconfirmed photos must NOT be
    marked species_confirmed=True. This guards the hide_confirmed UX: when
    threshold-slider regrouping merges a confirmed encounter with an
    unconfirmed neighbor, the merged encounter previously inherited the
    confirmed flag and the unconfirmed photos vanished from the review list.
    """
    from pipeline import load_photo_features, run_full_pipeline, serialize_results

    db, ids = _setup_db_with_photos(tmp_path)
    # Confirm species on only some photos of the first encounter — the other
    # photos in the same encounter remain unconfirmed.
    kid = db.add_keyword("Robin", is_species=True)
    for pid in ids[0][:1]:  # tag only the first photo
        db.tag_photo(pid, kid)

    photos = load_photo_features(db)
    results = run_full_pipeline(photos)
    serialized = serialize_results(results)

    target_ids = set(ids[0])
    target_enc = next(
        e for e in serialized["encounters"]
        if set(e["photo_ids"]) & target_ids
    )

    assert target_enc["species_confirmed"] is False, (
        "Partially-confirmed encounter must not be marked confirmed — "
        "otherwise hide_confirmed hides its unconfirmed photos."
    )


def test_serialize_results_mixed_species_not_marked_confirmed(tmp_path):
    """An encounter where photos carry DIFFERENT confirmed species (e.g. when
    threshold-slider regrouping merges two previously-separate confirmed
    encounters) must not be marked species_confirmed=True with one species
    arbitrarily winning. The mixed encounter should be visible for review.
    """
    from pipeline import load_photo_features, run_full_pipeline, serialize_results

    db, ids = _setup_db_with_photos(tmp_path)
    robin_kid = db.add_keyword("Robin", is_species=True)
    eagle_kid = db.add_keyword("Eagle", is_species=True)
    # Tag every photo in encounter 0 — but split between two species so the
    # encounter has a genuinely mixed confirmation set.
    db.tag_photo(ids[0][0], robin_kid)
    db.tag_photo(ids[0][1], robin_kid)
    db.tag_photo(ids[0][2], eagle_kid)

    photos = load_photo_features(db)
    results = run_full_pipeline(photos)
    serialized = serialize_results(results)

    target_ids = set(ids[0])
    target_enc = next(
        e for e in serialized["encounters"]
        if set(e["photo_ids"]) & target_ids
    )

    assert target_enc["species_confirmed"] is False, (
        "Encounter mixing different confirmed species must not be marked "
        "confirmed — every photo would be hidden under hide_confirmed."
    )


def test_serialize_results_mixed_fallback_species_is_deterministic(tmp_path):
    """For mixed encounters, confirmed_species falls back to the most
    frequent confirmed value so /api/encounters/species can untag a stable
    previous keyword on re-confirm. Set iteration order is not stable
    across processes, so the earlier next(iter(set)) fallback was
    effectively arbitrary.
    """
    from pipeline import load_photo_features, run_full_pipeline, serialize_results

    db, ids = _setup_db_with_photos(tmp_path)
    robin_kid = db.add_keyword("Robin", is_species=True)
    eagle_kid = db.add_keyword("Eagle", is_species=True)
    # 2x Robin, 1x Eagle — Robin wins by frequency regardless of which
    # photo happens to come first.
    db.tag_photo(ids[0][0], eagle_kid)
    db.tag_photo(ids[0][1], robin_kid)
    db.tag_photo(ids[0][2], robin_kid)

    photos = load_photo_features(db)
    results = run_full_pipeline(photos)
    serialized = serialize_results(results)

    target_ids = set(ids[0])
    target_enc = next(
        e for e in serialized["encounters"]
        if set(e["photo_ids"]) & target_ids
    )

    assert target_enc["species_confirmed"] is False
    assert target_enc["confirmed_species"] == "Robin"


def test_serialize_results_mixed_fallback_species_tiebreaks_by_first_photo(tmp_path):
    """When confirmed-species counts tie within an encounter, the fallback
    breaks ties by first appearance in photo order. This locks in
    deterministic behavior so re-confirming the same encounter twice
    untags the same previous keyword.
    """
    from pipeline import load_photo_features, run_full_pipeline, serialize_results

    db, ids = _setup_db_with_photos(tmp_path)
    robin_kid = db.add_keyword("Robin", is_species=True)
    eagle_kid = db.add_keyword("Eagle", is_species=True)
    # 1x Eagle (earliest photo), 1x Robin, 1x unconfirmed → counts tie
    # at 1 each, so the earlier photo's species (Eagle) wins the tiebreak.
    db.tag_photo(ids[0][0], eagle_kid)
    db.tag_photo(ids[0][1], robin_kid)

    photos = load_photo_features(db)
    results = run_full_pipeline(photos)
    serialized = serialize_results(results)

    target_ids = set(ids[0])
    target_enc = next(
        e for e in serialized["encounters"]
        if set(e["photo_ids"]) & target_ids
    )

    assert target_enc["species_confirmed"] is False
    assert target_enc["confirmed_species"] == "Eagle"


def test_serialize_results_uniformly_confirmed_remains_confirmed(tmp_path):
    """Sanity: an encounter where every photo shares the same confirmed
    species is still marked species_confirmed=True with confirmed_species set.
    """
    from pipeline import load_photo_features, run_full_pipeline, serialize_results

    db, ids = _setup_db_with_photos(tmp_path)
    kid = db.add_keyword("Robin", is_species=True)
    for pid in ids[0]:
        db.tag_photo(pid, kid)

    photos = load_photo_features(db)
    results = run_full_pipeline(photos)
    serialized = serialize_results(results)

    target_ids = set(ids[0])
    target_enc = next(
        e for e in serialized["encounters"]
        if set(e["photo_ids"]) <= target_ids
    )

    assert target_enc["species_confirmed"] is True
    assert target_enc["confirmed_species"] == "Robin"


def test_serialize_results_burst_override_derived_from_photos(tmp_path):
    """When every photo in a burst shares the same confirmed_species, the
    serialized burst gets a species_override matching that species. This
    survives a regroup that previously wiped overrides to None on every call.
    """
    from pipeline import load_photo_features, run_full_pipeline, serialize_results

    db, ids = _setup_db_with_photos(tmp_path)
    kid = db.add_keyword("Robin", is_species=True)
    for pid in ids[0]:
        db.tag_photo(pid, kid)

    photos = load_photo_features(db)
    results = run_full_pipeline(photos)
    serialized = serialize_results(results)

    confirmed_burst_ids = set(ids[0])
    saw_override = False
    for enc in serialized["encounters"]:
        for burst in enc.get("bursts", []):
            burst_ids = set(burst["photo_ids"])
            if burst_ids and burst_ids <= confirmed_burst_ids:
                assert burst["species_override"] is not None, (
                    "Burst whose photos are all confirmed Robin must surface a "
                    "species_override so frontend burst-confirmed indicators "
                    "survive a slider-driven regroup."
                )
                assert burst["species_override"]["species"] == "Robin"
                assert burst["species_override"]["confirmed"] is True
                saw_override = True
    assert saw_override, "test setup did not produce any all-Robin burst"


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


def test_eye_keypoint_stage_preflight_missing_config_defaults_disabled(
    tmp_path, monkeypatch
):
    """Missing eye_detect_enabled follows the global default: disabled."""
    from pipeline import eye_keypoint_stage_preflight

    assert eye_keypoint_stage_preflight({}) == "Disabled in config"


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


def test_eye_stage_gate1_failure_does_not_stamp_fingerprint(tmp_path, monkeypatch):
    """Gate 1 (classifier conf) short-circuits cheaply but must NOT stamp
    eye_kp_fingerprint: the gate is driven by the user-configurable
    `eye_classifier_conf_gate`, so stamping would permanently filter the
    photo out of `list_photos_for_eye_keypoint_stage` even after the user
    lowers the gate. The photo must remain eligible on a subsequent run
    with a more permissive threshold.
    """
    import keypoints as kp
    from pipeline import detect_eye_keypoints_stage

    db, pid, models_dir = _setup_eligible_mammal_with_files(
        tmp_path, classifier_conf=0.3,
    )
    monkeypatch.setattr(kp, "MODELS_DIR", models_dir)

    # First pass: high gate skips the photo at Gate 1.
    monkeypatch.setattr(
        kp, "detect_keypoints",
        lambda *a, **kw: (_ for _ in ()).throw(
            AssertionError("Gate 1 should short-circuit before model runs")
        ),
    )
    detect_eye_keypoints_stage(
        db, config={"eye_detect_enabled": True, "eye_classifier_conf_gate": 0.5},
    )

    # Fingerprint must still be NULL — the photo wasn't actually evaluated.
    fp_row = db.conn.execute(
        "SELECT eye_kp_fingerprint FROM photos WHERE id=?", (pid,),
    ).fetchone()
    assert fp_row[0] is None, (
        "Gate 1 (low classifier confidence) must not stamp eye_kp_fingerprint; "
        "stamping locks the photo out of future runs even after the user "
        "lowers eye_classifier_conf_gate."
    )

    # Photo must still surface as a candidate for the next stage run.
    rows = db.list_photos_for_eye_keypoint_stage()
    assert any(r["id"] == pid for r in rows)

    # Second pass: lower the gate. Now the model runs and writes eye fields.
    good = [
        {"name": "left_eye", "x": 300.0, "y": 300.0, "conf": 0.88},
        {"name": "right_eye", "x": 350.0, "y": 300.0, "conf": 0.85},
    ]
    monkeypatch.setattr(kp, "detect_keypoints", lambda *a, **kw: good)
    detect_eye_keypoints_stage(
        db, config={"eye_detect_enabled": True, "eye_classifier_conf_gate": 0.1},
    )
    eye_x, eye_y, eye_conf, _ = _read_eye_fields(db, pid)
    assert eye_x is not None and eye_y is not None and eye_conf is not None


def test_eye_stage_unrouted_taxonomy_does_not_stamp_fingerprint(tmp_path, monkeypatch):
    """When `_resolve_keypoint_model` returns None for a photo (e.g. the
    species' taxonomy class isn't in the routing map yet), the stage must
    not stamp eye_kp_fingerprint. The routing depends on
    `_EYE_KEYPOINT_MODEL_FOR_CLASS` and taxonomy data which can be extended
    later; stamping would prevent the photo from re-running after such
    an extension.
    """
    import keypoints as kp
    from pipeline import detect_eye_keypoints_stage

    db, pid, models_dir = _setup_eligible_mammal_with_files(
        tmp_path, taxonomy_class="Actinopterygii",  # ray-finned fish
    )
    monkeypatch.setattr(kp, "MODELS_DIR", models_dir)

    def _boom(*a, **kw):
        raise AssertionError(
            "detect_keypoints should not run for unrouted taxonomy"
        )

    monkeypatch.setattr(kp, "detect_keypoints", _boom)
    detect_eye_keypoints_stage(db, config={"eye_detect_enabled": True})

    fp_row = db.conn.execute(
        "SELECT eye_kp_fingerprint FROM photos WHERE id=?", (pid,),
    ).fetchone()
    assert fp_row[0] is None, (
        "Unrouted-taxonomy short-circuit must not stamp eye_kp_fingerprint; "
        "the routing map / taxonomy data can be extended later and the "
        "photo must remain eligible to re-run then."
    )
    rows = db.list_photos_for_eye_keypoint_stage()
    assert any(r["id"] == pid for r in rows)


def test_eye_stage_gate3_failure_stamps_fingerprint(tmp_path, monkeypatch):
    """Inverse case: when the model actually runs and finds no
    trustworthy eye (Gate 3/4 fail), eye_kp_fingerprint must be stamped so
    `list_photos_for_eye_keypoint_stage` does not keep returning the photo
    on every subsequent run. This is the original motivation for the
    no-eye attempt marker — it should not be weakened by the fix to the
    pre-model gates.
    """
    import keypoints as kp
    from pipeline import EYE_KP_FINGERPRINT_VERSION, detect_eye_keypoints_stage

    db, pid, models_dir = _setup_eligible_mammal_with_files(tmp_path)
    monkeypatch.setattr(kp, "MODELS_DIR", models_dir)

    # All eye points below the detection-conf gate → Gate 3 fails.
    low = [
        {"name": "left_eye", "x": 300.0, "y": 300.0, "conf": 0.2},
        {"name": "right_eye", "x": 350.0, "y": 300.0, "conf": 0.15},
    ]
    monkeypatch.setattr(kp, "detect_keypoints", lambda *a, **kw: low)
    detect_eye_keypoints_stage(
        db, config={"eye_detect_enabled": True, "eye_detection_conf_gate": 0.5},
    )

    fp_row = db.conn.execute(
        "SELECT eye_kp_fingerprint FROM photos WHERE id=?", (pid,),
    ).fetchone()
    assert fp_row[0] == EYE_KP_FINGERPRINT_VERSION
    # And the photo is no longer eligible on the next selection pass.
    rows = db.list_photos_for_eye_keypoint_stage()
    assert not any(r["id"] == pid for r in rows)


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


def test_eye_kp_fingerprint_version_is_string():
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    assert isinstance(EYE_KP_FINGERPRINT_VERSION, str)
    assert len(EYE_KP_FINGERPRINT_VERSION) > 0


def test_compute_group_fingerprint_is_stable_for_same_input():
    from pipeline import compute_group_fingerprint
    cfg = {"pipeline": {"foo": 1}}
    assert compute_group_fingerprint(cfg) == compute_group_fingerprint(cfg)


def test_compute_group_fingerprint_changes_with_encounter_defaults():
    """Bumping any value in encounters.DEFAULTS must change the fingerprint."""
    import encounters
    from pipeline import compute_group_fingerprint
    cfg = {}
    fp_before = compute_group_fingerprint(cfg)
    original = encounters.DEFAULTS.copy()
    try:
        encounters.DEFAULTS["w_time"] = original["w_time"] + 0.01
        fp_after = compute_group_fingerprint(cfg)
        assert fp_after != fp_before
    finally:
        encounters.DEFAULTS.clear()
        encounters.DEFAULTS.update(original)


def test_compute_group_fingerprint_changes_with_pipeline_override():
    """A workspace pipeline override of an encounter/burst param must change
    the fingerprint — otherwise the pipeline page can't tell that grouping
    settings have moved and would falsely claim "fresh" after a settings edit."""
    from pipeline import compute_group_fingerprint
    base = compute_group_fingerprint({})
    encounters_override = compute_group_fingerprint(
        {"pipeline": {"w_time": 0.99}},
    )
    assert encounters_override != base
    bursts_override = compute_group_fingerprint(
        {"pipeline": {"burst_time_gap": 7.5}},
    )
    assert bursts_override != base


def test_compute_group_fingerprint_ignores_unrelated_pipeline_keys():
    """Pipeline settings that don't drive grouping (e.g. detector / classifier
    knobs) must not bump the group fingerprint, otherwise unrelated config
    edits would mark Group as Outdated."""
    from pipeline import compute_group_fingerprint
    base = compute_group_fingerprint({})
    unrelated = compute_group_fingerprint(
        {"pipeline": {"classifier_model": "x", "detector_confidence": 0.9}},
    )
    assert unrelated == base


def test_serialize_results_counts_missing_timestamps():
    """Each serialized encounter reports how many of its photos lack a timestamp."""
    from pipeline import serialize_results

    results = {
        "encounters": [
            {  # 2 of 3 photos missing timestamps
                "species": None,
                "photos": [
                    {"id": 1, "timestamp": "2026-05-25T10:00:00", "confirmed_species": None},
                    {"id": 2, "timestamp": None, "confirmed_species": None},
                    {"id": 3, "timestamp": None, "confirmed_species": None},
                ],
                "photo_count": 3,
                "burst_count": 1,
                "time_range": [None, None],
            },
            {  # all timestamped
                "species": None,
                "photos": [
                    {"id": 4, "timestamp": "2026-05-25T11:00:00", "confirmed_species": None},
                    {"id": 5, "timestamp": "2026-05-25T11:00:05", "confirmed_species": None},
                ],
                "photo_count": 2,
                "burst_count": 1,
                "time_range": ["2026-05-25T11:00:00", "2026-05-25T11:00:05"],
            },
        ],
        "photos": [
            {"id": 1, "timestamp": "2026-05-25T10:00:00", "label": "REVIEW"},
            {"id": 2, "timestamp": None, "label": "REVIEW"},
            {"id": 3, "timestamp": None, "label": "REVIEW"},
            {"id": 4, "timestamp": "2026-05-25T11:00:00", "label": "KEEP"},
            {"id": 5, "timestamp": "2026-05-25T11:00:05", "label": "REVIEW"},
        ],
        "summary": {},
    }

    serialized = serialize_results(results)
    encs = serialized["encounters"]
    assert encs[0]["missing_timestamp_count"] == 2
    # Zero (not absent) so the JS `if (enc.missing_timestamp_count)` is clean.
    assert encs[1]["missing_timestamp_count"] == 0


def test_prune_results_recomputes_missing_timestamp_count(tmp_path):
    """Deleting a null-timestamp photo updates the encounter's missing count.

    Otherwise the ⚠ badge keeps showing the pre-deletion count.
    """
    from pipeline import load_results, prune_results

    cache = {
        "encounters": [
            {
                "species": None, "confirmed_species": None,
                "species_predictions": [], "species_confirmed": False,
                "photo_count": 3, "burst_count": 1,
                "time_range": [None, None],
                "photo_ids": [1, 2, 3],
                "missing_timestamp_count": 2,
                "bursts": [
                    {"photo_ids": [1, 2, 3], "species_predictions": [],
                     "species_override": None},
                ],
            },
        ],
        "photos": [
            {"id": 1, "label": "REVIEW", "timestamp": "2026-05-25T10:00:00"},
            {"id": 2, "label": "REVIEW", "timestamp": None},
            {"id": 3, "label": "REVIEW", "timestamp": None},
        ],
        "summary": {},
    }
    _write_cache(str(tmp_path), 1, cache)

    # Delete one of the two null-timestamp photos.
    prune_results(str(tmp_path), 1, [3])

    loaded = load_results(str(tmp_path), 1)
    enc = loaded["encounters"][0]
    assert enc["photo_count"] == 2
    assert enc["missing_timestamp_count"] == 1
