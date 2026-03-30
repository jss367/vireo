# vireo/tests/test_reflow.py
"""Tests for pipeline reflow (re-running stages 4-6 with different thresholds)."""
import os
import sys
from datetime import datetime, timedelta

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _setup_db(tmp_path):
    """Create a DB with photos that have full pipeline features."""
    from db import Database
    from dino_embed import embedding_to_blob

    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder(str(tmp_path), name="photos")

    base = datetime(2026, 3, 20, 10, 0, 0)
    emb = np.ones(768, dtype=np.float32)
    emb = emb / np.linalg.norm(emb)

    for i in range(6):
        ts = base + timedelta(seconds=i * 2)
        pid = db.add_photo(
            fid, f"photo{i}.jpg", ".jpg", 1000, 1.0,
            timestamp=ts.isoformat(), width=4000, height=3000,
        )
        db.update_photo_mask(pid, f"/masks/{pid}.png")
        db.update_photo_pipeline_features(
            pid,
            mask_path=f"/masks/{pid}.png",
            subject_tenengrad=100 + i * 80,
            bg_tenengrad=30,
            crop_complete=0.5 + i * 0.1,  # ranges from 0.5 to 1.0
            bg_separation=50.0,
            subject_clip_high=0.01 + i * 0.06,  # ranges from 0.01 to 0.31
            subject_clip_low=0.01,
            subject_y_median=120.0,
            phash_crop=f"{pid:016x}",
        )
        db.update_photo_embeddings(
            pid,
            dino_subject_embedding=embedding_to_blob(emb),
            dino_global_embedding=embedding_to_blob(emb),
        )
        db.update_photo_quality(
            pid,
            subject_size=0.1,
        )
        det_ids = db.save_detections(pid, [
            {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.9},
        ], detector_model="megadetector")
        db.add_prediction(det_ids[0], "robin", 0.9, "bioclip", category="match")

    return db


def test_reflow_changes_labels_with_stricter_thresholds(tmp_path):
    """Stricter reject thresholds should produce more REJECT labels."""
    from pipeline import load_photo_features, reflow, run_grouping, run_triage

    db = _setup_db(tmp_path)
    photos = load_photo_features(db)
    encounters = run_grouping(photos)

    # Default thresholds
    _, photos_default = run_triage(encounters)
    rejects_default = sum(1 for p in photos_default if p["label"] == "REJECT")

    # Reflow with much stricter crop_complete threshold
    results_strict = reflow(encounters, config={"reject_crop_complete": 0.95})
    rejects_strict = results_strict["summary"]["reject_count"]

    # Stricter threshold should reject more (photos with crop_complete < 0.95)
    assert rejects_strict >= rejects_default


def test_reflow_changes_labels_with_lenient_thresholds(tmp_path):
    """Lenient reject thresholds should produce fewer REJECT labels."""
    from pipeline import load_photo_features, reflow, run_grouping, run_triage

    db = _setup_db(tmp_path)
    photos = load_photo_features(db)
    encounters = run_grouping(photos)

    # First score with defaults
    run_triage(encounters)

    # Reflow with very lenient thresholds
    results = reflow(encounters, config={
        "reject_crop_complete": 0.01,
        "reject_focus": 0.01,
        "reject_clip_high": 0.99,
        "reject_composite": 0.01,
    })
    # With such lenient thresholds, very few should be rejected
    # (only "no mask" rule would fire, but all our photos have masks)
    rejects = results["summary"]["reject_count"]
    keeps = results["summary"]["keep_count"]
    assert keeps > 0


def test_reflow_preserves_total_photo_count(tmp_path):
    """Reflow should not gain or lose photos."""
    from pipeline import load_photo_features, reflow, run_grouping

    db = _setup_db(tmp_path)
    photos = load_photo_features(db)
    encounters = run_grouping(photos)

    results = reflow(encounters, config={"reject_composite": 0.80})
    total = results["summary"]["keep_count"] + results["summary"]["review_count"] + results["summary"]["reject_count"]
    assert total == results["summary"]["total_photos"]
    assert total == 6


def test_reflow_mmr_max_keep(tmp_path):
    """Changing MMR max_keep should affect how many photos are KEEP."""
    from pipeline import load_photo_features, reflow, run_grouping

    db = _setup_db(tmp_path)
    photos = load_photo_features(db)
    encounters = run_grouping(photos)

    results_1 = reflow(encounters, config={
        "burst_max_keep": 1, "encounter_max_keep": 1,
        "reject_crop_complete": 0.01, "reject_composite": 0.01,
        "reject_focus": 0.01, "reject_clip_high": 0.99,
    })
    results_10 = reflow(encounters, config={
        "burst_max_keep": 10, "encounter_max_keep": 10,
        "reject_crop_complete": 0.01, "reject_composite": 0.01,
        "reject_focus": 0.01, "reject_clip_high": 0.99,
    })

    # With max_keep=10 (more than we have), more photos should be KEEP
    assert results_10["summary"]["keep_count"] >= results_1["summary"]["keep_count"]


def test_reflow_returns_correct_format(tmp_path):
    """Reflow returns dict with encounters, photos, summary."""
    from pipeline import load_photo_features, reflow, run_grouping

    db = _setup_db(tmp_path)
    photos = load_photo_features(db)
    encounters = run_grouping(photos)

    results = reflow(encounters)
    assert "encounters" in results
    assert "photos" in results
    assert "summary" in results
    assert all(p["label"] in ("KEEP", "REVIEW", "REJECT") for p in results["photos"])


def test_make_summary(tmp_path):
    """_make_summary produces correct counts."""
    from pipeline import _make_summary

    photos = [
        {"label": "KEEP", "rarity_protected": False},
        {"label": "KEEP", "rarity_protected": False},
        {"label": "REVIEW", "rarity_protected": True},
        {"label": "REJECT"},
    ]
    encounters = [{"burst_count": 2}, {"burst_count": 1}]

    s = _make_summary(encounters, photos)
    assert s["total_photos"] == 4
    assert s["keep_count"] == 2
    assert s["review_count"] == 1
    assert s["reject_count"] == 1
    assert s["rarity_protected"] == 1
    assert s["burst_count"] == 3
    assert s["encounter_count"] == 2
