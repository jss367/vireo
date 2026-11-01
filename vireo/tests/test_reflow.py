# vireo/tests/test_reflow.py
"""Tests for pipeline reflow (re-running stages 4-6 with different thresholds)."""
import os
import sys
from datetime import datetime, timedelta

import numpy as np
from repositories.masks_features import MaskQuality, PipelineFeatures
from subject_readiness import REQUIRED_QUALITY_FIELDS

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from services import photo_ingest, workspace_folders


def _setup_db(tmp_path):
    """Create a DB with photos that have full pipeline features."""
    import config as cfg
    from db import Database
    from dino_embed import embedding_to_blob

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    cfg.save({"pipeline": {"sam2_variant": "sam2-small"}})
    db = Database(str(tmp_path / "test.db"))
    fid = workspace_folders.add_folder(db, str(tmp_path), name="photos")

    base = datetime(2026, 3, 20, 10, 0, 0)
    emb = np.ones(768, dtype=np.float32)
    emb = emb / np.linalg.norm(emb)

    for i in range(6):
        ts = base + timedelta(seconds=i * 2)
        pid = photo_ingest.add_photo(
            db, fid, f"photo{i}.jpg", ".jpg", 1000, 1.0,
            timestamp=ts.isoformat(), width=4000, height=3000,
        )
        mask_path = tmp_path / f"{pid}-mask.png"
        mask_path.write_bytes(b"scratch-mask")
        features = PipelineFeatures(
            mask_path=str(mask_path),
            subject_tenengrad=100 + i * 80,
            bg_tenengrad=30,
            crop_complete=0.5 + i * 0.1,
            bg_separation=50.0,
            subject_clip_high=0.01 + i * 0.06,
            subject_clip_low=0.01,
            subject_y_median=120.0,
            phash_crop=f"{pid:016x}",
        )
        db.masks_features.update_pipeline_features(pid, features)
        db.masks_features.update_embeddings(
            pid,
            dino_subject_embedding=embedding_to_blob(emb),
            dino_global_embedding=embedding_to_blob(emb),
        )
        db.commit()
        db.photos.update_quality(
            pid,
            subject_size=0.1,
        )
        det_ids = db.detections.save(pid, [
            {"box": {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}, "confidence": 0.9, "category": "animal"},
        ], detector_model="megadetector-v6")
        db.model_runs.record_detector_run(pid, "megadetector-v6", 1)
        db.commit()
        quality = MaskQuality(subject_size=.1, **{
            key: getattr(features, key) for key in REQUIRED_QUALITY_FIELDS if key != "subject_size"
        })
        db.masks_features.upsert_mask(pid, "sam2-small", str(mask_path), "megadetector-v6",
                                     (.2, .2, .4, .4), quality)
        db.masks_features.set_active_variant(pid, "sam2-small", .2)
        db.commit()
        db.predictions.add(det_ids[0], "robin", 0.9, "bioclip", category="match")
        db.commit()

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


def test_reflow_lenient_thresholds_preserve_review_without_validated_keep(tmp_path):
    """Passing reject thresholds leaves ranked photos for manual review."""
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
    assert results["summary"]["reject_count"] == 0
    assert results["summary"]["keep_count"] == 0
    assert results["summary"]["review_count"] == 6
    assert any(photo["ranking_selected"] for photo in results["photos"])
    assert all(photo["selection_eligible"] for photo in results["photos"])
    assert all(not photo["automatic_keep_eligible"] for photo in results["photos"])


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
    """Selection limits change provisional candidates without automatic KEEP."""
    from pipeline import load_photo_features, reflow, run_grouping

    db = _setup_db(tmp_path)
    photos = load_photo_features(db)
    encounters = run_grouping(photos)

    results_1 = reflow(encounters, config={
        "burst_max_keep": 1, "encounter_max_keep": 1,
        "reject_crop_complete": 0.01, "reject_composite": 0.01,
        "reject_focus": 0.01, "reject_clip_high": 0.99,
    })
    selected_1 = sum(photo["ranking_selected"] for photo in results_1["photos"])
    results_10 = reflow(encounters, config={
        "burst_max_keep": 10, "encounter_max_keep": 10,
        "reject_crop_complete": 0.01, "reject_composite": 0.01,
        "reject_focus": 0.01, "reject_clip_high": 0.99,
    })

    selected_10 = sum(photo["ranking_selected"] for photo in results_10["photos"])
    assert selected_1 == 1
    assert selected_10 == 6
    for results in (results_1, results_10):
        assert results["summary"]["keep_count"] == 0
        assert results["summary"]["review_count"] == 6


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
        {"label": "KEEP", "rarity_protected": False, "confirmed_species": "Blue Jay"},
        {"label": "KEEP", "rarity_protected": False, "confirmed_species": "Blue Jay"},
        {"label": "REVIEW", "rarity_protected": True, "confirmed_species": None},
        {"label": "REJECT"},
    ]
    encounters = [
        {"burst_count": 2, "photos": photos[:3], "bursts": [photos[:2], photos[2:3]]},
        {"burst_count": 1, "photos": photos[3:], "bursts": [photos[3:]]},
    ]

    s = _make_summary(encounters, photos)
    assert s["total_photos"] == 4
    assert s["keep_count"] == 2
    assert s["review_count"] == 1
    assert s["reject_count"] == 1
    assert s["rarity_protected"] == 1
    assert s["burst_count"] == 3
    assert s["encounter_count"] == 2
    assert s["confirmed_count"] == 1
    assert s["unconfirmed_count"] == 2
