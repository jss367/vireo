"""Tests for miss-detection classification.

classify_miss() is a pure function that derives three miss booleans from
per-photo features already written by the pipeline. No I/O, no DB access.
"""


def _row(**overrides):
    base = {
        "detection_conf": 0.9,
        "subject_size": 0.05,     # 5% of frame
        "crop_complete": 1.0,
        "subject_tenengrad": 80.0,
        "bg_tenengrad": 40.0,
        "burst_id": None,
    }
    base.update(overrides)
    return base


DEFAULT_CONFIG = {
    "miss_det_confidence": 0.25,
    "miss_det_confidence_burst": 0.15,
    "miss_bbox_area_min": 0.005,
    "miss_bbox_area_min_singleton": 0.002,
    "miss_oof_ratio": 0.5,
}


def test_no_subject_when_detection_below_threshold_singleton():
    from misses import classify_miss
    row = _row(detection_conf=0.10)
    flags = classify_miss(row, siblings=[], config=DEFAULT_CONFIG)
    assert flags == {"no_subject": True, "clipped": False, "oof": False}


def test_no_subject_excludes_other_categories():
    """When there's no bbox, clipped/oof can't be evaluated."""
    from misses import classify_miss
    row = _row(
        detection_conf=0.0,
        subject_size=None,
        crop_complete=None,
        subject_tenengrad=None,
        bg_tenengrad=None,
    )
    flags = classify_miss(row, siblings=[], config=DEFAULT_CONFIG)
    assert flags["no_subject"] is True
    assert flags["clipped"] is False
    assert flags["oof"] is False
