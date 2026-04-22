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


def test_clipped_when_bbox_too_small_singleton():
    from misses import classify_miss
    row = _row(subject_size=0.001)  # 0.1% — below singleton 0.2%
    flags = classify_miss(row, siblings=[], config=DEFAULT_CONFIG)
    assert flags["clipped"] is True


def test_not_clipped_when_bbox_small_but_above_singleton_threshold():
    from misses import classify_miss
    row = _row(subject_size=0.003)  # 0.3% — above singleton 0.2%
    flags = classify_miss(row, siblings=[], config=DEFAULT_CONFIG)
    assert flags["clipped"] is False


def test_clipped_when_crop_complete_below_reject_threshold_in_burst():
    from misses import classify_miss
    row = _row(crop_complete=0.40)  # touches edge
    siblings = [_row(crop_complete=1.0), _row(crop_complete=1.0)]
    flags = classify_miss(row, siblings=siblings, config=DEFAULT_CONFIG)
    assert flags["clipped"] is True


def test_clipped_when_bbox_much_smaller_than_burst_median():
    """Burst context: this frame's bbox is <10% of sibling median → miss."""
    from misses import classify_miss
    row = _row(subject_size=0.005)        # 0.5%
    siblings = [_row(subject_size=0.08), _row(subject_size=0.10)]  # 8%, 10%
    flags = classify_miss(row, siblings=siblings, config=DEFAULT_CONFIG)
    assert flags["clipped"] is True


def test_oof_when_background_sharper_than_subject_in_burst():
    from misses import classify_miss
    row = _row(subject_tenengrad=20.0, bg_tenengrad=80.0)  # ratio 0.25
    siblings = [_row(), _row()]
    flags = classify_miss(row, siblings=siblings, config=DEFAULT_CONFIG)
    assert flags["oof"] is True


def test_not_oof_when_ratio_above_threshold():
    from misses import classify_miss
    row = _row(subject_tenengrad=60.0, bg_tenengrad=80.0)  # ratio 0.75
    flags = classify_miss(row, siblings=[_row()], config=DEFAULT_CONFIG)
    assert flags["oof"] is False


def test_oof_singleton_requires_both_ratio_and_floor():
    """Singleton: ratio alone isn't enough — need absolute floor too."""
    from misses import classify_miss
    # Ratio is bad (0.25) but subject is still sharp in absolute terms.
    row = _row(subject_tenengrad=200.0, bg_tenengrad=800.0)
    flags = classify_miss(row, siblings=[], config=DEFAULT_CONFIG)
    assert flags["oof"] is False

    # Ratio is bad AND subject is below absolute floor.
    row = _row(subject_tenengrad=5.0, bg_tenengrad=20.0)
    flags = classify_miss(row, siblings=[], config=DEFAULT_CONFIG)
    assert flags["oof"] is True
