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


def test_no_spurious_flags_when_quality_features_missing():
    """Detection passed but quality pipeline hasn't populated features yet.

    A row with detection_conf above threshold but NULL subject_size /
    crop_complete / subject_tenengrad / bg_tenengrad means we haven't
    measured those features. We must NOT flag clipped/oof on absence of
    evidence — covers both singleton and in-burst evaluation.
    """
    from misses import classify_miss

    # Singleton: detection passed, no quality features measured.
    row = _row(
        detection_conf=0.9,
        subject_size=None,
        crop_complete=None,
        subject_tenengrad=None,
        bg_tenengrad=None,
    )
    flags = classify_miss(row, siblings=[], config=DEFAULT_CONFIG)
    assert flags == {"no_subject": False, "clipped": False, "oof": False}

    # In-burst: two photos, both with detection passed but no quality
    # features measured. Siblings with no subject_size must not poison
    # the burst-median check.
    row_a = _row(
        detection_conf=0.9,
        subject_size=None,
        crop_complete=None,
        subject_tenengrad=None,
        bg_tenengrad=None,
    )
    row_b = _row(
        detection_conf=0.9,
        subject_size=None,
        crop_complete=None,
        subject_tenengrad=None,
        bg_tenengrad=None,
    )
    flags_a = classify_miss(row_a, siblings=[row_b], config=DEFAULT_CONFIG)
    flags_b = classify_miss(row_b, siblings=[row_a], config=DEFAULT_CONFIG)
    assert flags_a == {"no_subject": False, "clipped": False, "oof": False}
    assert flags_b == {"no_subject": False, "clipped": False, "oof": False}


def test_classify_miss_falls_back_to_defaults_for_partial_config():
    """Pipeline configs reaching classify_miss are often partial — e.g.
    /api/pipeline/config stores only model keys under `pipeline` and
    Database.get_effective_config does a shallow top-level merge. Miss
    thresholds must fall back to config.DEFAULTS rather than raising
    KeyError and failing the whole pipeline job."""
    from misses import classify_miss

    # Partial config: none of the miss_* thresholds are present.
    partial = {"model": "whatever"}

    # With low detection confidence and empty siblings (singleton), the
    # default miss_det_confidence=0.25 should classify this as no_subject.
    row = _row(detection_conf=0.10)
    flags = classify_miss(row, siblings=[], config=partial)
    assert flags == {"no_subject": True, "clipped": False, "oof": False}

    # With good detection confidence and a tiny bbox, the default
    # miss_bbox_area_min_singleton=0.002 should classify as clipped.
    row = _row(subject_size=0.001)
    flags = classify_miss(row, siblings=[], config=partial)
    assert flags["clipped"] is True


def test_compute_misses_groups_by_burst_and_writes_flags(tmp_path):
    """Integration test: real DB, synthetic photo rows, verify flags written."""
    import config as cfg
    from db import Database
    from misses import compute_misses_for_workspace

    db = Database(str(tmp_path / "m.db"))
    # Database.__init__ auto-creates a "Default" workspace and sets it active.

    # Insert a folder for the photos.
    folder_id = db.add_folder("/tmp/fake")

    # Two photos in the same burst — one keeper, one lost-framing miss.
    p_keeper = db.add_photo(
        folder_id,
        "k.jpg",
        extension=".jpg",
        file_size=100,
        file_mtime=1.0,
        timestamp="2026-04-22T10:00:00",
    )
    p_miss = db.add_photo(
        folder_id,
        "m.jpg",
        extension=".jpg",
        file_size=100,
        file_mtime=2.0,
        timestamp="2026-04-22T10:00:00",
    )
    # Hand-write pipeline features + shared burst_id. Detection confidence
    # lives in the `detections` table (written by save_detections during the
    # classify stage), not photos.detection_conf.
    db.conn.executemany(
        "UPDATE photos SET burst_id=?, subject_size=?, "
        "crop_complete=?, subject_tenengrad=?, bg_tenengrad=? WHERE id=?",
        [
            ("B1", 0.08,  1.0, 80.0, 40.0, p_keeper),
            ("B1", 0.005, 1.0, 80.0, 40.0, p_miss),
        ],
    )
    db.conn.commit()
    for pid in (p_keeper, p_miss):
        db.save_detections(
            pid,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
              "confidence": 0.95, "category": "animal"}],
        )

    compute_misses_for_workspace(db, cfg.DEFAULTS["pipeline"])

    keeper = dict(db.conn.execute(
        "SELECT miss_no_subject, miss_clipped, miss_oof, miss_computed_at "
        "FROM photos WHERE id=?", (p_keeper,)
    ).fetchone())
    miss = dict(db.conn.execute(
        "SELECT miss_no_subject, miss_clipped, miss_oof, miss_computed_at "
        "FROM photos WHERE id=?", (p_miss,)
    ).fetchone())

    assert keeper["miss_clipped"] == 0
    assert miss["miss_clipped"] == 1
    assert keeper["miss_computed_at"] is not None
    assert miss["miss_computed_at"] is not None


def test_compute_misses_scoped_to_active_workspace(tmp_path):
    """Photos in folders linked only to workspace A must not be touched when
    compute runs with workspace B active."""
    import config as cfg
    from db import Database
    from misses import compute_misses_for_workspace

    db = Database(str(tmp_path / "m.db"))
    ws_a = db._active_workspace_id
    ws_b = db.create_workspace("Other")

    # Folder linked to A only — add it while A is active.
    db.set_active_workspace(ws_a)
    fa = db.add_folder("/tmp/a", name="a")
    p_a = db.add_photo(
        fa, "a.jpg", extension=".jpg", file_size=100, file_mtime=1.0,
        timestamp="2026-04-22T10:00:00",
    )
    db.conn.execute(
        "UPDATE photos SET subject_size=?, "
        "crop_complete=?, subject_tenengrad=?, bg_tenengrad=? WHERE id=?",
        (0.001, 1.0, 80.0, 40.0, p_a),
    )
    db.save_detections(
        p_a,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
          "confidence": 0.95, "category": "animal"}],
    )

    # Folder linked to B — add while B is active.
    db.set_active_workspace(ws_b)
    fb = db.add_folder("/tmp/b", name="b")
    p_b = db.add_photo(
        fb, "b.jpg", extension=".jpg", file_size=100, file_mtime=2.0,
        timestamp="2026-04-22T10:00:01",
    )
    db.conn.execute(
        "UPDATE photos SET subject_size=?, "
        "crop_complete=?, subject_tenengrad=?, bg_tenengrad=? WHERE id=?",
        (0.001, 1.0, 80.0, 40.0, p_b),
    )
    db.save_detections(
        p_b,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.2, "h": 0.2},
          "confidence": 0.95, "category": "animal"}],
    )
    db.conn.commit()

    # B is active; compute must only touch B's photo.
    compute_misses_for_workspace(db, cfg.DEFAULTS["pipeline"])

    row_a = dict(db.conn.execute(
        "SELECT miss_clipped, miss_computed_at FROM photos WHERE id=?", (p_a,)
    ).fetchone())
    row_b = dict(db.conn.execute(
        "SELECT miss_clipped, miss_computed_at FROM photos WHERE id=?", (p_b,)
    ).fetchone())

    assert row_a["miss_computed_at"] is None
    assert row_a["miss_clipped"] != 1
    assert row_b["miss_computed_at"] is not None
    assert row_b["miss_clipped"] == 1
