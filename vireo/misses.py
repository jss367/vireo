"""Miss-detection classification.

Pure derivation from per-photo features already written by the pipeline
(detection confidence, bbox fraction, crop completeness, subject/background
Tenengrad, burst id). No I/O, no DB access.

Three categories:
  - no_subject: detector didn't find an animal (subject-less frame)
  - clipped:    subject touches a frame edge or is too small
  - oof:        subject is out of focus (background sharper than subject)

no_subject is exclusive — when it's true, clipped/oof can't be evaluated
and both return False.
"""


def classify_miss(row, siblings, config):
    """Return {'no_subject': bool, 'clipped': bool, 'oof': bool}.

    Args:
        row: dict with detection_conf, subject_size, crop_complete,
            subject_tenengrad, bg_tenengrad, burst_id.
        siblings: list of sibling rows in the same burst (excluding `row`).
        config: dict with miss_* thresholds.
    """
    in_burst = bool(siblings)
    conf_threshold = (
        config["miss_det_confidence_burst"] if in_burst
        else config["miss_det_confidence"]
    )
    detection_conf = row.get("detection_conf") or 0.0
    if detection_conf < conf_threshold:
        return {"no_subject": True, "clipped": False, "oof": False}
    bbox_area_min = (
        config["miss_bbox_area_min"] if in_burst
        else config["miss_bbox_area_min_singleton"]
    )
    subject_size = row.get("subject_size") or 0.0
    crop_complete = row.get("crop_complete")

    clipped = False
    # Absolute: bbox too small to be usable.
    if subject_size < bbox_area_min:
        clipped = True
    # crop_complete < 0.6 signals the mask touches a frame edge (matches
    # the existing reject_crop_complete default in pipeline config).
    if crop_complete is not None and crop_complete < 0.60:
        clipped = True
    # Burst context: this frame's bbox is an order of magnitude smaller
    # than its siblings' median — the photographer lost framing.
    if in_burst:
        sibling_sizes = [
            s.get("subject_size") for s in siblings
            if s.get("subject_size")
        ]
        if sibling_sizes:
            import statistics
            median = statistics.median(sibling_sizes)
            if median > 0 and subject_size * 10 < median:
                clipped = True

    subject_t = row.get("subject_tenengrad") or 0.0
    bg_t = row.get("bg_tenengrad") or 0.0

    oof = False
    ratio_bad = bg_t > 0 and (subject_t / bg_t) < config["miss_oof_ratio"]
    # Absolute floor: below this, subject sharpness is motion-blur level.
    # Value chosen empirically to match reject_focus behavior.
    SHARPNESS_FLOOR = 10.0
    floor_bad = subject_t < SHARPNESS_FLOOR

    if in_burst:
        oof = ratio_bad or floor_bad
    else:
        oof = ratio_bad and floor_bad

    return {"no_subject": False, "clipped": clipped, "oof": oof}
