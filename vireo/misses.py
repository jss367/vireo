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
    return {"no_subject": False, "clipped": False, "oof": False}
