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

import logging
from collections import defaultdict
from datetime import UTC, datetime

log = logging.getLogger(__name__)


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
    # NULL quality features mean the pipeline hasn't measured them yet;
    # absence of evidence is not evidence of a miss. Each signal below
    # only fires when its required feature is actually present.
    subject_size = row.get("subject_size")
    crop_complete = row.get("crop_complete")

    clipped = False
    # Absolute: bbox too small to be usable.
    if subject_size is not None and subject_size < bbox_area_min:
        clipped = True
    # crop_complete < 0.6 signals the mask touches a frame edge (matches
    # the existing reject_crop_complete default in pipeline config).
    if crop_complete is not None and crop_complete < 0.60:
        clipped = True
    # Burst context: this frame's bbox is an order of magnitude smaller
    # than its siblings' median — the photographer lost framing. Only
    # evaluate when both this row and its siblings have a measured size
    # (zero is a legitimate measurement, so filter by `is not None`).
    if in_burst and subject_size is not None:
        sibling_sizes = [
            s.get("subject_size") for s in siblings
            if s.get("subject_size") is not None
        ]
        if sibling_sizes:
            import statistics
            median = statistics.median(sibling_sizes)
            if median > 0 and subject_size * 10 < median:
                clipped = True

    subject_t = row.get("subject_tenengrad")
    bg_t = row.get("bg_tenengrad")

    oof = False
    # Only evaluate OOF when both Tenengrad features are measured.
    if subject_t is not None and bg_t is not None:
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


def compute_misses_for_workspace(db, pipeline_config):
    """Compute and persist miss flags for photos in the active workspace.

    Reads per-photo features from `photos` (restricted to folders linked to
    the active workspace), groups by `burst_id`, calls classify_miss for
    each photo with its siblings as context, then writes the three flags
    and a timestamp in a single batch.

    Singletons (burst_id IS NULL) are evaluated alone, which triggers the
    stricter singleton thresholds inside classify_miss.
    """
    if not pipeline_config.get("miss_enabled", True):
        log.info("Miss detection disabled via miss_enabled=false")
        return 0

    rows = db.conn.execute(
        "SELECT p.id, p.burst_id, p.detection_conf, p.subject_size, "
        "       p.crop_complete, p.subject_tenengrad, p.bg_tenengrad "
        "FROM photos p "
        "JOIN workspace_folders wf ON wf.folder_id = p.folder_id "
        "WHERE wf.workspace_id = ?",
        (db._ws_id(),),
    ).fetchall()

    by_burst = defaultdict(list)
    singletons = []
    for r in rows:
        d = dict(r)
        if d["burst_id"]:
            by_burst[d["burst_id"]].append(d)
        else:
            singletons.append(d)

    now = datetime.now(UTC).isoformat(timespec="seconds")
    updates = []

    for burst_rows in by_burst.values():
        for row in burst_rows:
            siblings = [s for s in burst_rows if s["id"] != row["id"]]
            flags = classify_miss(row, siblings, pipeline_config)
            updates.append((
                int(flags["no_subject"]),
                int(flags["clipped"]),
                int(flags["oof"]),
                now,
                row["id"],
            ))

    for row in singletons:
        flags = classify_miss(row, siblings=[], config=pipeline_config)
        updates.append((
            int(flags["no_subject"]),
            int(flags["clipped"]),
            int(flags["oof"]),
            now,
            row["id"],
        ))

    db.conn.executemany(
        "UPDATE photos SET miss_no_subject=?, miss_clipped=?, miss_oof=?, "
        "miss_computed_at=? WHERE id=?",
        updates,
    )
    db.conn.commit()
    return len(updates)
