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

import config as _cfg
from db import commit_with_retry

log = logging.getLogger(__name__)


def _miss_threshold(config, key):
    """Read a miss-* threshold with a defaults fallback.

    Pipeline configs reaching classify_miss can be partial — e.g.
    `/api/pipeline/config` stores only model keys under `pipeline` and
    `Database.get_effective_config` does a shallow top-level merge.
    Fall back to the module DEFAULTS rather than raising KeyError and
    failing the whole pipeline job.
    """
    if key in config:
        return config[key]
    return _cfg.DEFAULTS["pipeline"][key]


def classify_miss(row, siblings, config):
    """Return {'no_subject': bool, 'clipped': bool, 'oof': bool}.

    Args:
        row: dict with detection_conf, subject_size, crop_complete,
            subject_tenengrad, bg_tenengrad, burst_id.
        siblings: list of sibling rows in the same burst (excluding `row`).
        config: dict with miss_* thresholds. Missing keys fall back to
            config.DEFAULTS["pipeline"] so partial pipeline configs
            (the common case) don't crash this stage.
    """
    in_burst = bool(siblings)
    conf_threshold = _miss_threshold(
        config,
        "miss_det_confidence_burst" if in_burst else "miss_det_confidence",
    )
    detection_conf = row.get("detection_conf") or 0.0
    if detection_conf < conf_threshold:
        return {"no_subject": True, "clipped": False, "oof": False}
    bbox_area_min = _miss_threshold(
        config,
        "miss_bbox_area_min" if in_burst else "miss_bbox_area_min_singleton",
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
        ratio_bad = bg_t > 0 and (subject_t / bg_t) < _miss_threshold(
            config, "miss_oof_ratio"
        )
        # Absolute floor: below this, subject sharpness is motion-blur level.
        # Value chosen empirically to match reject_focus behavior.
        SHARPNESS_FLOOR = 10.0
        floor_bad = subject_t < SHARPNESS_FLOOR

        if in_burst:
            oof = ratio_bad or floor_bad
        else:
            oof = ratio_bad and floor_bad

    return {"no_subject": False, "clipped": clipped, "oof": oof}


def compute_misses_for_workspace(
    db, pipeline_config, collection_id=None, exclude_photo_ids=None, now=None,
):
    """Compute and persist miss flags for photos in the active workspace.

    Reads per-photo features from `photos` (restricted to folders linked to
    the active workspace), groups by `burst_id`, calls classify_miss for
    each photo with its siblings as context, then writes the three flags
    and a timestamp in a single batch.

    Singletons (burst_id IS NULL) are evaluated alone, which triggers the
    singleton-specific thresholds inside classify_miss.

    When `collection_id` is given, only photos in that collection have
    their flags and `miss_computed_at` rewritten. Other workspace photos
    still contribute burst-sibling context to the classifier, but are
    not touched — so a partial pipeline run does not stamp
    `miss_computed_at` on photos outside its scope, which would
    otherwise defeat the `/misses?since=` review-window filter.

    `exclude_photo_ids` mirrors the preview-deselection filter applied by
    earlier pipeline stages (`params.exclude_photo_ids`). Those photos
    still contribute burst-sibling context but are not written to, so a
    run with deselections does not resurface or mass-flag deselected
    photos in /misses.

    `now` lets the caller inject the timestamp that will be written to
    `miss_computed_at`. The pipeline job passes the same value into both
    this function and the saved pipeline-results cache so the review
    UI's run-scoped `?since=` window matches what the DB stores.
    """
    if not pipeline_config.get("miss_enabled", True):
        log.info("Miss detection disabled via miss_enabled=false")
        return 0

    excluded = set(exclude_photo_ids) if exclude_photo_ids else set()

    ws_id = db._ws_id()
    # Detections are global now; scope *photos* to the workspace via
    # workspace_folders and filter detections at read time by the
    # workspace-effective detector_confidence threshold. Photos whose
    # highest-confidence box is below threshold are legitimate no_subject
    # candidates.
    import config as cfg
    min_conf = db.get_effective_config(cfg.load()).get(
        "detector_confidence", 0.2
    )
    rows = db.conn.execute(
        "SELECT p.id, p.burst_id, "
        "       (SELECT MAX(d.detector_confidence) FROM detections d "
        "        WHERE d.photo_id = p.id "
        "          AND d.detector_confidence >= ?) "
        "         AS detection_conf, "
        "       p.subject_size, p.crop_complete, "
        "       p.subject_tenengrad, p.bg_tenengrad "
        "FROM photos p "
        "JOIN workspace_folders wf ON wf.folder_id = p.folder_id "
        "WHERE wf.workspace_id = ?",
        (min_conf, ws_id),
    ).fetchall()

    target_ids = None
    if collection_id is not None:
        target_ids = db.collection_photo_ids(collection_id)

    by_burst = defaultdict(list)
    singletons = []
    for r in rows:
        d = dict(r)
        if d["burst_id"]:
            by_burst[d["burst_id"]].append(d)
        else:
            singletons.append(d)

    # Microsecond precision: the /misses?since=... review window uses
    # the earliest miss_computed_at from a run as a lower bound, and
    # bulk-reject reuses the same value. With seconds precision two runs
    # finishing in the same second collide, so the second run's window
    # would include the first run's misses (and bulk-reject could touch
    # them). ISO-8601 timestamps still sort lexicographically when
    # precision varies, so this is backward-compatible with rows written
    # at seconds precision.
    if now is None:
        now = datetime.now(UTC).isoformat(timespec="microseconds")
    updates = []

    for burst_rows in by_burst.values():
        for row in burst_rows:
            if target_ids is not None and row["id"] not in target_ids:
                continue
            if row["id"] in excluded:
                continue
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
        if target_ids is not None and row["id"] not in target_ids:
            continue
        if row["id"] in excluded:
            continue
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
    commit_with_retry(db.conn)
    return len(updates)
