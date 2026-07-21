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

import json
import logging
from collections import defaultdict
from datetime import UTC, datetime

import config as _cfg
from db import commit_with_retry

log = logging.getLogger(__name__)

MISS_CONFIG_KEYS = {
    "miss_enabled",
    "miss_det_confidence",
    "miss_det_confidence_burst",
    "miss_bbox_area_min",
    "miss_bbox_area_min_singleton",
    "miss_oof_ratio",
    "miss_classifier_override_conf",
}


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


def miss_config_from_effective(effective_config):
    """Return the miss-related config values from an effective config dict."""
    pipeline = effective_config.get("pipeline", {})
    if not isinstance(pipeline, dict):
        pipeline = {}
    values = {
        key: _miss_threshold(pipeline, key)
        for key in MISS_CONFIG_KEYS
        if key != "miss_enabled"
    }
    values["miss_enabled"] = pipeline.get(
        "miss_enabled", _cfg.DEFAULTS["pipeline"]["miss_enabled"]
    )
    values["detector_confidence"] = effective_config.get(
        "detector_confidence", _cfg.DEFAULTS["detector_confidence"]
    )
    return values


def _effective_detector_confidence(db):
    """Read the active workspace detector floor via the shared miss config."""
    import config as cfg

    return miss_config_from_effective(
        db.get_effective_config(cfg.load())
    )["detector_confidence"]


def _fetch_workspace_miss_rows(db, detector_confidence=None):
    """Fetch all active-workspace photo rows needed to derive miss flags."""
    if detector_confidence is None:
        detector_confidence = _effective_detector_confidence(db)
    ws_id = db._ws_id()
    rows = db.conn.execute(
        "SELECT p.id, p.folder_id, p.filename, p.companion_path, "
        "       p.timestamp, p.burst_id, "
        "       p.subject_size, p.crop_complete, "
        "       p.subject_tenengrad, p.bg_tenengrad, "
        "       p.miss_no_subject, p.miss_clipped, p.miss_oof, "
        "       p.miss_computed_at, p.flag, "
        "       (SELECT MAX(d.detector_confidence) FROM detections d "
        "        WHERE d.photo_id = p.id "
        "          AND d.detector_confidence >= ?) "
        "         AS detection_conf, "
        "       (SELECT MAX(pr.confidence) FROM predictions pr "
        "        JOIN detections d2 ON d2.id = pr.detection_id "
        "        WHERE d2.photo_id = p.id) "
        "         AS max_prediction_conf "
        "FROM photos p "
        "JOIN workspace_folders wf ON wf.folder_id = p.folder_id "
        "WHERE wf.workspace_id = ?",
        (detector_confidence, ws_id),
    ).fetchall()
    return [dict(r) for r in rows]


def _target_ids_from_scope(rows, collection_id=None, db=None, since=None,
                           photo_ids=None):
    target_ids = set(photo_ids) if photo_ids is not None else None
    if collection_id is not None:
        collection_ids = db.collection_photo_ids(collection_id)
        target_ids = (
            collection_ids if target_ids is None else target_ids & collection_ids
        )
    if since is not None:
        since_ids = {
            r["id"] for r in rows
            if r.get("miss_computed_at") is not None
            and r.get("miss_computed_at") >= since
        }
        target_ids = since_ids if target_ids is None else target_ids & since_ids
    return target_ids


def _derive_miss_updates(rows, pipeline_config, target_ids=None,
                         exclude_photo_ids=None, now=None):
    """Classify rows and return update tuples plus flags by photo id."""
    excluded = set(exclude_photo_ids) if exclude_photo_ids else set()
    override_conf = _miss_threshold(
        pipeline_config, "miss_classifier_override_conf"
    )

    by_burst = defaultdict(list)
    singletons = []
    for row in rows:
        if row["burst_id"]:
            by_burst[row["burst_id"]].append(row)
        else:
            singletons.append(row)

    if now is None:
        now = datetime.now(UTC).isoformat(timespec="microseconds")
    updates = []
    flags_by_id = {}

    def _apply_classifier_override(row, flags):
        if not flags["no_subject"]:
            return flags
        max_pred = row.get("max_prediction_conf")
        if max_pred is not None and max_pred >= override_conf:
            return {**flags, "no_subject": False}
        return flags

    def _should_update(row):
        if target_ids is not None and row["id"] not in target_ids:
            return False
        return row["id"] not in excluded

    def _classify(row, siblings):
        flags = classify_miss(row, siblings, pipeline_config)
        return _apply_classifier_override(row, flags)

    for burst_rows in by_burst.values():
        for row in burst_rows:
            if not _should_update(row):
                continue
            siblings = [s for s in burst_rows if s["id"] != row["id"]]
            flags = _classify(row, siblings)
            flags_by_id[row["id"]] = flags
            updates.append((
                int(flags["no_subject"]),
                int(flags["clipped"]),
                int(flags["oof"]),
                now,
                row["id"],
            ))

    for row in singletons:
        if not _should_update(row):
            continue
        flags = _classify(row, siblings=[])
        flags_by_id[row["id"]] = flags
        updates.append((
            int(flags["no_subject"]),
            int(flags["clipped"]),
            int(flags["oof"]),
            now,
            row["id"],
        ))

    return updates, flags_by_id


def _attach_primary_detections(db, photos):
    if not photos:
        return
    photo_ids = [p["id"] for p in photos]
    primary = {}
    CHUNK = 500
    for i in range(0, len(photo_ids), CHUNK):
        chunk = photo_ids[i:i + CHUNK]
        placeholders = ",".join("?" * len(chunk))
        det_rows = db.conn.execute(
            f"SELECT photo_id, box_x, box_y, box_w, box_h, "
            f"       detector_confidence "
            f"FROM detections "
            f"WHERE photo_id IN ({placeholders}) "
            f"ORDER BY photo_id, detector_confidence DESC",
            chunk,
        ).fetchall()
        for d in det_rows:
            primary.setdefault(d["photo_id"], d)
    for p in photos:
        d = primary.get(p["id"])
        if d is None:
            p["detection_box"] = None
            p["detection_conf"] = None
        else:
            p["detection_box"] = json.dumps({
                "x": d["box_x"], "y": d["box_y"],
                "w": d["box_w"], "h": d["box_h"],
            })
            p["detection_conf"] = d["detector_confidence"]


def preview_misses_for_workspace(db, pipeline_config, detector_confidence=None,
                                 since=None, photo_ids=None):
    """Return dynamically derived misses without writing DB flags."""
    if not pipeline_config.get("miss_enabled", True):
        return {"no_subject": [], "clipped": [], "oof": []}
    if detector_confidence is None:
        detector_confidence = _effective_detector_confidence(db)
    rows = _fetch_workspace_miss_rows(
        db, detector_confidence=detector_confidence
    )
    target_ids = _target_ids_from_scope(
        rows, since=since, photo_ids=photo_ids
    )
    _, flags_by_id = _derive_miss_updates(
        rows, pipeline_config, target_ids=target_ids
    )
    grouped = {"no_subject": [], "clipped": [], "oof": []}
    for row in rows:
        if row["id"] not in flags_by_id:
            continue
        if row.get("flag") == "rejected":
            continue
        flags = flags_by_id[row["id"]]
        photo = {
            key: row.get(key)
            for key in (
                "id", "folder_id", "filename", "companion_path",
                "timestamp", "burst_id", "subject_size", "crop_complete",
                "subject_tenengrad", "bg_tenengrad", "miss_computed_at",
                "flag",
            )
        }
        photo["miss_no_subject"] = int(flags["no_subject"])
        photo["miss_clipped"] = int(flags["clipped"])
        photo["miss_oof"] = int(flags["oof"])
        if flags["no_subject"]:
            grouped["no_subject"].append(dict(photo))
        if flags["clipped"]:
            grouped["clipped"].append(dict(photo))
        if flags["oof"]:
            grouped["oof"].append(dict(photo))

    for photos in grouped.values():
        photos.sort(
            key=lambda p: (
                p.get("timestamp") or "",
                p.get("filename") or "",
                p.get("id") or 0,
            ),
            reverse=True,
        )
        _attach_primary_detections(db, photos)
    return grouped


def compute_misses_for_workspace(
    db, pipeline_config, collection_id=None, exclude_photo_ids=None, now=None,
    detector_confidence=None, since=None, photo_ids=None,
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

    `photo_ids` accepts an already-resolved intersection of Misses-page
    filters. It composes with `collection_id` and `since` when supplied.

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

    # Detections are global now; scope *photos* to the workspace via
    # workspace_folders and filter detections at read time by the
    # workspace-effective detector_confidence threshold. Photos whose
    # highest-confidence box is below threshold are legitimate no_subject
    # candidates.
    if detector_confidence is None:
        detector_confidence = _effective_detector_confidence(db)
    # max_prediction_conf is the highest classifier confidence across all
    # detections of the photo, ignoring the workspace detector_confidence
    # cutoff. A classifier prediction at or above
    # `miss_classifier_override_conf` overrides the no_subject flag — the
    # canonical case is a hummingbird where megadetector returned a
    # below-threshold box but BioCLIP identified the species with high
    # confidence on that same box.
    rows = _fetch_workspace_miss_rows(
        db, detector_confidence=detector_confidence
    )
    target_ids = _target_ids_from_scope(
        rows, collection_id=collection_id, db=db, since=since,
        photo_ids=photo_ids,
    )

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
    updates, _ = _derive_miss_updates(
        rows, pipeline_config, target_ids=target_ids,
        exclude_photo_ids=exclude_photo_ids, now=now,
    )

    db.conn.executemany(
        "UPDATE photos SET miss_no_subject=?, miss_clipped=?, miss_oof=?, "
        "miss_computed_at=? WHERE id=?",
        updates,
    )
    commit_with_retry(db.conn)
    return len(updates)
