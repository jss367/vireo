"""Evidence packs for burst adjudication.

A burst is the run of frames a camera fired around one moment. Vireo's
scorer (``scoring.score_encounter`` through ``best_batch``) ranks the frames
of a burst from the pipeline's subject-masked quality features. An evidence
pack is that ranking plus everything a reviewer, human or agent, needs to
decide whether to trust it:

- ``coverage``: how many of the burst's frames actually have detection,
  mask, quality and eye features. A ranking over half a burst must say so
  ("no misses flagged" and "misses never computed" must never look alike).
- ``ambiguity``: the rule-based reasons the scorer's pick is not to be taken
  at face value (``assess_ambiguity``). Positive keeper validation is
  independent of relative ranking and is still required for automatic keeps.
- per-frame URLs for the 1:1 focus crop (``/photos/<id>/focus-crop``), the
  detection crop, the mask and the preview, so the reviewer judges
  sharpness from native pixels rather than a downscaled frame.

``group_folder_bursts`` chains one folder's frames into bursts with the same
rules ``best_batch.best_batch_scope`` applies around a seed photo, so the
folder listing (``/api/folders/<id>/bursts``) and the per-photo pack
(``/api/photos/<id>/burst-evidence``) agree on what a burst is.

Pure functions over database rows and the Best Batch response; the only
database access is through ``db.photos`` and ``build_best_batch_response``.
See ``docs/plans/2026-10-17-burst-evidence-adjudication-design.md``.
"""

import math
import os
import re

from best_batch import (
    DEFAULT_BEST_BATCH_MAX_PHOTOS,
    _filename_sequence_key,
    _time_gap_seconds,
    build_best_batch_response,
    limit_batch_rows,
)
from scoring import DEFAULTS as SCORING_DEFAULTS
from scoring import _normalize_scoring_config

#: Folders with more workspace-visible photos than this are refused by the
#: bursts listing: every burst is scored from its feature rows, and a listing
#: that size would be a job, not a request.
MAX_FOLDER_BURST_PHOTOS = 5000

#: Relative gap in the raw focus measurement (Tenengrad of the eye window or
#: of the subject mask) below which the top two frames are a close call: with
#: 0.10 the runner-up is within 10% of the leader. Composite quality is not
#: the right yardstick here because the scorer ranks it by percentile inside
#: the burst, so two frames 2% apart in sharpness sit 0.15 apart in quality.
DEFAULT_CLOSE_CALL_MARGIN = 0.10
MAX_CLOSE_CALL_MARGIN = 1.0

#: Side, in native pixels, of the default focus crop.
DEFAULT_FOCUS_CROP_SIZE = 512
MIN_FOCUS_CROP_SIZE = 128
MAX_FOCUS_CROP_SIZE = 1024

#: ``crop_complete`` below which the best frame's subject touches the edge.
EDGE_CROP_COMPLETE = 0.9

AMBIGUITY_CODES = (
    "positive_keep_not_validated",
    "unanalyzed_frames",
    "close_call",
    "no_eye_located",
    "subject_touches_edge",
    "multiple_subjects",
    "all_rejected",
    "subject_identity_uncertain",
    "subject_identity_conflict",
    "multiple_matching_subjects",
    "subject_analysis_missing",
)


def _row_extension(row):
    return (row["extension"] or os.path.splitext(row["filename"])[1]).lower()


def _time_ok(left, right, max_gap_seconds, *, require_timestamps):
    gap = _time_gap_seconds(left["timestamp"], right["timestamp"])
    if gap is None:
        return not require_timestamps
    return gap <= max_gap_seconds


def _sequence_bursts(rows, max_gap_seconds, max_sequence_gap):
    """Filename runs that qualify before Best Batch's capture-time fallback."""
    keyed = []
    for row in rows:
        key = _filename_sequence_key(row["filename"])
        if key:
            keyed.append(((key[0], _row_extension(row), key[2]), key[1], row))
    keyed.sort(key=lambda item: (item[0], item[1], item[2]["filename"], item[2]["id"]))

    bursts = []
    current = []
    prev = None
    for group, num, row in keyed:
        if (
            current
            and group == prev[0]
            and num - prev[1] <= max_sequence_gap
            and _time_ok(prev[2], row, max_gap_seconds, require_timestamps=False)
        ):
            current.append(row)
        else:
            if len(current) >= 2:
                bursts.append(current)
            current = [row]
        prev = (group, num, row)
    if len(current) >= 2:
        bursts.append(current)

    return bursts


def _capture_time_bursts(rows, max_gap_seconds):
    ordered = sorted(rows, key=lambda r: (
        r["timestamp"] is None, r["timestamp"] or "", r["filename"], r["id"],
    ))
    bursts = []
    current = []
    for row in ordered:
        if current and _time_ok(current[-1], row, max_gap_seconds, require_timestamps=True):
            current.append(row)
        else:
            if len(current) >= 2:
                bursts.append(current)
            current = [row]
    if len(current) >= 2:
        bursts.append(current)
    return bursts


def folder_burst_scopes(rows, max_gap_seconds=8.0, max_sequence_gap=2):
    """Distinct seed scopes, including capture-time fallbacks for numbered singletons.

    Best Batch may give a fallback seed a scope overlapping a filename run.
    Preserve both rather than omit that seed's neighbors or change its ranking.
    """
    rows = list(rows)
    runs = _sequence_bursts(rows, max_gap_seconds, max_sequence_gap)
    sequenced_ids = {r["id"] for run in runs for r in run}
    scopes = [{"seed_photo_id": run[0]["id"], "rows": run,
               "scope_method": "filename_sequence"} for run in runs]
    for run in _capture_time_bursts(rows, max_gap_seconds):
        seed = next((r for r in run if r["id"] not in sequenced_ids), None)
        if seed is not None:
            scopes.append({"seed_photo_id": seed["id"], "rows": run,
                           "scope_method": "capture_time"})
    return sorted(scopes, key=lambda scope: (
        scope["rows"][0]["filename"], scope["seed_photo_id"],
    ))


def group_folder_bursts(rows, max_gap_seconds=8.0, max_sequence_gap=2):
    """The distinct row groups reachable through Best Batch's seed scope rules."""
    return [scope["rows"] for scope in folder_burst_scopes(rows, max_gap_seconds, max_sequence_gap)]


def numeric_query_arg(args, name, default, convert, minimum, maximum):
    """Parse a finite bounded query value without silently replacing bad input."""
    if name not in args:
        return default
    try:
        value = convert(args.get(name))
        valid = math.isfinite(value) and minimum <= value <= maximum
    except (TypeError, ValueError, OverflowError):
        valid = False
    if not valid:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def coverage_counts(evidence_rows):
    """Feature coverage over ``analysis_evidence_rows`` rows (any iterable)."""
    rows = list(evidence_rows)
    return {
        "photos": len(rows),
        "with_detection": sum(1 for r in rows if (r["detection_count"] or 0) > 0),
        "with_mask": sum(1 for r in rows if r["has_mask"]),
        "with_quality": sum(1 for r in rows if r["has_quality"]),
        "with_eye": sum(1 for r in rows if r["has_eye"]),
    }


def _focus_measurements(first, second):
    """``(basis, s1, s2)``: the raw sharpness the two frames were ranked on.

    Both scored on an eye window compare eye Tenengrad; otherwise the whole
    subject's Tenengrad (``sharpness`` on a card), which is what the scorer
    fell back to for at least one of them.
    """
    if first.get("focus_basis") == "eye" and second.get("focus_basis") == "eye":
        return "eye", first.get("eye_tenengrad"), second.get("eye_tenengrad")
    return "subject", first.get("sharpness"), second.get("sharpness")


def assess_ambiguity(cards, coverage, *, eye_detect_enabled, margin=DEFAULT_CLOSE_CALL_MARGIN):
    """Why the scorer's pick for this burst should not be taken at face value.

    ``cards`` are the pack's cards in rank order (``label``, ``sharpness``,
    ``eye_tenengrad``, ``focus_basis``, ``crop_complete``,
    ``detection_count``). ``margin`` is the relative gap in raw sharpness
    below which the top two are a close call. Returns
    ``{"needs_review": bool, "reasons": [{"code", "detail"}]}``; the codes
    are ``AMBIGUITY_CODES``. ``no_eye_located`` only fires when the
    workspace has eye detection on: with it off every burst would carry the
    reason and it would mean nothing.
    """
    reasons = []
    photos = coverage.get("photos", 0)
    with_quality = coverage.get("with_quality", 0)
    if with_quality < photos:
        missing = photos - with_quality
        reasons.append({
            "code": "unanalyzed_frames",
            "detail": (
                f"{missing} of {photos} frames have no current quality analysis; "
                "the ranking covers only the analyzed ones"
            ),
        })

    cards = list(cards or [])
    for card in cards:
        status = card.get("evidence_status")
        if status and status != "ready":
            reasons.append({"code": status, "detail": f"{card.get('filename')}: {status.replace('_', ' ')}"})
    ranked = [c for c in cards if c.get("label") != "REJECT" and c.get("selection_eligible") is not False
              and c.get("evidence_status", "ready") == "ready"
              and ("evidence_status" not in c or c.get("rank") is not None)]
    # Scoring stores only hard failures in reject_reasons. Rarity protection
    # can promote REJECT to REVIEW while retaining those failures.
    if cards and all(c.get("label") == "REJECT" or c.get("reject_reasons") for c in cards):
        reasons.append({
            "code": "all_rejected",
            "detail": "every frame failed a hard-reject rule",
        })
    if len(ranked) >= 2:
        first, second = ranked[0], ranked[1]
        basis, s1, s2 = _focus_measurements(first, second)
        if s1 is not None and s2 is not None and s1 > 0:
            gap = abs(s1 - s2) / max(s1, s2)
            if gap < margin:
                reasons.append({
                    "code": "close_call",
                    "detail": (
                        f"{first.get('filename')} and {second.get('filename')} are "
                        f"{gap:.0%} apart in {basis} sharpness ({s1:.0f} vs {s2:.0f}; "
                        f"margin {margin:.0%})"
                    ),
                })

    best = ranked[0] if ranked else (cards[0] if cards else None)
    if best is not None:
        if eye_detect_enabled and best.get("focus_basis") != "eye":
            reasons.append({
                "code": "no_eye_located",
                "detail": (
                    f"focus of {best.get('filename')} is scored on the whole "
                    "subject; no eye was located"
                ),
            })
        crop_complete = best.get("crop_complete")
        if crop_complete is not None and crop_complete < EDGE_CROP_COMPLETE:
            reasons.append({
                "code": "subject_touches_edge",
                "detail": (
                    f"subject of {best.get('filename')} touches the frame edge "
                    f"(crop_complete {crop_complete:.2f})"
                ),
            })
        if (best.get("detection_count") or 0) >= 2 and not best.get("subject"):
            reasons.append({
                "code": "multiple_subjects",
                "detail": (
                    f"{best.get('filename')} has {best['detection_count']} animal "
                    "detections; the scorer may have scored the wrong one"
                ),
            })

    return {"needs_review": bool(reasons), "reasons": reasons}


def card_urls(photo_id, *, has_mask, focus_size=DEFAULT_FOCUS_CROP_SIZE, mask_variant=None):
    """The image URLs a reviewer needs for one frame."""
    focus = f"/photos/{photo_id}/focus-crop?size={int(focus_size)}"
    return {
        "thumbnail_url": f"/thumbnails/{photo_id}.jpg",
        "preview_url": f"/photos/{photo_id}/preview?size=1920",
        "crop_url": f"/photos/{photo_id}/crop",
        "focus_crop_url": focus,
        "focus_crop_scale": {"reference": "decoded_raster", "scale": 1, "original_scale": None},
        "focus_crop_mask_url": f"{focus}&mask=1" if has_mask else None,
        "mask_url": (
            f"/api/masks/{photo_id}/{mask_variant}.png"
            if has_mask and isinstance(mask_variant, str)
            and re.fullmatch(r"[A-Za-z0-9_-]+", mask_variant) else None
        ),
    }


def _attach_evidence(card, evidence, focus_size):
    card["has_mask"] = bool(evidence["has_mask"]) if evidence else False
    eligible = card.get("selection_eligible") is not False
    card["has_quality"] = bool(evidence["has_quality"]) and eligible if evidence else False
    card["has_eye"] = bool(evidence["has_eye"]) and eligible if evidence else False
    card["detection_count"] = int(evidence["detection_count"] or 0) if evidence else 0
    if evidence:
        card.setdefault("crop_complete", evidence["crop_complete"])
        card.setdefault("subject_size", evidence["subject_size"])
    else:
        card.setdefault("crop_complete", None)
        card.setdefault("subject_size", None)
    # sqlite3.Row iterates values, so test its keys explicitly.
    variant = evidence["mask_variant"] if evidence and "mask_variant" in evidence.keys() else None  # noqa: SIM118
    card.update(card_urls(card["id"], has_mask=card["has_mask"], focus_size=focus_size, mask_variant=variant))
    return card


def _scoring_settings(effective_cfg, margin):
    settings = {**SCORING_DEFAULTS, **_normalize_scoring_config(effective_cfg)}
    try:
        min_conf = float(effective_cfg.get("detector_confidence", 0.2))
    except (TypeError, ValueError):
        min_conf = 0.2
    return {
        "eye_detect_enabled": bool(settings.get("eye_detect_enabled", False)),
        "close_call_margin": margin,
        "detector_confidence": min_conf,
        "weights": {k: settings[k] for k in SCORING_DEFAULTS if k.startswith("w_")},
        "rejection_thresholds": {k: settings[k] for k in SCORING_DEFAULTS if k.startswith("reject_")},
    }


def build_evidence_pack(
    db, seed_photo_id, rows, *, effective_cfg, margin=DEFAULT_CLOSE_CALL_MARGIN,
    focus_size=DEFAULT_FOCUS_CROP_SIZE, evidence_rows=None, comparison=None,
):
    """The evidence pack for the burst ``rows`` (Best Batch scope rows).

    Wraps ``build_best_batch_response``: when the burst can be scored the
    pack is that response with ``coverage``, ``ambiguity``, ``scoring`` and
    per-card evidence and URLs added; when it cannot (fewer than two frames
    with pipeline features) ``scored`` is False, ``unscored_reason`` says
    why, ``best_photo_id`` is None and the cards are unranked frames, so a
    folder listing can still report the burst and its coverage.

    ``evidence_rows`` lets a caller that already fetched
    ``analysis_evidence_rows`` for a wider set pass it in.
    """
    rows = list(rows)
    photo_ids = [row["id"] for row in rows]
    scoring = _scoring_settings(effective_cfg, margin)
    if comparison is not None:
        return _subject_evidence_pack(db, seed_photo_id, rows, comparison, scoring, margin, focus_size)
    if evidence_rows is None:
        evidence_rows = db.photos.analysis_evidence_rows(photo_ids, scoring["detector_confidence"])

    result, error = build_best_batch_response(db, seed_photo_id, rows)
    if error:
        cards = []
        for row in rows:
            card = {
                "id": row["id"],
                "filename": row["filename"],
                "rank": None,
                "role": None,
                "label": None,
                "selection_eligible": False,
                "quality": None,
                "quality_pct": None,
                "focus": None,
                "focus_basis": None,
                "reasons": [],
                "reject_reasons": [],
                "flag": row["flag"] or "none",
                "rating": row["rating"],
            }
            cards.append(_attach_evidence(card, evidence_rows.get(row["id"]), focus_size))
        sequence_nums = [
            key[1] for key in (_filename_sequence_key(row["filename"]) for row in rows) if key
        ]
        pack = {
            "seed_photo_id": seed_photo_id,
            "photo_ids": photo_ids,
            "count": len(photo_ids),
            "sequence_range": (
                [min(sequence_nums), max(sequence_nums)] if sequence_nums else None
            ),
            "scored": False,
            "unscored_reason": error,
            "best_photo_id": None,
            "best_filename": None,
            "best_reasons": [],
            "cards": cards,
            "alternate_ids": [],
            "suggested_reject_ids": [],
        }
    else:
        pack = result
        pack["scored"] = True
        pack["unscored_reason"] = None
        for card in pack["cards"]:
            _attach_evidence(card, evidence_rows.get(card["id"]), focus_size)

    coverage = coverage_counts(pack["cards"])
    coverage["photos"] = len(photo_ids)
    pack["coverage"] = coverage
    pack["ambiguity"] = assess_ambiguity(
        pack["cards"], coverage,
        eye_detect_enabled=scoring["eye_detect_enabled"], margin=margin,
    )
    if pack.get("ranking_status") == "provisional":
        pack["ambiguity"]["needs_review"] = True
        pack["ambiguity"]["reasons"].append({
            "code": "positive_keep_not_validated", "detail": pack["keep_review_message"],
        })
    pack["scoring"] = scoring
    return pack


def _subject_evidence_pack(db, seed_photo_id, rows, comparison, scoring, margin, focus_size):
    from subject_ranking import subject_urls
    pack, _ = build_best_batch_response(db, seed_photo_id, rows, comparison=comparison)
    for card in pack["cards"]:
        card.update(subject_urls(card["id"], card["subject"], focus_size))
    pack["coverage"] = coverage_counts(pack["cards"])
    pack["coverage"]["with_selected_subject"] = sum(c["subject"]["detection_id"] is not None for c in pack["cards"])
    pack["ambiguity"] = assess_ambiguity(pack["cards"], pack["coverage"],
                                         eye_detect_enabled=scoring["eye_detect_enabled"], margin=margin)
    pack["scoring"] = scoring
    return pack


def _flag_counts(rows):
    counts = {"none": 0, "flagged": 0, "rejected": 0}
    for row in rows:
        flag = row["flag"] or "none"
        counts[flag] = counts.get(flag, 0) + 1
    return counts


def build_folder_bursts(
    db, folder_id, *, effective_cfg, margin=DEFAULT_CLOSE_CALL_MARGIN,
    max_gap_seconds=8.0, max_sequence_gap=2,
):
    """Every burst in one folder, each scored and assessed, plus folder coverage.

    Raises ``ValueError`` when the folder holds more than
    ``MAX_FOLDER_BURST_PHOTOS`` workspace-visible photos.
    """
    rows = db.photos.best_batch_folder_rows(folder_id, limit=MAX_FOLDER_BURST_PHOTOS + 1)
    if len(rows) > MAX_FOLDER_BURST_PHOTOS:
        raise ValueError(
            f"folder has more than {MAX_FOLDER_BURST_PHOTOS} photos; the bursts listing covers folders "
            f"of up to {MAX_FOLDER_BURST_PHOTOS}"
        )
    scoring = _scoring_settings(effective_cfg, margin)
    evidence_rows = db.photos.analysis_evidence_rows(
        [row["id"] for row in rows], scoring["detector_confidence"],
    )
    folder_coverage = coverage_counts(evidence_rows.values())
    folder_coverage["photos"] = len(rows)

    bursts = []
    photos_in_bursts = set()
    for scope in folder_burst_scopes(rows, max_gap_seconds, max_sequence_gap):
        burst_rows = limit_batch_rows(scope["rows"], scope["seed_photo_id"])
        pack = build_evidence_pack(
            db, scope["seed_photo_id"], burst_rows,
            effective_cfg=effective_cfg, margin=margin, evidence_rows=evidence_rows,
        )
        photos_in_bursts.update(r["id"] for r in scope["rows"])
        bursts.append({
            "seed_photo_id": scope["seed_photo_id"],
            "scope_method": scope["scope_method"],
            "scope_max_photos": DEFAULT_BEST_BATCH_MAX_PHOTOS,
            "burst_photo_count": len(scope["rows"]),
            "photo_ids": pack["photo_ids"],
            "count": pack["count"],
            "filenames": [burst_rows[0]["filename"], burst_rows[-1]["filename"]],
            "sequence_range": pack.get("sequence_range"),
            "flags": _flag_counts(burst_rows),
            "coverage": pack["coverage"],
            "scored": pack["scored"],
            "best_photo_id": pack["best_photo_id"],
            "ranked_candidate_id": pack.get("ranked_candidate_id"),
            "ranking_status": pack.get("ranking_status"),
            "ambiguity": pack["ambiguity"],
        })
    return {
        "folder_id": folder_id,
        "photo_count": len(rows),
        "coverage": folder_coverage,
        "burst_count": len(bursts),
        "photos_in_bursts": len(photos_in_bursts),
        "ambiguous_burst_count": sum(1 for b in bursts if b["ambiguity"]["needs_review"]),
        "bursts": bursts,
        "scoring": scoring,
    }
