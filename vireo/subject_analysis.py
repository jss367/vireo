"""Read-only readiness policy shared by scoring, selection and recovery.

A completed empty detector run is evidence, not a failed photograph. Missing
measurements never stand in for measured zeroes. Catalog loaders supply stage
provenance against the selected primary and current mask settings. Legacy
callers also need affirmative detector evidence, not just a cached mask.
"""

import math
from collections.abc import Mapping
from typing import Any

REQUIRED_QUALITY_FIELDS = (
    "subject_tenengrad", "bg_tenengrad", "subject_clip_high",
    "subject_clip_low", "subject_y_median", "crop_complete",
    "bg_separation", "subject_size",
)
SCORE_FIELDS = (
    "focus_score", "eye_focus_score", "exposure_score", "composition_score",
    "area_score", "noise_score", "quality_composite",
)


def explicit_decision(photo: Mapping[str, Any]) -> str | None:
    """Return a persisted photographer decision, never a prior AI label."""
    if photo.get("flag") in ("flagged", "picked"):
        return "KEEP"
    if photo.get("flag") == "rejected":
        return "REJECT"
    return None


def subject_analysis_readiness(photo: Mapping[str, Any]) -> dict[str, Any]:
    """Return ``status``, machine-readable ``reasons`` and ``eligible``.

    Status is not_run, failed, incomplete or complete. ``complete`` can still
    be ineligible: the explicit no-qualifying-subject policy is manual review.
    Stage failures must be supplied as provenance, never guessed from NULLs.
    Noise estimates and eye measurements are optional (their existing body/
    background fallbacks remain valid); all basic quality inputs are required.
    """
    provenance = photo.get("analysis_provenance") or {}
    detection = provenance.get("detection", {})
    if not detection and not (photo.get("detection_box") or photo.get("subject_present")
                              or photo.get("subject_absent")):
        return {"status": "not_run", "reasons": ["detection_not_run"], "eligible": False}
    segmentation = provenance.get("segmentation", {})
    for stage, evidence in (("detection", detection), ("segmentation", segmentation)):
        if stage == "segmentation" and (detection.get("no_qualifying_subject")
                or (not detection and photo.get("subject_absent"))):
            return {"status": "complete", "reasons": ["no_qualifying_subject"], "eligible": False}
        status = evidence.get("status")
        if status in ("not_run", "failed", "incomplete"):
            return {"status": status, "reasons": [f"{stage}_{status}"], "eligible": False}
        if evidence.get("current") is False:
            return {"status": "incomplete", "reasons": [f"{stage}_stale"], "eligible": False}
    if not (photo.get("mask_path") or photo.get("subject_mask_available")):
        status = "incomplete" if detection or photo.get("detection_box") else "not_run"
        return {"status": status, "reasons": ["missing_subject_mask"], "eligible": False}
    missing = [field for field in REQUIRED_QUALITY_FIELDS if not _measured(photo.get(field))]
    if missing:
        return {"status": "incomplete", "reasons": [f"missing_{field}" for field in missing], "eligible": False}
    return {"status": "complete", "reasons": [], "eligible": True}


def _measured(value: Any) -> bool:
    """Zero is measured; nulls, nonnumbers and nonfinite values are unavailable."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def mark_analysis_readiness(photo: dict[str, Any]) -> bool:
    """Refresh derived readiness, clearing stale scores for unavailable analysis."""
    readiness = subject_analysis_readiness(photo)
    photo["analysis_status"] = readiness["status"]
    photo["analysis_reasons"] = readiness["reasons"]
    photo["selection_eligible"] = readiness["eligible"]
    if not readiness["eligible"]:
        for field in SCORE_FIELDS:
            photo[field] = None
        photo["reject_reasons"] = []
        photo["label"] = explicit_decision(photo) or "REVIEW"
    return readiness["eligible"]
