"""Bounded identification evidence, without inferring same-animal continuity.

Reuse Review's subject assessments, score meanings and disagreement checks.
Candidate photo sets are an index into observed predictions, never an approved
propagation scope. Reading this packet neither classifies nor confirms photos.
"""

from __future__ import annotations

import os
from collections import Counter, defaultdict
from typing import TYPE_CHECKING

import config as cfg
from focus_crop import valid_subject_box
from labels import get_active_labels, get_saved_labels, load_merged_labels
from labels_fingerprint import LEGACY_SENTINEL, TOL_SENTINEL, compute_fingerprint
from services import detection_store, workspace_config
from services.prediction_evidence import attach_prediction_evidence
from services.service_types import JsonDict
from species_identity import SpeciesResolver

if TYPE_CHECKING:
    from db import Database

MAX_REVIEW_PHOTOS = 100


def selection_ids(body: object) -> list[int]:
    """Reject ambiguous or oversized scopes rather than silently truncating."""
    if not isinstance(body, dict):
        raise ValueError("request body must be an object")
    ids = body.get("photo_ids")
    if not isinstance(ids, list) or not 1 <= len(ids) <= MAX_REVIEW_PHOTOS:
        raise ValueError(f"photo_ids must contain 1–{MAX_REVIEW_PHOTOS} unique photo IDs")
    if any(type(pid) is not int or not 0 < pid < 2**63 for pid in ids):
        raise ValueError("photo_ids must contain positive integers")
    if len(set(ids)) != len(ids):
        raise ValueError("photo_ids must be unique")
    return ids


def _label_selection(db: Database) -> JsonDict:
    """Describe workspace defaults separately from the stored prediction run."""
    paths = workspace_config.get_workspace_active_labels(db)
    saved = {meta["labels_file"]: meta for meta in get_saved_labels()}
    metas = ([saved.get(path, {"labels_file": path}) for path in paths]
             if paths is not None else get_active_labels())
    result: JsonDict = {
        "source": "workspace" if paths is not None else "global",
        "scope": "Current defaults; an individual classification job can select different lists.",
        "lists": [{"name": meta.get("name") or os.path.basename(meta["labels_file"]),
                   "labels_file": meta["labels_file"],
                   "available": os.path.isfile(meta["labels_file"])} for meta in metas],
        "fingerprint": None, "label_count": None, "error": None,
    }
    try:
        labels = load_merged_labels(metas) if metas else []
    except (OSError, ValueError, TypeError) as exc:
        result["error"] = f"Could not read selected species lists: {exc}"
    else:
        result["label_count"] = len(labels)
        if labels:
            result["fingerprint"] = compute_fingerprint(labels)
    return result


def _label_match(fingerprint: str | None, selection: JsonDict) -> bool | None:
    if not fingerprint or fingerprint in {TOL_SENTINEL, LEGACY_SENTINEL} or not selection["fingerprint"]:
        return None
    return fingerprint == selection["fingerprint"]


def _prediction_rows(db: Database, ids: list[int], selection: JsonDict) -> dict[int, JsonDict]:
    """Enrich the same current rows Review uses; preserve taxon identity."""
    rows = [dict(row) for row in db.predictions.get_rows(photo_ids=ids)]
    attach_prediction_evidence(db, rows, db.get_effective_config(cfg.load()))
    resolver = SpeciesResolver(db=db)
    for row in rows:
        identity = resolver.prediction(row)
        row["species_identity"] = {"key": identity.key, "name": identity.display_name,
                                   "scientific_name": identity.scientific_name, "taxon_id": identity.taxon_id}
        row["matches_current_label_selection"] = _label_match(row["labels_fingerprint"], selection)
    return {row["id"]: row for row in rows}


def _subject(
    photo_id: int, raw: JsonDict, current: dict[int, JsonDict], eligible: set[int],
    historical: set[int],
) -> JsonDict:
    """Keep every candidate tied to its detection; full images get no crop."""
    did = raw["detection_id"]
    localized = (raw["detector_model"] != "full-image" and raw["category"] == "animal"
                 and valid_subject_box(raw))
    predictions = []
    for prediction in raw["predictions"]:
        row = current.get(prediction["prediction_id"])
        predictions.append({**prediction,
            "historical": prediction["prediction_id"] in historical,
            "current_review_row": row is not None,
            "species_identity": row["species_identity"] if row else None,
            "matches_current_label_selection": row["matches_current_label_selection"] if row else None,
            "evidence": row["evidence"] if row else None})
    reviewable = [p for p in predictions if p["current_review_row"] and p["status"] != "rejected"]
    reasons = set()
    if not localized:
        reasons.add("No usable localized animal box; inspect the full frame.")
    elif raw.get("subject_verdict") == "not_animal":
        reasons.add("The reviewer rejected this region as not an animal; its predictions are not evidence.")
    elif did not in eligible:
        reasons.add("This region is outside the current retained-subject selection.")
    if not reviewable:
        reasons.add("No current, unrejected prediction evidence for this region.")
    for prediction in reviewable:
        reasons.update(prediction["evidence"]["reasons"])
        if prediction["matches_current_label_selection"] is False:
            reasons.add("Stored predictions used a different species list from the current selection.")
        if prediction["labels_fingerprint"] == LEGACY_SENTINEL:
            reasons.add("The species-list provenance of this legacy prediction is unknown.")
    return {**{k: v for k, v in raw.items() if k != "predictions"},
            "localized": localized, "retained": did in eligible,
            "crop_url": f"/photos/{photo_id}/crop?detection_id={did}" if localized and did in eligible else None,
            "predictions": predictions, "review_reasons": sorted(reasons),
            "review_required": True, "propagation_verified": False}


def _candidates(photos: list[JsonDict]) -> list[JsonDict]:
    """Index top predictions per subject/model; never extend to group siblings."""
    candidates: dict[str, JsonDict] = {}
    for photo in photos:
        for subject in photo["subjects"]:
            if not subject["retained"] or not subject["localized"]:
                continue
            by_model: dict[str, list[JsonDict]] = defaultdict(list)
            for prediction in subject["predictions"]:
                if (prediction["current_review_row"] and prediction["status"] != "rejected"
                        and prediction["matches_current_label_selection"] is not False
                        and prediction["evidence"]["subject"]["decision"] not in {
                            "not_enough_evidence", "invalid_subject"}):
                    by_model[prediction["classifier_model"]].append(prediction)
            for model, rows in by_model.items():
                best = max((p["confidence"] or 0) for p in rows)
                for prediction in rows:
                    if (prediction["confidence"] or 0) != best:
                        continue
                    identity = prediction["species_identity"]
                    candidate = candidates.setdefault(identity["key"], {
                        "species_identity": identity, "photo_ids": [], "subjects": [],
                        "review_required": True, "propagation_verified": False,
                    })
                    if photo["id"] not in candidate["photo_ids"]:
                        candidate["photo_ids"].append(photo["id"])
                    candidate["subjects"].append({"photo_id": photo["id"],
                        "detection_id": subject["detection_id"], "prediction_id": prediction["prediction_id"],
                        "model": model, "crop_url": subject["crop_url"], "preview_url": photo["preview_url"],
                        "box_area": subject["box_w"] * subject["box_h"],
                        "review_reasons": subject["review_reasons"]})
    for candidate in candidates.values():
        # Larger localized regions are inspection candidates, not proof of ID.
        ordered = sorted(candidate["subjects"], key=lambda s: -s["box_area"])
        unique = {(s["photo_id"], s["detection_id"]): s for s in reversed(ordered)}
        candidate["inspection_candidates"] = sorted(unique.values(), key=lambda s: -s["box_area"])[:3]
    return list(candidates.values())


def build_species_evidence(db: Database, ids: list[int]) -> JsonDict:
    """Return exact-scope evidence; a missing detection never means no animal."""
    selection = _label_selection(db)
    current = _prediction_rows(db, ids, selection)
    raw_photos = db.photo_species.confirmation_evidence(ids)
    by_id = {row["id"]: row for row in raw_photos}
    all_predictions = [p["prediction_id"] for row in raw_photos
                       for subject in row["subjects"] for p in subject["predictions"]]
    historical = db.predictions.superseded_ids(all_predictions)
    retained = detection_store.get_detections_for_photos(db, ids)
    photos = []
    for pid in ids:
        raw = by_id[pid]
        eligible = {d["id"] for d in retained.get(pid, [])
                    if d["category"] == "animal" and d["detector_model"] != "full-image"}
        subjects = [_subject(pid, subject, current, eligible, historical) for subject in raw["subjects"]]
        reasons = sorted({reason for subject in subjects for reason in subject["review_reasons"]})
        if len(eligible) > 1:
            reasons.append("Multiple localized regions; check whether they represent different animals.")
        if not eligible:
            reasons.append("No retained animal region; inspect the frame. Animal absence is not established.")
        photos.append({"id": pid, "filename": raw["filename"], "timestamp": raw["timestamp"],
            "flag": raw["flag"], "preview_url": f"/photos/{pid}/preview?size=1920",
            "species_assignments": db.species_provenance.assignments(pid),
            "subjects": subjects, "review_reasons": reasons, "review_required": True})
    fingerprints = {p["labels_fingerprint"] for row in raw_photos
                    for subject in row["subjects"] for p in subject["predictions"]}
    stored = [row for row in db.model_runs.get_labels_fingerprints() if row["fingerprint"] in fingerprints]
    models = Counter(p["classifier_model"] for row in raw_photos
                     for subject in row["subjects"] for p in subject["predictions"])
    return {"workspace_id": db.require_workspace_id(), "photo_ids": ids, "photo_count": len(ids),
        "scope_method": "explicit_photo_selection", "photos": photos,
        "current_label_selection": selection, "stored_label_sets": stored,
        "stored_classifier_models": [{"name": name, "prediction_count": count}
                                     for name, count in sorted(models.items())],
        "candidate_selections": _candidates(photos),
        "continuity": {"verified": False,
            "explanation": "Capture-time groups and matching predictions do not establish same-animal continuity."},
        "automatic_assignment_eligible": False,
        "review_message": "Inspect identification evidence before confirming exact photo IDs. Candidate sets are predictions, not reviewed propagation scopes.",
        "score_message": "Relative classifier scores are not calibrated probabilities of correct identification."}
