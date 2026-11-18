"""Subject evidence for manual review in the active workspace."""

from __future__ import annotations

from typing import TYPE_CHECKING

import config as cfg
from config import config_value
from highlights_payload import species_canonicalizer
from species_memberships import SECONDARY_SUBJECT_FLOOR, photo_species_memberships

if TYPE_CHECKING:
    from db import Database
    from services.service_types import JsonDict


def _review_buckets(db: Database) -> list[JsonDict]:
    canonicalize = species_canonicalizer(db)
    counts = {}
    examples = {}
    for photo in db.species_curation.get_highlights_candidates(None):
        for member in photo_species_memberships(photo, canonicalize, 0.70):
            name = member.species
            counts[name] = counts.get(name, 0) + 1
            if member.accepted_name or not member.prediction:
                continue
            prediction = member.prediction
            weak = member.whole_frame or (prediction.get("detector_confidence") or 0) < SECONDARY_SUBJECT_FLOOR
            row = dict(id=photo["id"], filename=photo["filename"], species=name,
                       detection_id=None if member.whole_frame else prediction.get("detection_id"),
                       weak=weak, whole_frame=member.whole_frame)
            examples.setdefault(name, []).append(row)
    return [dict(row, bucket_size=counts[name]) for name in sorted(examples)
            for row in examples[name] if counts[name] <= 2 or row["weak"]]


def _references(db: Database, floor: float) -> list[JsonDict]:
    from subjects import retained

    rows = []
    for collection in db.collections.list_all():
        if collection["name"] != "Subject audit references" or collection["visual_json"]:
            continue
        for pid in db.collections.get_photo_ids(collection["id"]):
            photo = db.photos.get(pid, verify_workspace=True)
            if photo is None:
                continue
            policy = db.subjects.primary_state(pid)
            subject_floor = policy["min_conf"] if policy is not None and policy["min_conf"] is not None else floor
            subject_model = policy["detector_model"] if policy is not None else None
            subjects = retained(db, pid, subject_floor, detector_model=subject_model)
            det = subjects[0]["id"] if subjects else None
            predictions = db.predictions.top_unrejected_by_detection(
                [pid], min_detector_confidence=subject_floor).get(pid, [])
            name = next((p["species"] for p in predictions if p["detection_id"] == det), None)
            rows.append(dict(id=pid, filename=photo["filename"], detection_id=det, species=name))
    return rows


def subject_findings(db: Database) -> list[JsonDict]:
    floor = config_value(db.get_effective_config(cfg.load()), "detector_confidence")
    return db.audit.subject_findings(floor)


def subject_audit(db: Database) -> JsonDict:
    floor = config_value(db.get_effective_config(cfg.load()), "detector_confidence")
    return {
        "findings": db.audit.subject_findings(floor),
        "review": _review_buckets(db),
        "references": _references(db, floor),
    }
