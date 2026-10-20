"""Reviewable, revalidated photo sets and explicit encounter transitions."""
from __future__ import annotations

import copy
import hashlib
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db import Database
    from services.service_types import JsonDict


def preview(db: Database, confirmation: object, body: JsonDict) -> JsonDict:
    """Build the exact proposed keyword delta without writing any catalog state."""
    # Dynamic workflow object is owned by the encounters blueprint; its resolved
    # keyword identities are deliberately reused rather than reimplemented here.
    from typing import Any, cast
    edit = cast(Any, confirmation)
    source_ids = body.get("source_photo_ids", edit.photo_ids)
    evidence = db.photo_species.confirmation_evidence(source_ids)
    species = db.photo_species.get_species_for_photos(source_ids)
    target_state = db.keywords.species_confirmation_target(edit.species)
    target = target_state["keyword"]
    proposed_species = target["name"] if target else db.resolve_species_display_name(edit.species)
    historical = db.predictions.superseded_ids([
        p["prediction_id"] for photo in evidence for subject in photo["subjects"] for p in subject["predictions"]
    ])
    already = db.get_photos_with_equivalent_species(source_ids, target["id"]) if target else set()
    if target and edit.old_rows_by_photo:
        already = db.get_photos_with_equivalent_species(source_ids, target["id"], exclude_keyword_ids={
            row["id"] for rows in edit.old_rows_by_photo.values() for row in rows
        })
    import config as cfg
    threshold = db.get_effective_config(cfg.load()).get("detector_confidence", 0.2)
    selected = set(edit.photo_ids)
    rows = []
    for row in evidence:
        pid = row["id"]
        for subject in row["subjects"]:
            subject["below_threshold"] = (subject["detector_confidence"] or 0) < threshold
            for prediction in subject["predictions"]:
                prediction["historical"] = prediction["prediction_id"] in historical
        row["existing_species"] = species.get(pid, [])
        row["selected"] = pid in selected
        row["additions"] = (
            [proposed_species] if pid in selected and not edit.remove_mode and pid not in already else []
        )
        row["removals"] = [r["name"] for r in edit.old_rows_by_photo.get(pid, [])]
        rows.append(row)
    order = {pid: i for i, pid in enumerate(source_ids)}
    rows.sort(key=lambda row: order[row["id"]])
    state = {
        "workspace": db.require_workspace_id(), "encounter": edit.target_enc,
        "burst_index": edit.burst_index, "rows": rows, "species": edit.species,
        "add": edit.add_mode, "remove": edit.remove_mode,
        "previous_species": edit.previous_species, "split": bool(body.get("split")),
        "target": target_state,
        # Burst edits refresh the entire encounter, including sibling bursts.
        # Bind their live species inputs without expanding the visible matrix
        # or invalidating on unrelated sibling flags/classifier measurements.
        "refresh_species": {
            "keywords": db.photo_species.confirmation_species_evidence(edit.target_enc["photo_ids"]),
            "identities": db.photo_species.get_species_for_photos(edit.target_enc["photo_ids"], include_identities=True),
        },
        "homonyms": db.photo_species.confirmation_homonym_evidence([
            name for name in (target["name"] if target else edit.species, edit.previous_species) if name
        ]),
        "group_changes": edit.group_plan,
        "group_evidence": db.photo_species.confirmation_evidence(edit.group_plan["expected_burst_photo_ids"])
        if edit.group_plan is not None else None,
    }
    token = hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()
    return {"ok": True, "preview_token": token, "photos": rows,
            "photo_ids": list(edit.photo_ids), "photo_count": len(selected),
            "species": proposed_species, "previous_species": edit.previous_species,
            "split": bool(body.get("split")), "previous_species_choices": edit.current_species_list}


def _refresh_group(
    db: Database, results: JsonDict, enc: JsonDict, *, membership_changed: bool,
    selected: set[int], preferred_order: list[str],
) -> None:
    """Derive labels from all members after a subset edit, preserving extra taxa."""
    from pipeline import derive_burst_override, rebuild_species_predictions
    from pipeline_results import (
        burst_species_list,
        compute_time_range,
        encounter_confirmed_species_list,
        rebuild_encounter_species_label,
    )

    ids = enc["photo_ids"]
    # Identities (not just display names) so linked homonyms — two different
    # taxa the user kept under the same spelling — do not collapse into one
    # shared species when derive_burst_override compares frames.
    actual = db.photo_species.get_species_for_photos(ids, include_identities=True)
    bursts = enc.get("bursts") or []
    previous_orders = [burst_species_list(enc, burst) for burst in bursts]
    def override(photo_ids: list[int], previous_order: list[str]) -> JsonDict | None:
        return derive_burst_override([
            {
                "confirmed_species_list": [entry["name"] for entry in actual.get(pid, [])],
                "confirmed_species_identities": actual.get(pid, []),
            }
            for pid in photo_ids
        ], preferred_order=preferred_order if selected.intersection(photo_ids) else previous_order)
    common = override(ids, encounter_confirmed_species_list(enc)) or {}
    enc["confirmed_species_list"] = common.get("species_list", [])
    enc["confirmed_species"] = common.get("species")
    enc["species_confirmed"] = bool(common.get("confirmed"))
    enc["photo_count"] = len(ids)
    enc["burst_count"] = len(enc.get("bursts") or [])
    photos_by_id = {p["id"]: p for p in results.get("photos", [])}
    enc["time_range"] = compute_time_range(photos_by_id, ids)
    if "missing_timestamp_count" in enc:
        enc["missing_timestamp_count"] = sum(not photos_by_id.get(pid, {}).get("timestamp") for pid in ids)
    enc["species_predictions"] = rebuild_species_predictions(results, ids)
    # A split cannot inherit a guess supported only by the other membership.
    # Explicit confirmations above remain independent of classifier summaries.
    enc["species"] = rebuild_encounter_species_label(
        results, ids, fallback=None if membership_changed else enc.get("species"),
    )
    if membership_changed:
        enc.pop("focus_cohorts", None)
    enc.pop("trace", None)
    for burst, previous_order in zip(bursts, previous_orders, strict=True):
        burst["species_override"] = override(burst["photo_ids"], previous_order)
        burst["species_predictions"] = rebuild_species_predictions(results, burst["photo_ids"])
        if "species" in burst:
            burst["species"] = rebuild_encounter_species_label(results, burst["photo_ids"])


def _split_insert_before(results: JsonDict, original: list[int], detached: list[int], remaining: list[int]) -> bool:
    """Use strict complete timestamp order, otherwise preserve first appearance."""
    from pipeline_results import compute_time_range

    photos = {p["id"]: p for p in results.get("photos", [])}
    if all(photos.get(pid, {}).get("timestamp") for pid in original):
        detached_range = compute_time_range(photos, detached)
        remaining_range = compute_time_range(photos, remaining)
        if detached_range[1] is not None and remaining_range[0] is not None and detached_range[1] < remaining_range[0]:
            return True
        if remaining_range[1] is not None and detached_range[0] is not None and remaining_range[1] < detached_range[0]:
            return False
    return original[0] in detached


def apply_scope(
    db: Database, results: JsonDict, enc: JsonDict, ids: list[int], *, split: bool, preferred_order: list[str],
) -> None:
    """Optionally separate exactly the selected frames; never infer splits from disagreement."""
    selected = set(ids)
    if split:
        original = list(enc["photo_ids"])
        detached = copy.deepcopy(enc)
        detached["photo_ids"] = [pid for pid in enc["photo_ids"] if pid in selected]
        enc["photo_ids"] = [pid for pid in enc["photo_ids"] if pid not in selected]
        for group, keep in ((enc, False), (detached, True)):
            bursts = []
            for burst in group.get("bursts") or []:
                members = [pid for pid in burst["photo_ids"] if (pid in selected) == keep]
                if members:
                    child = copy.deepcopy(burst)
                    child["photo_ids"] = members
                    if members != burst["photo_ids"]:
                        child.pop("trace", None)
                        child.pop("focus_cohorts", None)
                    bursts.append(child)
            group["bursts"] = bursts
        index = results["encounters"].index(enc)
        insert_before = _split_insert_before(results, original, detached["photo_ids"], enc["photo_ids"])
        results["encounters"].insert(index if insert_before else index + 1, detached)
        _refresh_group(db, results, detached, membership_changed=True, selected=selected, preferred_order=preferred_order)
    _refresh_group(db, results, enc, membership_changed=split, selected=selected, preferred_order=preferred_order)
    summary = results.setdefault("summary", {})
    summary["encounter_count"] = len(results["encounters"])
    summary["burst_count"] = sum(e.get("burst_count", 0) for e in results["encounters"])
