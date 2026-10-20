"""Refresh saved quality suggestions from current catalog evidence, read-only.

Grouping and review confirmations belong to the saved review. Quality inputs,
photographer flags and ratings belong to the live catalog; historic automatic
labels are never evidence for a new suggestion.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db import Database
    from services.service_types import JsonDict


def refresh_saved_suggestions(db: Database, data: JsonDict) -> None:
    """Re-score the saved members without inference or catalog/sidecar writes."""
    import config as cfg
    from pipeline import (
        _clean_serialized_photo,
        _clear_derived_scores,
        load_photo_features,
        refresh_serialized_summary,
        run_triage,
    )

    # Identify/species-only reviews deliberately do not run quality triage.
    if data.get("review_mode") == "species":
        return
    effective = db.get_effective_config(cfg.load())
    ids = [p["id"] for p in data.get("photos", [])]
    current = load_photo_features(
        db, photo_ids=ids, config=effective, effective_config=effective,
    )
    photos = {p["id"]: p for p in current}
    encounters = _current_groups(data, photos)
    _, scored = run_triage(encounters, config=effective.get("pipeline", {}))
    # Cohorts are derived from current measurements, unlike saved grouping edits.
    for saved, current_group in zip(data.get("encounters", []), encounters, strict=True):
        if "focus_cohorts" in current_group:
            saved["focus_cohorts"] = current_group["focus_cohorts"]
        else:
            saved.pop("focus_cohorts", None)
    scored_by_id = {p["id"]: p for p in scored}
    # Preserve saved review annotations, but replace every live input and
    # derived score (including explicit nulls) so stale measurements cannot win.
    _clear_derived_scores(data.get("photos", []))
    data["photos"] = [
        {**old, **_clean_serialized_photo(scored_by_id[old["id"]])}
        for old in data.get("photos", []) if old["id"] in scored_by_id
    ]
    _refresh_species_summaries(data, scored)
    refresh_serialized_summary(data)


def _refresh_species_summaries(data: JsonDict, current: list[JsonDict]) -> None:
    """Refresh derived classifier summaries without changing explicit species edits."""
    from pipeline import rebuild_species_predictions
    from pipeline_results import candidate_species_override, rebuild_encounter_species_label

    # Use freshly loaded rows, not saved grouping-time classifier hints.
    photos_by_id = {photo["id"]: photo for photo in current}
    for encounter in data.get("encounters", []):
        ids = encounter.get("photo_ids", [])
        subset = {"photos": [photos_by_id[pid] for pid in ids if pid in photos_by_id]}
        encounter["species_predictions"] = rebuild_species_predictions(subset, ids)
        encounter["species"] = rebuild_encounter_species_label(subset, ids)
        for burst in encounter.get("bursts", []):
            if not isinstance(burst, dict):
                continue  # Raw photo-id lists have no derived species fields.
            ids = burst.get("photo_ids", [])
            subset = {"photos": [photos_by_id[pid] for pid in ids if pid in photos_by_id]}
            burst["species_predictions"] = rebuild_species_predictions(subset, ids)
            species = rebuild_encounter_species_label(subset, ids)
            if "species" in burst:
                burst["species"] = species
            override = burst.get("species_override")
            if (isinstance(override, dict) and not override.get("confirmed")
                    and not isinstance(override.get("species_list"), list)):
                burst["species_override"] = candidate_species_override(species)


def _current_groups(data: JsonDict, photos: dict[int, JsonDict]) -> list[JsonDict]:
    """Keep saved edits/confirmations and remove no-longer-visible members."""
    groups = []
    saved = []
    assigned = set()
    for encounter in data.get("encounters", []):
        ids = [pid for pid in encounter.get("photo_ids", []) if pid in photos]
        if not ids:
            continue
        encounter["photo_ids"] = ids
        encounter["photo_count"] = len(ids)
        members = set(ids)
        bursts = []
        saved_bursts = []
        covered = set()
        for burst in encounter.get("bursts", []):
            old_ids = burst.get("photo_ids", []) if isinstance(burst, dict) else burst
            burst_ids = [pid for pid in old_ids if pid in members]
            if not burst_ids:
                continue
            bursts.append([photos[pid] for pid in burst_ids])
            saved_bursts.append({**burst, "photo_ids": burst_ids} if isinstance(burst, dict) else burst_ids)
            covered.update(burst_ids)
        remainder = [pid for pid in ids if pid not in covered]
        if remainder:
            bursts.append([photos[pid] for pid in remainder])
            saved_bursts.append({"photo_ids": remainder})
        encounter["bursts"] = saved_bursts
        encounter["burst_count"] = len(bursts)
        groups.append({"photos": [photos[pid] for pid in ids], "bursts": bursts})
        saved.append(encounter)
        assigned.update(ids)
    # Very old/partial snapshots may contain photo rows without grouping.
    remainder = [pid for pid in photos if pid not in assigned]
    if remainder:
        members = [photos[pid] for pid in remainder]
        groups.append({"photos": members, "bursts": [members]})
        saved.append({"photo_ids": remainder, "photo_count": len(remainder),
                      "burst_count": 1, "bursts": [{"photo_ids": remainder}]})
    data["encounters"] = saved
    return groups
