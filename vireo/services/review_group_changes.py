"""Atomic Group Review flag/removal changes shared with species confirmation."""
from __future__ import annotations

from typing import TYPE_CHECKING

from pipeline_results import (
    build_species_override,
    burst_species_list,
    candidate_species_override,
    empty_species_override,
    rebuild_encounter_species_label,
)

if TYPE_CHECKING:
    from db import Database
    from services.service_types import JsonDict


class InvalidGroupChanges(ValueError):
    """The reviewed burst or its decisions no longer match the request."""


def detach_review_photo(results: JsonDict, enc_idx: int, burst_idx: int, photo_id: int) -> None:
    """Apply a validated photo removal to a canonical pipeline snapshot."""
    from pipeline import rebuild_species_predictions

    encounters = results["encounters"]
    enc = encounters[enc_idx]
    bursts = enc["bursts"]
    raw_burst = bursts[burst_idx]
    burst: JsonDict = {"photo_ids": raw_burst} if isinstance(raw_burst, list) else raw_burst
    bursts[burst_idx] = burst
    source_override = burst.get("species_override") or {}

    # Remove photo from burst
    burst["photo_ids"].remove(photo_id)
    # Burst-local pair/cohort provenance no longer describes these members.
    burst.pop("trace", None)
    burst.pop("focus_cohorts", None)

    if len(burst["photo_ids"]) == 0:
        # Last photo — remove the empty burst
        bursts.pop(burst_idx)
    else:
        # Recalculate source burst predictions without the removed photo
        burst["species_predictions"] = rebuild_species_predictions(results, burst["photo_ids"])

    # Create new single-photo burst in the same encounter
    new_burst_predictions = rebuild_species_predictions(results, [photo_id])
    new_burst_species = rebuild_encounter_species_label(results, [photo_id])
    source_list = (
        source_override.get("species_list")
        if isinstance(source_override, dict) else None
    )
    source_has_authoritative_list = isinstance(source_list, list)
    source_legacy_confirmed = bool(
        source_override.get("confirmed") and source_override.get("species")
    )
    if source_has_authoritative_list or source_legacy_confirmed:
        # Copy the source burst's authoritative set — including the
        # explicit-empty sentinel a remove leaves behind AND the
        # mixed-but-edited shape ({confirmed: False, species_list: [A]})
        # a set edit leaves on a still-mixed burst. Every array-valued
        # species_list is authoritative (burst_species_list), so a null
        # override on the split-off photo would let it inherit the
        # encounter's stale baseline and resurrect a species this edit
        # already dropped.
        copied_list = burst_species_list({}, burst)
        if copied_list:
            # Preserve the source's confirmed flag: True for a genuinely
            # confirmed burst, False for a mixed-but-edited baseline.
            new_burst_override = build_species_override(
                copied_list,
                confirmed=bool(source_override.get("confirmed")),
            )
        else:
            new_burst_override = empty_species_override()
    elif enc.get("confirmed_species"):
        # Inherit the encounter's prior confirmed species by leaving the
        # override empty. Also covers the mixed/partial state where
        # species_confirmed is False but confirmed_species records the
        # dominant prior species (pipeline.py builds encounter payloads
        # this way for encounters whose photos disagree on confirmed
        # species). A classifier-guess override here would mask that
        # prior tag: the confirm endpoint reads species_override.species
        # without inspecting the confirmed flag, so a later burst confirm
        # would target the guess as previous_species instead of the real
        # prior species and leave both keywords on the photo.
        new_burst_override = None
    else:
        new_burst_override = candidate_species_override(new_burst_species)
    new_burst = {
        "photo_ids": [photo_id],
        "species_predictions": new_burst_predictions,
        "species_override": new_burst_override,
    }
    bursts.append(new_burst)
    enc["burst_count"] = len(bursts)
    # Recalculate encounter-level predictions
    enc["species_predictions"] = rebuild_species_predictions(results, enc["photo_ids"])
    # Encounter composition didn't change but burst structure did; trace
    # is pair-level only, so it remains valid. No trace mutation needed.

    # Update summary
    results["summary"]["burst_count"] = sum(
        e.get("burst_count", 0) for e in encounters
    )


def validate_group_changes(db: Database, cached: JsonDict, enc: JsonDict, body: JsonDict) -> JsonDict | None:
    group = body.get("group_changes")
    if group is None:
        return None
    if not isinstance(group, dict) or not (body.get("preview") or body.get("preview_token")):
        raise InvalidGroupChanges("Combined Group Review changes require a species preview")
    zones: list[list[int]] = []
    for key in ("picks", "rejects", "candidates", "removed"):
        ids = group.get(key)
        if not isinstance(ids, list) or any(type(pid) is not int for pid in ids):
            raise InvalidGroupChanges("Group Review decisions must be lists of photo IDs")
        zones.append(ids)
    all_ids = [pid for ids in zones for pid in ids]
    if len(set(all_ids)) != len(all_ids):
        raise InvalidGroupChanges("A Group Review photo cannot appear in multiple decisions")
    ei, bi = group.get("encounter_index"), group.get("burst_index")
    if type(ei) is not int or type(bi) is not int or not 0 <= ei < len(cached["encounters"]):
        raise InvalidGroupChanges("The photo groups changed. Reload before applying.")
    source_enc = cached["encounters"][ei]
    bursts = source_enc.get("bursts") or []
    if source_enc is not enc or not 0 <= bi < len(bursts):
        raise InvalidGroupChanges("The photo groups changed. Reload before applying.")
    source = bursts[bi]
    ids = source["photo_ids"] if isinstance(source, dict) else source
    if group.get("expected_burst_photo_ids") != ids or set(all_ids) != set(ids):
        raise InvalidGroupChanges("The photo groups changed. Reload before applying.")
    if not set(body["photo_ids"]) <= set(ids) - set(group["removed"]):
        raise InvalidGroupChanges("Removed photos cannot receive the confirmed species")
    if set(db.photo_visibility.visible_photo_ids(all_ids)) != set(all_ids):
        raise InvalidGroupChanges("Group Review photos are not in the active workspace")
    return group


def apply_group_changes(db: Database, cached: JsonDict, group: JsonDict) -> JsonDict:
    """Write flags and detach frames without committing or saving the cache.

    Returns ``{"items": [...], "before_attributions": {...}, "touched_ids": [...]}``:
    ``items`` is the catalog edit's reversible flag items, ``before_attributions``
    the pre-write attribution snapshot captured so the enclosing grouping edit
    can record replay metadata, and ``touched_ids`` every photo whose zone
    decision was explicit so attribution is published even when the raw flag
    is unchanged (a legacy ``flagged`` in Picks, an unattributed ``none`` in
    Candidates).
    """
    from pipeline import refresh_serialized_summary

    touched_ids = []
    for key in ("picks", "rejects", "candidates"):
        for pid in group[key]:
            if pid not in touched_ids:
                touched_ids.append(pid)
    before_attributions = db.photo_review.flag_attributions(touched_ids)
    items = []
    flag_by_id = {}
    for key, target in (("picks", "flagged"), ("rejects", "rejected"), ("candidates", "none")):
        attributed_ids = []
        for pid in group[key]:
            row = db.photos.get(pid)
            old = row["flag"] or "none"
            flag_by_id[pid] = target
            if target == "none" and old not in ("flagged", "rejected"):
                # 'none' photo placed in Candidates: no flag write, but the
                # explicit Review choice still confirms attribution.
                attributed_ids.append(pid)
                items.append({"photo_id": pid, "old_value": old, "new_value": target})
                continue
            # For old == target, the UPDATE still fires the invalidation
            # trigger; set_current_flag_note republishes the explicit note.
            db.photo_review.set_flag(pid, target, _commit=False)
            if old != target:
                db.queue_flag_change_if_enabled(pid, target, _commit=False)
            attributed_ids.append(pid)
            items.append({"photo_id": pid, "old_value": old, "new_value": target})
        if attributed_ids:
            db.photo_review.set_current_flag_note(
                attributed_ids, target, "Applied Group Review species and cull decisions",
            )
    for pid in group["removed"]:
        for ei, enc in enumerate(cached["encounters"]):
            bi = next((i for i, burst in enumerate(enc.get("bursts") or [])
                       if pid in (burst["photo_ids"] if isinstance(burst, dict) else burst)), None)
            if bi is not None:
                detach_review_photo(cached, ei, bi, pid)
                break
        else:
            raise InvalidGroupChanges("A removed photo disappeared while applying Group Review")
    for photo in cached.get("photos", []):
        if photo["id"] in flag_by_id:
            flag = flag_by_id[photo["id"]]
            photo["flag"] = flag
    refresh_serialized_summary(cached)
    return {
        "items": items,
        "before_attributions": before_attributions,
        "touched_ids": touched_ids,
    }
