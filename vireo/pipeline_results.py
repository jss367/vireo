"""Edit cached Process Review results (encounters / bursts) in place.

The review page lets the user detach a burst or photo and confirm species
without re-running the pipeline; these helpers keep the cached result
structure (photo ids, counts, time ranges, species labels) consistent
after such edits. Pure functions over the results dict.
"""


def compute_time_range(photos_by_id, photo_ids):
    """Return [min_ts, max_ts] ISO strings for photo_ids, or [None, None]."""
    timestamps = [
        photos_by_id[pid]["timestamp"]
        for pid in photo_ids
        if pid in photos_by_id and photos_by_id[pid].get("timestamp")
    ]
    if not timestamps:
        return [None, None]
    return [min(timestamps), max(timestamps)]


def rebuild_encounter_species_label(results, photo_ids, fallback=None):
    """Return the encounter-level species label for exactly photo_ids."""
    from encounters import encounter_species_label

    id_set = set(photo_ids or [])
    photos = [p for p in results.get("photos", []) if p.get("id") in id_set]
    name, confidence = encounter_species_label(photos)
    if name is None and fallback is not None:
        return list(fallback)
    return [name, confidence]


def candidate_species_override(species_label):
    """Convert a derived species label into an unconfirmed burst candidate."""
    species = species_label[0] if species_label else None
    if not species:
        return None
    return {"species": species, "confirmed": False}


# -- Multi-species confirmation ---------------------------------------------
#
# A photo can legitimately carry two confirmed species (two MegaDetector
# boxes, two subjects). Cached results keep the legacy single-valued fields
# (``photo.confirmed_species``, ``encounter.confirmed_species``,
# ``species_override.species``) as the *primary* species so older consumers
# keep working, and add list-valued siblings (``confirmed_species_list``,
# ``species_override.species_list``) that carry every confirmed species.
# These helpers read either shape.


def _as_species_list(container, list_key, single_key):
    if not isinstance(container, dict):
        return []
    lst = container.get(list_key)
    if lst is None:
        single = container.get(single_key)
        lst = [single] if single else []
    return [s for s in lst if s]


def photo_confirmed_species_list(photo):
    """Every species confirmed on a cached photo dict (primary first)."""
    return _as_species_list(photo, "confirmed_species_list", "confirmed_species")


def encounter_confirmed_species_list(enc):
    """Every species a cached encounter is confirmed as (primary first)."""
    return _as_species_list(enc, "confirmed_species_list", "confirmed_species")


def burst_species_list(enc, burst):
    """The species set a burst currently holds: its override, else the
    encounter's.

    An override carrying a ``species_list`` array is authoritative whatever
    its ``confirmed`` flag: confirmed (uniform frames), unconfirmed (a mixed
    burst after a set edit — its frames differ but the edited baseline is
    still what the burst holds, not the encounter's stale list) or empty
    (the sentinel a remove leaves so the burst does not inherit the species
    just untagged). Only the legacy shape without the array is a detach-time
    candidate (``confirmed: False``, a classifier guess); that is a display
    hint, not the burst's set — treating it as such would make a replace
    validate ``previous_species`` against the guess and an add promote the
    guess. Rapid review derives its baseline the same way.
    """
    ovr = burst.get("species_override") if isinstance(burst, dict) else None
    if isinstance(ovr, dict):
        explicit = ovr.get("species_list")
        if isinstance(explicit, list):
            return [s for s in explicit if s]
        if ovr.get("confirmed") and ovr.get("species"):
            return [ovr["species"]]
    return encounter_confirmed_species_list(enc)


def species_key_set(names):
    """Case/quote-insensitive identity set for a list of species names."""
    from keyword_normalization import keyword_match_key

    return {keyword_match_key(n) for n in names if n}


def build_species_override(species_list, confirmed=True):
    """Burst override dict for ``species_list`` (None when the list is empty).

    ``None`` means "no override: inherit the encounter" — the regroup
    derivation uses it for bursts that are not uniformly confirmed. Use
    :func:`empty_species_override` for a burst that was explicitly cleared.
    """
    species_list = [s for s in species_list if s]
    if not species_list:
        return None
    return {
        "species": species_list[0],
        "confirmed": confirmed,
        "species_list": list(species_list),
    }


def empty_species_override():
    """Override for a burst explicitly confirmed as *no* species.

    Distinct from ``None`` (inherit the encounter): after a remove strips a
    burst's last species, the encounter may still be confirmed, and the
    burst must not present that species as its own.
    """
    return {"species": None, "confirmed": False, "species_list": []}


def updated_species_list(current, species, previous_species=None,
                         add=False, remove=False):
    """Return ``current`` after confirming ``species`` in the given mode.

    * remove: drop ``species``.
    * add: append ``species`` (or refresh its spelling in place).
    * replace (default): swap ``previous_species`` for ``species`` in place;
      when nothing matches, ``species`` is appended.
    Identity is by ``keyword_match_key`` so spelling variants collapse.
    """
    from keyword_normalization import keyword_match_key

    target = keyword_match_key(species)
    prev = keyword_match_key(previous_species) if previous_species else None
    if remove:
        return [s for s in current if keyword_match_key(s) != target]
    out = []
    placed = False
    for s in current:
        k = keyword_match_key(s)
        if k == target or (not add and prev is not None and k == prev):
            if not placed:
                out.append(species)
                placed = True
            continue
        out.append(s)
    if not placed:
        out.append(species)
    return out


def set_encounter_confirmed_species(enc, species_list):
    """Write the encounter-level confirmation fields for ``species_list``."""
    species_list = [s for s in species_list if s]
    enc["species_confirmed"] = bool(species_list)
    enc["confirmed_species"] = species_list[0] if species_list else None
    enc["confirmed_species_list"] = list(species_list)


def find_merge_target(encounters, detached_range, target_species):
    """Find an encounter index whose confirmed species matches target_species and
    whose time range is adjacent to detached_range (no other encounter sits in
    the gap between them). Returns None if none found.
    """
    d_min, d_max = detached_range
    if d_min is None or d_max is None:
        return None

    other_ranges = []
    for i, e in enumerate(encounters):
        tr = e.get("time_range") or [None, None]
        if tr[0] is not None and tr[1] is not None:
            other_ranges.append((i, tr[0], tr[1]))

    for i, e in enumerate(encounters):
        if not e.get("species_confirmed"):
            continue
        if e.get("confirmed_species") != target_species:
            continue
        tr = e.get("time_range") or [None, None]
        if tr[0] is None or tr[1] is None:
            continue
        c_min, c_max = tr[0], tr[1]
        if c_max < d_min:
            gap_start, gap_end = c_max, d_min
        elif d_max < c_min:
            gap_start, gap_end = d_max, c_min
        else:
            return i  # overlapping — treat as adjacent
        intervening = False
        for j, o_min, o_max in other_ranges:
            if j == i:
                continue
            if o_max > gap_start and o_min < gap_end:
                intervening = True
                break
        if not intervening:
            return i
    return None


def auto_detach_burst_for_species(results, enc_idx, burst_idx, new_species):
    """Detach the burst at (enc_idx, burst_idx) from its encounter. If an adjacent
    encounter already has new_species as its confirmed species, merge the burst
    into that encounter; otherwise create a new single-burst encounter with
    new_species confirmed. Mutates results in place.
    """
    from pipeline import rebuild_species_predictions

    encounters = results["encounters"]
    enc = encounters[enc_idx]
    bursts = enc["bursts"]
    detached = bursts.pop(burst_idx)
    detached_ids = detached["photo_ids"]

    photos_by_id = {p["id"]: p for p in results.get("photos", [])}
    detached_range = compute_time_range(photos_by_id, detached_ids)

    if len(bursts) == 0:
        encounters.pop(enc_idx)
    else:
        remaining = [pid for pid in enc["photo_ids"] if pid not in set(detached_ids)]
        enc["photo_ids"] = remaining
        enc["photo_count"] = len(remaining)
        enc["burst_count"] = len(bursts)
        enc["species_predictions"] = rebuild_species_predictions(results, remaining)
        enc["species"] = rebuild_encounter_species_label(
            results, remaining, fallback=enc.get("species")
        )
        for b in bursts:
            b["species_predictions"] = rebuild_species_predictions(results, b["photo_ids"])
        enc["time_range"] = compute_time_range(photos_by_id, remaining)
        # Pair indices in trace reference the original composition; drop it
        # so the algorithm-trace panel renders an honest "needs recompute"
        # state instead of stale rows.
        enc.pop("trace", None)
        enc.pop("visual_neighbor_photo_ids", None)

    detached["species_predictions"] = rebuild_species_predictions(results, detached_ids)

    merge_idx = find_merge_target(encounters, detached_range, new_species)
    if merge_idx is not None:
        target = encounters[merge_idx]
        target["bursts"].append(detached)
        target["photo_ids"] = list(target["photo_ids"]) + list(detached_ids)
        target["photo_count"] = len(target["photo_ids"])
        target["burst_count"] = len(target["bursts"])
        target["species_predictions"] = rebuild_species_predictions(
            results, target["photo_ids"]
        )
        target["species"] = rebuild_encounter_species_label(
            results, target["photo_ids"], fallback=target.get("species")
        )
        # Same reason as above — target's trace no longer matches its photo set.
        target.pop("trace", None)
        t_min, t_max = target.get("time_range") or [None, None]
        d_min, d_max = detached_range
        mins = [x for x in (t_min, d_min) if x is not None]
        maxs = [x for x in (t_max, d_max) if x is not None]
        target["time_range"] = [
            min(mins) if mins else None,
            max(maxs) if maxs else None,
        ]
    else:
        detached_species = rebuild_encounter_species_label(
            results, detached_ids, fallback=enc.get("species")
        )
        # The detached burst may carry more than one confirmed species; the
        # new encounter inherits the whole list with new_species as primary.
        # Only the burst's own authoritative set counts: a detach-time
        # candidate ({"species": guess, "confirmed": False}, no list) is a
        # classifier hint and must not become a confirmed second species.
        detached_list = burst_species_list({}, detached)
        new_keys = species_key_set([new_species])
        confirmed_list = [new_species] + [
            s for s in detached_list
            if species_key_set([s]) != new_keys
        ]
        encounters.append({
            "species": detached_species,
            "confirmed_species": new_species,
            "confirmed_species_list": confirmed_list,
            "species_predictions": detached["species_predictions"],
            "species_confirmed": True,
            "photo_count": len(detached_ids),
            "burst_count": 1,
            "time_range": detached_range,
            "photo_ids": list(detached_ids),
            "bursts": [detached],
        })

    summary = results.setdefault("summary", {})
    summary["encounter_count"] = len(encounters)
    summary["burst_count"] = sum(e.get("burst_count", 0) for e in encounters)
