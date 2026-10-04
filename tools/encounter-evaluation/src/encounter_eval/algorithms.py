"""Interchangeable baselines and a deliberately small sequence experiment.

All scores here are heuristics, not calibrated probabilities. Algorithms receive
only photo evidence; expected labels are owned by the separate scoring layer.
"""

from __future__ import annotations

import math
from collections import defaultdict

from .common import Group, restore_features, validate_groups
from .library import normalize, timestamp

DEFAULTS = {"confidence": 0.65, "margin": 0.2, "detector_confidence": 0.2,
            "context_frames": 4, "context_seconds": 3.0, "transition_penalty": 0.5}
SEARCH_SPACE = {"confidence": [0.4, 0.55, 0.7, 0.85], "margin": [0.1, 0.2, 0.35],
                "context_frames": [2, 4, 8], "transition_penalty": [0.2, 0.5, 0.9]}


def _iou(a, b):
    if any(d.get(k) is None for d in (a, b) for k in ("box_x", "box_y", "box_w", "box_h")):
        return 0
    left, top = max(a["box_x"], b["box_x"]), max(a["box_y"], b["box_y"])
    right = min(a["box_x"] + a["box_w"], b["box_x"] + b["box_w"])
    bottom = min(a["box_y"] + a["box_h"], b["box_y"] + b["box_h"])
    intersection = max(0, right - left) * max(0, bottom - top)
    union = a["box_w"] * a["box_h"] + b["box_w"] * b["box_h"] - intersection
    return intersection / union if union > 0 else 0


def observation(photo, cfg):
    """A qualified winner per source, consensus per box, then union across boxes.

    Disagreement on one box never invents a second species. Non-qualifying or
    absent sources abstain. Latest-source selection occurred in the reader.
    """
    detections = photo.get("evidence", [])
    real = [d for d in detections if d["detector_model"] != "full-image" and d["category"] == "animal"
            and (d["detector_confidence"] or 0) >= cfg["detector_confidence"]]
    kept = []
    for det in sorted(real, key=lambda d: (-(d["detector_confidence"] or 0), d["id"])):
        if not any(_iou(det, previous) >= 0.8 for previous in kept):
            kept.append(det)
    if not kept:
        kept = [d for d in detections if d["detector_model"] == "full-image"]
    taxa, unresolved = set(), False
    for subject in kept:
        winners = set()
        for source in subject["sources"]:
            if source["mode"] != "exclusive":
                raise ValueError("This candidate requires exclusive classifier sources")
            # Aliases within one source must not manufacture a runner-up.
            scores = defaultdict(float)
            for prediction in source["predictions"]:
                score = prediction["score"]
                if isinstance(score, (int, float)) and math.isfinite(score) and 0 <= score <= 1:
                    scores[prediction["taxon"]] = max(scores[prediction["taxon"]], score)
            ranked = sorted(scores, key=lambda key: (-scores[key], key))
            if not ranked:
                continue
            first = scores[ranked[0]]
            second = scores[ranked[1]] if len(ranked) > 1 else 0
            if first >= cfg["confidence"] and first - second >= cfg["margin"]:
                winners.add(ranked[0])
        if len(winners) == 1:
            taxa.update(winners)
        else:
            unresolved = True
    return tuple(sorted(taxa)), unresolved, bool(kept)


def _groups(photos, states, reasons):
    groups = []
    for photo, state, reason in zip(photos, states, reasons, strict=True):
        if groups and groups[-1].roster == state:
            previous = groups[-1]
            groups[-1] = Group((*previous.photo_ids, photo["id"]), state,
                               previous.reason if previous.reason == reason else "Direct and sequence evidence")
        else:
            groups.append(Group((photo["id"],), state, reason))
    return groups


def _sequence(photos, cfg):
    observations = [observation(p, cfg) for p in photos]
    direct = [taxa if taxa and not unresolved else None for taxa, unresolved, _ in observations]
    times = [timestamp(p["timestamp"]) for p in photos]
    candidates = []
    radius = cfg["context_frames"]
    for i, state in enumerate(direct):
        options = {state, None}
        if times[i]:
            for j in range(max(0, i - radius), min(len(photos), i + radius + 1)):
                if times[j] and abs((times[j] - times[i]).total_seconds()) <= cfg["context_seconds"]:
                    options.add(direct[j])
        # At most 2 * context_frames + 2 states, not a powerset of the shoot.
        candidates.append(sorted(options, key=lambda s: (s is not None, s or ())))
    previous = {}
    links = []
    for i, options in enumerate(candidates):
        costs, back = {}, {}
        observed, unresolved, has_subject = observations[i]
        for state in options:
            if state is None:
                unary = 0.8 if direct[i] else 0.3
            elif direct[i]:
                unary = 4.0 * len(set(direct[i]) - set(state)) + 2.0 * len(set(state) - set(direct[i]))
            elif unresolved and has_subject:
                # Never smooth away an unclassified additional box or genuine
                # model disagreement. This frame stays reviewable.
                unary = math.inf
            else:
                unary = 0.05  # missing detector output can borrow nearby context
            if i == 0:
                costs[state], back[state] = unary, None
            else:
                strength = cfg["transition_penalty"]
                if times[i] and times[i - 1]:
                    dt = (times[i] - times[i - 1]).total_seconds()
                    strength *= math.exp(-dt / max(cfg["context_seconds"], 0.001))
                else:
                    strength = 0
                # Available scene evidence moderates continuity; it never erases
                # a sufficiently supported single-frame species appearance.
                a, b = photos[i - 1].get("dino_global_embedding"), photos[i].get("dino_global_embedding")
                if a is not None and b is not None:
                    from encounters import sim_embedding
                    strength *= 0.25 + 0.75 * sim_embedding(a, b)
                parent = min(previous, key=lambda r: (previous[r] + (strength if r != state else 0),
                                                     r is not None, r or ()))
                costs[state] = unary + previous[parent] + (strength if parent != state else 0)
                back[state] = parent
        links.append(back)
        previous = costs
    if not photos:
        return []
    state = min(previous, key=lambda s: (previous[s], s is not None, s or ()))
    states = []
    for i in reversed(range(len(photos))):
        states.append(state)
        state = links[i][state]
    states.reverse()
    reasons = ["Unresolved evidence" if s is None else "Direct subject evidence" if s == direct[i]
               else "Species inferred from neighboring frames" for i, s in enumerate(states)]
    return _groups(photos, states, reasons)


def run_algorithm(name, photos, params=None, grouping_config=None):
    params = params or {}
    restore_features(photos)
    if name == "production":
        from encounters import DEFAULTS as PRODUCTION_DEFAULTS
        from encounters import grouping_species_predictions, segment_encounters
        from species_identity import species_entry_key
        if set(params) - set(PRODUCTION_DEFAULTS):
            raise ValueError("Unknown production grouping parameter")
        groups = []
        for encounter in segment_encounters(photos, config={**(grouping_config or {}), **params}):
            species = encounter["species"][0]
            # Recover the winning taxon by identity, not by display name: the
            # confidence-weighted vote in encounter_species_label already picks
            # a specific source identity, so collapsing here on a shared display
            # name could score the encounter as the wrong taxon.
            weights = defaultdict(float)
            key_by_identity = {}
            for encounter_photo in encounter["photos"]:
                for entry in (grouping_species_predictions(encounter_photo) or []):
                    identity = species_entry_key(entry)
                    weights[identity] += entry[1]
                    lookup = encounter_photo.get("species_keys", {}).get(identity)
                    if lookup is not None:
                        key_by_identity[identity] = lookup
            if species and weights:
                winner = max(weights, key=weights.get)
                key = key_by_identity.get(winner) or ("name:" + normalize(species))
            else:
                key = "name:" + normalize(species) if species else None
            groups.append(Group(tuple(p["id"] for p in encounter["photos"]),
                                (key,) if species else None, "Current production grouping and species label"))
    elif name in {"independent", "sequence"}:
        cfg = {**DEFAULTS, **params}
        if set(params) - set(DEFAULTS):
            raise ValueError("Unknown candidate parameter")
        for key in ("confidence", "margin", "detector_confidence"):
            if not isinstance(cfg[key], (int, float)) or not 0 <= cfg[key] <= 1:
                raise ValueError(f"{key} must be between 0 and 1")
        if not isinstance(cfg["context_frames"], int) or not 0 <= cfg["context_frames"] <= 32:
            raise ValueError("context_frames must be an integer between 0 and 32")
        for key in ("context_seconds", "transition_penalty"):
            if not isinstance(cfg[key], (int, float)) or not math.isfinite(cfg[key]) or cfg[key] < 0:
                raise ValueError(f"{key} must be finite and nonnegative")
        # Keep reliable time/scene session cuts independent of species states.
        chunks = [[]]
        hard_gap = (grouping_config or {}).get("hard_cut_time", 180.0)
        for photo in photos:
            if chunks[-1]:
                before, now = timestamp(chunks[-1][-1]["timestamp"]), timestamp(photo["timestamp"])
                if before and now and (now - before).total_seconds() > hard_gap:
                    chunks.append([])
            chunks[-1].append(photo)
        groups = []
        for chunk in chunks:
            if name == "sequence":
                groups.extend(_sequence(chunk, cfg))
            else:
                obs = [observation(p, cfg) for p in chunk]
                states = [taxa if taxa and not unresolved else None for taxa, unresolved, _ in obs]
                groups.extend(_groups(chunk, states, ["Independent subject evidence"] * len(chunk)))
    else:
        raise ValueError(f"Unknown algorithm: {name}")
    validate_groups(photos, groups)
    return groups
