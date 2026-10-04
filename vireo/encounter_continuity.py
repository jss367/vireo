"""Conservative sequence evidence for encounter grouping, without editing tags.

Detections use the database reader's compact shape (x/y/w/h, confidence,
detector_model, category) plus identity-bearing ``predictions`` tuples. Raw
classifier evidence stays separate from the grouping-only feature adjustments.
"""

import math
from collections import defaultdict
from datetime import UTC, datetime

from encounters import _confident_species_prediction
from weak_detections import contextual_weak_runs


def _time(photo):
    try:
        value = photo.get("timestamp")
        value = value if isinstance(value, datetime) else datetime.fromisoformat(value)
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    except (TypeError, ValueError):
        return None


def _box(detection):
    try:
        x, y, w, h = (float(detection[k]) for k in ("x", "y", "w", "h"))
    except (KeyError, TypeError, ValueError):
        return None
    if (
        not all(math.isfinite(v) for v in (x, y, w, h))
        or min(x, y) < 0
        or min(w, h) <= 0
        or x + w > 1.001
        or y + h > 1.001
    ):
        return None
    return x, y, w, h


def _overlap(a, b):
    a, b = _box(a), _box(b)
    if a is None or b is None:
        return 0.0
    x, y, w, h = a
    xx, yy, ww, hh = b
    area = max(0, min(x + w, xx + ww) - max(x, xx)) * max(0, min(y + h, yy + hh) - max(y, yy))
    return area / (w * h + ww * hh - area)


def _winner(entries, confidence=0.8):
    return _confident_species_prediction(
        {"species_top5": entries},
        {
            "species_hard_cut_confidence": confidence,
            "species_hard_cut_margin": 0.6,
        },
    )


def _conflicting_model(entries, target):
    """Any independent confident classifier can veto a continuity repair."""
    by_model = defaultdict(list)
    for entry in entries:
        by_model[entry[2] if len(entry) > 2 else "unknown"].append(entry)
    for values in by_model.values():
        winner = _winner(values)
        if winner and winner["key"] != target:
            return True
    return False


def _weak_windows(photos, max_gap):
    """Cheap temporal preselection before loading low-confidence evidence."""
    folders = defaultdict(list)
    for p in photos:
        when = _time(p)
        if when is not None and p.get("folder_id") is not None:
            folders[p["folder_id"]].append((when, p["id"], p))
    for folder in folders.values():
        ordered = [p for _, _, p in sorted(folder)]
        index = 0
        while index < len(ordered):
            if ordered[index].get("subject_present"):
                index += 1
                continue
            start = index
            while index < len(ordered) and not ordered[index].get("subject_present"):
                index += 1
            if start == 0 or index == len(ordered) or index - start > 3:
                continue
            window = ordered[start - 1 : index + 1]
            if (
                any(p.get("subject_absent") for p in window[1:-1])
                and (_time(window[-1]) - _time(window[0])).total_seconds() <= max_gap
            ):
                yield window


def _flip_window(left, middle, right):
    triple = (left, middle, right)
    if any(p.get("isolated_species_context") for p in triple):
        return None
    if any(not p.get("subject_present") or p.get("subject_absent") for p in triple):
        return None
    if left.get("folder_id") is None or len({p.get("folder_id") for p in triple}) != 1:
        return None
    times = [_time(p) for p in triple]
    if any(t is None for t in times):
        return None
    gaps = [(times[i + 1] - times[i]).total_seconds() for i in range(2)]
    if min(gaps) <= 0 or sum(gaps) > 0.30:
        return None
    winners = [_winner(p.get("species_top5") or [], 0.95 if i != 1 else 0.8) for i, p in enumerate(triple)]
    if not all(winners):
        return None
    a, m, b = winners
    if a["key"] != b["key"] or a["key"] == m["key"]:
        return None
    if m["confidence"] > 0.90 or min(a["confidence"], b["confidence"]) - m["confidence"] < 0.10:
        return None
    if len({e[2] for e in middle["species_top5"] if len(e) > 2}) != 1:
        return None
    return winners, sum(gaps)


def evidence_photo_ids(photos, config):
    """Load additional evidence only for potentially eligible short windows."""
    needed = set()
    pipeline = config.get("pipeline", {})
    if pipeline.get("weak_detection_rescue_enabled", True):
        for window in _weak_windows(photos, min(pipeline.get("burst_time_gap", 3.0), 3.0)):
            needed.update(p["id"] for p in window)
    for triple in zip(photos, photos[1:], photos[2:], strict=False):
        if _flip_window(*triple):
            needed.update(p["id"] for p in triple)
    return needed


def _animal_detections(evidence):
    return {
        pid: sorted(
            (
                d
                for d in detections
                if d["detector_model"] == "megadetector-v6" and d["category"] == "animal" and (d["confidence"] or 0) > 0
            ),
            key=lambda d: -d["confidence"],
        )
        for pid, detections in evidence.items()
    }


def _predictions(detections, top_k):
    return sorted((p for d in detections for p in d.get("predictions", [])), key=lambda p: -p[1])[:top_k]


def _recover_weak(photos, evidence, animals, config):
    pipeline = config.get("pipeline", {})
    if not pipeline.get("weak_detection_rescue_enabled", True):
        return photos
    floor = config.get("detector_confidence", 0.2)
    top_k = config.get("top_k_predictions", 5)
    max_gap = min(pipeline.get("burst_time_gap", 3.0), 3.0)
    by_id = {p["id"]: p for p in photos}
    changed = {}
    timed = [{"id": p["id"], "folder_id": p.get("folder_id"), "timestamp": _time(p)} for p in photos]
    for run in contextual_weak_runs(timed, animals, detector_confidence=floor, weak_confidence=1e-6, max_gap=max_gap):
        ids, left, right = run["photo_ids"], run["left_photo_id"], run["right_photo_id"]
        if len(ids) > 3 or not any(by_id[pid].get("subject_absent") for pid in ids):
            continue
        if (_time(by_id[right]) - _time(by_id[left])).total_seconds() > max_gap:
            continue
        if any(sum(d["confidence"] >= floor for d in animals[pid]) != 1 for pid in (left, right)):
            continue
        winners = [_winner(by_id[pid].get("species_top5") or []) for pid in (left, right)]
        if not all(winners) or winners[0]["key"] != winners[1]["key"]:
            continue
        target = winners[0]["key"]
        selected = {}
        for pid in ids:
            if any(_overlap(animals[pid][0], animals[a][0]) < 0.02 for a in (left, right)):
                break
            if any(_conflicting_model(d.get("predictions", []), target) for d in animals[pid][1:]):
                break
            full_detections = [d for d in evidence[pid] if d["detector_model"] == "full-image"]
            full = _predictions(full_detections, top_k)
            crop = _predictions([animals[pid][0]], top_k)
            if any(_conflicting_model(d.get("predictions", []), target) for d in [*full_detections, animals[pid][0]]):
                break
            options = [(name, entries, _winner(entries)) for name, entries in (("full_image", full), ("crop", crop))]
            if any(w and w["key"] != target for _, _, w in options):
                break
            match = next(((name, entries) for name, entries, w in options if w and w["key"] == target), None)
            if match is None:
                break
            selected[pid] = match
        else:
            for pid in ids:
                if not by_id[pid].get("subject_absent"):
                    continue
                source, entries = selected[pid]
                d = animals[pid][0]
                changed[pid] = {
                    **by_id[pid],
                    "species_top5": entries,
                    "subject_absent": False,
                    "subject_present": False,
                    "subject_uncertain": True,
                    "detection_box": {k: d[k] for k in ("x", "y", "w", "h")},
                    "detection_conf": d["confidence"],
                    "weak_detection_context": {
                        "evidence": source + "_sequence",
                        "species_key": target,
                        "left_photo_id": left,
                        "right_photo_id": right,
                    },
                }
    return [changed.get(p["id"], p) for p in photos]


def _matching_subject(detections, target, floor, confidence):
    matches = []
    for d in detections:
        if _conflicting_model(d.get("predictions", []), target):
            return None
        if d["confidence"] >= floor:
            strong = _winner(d.get("predictions", []), confidence)
            if strong and strong["key"] == target and _box(d):
                matches.append(d)
    return matches[0] if len(matches) == 1 else None


def _visual_conflict(middle, anchors):
    for key in ("dino_subject_embedding", "dino_global_embedding"):
        for anchor in anchors:
            u, v = middle.get(key), anchor.get(key)
            if u is None or v is None:
                continue
            if len(u) != len(v):
                return True
            norm = math.sqrt(sum(float(x) ** 2 for x in u) * sum(float(x) ** 2 for x in v))
            if not math.isfinite(norm) or norm <= 0:
                return True
            if sum(float(x) * float(y) for x, y in zip(u, v, strict=True)) / norm < 0.8:
                return True
    return False


def _suppress_isolated(photos, evidence, animals, config):
    proposed = {}
    floor = config.get("detector_confidence", 0.2)
    for left, middle, right in zip(photos, photos[1:], photos[2:], strict=False):
        context = _flip_window(left, middle, right)
        if context is None:
            continue
        winners, duration = context
        detections = [
            _matching_subject(animals.get(p["id"], []), winner["key"], floor, 0.95 if i != 1 else 0.8)
            for i, (p, winner) in enumerate(zip((left, middle, right), winners, strict=True))
        ]
        if any(d is None for d in detections):
            continue
        # Display-level top K can hide an agreeing second classifier. Keep
        # corroborated crop classifications using the complete detection rows.
        middle_models = {entry[2] if len(entry) > 2 else "unknown"
                         for entry in detections[1].get("predictions", [])}
        if len(middle_models) != 1:
            continue
        overlaps = [
            _overlap(detections[0], detections[1]),
            _overlap(detections[1], detections[2]),
            _overlap(detections[0], detections[2]),
        ]
        if min(overlaps) < 0.80:
            continue
        full = [
            entry
            for d in evidence.get(middle["id"], [])
            if d["detector_model"] == "full-image"
            for entry in d.get("predictions", [])
        ]
        if _conflicting_model(full, winners[0]["key"]):
            continue
        if _visual_conflict(middle, (left, right)):
            continue
        proposed[middle["id"]] = {
            "anchor_ids": [left["id"], right["id"]],
            "anchor_species": winners[0]["species"],
            "anchor_key": winners[0]["key"],
            "anchor_confidences": [winners[0]["confidence"], winners[2]["confidence"]],
            "conflicting_species": winners[1]["species"],
            "conflicting_confidence": winners[1]["confidence"],
            "span_seconds": duration,
            "overlaps": overlaps,
            "original_species_top5": list(middle["species_top5"]),
        }
    accepted = {pid: d for pid, d in proposed.items() if not set(d["anchor_ids"]) & proposed.keys()}
    # No synthetic classifier score: abstain for grouping and let surrounding
    # observations supply the encounter suggestion. Per-subject predictions stay intact.
    return [
        {**p, "grouping_species_top5": [], "isolated_species_context": accepted[p["id"]]} if p["id"] in accepted else p
        for p in photos
    ]


def apply_encounter_continuity(photos, evidence_by_photo, config=None, *, repair_isolated=True):
    """Return grouping features with the evaluated repairs; never mutate inputs."""
    config = config or {}
    animals = _animal_detections(evidence_by_photo)
    photos = _recover_weak(photos, evidence_by_photo, animals, config)
    return _suppress_isolated(photos, evidence_by_photo, animals, config) if repair_isolated else photos
