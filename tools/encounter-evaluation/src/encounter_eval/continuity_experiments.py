"""Offline continuity challengers; these rules are never loaded by the app.

Start with the merged production repairs, then test one bounded relaxation.
No reference labels are accepted by this module.
"""

from .continuity import apply_continuity, native_evidence


def candidates():
    specs = [{"id": "current", "family": "current", "params": {}}]
    for name, family, params in (
        ("weak-more-motion", "weak-motion", {"overlap": 0.005}),
        ("weak-longer-span", "weak-length", {"span": 10.0}),
        ("weak-more-frames", "weak-length", {"frames": 8}),
        ("weak-longer-sequence", "weak-length", {"span": 10.0, "frames": 8}),
        ("weak-lower-confidence", "weak-confidence", {"confidence": 0.6, "margin": 0.2}),
        ("weak-lower-confidence-moving", "weak-confidence", {"confidence": 0.6, "margin": 0.2, "overlap": 0.005}),
        ("weak-anchor-context", "weak-context", {"anchor_only": True, "overlap": 0.2, "box_floor": 0.03}),
        ("isolated-longer-span", "isolated", {"kind": "isolated", "span": 1.0}),
        ("isolated-more-motion", "isolated", {"kind": "isolated", "overlap": 0.5}),
        ("isolated-longer-moving", "isolated", {"kind": "isolated", "span": 1.0, "overlap": 0.5}),
    ):
        specs.append({"id": name, "family": family, "params": params})
    return specs


def _winner(entries, confidence, margin):
    from encounters import _confident_species_prediction

    return _confident_species_prediction(
        {"species_top5": entries},
        {"species_hard_cut_confidence": confidence, "species_hard_cut_margin": margin},
    )


def _weak(photos, evidence, animals, config, params):
    from encounter_continuity import _conflicting_model, _overlap, _predictions, _time, _visual_conflict
    from encounters import grouping_species_predictions
    from weak_detections import contextual_weak_runs

    if not config.get("pipeline", {}).get("weak_detection_rescue_enabled", True):
        return photos
    floor = config.get("detector_confidence", 0.2)
    span, frames = params.get("span", 3.0), params.get("frames", 3)
    confidence, margin = params.get("confidence", 0.8), params.get("margin", 0.6)
    overlap, box_floor = params.get("overlap", 0.02), params.get("box_floor", 1e-6)
    top_k = config.get("top_k_predictions", 5)
    by_id = {p["id"]: p for p in photos}
    changes = {}
    timed = [{"id": p["id"], "folder_id": p.get("folder_id"), "timestamp": _time(p)} for p in photos]
    for run in contextual_weak_runs(
        timed,
        animals,
        detector_confidence=floor,
        weak_confidence=box_floor,
        max_gap=span,
    ):
        ids = run["photo_ids"]
        anchors = [by_id[run[k]] for k in ("left_photo_id", "right_photo_id")]
        if len(ids) > frames or not any(by_id[pid].get("subject_absent") for pid in ids):
            continue
        if (_time(anchors[1]) - _time(anchors[0])).total_seconds() > span:
            continue
        if any(a.get("isolated_species_context") for a in anchors):
            continue
        if any(sum(d["confidence"] >= floor for d in animals[a["id"]]) != 1 for a in anchors):
            continue
        winners = [_winner(grouping_species_predictions(a) or [], 0.8, 0.6) for a in anchors]
        if not all(winners) or winners[0]["key"] != winners[1]["key"]:
            continue
        target = winners[0]["key"]
        # Even an anchor's secondary subject or hidden classifier may veto.
        if any(_conflicting_model(d["predictions"], target) for a in anchors for d in evidence[a["id"]]):
            continue
        selected = {}
        for pid in ids:
            primary = animals[pid][0]
            if any(_overlap(primary, animals[a["id"]][0]) < overlap for a in anchors):
                break
            if _visual_conflict(by_id[pid], anchors):
                break
            if any(_conflicting_model(d["predictions"], target) for d in evidence[pid]):
                break
            sources = [
                _predictions([d for d in evidence[pid] if d["detector_model"] == "full-image"], top_k),
                _predictions([primary], top_k),
            ]
            support = None
            for entries in sources:
                winner = _winner(entries, confidence, margin)
                if winner and winner["key"] == target:
                    support = entries
                    break
            if support is None and not params.get("anchor_only", False):
                break
            # Abstain when borrowing anchor context; do not invent a model score.
            selected[pid] = support or []
        else:
            for pid in ids:
                if not by_id[pid].get("subject_absent"):
                    continue
                d = animals[pid][0]
                changes[pid] = {
                    **by_id[pid],
                    "subject_absent": False,
                    "subject_present": False,
                    "subject_uncertain": True,
                    "grouping_species_top5": selected[pid],
                    "detection_box": {k: d[k] for k in ("x", "y", "w", "h")},
                    "detection_conf": d["confidence"],
                    "experimental_continuity": {
                        "kind": "weak",
                        "anchor_ids": [a["id"] for a in anchors],
                        "support": "classifier" if selected[pid] else "anchor_context",
                    },
                }
    return [changes.get(p["id"], p) for p in photos]


def _isolated(photos, evidence, animals, config, params):
    from encounter_continuity import _conflicting_model, _matching_subject, _overlap, _time, _visual_conflict
    from encounters import grouping_species_predictions

    proposed = {}
    floor = config.get("detector_confidence", 0.2)
    for left, middle, right in zip(photos, photos[1:], photos[2:], strict=False):
        triple = (left, middle, right)
        if any(
            p.get("isolated_species_context") or not p.get("subject_present") or p.get("subject_absent") for p in triple
        ):
            continue
        if left.get("folder_id") is None or len({p.get("folder_id") for p in triple}) != 1:
            continue
        times = [_time(p) for p in triple]
        if any(t is None for t in times):
            continue
        gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:], strict=False)]
        if min(gaps) <= 0 or sum(gaps) > params.get("span", 0.3):
            continue
        winners = [
            _winner(grouping_species_predictions(p) or [], 0.8 if i == 1 else 0.95, 0.6) for i, p in enumerate(triple)
        ]
        if not all(winners):
            continue
        a, m, b = winners
        if a["key"] != b["key"] or a["key"] == m["key"] or m["confidence"] > 0.9:
            continue
        if min(a["confidence"], b["confidence"]) - m["confidence"] < 0.1:
            continue
        detections = [
            _matching_subject(animals.get(p["id"], []), w["key"], floor, 0.8 if i == 1 else 0.95)
            for i, (p, w) in enumerate(zip(triple, winners, strict=True))
        ]
        if any(d is None for d in detections):
            continue
        if len({e[2] for e in detections[1]["predictions"]}) != 1:
            continue
        if min(_overlap(detections[i], detections[j]) for i, j in ((0, 1), (1, 2), (0, 2))) < params.get(
            "overlap", 0.8
        ):
            continue
        full = [q for d in evidence[middle["id"]] if d["detector_model"] == "full-image" for q in d["predictions"]]
        if _conflicting_model(full, a["key"]) or _visual_conflict(middle, (left, right)):
            continue
        proposed[middle["id"]] = {"kind": "isolated", "anchor_ids": [left["id"], right["id"]]}
    accepted = {pid: c for pid, c in proposed.items() if not set(c["anchor_ids"]) & proposed.keys()}
    return [
        {**p, "grouping_species_top5": [], "experimental_continuity": accepted[p["id"]]} if p["id"] in accepted else p
        for p in photos
    ]


def prepare_baseline(photos, config):
    """Apply the exact merged rules to a retained pre-merge feature snapshot."""
    baseline = apply_continuity(photos, config)
    evidence, identities = native_evidence(photos)
    from encounter_continuity import _animal_detections

    return baseline, evidence, identities, _animal_detections(evidence)


def apply_candidate(prepared, config, spec):
    photos, evidence, identities, animals = prepared
    if spec["id"] == "current":
        return photos
    params = spec["params"]
    transform = _isolated if params.get("kind") == "isolated" else _weak
    adjusted = transform(photos, evidence, animals, config, params)
    result = []
    for photo in adjusted:
        keys = dict(photo.get("species_keys", {}))
        for entry in photo.get("grouping_species_top5", []):
            if len(entry) > 3 and entry[3] in identities:
                keys[entry[3]] = identities[entry[3]]
        result.append({**photo, "species_keys": keys})
    return result
