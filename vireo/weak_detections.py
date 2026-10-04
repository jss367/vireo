"""Context-aware rescue for low-confidence animal detections.

MegaDetector confidence is intentionally conservative, but treating a box at
0.199 as definitive evidence that no subject exists creates a sharp grouping
cliff.  This module identifies only the safer case: a contiguous run of weak
animal detections, in one folder, bracketed by normal-confidence detections in
the same short camera sequence.

The classifier may evaluate every candidate run.  Encounter grouping applies
the stronger ``matching_anchor_species`` gate before treating the middle
frames as uncertain rather than absent.
"""

from __future__ import annotations

from datetime import datetime


def _get(row, key, default=None):
    """Read from plain dicts and sqlite3.Row values."""
    if hasattr(row, "get"):
        return row.get(key, default)
    try:
        value = row[key]
    except (KeyError, IndexError, TypeError):
        return default
    return value


def _parse_timestamp(value):
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _confidence(detection):
    value = _get(detection, "confidence")
    if value is None:
        value = _get(detection, "detector_confidence")
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _max_animal_confidence(detections):
    values = [
        _confidence(det)
        for det in (detections or [])
        if _get(det, "category", "animal") == "animal"
        and _get(det, "detector_model") != "full-image"
    ]
    return max(values, default=0.0)


def contextual_weak_runs(
    photos,
    detections_by_photo,
    *,
    detector_confidence=0.20,
    weak_confidence=0.12,
    max_gap=3.0,
):
    """Return weak runs bracketed by strong detections.

    Each result is a dict containing ``photo_ids``, ``left_photo_id``, and
    ``right_photo_id``.  A weak run is eligible only when every adjacent pair
    is within ``max_gap`` seconds and every photo is in the same folder.  A
    truly empty frame, a box below ``weak_confidence``, a folder boundary, or
    a long pause breaks the bridge.
    """
    if weak_confidence >= detector_confidence:
        return []

    photos_by_folder = {}
    for photo in photos or []:
        timestamp = _parse_timestamp(_get(photo, "timestamp"))
        folder_id = _get(photo, "folder_id")
        if timestamp is None or folder_id is None:
            continue
        photo_id = _get(photo, "id")
        confidence = _max_animal_confidence(
            detections_by_photo.get(photo_id, [])
        )
        if confidence >= detector_confidence:
            state = "strong"
        elif confidence >= weak_confidence:
            state = "weak"
        else:
            state = "none"
        photos_by_folder.setdefault(folder_id, []).append({
            "id": photo_id,
            "folder_id": folder_id,
            "timestamp": timestamp,
            "confidence": confidence,
            "state": state,
        })

    runs = []
    for ordered in photos_by_folder.values():
        # Analyze each folder independently. Different imports can contain
        # overlapping capture times; an unrelated photo from another folder
        # must not interrupt an otherwise valid camera sequence.
        ordered.sort(key=lambda item: (item["timestamp"], item["id"] or 0))
        index = 0
        while index < len(ordered):
            if ordered[index]["state"] != "weak":
                index += 1
                continue
            start = index
            while (
                index + 1 < len(ordered)
                and ordered[index + 1]["state"] == "weak"
            ):
                index += 1
            end = index
            left = ordered[start - 1] if start > 0 else None
            right = ordered[end + 1] if end + 1 < len(ordered) else None
            weak_items = ordered[start:end + 1]

            if (
                left is not None
                and right is not None
                and left["state"] == "strong"
                and right["state"] == "strong"
            ):
                sequence = [left, *weak_items, right]
                gaps_are_short = all(
                    0.0
                    <= (b["timestamp"] - a["timestamp"]).total_seconds()
                    <= max_gap
                    for a, b in zip(sequence, sequence[1:], strict=False)
                )
                if gaps_are_short:
                    runs.append({
                        "photo_ids": [item["id"] for item in weak_items],
                        "left_photo_id": left["id"],
                        "right_photo_id": right["id"],
                        "left_confidence": left["confidence"],
                        "right_confidence": right["confidence"],
                    })
            index += 1
    return runs


def contextual_weak_photo_ids(
    photos,
    detections_by_photo,
    *,
    detector_confidence=0.20,
    weak_confidence=0.12,
    max_gap=3.0,
):
    """Return photo IDs selected for contextual weak-box classification."""
    return {
        photo_id
        for run in contextual_weak_runs(
            photos,
            detections_by_photo,
            detector_confidence=detector_confidence,
            weak_confidence=weak_confidence,
            max_gap=max_gap,
        )
        for photo_id in run["photo_ids"]
    }


def _normalized_species(name):
    return " ".join(str(name or "").strip().casefold().split())


def _anchor_species(photo_id, species_by_photo, confirmed_by_photo, min_confidence):
    confirmed = (confirmed_by_photo or {}).get(photo_id)
    if confirmed:
        return _normalized_species(confirmed), str(confirmed), 1.0

    best = None
    for entry in (species_by_photo or {}).get(photo_id, []):
        if not entry or len(entry) < 2:
            continue
        try:
            confidence = float(entry[1])
        except (TypeError, ValueError):
            continue
        if confidence < min_confidence:
            continue
        name = str(entry[0] or "").strip()
        key = _normalized_species(name)
        if key and (best is None or confidence > best[2]):
            best = (key, name, confidence)
    return best


def matching_anchor_species(
    run,
    species_by_photo,
    *,
    confirmed_by_photo=None,
    min_confidence=0.40,
):
    """Return shared anchor species metadata, or ``None`` when unsafe.

    Both strong anchor photos must independently identify the same species.
    A user-confirmed keyword is accepted as confidence 1.0; otherwise the
    best classifier result must meet ``min_confidence``.
    """
    left = _anchor_species(
        run["left_photo_id"], species_by_photo, confirmed_by_photo,
        min_confidence,
    )
    right = _anchor_species(
        run["right_photo_id"], species_by_photo, confirmed_by_photo,
        min_confidence,
    )
    if left is None or right is None or left[0] != right[0]:
        return None
    return {
        "species": left[1],
        "left_confidence": left[2],
        "right_confidence": right[2],
        "left_photo_id": run["left_photo_id"],
        "right_photo_id": run["right_photo_id"],
    }


def matching_full_image_runs(
    photos, detections_by_photo, species_by_photo, fallback_by_photo, *,
    detector_confidence=0.20, max_gap=3.0, config=None,
):
    """Bridge short detector dropouts corroborated by full-image classifiers.

    Unlike ordinary weak-box rescue, every middle frame must independently
    identify the anchors' species with the confident-species gate. Require a
    real positive-confidence animal box overlapping both anchor boxes, at most
    three middle frames, and at most three seconds from anchor to anchor. A
    full-image prediction alone cannot turn an empty scene into an encounter.
    Species keys (including source identities), not display names, must agree.
    No confirmed photo tags are used as evidence here.
    """
    from encounters import _confident_species_prediction

    def best_box(pid):
        candidates = [d for d in detections_by_photo.get(pid, [])
                      if _get(d, "category", "animal") == "animal"
                      and _get(d, "detector_model") != "full-image"]
        return max(candidates, key=_confidence, default=None)

    def overlap(a, b):
        if a is None or b is None:
            return 0.0
        try:
            ax, ay, aw, ah = (float(_get(a, k)) for k in ("x", "y", "w", "h"))
            bx, by, bw, bh = (float(_get(b, k)) for k in ("x", "y", "w", "h"))
        except (TypeError, ValueError):
            return 0.0
        intersection = max(0, min(ax+aw, bx+bw)-max(ax, bx)) * max(0, min(ay+ah, by+bh)-max(ay, by))
        union = aw*ah + bw*bh - intersection
        return intersection / union if union > 0 else 0.0

    by_id = {_get(p, 'id'): p for p in photos}
    matched = []
    # A tiny positive floor excludes zero-box detector runs. This is NOT a
    # new general detection threshold: the independent classifier, geometry,
    # short-span, and two-sided identity gates below must all pass.
    for run in contextual_weak_runs(photos, detections_by_photo,
                                   detector_confidence=detector_confidence,
                                   weak_confidence=1e-6, max_gap=min(max_gap, 3.0)):
        ids = run['photo_ids']
        if len(ids) > 3 or any(pid not in fallback_by_photo for pid in ids):
            continue
        left, right = run['left_photo_id'], run['right_photo_id']
        duration = (_parse_timestamp(_get(by_id[right], 'timestamp')) -
                    _parse_timestamp(_get(by_id[left], 'timestamp'))).total_seconds()
        if duration > min(max_gap, 3.0):
            continue
        winners = [_confident_species_prediction({'species_top5': entries}, config=config)
                   for entries in [species_by_photo.get(left), species_by_photo.get(right),
                                   *(fallback_by_photo[pid] for pid in ids)]]
        if any(winner is None for winner in winners) or len({winner['key'] for winner in winners}) != 1:
            continue
        # Multiple qualifying subjects on an anchor make a whole-image label
        # insufficient to identify which subject continued through the gap.
        if any(sum(_confidence(d) >= detector_confidence and _get(d, 'category', 'animal') == 'animal'
                   for d in detections_by_photo.get(pid, [])) != 1 for pid in (left, right)):
            continue
        if any(overlap(best_box(pid), best_box(anchor)) < 0.10 for pid in ids for anchor in (left, right)):
            continue
        matched.append({**run, 'species': winners[0]['species'], 'species_key': winners[0]['key'],
                        'evidence': 'full_image_sequence',
                        'left_confidence': winners[0]['confidence'], 'right_confidence': winners[1]['confidence']})
    return matched
