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
