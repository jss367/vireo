"""Read-only location review suggestions and GPS discrepancy detection."""

import hashlib
import json
import math
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from xmp import read_sync_preview_metadata


def has_usable_coordinates(photo):
    try:
        lat, lng = float(photo["latitude"]), float(photo["longitude"])
    except (TypeError, ValueError):
        return False
    return math.isfinite(lat) and math.isfinite(lng) and -90 <= lat <= 90 and -180 <= lng <= 180


def distance_meters(first, second):
    lat1, lon1, lat2, lon2 = map(math.radians, (
        first["latitude"], first["longitude"], second["latitude"], second["longitude"],
    ))
    haversine = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 6371000 * 2 * math.asin(math.sqrt(min(1, max(0, haversine))))


def _read_discrepancy_sidecars(paths, sidecar_reader=None):
    # Deduplicate RAW/JPEG companions. The caller supplies at most one database
    # batch (400 photos), so both worker count and queued work stay bounded.
    paths = list(dict.fromkeys(paths))
    if sidecar_reader is not None or len(paths) < 2:
        reader = sidecar_reader or read_sync_preview_metadata
        return {path: reader(path) for path in paths}
    with ThreadPoolExecutor(max_workers=min(8, len(paths))) as pool:
        return dict(zip(paths, pool.map(read_sync_preview_metadata, paths), strict=True))


def gps_discrepancies(db, photo_ids, minimum_distance_m=500, include_reviewed=False, *, sidecar_reader=None):
    """Compare original photo GPS with the same assigned place used by sync.

    Callers authorize the entire selection first. Only discrepant photos need
    a sidecar read. An existing correction is resolved only when it actually
    exists on disk, never merely because a sync was queued or attempted.
    A cached ``sidecar_reader`` lets callers revalidate database evidence
    under a writer lock without filesystem I/O; None means the path was
    not in that snapshot and must be reviewed again.
    """
    result = []
    for offset in range(0, len(photo_ids), 400):
        chunk = photo_ids[offset:offset + 400]
        placeholders = ','.join('?' for _ in chunk)
        rows = db.conn.execute(f"""
            WITH ranked AS (
                SELECT pk.photo_id, k.id keyword_id, k.name, k.latitude, k.longitude,
                       ROW_NUMBER() OVER (PARTITION BY pk.photo_id
                         ORDER BY (k.parent_id IS NULL) ASC, k.id DESC) rn
                FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id
                WHERE pk.photo_id IN ({placeholders}) AND k.type = 'location'
                  AND k.latitude IS NOT NULL AND k.longitude IS NOT NULL
            )
            SELECT p.id, p.filename, p.companion_path, p.timestamp, p.latitude, p.longitude,
                   f.path folder_path, k.keyword_id, k.name assigned_name,
                   k.latitude assigned_latitude, k.longitude assigned_longitude,
                   r.fingerprint reviewed_fingerprint
            FROM ranked k JOIN photos p ON p.id = k.photo_id
            JOIN folders f ON f.id = p.folder_id
            LEFT JOIN location_gps_reviews r ON r.photo_id = p.id
            WHERE k.rn = 1
            ORDER BY p.timestamp, p.id
        """, chunk).fetchall()
        candidates = []
        for row in rows:
            photo = dict(row)
            assigned = {
                "keyword_id": row["keyword_id"], "name": row["assigned_name"],
                "latitude": row["assigned_latitude"], "longitude": row["assigned_longitude"],
            }
            if not has_usable_coordinates(photo) or not has_usable_coordinates(assigned):
                continue
            distance = distance_meters(photo, assigned)
            if distance <= minimum_distance_m:
                continue
            sidecar_path = os.path.join(row["folder_path"], os.path.splitext(row["filename"])[0] + '.xmp')
            candidates.append((photo, assigned, distance, sidecar_path))
        sidecars = _read_discrepancy_sidecars((item[3] for item in candidates), sidecar_reader)
        for photo, assigned, distance, sidecar_path in candidates:
            metadata = sidecars[sidecar_path]
            if metadata is None:
                continue
            override = metadata.get("location")
            if override and has_usable_coordinates(override) and distance_meters(override, assigned) < 1:
                continue
            # Re-review when either coordinate source or the on-disk override changes.
            evidence = [photo["latitude"], photo["longitude"], assigned, override, metadata["status"]]
            fingerprint = hashlib.sha256(json.dumps(evidence, sort_keys=True).encode()).hexdigest()
            if not include_reviewed and photo["reviewed_fingerprint"] == fingerprint:
                continue
            result.append({
                key: photo[key] for key in (
                    "id", "filename", "companion_path", "timestamp", "latitude", "longitude",
                )
            } | {"assigned_location": assigned, "distance_m": distance, "fingerprint": fingerprint,
                 "sidecar_location": override})
    return result


def discrepancy_groups(photos):
    """Keep batches bounded to a place and capture date; expose every photo."""
    batches = {}
    for photo in sorted(photos, key=lambda p: (p["timestamp"] or '', p["id"])):
        timestamp = capture_time(photo["timestamp"])
        key = (photo["assigned_location"]["keyword_id"], str(timestamp.date()) if timestamp else photo["id"])
        batches.setdefault(key, []).append(photo)
    return [{
        "id": index, "count": len(batch), "photos": batch,
        "photo_ids": [p["id"] for p in batch],
        "assigned_location": batch[0]["assigned_location"],
        "center": {"lat": batch[0]["latitude"], "lng": batch[0]["longitude"]},
        "captured_from": batch[0]["timestamp"], "captured_to": batch[-1]["timestamp"],
        "spread_m": max(p["distance_m"] for p in batch),
    } for index, batch in enumerate(
        (items[start:start + 100] for items in batches.values() for start in range(0, len(items), 100)), start=1,
    )]


def capture_time(value):
    """Keep camera wall time separate from timestamps with known offsets.

    A date alone is insufficient evidence for grouping. Unknown offsets must
    not silently be interpreted in the server's timezone. Retain the camera's
    local date; aware datetime comparisons and subtraction account for offsets.
    """
    if not isinstance(value, str) or len(value) < 16:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, OverflowError):
        return None


def time_review_groups(photos, gap_minutes=60):
    """Suggest bounded outings; never infer coordinates from capture time.

    Split at a gap, calendar-day boundary, or four hours from the first photo.
    Missing/invalid times stay as singletons. Offset-aware and naive times
    form separate queues because their relative order is unknown.
    """
    dated = []
    undated = []
    for photo in photos:
        if has_usable_coordinates(photo):
            continue
        timestamp = capture_time(photo["timestamp"])
        if timestamp is None:
            undated.append([photo])
        else:
            dated.append((timestamp.tzinfo is not None, timestamp, photo))
    dated.sort(key=lambda item: (item[0], item[1], item[2]["id"]))
    batches = []
    first = previous = None
    previous_aware = None
    for aware, timestamp, photo in dated:
        if (
            first is None
            or aware != previous_aware
            or timestamp.date() != previous.date()
            or timestamp - previous > timedelta(minutes=gap_minutes)
            or timestamp - first > timedelta(hours=4)
        ):
            batches.append([])
            first = timestamp
        batches[-1].append(photo)
        previous, previous_aware = timestamp, aware
    batches.extend(sorted(undated, key=lambda batch: batch[0]["id"]))
    groups = []
    for index, batch in enumerate(batches, start=1):
        data = [
            {
                key: photo[key]
                for key in (
                    "id",
                    "filename",
                    "companion_path",
                    "timestamp",
                    "latitude",
                    "longitude",
                )
            }
            for photo in batch
        ]
        for item in data:
            # Invalid/partial coordinates are not map evidence and may include
            # non-finite values which browsers cannot parse as JSON.
            item["latitude"] = item["longitude"] = None
        groups.append(
            {
                "id": f"time-{index}",
                "grouping": "time",
                "count": len(data),
                "photo_ids": [photo["id"] for photo in data],
                "photos": data,
                "center": None,
                "bounds": None,
                "spread_m": None,
                "captured_from": data[0]["timestamp"] if capture_time(data[0]["timestamp"]) else None,
                "captured_to": data[-1]["timestamp"] if capture_time(data[-1]["timestamp"]) else None,
            }
        )
    return groups
