"""Burst clustering within encounters for the culling pipeline (Stage 3).

Groups near-duplicates and small pose variants into tight bursts within each
encounter. Uses sequential cuts on time gap, crop pHash hamming distance,
and DINOv2 crop embedding cosine similarity.

All thresholds are configurable with defaults from the pipeline design doc.
"""

import logging

import numpy as np

log = logging.getLogger(__name__)

# Default thresholds (from design doc, subject to calibration)
DEFAULTS = {
    "burst_time_gap": 3.0,  # seconds — cut if delta_t exceeds this
    "burst_phash_threshold": 12,  # hamming distance — cut if exceeds this
    "burst_embedding_threshold": 0.80,  # cosine similarity — cut if below this
}


_warned_dim_mismatch = False


def _cosine_sim(a, b):
    """Cosine similarity between two vectors, clamped to [0, 1]."""
    if a is None or b is None:
        return 1.0  # no embedding → don't cut on this criterion
    if a.shape != b.shape:
        # Stale DINOv2 embeddings from a previous variant can reach here on
        # datasets that straddle a variant switch. Treat as "no embedding
        # signal" — same as the None branch — so the burst-cut decision
        # falls back to time + phash instead of raising.
        global _warned_dim_mismatch
        if not _warned_dim_mismatch:
            log.warning(
                "Embedding dim mismatch in burst detection (%s vs %s) — "
                "stale DINOv2 embeddings present; re-embed affected photos",
                a.shape, b.shape,
            )
            _warned_dim_mismatch = True
        return 1.0
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 1.0
    return max(0.0, float(np.dot(a, b) / (norm_a * norm_b)))


def _hamming_distance(phash_a, phash_b):
    """Hamming distance between two hex-encoded pHash strings.

    Returns:
        int — number of differing bits, or -1 if either is None
    """
    if phash_a is None or phash_b is None:
        return -1  # no hash → don't cut on this criterion
    try:
        int_a = int(phash_a, 16)
        int_b = int(phash_b, 16)
        return bin(int_a ^ int_b).count("1")
    except (ValueError, TypeError):
        return -1


def _parse_timestamp(ts):
    """Parse a timestamp to datetime. Returns None on failure."""
    from datetime import datetime

    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts
    try:
        return datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        return None


def _time_delta_seconds(ts_a, ts_b):
    """Absolute time difference in seconds."""
    if ts_a is None or ts_b is None:
        return 0.0  # no timestamp → don't cut on this criterion
    return abs((ts_a - ts_b).total_seconds())


def detect_bursts(photos, config=None):
    """Detect burst boundaries within an encounter.

    Walk through photos in timestamp order. Cut a new burst between
    adjacent photos (i, i+1) if ANY of these fire:
      - delta_t > burst_time_gap
      - Hamming(phash_crop_i, phash_crop_j) > burst_phash_threshold
      - cosine(dino_subject_i, dino_subject_j) < burst_embedding_threshold

    Args:
        photos: list of photo dicts (already within one encounter), each with:
            - timestamp: datetime or ISO string
            - phash_crop: hex string or None
            - dino_subject_embedding: numpy array or None
        config: optional dict overriding DEFAULTS

    Returns:
        list of burst lists (each inner list is a group of photo dicts)
    """
    cfg = {**DEFAULTS, **(config or {})}

    if len(photos) <= 1:
        return [photos] if photos else []

    # Sort by timestamp (should already be sorted within an encounter, but be safe)
    sorted_photos = sorted(
        photos,
        key=lambda p: _parse_timestamp(p.get("timestamp")) or __import__("datetime").datetime.min,
    )

    cuts = set()
    for i in range(len(sorted_photos) - 1):
        pa = sorted_photos[i]
        pb = sorted_photos[i + 1]

        # Time gap
        ts_a = _parse_timestamp(pa.get("timestamp"))
        ts_b = _parse_timestamp(pb.get("timestamp"))
        dt = _time_delta_seconds(ts_a, ts_b)
        if dt > cfg["burst_time_gap"]:
            cuts.add(i)
            continue

        # Crop pHash hamming distance
        hamming = _hamming_distance(pa.get("phash_crop"), pb.get("phash_crop"))
        if hamming >= 0 and hamming > cfg["burst_phash_threshold"]:
            cuts.add(i)
            continue

        # DINOv2 subject embedding cosine similarity
        cos = _cosine_sim(
            pa.get("dino_subject_embedding"),
            pb.get("dino_subject_embedding"),
        )
        if cos < cfg["burst_embedding_threshold"]:
            cuts.add(i)
            continue

    # Build bursts from cut points
    bursts = []
    start = 0
    for i in sorted(cuts):
        bursts.append(sorted_photos[start: i + 1])
        start = i + 1
    bursts.append(sorted_photos[start:])

    return [b for b in bursts if b]


def segment_bursts_for_encounters(encounters, config=None):
    """Run burst detection on each encounter and attach burst structure.

    Args:
        encounters: list of encounter dicts from segment_encounters(),
                    each with a 'photos' key
        config: optional dict overriding DEFAULTS

    Returns:
        Same encounters list, each enriched with:
            - bursts: list of burst lists
            - burst_count: int
    """
    for enc in encounters:
        bursts = detect_bursts(enc["photos"], config=config)
        enc["bursts"] = bursts
        enc["burst_count"] = len(bursts)

    total_bursts = sum(e["burst_count"] for e in encounters)
    log.info(
        "Burst detection: %d bursts across %d encounters",
        total_bursts,
        len(encounters),
    )
    return encounters
