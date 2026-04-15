"""Encounter segmentation for the culling pipeline (Stage 2).

Groups photos into encounters — contiguous runs of images of the same
subject in the same situation. Two-pass approach:
  Pass 1: Cut timeline into microsegments on adjacent-pair similarity
  Pass 2: Merge neighboring microsegments that are likely the same encounter

All thresholds are configurable with defaults from the pipeline design doc.
"""

import logging
import math
from collections import defaultdict
from datetime import datetime

import numpy as np

log = logging.getLogger(__name__)

# Default thresholds (from design doc, subject to calibration)
DEFAULTS = {
    # S_enc weights
    "w_time": 0.35,
    "w_subj": 0.35,
    "w_global": 0.15,
    "w_species": 0.10,
    "w_meta": 0.05,
    # Similarity parameters
    "tau_enc": 40.0,  # time constant for sim_time (seconds)
    # Pass 1 cut thresholds
    "hard_cut_time": 180.0,  # seconds
    "hard_cut_score": 0.42,
    "soft_cut_score": 0.52,
    # Pass 2 merge thresholds
    "merge_score": 0.62,
    "merge_max_gap": 60.0,  # seconds
    "merge_tau": 20.0,  # time constant for merge gap decay
}


_warned_dim_mismatch = False


def _cosine_sim(a, b):
    """Cosine similarity between two vectors, clamped to [0, 1]."""
    if a is None or b is None:
        return 0.0
    if a.shape != b.shape:
        # Stale DINOv2 embeddings from a previous variant can slip through
        # when pipeline.dinov2_variant isn't configured (load_photo_features
        # only filters when it is). Treat as "no similarity signal" instead
        # of crashing the grouping stage with "shapes not aligned".
        global _warned_dim_mismatch
        if not _warned_dim_mismatch:
            log.warning(
                "Embedding dim mismatch (%s vs %s) — stale DINOv2 embeddings "
                "present; re-embed affected photos or set "
                "pipeline.dinov2_variant in config to drop them cleanly",
                a.shape, b.shape,
            )
            _warned_dim_mismatch = True
        return 0.0
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return max(0.0, float(np.dot(a, b) / (norm_a * norm_b)))


def _parse_timestamp(ts):
    """Parse a timestamp string to datetime. Returns None on failure."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts
    try:
        return datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        return None


def _time_delta_seconds(ts_a, ts_b):
    """Absolute time difference in seconds between two timestamps."""
    if ts_a is None or ts_b is None:
        return float("inf")
    return abs((ts_a - ts_b).total_seconds())


def _haversine_meters(lat1, lon1, lat2, lon2):
    """Haversine distance between two GPS coordinates in meters."""
    R = 6_371_000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# -- Pairwise similarity components (Section 2.1) --


def sim_time(dt_seconds, tau=40.0):
    """Time similarity: exp(-|dt| / tau). Range [0, 1]."""
    if dt_seconds == float("inf"):
        return 0.0
    return math.exp(-abs(dt_seconds) / tau)


def sim_embedding(emb_a, emb_b):
    """Embedding similarity: max(0, cosine(a, b)). Range [0, 1]."""
    return _cosine_sim(emb_a, emb_b)


def sim_species(species_a, species_b):
    """Species similarity via Bhattacharyya coefficient on shared top-5 species.

    Args:
        species_a: list of (species_name, confidence[, model]) tuples
        species_b: list of (species_name, confidence[, model]) tuples

    Returns:
        float in [0, 1]
    """
    if not species_a or not species_b:
        return 0.0
    dict_a = {s[0]: s[1] for s in species_a}
    dict_b = {s[0]: s[1] for s in species_b}
    shared = set(dict_a.keys()) & set(dict_b.keys())
    if not shared:
        return 0.0
    return sum(math.sqrt(dict_a[s] * dict_b[s]) for s in shared)


def sim_meta(photo_a, photo_b):
    """Metadata similarity (focal length + GPS).

    Args:
        photo_a, photo_b: dicts with optional keys 'focal_length', 'latitude', 'longitude'

    Returns:
        float in [0, 1]
    """
    has_gps_a = photo_a.get("latitude") is not None and photo_a.get("longitude") is not None
    has_gps_b = photo_b.get("latitude") is not None and photo_b.get("longitude") is not None
    both_gps = has_gps_a and has_gps_b

    fl_a = photo_a.get("focal_length")
    fl_b = photo_b.get("focal_length")

    # Focal length similarity
    sim_fl = 0.0
    if fl_a and fl_b and fl_a > 0 and fl_b > 0:
        sim_fl = math.exp(-abs(math.log(fl_a / fl_b)) / 0.15)

    if both_gps:
        dist = _haversine_meters(
            photo_a["latitude"], photo_a["longitude"],
            photo_b["latitude"], photo_b["longitude"],
        )
        sim_gps = math.exp(-dist / 30.0)
        return 0.4 * sim_fl + 0.6 * sim_gps
    else:
        return sim_fl


def compute_s_enc(photo_a, photo_b, config=None):
    """Compute the combined encounter similarity score S_enc(a, b).

    Args:
        photo_a, photo_b: dicts with keys:
            - timestamp: datetime or ISO string
            - dino_subject_embedding: numpy array or None
            - dino_global_embedding: numpy array or None
            - species_top5: list of (name, confidence[, model]) tuples or None
            - latitude, longitude: float or None
            - focal_length: float or None
            - burst_id: str or None (camera burst ID)
        config: optional dict overriding DEFAULTS

    Returns:
        float — similarity score (higher = more likely same encounter)
    """
    cfg = {**DEFAULTS, **(config or {})}

    ts_a = _parse_timestamp(photo_a.get("timestamp"))
    ts_b = _parse_timestamp(photo_b.get("timestamp"))
    dt = _time_delta_seconds(ts_a, ts_b)

    st = sim_time(dt, tau=cfg["tau_enc"])
    ss = sim_embedding(photo_a.get("dino_subject_embedding"), photo_b.get("dino_subject_embedding"))
    sg = sim_embedding(photo_a.get("dino_global_embedding"), photo_b.get("dino_global_embedding"))
    sp = sim_species(photo_a.get("species_top5"), photo_b.get("species_top5"))
    sm = sim_meta(photo_a, photo_b)

    # Check which terms are available for renormalization
    weights = {}
    if dt != float("inf"):
        weights["time"] = cfg["w_time"]
    if photo_a.get("dino_subject_embedding") is not None and photo_b.get("dino_subject_embedding") is not None:
        weights["subj"] = cfg["w_subj"]
    if photo_a.get("dino_global_embedding") is not None and photo_b.get("dino_global_embedding") is not None:
        weights["global"] = cfg["w_global"]
    if photo_a.get("species_top5") and photo_b.get("species_top5"):
        weights["species"] = cfg["w_species"]
    # Meta always contributes (even if 0)
    weights["meta"] = cfg["w_meta"]

    total_weight = sum(weights.values())
    if total_weight == 0:
        return 0.0

    scores = {
        "time": st,
        "subj": ss,
        "global": sg,
        "species": sp,
        "meta": sm,
    }

    # Renormalized weighted sum
    s_enc = sum(weights.get(k, 0) * scores[k] for k in scores) / total_weight
    return s_enc


# -- Pass 1: Cut timeline into microsegments (Section 2.2) --


def cut_microsegments(photos, config=None):
    """Sort photos by timestamp and cut into microsegments.

    Args:
        photos: list of photo dicts (see compute_s_enc for required keys)
        config: optional dict overriding DEFAULTS

    Returns:
        list of lists (each inner list is a microsegment of photo dicts)
    """
    cfg = {**DEFAULTS, **(config or {})}

    if len(photos) <= 1:
        return [photos] if photos else []

    # Sort by timestamp
    sorted_photos = sorted(
        photos,
        key=lambda p: _parse_timestamp(p.get("timestamp")) or datetime.min,
    )

    # Compute pairwise S_enc scores
    scores = []
    for i in range(len(sorted_photos) - 1):
        s = compute_s_enc(sorted_photos[i], sorted_photos[i + 1], config=cfg)
        scores.append(s)

    # Determine cut points
    cuts = set()
    recent_scores = []  # sliding window for soft cut detection

    for i, score in enumerate(scores):
        ts_a = _parse_timestamp(sorted_photos[i].get("timestamp"))
        ts_b = _parse_timestamp(sorted_photos[i + 1].get("timestamp"))
        dt = _time_delta_seconds(ts_a, ts_b)

        # Hard rule: shared camera burst ID → no cut
        bid_a = sorted_photos[i].get("burst_id")
        bid_b = sorted_photos[i + 1].get("burst_id")
        if bid_a is not None and bid_b is not None and bid_a == bid_b:
            recent_scores.append(score)
            if len(recent_scores) > 3:
                recent_scores.pop(0)
            continue

        # Hard cut: time gap
        if dt > cfg["hard_cut_time"]:
            cuts.add(i)
            recent_scores = []
            continue

        # Hard cut: low score
        if score < cfg["hard_cut_score"]:
            cuts.add(i)
            recent_scores = []
            continue

        # Soft cut: gradual drift (2 of last 3 scores below threshold)
        recent_scores.append(score)
        if len(recent_scores) > 3:
            recent_scores.pop(0)
        if len(recent_scores) >= 3:
            below = sum(1 for s in recent_scores if s < cfg["soft_cut_score"])
            if below >= 2:
                cuts.add(i)
                recent_scores = []
                continue

    # Build segments from cut points
    segments = []
    start = 0
    for i in sorted(cuts):
        segments.append(sorted_photos[start: i + 1])
        start = i + 1
    segments.append(sorted_photos[start:])

    return [seg for seg in segments if seg]


# -- Pass 2: Merge neighboring microsegments (Section 2.3) --


def _segment_mean_embedding(segment, key):
    """Compute mean embedding for a segment."""
    embeddings = [p[key] for p in segment if p.get(key) is not None]
    if not embeddings:
        return None
    return np.mean(embeddings, axis=0)


def _segment_mean_species(segment):
    """Aggregate species predictions across a segment.

    Returns list of (species, mean_confidence) sorted by confidence desc.
    """
    species_scores = defaultdict(list)
    for p in segment:
        for entry in (p.get("species_top5") or []):
            species_scores[entry[0]].append(entry[1])
    if not species_scores:
        return []
    return sorted(
        [(name, sum(confs) / len(confs)) for name, confs in species_scores.items()],
        key=lambda x: x[1],
        reverse=True,
    )


def _segment_timestamp(segment, which="last"):
    """Get the first or last timestamp from a segment."""
    for p in (segment if which == "first" else reversed(segment)):
        ts = _parse_timestamp(p.get("timestamp"))
        if ts is not None:
            return ts
    return None


def compute_s_seg(seg_a, seg_b, config=None):
    """Compute segment-level merge similarity S_seg(A, B).

    Args:
        seg_a, seg_b: lists of photo dicts (microsegments)
        config: optional dict overriding DEFAULTS

    Returns:
        float — segment merge score
    """
    cfg = {**DEFAULTS, **(config or {})}

    # 1. Mean of top-3 pairwise S_enc between tail(A) and head(B)
    tail_a = seg_a[-min(3, len(seg_a)):]
    head_b = seg_b[: min(3, len(seg_b))]
    pairwise_scores = []
    for pa in tail_a:
        for pb in head_b:
            pairwise_scores.append(compute_s_enc(pa, pb, config=cfg))
    pairwise_scores.sort(reverse=True)
    top3_mean = np.mean(pairwise_scores[: min(3, len(pairwise_scores))])

    # 2. Cosine of mean subject embeddings
    mean_emb_a = _segment_mean_embedding(seg_a, "dino_subject_embedding")
    mean_emb_b = _segment_mean_embedding(seg_b, "dino_subject_embedding")
    emb_sim = _cosine_sim(mean_emb_a, mean_emb_b)

    # 3. Species similarity of segment-level species
    species_a = _segment_mean_species(seg_a)
    species_b = _segment_mean_species(seg_b)
    sp_sim = sim_species(species_a, species_b)

    # 4. Time gap decay
    last_a = _segment_timestamp(seg_a, "last")
    first_b = _segment_timestamp(seg_b, "first")
    gap = _time_delta_seconds(last_a, first_b)
    gap_sim = math.exp(-gap / cfg["merge_tau"]) if gap != float("inf") else 0.0

    return 0.5 * top3_mean + 0.2 * emb_sim + 0.2 * sp_sim + 0.1 * gap_sim


def merge_microsegments(segments, config=None):
    """Merge neighboring microsegments that likely belong to the same encounter.

    Args:
        segments: list of microsegments (each a list of photo dicts)
        config: optional dict overriding DEFAULTS

    Returns:
        list of merged segments
    """
    cfg = {**DEFAULTS, **(config or {})}

    if len(segments) <= 1:
        return segments

    merged = [segments[0]]
    for seg in segments[1:]:
        last_a = _segment_timestamp(merged[-1], "last")
        first_b = _segment_timestamp(seg, "first")
        gap = _time_delta_seconds(last_a, first_b)

        if gap <= cfg["merge_max_gap"]:
            s_seg = compute_s_seg(merged[-1], seg, config=cfg)
            if s_seg > cfg["merge_score"]:
                # Merge: extend the last segment
                merged[-1] = merged[-1] + seg
                continue

        merged.append(seg)

    return merged


# -- Encounter-level species label (Section 2.4) --


def encounter_species_label(photos):
    """Aggregate per-photo species into an encounter-level label.

    Uses confidence-weighted majority vote across all photos' top-5 predictions.

    Args:
        photos: list of photo dicts with 'species_top5' key

    Returns:
        (species_name, confidence) or (None, 0.0) if no predictions
    """
    species_weights = defaultdict(float)
    for p in photos:
        for entry in (p.get("species_top5") or []):
            species_weights[entry[0]] += entry[1]

    if not species_weights:
        return (None, 0.0)

    winner = max(species_weights, key=species_weights.get)
    # Normalize confidence: total weight / (number of photos * max possible per photo)
    n_photos = len([p for p in photos if p.get("species_top5")])
    if n_photos == 0:
        return (winner, 0.0)
    avg_conf = species_weights[winner] / n_photos
    return (winner, round(avg_conf, 4))


# -- Full encounter segmentation pipeline --


def segment_encounters(photos, config=None):
    """Run the full two-pass encounter segmentation pipeline.

    Args:
        photos: list of photo dicts with all required features
        config: optional dict overriding DEFAULTS

    Returns:
        list of encounter dicts, each with:
            - photos: list of photo dicts in this encounter
            - species: (name, confidence) tuple
            - photo_count: int
            - time_range: (first_timestamp, last_timestamp) or (None, None)
    """
    # Pass 1: Cut into microsegments
    microsegments = cut_microsegments(photos, config=config)
    log.info("Pass 1: %d microsegments from %d photos", len(microsegments), len(photos))

    # Pass 2: Merge neighbors
    merged = merge_microsegments(microsegments, config=config)
    log.info("Pass 2: %d encounters after merging", len(merged))

    # Build encounter objects
    encounters = []
    for seg in merged:
        species = encounter_species_label(seg)
        first_ts = _segment_timestamp(seg, "first")
        last_ts = _segment_timestamp(seg, "last")
        encounters.append({
            "photos": seg,
            "species": species,
            "photo_count": len(seg),
            "time_range": (
                first_ts.isoformat() if first_ts else None,
                last_ts.isoformat() if last_ts else None,
            ),
        })

    return encounters
