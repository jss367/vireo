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


def _has_similarity_signal(photo):
    """True if a photo carries any signal the score cut can act on.

    Used to decide whether a pair of timestamp-less photos can be judged by
    similarity (screenshots/exports with embeddings) or is genuinely
    signal-less (unreadable files — no detection ran, so no embeddings or
    species) and must instead be grouped by file order.

    A detector verdict counts too: subject_absent/subject_present means
    detection ran. compute_s_enc turns an absent-vs-present pair into an
    active dissimilarity (score cut), so we must NOT force-group such a pair
    by file order.

    Metadata counts too: sim_meta always contributes to compute_s_enc, so a
    usable focal length (> 0) or a GPS fix (both latitude and longitude) lets
    the score cut distinguish two timestamp-less photos (e.g. imported images
    with stripped capture dates but retained lens/GPS data). Force-grouping
    those by file order would discard the signal sim_meta would have produced.

    Only rows where detection genuinely never ran (no embeddings, no species,
    no detector verdict) AND that carry no usable metadata (no focal length,
    no GPS) are signal-less.
    """
    fl = photo.get("focal_length")
    has_focal = fl is not None and fl > 0
    has_gps = photo.get("latitude") is not None and photo.get("longitude") is not None
    return (
        photo.get("dino_subject_embedding") is not None
        or photo.get("dino_global_embedding") is not None
        or bool(photo.get("species_top5"))
        or bool(photo.get("subject_absent"))
        or bool(photo.get("subject_present"))
        or has_focal
        or has_gps
    )


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


def compute_s_enc(photo_a, photo_b, config=None, return_components=False):
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
        return_components: when True, return (score, components_dict) where
            components_dict maps each signal name to {value, weight, used}.

    Returns:
        float — similarity score (higher = more likely same encounter), or
        (float, dict) when return_components is True.
    """
    cfg = {**DEFAULTS, **(config or {})}

    ts_a = _parse_timestamp(photo_a.get("timestamp"))
    ts_b = _parse_timestamp(photo_b.get("timestamp"))
    dt = _time_delta_seconds(ts_a, ts_b)

    # Detector-state encoding from load_photo_features (mutually exclusive,
    # so at most one is True per photo):
    #   subject_absent  = detector ran, no qualifying detection (real signal)
    #   subject_present = detector ran, has a qualifying detection
    #   both False      = detector hasn't run yet — STATE IS UNKNOWN
    #
    # Three cases drive the subj/species treatment:
    #   - asymmetric (one absent, one *present*): contribute active 0 with
    #     full weight. The detector's "no subject" verdict on one side
    #     against affirmative evidence on the other is real dissimilarity.
    #     Without this, meta=1.0 (same lens) renormalizes the score back
    #     up purely on time.
    #   - both absent: NEUTRAL — drop the signal. Holds even when stale
    #     embeddings/species are still cached on the photo rows from a
    #     prior run. Reading them would re-activate similarity and
    #     contradict the detector's verdict.
    #   - any other case (absent vs unknown, present vs unknown, both
    #     unknown, both present): fall through to standard cached-feature
    #     similarity. Notably, "absent vs unknown" must NOT trigger the
    #     asymmetric penalty — we have no evidence those photos differ.
    absent_a = bool(photo_a.get("subject_absent"))
    absent_b = bool(photo_b.get("subject_absent"))
    present_a = bool(photo_a.get("subject_present"))
    present_b = bool(photo_b.get("subject_present"))
    asymmetric_subject = (absent_a and present_b) or (absent_b and present_a)
    both_absent = absent_a and absent_b

    st = sim_time(dt, tau=cfg["tau_enc"])
    if asymmetric_subject or both_absent:
        ss = 0.0
        sp = 0.0
    else:
        ss = sim_embedding(photo_a.get("dino_subject_embedding"), photo_b.get("dino_subject_embedding"))
        sp = sim_species(photo_a.get("species_top5"), photo_b.get("species_top5"))
    sg = sim_embedding(photo_a.get("dino_global_embedding"), photo_b.get("dino_global_embedding"))
    sm = sim_meta(photo_a, photo_b)

    # Per-component missing flags: which photo (if any) lacks the signal.
    # Used by the trace UI to surface "compute embeddings on photo B" hints
    # without conflating a missing signal with an explicitly-zeroed weight.
    has_subj_a = photo_a.get("dino_subject_embedding") is not None
    has_subj_b = photo_b.get("dino_subject_embedding") is not None
    has_global_a = photo_a.get("dino_global_embedding") is not None
    has_global_b = photo_b.get("dino_global_embedding") is not None
    has_species_a = bool(photo_a.get("species_top5"))
    has_species_b = bool(photo_b.get("species_top5"))
    has_time_a = ts_a is not None
    has_time_b = ts_b is not None

    # `missing` = "we don't know yet"; `absent` = "we ran the detector and
    # there was no animal." Trace consumers render these differently:
    # missing prompts "embed this photo", absent is itself the reason
    # the encounter cut.
    missing = {
        "time": (not has_time_a, not has_time_b),
        "subj": (not has_subj_a and not absent_a, not has_subj_b and not absent_b),
        "global": (not has_global_a, not has_global_b),
        "species": (not has_species_a and not absent_a, not has_species_b and not absent_b),
        "meta": (False, False),
    }
    absent = {
        "time": (False, False),
        "subj": (absent_a, absent_b),
        "global": (False, False),
        "species": (absent_a, absent_b),
        "meta": (False, False),
    }

    used = {
        "time": dt != float("inf"),
        # Asymmetric subject_absent ⇒ subj/species are USED (active 0).
        # Both-sides absent drops the signal — two subjectless frames carry
        # no evidence either way (and this overrides any stale cached
        # embeddings/species; see the absent-handling block above).
        "subj": asymmetric_subject or (
            has_subj_a and has_subj_b and not both_absent
        ),
        "global": has_global_a and has_global_b,
        "species": asymmetric_subject or (
            has_species_a and has_species_b and not both_absent
        ),
        # Meta always contributes (even if 0)
        "meta": True,
    }
    weight_keys = {
        "time": "w_time",
        "subj": "w_subj",
        "global": "w_global",
        "species": "w_species",
        "meta": "w_meta",
    }
    values = {"time": st, "subj": ss, "global": sg, "species": sp, "meta": sm}

    total_weight = sum(cfg[weight_keys[k]] for k, u in used.items() if u)
    if total_weight == 0:
        s_enc = 0.0
    else:
        s_enc = sum(cfg[weight_keys[k]] * values[k] for k, u in used.items() if u) / total_weight

    if not return_components:
        return s_enc

    components = {
        k: {
            "value": float(values[k]),
            "weight": float(cfg[weight_keys[k]]),
            "used": bool(used[k]),
            "missing_a": bool(missing[k][0]),
            "missing_b": bool(missing[k][1]),
            "absent_a": bool(absent[k][0]),
            "absent_b": bool(absent[k][1]),
        }
        for k in values
    }
    return s_enc, components


# -- Pass 1: Cut timeline into microsegments (Section 2.2) --


def cut_microsegments(photos, config=None, emit_trace=False):
    """Sort photos by timestamp and cut into microsegments.

    Args:
        photos: list of photo dicts (see compute_s_enc for required keys)
        config: optional dict overriding DEFAULTS
        emit_trace: when True, return (segments, trace) where each trace
            entry is {pair_index, score, dt_seconds, decision, components,
            thresholds}. Decisions: kept, cut_time, cut_score, cut_soft,
            burst_id_kept.

    Returns:
        list of lists (each inner list is a microsegment of photo dicts), or
        (segments, trace) when emit_trace is True.
    """
    cfg = {**DEFAULTS, **(config or {})}

    if len(photos) <= 1:
        if emit_trace:
            return ([photos] if photos else []), []
        return [photos] if photos else []

    # Sort by timestamp. Null-timestamp photos (scan I/O errors, unreadable
    # files) sort AFTER all timestamped photos — never datetime.min, which
    # would pile them at the top of the review timeline and read as "the whole
    # pipeline failed to group." Within the null group, order by
    # (folder_id, filename) so consecutive frames from one shoot stay adjacent
    # even without EXIF, letting the both-null branch below keep them together.
    sorted_photos = sorted(
        photos,
        key=lambda p: (
            (0, _parse_timestamp(p.get("timestamp")), 0, "")
            if _parse_timestamp(p.get("timestamp")) is not None
            else (1, datetime.min, p.get("folder_id") or 0, p.get("filename") or "")
        ),
    )

    cuts = set()
    recent_scores = []  # sliding window for soft cut detection
    trace = [] if emit_trace else None

    for i in range(len(sorted_photos) - 1):
        if emit_trace:
            score, components = compute_s_enc(
                sorted_photos[i], sorted_photos[i + 1], config=cfg, return_components=True
            )
        else:
            score = compute_s_enc(sorted_photos[i], sorted_photos[i + 1], config=cfg)
            components = None

        ts_a = _parse_timestamp(sorted_photos[i].get("timestamp"))
        ts_b = _parse_timestamp(sorted_photos[i + 1].get("timestamp"))
        dt = _time_delta_seconds(ts_a, ts_b)

        bid_a = sorted_photos[i].get("burst_id")
        bid_b = sorted_photos[i + 1].get("burst_id")
        decision = None

        both_null = ts_a is None and ts_b is None
        folder_a = sorted_photos[i].get("folder_id")
        folder_b = sorted_photos[i + 1].get("folder_id")
        # "Signal-less" = no timestamp AND no usable similarity signal on
        # either side (no embeddings, no species). Unreadable files are
        # signal-less; undated-but-readable photos (e.g. screenshots, which
        # still get embeddings) are NOT — they must be judged on the score
        # cut, not force-grouped by file order.
        #
        # Also require the same folder. The null sort key
        # (1, datetime.min, folder_id, filename) places the last null of
        # one folder adjacent to the first null of the next, so without
        # this guard the both-null branch would fuse unrelated unreadable
        # files from separate shoots into a single encounter. Photos
        # missing folder_id (shouldn't happen in production scans) fall
        # through to the score cut to stay on the safe side.
        signal_less = (
            both_null
            and folder_a is not None
            and folder_a == folder_b
            and not (
                _has_similarity_signal(sorted_photos[i])
                or _has_similarity_signal(sorted_photos[i + 1])
            )
        )

        if bid_a is not None and bid_b is not None and bid_a == bid_b:
            decision = "burst_id_kept"
            recent_scores.append(score)
            if len(recent_scores) > 3:
                recent_scores.pop(0)
        elif signal_less:
            # No time signal and no embeddings/species — every similarity
            # score is ~0, so the score cut would split the whole null run
            # into singletons. With no reliable basis to separate them, keep
            # contiguous nulls together by file order.
            decision = "kept_no_timestamp"
            recent_scores = []
        elif not both_null and dt > cfg["hard_cut_time"]:
            # Time cut only applies when at least one side has a timestamp.
            # Asymmetric pairs (one null, one real) give dt=inf and cut here,
            # so the null cluster never absorbs a real photo. Both-null pairs
            # that reached this point DO have a similarity signal and fall
            # through to the score cut below.
            cuts.add(i)
            recent_scores = []
            decision = "cut_time"
        elif score < cfg["hard_cut_score"]:
            cuts.add(i)
            recent_scores = []
            decision = "cut_score"
        else:
            recent_scores.append(score)
            if len(recent_scores) > 3:
                recent_scores.pop(0)
            if len(recent_scores) >= 3:
                below = sum(1 for s in recent_scores if s < cfg["soft_cut_score"])
                if below >= 2:
                    cuts.add(i)
                    recent_scores = []
                    decision = "cut_soft"
            if decision is None:
                decision = "kept"

        if emit_trace:
            trace.append({
                "pair_index": i,
                "photo_a_id": sorted_photos[i].get("id"),
                "photo_b_id": sorted_photos[i + 1].get("id"),
                "photo_a_filename": sorted_photos[i].get("filename"),
                "photo_b_filename": sorted_photos[i + 1].get("filename"),
                "score": float(score),
                "dt_seconds": float(dt) if dt != float("inf") else None,
                "decision": decision,
                "components": components,
                "thresholds": {
                    "hard_cut_time": cfg["hard_cut_time"],
                    "hard_cut_score": cfg["hard_cut_score"],
                    "soft_cut_score": cfg["soft_cut_score"],
                },
            })

    # Build segments from cut points
    segments = []
    start = 0
    for i in sorted(cuts):
        segments.append(sorted_photos[start: i + 1])
        start = i + 1
    segments.append(sorted_photos[start:])
    segments = [seg for seg in segments if seg]

    if emit_trace:
        return segments, trace
    return segments


# -- Pass 2: Merge neighboring microsegments (Section 2.3) --


def _segment_mean_embedding(segment, key):
    """Compute mean embedding for a segment.

    When a segment straddles a DINOv2 variant switch (stale rows left behind
    at a different dim), np.mean over a ragged list raises. Take the mean
    over the majority shape only — matches the dominant variant in the
    segment and ignores the minority outliers for merge scoring.
    """
    embeddings = [p[key] for p in segment if p.get(key) is not None]
    if not embeddings:
        return None
    shape_counts = defaultdict(int)
    for e in embeddings:
        shape_counts[e.shape] += 1
    majority_shape = max(shape_counts, key=shape_counts.get)
    matching = [e for e in embeddings if e.shape == majority_shape]
    return np.mean(matching, axis=0)


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
    merged, _ = _merge_microsegments_with_map(segments, config=config)
    return merged


def _merge_microsegments_with_map(segments, config=None):
    """Like merge_microsegments but also returns a per-merged-segment count
    of how many original microsegments were fused into it.

    Returns:
        (merged_segments, counts) where counts[i] is the number of original
        microsegments that ended up inside merged_segments[i].
    """
    cfg = {**DEFAULTS, **(config or {})}

    if len(segments) <= 1:
        return list(segments), [1] * len(segments)

    merged = [segments[0]]
    counts = [1]
    for seg in segments[1:]:
        last_a = _segment_timestamp(merged[-1], "last")
        first_b = _segment_timestamp(seg, "first")
        gap = _time_delta_seconds(last_a, first_b)

        did_merge = False
        if gap <= cfg["merge_max_gap"]:
            s_seg = compute_s_seg(merged[-1], seg, config=cfg)
            if s_seg > cfg["merge_score"]:
                # Merge: extend the last segment
                merged[-1] = merged[-1] + seg
                counts[-1] += 1
                did_merge = True

        if not did_merge:
            merged.append(seg)
            counts.append(1)

    return merged, counts


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


def segment_encounters(photos, config=None, emit_trace=False):
    """Run the full two-pass encounter segmentation pipeline.

    Args:
        photos: list of photo dicts with all required features
        config: optional dict overriding DEFAULTS
        emit_trace: when True, attach a per-encounter `trace` field — a list
            of cut-point trace entries that fall inside that encounter (after
            potential microsegment fusion). Boundary entries between two
            microsegments that ended up in the same merged encounter are
            re-tagged with decision="merged_back".

    Returns:
        list of encounter dicts, each with:
            - photos: list of photo dicts in this encounter
            - species: (name, confidence) tuple
            - photo_count: int
            - time_range: (first_timestamp, last_timestamp) or (None, None)
            - trace: (only when emit_trace) list of trace dicts for the
              internal adjacent pairs of this encounter
    """
    # Pass 1: Cut into microsegments
    if emit_trace:
        microsegments, full_trace = cut_microsegments(
            photos, config=config, emit_trace=True
        )
    else:
        microsegments = cut_microsegments(photos, config=config)
        full_trace = None
    log.info("Pass 1: %d microsegments from %d photos", len(microsegments), len(photos))

    # Pass 2: Merge neighbors
    if emit_trace:
        merged, merge_counts = _merge_microsegments_with_map(
            microsegments, config=config
        )
    else:
        merged = merge_microsegments(microsegments, config=config)
        merge_counts = None
    log.info("Pass 2: %d encounters after merging", len(merged))

    # Build encounter objects
    encounters = []
    pair_cursor = 0  # index into full_trace
    micro_cursor = 0  # index into microsegments

    for enc_idx, seg in enumerate(merged):
        species = encounter_species_label(seg)
        first_ts = _segment_timestamp(seg, "first")
        last_ts = _segment_timestamp(seg, "last")
        enc = {
            "photos": seg,
            "species": species,
            "photo_count": len(seg),
            "time_range": (
                first_ts.isoformat() if first_ts else None,
                last_ts.isoformat() if last_ts else None,
            ),
        }
        if emit_trace:
            n_micros = merge_counts[enc_idx]
            enc_trace = []
            for k in range(n_micros):
                m = microsegments[micro_cursor]
                # Internal pairs of this microsegment (one fewer than its photo count)
                for _ in range(len(m) - 1):
                    enc_trace.append(full_trace[pair_cursor])
                    pair_cursor += 1
                micro_cursor += 1
                # Boundary pair after this microsegment
                if pair_cursor < len(full_trace):
                    if k < n_micros - 1:
                        # This boundary lives inside the same merged encounter:
                        # re-tag as merged_back (post-merge truth).
                        boundary = dict(full_trace[pair_cursor])
                        boundary["decision"] = "merged_back"
                        enc_trace.append(boundary)
                        pair_cursor += 1
                    else:
                        # Boundary belongs to the gap between encounters; skip.
                        pair_cursor += 1
            enc["trace"] = enc_trace
        encounters.append(enc)

    return encounters
