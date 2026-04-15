"""MMR selection and triage for the culling pipeline (Stages 5-6).

Stage 5: Maximal Marginal Relevance selection at burst and encounter level.
Stage 6: Triage into KEEP / REVIEW / REJECT with species rarity protection.

All weights and thresholds are configurable with defaults from the design doc.
"""

import logging
from collections import defaultdict

import numpy as np

log = logging.getLogger(__name__)

DEFAULTS = {
    # MMR parameters (Section 5.2)
    "burst_lambda": 0.85,
    "burst_max_keep": 3,
    "encounter_lambda": 0.70,
    "encounter_max_keep": 5,
    # Diversity distance weights (Section 5.1)
    "div_w_embedding": 0.60,
    "div_w_phash": 0.40,
}


# -- Diversity distance (Section 5.1) --


_warned_dim_mismatch = False


def _cosine_sim(a, b):
    """Cosine similarity, 0 if either is None or dims don't match."""
    if a is None or b is None:
        return 0.0
    if a.shape != b.shape:
        # Stale DINOv2 embeddings from a previous variant can coexist with
        # current-variant embeddings in the DB; pipeline.load_photo_features
        # filters them when pipeline.dinov2_variant is configured, but
        # highlights and other call paths can still receive mixed dims.
        # Degrade to "no similarity signal" rather than crashing MMR.
        global _warned_dim_mismatch
        if not _warned_dim_mismatch:
            log.warning(
                "Embedding dim mismatch (%s vs %s) — stale DINOv2 embeddings "
                "present; re-embed affected photos to restore MMR diversity",
                a.shape, b.shape,
            )
            _warned_dim_mismatch = True
        return 0.0
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return max(0.0, float(np.dot(a, b) / (norm_a * norm_b)))


def _phash_similarity(hash_a, hash_b):
    """pHash similarity in [0, 1]. 1.0 = identical, 0.0 = maximally different."""
    if hash_a is None or hash_b is None:
        return 0.5  # unknown → neutral
    try:
        int_a = int(hash_a, 16)
        int_b = int(hash_b, 16)
        hamming = bin(int_a ^ int_b).count("1")
        # 64-bit hash → max hamming is 64
        return 1.0 - (hamming / 64.0)
    except (ValueError, TypeError):
        return 0.5


def diversity_distance(photo_i, photo_j, config=None):
    """Compute diversity distance D_div(i, j) (Section 5.1).

    D_div = w_emb * (1 - cosine(s_i, s_j)) + w_phash * (1 - sim_hash(i, j))

    Args:
        photo_i, photo_j: photo dicts with 'dino_subject_embedding' and 'phash_crop'
        config: optional dict overriding DEFAULTS

    Returns:
        float in [0, 1] — higher = more diverse
    """
    cfg = {**DEFAULTS, **(config or {})}

    emb_dist = 1.0 - _cosine_sim(
        photo_i.get("dino_subject_embedding"),
        photo_j.get("dino_subject_embedding"),
    )
    hash_dist = 1.0 - _phash_similarity(
        photo_i.get("phash_crop"),
        photo_j.get("phash_crop"),
    )

    return cfg["div_w_embedding"] * emb_dist + cfg["div_w_phash"] * hash_dist


# -- MMR selection (Section 5.2) --


def mmr_select(candidates, lam, max_keep, config=None):
    """Select photos using Maximal Marginal Relevance.

    score_add(i | K) = λ * Q_i + (1 - λ) * min_{j in K} D_div(i, j)

    Args:
        candidates: list of photo dicts (must have 'quality_composite' set)
        lam: quality-diversity trade-off (higher = more quality-focused)
        max_keep: maximum number of photos to select
        config: optional dict for diversity_distance

    Returns:
        list of selected photo dicts
    """
    if not candidates:
        return []
    if len(candidates) <= max_keep:
        return list(candidates)

    selected = []
    remaining = list(candidates)

    # First pick: highest quality
    remaining.sort(key=lambda p: p.get("quality_composite", 0), reverse=True)
    selected.append(remaining.pop(0))

    while len(selected) < max_keep and remaining:
        best_score = -1
        best_idx = 0

        for idx, cand in enumerate(remaining):
            q = cand.get("quality_composite", 0)
            # Min diversity distance to any already-selected photo
            min_div = min(
                diversity_distance(cand, sel, config=config) for sel in selected
            )
            mmr_score = lam * q + (1 - lam) * min_div
            if mmr_score > best_score:
                best_score = mmr_score
                best_idx = idx

        selected.append(remaining.pop(best_idx))

    return selected


# -- Triage pipeline (Stage 6) --


def _get_species(photo):
    """Get the top species for a photo, or None."""
    top5 = photo.get("species_top5")
    if top5 and len(top5) > 0:
        return top5[0][0]
    return None


def species_rarity_protection(photos):
    """Apply species rarity protection (Section 6.1).

    If every photo of a species is REJECT, promote the best one to REVIEW.

    Args:
        photos: list of all photo dicts (across all encounters) with
                'label' and 'quality_composite' already set.

    Returns:
        set of photo IDs that were rarity-protected
    """
    # Group by species
    by_species = defaultdict(list)
    for p in photos:
        sp = _get_species(p)
        if sp:
            by_species[sp].append(p)

    protected_ids = set()
    for species, species_photos in by_species.items():
        non_rejected = [p for p in species_photos if p.get("label") != "REJECT"]
        if len(non_rejected) == 0 and species_photos:
            # All rejected — promote best to REVIEW
            best = max(species_photos, key=lambda p: p.get("quality_composite", 0))
            best["label"] = "REVIEW"
            best["rarity_protected"] = True
            protected_ids.add(best.get("id"))
            log.info(
                "Rarity protection: %s — promoted photo %s to REVIEW",
                species,
                best.get("id"),
            )

    return protected_ids


def triage_encounters(encounters, config=None):
    """Run the full triage pipeline on scored encounters (Stages 5-6).

    For each encounter:
      1. Hard rejects already labeled by score_encounter()
      2. MMR select within each burst (non-rejected candidates)
      3. MMR select across encounter survivors
      4. Label: KEEP (MMR-selected), REJECT (hard reject), REVIEW (everything else)
    Then apply species rarity protection across all encounters.

    Args:
        encounters: list of encounter dicts, each with 'bursts' (from
                    segment_bursts_for_encounters) and photos scored by
                    score_encounter()
        config: optional dict overriding DEFAULTS

    Returns:
        encounters list (modified in place), plus flat list of all photos
    """
    cfg = {**DEFAULTS, **(config or {})}
    all_photos = []

    for enc in encounters:
        burst_survivors = []

        for burst in enc.get("bursts", [enc]):
            # Get the photos list — burst might be a list directly or a dict
            if isinstance(burst, dict):
                burst_photos = burst.get("photos", [])
            else:
                burst_photos = burst  # burst is already a list of photo dicts

            # Non-rejected candidates
            candidates = [p for p in burst_photos if p.get("label") != "REJECT"]

            # MMR select within burst
            keeps = mmr_select(
                candidates,
                lam=cfg["burst_lambda"],
                max_keep=cfg["burst_max_keep"],
                config=cfg,
            )
            keep_ids = {id(p) for p in keeps}

            burst_survivors.extend(keeps)

        # MMR select across encounter survivors
        enc_keeps = mmr_select(
            burst_survivors,
            lam=cfg["encounter_lambda"],
            max_keep=cfg["encounter_max_keep"],
            config=cfg,
        )
        enc_keep_ids = {id(p) for p in enc_keeps}

        # Label all photos in this encounter
        for burst in enc.get("bursts", [enc]):
            if isinstance(burst, dict):
                burst_photos = burst.get("photos", [])
            else:
                burst_photos = burst

            for photo in burst_photos:
                if photo.get("label") == "REJECT":
                    pass  # already labeled
                elif id(photo) in enc_keep_ids:
                    photo["label"] = "KEEP"
                else:
                    photo["label"] = "REVIEW"
                all_photos.append(photo)

    # Species rarity protection across all encounters
    protected = species_rarity_protection(all_photos)

    # Summary stats
    n_keep = sum(1 for p in all_photos if p.get("label") == "KEEP")
    n_review = sum(1 for p in all_photos if p.get("label") == "REVIEW")
    n_reject = sum(1 for p in all_photos if p.get("label") == "REJECT")
    log.info(
        "Triage: %d KEEP, %d REVIEW, %d REJECT (%d rarity-protected)",
        n_keep,
        n_review,
        n_reject,
        len(protected),
    )

    return encounters, all_photos
