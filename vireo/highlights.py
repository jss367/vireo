"""Highlights selection — picks the best, most diverse photos from a folder.

Reuses MMR selection logic from vireo.selection for quality+diversity ranking,
with an added per-species cap to ensure variety across species.
"""

from collections import defaultdict

import numpy as np

from vireo.selection import diversity_distance


def select_highlights(candidates, count, max_per_species):
    """Select highlight photos using MMR with per-species caps.

    Args:
        candidates: list of photo dicts with 'quality_score', 'species',
                    'dino_subject_embedding', 'phash_crop'
        count: target number of highlights
        max_per_species: maximum photos per species (None grouped as 'Unidentified')

    Returns:
        list of selected photo dicts, ordered by selection order (best first)
    """
    if not candidates or count <= 0:
        return []

    # Deserialize DINO embeddings from bytes to numpy arrays for cosine sim
    for p in candidates:
        emb = p.get("dino_subject_embedding")
        if isinstance(emb, (bytes, memoryview)):
            p["dino_subject_embedding"] = np.frombuffer(emb, dtype=np.float32).copy()

    # Track species counts
    species_counts = defaultdict(int)
    lam = 0.70  # quality-diversity trade-off (same as encounter-level MMR)

    selected = []
    remaining = sorted(candidates, key=lambda p: p.get("quality_score", 0), reverse=True)

    while len(selected) < count and remaining:
        best_score = -1
        best_idx = -1

        for idx, cand in enumerate(remaining):
            sp = cand.get("species") or "Unidentified"
            if species_counts[sp] >= max_per_species:
                continue

            q = cand.get("quality_score", 0)
            if not selected:
                mmr_score = q
            else:
                min_div = min(
                    diversity_distance(cand, sel) for sel in selected
                )
                mmr_score = lam * q + (1 - lam) * min_div

            if mmr_score > best_score:
                best_score = mmr_score
                best_idx = idx

        if best_idx < 0:
            break  # All remaining photos are species-capped

        pick = remaining.pop(best_idx)
        sp = pick.get("species") or "Unidentified"
        species_counts[sp] += 1
        selected.append(pick)

    return selected
