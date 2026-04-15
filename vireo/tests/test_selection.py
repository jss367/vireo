# vireo/tests/test_selection.py
"""Tests for MMR selection and triage (Stages 5-6)."""
import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _make_photo(photo_id, quality, emb=None, phash=None, species=None, label=None):
    """Helper to build a scored photo dict."""
    return {
        "id": photo_id,
        "quality_composite": quality,
        "dino_subject_embedding": emb,
        "phash_crop": phash,
        "species_top5": species,
        "label": label,
        "mask_path": "/masks/test.png",
    }


# -- diversity_distance --


def test_diversity_identical():
    """Identical photos should have diversity distance ~0."""
    from selection import diversity_distance

    emb = np.ones(768, dtype=np.float32)
    a = _make_photo(1, 0.8, emb=emb, phash="abcdef0123456789")
    score = diversity_distance(a, a)
    assert score < 0.05


def test_diversity_orthogonal():
    """Orthogonal embeddings + different hashes → high diversity."""
    from selection import diversity_distance

    emb_a = np.array([1, 0, 0] * 256, dtype=np.float32)
    emb_b = np.array([0, 1, 0] * 256, dtype=np.float32)
    a = _make_photo(1, 0.8, emb=emb_a, phash="0000000000000000")
    b = _make_photo(2, 0.7, emb=emb_b, phash="ffffffffffffffff")
    score = diversity_distance(a, b)
    assert score > 0.8


def test_diversity_missing_features():
    """Missing features should give neutral diversity (~0.5)."""
    from selection import diversity_distance

    a = _make_photo(1, 0.8)
    b = _make_photo(2, 0.7)
    score = diversity_distance(a, b)
    # With no embeddings (→ 1.0 emb_dist) and no phash (→ 0.5 hash_dist)
    # = 0.6 * 1.0 + 0.4 * 0.5 = 0.8
    assert 0.3 < score < 0.9


def test_diversity_mismatched_embedding_dims_does_not_crash():
    """Photos embedded under different DINOv2 variants must not crash MMR.

    If the DB still holds embeddings of mixed dims (e.g. 768 from vit-b14
    and 1024 from vit-l14) because a variant switch left stale rows,
    diversity_distance should treat them as 'no embedding comparison
    available' and degrade gracefully instead of raising ValueError.
    """
    from selection import diversity_distance

    emb_a = np.ones(768, dtype=np.float32)
    emb_b = np.ones(1024, dtype=np.float32)
    a = _make_photo(1, 0.8, emb=emb_a, phash="0000000000000000")
    b = _make_photo(2, 0.7, emb=emb_b, phash="ffffffffffffffff")
    score = diversity_distance(a, b)
    assert 0.0 <= score <= 1.0


def test_mmr_runs_with_mixed_embedding_dims():
    """MMR over a mixed-dim candidate set should produce selections, not crash."""
    from selection import mmr_select

    emb_768 = np.ones(768, dtype=np.float32)
    emb_1024 = np.ones(1024, dtype=np.float32)
    candidates = [
        _make_photo(1, 0.9, emb=emb_768, phash="0000000000000000"),
        _make_photo(2, 0.7, emb=emb_1024, phash="ffffffffffffffff"),
        _make_photo(3, 0.5, emb=emb_768, phash="aaaaaaaaaaaaaaaa"),
    ]
    selected = mmr_select(candidates, lam=0.70, max_keep=2)
    assert len(selected) == 2


# -- mmr_select --


def test_mmr_selects_highest_quality_first():
    """First MMR pick should be the highest quality candidate."""
    from selection import mmr_select

    emb = np.ones(768, dtype=np.float32)
    candidates = [
        _make_photo(1, 0.9, emb=emb),
        _make_photo(2, 0.5, emb=emb),
        _make_photo(3, 0.3, emb=emb),
    ]
    selected = mmr_select(candidates, lam=0.85, max_keep=1)
    assert len(selected) == 1
    assert selected[0]["id"] == 1


def test_mmr_prefers_diversity():
    """With low lambda, MMR should prefer diverse photos over similar high-quality ones."""
    from selection import mmr_select

    emb_a = np.array([1, 0, 0] * 256, dtype=np.float32)
    emb_b = np.array([0, 1, 0] * 256, dtype=np.float32)
    candidates = [
        _make_photo(1, 0.9, emb=emb_a, phash="0000000000000000"),
        _make_photo(2, 0.85, emb=emb_a, phash="0000000000000001"),  # similar to #1
        _make_photo(3, 0.7, emb=emb_b, phash="ffffffffffffffff"),   # diverse from #1
    ]
    selected = mmr_select(candidates, lam=0.3, max_keep=2)
    selected_ids = {p["id"] for p in selected}
    # Should pick #1 (best quality) and #3 (most diverse), not #2
    assert 1 in selected_ids
    assert 3 in selected_ids


def test_mmr_respects_max_keep():
    """MMR should not return more than max_keep photos."""
    from selection import mmr_select

    emb = np.ones(768, dtype=np.float32)
    candidates = [_make_photo(i, 0.5 + i * 0.1, emb=emb) for i in range(10)]
    selected = mmr_select(candidates, lam=0.85, max_keep=3)
    assert len(selected) == 3


def test_mmr_fewer_than_max():
    """If candidates < max_keep, return all."""
    from selection import mmr_select

    candidates = [_make_photo(1, 0.8), _make_photo(2, 0.7)]
    selected = mmr_select(candidates, lam=0.85, max_keep=5)
    assert len(selected) == 2


def test_mmr_empty():
    from selection import mmr_select

    assert mmr_select([], lam=0.85, max_keep=3) == []


# -- species_rarity_protection --


def test_rarity_protection_promotes_best_reject():
    """If all photos of a species are REJECT, the best gets promoted to REVIEW."""
    from selection import species_rarity_protection

    photos = [
        _make_photo(1, 0.3, species=[("barn_owl", 0.9)], label="REJECT"),
        _make_photo(2, 0.2, species=[("barn_owl", 0.85)], label="REJECT"),
        _make_photo(3, 0.8, species=[("robin", 0.9)], label="KEEP"),
    ]
    protected = species_rarity_protection(photos)
    assert 1 in protected  # best barn_owl
    assert photos[0]["label"] == "REVIEW"
    assert photos[0].get("rarity_protected") is True


def test_rarity_no_protection_when_non_rejected_exists():
    """If a species has at least one non-REJECT photo, no protection needed."""
    from selection import species_rarity_protection

    photos = [
        _make_photo(1, 0.3, species=[("robin", 0.9)], label="REJECT"),
        _make_photo(2, 0.6, species=[("robin", 0.85)], label="KEEP"),
    ]
    protected = species_rarity_protection(photos)
    assert len(protected) == 0


def test_rarity_protection_per_species():
    """Rarity protection applies independently per species."""
    from selection import species_rarity_protection

    photos = [
        _make_photo(1, 0.3, species=[("owl", 0.9)], label="REJECT"),
        _make_photo(2, 0.2, species=[("owl", 0.8)], label="REJECT"),
        _make_photo(3, 0.4, species=[("hawk", 0.9)], label="REJECT"),
    ]
    protected = species_rarity_protection(photos)
    # Both owl and hawk should have their best promoted
    assert 1 in protected  # best owl
    assert 3 in protected  # best (only) hawk


# -- triage_encounters (full pipeline) --


def test_triage_end_to_end():
    """Full triage pipeline: score, select, label."""
    from selection import triage_encounters

    emb_a = np.array([1, 0, 0] * 256, dtype=np.float32)
    emb_b = np.array([0, 1, 0] * 256, dtype=np.float32)

    encounters = [
        {
            "photos": [],  # will be populated in bursts
            "species": ("robin", 0.9),
            "bursts": [
                [
                    # Burst 1: 3 similar photos, one is rejected
                    {
                        "id": 1,
                        "quality_composite": 0.8,
                        "dino_subject_embedding": emb_a,
                        "phash_crop": "0000000000000000",
                        "species_top5": [("robin", 0.9)],
                        "label": None,
                        "mask_path": "/m/1.png",
                    },
                    {
                        "id": 2,
                        "quality_composite": 0.7,
                        "dino_subject_embedding": emb_a,
                        "phash_crop": "0000000000000001",
                        "species_top5": [("robin", 0.85)],
                        "label": None,
                        "mask_path": "/m/2.png",
                    },
                    {
                        "id": 3,
                        "quality_composite": 0.3,
                        "label": "REJECT",
                        "dino_subject_embedding": emb_a,
                        "phash_crop": "0000000000000002",
                        "species_top5": [("robin", 0.8)],
                        "mask_path": None,
                    },
                ],
                [
                    # Burst 2: different pose
                    {
                        "id": 4,
                        "quality_composite": 0.75,
                        "dino_subject_embedding": emb_b,
                        "phash_crop": "ffffffffffffffff",
                        "species_top5": [("robin", 0.9)],
                        "label": None,
                        "mask_path": "/m/4.png",
                    },
                ],
            ],
        },
    ]

    _, all_photos = triage_encounters(encounters)

    labels = {p["id"]: p["label"] for p in all_photos}
    assert labels[3] == "REJECT"  # hard reject stays
    # At least one KEEP
    keep_count = sum(1 for l in labels.values() if l == "KEEP")
    assert keep_count >= 1
    # All photos have a label
    assert all(p["label"] in ("KEEP", "REVIEW", "REJECT") for p in all_photos)


def test_triage_with_rarity_protection():
    """Rarity protection fires when all photos of a species are rejected."""
    from selection import triage_encounters

    encounters = [
        {
            "photos": [],
            "species": ("rare_owl", 0.9),
            "bursts": [
                [
                    {
                        "id": 1,
                        "quality_composite": 0.35,
                        "label": "REJECT",
                        "species_top5": [("rare_owl", 0.9)],
                        "dino_subject_embedding": None,
                        "phash_crop": None,
                        "mask_path": None,
                    },
                    {
                        "id": 2,
                        "quality_composite": 0.25,
                        "label": "REJECT",
                        "species_top5": [("rare_owl", 0.85)],
                        "dino_subject_embedding": None,
                        "phash_crop": None,
                        "mask_path": None,
                    },
                ],
            ],
        },
    ]

    _, all_photos = triage_encounters(encounters)
    # Best rare_owl (id=1) should be promoted to REVIEW
    labels = {p["id"]: p["label"] for p in all_photos}
    assert labels[1] == "REVIEW"
    assert labels[2] == "REJECT"

    # Check rarity_protected flag
    photo_1 = next(p for p in all_photos if p["id"] == 1)
    assert photo_1.get("rarity_protected") is True
