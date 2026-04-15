# vireo/tests/test_encounters.py
"""Tests for encounter segmentation (Stage 2).

Uses synthetic photo dicts with controlled timestamps, embeddings, and
species predictions to verify the segmentation logic.
"""
import math
import os
import sys
from datetime import datetime, timedelta

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _make_photo(
    ts_offset_s=0,
    subj_emb=None,
    global_emb=None,
    species=None,
    lat=None,
    lon=None,
    focal_length=None,
    burst_id=None,
    photo_id=None,
):
    """Helper to build a photo dict with a timestamp offset from a base time."""
    base = datetime(2026, 3, 20, 10, 0, 0)
    return {
        "id": photo_id or ts_offset_s,
        "timestamp": (base + timedelta(seconds=ts_offset_s)).isoformat(),
        "dino_subject_embedding": subj_emb,
        "dino_global_embedding": global_emb,
        "species_top5": species,
        "latitude": lat,
        "longitude": lon,
        "focal_length": focal_length,
        "burst_id": burst_id,
    }


# -- sim_time --


def test_sim_time_zero_gap():
    from encounters import sim_time

    assert sim_time(0.0) == 1.0


def test_sim_time_large_gap():
    from encounters import sim_time

    # 200 seconds with tau=40 → exp(-5) ≈ 0.0067
    score = sim_time(200.0, tau=40.0)
    assert score < 0.01


def test_sim_time_one_tau():
    from encounters import sim_time

    # At exactly tau, score = exp(-1) ≈ 0.368
    score = sim_time(40.0, tau=40.0)
    assert abs(score - math.exp(-1)) < 0.001


# -- sim_embedding --


def test_sim_embedding_identical():
    from encounters import sim_embedding

    emb = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    assert abs(sim_embedding(emb, emb) - 1.0) < 0.001


def test_sim_embedding_orthogonal():
    from encounters import sim_embedding

    a = np.array([1.0, 0.0], dtype=np.float32)
    b = np.array([0.0, 1.0], dtype=np.float32)
    assert sim_embedding(a, b) == 0.0


def test_sim_embedding_none():
    from encounters import sim_embedding

    emb = np.array([1.0, 0.0], dtype=np.float32)
    assert sim_embedding(emb, None) == 0.0
    assert sim_embedding(None, None) == 0.0


def test_sim_embedding_mismatched_dims_returns_zero():
    """Mismatched embedding dims (from a variant switch leaving stale rows)
    must not crash the pipeline. Treat as 'no similarity info' = 0.0."""
    from encounters import sim_embedding

    emb_768 = np.ones(768, dtype=np.float32)
    emb_1024 = np.ones(1024, dtype=np.float32)
    assert sim_embedding(emb_768, emb_1024) == 0.0
    assert sim_embedding(emb_1024, emb_768) == 0.0


def test_segment_encounters_survives_mixed_dims():
    """A run containing both 768-dim and 1024-dim embeddings must segment
    without raising 'shapes not aligned' from np.dot."""
    from encounters import segment_encounters

    emb_768 = np.ones(768, dtype=np.float32)
    emb_1024 = np.ones(1024, dtype=np.float32)
    photos = [
        _make_photo(ts_offset_s=0, subj_emb=emb_768, global_emb=emb_768),
        _make_photo(ts_offset_s=5, subj_emb=emb_1024, global_emb=emb_1024),
        _make_photo(ts_offset_s=10, subj_emb=emb_768, global_emb=emb_768),
    ]
    encounters = segment_encounters(photos)
    assert isinstance(encounters, list)


# -- sim_species --


def test_sim_species_identical():
    from encounters import sim_species

    sp = [("robin", 0.9), ("sparrow", 0.05)]
    # Bhattacharyya: sqrt(0.9*0.9) + sqrt(0.05*0.05) = 0.9 + 0.05 = 0.95
    score = sim_species(sp, sp)
    assert abs(score - 0.95) < 0.01


def test_sim_species_no_overlap():
    from encounters import sim_species

    a = [("robin", 0.9)]
    b = [("eagle", 0.8)]
    assert sim_species(a, b) == 0.0


def test_sim_species_partial_overlap():
    from encounters import sim_species

    a = [("robin", 0.8), ("sparrow", 0.1)]
    b = [("robin", 0.6), ("hawk", 0.3)]
    # Only robin overlaps: sqrt(0.8 * 0.6) ≈ 0.693
    score = sim_species(a, b)
    assert abs(score - math.sqrt(0.8 * 0.6)) < 0.01


def test_sim_species_empty():
    from encounters import sim_species

    assert sim_species([], [("robin", 0.9)]) == 0.0
    assert sim_species(None, None) == 0.0


# -- sim_meta --


def test_sim_meta_with_gps():
    from encounters import sim_meta

    a = {"latitude": 37.7749, "longitude": -122.4194, "focal_length": 400}
    b = {"latitude": 37.7749, "longitude": -122.4194, "focal_length": 400}
    # Same location, same focal length → score near 1.0
    score = sim_meta(a, b)
    assert score > 0.9


def test_sim_meta_without_gps():
    from encounters import sim_meta

    a = {"focal_length": 400}
    b = {"focal_length": 400}
    # No GPS → only focal length: exp(-0/0.15) = 1.0, weight 1.0
    score = sim_meta(a, b)
    assert abs(score - 1.0) < 0.01


def test_sim_meta_different_focal():
    from encounters import sim_meta

    a = {"focal_length": 100}
    b = {"focal_length": 400}
    # |log(100/400)| / 0.15 = |log(0.25)| / 0.15 ≈ 1.386/0.15 ≈ 9.24 → exp(−9.24) ≈ 0
    score = sim_meta(a, b)
    assert score < 0.01


def test_sim_meta_no_metadata():
    from encounters import sim_meta

    assert sim_meta({}, {}) == 0.0


# -- compute_s_enc --


def test_s_enc_identical_photos():
    from encounters import compute_s_enc

    emb = np.ones(768, dtype=np.float32)
    species = [("robin", 0.9)]
    p = _make_photo(ts_offset_s=0, subj_emb=emb, global_emb=emb, species=species)
    score = compute_s_enc(p, p)
    assert score > 0.9


def test_s_enc_distant_photos():
    from encounters import compute_s_enc

    emb_a = np.array([1, 0, 0] * 256, dtype=np.float32)
    emb_b = np.array([0, 1, 0] * 256, dtype=np.float32)
    a = _make_photo(ts_offset_s=0, subj_emb=emb_a, global_emb=emb_a, species=[("robin", 0.9)])
    b = _make_photo(ts_offset_s=300, subj_emb=emb_b, global_emb=emb_b, species=[("eagle", 0.8)])
    score = compute_s_enc(a, b)
    assert score < 0.2


def test_s_enc_renormalize_missing_embeddings():
    """S_enc should renormalize weights when some features are missing."""
    from encounters import compute_s_enc

    # No embeddings, no species — only time and meta contribute
    a = _make_photo(ts_offset_s=0)
    b = _make_photo(ts_offset_s=5)
    score = compute_s_enc(a, b)
    # Should still produce a valid score
    assert 0.0 <= score <= 1.0
    # Close in time → should be reasonably high
    assert score > 0.5


# -- cut_microsegments --


def test_cut_hard_time_gap():
    """Photos separated by >180s should be in different segments."""
    from encounters import cut_microsegments

    emb = np.ones(768, dtype=np.float32)
    photos = [
        _make_photo(0, subj_emb=emb, global_emb=emb),
        _make_photo(5, subj_emb=emb, global_emb=emb),
        _make_photo(200, subj_emb=emb, global_emb=emb),  # >180s gap
        _make_photo(205, subj_emb=emb, global_emb=emb),
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 2
    assert len(segments[0]) == 2
    assert len(segments[1]) == 2


def test_cut_hard_low_score():
    """Very dissimilar adjacent photos should be in different segments."""
    from encounters import cut_microsegments

    emb_a = np.array([1, 0, 0] * 256, dtype=np.float32)
    emb_b = np.array([0, 1, 0] * 256, dtype=np.float32)
    photos = [
        _make_photo(0, subj_emb=emb_a, global_emb=emb_a, species=[("robin", 0.9)]),
        _make_photo(10, subj_emb=emb_b, global_emb=emb_b, species=[("eagle", 0.9)]),
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 2


def test_no_cut_for_similar_photos():
    """Similar photos close in time should stay in the same segment."""
    from encounters import cut_microsegments

    emb = np.ones(768, dtype=np.float32)
    species = [("robin", 0.9)]
    photos = [
        _make_photo(0, subj_emb=emb, global_emb=emb, species=species),
        _make_photo(5, subj_emb=emb, global_emb=emb, species=species),
        _make_photo(10, subj_emb=emb, global_emb=emb, species=species),
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 1
    assert len(segments[0]) == 3


def test_cut_single_photo():
    """A single photo should produce one segment."""
    from encounters import cut_microsegments

    photos = [_make_photo(0)]
    segments = cut_microsegments(photos)
    assert len(segments) == 1


def test_cut_empty():
    """Empty input produces empty output."""
    from encounters import cut_microsegments

    assert cut_microsegments([]) == []


def test_burst_id_prevents_cut():
    """Photos sharing a camera burst ID should not be cut apart."""
    from encounters import cut_microsegments

    emb_a = np.array([1, 0, 0] * 256, dtype=np.float32)
    emb_b = np.array([0, 1, 0] * 256, dtype=np.float32)
    photos = [
        _make_photo(0, subj_emb=emb_a, species=[("robin", 0.9)], burst_id="B001"),
        _make_photo(10, subj_emb=emb_b, species=[("eagle", 0.9)], burst_id="B001"),
    ]
    segments = cut_microsegments(photos)
    # Despite different embeddings/species, shared burst_id keeps them together
    assert len(segments) == 1


# -- merge_microsegments --


def test_merge_similar_segments():
    """Close, similar segments should be merged."""
    from encounters import merge_microsegments

    emb = np.ones(768, dtype=np.float32)
    species = [("robin", 0.9)]
    seg_a = [
        _make_photo(0, subj_emb=emb, global_emb=emb, species=species),
        _make_photo(5, subj_emb=emb, global_emb=emb, species=species),
    ]
    seg_b = [
        _make_photo(30, subj_emb=emb, global_emb=emb, species=species),
        _make_photo(35, subj_emb=emb, global_emb=emb, species=species),
    ]
    merged = merge_microsegments([seg_a, seg_b])
    assert len(merged) == 1
    assert len(merged[0]) == 4


def test_no_merge_large_gap():
    """Segments separated by >60s should not be merged."""
    from encounters import merge_microsegments

    emb = np.ones(768, dtype=np.float32)
    species = [("robin", 0.9)]
    seg_a = [_make_photo(0, subj_emb=emb, global_emb=emb, species=species)]
    seg_b = [_make_photo(120, subj_emb=emb, global_emb=emb, species=species)]  # 120s gap
    merged = merge_microsegments([seg_a, seg_b])
    assert len(merged) == 2


def test_no_merge_dissimilar():
    """Dissimilar segments within gap should not be merged."""
    from encounters import merge_microsegments

    emb_a = np.array([1, 0, 0] * 256, dtype=np.float32)
    emb_b = np.array([0, 1, 0] * 256, dtype=np.float32)
    seg_a = [_make_photo(0, subj_emb=emb_a, global_emb=emb_a, species=[("robin", 0.9)])]
    seg_b = [_make_photo(30, subj_emb=emb_b, global_emb=emb_b, species=[("eagle", 0.9)])]
    merged = merge_microsegments([seg_a, seg_b])
    assert len(merged) == 2


def test_merge_single_segment():
    from encounters import merge_microsegments

    seg = [_make_photo(0)]
    assert merge_microsegments([seg]) == [seg]


# -- encounter_species_label --


def test_encounter_species_label_majority():
    from encounters import encounter_species_label

    photos = [
        _make_photo(0, species=[("robin", 0.9), ("sparrow", 0.05)]),
        _make_photo(5, species=[("robin", 0.8), ("hawk", 0.1)]),
        _make_photo(10, species=[("robin", 0.7)]),
    ]
    name, conf = encounter_species_label(photos)
    assert name == "robin"
    assert conf > 0.5


def test_encounter_species_label_tie():
    """When species tie on total weight, one should still be returned."""
    from encounters import encounter_species_label

    photos = [
        _make_photo(0, species=[("robin", 0.5)]),
        _make_photo(5, species=[("eagle", 0.5)]),
    ]
    name, conf = encounter_species_label(photos)
    assert name in ("robin", "eagle")


def test_encounter_species_label_empty():
    from encounters import encounter_species_label

    name, conf = encounter_species_label([_make_photo(0)])
    assert name is None
    assert conf == 0.0


# -- segment_encounters (full pipeline) --


def test_segment_encounters_end_to_end():
    """Full pipeline: three photo groups → three encounters."""
    from encounters import segment_encounters

    emb_a = np.array([1, 0, 0] * 256, dtype=np.float32)
    emb_b = np.array([0, 1, 0] * 256, dtype=np.float32)
    emb_c = np.array([0, 0, 1] * 256, dtype=np.float32)

    photos = [
        # Encounter 1: robin, t=0-10
        _make_photo(0, subj_emb=emb_a, global_emb=emb_a, species=[("robin", 0.9)]),
        _make_photo(5, subj_emb=emb_a, global_emb=emb_a, species=[("robin", 0.85)]),
        _make_photo(10, subj_emb=emb_a, global_emb=emb_a, species=[("robin", 0.8)]),
        # Encounter 2: eagle, t=300+ (hard time cut)
        _make_photo(300, subj_emb=emb_b, global_emb=emb_b, species=[("eagle", 0.9)]),
        _make_photo(305, subj_emb=emb_b, global_emb=emb_b, species=[("eagle", 0.85)]),
        # Encounter 3: hawk, t=600+ (hard time cut)
        _make_photo(600, subj_emb=emb_c, global_emb=emb_c, species=[("hawk", 0.9)]),
    ]

    encounters = segment_encounters(photos)
    assert len(encounters) == 3
    assert encounters[0]["photo_count"] == 3
    assert encounters[0]["species"][0] == "robin"
    assert encounters[1]["photo_count"] == 2
    assert encounters[1]["species"][0] == "eagle"
    assert encounters[2]["photo_count"] == 1
    assert encounters[2]["species"][0] == "hawk"


def test_segment_encounters_with_merge():
    """Two close segments of the same subject should merge into one encounter."""
    from encounters import segment_encounters

    emb = np.ones(768, dtype=np.float32)
    species = [("robin", 0.9)]

    # Force a hard cut by inserting a tiny gap with a slightly different embedding
    # that's just low enough to trigger hard_cut_score, then test that merge reconnects
    photos = [
        _make_photo(0, subj_emb=emb, global_emb=emb, species=species),
        _make_photo(5, subj_emb=emb, global_emb=emb, species=species),
        # 25s gap — within merge range, same subject
        _make_photo(30, subj_emb=emb, global_emb=emb, species=species),
        _make_photo(35, subj_emb=emb, global_emb=emb, species=species),
    ]

    encounters = segment_encounters(photos)
    # Should be 1 encounter (identical embeddings, close in time)
    assert len(encounters) == 1
    assert encounters[0]["photo_count"] == 4
