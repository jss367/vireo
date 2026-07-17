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


def test_segment_encounters_mixed_dims_in_long_run():
    """Regression: when time/species keep compute_s_enc above the hard-cut
    threshold, mixed-dim photos stay in the same microsegment and later
    _segment_mean_embedding (np.mean over a ragged list) must not crash."""
    from encounters import segment_encounters

    emb_768 = np.ones(768, dtype=np.float32)
    emb_1024 = np.ones(1024, dtype=np.float32)
    # Tight timestamps + shared species keep S_enc high enough to prevent
    # a hard cut, so all 5 photos land in one microsegment with mixed dims.
    species = [("bird", 0.9)]
    photos = [
        _make_photo(ts_offset_s=0, subj_emb=emb_768, global_emb=emb_768, species=species),
        _make_photo(ts_offset_s=1, subj_emb=emb_1024, global_emb=emb_1024, species=species),
        _make_photo(ts_offset_s=2, subj_emb=emb_768, global_emb=emb_768, species=species),
        _make_photo(ts_offset_s=3, subj_emb=emb_1024, global_emb=emb_1024, species=species),
        _make_photo(ts_offset_s=4, subj_emb=emb_768, global_emb=emb_768, species=species),
    ]
    encounters = segment_encounters(photos)
    assert isinstance(encounters, list)


def test_segment_mean_embedding_mixed_dims():
    """_segment_mean_embedding must not raise when photos in a segment hold
    embeddings of different dims; mean over the majority dim is acceptable."""
    from encounters import _segment_mean_embedding

    emb_768 = np.ones(768, dtype=np.float32)
    emb_1024 = np.ones(1024, dtype=np.float32)
    segment = [
        {"dino_subject_embedding": emb_768},
        {"dino_subject_embedding": emb_1024},
        {"dino_subject_embedding": emb_768},
    ]
    mean = _segment_mean_embedding(segment, "dino_subject_embedding")
    assert mean is not None
    assert mean.shape in ((768,), (1024,))


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


def _strong_species(name):
    return [
        (name, 0.94, "classifier-a"),
        (name, 0.90, "classifier-b"),
    ]


def test_confident_species_prediction_requires_model_consensus():
    from encounters import _confident_species_prediction

    photo = _make_photo(species=[
        ("Verdin", 0.96, "classifier-a"),
        ("Costa's Hummingbird", 0.95, "classifier-b"),
    ])

    assert _confident_species_prediction(photo) is None


def test_confident_species_prediction_requires_decisive_margin():
    from encounters import _confident_species_prediction

    photo = _make_photo(species=[
        ("Verdin", 0.90, "classifier-a"),
        ("Costa's Hummingbird", 0.40, "classifier-a"),
    ])

    assert _confident_species_prediction(photo) is None


def test_confident_species_prediction_ignores_stale_absent_subject():
    from encounters import _confident_species_prediction

    photo = _make_photo(species=_strong_species("Verdin"))
    photo["subject_absent"] = True

    assert _confident_species_prediction(photo) is None


def test_confident_species_change_is_hard_encounter_boundary():
    """Strong classifier disagreement wins over time, embeddings, and a
    shared camera burst id, and pass 2 must not stitch the cut back together.
    """
    from encounters import cut_microsegments, segment_encounters

    emb = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    photos = [
        _make_photo(
            ts_offset_s=0,
            subj_emb=emb,
            global_emb=emb,
            species=_strong_species("Verdin"),
            focal_length=600,
            burst_id="camera-burst-1",
            photo_id=1,
        ),
        _make_photo(
            ts_offset_s=0.05,
            subj_emb=emb,
            global_emb=emb,
            species=_strong_species("Costa's Hummingbird"),
            focal_length=600,
            burst_id="camera-burst-1",
            photo_id=2,
        ),
    ]

    microsegments, trace = cut_microsegments(photos, emit_trace=True)
    assert [len(segment) for segment in microsegments] == [1, 1]
    assert trace[0]["decision"] == "cut_species"
    conflict = trace[0]["species_conflict"]
    assert conflict["photo_a_species"] == "Verdin"
    assert conflict["photo_b_species"] == "Costa's Hummingbird"
    for field in (
        "photo_a_confidence",
        "photo_a_margin",
        "photo_b_confidence",
        "photo_b_margin",
    ):
        assert math.isclose(conflict[field], 0.92)

    encounters = segment_encounters(photos)
    assert [encounter["photo_count"] for encounter in encounters] == [1, 1]


def test_weak_species_change_does_not_force_encounter_boundary():
    from encounters import segment_encounters

    emb = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    photos = [
        _make_photo(
            ts_offset_s=0,
            subj_emb=emb,
            global_emb=emb,
            species=[("Verdin", 0.79, "classifier-a")],
            focal_length=600,
        ),
        _make_photo(
            ts_offset_s=0.05,
            subj_emb=emb,
            global_emb=emb,
            species=[("Costa's Hummingbird", 0.79, "classifier-a")],
            focal_length=600,
        ),
    ]

    encounters = segment_encounters(photos)
    assert [encounter["photo_count"] for encounter in encounters] == [2]


def test_same_confident_species_with_different_case_stays_together():
    from encounters import segment_encounters

    emb = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    photos = [
        _make_photo(
            ts_offset_s=0,
            subj_emb=emb,
            global_emb=emb,
            species=_strong_species("Costa's Hummingbird"),
        ),
        _make_photo(
            ts_offset_s=0.05,
            subj_emb=emb,
            global_emb=emb,
            species=_strong_species("  costa's   hummingbird "),
        ),
    ]

    encounters = segment_encounters(photos)
    assert [encounter["photo_count"] for encounter in encounters] == [2]


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
    """A camera burst still overrides ordinary score dissimilarity when the
    classifier evidence is too weak to assert a species change.
    """
    from encounters import cut_microsegments

    emb_a = np.array([1, 0, 0] * 256, dtype=np.float32)
    emb_b = np.array([0, 1, 0] * 256, dtype=np.float32)
    photos = [
        _make_photo(0, subj_emb=emb_a, species=[("robin", 0.7)], burst_id="B001"),
        _make_photo(10, subj_emb=emb_b, species=[("eagle", 0.7)], burst_id="B001"),
    ]
    segments = cut_microsegments(photos)
    # Despite different embeddings and weak species predictions, the shared
    # burst id keeps them together. Strong species evidence is tested above.
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


def test_compute_s_enc_returns_components_when_asked():
    from encounters import compute_s_enc
    photo_a = {
        "timestamp": "2026-03-07T11:32:04",
        "latitude": 33.7, "longitude": -118.0,
        "focal_length": 600.0,
    }
    photo_b = {
        "timestamp": "2026-03-07T11:32:09",
        "latitude": 33.7, "longitude": -118.0,
        "focal_length": 600.0,
    }
    score, components = compute_s_enc(photo_a, photo_b, return_components=True)
    assert isinstance(score, float)
    assert set(components.keys()) >= {"time", "subj", "global", "species", "meta"}
    # Each component is a dict {value, weight, used}
    assert components["time"]["value"] >= 0.0
    assert components["time"]["weight"] == 0.35  # default w_time
    assert components["time"]["used"] is True   # both photos have timestamps
    assert components["species"]["used"] is False  # neither has species_top5


def test_cut_microsegments_emits_trace():
    import pytest
    from encounters import cut_microsegments
    # 3 photos: two close-in-time, one far apart -> hard time cut between #2 and #3
    photos = [
        {"timestamp": "2026-03-07T11:32:00", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
        {"timestamp": "2026-03-07T11:32:05", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
        {"timestamp": "2026-03-07T11:40:00", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
    ]
    segments, trace = cut_microsegments(photos, emit_trace=True)
    assert len(segments) == 2  # cut between #2 and #3
    assert len(trace) == 2  # one entry per adjacent pair
    # Pair 0->1: kept (small gap, no cut)
    assert trace[0]["pair_index"] == 0
    assert trace[0]["decision"] == "kept"
    assert trace[0]["dt_seconds"] == 5.0
    assert "components" in trace[0]
    # Pair 1->2: hard time cut
    assert trace[1]["pair_index"] == 1
    assert trace[1]["decision"] == "cut_time"
    assert trace[1]["dt_seconds"] == 475.0
    # Internal consistency: per-pair score == weighted sum over USED components
    for entry in trace:
        comps = entry["components"]
        used_items = [c for c in comps.values() if c["used"]]
        total_weight = sum(c["weight"] for c in used_items)
        if total_weight > 0:
            expected = sum(c["value"] * c["weight"] for c in used_items) / total_weight
        else:
            expected = 0.0
        assert entry["score"] == pytest.approx(expected, abs=1e-9)


def test_cut_microsegments_emits_cut_score_decision():
    """A pair whose S_enc falls below hard_cut_score is tagged 'cut_score'."""
    from encounters import cut_microsegments
    # Two photos with tight time gap (so dt < hard_cut_time) but force the
    # hard_cut_score above the achievable score so the cut fires.
    photos = [
        {"timestamp": "2026-03-07T11:32:00", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
        {"timestamp": "2026-03-07T11:32:05", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
    ]
    segments, trace = cut_microsegments(photos, config={"hard_cut_score": 1.5}, emit_trace=True)
    assert len(segments) == 2
    assert len(trace) == 1
    assert trace[0]["decision"] == "cut_score"


def test_cut_microsegments_emits_burst_id_kept_decision():
    """When both photos share a burst_id, the pair is tagged 'burst_id_kept'."""
    from encounters import cut_microsegments
    photos = [
        {"timestamp": "2026-03-07T11:32:00", "latitude": 33.7, "longitude": -118.0,
         "focal_length": 600.0, "burst_id": "B1"},
        {"timestamp": "2026-03-07T11:32:05", "latitude": 33.7, "longitude": -118.0,
         "focal_length": 600.0, "burst_id": "B1"},
    ]
    # Even with a punishing hard_cut_score, the burst_id short-circuit should win.
    segments, trace = cut_microsegments(photos, config={"hard_cut_score": 1.5}, emit_trace=True)
    assert len(segments) == 1
    assert len(trace) == 1
    assert trace[0]["decision"] == "burst_id_kept"


def test_segment_encounters_attaches_trace_to_each_encounter():
    from encounters import segment_encounters
    # 4 photos: two pairs separated by big time gap -> 2 encounters of 2 photos each
    photos = [
        {"timestamp": "2026-03-07T11:32:00", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
        {"timestamp": "2026-03-07T11:32:05", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
        {"timestamp": "2026-03-07T11:50:00", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
        {"timestamp": "2026-03-07T11:50:05", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
    ]
    encounters = segment_encounters(photos, emit_trace=True)
    assert len(encounters) == 2
    for enc in encounters:
        assert "trace" in enc
        # 2 photos -> 1 internal pair
        assert len(enc["trace"]) == 1
        assert enc["trace"][0]["decision"] == "kept"


def test_trace_includes_pair_photo_ids_and_filenames():
    """Trace entries surface which two photos formed the pair, so the
    review UI can render filenames + thumbs and let the user click through.
    """
    from encounters import cut_microsegments
    photos = [
        {"id": 11, "filename": "DSC_1384.NEF",
         "timestamp": "2026-03-07T11:32:00", "focal_length": 600.0},
        {"id": 22, "filename": "DSC_1385.NEF",
         "timestamp": "2026-03-07T11:32:05", "focal_length": 600.0},
    ]
    _, trace = cut_microsegments(photos, emit_trace=True)
    assert len(trace) == 1
    assert trace[0]["photo_a_id"] == 11
    assert trace[0]["photo_b_id"] == 22
    assert trace[0]["photo_a_filename"] == "DSC_1384.NEF"
    assert trace[0]["photo_b_filename"] == "DSC_1385.NEF"


def test_trace_position_aligns_with_encounter_photo_order():
    """Within an encounter, the i-th trace entry must describe the pair
    (photos[i], photos[i+1]) in encounter-photo order. The pipeline-review
    UI relies on this to recover pair photos from old caches that were
    written before photo_a_id/photo_b_id were added to trace entries —
    falling back to enc.photo_ids[i] / enc.photo_ids[i+1].
    """
    from encounters import segment_encounters

    # Three encounters of varying sizes, separated by big time gaps so
    # cut_microsegments produces distinct microsegments. Within each
    # encounter the photos remain in timestamp order.
    photos = [
        # Encounter A: 3 photos
        {"id": 100, "timestamp": "2026-03-07T10:00:00", "focal_length": 600.0},
        {"id": 101, "timestamp": "2026-03-07T10:00:02", "focal_length": 600.0},
        {"id": 102, "timestamp": "2026-03-07T10:00:04", "focal_length": 600.0},
        # Encounter B: 2 photos
        {"id": 200, "timestamp": "2026-03-07T11:00:00", "focal_length": 600.0},
        {"id": 201, "timestamp": "2026-03-07T11:00:03", "focal_length": 600.0},
        # Encounter C: 4 photos
        {"id": 300, "timestamp": "2026-03-07T12:00:00", "focal_length": 600.0},
        {"id": 301, "timestamp": "2026-03-07T12:00:02", "focal_length": 600.0},
        {"id": 302, "timestamp": "2026-03-07T12:00:04", "focal_length": 600.0},
        {"id": 303, "timestamp": "2026-03-07T12:00:06", "focal_length": 600.0},
    ]
    encounters = segment_encounters(photos, emit_trace=True)
    assert len(encounters) == 3

    for enc in encounters:
        photo_ids = [p["id"] for p in enc["photos"]]
        trace = enc["trace"]
        assert len(trace) == len(photo_ids) - 1
        for i, t in enumerate(trace):
            assert t["photo_a_id"] == photo_ids[i], (
                f"trace[{i}].photo_a_id={t['photo_a_id']} != photo_ids[{i}]={photo_ids[i]}"
            )
            assert t["photo_b_id"] == photo_ids[i + 1], (
                f"trace[{i}].photo_b_id={t['photo_b_id']} != photo_ids[{i+1}]={photo_ids[i + 1]}"
            )


def test_components_flag_which_photo_is_missing_each_signal():
    """Each component tags missing_a / missing_b so the UI can say
    "missing on DSC_1385" instead of just rendering a bare dot. This is
    what makes the half-processed-photo case actionable in the trace.
    """
    import numpy as np
    from encounters import compute_s_enc

    subj_emb = np.ones(128, dtype=np.float32)
    photo_a = {
        "id": 1, "filename": "A.NEF",
        "timestamp": "2026-03-07T11:32:00",
        "focal_length": 600.0,
        "dino_subject_embedding": subj_emb,
        "dino_global_embedding": subj_emb,
        "species_top5": [("Western Bluebird", 0.9)],
    }
    photo_b = {
        "id": 2, "filename": "B.NEF",
        "timestamp": "2026-03-07T11:32:05",
        "focal_length": 600.0,
        # subj/global embeddings + species deliberately absent on B
    }
    _, components = compute_s_enc(photo_a, photo_b, return_components=True)
    # Subj signal present on A, missing on B
    assert components["subj"]["used"] is False
    assert components["subj"]["missing_a"] is False
    assert components["subj"]["missing_b"] is True
    # Same for global and species
    assert components["global"]["missing_a"] is False
    assert components["global"]["missing_b"] is True
    assert components["species"]["missing_a"] is False
    assert components["species"]["missing_b"] is True
    # Time and meta both present on both photos
    assert components["time"]["missing_a"] is False
    assert components["time"]["missing_b"] is False
    assert components["meta"]["missing_a"] is False
    assert components["meta"]["missing_b"] is False


def test_segment_encounters_marks_merged_back_boundaries():
    """When two microsegments get merged, the boundary pair should be tagged merged_back."""
    from encounters import segment_encounters
    photos = [
        {"timestamp": f"2026-03-07T11:32:0{i}", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0}
        for i in range(4)
    ]
    cfg = {"hard_cut_score": 1.5, "merge_score": -1.0, "merge_max_gap": 60.0}
    encounters = segment_encounters(photos, config=cfg, emit_trace=True)
    assert len(encounters) == 1  # all merged back
    enc = encounters[0]
    decisions = [t["decision"] for t in enc["trace"]]
    # 3 internal pairs across 4 microsegments: 3 of them are boundaries that got merged_back
    assert decisions.count("merged_back") == 3


# -- subject_absent: detector ran and found nothing --
#
# When one photo has a subject and the other has been classified
# `miss_no_subject` (detector ran, found no animal), that asymmetry is
# real evidence the two frames don't share an encounter. The grouper
# must count it as such, not silently drop the signal.


def test_compute_s_enc_subject_absent_asymmetric_actively_dissimilar():
    """A→has subject, B→subject_absent: subj/species contribute 0.0 with
    full weight (signal *used*), instead of being dropped from the
    weighted average. Otherwise meta=1.0 (same lens) renormalizes to a
    misleading high score."""
    import numpy as np
    from encounters import compute_s_enc

    subj_emb = np.ones(128, dtype=np.float32)
    photo_a = {
        "timestamp": "2026-04-04T10:29:36",
        "focal_length": 600.0,
        "dino_subject_embedding": subj_emb,
        "dino_global_embedding": subj_emb,
        "species_top5": [("Ruddy Duck", 0.9, "m1")],
        "subject_absent": False,
        "subject_present": True,  # detector found the duck
    }
    photo_b = {
        "timestamp": "2026-04-04T10:30:07",  # 31 s later
        "focal_length": 600.0,
        # No subject features — detector ran and found nothing
        "subject_absent": True,
        "subject_present": False,
    }
    score, components = compute_s_enc(photo_a, photo_b, return_components=True)

    # subj is now USED (active 0), not silently dropped
    assert components["subj"]["used"] is True
    assert components["subj"]["value"] == 0.0
    assert components["subj"]["absent_b"] is True
    assert components["subj"]["absent_a"] is False
    assert components["subj"]["missing_b"] is False  # not "missing", actively absent

    # Same for species
    assert components["species"]["used"] is True
    assert components["species"]["value"] == 0.0
    assert components["species"]["absent_b"] is True

    # And the headline score is below hard_cut_score — meta=1.0 can't
    # rescue it anymore.
    assert score < 0.42, (
        f"asymmetric subject_absent should land below hard_cut_score (0.42); "
        f"got {score:.3f}"
    )


def test_compute_s_enc_subject_absent_both_sides_drops_signal():
    """Both photos subject_absent: subj/species are *neutral* (drop and
    renormalize, like the uncomputed case). Two subjectless frames could
    still belong to the same encounter — we have no evidence either way."""
    from encounters import compute_s_enc

    photo_a = {
        "timestamp": "2026-04-04T10:30:07",
        "focal_length": 600.0,
        "subject_absent": True,
    }
    photo_b = {
        "timestamp": "2026-04-04T10:30:08",  # tight gap
        "focal_length": 600.0,
        "subject_absent": True,
    }
    _, components = compute_s_enc(photo_a, photo_b, return_components=True)
    assert components["subj"]["used"] is False
    assert components["species"]["used"] is False


def test_compute_s_enc_both_absent_ignores_stale_cached_features():
    """Both photos subject_absent BUT both still carry stale embeddings
    and species from a prior run (e.g. user raised detector_confidence,
    re-ran regroup; load_photo_features now flags both as subject_absent
    while old features remain on the photo rows). The neutral "no
    evidence either way" rule must hold — we must NOT re-activate
    similarity from stale features and pull the encounter back together.
    """
    import numpy as np
    from encounters import compute_s_enc

    stale_emb = np.ones(128, dtype=np.float32)
    stale_species = [("Ruddy Duck", 0.9, "m1")]
    photo_a = {
        "timestamp": "2026-04-04T10:30:07",
        "focal_length": 600.0,
        "subject_absent": True,
        # Cached from the pre-threshold-change run — must be ignored.
        "dino_subject_embedding": stale_emb,
        "species_top5": stale_species,
    }
    photo_b = {
        "timestamp": "2026-04-04T10:30:08",
        "focal_length": 600.0,
        "subject_absent": True,
        "dino_subject_embedding": stale_emb,
        "species_top5": stale_species,
    }
    _, components = compute_s_enc(photo_a, photo_b, return_components=True)
    assert components["subj"]["used"] is False, (
        "stale subject embedding must not contribute when both photos are "
        "subject_absent — the detector ran and confirmed empty, that's "
        "neutral evidence not similarity"
    )
    assert components["subj"]["value"] == 0.0
    assert components["species"]["used"] is False
    assert components["species"]["value"] == 0.0
    # absent flags still marked, so the trace UI renders the muted
    # "neutral (no subject on both)" message.
    assert components["subj"]["absent_a"] is True
    assert components["subj"]["absent_b"] is True


def test_compute_s_enc_asymmetric_requires_subject_present_not_just_not_absent():
    """The asymmetric-no-subject penalty must only fire when the non-absent
    side has *affirmative* subject evidence (`subject_present=True`).
    Otherwise we conflate "present vs absent" with "unknown vs absent" —
    e.g. during a regroup-only run where some photos have never been
    detected — and impose hard penalties on pairs we have no evidence
    are actually different.
    """
    from encounters import compute_s_enc

    # A: detector hasn't run yet (subject_absent=False, subject_present=False)
    # B: detector ran and confirmed empty
    photo_a_unknown = {
        "timestamp": "2026-04-04T10:30:07",
        "focal_length": 600.0,
        "subject_absent": False,
        "subject_present": False,
    }
    photo_b_absent = {
        "timestamp": "2026-04-04T10:30:08",
        "focal_length": 600.0,
        "subject_absent": True,
        "subject_present": False,
    }
    _, components = compute_s_enc(photo_a_unknown, photo_b_absent, return_components=True)
    assert components["subj"]["used"] is False, (
        "absent vs unknown is not evidence of dissimilarity — must drop, "
        "not penalize"
    )
    assert components["species"]["used"] is False


def test_compute_s_enc_asymmetric_fires_when_other_side_subject_present():
    """Sanity: the asymmetric branch must STILL fire when the non-absent
    side has `subject_present=True` (detector found a passing detection).
    This is the apr2026 Ruddy Duck scenario the original PR targets.
    """
    import numpy as np
    from encounters import compute_s_enc

    subj_emb = np.ones(128, dtype=np.float32)
    photo_a_present = {
        "timestamp": "2026-04-04T10:29:36",
        "focal_length": 600.0,
        "subject_absent": False,
        "subject_present": True,
        "dino_subject_embedding": subj_emb,
        "dino_global_embedding": subj_emb,
        "species_top5": [("Ruddy Duck", 0.9, "m1")],
    }
    photo_b_absent = {
        "timestamp": "2026-04-04T10:30:07",
        "focal_length": 600.0,
        "subject_absent": True,
        "subject_present": False,
    }
    score, components = compute_s_enc(photo_a_present, photo_b_absent, return_components=True)
    assert components["subj"]["used"] is True, (
        "asymmetric must fire when the non-absent side has affirmative "
        "subject evidence (subject_present=True)"
    )
    assert components["subj"]["value"] == 0.0
    assert components["species"]["used"] is True
    assert components["species"]["value"] == 0.0
    assert score < 0.42


def test_compute_s_enc_uncomputed_still_drops_signal():
    """Regression: when subject features are simply not yet computed
    (no `subject_absent` flag, no embedding), we must still drop the
    signal and renormalize. Otherwise mid-pipeline runs penalize photos
    whose embeddings haven't been written yet."""
    import numpy as np
    from encounters import compute_s_enc

    subj_emb = np.ones(128, dtype=np.float32)
    photo_a = {
        "timestamp": "2026-04-04T10:29:36",
        "focal_length": 600.0,
        "dino_subject_embedding": subj_emb,
        "dino_global_embedding": subj_emb,
        "species_top5": [("Ruddy Duck", 0.9, "m1")],
    }
    photo_b = {
        "timestamp": "2026-04-04T10:29:37",
        "focal_length": 600.0,
        # No subject_absent flag and no features — "not computed yet"
    }
    _, components = compute_s_enc(photo_a, photo_b, return_components=True)
    assert components["subj"]["used"] is False
    assert components["species"]["used"] is False
    assert components["subj"]["missing_b"] is True
    assert components["subj"]["absent_b"] is False


def test_segment_encounters_cuts_at_subject_absent_asymmetry():
    """End-to-end: a Ruddy-Duck-like burst followed by 5 subjectless
    frames at +31 s should split into two encounters, not be glued
    together by meta=1.0 dominance."""
    import numpy as np
    from encounters import segment_encounters

    subj_emb = np.ones(128, dtype=np.float32)
    photos = [
        # 6 duck frames in a tight burst
        {"id": i, "timestamp": f"2026-04-04T10:29:3{i}", "focal_length": 600.0,
         "dino_subject_embedding": subj_emb, "dino_global_embedding": subj_emb,
         "species_top5": [("Ruddy Duck", 0.9, "m1")],
         "subject_absent": False, "subject_present": True}
        for i in range(0, 6)
    ] + [
        # 5 subject-absent frames 31+ s later (what 1761..1765 looked like)
        {"id": 100 + i, "timestamp": f"2026-04-04T10:30:0{i}", "focal_length": 600.0,
         "subject_absent": True, "subject_present": False}
        for i in range(0, 5)
    ]
    encounters = segment_encounters(photos)
    assert len(encounters) >= 2, (
        f"expected at least 2 encounters across the subject-absent boundary; "
        f"got {len(encounters)} of sizes "
        f"{[e['photo_count'] for e in encounters]}"
    )
    # The first encounter should not absorb the subjectless frames.
    assert encounters[0]["photo_count"] == 6


# -- null-timestamp grouping (2026-05-29) --


def _null_ts_photo(photo_id, folder_id, filename, subj_emb=None,
                   global_emb=None, species=None, focal_length=None):
    """A photo with no timestamp (scan I/O error / unreadable file)."""
    return {
        "id": photo_id,
        "timestamp": None,
        "folder_id": folder_id,
        "filename": filename,
        "dino_subject_embedding": subj_emb,
        "dino_global_embedding": global_emb,
        "species_top5": species,
        "focal_length": focal_length,
    }


def test_time_delta_both_none_still_inf():
    """_time_delta_seconds keeps its inf contract for None inputs.

    It's shared with the merge stage, which divides by / compares against the
    result; returning None there would crash. The both-null special case lives
    in cut_microsegments, not here.
    """
    from encounters import _time_delta_seconds

    assert _time_delta_seconds(None, None) == float("inf")
    assert _time_delta_seconds(None, datetime(2026, 5, 25, 10, 0, 0)) == float("inf")
    assert _time_delta_seconds(datetime(2026, 5, 25, 10, 0, 0), None) == float("inf")


def test_cut_keeps_contiguous_null_timestamps_together():
    """Signal-less null-timestamp photos group into one segment by file order.

    Unreadable files have no timestamp AND no embeddings/species, so every
    similarity signal is ~0. Without the both-null guard the score cut would
    split them into singletons.
    """
    from encounters import cut_microsegments

    photos = [
        _null_ts_photo(1, 10, "DSC_8039.NEF"),
        _null_ts_photo(2, 10, "DSC_8041.NEF"),
        _null_ts_photo(3, 10, "DSC_8042.NEF"),
        _null_ts_photo(4, 10, "DSC_8043.NEF"),
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 1
    assert len(segments[0]) == 4


def test_cut_null_timestamps_sort_last_by_file():
    """Null-timestamp photos sort after all timestamped photos, by (folder, filename)."""
    from encounters import cut_microsegments

    emb = np.ones(768, dtype=np.float32)
    species = [("robin", 0.9)]
    photos = [
        _null_ts_photo(99, 10, "DSC_8042.NEF"),
        _make_photo(0, subj_emb=emb, global_emb=emb, species=species, photo_id=1),
        _null_ts_photo(98, 10, "DSC_8041.NEF"),
        _make_photo(5, subj_emb=emb, global_emb=emb, species=species, photo_id=2),
    ]
    segments = cut_microsegments(photos)
    flat = [p["id"] for seg in segments for p in seg]
    # Real-timestamp photos (1, 2) first in time order, then nulls by filename.
    assert flat == [1, 2, 98, 99]


def test_cut_asymmetric_null_still_cuts():
    """A null-timestamp photo adjacent to a real one cuts cleanly.

    The null cluster must never contaminate a real encounter.
    """
    from encounters import cut_microsegments

    emb = np.ones(768, dtype=np.float32)
    species = [("robin", 0.9)]
    photos = [
        _make_photo(0, subj_emb=emb, global_emb=emb, species=species, photo_id=1),
        _make_photo(5, subj_emb=emb, global_emb=emb, species=species, photo_id=2),
        _null_ts_photo(50, 10, "DSC_8039.NEF"),
        _null_ts_photo(51, 10, "DSC_8041.NEF"),
    ]
    segments = cut_microsegments(photos)
    # One real-timestamp segment, one null-timestamp segment — never merged.
    assert len(segments) == 2
    assert {p["id"] for p in segments[0]} == {1, 2}
    assert {p["id"] for p in segments[1]} == {50, 51}


def test_cut_both_null_with_signal_still_cuts_on_score():
    """Two undated photos that DO have embeddings (e.g. screenshots) are judged
    on visual similarity, not force-grouped by file order.

    The both-null keep-together rule is only for signal-less rows (unreadable
    files). When real similarity signal exists, dissimilar undated images must
    still split.
    """
    from encounters import cut_microsegments

    emb_a = np.array([1, 0, 0] * 256, dtype=np.float32)
    emb_b = np.array([0, 1, 0] * 256, dtype=np.float32)
    photos = [
        {"id": 1, "timestamp": None, "folder_id": 10, "filename": "shot_a.png",
         "dino_subject_embedding": emb_a, "dino_global_embedding": emb_a,
         "species_top5": [("robin", 0.9)]},
        {"id": 2, "timestamp": None, "folder_id": 10, "filename": "shot_b.png",
         "dino_subject_embedding": emb_b, "dino_global_embedding": emb_b,
         "species_top5": [("eagle", 0.9)]},
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 2


def test_cut_both_null_with_similar_signal_groups():
    """Undated photos with matching embeddings/species still group (score keeps)."""
    from encounters import cut_microsegments

    emb = np.ones(768, dtype=np.float32)
    species = [("robin", 0.9)]
    photos = [
        {"id": 1, "timestamp": None, "folder_id": 10, "filename": "shot_a.png",
         "dino_subject_embedding": emb, "dino_global_embedding": emb,
         "species_top5": species},
        {"id": 2, "timestamp": None, "folder_id": 10, "filename": "shot_b.png",
         "dino_subject_embedding": emb, "dino_global_embedding": emb,
         "species_top5": species},
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 1
    assert len(segments[0]) == 2


def test_cut_both_null_detector_conflict_cuts():
    """Two undated photos with no embeddings/species but conflicting detector
    verdicts (one subject_absent, one subject_present) must split, not
    force-group.

    The detector already ran and disagrees about whether a subject is present.
    compute_s_enc treats absent-vs-present as active dissimilarity (score ~0),
    so this pair must fall through to the score cut rather than the
    signal-less keep-together branch — otherwise an undated empty frame would
    be collapsed into an undated animal frame whenever embeddings happen to be
    missing or failed.
    """
    from encounters import cut_microsegments

    photos = [
        {"id": 1, "timestamp": None, "folder_id": 10, "filename": "a.NEF",
         "dino_subject_embedding": None, "dino_global_embedding": None,
         "species_top5": None, "subject_absent": True, "subject_present": False},
        {"id": 2, "timestamp": None, "folder_id": 10, "filename": "b.NEF",
         "dino_subject_embedding": None, "dino_global_embedding": None,
         "species_top5": None, "subject_absent": False, "subject_present": True},
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 2, (
        f"Expected the detector conflict to cut, got "
        f"{[[p['id'] for p in seg] for seg in segments]}"
    )


def test_cut_both_null_no_detector_run_still_groups():
    """Genuinely signal-less nulls (detector never ran) still group by file
    order even when subject_absent/subject_present are both False.

    Guards against over-tightening _has_similarity_signal: an unreadable file
    has no embeddings, no species, AND no detector verdict, so it must remain
    in the keep-together branch.
    """
    from encounters import cut_microsegments

    photos = [
        {"id": 1, "timestamp": None, "folder_id": 10, "filename": "a.NEF",
         "dino_subject_embedding": None, "dino_global_embedding": None,
         "species_top5": None, "subject_absent": False, "subject_present": False},
        {"id": 2, "timestamp": None, "folder_id": 10, "filename": "b.NEF",
         "dino_subject_embedding": None, "dino_global_embedding": None,
         "species_top5": None, "subject_absent": False, "subject_present": False},
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 1
    assert len(segments[0]) == 2


def test_cut_both_null_focal_length_metadata_cuts():
    """Two undated photos with no embeddings/species/detector verdict but
    differing focal lengths must be judged by the score cut, not force-grouped.

    compute_s_enc always folds sim_meta into the score, so a focal-length
    mismatch (e.g. imports with stripped capture dates but retained lens data)
    is real dissimilarity signal. _has_similarity_signal must recognise focal
    length so the pair falls through to the score cut instead of the
    signal-less keep-together branch.
    """
    from encounters import cut_microsegments

    photos = [
        {"id": 1, "timestamp": None, "folder_id": 10, "filename": "a.jpg",
         "dino_subject_embedding": None, "dino_global_embedding": None,
         "species_top5": None, "focal_length": 24.0},
        {"id": 2, "timestamp": None, "folder_id": 10, "filename": "b.jpg",
         "dino_subject_embedding": None, "dino_global_embedding": None,
         "species_top5": None, "focal_length": 400.0},
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 2, (
        f"Expected the focal-length mismatch to cut, got "
        f"{[[p['id'] for p in seg] for seg in segments]}"
    )


def test_cut_both_null_gps_metadata_cuts():
    """Undated photos with no embeddings/species/detector verdict but distant
    GPS fixes must cut on the score, not force-group by file order."""
    from encounters import cut_microsegments

    photos = [
        {"id": 1, "timestamp": None, "folder_id": 10, "filename": "a.jpg",
         "dino_subject_embedding": None, "dino_global_embedding": None,
         "species_top5": None, "latitude": 47.6, "longitude": -122.3},
        {"id": 2, "timestamp": None, "folder_id": 10, "filename": "b.jpg",
         "dino_subject_embedding": None, "dino_global_embedding": None,
         "species_top5": None, "latitude": 40.7, "longitude": -74.0},
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 2, (
        f"Expected the GPS mismatch to cut, got "
        f"{[[p['id'] for p in seg] for seg in segments]}"
    )


def test_cut_both_null_no_metadata_still_groups():
    """Truly signal-less nulls — no embeddings/species/detector verdict AND no
    focal length or GPS — still group by file order.

    Guards against over-tightening: a focal_length of 0 / None and absent GPS
    must NOT count as signal, keeping unreadable files in the keep-together
    branch.
    """
    from encounters import cut_microsegments

    photos = [
        {"id": 1, "timestamp": None, "folder_id": 10, "filename": "a.NEF",
         "dino_subject_embedding": None, "dino_global_embedding": None,
         "species_top5": None, "focal_length": 0, "latitude": None,
         "longitude": None},
        {"id": 2, "timestamp": None, "folder_id": 10, "filename": "b.NEF",
         "dino_subject_embedding": None, "dino_global_embedding": None,
         "species_top5": None, "focal_length": None, "latitude": None,
         "longitude": None},
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 1
    assert len(segments[0]) == 2


def test_cut_null_timestamps_cut_at_folder_boundary():
    """Signal-less nulls from different folders must NOT fuse into one encounter.

    The null sort key (1, datetime.min, folder_id, filename) places the last
    null of folder A adjacent to the first null of folder B. Without a folder
    boundary check the both-null branch would force-group unrelated unreadable
    files from separate shoots.
    """
    from encounters import cut_microsegments

    photos = [
        _null_ts_photo(1, 10, "DSC_8039.NEF"),
        _null_ts_photo(2, 10, "DSC_8041.NEF"),
        _null_ts_photo(3, 20, "DSC_9001.NEF"),
        _null_ts_photo(4, 20, "DSC_9002.NEF"),
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 2, (
        f"Expected one segment per folder, got "
        f"{[[p['id'] for p in seg] for seg in segments]}"
    )
    assert {p["id"] for p in segments[0]} == {1, 2}
    assert {p["id"] for p in segments[1]} == {3, 4}


def test_cut_both_null_signalful_cuts_at_folder_boundary():
    """Two undated photos with MATCHING embeddings but in DIFFERENT folders must
    cut at the folder boundary.

    folder_id is not part of compute_s_enc, so without an explicit boundary cut
    these visually identical undated frames would slide under the score cut and
    merge two separate shoots into one encounter. The folder change is a hard
    cut for ALL both-null pairs, not just signal-less ones.
    """
    from encounters import cut_microsegments

    emb = np.ones(768, dtype=np.float32)
    species = [("robin", 0.9)]
    photos = [
        {"id": 1, "timestamp": None, "folder_id": 10, "filename": "shot_a.png",
         "dino_subject_embedding": emb, "dino_global_embedding": emb,
         "species_top5": species},
        {"id": 2, "timestamp": None, "folder_id": 20, "filename": "shot_b.png",
         "dino_subject_embedding": emb, "dino_global_embedding": emb,
         "species_top5": species},
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 2, (
        f"Expected the folder boundary to cut despite matching signal, got "
        f"{[[p['id'] for p in seg] for seg in segments]}"
    )
    assert {p["id"] for p in segments[0]} == {1}
    assert {p["id"] for p in segments[1]} == {2}


def test_cut_both_null_signalful_same_folder_groups():
    """Counterpart to the cross-folder cut: two undated photos with MATCHING
    embeddings in the SAME folder are still judged by the score and group.

    The folder-boundary cut must not over-fire within a single shoot.
    """
    from encounters import cut_microsegments

    emb = np.ones(768, dtype=np.float32)
    species = [("robin", 0.9)]
    photos = [
        {"id": 1, "timestamp": None, "folder_id": 10, "filename": "shot_a.png",
         "dino_subject_embedding": emb, "dino_global_embedding": emb,
         "species_top5": species},
        {"id": 2, "timestamp": None, "folder_id": 10, "filename": "shot_b.png",
         "dino_subject_embedding": emb, "dino_global_embedding": emb,
         "species_top5": species},
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 1
    assert len(segments[0]) == 2


def test_cut_null_timestamps_missing_folder_id_still_cuts():
    """Defensive: nulls with no folder_id at all fall through to the score cut.

    Production scans always populate folder_id, but if it's somehow None the
    safe behaviour is to cut rather than collapse unrelated rows.
    """
    from encounters import cut_microsegments

    photos = [
        {"id": 1, "timestamp": None, "folder_id": None, "filename": "a.NEF",
         "dino_subject_embedding": None, "dino_global_embedding": None,
         "species_top5": None},
        {"id": 2, "timestamp": None, "folder_id": None, "filename": "b.NEF",
         "dino_subject_embedding": None, "dino_global_embedding": None,
         "species_top5": None},
    ]
    segments = cut_microsegments(photos)
    assert len(segments) == 2
