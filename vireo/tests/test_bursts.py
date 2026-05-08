# vireo/tests/test_bursts.py
"""Tests for burst clustering within encounters (Stage 3).

Uses synthetic photo dicts to verify burst boundary detection based on
time gaps and embedding cosine similarity.
"""
import os
import sys
from datetime import datetime, timedelta

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _make_photo(ts_offset_s=0, subj_emb=None, photo_id=None):
    """Helper to build a photo dict for burst testing."""
    base = datetime(2026, 3, 20, 10, 0, 0)
    return {
        "id": photo_id or ts_offset_s,
        "timestamp": (base + timedelta(seconds=ts_offset_s)).isoformat(),
        "dino_subject_embedding": subj_emb,
    }


# -- Burst detection: time gap --


def test_burst_cut_on_time_gap():
    """Photos separated by >3s should be in different bursts."""
    from bursts import detect_bursts

    photos = [
        _make_photo(0.0),
        _make_photo(0.5),
        _make_photo(1.0),
        _make_photo(5.0),  # >3s gap
        _make_photo(5.5),
    ]
    bursts = detect_bursts(photos)
    assert len(bursts) == 2
    assert len(bursts[0]) == 3
    assert len(bursts[1]) == 2


def test_burst_no_cut_within_time():
    """Photos within 3s should stay in the same burst."""
    from bursts import detect_bursts

    photos = [
        _make_photo(0.0),
        _make_photo(0.3),
        _make_photo(0.6),
        _make_photo(0.9),
    ]
    bursts = detect_bursts(photos)
    assert len(bursts) == 1
    assert len(bursts[0]) == 4


# -- Burst detection: embedding cosine --


def test_burst_cut_on_embedding():
    """Low cosine similarity should trigger a burst cut."""
    from bursts import detect_bursts

    emb_a = np.array([1, 0, 0] * 256, dtype=np.float32)
    emb_b = np.array([0, 1, 0] * 256, dtype=np.float32)  # orthogonal → cosine=0

    photos = [
        _make_photo(0.0, subj_emb=emb_a),
        _make_photo(0.5, subj_emb=emb_a),
        _make_photo(1.0, subj_emb=emb_b),  # different subject
        _make_photo(1.5, subj_emb=emb_b),
    ]
    bursts = detect_bursts(photos)
    assert len(bursts) == 2


def test_burst_no_cut_similar_embedding():
    """High cosine similarity should not trigger a cut."""
    from bursts import detect_bursts

    emb = np.ones(768, dtype=np.float32)
    photos = [
        _make_photo(0.0, subj_emb=emb),
        _make_photo(0.5, subj_emb=emb),
    ]
    bursts = detect_bursts(photos)
    assert len(bursts) == 1


def test_burst_mismatched_embedding_dims_does_not_crash():
    """Adjacent photos with stale-variant embeddings at different dims must
    not raise 'shapes not aligned' — treat as 'no embedding signal' so the
    cut decision falls back to the time gap."""
    from bursts import detect_bursts

    emb_768 = np.ones(768, dtype=np.float32)
    emb_1024 = np.ones(1024, dtype=np.float32)
    photos = [
        _make_photo(0.0, subj_emb=emb_768),
        _make_photo(0.5, subj_emb=emb_1024),
        _make_photo(1.0, subj_emb=emb_768),
    ]
    bursts = detect_bursts(photos)
    assert isinstance(bursts, list)
    assert sum(len(b) for b in bursts) == 3


def test_burst_no_cut_missing_embedding():
    """Missing embeddings should not trigger a cut on that criterion."""
    from bursts import detect_bursts

    photos = [
        _make_photo(0.0, subj_emb=None),
        _make_photo(0.5, subj_emb=None),
    ]
    bursts = detect_bursts(photos)
    assert len(bursts) == 1


# -- Edge cases --


def test_burst_single_photo():
    """Single photo produces one burst."""
    from bursts import detect_bursts

    bursts = detect_bursts([_make_photo(0)])
    assert len(bursts) == 1
    assert len(bursts[0]) == 1


def test_burst_empty():
    """Empty input produces empty output."""
    from bursts import detect_bursts

    assert detect_bursts([]) == []


# -- Configurable thresholds --


def test_burst_custom_time_gap():
    """Custom time gap threshold changes where cuts happen."""
    from bursts import detect_bursts

    photos = [
        _make_photo(0.0),
        _make_photo(2.0),
        _make_photo(4.0),
    ]
    # Default 3s → no cuts (gaps are 2s each)
    assert len(detect_bursts(photos)) == 1

    # With 1.5s threshold → two cuts
    assert len(detect_bursts(photos, config={"burst_time_gap": 1.5})) == 3


def test_burst_custom_embedding_threshold():
    """Custom embedding threshold changes sensitivity."""
    from bursts import detect_bursts

    # Two embeddings with moderate similarity (~0.7)
    rng = np.random.RandomState(42)
    base = rng.randn(768).astype(np.float32)
    noise = rng.randn(768).astype(np.float32) * 0.5
    similar = base + noise
    # Normalize
    base = base / np.linalg.norm(base)
    similar = similar / np.linalg.norm(similar)
    cos = float(np.dot(base, similar))

    photos = [
        _make_photo(0.0, subj_emb=base),
        _make_photo(0.5, subj_emb=similar),
    ]

    # Default 0.80 → likely cut if cosine < 0.80
    result_default = detect_bursts(photos)

    # Very low threshold → no cut
    result_low = detect_bursts(photos, config={"burst_embedding_threshold": 0.3})
    assert len(result_low) == 1


# -- Combined criteria --


def test_burst_any_criterion_triggers_cut():
    """A cut fires if either time or embedding criterion fires."""
    from bursts import detect_bursts

    emb = np.ones(768, dtype=np.float32)
    photos = [
        _make_photo(0.0, subj_emb=emb),
        # Only time triggers: 5s > 3s
        _make_photo(5.0, subj_emb=emb),
    ]
    bursts = detect_bursts(photos)
    assert len(bursts) == 2  # time alone is enough


# -- segment_bursts_for_encounters --


def test_segment_bursts_for_encounters():
    """Integration: burst detection enriches encounter dicts."""
    from bursts import segment_bursts_for_encounters

    emb = np.ones(768, dtype=np.float32)
    encounters = [
        {
            "photos": [
                _make_photo(0.0, subj_emb=emb),
                _make_photo(0.5, subj_emb=emb),
                _make_photo(5.0, subj_emb=emb),  # time gap → new burst
                _make_photo(5.3, subj_emb=emb),
            ],
            "species": ("robin", 0.9),
            "photo_count": 4,
            "time_range": ("2026-03-20T10:00:00", "2026-03-20T10:00:05"),
        }
    ]

    result = segment_bursts_for_encounters(encounters)
    assert len(result) == 1
    assert result[0]["burst_count"] == 2
    assert len(result[0]["bursts"]) == 2
    assert len(result[0]["bursts"][0]) == 2
    assert len(result[0]["bursts"][1]) == 2
