"""Tests for highlights selection logic."""
import numpy as np

from vireo.highlights import select_highlights


def _make_photo(pid, quality, species=None, embedding=None):
    """Helper to build a photo-like dict for testing."""
    if embedding is None:
        rng = np.random.RandomState(pid)
        embedding = rng.randn(384).astype(np.float32).tobytes()
    return {
        "id": pid,
        "quality_score": quality,
        "species": species,
        "dino_subject_embedding": embedding,
        "phash_crop": f"{pid:016x}",
    }


def test_select_highlights_basic():
    """Selects top N photos by quality with diversity."""
    photos = [_make_photo(i, 0.9 - i * 0.1) for i in range(10)]
    result = select_highlights(photos, count=3, max_per_species=5)
    assert len(result) == 3
    # First pick should be highest quality
    assert result[0]["id"] == 0


def test_select_highlights_respects_max_per_species():
    """Per-species cap is enforced."""
    photos = [_make_photo(i, 0.9 - i * 0.05, species="Eagle") for i in range(10)]
    result = select_highlights(photos, count=10, max_per_species=2)
    eagle_count = sum(1 for p in result if p["species"] == "Eagle")
    assert eagle_count <= 2


def test_select_highlights_unidentified_capped():
    """Photos without species are grouped under 'Unidentified' and capped."""
    photos = [_make_photo(i, 0.9 - i * 0.05, species=None) for i in range(10)]
    result = select_highlights(photos, count=10, max_per_species=3)
    assert len(result) <= 3


def test_select_highlights_fewer_than_count():
    """Returns all photos when fewer than count are available."""
    photos = [_make_photo(i, 0.8) for i in range(3)]
    result = select_highlights(photos, count=10, max_per_species=5)
    assert len(result) == 3


def test_select_highlights_empty():
    """Empty input returns empty output."""
    result = select_highlights([], count=10, max_per_species=5)
    assert result == []


def test_select_highlights_species_diversity():
    """With multiple species, selection includes variety."""
    photos = []
    for i, sp in enumerate(["Eagle", "Hawk", "Owl", "Finch", "Wren"]):
        for j in range(5):
            photos.append(_make_photo(i * 5 + j, 0.9 - j * 0.05, species=sp))
    result = select_highlights(photos, count=10, max_per_species=3)
    species_in_result = set(p["species"] for p in result)
    # Should have multiple species represented
    assert len(species_in_result) >= 3
