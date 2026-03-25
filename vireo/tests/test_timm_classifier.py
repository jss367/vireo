# vireo/tests/test_timm_classifier.py
"""Tests for TimmClassifier — uses mocked model to avoid downloading weights."""

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from PIL import Image


def _make_test_image():
    """Create a temporary test image."""
    f = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
    path = f.name
    f.close()
    img = Image.new("RGB", (336, 336), color="green")
    img.save(path)
    return path


def _make_fake_classifier(label_descriptions=None):
    """Build a TimmClassifier with fake internals — no timm download needed."""
    import torch
    from timm_classifier import TimmClassifier
    from torchvision import transforms

    # Bypass __init__ entirely
    clf = object.__new__(TimmClassifier)

    clf._class_names = [
        "Sturnus vulgaris",
        "Turdus migratorius",
        "Corvus brachyrhynchos",
    ]

    if label_descriptions is None:
        label_descriptions = {
            "Sturnus vulgaris": "European Starling, Bird",
            "Turdus migratorius": "American Robin, Bird",
            "Corvus brachyrhynchos": "American Crow, Bird",
        }

    clf._common_names = {}
    for sci_name, desc in label_descriptions.items():
        parts = desc.rsplit(", ", 1)
        common = parts[0] if len(parts) > 1 else desc
        if common.lower() != sci_name.lower():
            clf._common_names[sci_name.lower()] = common

    clf._taxonomy = None
    clf._device = "cpu"

    # Simple transform that resizes to 336x336 and converts to tensor
    clf._transform = transforms.Compose([
        transforms.Resize((336, 336)),
        transforms.ToTensor(),
    ])

    # Fake model that returns fixed logits: class 0 highest
    class FakeModel:
        def __call__(self, x):
            batch_size = x.shape[0]
            return torch.tensor([[5.0, 2.0, 0.5]] * batch_size)

    clf._model = FakeModel()

    return clf


def test_classify_returns_predictions():
    """classify() returns a list of dicts with species, score, auto_tag."""
    clf = _make_fake_classifier()

    path = _make_test_image()
    try:
        results = clf.classify(path)
        assert isinstance(results, list)
        assert len(results) > 0
        top = results[0]
        assert "species" in top
        assert "score" in top
        assert "auto_tag" in top
        assert top["auto_tag"].startswith("auto:")
        assert 0 <= top["score"] <= 1
    finally:
        os.unlink(path)


def test_classify_maps_scientific_to_common():
    """Top prediction maps scientific name to common name."""
    clf = _make_fake_classifier()

    path = _make_test_image()
    try:
        results = clf.classify(path)
        top = results[0]
        # Sturnus vulgaris should map to European Starling
        assert top["species"] == "European Starling"
    finally:
        os.unlink(path)


def test_classify_includes_taxonomy():
    """Each prediction includes taxonomy with scientific_name."""
    clf = _make_fake_classifier()

    path = _make_test_image()
    try:
        results = clf.classify(path)
        top = results[0]
        assert "taxonomy" in top
        assert "scientific_name" in top["taxonomy"]
        assert top["taxonomy"]["scientific_name"] == "Sturnus vulgaris"
    finally:
        os.unlink(path)


def test_classify_threshold_filters():
    """classify() filters results below threshold."""
    clf = _make_fake_classifier()

    path = _make_test_image()
    try:
        # High threshold should filter most results
        results = clf.classify(path, threshold=0.9)
        for r in results:
            assert r["score"] >= 0.9
    finally:
        os.unlink(path)


def test_classify_confidence_tag():
    """Each result includes a confidence tag."""
    clf = _make_fake_classifier()

    path = _make_test_image()
    try:
        results = clf.classify(path, threshold=0.0)
        for r in results:
            assert "confidence_tag" in r
            assert r["confidence_tag"].startswith("auto:confidence:")
    finally:
        os.unlink(path)


def test_classify_fallback_to_scientific_name():
    """If no common name mapping, use scientific name as-is."""
    clf = _make_fake_classifier(label_descriptions={})

    path = _make_test_image()
    try:
        results = clf.classify(path)
        top = results[0]
        assert top["species"] == "Sturnus vulgaris"
    finally:
        os.unlink(path)


def test_classify_all_results_sorted_by_score():
    """Results are sorted by descending score."""
    clf = _make_fake_classifier()

    path = _make_test_image()
    try:
        results = clf.classify(path, threshold=0.0)
        scores = [r["score"] for r in results]
        assert scores == sorted(scores, reverse=True)
    finally:
        os.unlink(path)


def test_known_models_have_model_type():
    """All entries in KNOWN_MODELS have a model_type field."""
    from models import KNOWN_MODELS

    for m in KNOWN_MODELS:
        assert "model_type" in m, f"Model {m['id']} missing model_type"
        assert m["model_type"] in ("bioclip", "timm"), (
            f"Model {m['id']} has unexpected model_type: {m['model_type']}"
        )


def test_timm_model_in_known_models():
    """The timm iNat21 model is in KNOWN_MODELS."""
    from models import KNOWN_MODELS

    timm_models = [m for m in KNOWN_MODELS if m["model_type"] == "timm"]
    assert len(timm_models) >= 1
    inat = timm_models[0]
    assert inat["id"] == "timm-inat21-eva02-l"
    assert "iNat21" in inat["name"]
    assert inat["model_str"].startswith("hf-hub:timm/")


def test_get_models_includes_model_type():
    """get_models() returns model_type for each model."""
    from models import get_models

    models = get_models()
    for m in models:
        assert "model_type" in m, f"Model {m['id']} missing model_type from get_models()"
