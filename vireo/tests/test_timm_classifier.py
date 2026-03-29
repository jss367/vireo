# vireo/tests/test_timm_classifier.py
"""Tests for TimmClassifier -- uses mocked ONNX session to avoid downloading models."""

import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import numpy as np
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


def _make_model_dir(tmp_path, label_descriptions=None):
    """Create a fake model directory with JSON config files.

    Returns the model directory path.
    """
    model_dir = tmp_path / "timm-eva02-large-inat21"
    model_dir.mkdir(parents=True, exist_ok=True)

    class_names = [
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

    config = {
        "input_size": [3, 336, 336],
        "mean": [0.485, 0.456, 0.406],
        "std": [0.229, 0.224, 0.225],
    }

    with open(model_dir / "class_names.json", "w") as f:
        json.dump(class_names, f)
    with open(model_dir / "label_descriptions.json", "w") as f:
        json.dump(label_descriptions, f)
    with open(model_dir / "config.json", "w") as f:
        json.dump(config, f)
    # Create a dummy model.onnx file (just needs to exist for file checks)
    (model_dir / "model.onnx").write_text("dummy")

    return model_dir


def _make_fake_session(num_classes=3):
    """Create a mock ONNX InferenceSession that returns fixed logits."""
    session = MagicMock()

    # Mock get_inputs to return an input with a name
    mock_input = MagicMock()
    mock_input.name = "input"
    session.get_inputs.return_value = [mock_input]

    # Mock run to return logits: class 0 highest
    def fake_run(output_names, input_dict):
        batch_size = list(input_dict.values())[0].shape[0]
        logits = np.array([[5.0, 2.0, 0.5]] * batch_size, dtype=np.float32)
        return [logits]

    session.run = fake_run
    return session


def _make_fake_classifier(tmp_path, label_descriptions=None):
    """Build a TimmClassifier with fake ONNX session -- no model download needed."""
    from timm_classifier import TimmClassifier

    model_dir = _make_model_dir(tmp_path, label_descriptions)
    fake_session = _make_fake_session()

    # Patch the models root and create_session to avoid real ONNX loading
    with patch("timm_classifier._MODELS_ROOT", str(tmp_path)), \
         patch("timm_classifier.onnx_runtime.create_session", return_value=fake_session):
        clf = TimmClassifier(
            "hf-hub:timm/eva02_large_patch14_clip_336.merged2b_ft_inat21"
        )

    return clf


def test_json_config_loading(tmp_path):
    """Verify that JSON config files are loaded correctly during __init__."""
    from timm_classifier import TimmClassifier

    model_dir = _make_model_dir(tmp_path)
    fake_session = _make_fake_session()

    with patch("timm_classifier._MODELS_ROOT", str(tmp_path)), \
         patch("timm_classifier.onnx_runtime.create_session", return_value=fake_session):
        clf = TimmClassifier(
            "hf-hub:timm/eva02_large_patch14_clip_336.merged2b_ft_inat21"
        )

    assert len(clf._class_names) == 3
    assert clf._class_names[0] == "Sturnus vulgaris"
    assert clf._input_size == (336, 336)
    assert clf._mean == [0.485, 0.456, 0.406]
    assert clf._std == [0.229, 0.224, 0.225]
    assert "sturnus vulgaris" in clf._common_names
    assert clf._common_names["sturnus vulgaris"] == "European Starling"


def test_missing_model_dir(tmp_path):
    """__init__ raises FileNotFoundError when model directory is missing."""
    from timm_classifier import TimmClassifier

    with patch("timm_classifier._MODELS_ROOT", str(tmp_path)):
        with pytest.raises(FileNotFoundError, match="ONNX model not found"):
            TimmClassifier(
                "hf-hub:timm/eva02_large_patch14_clip_336.merged2b_ft_inat21"
            )


def test_unknown_model_str():
    """__init__ raises ValueError for unknown model_str."""
    from timm_classifier import TimmClassifier

    with pytest.raises(ValueError, match="Unknown timm model"):
        TimmClassifier("some-unknown-model")


def test_classify_returns_predictions(tmp_path):
    """classify() returns a list of dicts with species, score, auto_tag."""
    clf = _make_fake_classifier(tmp_path)

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


def test_classify_maps_scientific_to_common(tmp_path):
    """Top prediction maps scientific name to common name."""
    clf = _make_fake_classifier(tmp_path)

    path = _make_test_image()
    try:
        results = clf.classify(path)
        top = results[0]
        # Sturnus vulgaris should map to European Starling
        assert top["species"] == "European Starling"
    finally:
        os.unlink(path)


def test_classify_includes_taxonomy(tmp_path):
    """Each prediction includes taxonomy with scientific_name."""
    clf = _make_fake_classifier(tmp_path)

    path = _make_test_image()
    try:
        results = clf.classify(path)
        top = results[0]
        assert "taxonomy" in top
        assert "scientific_name" in top["taxonomy"]
        assert top["taxonomy"]["scientific_name"] == "Sturnus vulgaris"
    finally:
        os.unlink(path)


def test_classify_threshold_filters(tmp_path):
    """classify() filters results below threshold."""
    clf = _make_fake_classifier(tmp_path)

    path = _make_test_image()
    try:
        # High threshold should filter most results
        results = clf.classify(path, threshold=0.9)
        for r in results:
            assert r["score"] >= 0.9
    finally:
        os.unlink(path)


def test_classify_confidence_tag(tmp_path):
    """Each result includes a confidence tag."""
    clf = _make_fake_classifier(tmp_path)

    path = _make_test_image()
    try:
        results = clf.classify(path, threshold=0.0)
        for r in results:
            assert "confidence_tag" in r
            assert r["confidence_tag"].startswith("auto:confidence:")
    finally:
        os.unlink(path)


def test_classify_fallback_to_scientific_name(tmp_path):
    """If no common name mapping, use scientific name as-is."""
    clf = _make_fake_classifier(tmp_path, label_descriptions={})

    path = _make_test_image()
    try:
        results = clf.classify(path)
        top = results[0]
        assert top["species"] == "Sturnus vulgaris"
    finally:
        os.unlink(path)


def test_classify_all_results_sorted_by_score(tmp_path):
    """Results are sorted by descending score."""
    clf = _make_fake_classifier(tmp_path)

    path = _make_test_image()
    try:
        results = clf.classify(path, threshold=0.0)
        scores = [r["score"] for r in results]
        assert scores == sorted(scores, reverse=True)
    finally:
        os.unlink(path)


def test_classify_batch(tmp_path):
    """classify_batch() returns one result list per image."""
    clf = _make_fake_classifier(tmp_path)

    img1 = Image.new("RGB", (336, 336), color="red")
    img2 = Image.new("RGB", (336, 336), color="blue")

    results = clf.classify_batch([img1, img2], threshold=0.0)
    assert len(results) == 2
    assert isinstance(results[0], list)
    assert isinstance(results[1], list)
    # Each image should produce results for all 3 classes
    assert len(results[0]) == 3
    assert len(results[1]) == 3


def test_classify_with_pil_image(tmp_path):
    """classify() accepts PIL Image directly."""
    clf = _make_fake_classifier(tmp_path)

    img = Image.new("RGB", (500, 400), color="green")
    results = clf.classify(img)
    assert isinstance(results, list)
    assert len(results) > 0


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
