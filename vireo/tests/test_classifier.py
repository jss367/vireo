# vireo/tests/test_classifier.py
"""Tests for Classifier -- uses mocked ONNX sessions to avoid downloading models."""

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
    img = Image.new("RGB", (224, 224), color="red")
    img.save(path)
    return path


def _make_model_dir(tmp_path, num_labels=3, include_tol=False):
    """Create a fake model directory with config and dummy files.

    Returns the model directory path.
    """
    model_dir = tmp_path / "bioclip-vit-b-16"
    model_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "input_size": [3, 224, 224],
        "mean": [0.48145466, 0.4578275, 0.40821073],
        "std": [0.26862954, 0.26130258, 0.27577711],
    }

    with open(model_dir / "config.json", "w") as f:
        json.dump(config, f)

    # Create dummy ONNX model files (just need to exist for file checks)
    (model_dir / "image_encoder.onnx").write_text("dummy")
    (model_dir / "text_encoder.onnx").write_text("dummy")
    (model_dir / "tokenizer.json").write_text("dummy")

    if include_tol:
        # Create Tree of Life data files
        embedding_dim = 512
        tol_classes = [
            {
                "species": "Turdus migratorius",
                "common_name": "American Robin",
                "kingdom": "Animalia",
                "phylum": "Chordata",
                "class": "Aves",
                "order": "Passeriformes",
                "family": "Turdidae",
                "genus": "Turdus",
            },
            {
                "species": "Corvus brachyrhynchos",
                "common_name": "American Crow",
                "kingdom": "Animalia",
                "phylum": "Chordata",
                "class": "Aves",
                "order": "Passeriformes",
                "family": "Corvidae",
                "genus": "Corvus",
            },
            {
                "species": "Sturnus vulgaris",
                "common_name": "European Starling",
                "kingdom": "Animalia",
                "phylum": "Chordata",
                "class": "Aves",
                "order": "Passeriformes",
                "family": "Sturnidae",
                "genus": "Sturnus",
            },
        ]

        # Embeddings: (embedding_dim, num_species) -- transposed
        tol_emb = np.random.randn(embedding_dim, len(tol_classes)).astype(np.float32)
        # Normalize columns
        norms = np.linalg.norm(tol_emb, axis=0, keepdims=True)
        tol_emb = tol_emb / norms

        np.save(str(model_dir / "tol_embeddings.npy"), tol_emb)
        with open(model_dir / "tol_classes.json", "w") as f:
            json.dump(tol_classes, f)

    return model_dir


def _make_fake_image_session(embedding_dim=512):
    """Create a mock ONNX InferenceSession for the image encoder."""
    session = MagicMock()
    mock_input = MagicMock()
    mock_input.name = "pixel_values"
    session.get_inputs.return_value = [mock_input]

    def fake_run(output_names, input_dict):
        batch_size = list(input_dict.values())[0].shape[0]
        # Return random normalized embeddings
        features = np.random.randn(batch_size, embedding_dim).astype(np.float32)
        norms = np.linalg.norm(features, axis=-1, keepdims=True)
        features = features / norms
        return [features]

    session.run = fake_run
    return session


def _make_fake_text_session(embedding_dim=512):
    """Create a mock ONNX InferenceSession for the text encoder."""
    session = MagicMock()
    mock_input = MagicMock()
    mock_input.name = "input_ids"
    session.get_inputs.return_value = [mock_input]

    def fake_run(output_names, input_dict):
        batch_size = list(input_dict.values())[0].shape[0]
        features = np.random.randn(batch_size, embedding_dim).astype(np.float32)
        norms = np.linalg.norm(features, axis=-1, keepdims=True)
        features = features / norms
        return [features]

    session.run = fake_run
    return session


def _make_fake_tokenizer():
    """Create a mock tokenizer that returns fake token IDs."""
    tokenizer = MagicMock()

    class FakeEncoding:
        def __init__(self):
            self.ids = list(range(10))

    tokenizer.encode.return_value = FakeEncoding()
    tokenizer.encode_batch.return_value = [FakeEncoding() for _ in range(5)]
    return tokenizer


def _make_custom_classifier(tmp_path, labels=None):
    """Build a Classifier in custom labels mode with fake ONNX sessions."""
    from classifier import Classifier

    if labels is None:
        labels = ["bird", "cat", "dog"]

    model_dir = _make_model_dir(tmp_path)
    fake_image_session = _make_fake_image_session()
    fake_text_session = _make_fake_text_session()
    fake_tokenizer = _make_fake_tokenizer()

    with (
        patch("classifier._MODELS_ROOT", str(tmp_path)),
        patch(
            "classifier.onnx_runtime.create_session",
            side_effect=[fake_image_session, fake_text_session],
        ),
        patch("classifier._load_tokenizer", return_value=fake_tokenizer),
    ):
        clf = Classifier(
            labels=labels,
            model_str="ViT-B-16",
            pretrained_str="/fake/path",
        )

    return clf


def _make_tol_classifier(tmp_path):
    """Build a Classifier in Tree of Life mode with fake ONNX sessions."""
    from classifier import Classifier

    model_dir = _make_model_dir(tmp_path, include_tol=True)
    fake_image_session = _make_fake_image_session()

    with (
        patch("classifier._MODELS_ROOT", str(tmp_path)),
        patch(
            "classifier.onnx_runtime.create_session",
            return_value=fake_image_session,
        ),
    ):
        clf = Classifier(
            labels=None,
            model_str="ViT-B-16",
            pretrained_str="/fake/path",
        )

    return clf


class TestCustomLabelsMode:
    """Tests for custom labels classification mode."""

    def test_classify_returns_predictions(self, tmp_path):
        """classify() returns a list of dicts with species, score, and auto_tag."""
        clf = _make_custom_classifier(tmp_path)

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

    def test_classify_with_threshold(self, tmp_path):
        """classify() filters results below threshold."""
        clf = _make_custom_classifier(tmp_path)

        path = _make_test_image()
        try:
            results = clf.classify(path, threshold=0.99)
            for r in results:
                assert r["score"] >= 0.99
        finally:
            os.unlink(path)

    def test_classify_includes_confidence_tag(self, tmp_path):
        """Each result includes a confidence tag like 'auto:confidence:0.95'."""
        clf = _make_custom_classifier(tmp_path)

        path = _make_test_image()
        try:
            results = clf.classify(path, threshold=0.0)
            assert len(results) > 0
            top = results[0]
            assert "confidence_tag" in top
            assert top["confidence_tag"].startswith("auto:confidence:")
        finally:
            os.unlink(path)

    def test_classify_with_embedding_returns_tuple(self, tmp_path):
        """classify_with_embedding returns (predictions, embedding) tuple."""
        clf = _make_custom_classifier(tmp_path)

        img = Image.new("RGB", (224, 224), color="blue")
        preds, embedding = clf.classify_with_embedding(img, threshold=0.0)

        assert isinstance(preds, list)
        assert len(preds) == 3  # bird, cat, dog
        assert isinstance(embedding, np.ndarray)
        assert embedding.dtype == np.float32
        assert embedding.ndim == 1
        # Should be normalized (approximately unit length)
        assert abs(np.linalg.norm(embedding) - 1.0) < 0.01

    def test_classify_batch_with_embedding(self, tmp_path):
        """classify_batch_with_embedding returns list of (preds, emb) tuples."""
        clf = _make_custom_classifier(tmp_path)

        imgs = [
            Image.new("RGB", (224, 224), color="red"),
            Image.new("RGB", (224, 224), color="green"),
        ]
        results = clf.classify_batch_with_embedding(imgs, threshold=0.0)

        assert len(results) == 2
        for preds, emb in results:
            assert isinstance(preds, list)
            assert len(preds) == 3
            assert isinstance(emb, np.ndarray)
            assert emb.dtype == np.float32

    def test_scores_sum_to_one(self, tmp_path):
        """Softmax probabilities should sum to approximately 1."""
        clf = _make_custom_classifier(tmp_path)

        img = Image.new("RGB", (224, 224), color="green")
        preds, _ = clf.classify_with_embedding(img, threshold=0.0)
        total = sum(p["score"] for p in preds)
        assert abs(total - 1.0) < 0.01

    def test_empty_labels_raises(self, tmp_path):
        """Empty labels list raises ValueError."""
        from classifier import Classifier

        _make_model_dir(tmp_path)
        fake_image_session = _make_fake_image_session()
        with (
            patch("classifier._MODELS_ROOT", str(tmp_path)),
            patch(
                "classifier.onnx_runtime.create_session",
                return_value=fake_image_session,
            ),
            pytest.raises(ValueError, match="labels list must not be empty"),
        ):
            Classifier(labels=[], model_str="ViT-B-16")

    def test_unknown_model_raises(self):
        """Unknown model_str raises ValueError."""
        from classifier import Classifier

        with pytest.raises(ValueError, match="Unknown BioCLIP model"):
            Classifier(labels=["bird"], model_str="unknown-model")

    def test_missing_model_dir_raises(self, tmp_path):
        """Missing model directory raises FileNotFoundError."""
        from classifier import Classifier

        with (
            patch("classifier._MODELS_ROOT", str(tmp_path)),
            pytest.raises(FileNotFoundError, match="image encoder"),
        ):
            Classifier(labels=["bird"], model_str="ViT-B-16")

    def test_embedding_cache(self, tmp_path):
        """Embeddings are cached to disk and loaded on second init."""
        from classifier import Classifier

        model_dir = _make_model_dir(tmp_path)
        fake_image_session = _make_fake_image_session()
        fake_text_session = _make_fake_text_session()
        fake_tokenizer = _make_fake_tokenizer()
        cache_dir = tmp_path / "cache"

        with (
            patch("classifier._MODELS_ROOT", str(tmp_path)),
            patch("classifier.CACHE_DIR", str(cache_dir)),
            patch(
                "classifier._MANIFEST_PATH",
                str(cache_dir / "manifest.json"),
            ),
            patch(
                "classifier.onnx_runtime.create_session",
                side_effect=[fake_image_session, fake_text_session],
            ),
            patch("classifier._load_tokenizer", return_value=fake_tokenizer),
        ):
            # First init: computes and caches
            clf1 = Classifier(labels=["bird", "cat"], model_str="ViT-B-16")
            emb1 = clf1._txt_embeddings.copy()

        # Cache file should exist
        cache_files = list(cache_dir.glob("*.npy"))
        assert len(cache_files) == 1

        fake_image_session2 = _make_fake_image_session()
        with (
            patch("classifier._MODELS_ROOT", str(tmp_path)),
            patch("classifier.CACHE_DIR", str(cache_dir)),
            patch(
                "classifier._MANIFEST_PATH",
                str(cache_dir / "manifest.json"),
            ),
            patch(
                "classifier.onnx_runtime.create_session",
                return_value=fake_image_session2,
            ),
        ):
            # Second init: loads from cache (text session not needed)
            clf2 = Classifier(labels=["bird", "cat"], model_str="ViT-B-16")

        np.testing.assert_array_equal(clf2._txt_embeddings, emb1)


class TestTreeOfLifeMode:
    """Tests for Tree of Life classification mode."""

    def test_tol_classify_returns_predictions(self, tmp_path):
        """classify() in ToL mode returns predictions with taxonomy."""
        clf = _make_tol_classifier(tmp_path)

        path = _make_test_image()
        try:
            results = clf.classify(path, threshold=0.0)
            assert isinstance(results, list)
            assert len(results) > 0
            top = results[0]
            assert "species" in top
            assert "score" in top
            assert "auto_tag" in top
        finally:
            os.unlink(path)

    def test_tol_includes_taxonomy(self, tmp_path):
        """Tree of Life results include taxonomy hierarchy."""
        clf = _make_tol_classifier(tmp_path)

        img = Image.new("RGB", (224, 224), color="green")
        preds, _ = clf.classify_with_embedding(img, threshold=0.0)
        # At least some results should have taxonomy
        has_taxonomy = [p for p in preds if "taxonomy" in p]
        assert len(has_taxonomy) > 0
        tax = has_taxonomy[0]["taxonomy"]
        assert "scientific_name" in tax

    def test_tol_classify_with_embedding(self, tmp_path):
        """classify_with_embedding in ToL mode returns embedding."""
        clf = _make_tol_classifier(tmp_path)

        img = Image.new("RGB", (224, 224), color="red")
        preds, embedding = clf.classify_with_embedding(img, threshold=0.0)

        assert isinstance(preds, list)
        assert isinstance(embedding, np.ndarray)
        assert embedding.dtype == np.float32
        assert abs(np.linalg.norm(embedding) - 1.0) < 0.01

    def test_tol_missing_files_raises(self, tmp_path):
        """Missing ToL files raise FileNotFoundError."""
        from classifier import Classifier

        # Create model dir with config but no ToL files
        _make_model_dir(tmp_path, include_tol=False)
        fake_image_session = _make_fake_image_session()

        with (
            patch("classifier._MODELS_ROOT", str(tmp_path)),
            patch(
                "classifier.onnx_runtime.create_session",
                return_value=fake_image_session,
            ),
            pytest.raises(FileNotFoundError, match="Tree of Life"),
        ):
            Classifier(labels=None, model_str="ViT-B-16")

    def test_tol_batch_classify(self, tmp_path):
        """classify_batch_with_embedding works in ToL mode."""
        clf = _make_tol_classifier(tmp_path)

        imgs = [
            Image.new("RGB", (224, 224), color="red"),
            Image.new("RGB", (224, 224), color="blue"),
        ]
        results = clf.classify_batch_with_embedding(imgs, threshold=0.0)

        assert len(results) == 2
        for preds, emb in results:
            assert isinstance(preds, list)
            assert isinstance(emb, np.ndarray)


class TestEmbeddingCache:
    """Tests for the embedding cache path utility."""

    def test_cache_path_uses_npy_extension(self):
        """Cache path should use .npy extension (not .pt)."""
        from classifier import _embedding_cache_path

        path = _embedding_cache_path(["bird", "cat"], "ViT-B-16")
        assert path.endswith(".npy")

    def test_cache_path_deterministic(self):
        """Same inputs produce same cache path."""
        from classifier import _embedding_cache_path

        p1 = _embedding_cache_path(["bird", "cat"], "ViT-B-16")
        p2 = _embedding_cache_path(["bird", "cat"], "ViT-B-16")
        assert p1 == p2

    def test_cache_path_differs_by_model(self):
        """Different models produce different cache paths."""
        from classifier import _embedding_cache_path

        p1 = _embedding_cache_path(["bird"], "ViT-B-16")
        p2 = _embedding_cache_path(["bird"], "hf-hub:imageomics/bioclip-2")
        assert p1 != p2

    def test_cache_path_differs_by_labels(self):
        """Different labels produce different cache paths."""
        from classifier import _embedding_cache_path

        p1 = _embedding_cache_path(["bird", "cat"], "ViT-B-16")
        p2 = _embedding_cache_path(["bird", "dog"], "ViT-B-16")
        assert p1 != p2
