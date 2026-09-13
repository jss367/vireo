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
    # Real exported CLIP image heads report a static feature width on the
    # last output axis; _image_encoder_embedding_dim reads it to validate
    # label-embedding payloads.
    mock_output = MagicMock()
    mock_output.shape = ["batch", embedding_dim]
    session.get_outputs.return_value = [mock_output]

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

    def test_embedding_computation_honors_cancel_check_between_labels(self):
        """A cancelled embedding precompute stops before processing more labels."""
        from classifier import _compute_embeddings_with_progress

        text_session = _make_fake_text_session()
        tokenizer = _make_fake_tokenizer()
        checks = {"count": 0}

        def cancel_check():
            checks["count"] += 1
            return checks["count"] >= 3

        with pytest.raises(RuntimeError, match="classification cancelled"):
            _compute_embeddings_with_progress(
                text_session,
                "input_ids",
                tokenizer,
                ["bird", "cat", "dog"],
                cancel_check=cancel_check,
            )

    def test_embedding_computation_pauses_between_labels_and_resumes(
        self, tmp_path,
    ):
        """A pause request checkpoints the finished labels and unwinds with
        ClassifierLoadPaused; the next call resumes from the checkpoint and
        only encodes the remaining labels."""
        from classifier import (
            ClassifierLoadPaused,
            _compute_embeddings_with_progress,
        )
        from embedding_cache import EmbeddingCheckpoint

        text_session = _make_fake_text_session()
        tokenizer = _make_fake_tokenizer()
        runs = {"count": 0}
        original_run = text_session.run

        def counting_run(*args, **kwargs):
            runs["count"] += 1
            return original_run(*args, **kwargs)

        text_session.run = counting_run

        labels = ["bird", "cat", "dog", "emu"]
        checkpoint = EmbeddingCheckpoint(str(tmp_path), "digest", len(labels))
        pause_polls = {"count": 0}

        def pause_check():
            pause_polls["count"] += 1
            # Pause request lands after two labels are finished.
            return pause_polls["count"] == 3

        with pytest.raises(ClassifierLoadPaused):
            _compute_embeddings_with_progress(
                text_session,
                "input_ids",
                tokenizer,
                labels,
                pause_check=pause_check,
                checkpoint=checkpoint,
            )

        saved = checkpoint.load()
        assert saved is not None and saved.shape == (2, 512), (
            "the two finished labels must be checkpointed before unwinding"
        )
        runs_per_label = runs["count"] // 2
        assert runs_per_label >= 1

        progress = []
        runs["count"] = 0
        result = _compute_embeddings_with_progress(
            text_session,
            "input_ids",
            tokenizer,
            labels,
            progress_callback=lambda done, total: progress.append((done, total)),
            checkpoint=checkpoint,
        )

        assert progress[0] == (2, 4), (
            "progress must resume from the checkpoint, not restart at 0 — "
            f"got {progress[0]}"
        )
        assert result.shape == (512, 4)
        np.testing.assert_array_equal(result[:, :2], saved.T)
        assert runs["count"] == 2 * runs_per_label, (
            "only the two remaining labels may be encoded after resume"
        )

    def test_embedding_cancellation_checkpoints_finished_labels(self, tmp_path):
        """Cancel keeps the work already done so a later run resumes it."""
        from classifier import _compute_embeddings_with_progress
        from embedding_cache import EmbeddingCheckpoint

        text_session = _make_fake_text_session()
        tokenizer = _make_fake_tokenizer()
        labels = ["bird", "cat", "dog"]
        checkpoint = EmbeddingCheckpoint(str(tmp_path), "digest", len(labels))
        checks = {"count": 0}

        def cancel_check():
            checks["count"] += 1
            return checks["count"] >= 3

        with pytest.raises(RuntimeError, match="classification cancelled"):
            _compute_embeddings_with_progress(
                text_session,
                "input_ids",
                tokenizer,
                labels,
                cancel_check=cancel_check,
                checkpoint=checkpoint,
            )

        saved = checkpoint.load()
        assert saved is not None and saved.shape[0] >= 1

    def test_load_or_compute_pause_unwinds_and_resumes_through_cache(
        self, tmp_path,
    ):
        """End to end through the cache service: a pause propagates as
        ClassifierLoadPaused (not as a cancel), leaves a checkpoint in the
        cache directory, and the retry publishes the complete payload and
        removes the checkpoint."""
        from classifier import (
            ClassifierLoadPaused,
            _load_or_compute_label_embeddings,
        )
        from embedding_cache import CHECKPOINT_SUFFIX

        model_dir = _make_model_dir(tmp_path)
        cache_dir = tmp_path / "cache"
        fake_tokenizer = _make_fake_tokenizer()
        labels = ["bird", "cat", "dog", "emu"]
        state = {"paused": True, "polls": 0}

        def pause_check():
            if not state["paused"]:
                return False
            state["polls"] += 1
            # The pre-load probe passes; the pause lands after two labels.
            return state["polls"] == 4

        with (
            patch("classifier.CACHE_DIR", str(cache_dir)),
            patch(
                "classifier._MANIFEST_PATH", str(cache_dir / "manifest.json"),
            ),
            patch(
                "classifier.onnx_runtime.create_session_with_self_heal",
                side_effect=lambda *a, **k: _make_fake_text_session(),
            ),
            patch("classifier._load_tokenizer", return_value=fake_tokenizer),
        ):
            with pytest.raises(ClassifierLoadPaused):
                _load_or_compute_label_embeddings(
                    labels, "ViT-B-16", str(model_dir),
                    pause_check=pause_check,
                )
            partials = list(cache_dir.glob(f"*{CHECKPOINT_SUFFIX}"))
            assert len(partials) == 1, "pause must leave one checkpoint"
            assert not list(
                f for f in cache_dir.glob("*.npy")
                if not f.name.endswith(CHECKPOINT_SUFFIX)
            ), "no final payload may exist before completion"

            state["paused"] = False
            progress = []
            classes, embeddings, _, _ = _load_or_compute_label_embeddings(
                labels, "ViT-B-16", str(model_dir),
                pause_check=pause_check,
                progress_callback=lambda d, t: progress.append((d, t)),
            )

        assert classes == labels
        assert embeddings.shape == (512, 4)
        assert progress and progress[0][0] >= 2, (
            f"resume must start from the checkpoint; got {progress[:2]}"
        )
        assert not list(cache_dir.glob(f"*{CHECKPOINT_SUFFIX}"))
        finals = [
            f for f in cache_dir.glob("*.npy")
            if not f.name.endswith(CHECKPOINT_SUFFIX)
        ]
        assert len(finals) == 1

    def test_classify_returns_predictions(self, tmp_path):
        """classify() returns a list of dicts with species, score, and auto_tag."""
        clf = _make_custom_classifier(tmp_path)

        path = _make_test_image()
        try:
            # threshold=0.0 keeps this a shape check; the default 0.4 can filter
            # everything out when the mocked ONNX sessions return random embeddings
            # whose 3-way softmax happens to land near uniform (~1/3 each).
            results = clf.classify(path, threshold=0.0)
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

    def test_label_space_size_counts_custom_labels(self, tmp_path):
        """Custom-label mode reports how many species it can return."""
        clf = _make_custom_classifier(tmp_path, labels=["bird", "cat"])
        assert clf.label_space_size == 2

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

    def test_text_self_heal_revalidates_against_healed_image_encoder(
        self, tmp_path
    ):
        """A text-encoder self-heal must not fail the run that triggered it.

        The expected embedding width comes from the image session loaded
        *before* the text side self-heals, and a heal redownloads the whole
        snapshot at HuggingFace's current revision. If that revision exports
        a different feature width, validating the freshly computed payload
        against the pre-heal width would reject embeddings the heal just
        repaired, so the repairing run fails and only a manual retry works.
        The image side must be rebuilt and the payload re-validated instead.
        """
        from classifier import Classifier

        model_dir = _make_model_dir(tmp_path)
        fake_tokenizer = _make_fake_tokenizer()
        calls = {"image": 0, "text": 0}

        def _redownload():
            # A real redownload restores the purged file from the current
            # upstream revision — here, a wider export than what was on disk.
            (model_dir / "text_encoder.onnx").write_text("healed-wider")

        def _fake_create_session(path):
            if path.endswith("image_encoder.onnx"):
                calls["image"] += 1
                return _make_fake_image_session(
                    512 if calls["image"] == 1 else 768
                )
            calls["text"] += 1
            if calls["text"] == 1:
                # Corruption signature that trips create_session_with_self_heal.
                raise RuntimeError("Protobuf parsing failed")
            return _make_fake_text_session(768)

        with (
            patch("classifier._MODELS_ROOT", str(tmp_path)),
            patch("classifier.CACHE_DIR", str(tmp_path / "cache")),
            patch(
                "classifier._MANIFEST_PATH",
                str(tmp_path / "cache" / "manifest.json"),
            ),
            patch(
                "classifier.onnx_runtime.create_session",
                side_effect=_fake_create_session,
            ),
            patch(
                "models.build_self_heal_redownloader",
                return_value=_redownload,
            ),
            patch("classifier._load_tokenizer", return_value=fake_tokenizer),
        ):
            clf = Classifier(labels=["bird", "cat"], model_str="ViT-B-16")

        assert clf._txt_embeddings.shape == (768, 2), (
            "the healed snapshot's embeddings must be accepted, not rejected "
            f"against the pre-heal width; got {clf._txt_embeddings.shape}"
        )
        assert calls["image"] == 2, (
            "the image encoder must be rebuilt from the healed snapshot so "
            "inference matmuls against the new width"
        )


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

    def test_tol_label_space_size_counts_all_species(self, tmp_path):
        """ToL mode reports its full species count for the Jobs page row."""
        clf = _make_tol_classifier(tmp_path)
        assert clf.label_space_size == 3

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


class TestTextEncoderBatchRejection:
    """Tests for the strict batched-only text encoder path.

    Old BioCLIP ONNX exports baked batch=1 into a downstream Reshape node
    (see scripts/export_onnx.py:_TextEncoderWrapper). The classifier used
    to silently fall back to per-row inference (~50× slower); now it
    raises a clear error that tells the user to re-download the model.
    """

    def _make_batch1_only_text_session(self, embedding_dim=512):
        """Mock text session that raises for batch > 1, succeeds for batch == 1.

        Simulates the legacy text_encoder.onnx behaviour where the internal
        'gemm_input_reshape' node fails whenever batch > 1.
        """
        session = MagicMock()
        mock_input = MagicMock()
        mock_input.name = "input_ids"
        session.get_inputs.return_value = [mock_input]

        def fake_run(output_names, input_dict):
            tokens = list(input_dict.values())[0]
            if tokens.shape[0] != 1:
                raise RuntimeError(
                    "Non-zero status code returned while running Reshape node. "
                    "Name:'gemm_input_reshape' ... input_shape_size == size was false."
                )
            features = np.random.randn(1, embedding_dim).astype(np.float32)
            norms = np.linalg.norm(features, axis=-1, keepdims=True)
            return [features / norms]

        session.run = fake_run
        return session

    def test_batch_rejection_writes_verify_failed_sentinel(self, tmp_path):
        """A stale text encoder ONNX must self-heal via the existing Repair
        flow: write `.verify_failed` into the model dir so Settings → Models
        flips the install to 'incomplete' and surfaces the Repair button.

        Without the sentinel, the install keeps passing pinned-revision
        verification (because the user's bytes still match the old upstream
        commit they were downloaded from), so Settings shows the model as
        healthy while every pipeline run blows up at inference. The sentinel
        bridges that gap: detection happens at runtime, repair happens via
        the same Settings flow used for any other corruption case.
        """
        import model_verify
        import pytest
        from classifier import Classifier

        model_dir = _make_model_dir(tmp_path)
        fake_image_session = _make_fake_image_session()
        fake_text_session = self._make_batch1_only_text_session()
        fake_tokenizer = _make_fake_tokenizer()

        with (
            patch("classifier._MODELS_ROOT", str(tmp_path)),
            patch("classifier.CACHE_DIR", str(tmp_path / "cache")),
            patch(
                "classifier._MANIFEST_PATH",
                str(tmp_path / "cache" / "manifest.json"),
            ),
            patch(
                "classifier.onnx_runtime.create_session",
                side_effect=[fake_image_session, fake_text_session],
            ),
            patch("classifier._load_tokenizer", return_value=fake_tokenizer),
            pytest.raises(RuntimeError) as excinfo,
        ):
            Classifier(labels=["bird", "cat"], model_str="ViT-B-16")

        # The error message must point users at the existing Repair flow,
        # not tell them to manually re-download.
        msg = str(excinfo.value)
        assert "Settings" in msg and "Models" in msg, (
            f"error should direct user to Settings → Models, got: {msg!r}"
        )
        assert "Repair" in msg, (
            f"error should mention Repair button, got: {msg!r}"
        )

        # The sentinel must have been written so models._classify_model_state
        # flips this install to 'incomplete'.
        sentinel = model_dir / model_verify.VERIFY_FAILED_SENTINEL
        assert sentinel.is_file(), (
            f".verify_failed sentinel must be written to {sentinel}"
        )
        # The sentinel reason should identify the specific failure mode so
        # future debugging from .vireo logs / support output can distinguish
        # this from hash-mismatch corruption.
        reason = sentinel.read_text()
        assert "stale-export" in reason or "batched" in reason.lower(), (
            f"sentinel reason should identify stale-export, got: {reason!r}"
        )

    def test_transient_text_session_error_does_not_write_sentinel(
        self, tmp_path
    ):
        """A transient ONNX runtime failure (memory pressure, provider
        glitch, mmap race) must NOT mark the install as needing Repair.

        Only the specific stale-export signature (the ``gemm_input_reshape``
        node from old BioCLIP exports) flips the install to incomplete.
        Other inference errors propagate without mutating model state, so
        a healthy install isn't permanently flagged for Repair after a
        one-off runtime hiccup.
        """
        import model_verify
        from classifier import Classifier

        session = MagicMock()
        mock_input = MagicMock()
        mock_input.name = "input_ids"
        session.get_inputs.return_value = [mock_input]

        # A generic ORT runtime error that doesn't match the stale-export
        # signature — e.g. an out-of-memory failure during inference.
        def fake_run(output_names, input_dict):
            raise RuntimeError(
                "Failed to allocate memory for inference; "
                "system under memory pressure."
            )

        session.run = fake_run

        model_dir = _make_model_dir(tmp_path)
        fake_image_session = _make_fake_image_session()
        fake_tokenizer = _make_fake_tokenizer()

        with (
            patch("classifier._MODELS_ROOT", str(tmp_path)),
            patch("classifier.CACHE_DIR", str(tmp_path / "cache")),
            patch(
                "classifier._MANIFEST_PATH",
                str(tmp_path / "cache" / "manifest.json"),
            ),
            patch(
                "classifier.onnx_runtime.create_session",
                side_effect=[fake_image_session, session],
            ),
            patch("classifier._load_tokenizer", return_value=fake_tokenizer),
            pytest.raises(RuntimeError, match="memory"),
        ):
            Classifier(labels=["bird", "cat"], model_str="ViT-B-16")

        # The sentinel must NOT have been written: no Repair badge for
        # transient errors.
        sentinel = model_dir / model_verify.VERIFY_FAILED_SENTINEL
        assert not sentinel.is_file(), (
            f"transient error must not write sentinel; found at {sentinel}"
        )


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


class TestEmbeddingPrecompute:
    def test_precompute_loads_only_text_encoder(self, tmp_path):
        from classifier import _embedding_cache_path, precompute_label_embeddings

        model_dir = _make_model_dir(tmp_path)
        text_session = _make_fake_text_session()
        # One encoding per input label exercises the intended (num_labels,
        # context_length) batch shape rather than the tokenizer's default
        # 5-row fixture — which would leave 75 rows zero-filled in the
        # 80-row _tokenize() batch used by _compute_embeddings_with_progress.
        tokenizer = MagicMock()

        class FakeEncoding:
            def __init__(self):
                self.ids = list(range(10))

        tokenizer.encode.return_value = FakeEncoding()
        tokenizer.encode_batch.side_effect = lambda texts: [
            FakeEncoding() for _ in texts
        ]
        labels = ["bird", "cat"]
        cache_dir = tmp_path / "cache"

        with (
            patch("classifier.CACHE_DIR", str(cache_dir)),
            patch(
                "classifier._MANIFEST_PATH",
                str(cache_dir / "manifest.json"),
            ),
            patch(
                "classifier.onnx_runtime.create_session_with_self_heal",
                return_value=text_session,
            ) as create_session,
            patch("classifier._load_tokenizer", return_value=tokenizer),
            patch("models.build_self_heal_redownloader", return_value=None),
        ):
            count = precompute_label_embeddings(
                labels,
                model_str="ViT-B-16",
                pretrained_str=str(model_dir),
            )

            assert count == 2
            assert create_session.call_count == 1
            assert create_session.call_args.args[0].endswith("text_encoder.onnx")

            cache_path = _embedding_cache_path(
                labels, "ViT-B-16", str(model_dir),
            )
            payload = np.load(cache_path)
            assert payload.shape == (512, 2)
            assert payload.dtype == np.float32

            # A second precompute for the same labels must hit the cache: no
            # additional text encoder is loaded.
            count_again = precompute_label_embeddings(
                labels,
                model_str="ViT-B-16",
                pretrained_str=str(model_dir),
            )

            assert count_again == 2
            assert create_session.call_count == 1

            tokenizer.encode_batch.reset_mock()
            progress = []
            count_extended = precompute_label_embeddings(
                ["cat", "dog", "bird"],
                model_str="ViT-B-16", pretrained_str=str(model_dir),
                progress_callback=lambda current, total: progress.append((current, total)),
            )
            assert count_extended == 3
            assert create_session.call_count == 2
            # Only the new species goes through the text encoder. Existing
            # labels remain reusable even in a different order.
            assert tokenizer.encode_batch.call_count == 1
            assert all("dog" in text for text in tokenizer.encode_batch.call_args.args[0])
            assert progress[0] == (2, 3), "cached labels should be counted before estimating uncached work"
            assert progress[-1] == (3, 3)

            from classifier import _embedding_is_cached
            assert _embedding_is_cached(["dog", "cat"], "ViT-B-16", str(model_dir))

            precompute_label_embeddings(
                ["dog", "cat"], model_str="ViT-B-16", pretrained_str=str(model_dir),
            )
            assert create_session.call_count == 2, "a cached subset must not load the text encoder"


class TestGpuLockScope:
    """Regression: the GPU semaphore must wrap only the image-encoder
    ``session.run`` call inside ``_get_image_embedding`` — not the
    preprocessing or normalisation around it. Codex P2 on PR #899.
    """

    def test_get_image_embedding_holds_gpu_lock_around_session_run(self, tmp_path):
        """``_image_session.run`` must execute with the GPU lock held;
        preprocessing/normalisation must not."""
        import pipeline_locks

        clf = _make_custom_classifier(tmp_path)
        # Declare a GPU provider so the conditional lock engages — this
        # is the on-GPU path; the CPU-only skip is covered in its own
        # test below.
        clf._image_session.get_providers.return_value = [
            "CUDAExecutionProvider", "CPUExecutionProvider",
        ]

        snapshots = {}
        original_run = clf._image_session.run

        def record_during_run(output_names, input_dict):
            snapshots["during_run"] = pipeline_locks._GPU_SEMAPHORE._value
            return original_run(output_names, input_dict)

        clf._image_session.run = record_during_run

        baseline = pipeline_locks._GPU_SEMAPHORE._value
        img = Image.new("RGB", (224, 224), color="red")
        clf._get_image_embedding(img)

        assert pipeline_locks._GPU_SEMAPHORE._value == baseline, (
            "semaphore must be released on the way out"
        )
        assert snapshots["during_run"] == baseline - 1, (
            "GPU lock must be held during _image_session.run"
        )

    def test_classify_batch_with_embedding_holds_lock_only_at_session_run(self, tmp_path):
        """In the batch path, each per-image ``session.run`` holds the
        lock; the surrounding loop body (preprocessing, softmax,
        result-building) must not.
        """
        import pipeline_locks

        clf = _make_custom_classifier(tmp_path)
        clf._image_session.get_providers.return_value = [
            "CUDAExecutionProvider", "CPUExecutionProvider",
        ]

        snapshots = {"in_run": [], "post_run_in_loop": []}
        original_run = clf._image_session.run

        def record_during_run(output_names, input_dict):
            snapshots["in_run"].append(pipeline_locks._GPU_SEMAPHORE._value)
            return original_run(output_names, input_dict)

        clf._image_session.run = record_during_run

        baseline = pipeline_locks._GPU_SEMAPHORE._value
        images = [Image.new("RGB", (224, 224), color="red") for _ in range(3)]
        results = clf.classify_batch_with_embedding(images)

        assert len(results) == 3
        assert pipeline_locks._GPU_SEMAPHORE._value == baseline, (
            "semaphore must be released on the way out"
        )
        assert snapshots["in_run"] == [baseline - 1] * 3, (
            "GPU lock must be held during every per-image session.run call"
        )

    def test_get_image_embedding_skips_gpu_lock_for_cpu_only_session(self, tmp_path):
        """When BioCLIP runs on a CPU-only provider (Apple Silicon
        excludes CoreML for external-data ONNX models; CPU-only installs
        likewise), the image encoder must not take the process-wide GPU
        semaphore. Codex P2 on PR #899.
        """
        import pipeline_locks

        clf = _make_custom_classifier(tmp_path)
        clf._image_session.get_providers.return_value = ["CPUExecutionProvider"]

        snapshots = {}
        original_run = clf._image_session.run

        def record_during_run(output_names, input_dict):
            snapshots["during_run"] = pipeline_locks._GPU_SEMAPHORE._value
            return original_run(output_names, input_dict)

        clf._image_session.run = record_during_run

        baseline = pipeline_locks._GPU_SEMAPHORE._value
        img = Image.new("RGB", (224, 224), color="red")
        clf._get_image_embedding(img)

        assert snapshots["during_run"] == baseline, (
            "CPU-only image session must not take the GPU semaphore"
        )


@pytest.mark.parametrize("source_changed", [False, True])
def test_upgrade_reuses_legacy_set_only_with_exact_label_identity(tmp_path, monkeypatch, source_changed):
    from types import SimpleNamespace

    import classifier
    import classify_job
    from embedding_cache import LabelEmbeddingCache

    model_dir = _make_model_dir(tmp_path)
    monkeypatch.setattr(classifier, "CACHE_DIR", str(tmp_path / "cache"))
    identity = classifier._embedding_identity(["bird", "cat"], "ViT-B-16", str(model_dir))
    cache = classifier._embedding_cache_service()
    payload = np.arange(1024, dtype=np.float32).reshape(512, 2)
    cache.get_or_compute(identity, lambda: payload)
    source = tmp_path / "region.txt"
    source.write_text("bird\n" + ("dog\n" if source_changed else "cat\n"))
    # Older caches have no label list in their manifest. Reconstruct a
    # candidate from the catalog's recorded source files, then verify its key.
    db = SimpleNamespace(get_labels_fingerprints=lambda: [{"sources": [str(source)]}])
    monkeypatch.setattr(classify_job, "get_saved_labels", lambda: [])
    classify_job._reuse_saved_label_embeddings(db, "ViT-B-16", str(model_dir), ["bird", "cat", "dog"])
    individual = LabelEmbeddingCache(cache.cache_dir, identity, 512)
    if source_changed:
        assert individual.read("bird") is None
    else:
        np.testing.assert_array_equal(individual.read("bird"), payload[:, 0])
        np.testing.assert_array_equal(individual.read("cat"), payload[:, 1])
    assert individual.read("dog") is None
