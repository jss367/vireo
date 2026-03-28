"""Tests for text_encoder module."""
import os
import sys

import pytest

pytest.importorskip("torch")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np


def _make_fake_classifier(fake_features):
    """Build a FakeClassifier that returns fake_features from encode_text."""
    import types

    class _TokenResult:
        def __init__(self, texts):
            self.texts = texts
        def to(self, device):
            return self

    class FakeClassifier:
        def __init__(self):
            self.model = types.SimpleNamespace()
            self.model.encode_text = lambda txts: types.SimpleNamespace(
                float=lambda: types.SimpleNamespace(
                    cpu=lambda: types.SimpleNamespace(numpy=lambda: fake_features)
                ),
            )
            self.tokenizer = lambda texts: _TokenResult(texts)
            self.device = "cpu"

    return FakeClassifier()


def test_encode_text_returns_normalized_vector(monkeypatch):
    """encode_text returns a unit-length float32 vector."""
    fake_features = np.random.randn(1, 512).astype(np.float32)
    fake_features = fake_features / np.linalg.norm(fake_features)

    monkeypatch.setattr(
        "text_encoder._get_classifier",
        lambda model_str, pretrained_str: _make_fake_classifier(fake_features),
    )

    from text_encoder import encode_text
    result = encode_text("bird in flight", model_str="ViT-B-16", pretrained_str="/fake/path")
    assert isinstance(result, np.ndarray)
    assert result.dtype == np.float32
    assert abs(np.linalg.norm(result) - 1.0) < 1e-5


def test_encode_text_zero_vector(monkeypatch):
    """encode_text handles zero vector without crashing."""
    fake_features = np.zeros((1, 512), dtype=np.float32)

    monkeypatch.setattr(
        "text_encoder._get_classifier",
        lambda model_str, pretrained_str: _make_fake_classifier(fake_features),
    )

    from text_encoder import encode_text
    result = encode_text("nothing", model_str="ViT-B-16", pretrained_str="/fake/path")
    assert isinstance(result, np.ndarray)
    assert np.linalg.norm(result) == 0.0
