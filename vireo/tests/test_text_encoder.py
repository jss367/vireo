"""Tests for text_encoder module."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import pytest


def test_encode_text_returns_normalized_vector(monkeypatch):
    """encode_text returns a unit-length float32 vector."""
    # Mock the heavy ML imports to keep tests fast
    import types
    fake_torch = types.ModuleType("torch")
    fake_torch.no_grad = lambda: type("ctx", (), {"__enter__": lambda s: None, "__exit__": lambda s, *a: None})()

    fake_features = np.random.randn(1, 512).astype(np.float32)
    fake_features = fake_features / np.linalg.norm(fake_features)

    class FakeClassifier:
        def __init__(self, **kw):
            self.model = types.SimpleNamespace()
            self.model.encode_text = lambda txts: types.SimpleNamespace(
                cpu=lambda: types.SimpleNamespace(
                    numpy=lambda: fake_features,
                    __truediv__=lambda s, o: s,
                ),
                float=lambda: types.SimpleNamespace(
                    cpu=lambda: types.SimpleNamespace(numpy=lambda: fake_features)
                ),
            )
            self.tokenizer = lambda texts: texts
            self.device = "cpu"

    monkeypatch.setattr("text_encoder._get_classifier", lambda model_str, pretrained_str: FakeClassifier())

    from text_encoder import encode_text
    result = encode_text("bird in flight", model_str="ViT-B-16", pretrained_str="/fake/path")
    assert isinstance(result, np.ndarray)
    assert result.dtype == np.float32
    assert abs(np.linalg.norm(result) - 1.0) < 1e-5
