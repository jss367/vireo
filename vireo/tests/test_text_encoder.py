"""Tests for text_encoder module -- uses mocked ONNX sessions."""
import os
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _make_fake_text_session(fake_features):
    """Build a fake ONNX text encoder session that returns fake_features."""
    session = MagicMock()
    mock_input = MagicMock()
    mock_input.name = "input_ids"
    session.get_inputs.return_value = [mock_input]

    def fake_run(output_names, input_dict):
        return [fake_features]

    session.run = fake_run
    return session


def _make_fake_tokenizer():
    """Create a mock tokenizer that returns fake token IDs."""
    tokenizer = MagicMock()

    class FakeEncoding:
        def __init__(self):
            self.ids = list(range(10))

    tokenizer.encode.return_value = FakeEncoding()
    return tokenizer


def test_encode_text_returns_normalized_vector(monkeypatch):
    """encode_text returns a unit-length float32 vector."""
    fake_features = np.random.randn(1, 512).astype(np.float32)

    fake_session = _make_fake_text_session(fake_features)
    fake_tokenizer = _make_fake_tokenizer()

    # Clear the session cache so our mock gets used
    monkeypatch.setattr("text_encoder._session_cache", {})
    monkeypatch.setattr(
        "text_encoder._get_text_session",
        lambda model_str, pretrained_str=None: (
            fake_session,
            "input_ids",
            fake_tokenizer,
        ),
    )

    from text_encoder import encode_text

    result = encode_text(
        "bird in flight", model_str="ViT-B-16", pretrained_str="/fake/path"
    )
    assert isinstance(result, np.ndarray)
    assert result.dtype == np.float32
    assert abs(np.linalg.norm(result) - 1.0) < 1e-5


def test_encode_text_zero_vector(monkeypatch):
    """encode_text handles zero vector without crashing."""
    fake_features = np.zeros((1, 512), dtype=np.float32)

    fake_session = _make_fake_text_session(fake_features)
    fake_tokenizer = _make_fake_tokenizer()

    monkeypatch.setattr("text_encoder._session_cache", {})
    monkeypatch.setattr(
        "text_encoder._get_text_session",
        lambda model_str, pretrained_str=None: (
            fake_session,
            "input_ids",
            fake_tokenizer,
        ),
    )

    from text_encoder import encode_text

    result = encode_text(
        "nothing", model_str="ViT-B-16", pretrained_str="/fake/path"
    )
    assert isinstance(result, np.ndarray)
    assert np.linalg.norm(result) == 0.0


def test_encode_text_caching(monkeypatch, tmp_path):
    """_get_text_session caches by model directory."""
    from text_encoder import _get_text_session

    # Clear cache
    monkeypatch.setattr("text_encoder._session_cache", {})

    fake_features = np.random.randn(1, 512).astype(np.float32)
    fake_session = _make_fake_text_session(fake_features)
    fake_tokenizer = _make_fake_tokenizer()

    # Create a fake model directory
    model_dir = tmp_path / "bioclip-vit-b-16"
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "text_encoder.onnx").write_text("dummy")
    (model_dir / "tokenizer.json").write_text("dummy")

    # Mock the tokenizers module so Tokenizer.from_file returns our fake
    import types

    mock_tokenizers = types.ModuleType("tokenizers")
    mock_tokenizers.Tokenizer = MagicMock()
    mock_tokenizers.Tokenizer.from_file = MagicMock(return_value=fake_tokenizer)

    with (
        patch("text_encoder._MODELS_ROOT", str(tmp_path)),
        patch(
            "text_encoder.onnx_runtime.create_session",
            return_value=fake_session,
        ),
        patch.dict("sys.modules", {"tokenizers": mock_tokenizers}),
    ):
        result1 = _get_text_session("ViT-B-16")
        result2 = _get_text_session("ViT-B-16")

    # Same object returned (cached)
    assert result1 is result2


def test_unknown_model_raises(monkeypatch):
    """_get_text_session raises ValueError for unknown model."""
    monkeypatch.setattr("text_encoder._session_cache", {})

    from text_encoder import _get_text_session

    with pytest.raises(ValueError, match="Unknown BioCLIP model"):
        _get_text_session("unknown-model")
