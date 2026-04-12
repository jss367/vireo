"""Tests for ONNX Runtime utility module."""
import numpy as np


def test_get_providers_returns_list():
    from vireo.onnx_runtime import get_providers
    providers = get_providers()
    assert isinstance(providers, list)
    assert "CPUExecutionProvider" in providers


def test_get_providers_cpu_always_last():
    from vireo.onnx_runtime import get_providers
    providers = get_providers()
    assert providers[-1] == "CPUExecutionProvider"


def test_preprocess_image_shape():
    from PIL import Image

    from vireo.onnx_runtime import preprocess_image
    img = Image.new("RGB", (800, 600))
    arr = preprocess_image(img, size=(224, 224), mean=[0.5, 0.5, 0.5], std=[0.5, 0.5, 0.5])
    assert arr.shape == (1, 3, 224, 224)
    assert arr.dtype == np.float32


def test_preprocess_image_normalization():
    from PIL import Image

    from vireo.onnx_runtime import preprocess_image
    # All-white image (255, 255, 255) -> after /255 = 1.0 -> after norm with mean=0.5, std=0.5 -> 1.0
    img = Image.new("RGB", (10, 10), (255, 255, 255))
    arr = preprocess_image(img, size=(10, 10), mean=[0.5, 0.5, 0.5], std=[0.5, 0.5, 0.5])
    np.testing.assert_allclose(arr[0, 0, 0, 0], 1.0, atol=0.01)


def test_preprocess_image_center_crop():
    from PIL import Image

    from vireo.onnx_runtime import preprocess_image
    # Non-square image should be center-cropped
    img = Image.new("RGB", (300, 100))
    arr = preprocess_image(img, size=(50, 50), mean=[0.5, 0.5, 0.5], std=[0.5, 0.5, 0.5], center_crop=True)
    assert arr.shape == (1, 3, 50, 50)


def test_softmax():
    from vireo.onnx_runtime import softmax
    logits = np.array([1.0, 2.0, 3.0])
    probs = softmax(logits)
    assert abs(probs.sum() - 1.0) < 1e-6
    assert probs[2] > probs[1] > probs[0]


def test_nms_basic():
    from vireo.onnx_runtime import nms
    # Two overlapping boxes, one with higher score
    boxes = np.array([
        [10, 10, 50, 50],
        [12, 12, 52, 52],  # overlaps heavily with first
        [100, 100, 150, 150],  # separate
    ], dtype=np.float32)
    scores = np.array([0.9, 0.8, 0.7], dtype=np.float32)
    keep = nms(boxes, scores, iou_threshold=0.5)
    assert 0 in keep  # highest score kept
    assert 2 in keep  # non-overlapping kept
    assert 1 not in keep  # suppressed


def test_create_session_excludes_coreml_for_external_data(tmp_path):
    """CoreML should be excluded when a .onnx.data sidecar exists."""
    from unittest.mock import MagicMock, patch

    model_file = tmp_path / "model.onnx"
    sidecar = tmp_path / "model.onnx.data"
    model_file.write_bytes(b"fake")
    sidecar.write_bytes(b"fake")

    mock_session = MagicMock()
    mock_session.get_providers.return_value = ["CPUExecutionProvider"]

    with patch("vireo.onnx_runtime.get_providers", return_value=[
        "CoreMLExecutionProvider", "CPUExecutionProvider",
    ]), patch("onnxruntime.InferenceSession", return_value=mock_session) as mock_cls:
        from vireo.onnx_runtime import create_session
        create_session(str(model_file))

    # CoreML must have been stripped from the providers list
    _args, kwargs = mock_cls.call_args
    assert "CoreMLExecutionProvider" not in kwargs.get("providers", _args[1] if len(_args) > 1 else [])


def test_create_session_keeps_coreml_without_sidecar(tmp_path):
    """CoreML should be kept when there is no .onnx.data sidecar."""
    from unittest.mock import MagicMock, patch

    model_file = tmp_path / "model.onnx"
    model_file.write_bytes(b"fake")
    # No sidecar created

    mock_session = MagicMock()
    mock_session.get_providers.return_value = [
        "CoreMLExecutionProvider", "CPUExecutionProvider",
    ]

    with patch("vireo.onnx_runtime.get_providers", return_value=[
        "CoreMLExecutionProvider", "CPUExecutionProvider",
    ]), patch("onnxruntime.InferenceSession", return_value=mock_session) as mock_cls:
        from vireo.onnx_runtime import create_session
        create_session(str(model_file))

    # CoreML must still be present
    _args, kwargs = mock_cls.call_args
    providers_used = kwargs.get("providers", _args[1] if len(_args) > 1 else [])
    assert "CoreMLExecutionProvider" in providers_used
    assert "CPUExecutionProvider" in providers_used
