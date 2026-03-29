# vireo/tests/test_detector.py
"""Tests for MegaDetector ONNX-based detection.

Tests verify that the ONNX-based detector loads correctly and that
preprocessing, postprocessing, and the public API work as expected.
"""
import os
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_megadetector_onnx_session_loads():
    """MegaDetector ONNX session must load when model file exists."""
    try:
        import onnxruntime  # noqa: F401
    except ImportError:
        pytest.skip("onnxruntime not installed")

    import detector

    if not os.path.exists(detector.MEGADETECTOR_ONNX_PATH):
        pytest.skip("MegaDetector ONNX model not downloaded")

    # Reset singleton so we actually test loading
    detector._session = None

    try:
        session = detector._get_session()
        assert session is not None
    finally:
        detector._session = None


def test_megadetector_onnx_missing_raises():
    """_get_session() must raise RuntimeError when model file is missing."""
    try:
        import onnxruntime  # noqa: F401
    except ImportError:
        pytest.skip("onnxruntime not installed")

    import detector

    # Point to a non-existent path
    original_path = detector.MEGADETECTOR_ONNX_PATH
    detector.MEGADETECTOR_ONNX_PATH = "/tmp/nonexistent/model.onnx"
    detector._session = None

    try:
        with pytest.raises(RuntimeError, match="MegaDetector ONNX model not found"):
            detector._get_session()
    finally:
        detector.MEGADETECTOR_ONNX_PATH = original_path
        detector._session = None


def test_preprocess_output_shape():
    """_preprocess must return correct tensor shape and preprocess info."""
    import detector

    # Create a test image array (100x200x3)
    img_array = np.random.randint(0, 255, (100, 200, 3), dtype=np.uint8)

    tensor, info = detector._preprocess(img_array)

    assert tensor.shape == (1, 3, 640, 640)
    assert tensor.dtype == np.float32
    assert tensor.min() >= 0.0
    assert tensor.max() <= 1.0

    scale, pad_x, pad_y, orig_w, orig_h = info
    assert orig_w == 200
    assert orig_h == 100
    assert scale > 0


def test_preprocess_preserves_aspect_ratio():
    """Letterbox preprocessing must preserve aspect ratio."""
    import detector

    # Wide image
    img_array = np.random.randint(0, 255, (100, 400, 3), dtype=np.uint8)
    tensor, (scale, pad_x, pad_y, orig_w, orig_h) = detector._preprocess(img_array)

    # Scale should be limited by the wider dimension
    expected_scale = 640 / 400
    assert abs(scale - expected_scale) < 0.01

    # Vertical padding should be present (image is wider than tall)
    assert pad_y > 0


def test_postprocess_empty_output():
    """_postprocess must return empty list when no detections pass threshold."""
    import detector

    # Simulated output with very low confidence
    output = np.zeros((1, 5, 7), dtype=np.float32)
    preprocess_info = (1.0, 0, 0, 640, 640)
    result = detector._postprocess([output], preprocess_info, 0.5)
    assert result == []


def test_postprocess_with_detections():
    """_postprocess must correctly convert ONNX output to detection dicts."""
    import detector

    # Simulate (1, N, 8) format: [x1, y1, x2, y2, obj_conf, cls0, cls1, cls2]
    # One detection with high confidence animal
    output = np.array(
        [
            [
                [100, 100, 300, 300, 0.9, 0.95, 0.02, 0.03],
            ]
        ],
        dtype=np.float32,
    )
    preprocess_info = (1.0, 0, 0, 640, 640)
    result = detector._postprocess([output], preprocess_info, 0.2)

    assert len(result) == 1
    det = result[0]
    assert "box" in det
    assert "confidence" in det
    assert "category" in det
    assert det["category"] == "animal"
    assert det["confidence"] > 0.5
    assert all(k in det["box"] for k in ("x", "y", "w", "h"))


def test_postprocess_cxcywh_format():
    """_postprocess must handle cx/cy/w/h format (no objectness score)."""
    import detector

    # Simulate (1, N, 7) format: [cx, cy, w, h, cls0, cls1, cls2]
    # This is 7 columns which hits the num_cols >= 7 branch differently,
    # but let's test the 5-6 column format with (1, N, 6)
    # Actually, 7 columns hits the >= 7 branch. Let's test exactly the
    # >= 5 branch by using 5 columns: [cx, cy, w, h, cls0]
    output = np.array(
        [
            [
                [320, 320, 200, 200, 0.9],
            ]
        ],
        dtype=np.float32,
    )
    preprocess_info = (1.0, 0, 0, 640, 640)
    result = detector._postprocess([output], preprocess_info, 0.2)

    assert len(result) == 1
    det = result[0]
    assert det["category"] == "animal"  # class 0
    assert det["confidence"] > 0.5


def test_postprocess_transposed_format():
    """_postprocess must handle (1, C, N) transposed output format."""
    import detector

    # (1, 8, 2) where 8 < 2 is False, so this should NOT be transposed
    # Let's use (1, 8, 100) where 8 < 100 is True -> will be transposed
    output = np.zeros((1, 8, 100), dtype=np.float32)
    # Put one detection in column 0
    output[0, 0, 0] = 100  # x1
    output[0, 1, 0] = 100  # y1
    output[0, 2, 0] = 300  # x2
    output[0, 3, 0] = 300  # y2
    output[0, 4, 0] = 0.9  # obj_conf
    output[0, 5, 0] = 0.95  # cls0 (animal)
    output[0, 6, 0] = 0.01  # cls1 (person)
    output[0, 7, 0] = 0.01  # cls2 (vehicle)

    preprocess_info = (1.0, 0, 0, 640, 640)
    result = detector._postprocess([output], preprocess_info, 0.2)

    assert len(result) >= 1
    assert result[0]["category"] == "animal"


def test_get_primary_detection():
    """get_primary_detection must return highest-confidence animal."""
    from detector import get_primary_detection

    detections = [
        {"box": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}, "confidence": 0.6, "category": "animal"},
        {"box": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}, "confidence": 0.9, "category": "animal"},
        {"box": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}, "confidence": 0.95, "category": "person"},
    ]

    primary = get_primary_detection(detections)
    assert primary is not None
    assert primary["confidence"] == 0.9
    assert primary["category"] == "animal"


def test_get_primary_detection_no_animals():
    """get_primary_detection must return None when no animals found."""
    from detector import get_primary_detection

    detections = [
        {"box": {"x": 0, "y": 0, "w": 0.5, "h": 0.5}, "confidence": 0.9, "category": "person"},
    ]
    assert get_primary_detection(detections) is None
    assert get_primary_detection([]) is None


def test_megadetector_onnx_model_file_valid():
    """Verify MegaDetector ONNX model file exists and is valid."""
    try:
        import onnxruntime  # noqa: F401
    except ImportError:
        pytest.skip("onnxruntime not installed")

    import detector

    if not os.path.exists(detector.MEGADETECTOR_ONNX_PATH):
        pytest.skip("MegaDetector ONNX model not downloaded")

    # Check file is non-trivially sized (ONNX models are at least a few MB)
    size = os.path.getsize(detector.MEGADETECTOR_ONNX_PATH)
    assert size > 1_000_000, f"Model file too small ({size} bytes), may be corrupted"
