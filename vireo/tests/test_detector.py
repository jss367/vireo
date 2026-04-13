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


def test_postprocess_cxcywh_format_yolov9():
    """7-column YOLOv9 output (no objectness) parses as cxcywh + 3 classes.

    MegaDetector V6 (YOLOv9c) produces (1, 7, 8400) output with the
    layout [cx, cy, w, h, animal_score, person_score, vehicle_score].
    A previous version of the parser keyed on ``num_cols >= 7`` and
    sent this through the with-objectness branch, multiplying the
    animal score by max(person, vehicle) — yielding near-zero on bird
    photos and corrupting category labels off-by-one.
    """
    import detector

    # (1, N, 7) — single detection, animal class strong, person/vehicle ~0
    output = np.array(
        [
            [
                [320, 320, 200, 200, 0.9, 0.02, 0.01],
            ]
        ],
        dtype=np.float32,
    )
    preprocess_info = (1.0, 0, 0, 640, 640)
    result = detector._postprocess([output], preprocess_info, 0.2)

    assert len(result) == 1
    det = result[0]
    assert det["category"] == "animal"
    # Confidence must be the raw animal score (0.9), not animal × max(other)
    assert det["confidence"] == pytest.approx(0.9, abs=1e-5)
    # Box must be derived from cxcywh, not interpreted as xyxy
    # cx=320, cy=320, w=200, h=200 → x1=220, y1=220, x2=420, y2=420
    # Normalized to 640: x=220/640=0.34375, w=200/640=0.3125
    assert det["box"]["x"] == pytest.approx(220 / 640, abs=1e-4)
    assert det["box"]["y"] == pytest.approx(220 / 640, abs=1e-4)
    assert det["box"]["w"] == pytest.approx(200 / 640, abs=1e-4)
    assert det["box"]["h"] == pytest.approx(200 / 640, abs=1e-4)


def test_postprocess_yolov9_category_labels_correct():
    """7-column YOLOv9 output assigns categories from the class index in
    cols 4-6, not via the off-by-one shifted lookup the old parser did."""
    import detector

    # Three detections, one strong in each class
    output = np.array(
        [
            [
                [100, 100, 50, 50, 0.9, 0.0, 0.0],   # animal
                [200, 200, 50, 50, 0.0, 0.85, 0.0],  # person
                [300, 300, 50, 50, 0.0, 0.0, 0.8],   # vehicle
            ]
        ],
        dtype=np.float32,
    )
    preprocess_info = (1.0, 0, 0, 640, 640)
    result = detector._postprocess([output], preprocess_info, 0.2)

    cats = sorted(d["category"] for d in result)
    assert cats == ["animal", "person", "vehicle"]


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


def test_ensure_weights_noop_when_present(tmp_path, monkeypatch):
    """ensure_megadetector_weights() returns path without downloading when file exists."""
    import detector

    fake_path = tmp_path / "model.onnx"
    fake_path.write_bytes(b"x" * 1024)

    monkeypatch.setattr(detector, "MEGADETECTOR_ONNX_DIR", str(tmp_path))
    monkeypatch.setattr(detector, "MEGADETECTOR_ONNX_PATH", str(fake_path))

    download_calls = []

    def fake_hf_hub_download(**kwargs):
        download_calls.append(kwargs)
        return str(fake_path)

    import sys
    import types

    fake_hf = types.ModuleType("huggingface_hub")
    fake_hf.hf_hub_download = fake_hf_hub_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)

    progress_calls = []
    result = detector.ensure_megadetector_weights(
        progress_callback=lambda p, c, t: progress_calls.append((p, c, t)),
    )

    assert result == str(fake_path)
    assert download_calls == []
    assert progress_calls == []


def test_ensure_weights_downloads_when_missing(tmp_path, monkeypatch):
    """ensure_megadetector_weights() downloads and copies file when missing; invokes progress callback."""
    import detector

    dest_dir = tmp_path / "megadetector-v6"
    dest_path = dest_dir / "model.onnx"
    monkeypatch.setattr(detector, "MEGADETECTOR_ONNX_DIR", str(dest_dir))
    monkeypatch.setattr(detector, "MEGADETECTOR_ONNX_PATH", str(dest_path))

    # Simulate HF cache returning a file at a different path (which forces a copy).
    cache_path = tmp_path / "hf-cache" / "model.onnx"
    cache_path.parent.mkdir()
    cache_path.write_bytes(b"m" * 2048)

    def fake_hf_hub_download(**kwargs):
        assert kwargs["filename"] == "model.onnx"
        assert kwargs["subfolder"] == "megadetector-v6"
        return str(cache_path)

    import sys
    import types

    fake_hf = types.ModuleType("huggingface_hub")
    fake_hf.hf_hub_download = fake_hf_hub_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)

    progress_calls = []
    result = detector.ensure_megadetector_weights(
        progress_callback=lambda p, c, t: progress_calls.append((p, c, t)),
    )

    assert result == str(dest_path)
    assert dest_path.is_file()
    assert dest_path.read_bytes() == b"m" * 2048
    assert len(progress_calls) >= 2
    assert progress_calls[0][1] == 0 and progress_calls[0][2] == 1
    assert progress_calls[-1][1] == 1 and progress_calls[-1][2] == 1


def test_ensure_weights_raises_on_download_failure(tmp_path, monkeypatch):
    """ensure_megadetector_weights() raises RuntimeError with remediation hint when download fails."""
    import detector

    dest_dir = tmp_path / "megadetector-v6"
    dest_path = dest_dir / "model.onnx"
    monkeypatch.setattr(detector, "MEGADETECTOR_ONNX_DIR", str(dest_dir))
    monkeypatch.setattr(detector, "MEGADETECTOR_ONNX_PATH", str(dest_path))

    def fake_hf_hub_download(**kwargs):
        raise ConnectionError("network unreachable")

    import sys
    import types

    fake_hf = types.ModuleType("huggingface_hub")
    fake_hf.hf_hub_download = fake_hf_hub_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)

    with pytest.raises(RuntimeError, match="Failed to download MegaDetector"):
        detector.ensure_megadetector_weights()

    assert not dest_path.exists()


def test_ensure_weights_atomic_and_serialized(tmp_path, monkeypatch):
    """Concurrent callers download once and never observe a partial file
    at the final path. Guards against the copy/copy race Codex flagged."""
    import threading

    import detector

    dest_dir = tmp_path / "megadetector-v6"
    dest_path = dest_dir / "model.onnx"
    monkeypatch.setattr(detector, "MEGADETECTOR_ONNX_DIR", str(dest_dir))
    monkeypatch.setattr(detector, "MEGADETECTOR_ONNX_PATH", str(dest_path))

    cache_path = tmp_path / "hf-cache" / "model.onnx"
    cache_path.parent.mkdir()
    cache_path.write_bytes(b"m" * 4096)

    call_count = 0
    observed_partial = False
    download_started = threading.Event()

    def fake_hf_hub_download(**kwargs):
        nonlocal call_count
        call_count += 1
        # Release the second thread so it can race the in-progress download.
        download_started.set()
        return str(cache_path)

    import sys
    import types

    fake_hf = types.ModuleType("huggingface_hub")
    fake_hf.hf_hub_download = fake_hf_hub_download
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_hf)

    def run():
        detector.ensure_megadetector_weights()

    def observer():
        # Wait until thread-A is inside the download block, then repeatedly
        # probe the final path. With atomic replace + lock it must either
        # not exist, or be the full 4096 bytes — never anything in between.
        nonlocal observed_partial
        download_started.wait(timeout=2.0)
        for _ in range(100):
            if dest_path.is_file():
                size = dest_path.stat().st_size
                if 0 < size < 4096:
                    observed_partial = True
                    return

    t1 = threading.Thread(target=run)
    t2 = threading.Thread(target=observer)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Second caller enters after t1 finished, so exactly one real download occurred.
    detector.ensure_megadetector_weights()
    assert call_count == 1
    assert dest_path.stat().st_size == 4096
    assert not observed_partial


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
