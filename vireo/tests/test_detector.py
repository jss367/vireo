# vireo/tests/test_detector.py
"""Tests for MegaDetector ONNX-based detection.

Tests verify that the ONNX-based detector loads correctly and that
preprocessing, postprocessing, and the public API work as expected.
"""
import os
import sys

import numpy as np
import pytest
from testing.waits import synchronization_timeout

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_detect_animals_uses_hard_floor(monkeypatch):
    """detect_animals no longer takes a confidence_threshold param; it uses 0.01 floor."""
    from detector import RAW_CONF_FLOOR
    assert RAW_CONF_FLOOR == 0.01

    # Signature regression: should not accept confidence_threshold kwarg
    import inspect

    from detector import detect_animals
    sig = inspect.signature(detect_animals)
    assert "confidence_threshold" not in sig.parameters


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


def test_tiled_windows_cover_center_and_edges_without_duplicates():
    import detector

    windows = list(detector._tile_windows(1000, 500))

    assert len(windows) == 9
    assert windows[0] == (200, 0, 800, 300)
    assert (0, 0, 600, 300) in windows
    assert (400, 200, 1000, 500) in windows
    # Windows 2..5 are the four corner crops. Windows 6..8 are the three
    # remaining edge-center crops that fill the internal seams the corner
    # crops leave behind under _map_tile_detection's clipping rejection.
    assert set(windows[2:6]) == {
        (0, 0, 600, 300),
        (400, 0, 1000, 300),
        (0, 200, 600, 500),
        (400, 200, 1000, 500),
    }
    assert set(windows[6:]) == {
        (0, 100, 600, 400),
        (400, 100, 1000, 400),
        (200, 200, 800, 500),
    }
    assert len(set(windows)) == len(windows)


def test_every_tile_window_is_the_sole_interior_window_for_some_subject():
    """No window in the grid is redundant, so no crop subset is seam-safe.

    The sibling tests above pin the crop *count*; this one pins the reason
    the count cannot be lowered. Pixel-area coverage is not usable coverage
    because ``_map_tile_detection`` discards any proposal clipped by an
    internal crop boundary, so a subject only counts as covered by a window
    that holds it as an interior box. For each of the nine windows there is
    a subject that survives in that window and nowhere else -- which is why
    an early stop after any prefix of the grid silently drops subjects, and
    why gating one on the found detection's distance from a seam would not
    help either (a strong hit says nothing about a different animal
    elsewhere in the frame).
    """
    import detector

    width, height = 1000, 500
    windows = list(detector._tile_windows(width, height))

    # Bands interior to exactly one of the three start positions per axis.
    x_bands = {0: (0.05, 0.15), 200: (0.35, 0.65), 400: (0.85, 0.95)}
    y_bands = {0: (0.05, 0.15), 100: (0.35, 0.65), 200: (0.85, 0.95)}

    for target in windows:
        left, top = target[0], target[1]
        x1, x2 = x_bands[left]
        y1, y2 = y_bands[top]

        survivors = []
        for window in windows:
            w_left, w_top, w_right, w_bottom = window
            tile_w = w_right - w_left
            tile_h = w_bottom - w_top
            # What the model can see in this crop: the subject clipped to
            # the crop's own bounds.
            tx1 = max(0.0, (x1 * width - w_left) / tile_w)
            tx2 = min(1.0, (x2 * width - w_left) / tile_w)
            ty1 = max(0.0, (y1 * height - w_top) / tile_h)
            ty2 = min(1.0, (y2 * height - w_top) / tile_h)
            if tx2 <= tx1 or ty2 <= ty1:
                continue  # subject lies entirely outside this crop
            proposal = {
                "box": {"x": tx1, "y": ty1, "w": tx2 - tx1, "h": ty2 - ty1},
                "confidence": 0.5,
                "category": "animal",
            }
            if detector._map_tile_detection(
                proposal, window, width, height,
            ) is not None:
                survivors.append(window)

        assert survivors == [target], (
            f"subject x={x1}-{x2} y={y1}-{y2} survives in {survivors}; "
            f"window {target} must be the only one that keeps it"
        )


def test_map_tile_detection_rejects_internal_seams_and_maps_interior_box():
    import detector

    window = (200, 0, 800, 300)
    seam_box = {
        "box": {"x": 0.0, "y": 0.2, "w": 0.3, "h": 0.4},
        "confidence": 0.9,
        "category": "animal",
    }
    assert detector._map_tile_detection(seam_box, window, 1000, 500) is None

    interior = {
        "box": {"x": 0.25, "y": 0.2, "w": 0.5, "h": 0.4},
        "confidence": 0.8,
        "category": "animal",
    }
    mapped = detector._map_tile_detection(interior, window, 1000, 500)
    assert mapped is not None
    assert mapped["box"]["x"] == pytest.approx(0.35)
    assert mapped["box"]["y"] == pytest.approx(0.12)
    assert mapped["box"]["w"] == pytest.approx(0.30)
    assert mapped["box"]["h"] == pytest.approx(0.24)


def test_merge_detections_does_not_truncate_full_frame_subjects():
    import detector

    full_frame = [
        {
            "box": {"x": i / 100, "y": 0.1, "w": 0.005, "h": 0.01},
            "confidence": 0.01 + i / 1000,
            "category": "animal",
        }
        for i in range(21)
    ]
    tiled = [{
        "box": {"x": 0.8, "y": 0.8, "w": 0.1, "h": 0.1},
        "confidence": 0.9,
        "category": "animal",
    }]

    merged = detector._merge_detections(full_frame, tiled)

    assert len(merged) == 22
    assert all(detection in merged for detection in full_frame)
    assert tiled[0] in merged


def test_merge_detections_upgrades_overlapping_full_frame_subject():
    import detector

    weak = {
        "box": {"x": 0.2, "y": 0.2, "w": 0.1, "h": 0.1},
        "confidence": 0.05,
        "category": "animal",
    }
    strong = {
        "box": {"x": 0.2, "y": 0.2, "w": 0.1, "h": 0.1},
        "confidence": 0.9,
        "category": "animal",
    }

    assert detector._merge_detections([weak], [strong]) == [strong]


def test_merge_detections_caps_tiled_additions_per_category():
    import detector

    tiled = [
        {
            "box": {"x": i / 100, "y": 0.1, "w": 0.005, "h": 0.01},
            "confidence": 0.99 - i / 1000,
            "category": "person",
        }
        for i in range(21)
    ]
    animal = {
        "box": {"x": 0.8, "y": 0.8, "w": 0.1, "h": 0.1},
        "confidence": 0.3,
        "category": "animal",
    }
    tiled.append(animal)

    merged = detector._merge_detections([], tiled)

    assert animal in merged
    assert sum(d["category"] == "person" for d in merged) == 20


def test_detect_animals_runs_tiled_fallback_after_weak_full_pass(
    monkeypatch, tmp_path,
):
    from unittest.mock import MagicMock

    import detector

    fake_session = MagicMock()
    fake_session.get_inputs.return_value = [MagicMock(name="images")]
    weak = np.array(
        [[[320, 320, 100, 100, 0.05, 0.0, 0.0]]], dtype=np.float32,
    )
    strong = np.array(
        [[[320, 320, 100, 100, 0.90, 0.0, 0.0]]], dtype=np.float32,
    )
    empty = np.zeros((1, 0, 7), dtype=np.float32)
    outputs = [weak, weak, strong, *([empty] * 8)]
    fake_session.run.side_effect = lambda *_args, **_kwargs: [outputs.pop(0)]
    monkeypatch.setattr(detector, "_get_session", lambda: fake_session)

    img_path = tmp_path / "small-bird.jpg"
    from PIL import Image
    Image.new("RGB", (1000, 500), color="green").save(img_path)

    detections = detector.detect_animals(str(img_path))

    # One full-frame pass plus every seam-safe tile. Even a strong tile
    # detection must not short-circuit the remaining edge-center crops:
    # _map_tile_detection rejects proposals clipped by an internal crop
    # boundary, so the edge-center crops are the only ones that catch a
    # second subject straddling two corner crops' shared seam.
    # Preview full-frame + source-rendition full-frame + all nine tiles.
    assert fake_session.run.call_count == 11
    assert detections[0]["confidence"] == pytest.approx(0.9)
    assert detections[0]["category"] == "animal"
    assert detections[0]["box"]["w"] < 0.2


def test_detect_animals_keeps_running_after_strong_early_tile(
    monkeypatch, tmp_path,
):
    """A strong hit in an early crop must not skip the seam-fixing crops.

    ``_map_tile_detection`` rejects boxes clipped by an internal crop
    boundary, so pixel coverage across the first six center+corner crops is
    not equivalent to usable detection coverage: a second animal that lies
    along an internal seam only appears interior in one of the three
    remaining edge-center crops.  A previous version of ``_tiled_fallback``
    early-stopped after those first six crops and silently dropped such
    subjects; keep the coverage invariant by asserting every crop still runs
    even when the very first tile already returns a strong animal.
    """
    from unittest.mock import MagicMock

    import detector

    fake_session = MagicMock()
    fake_session.get_inputs.return_value = [MagicMock(name="images")]
    # Weak full-frame, then strong (0.90) at the first tile, six empties
    # for the corner/center coverage subset, then a strong (0.90) hit in
    # one of the three edge-center crops.  With the previous early-stop
    # after the sixth crop, ``run.call_count`` would have been 7 and only
    # one tiled subject would have survived.
    weak = np.array(
        [[[320, 320, 100, 100, 0.05, 0.0, 0.0]]], dtype=np.float32,
    )
    strong = np.array(
        [[[320, 320, 50, 50, 0.90, 0.0, 0.0]]], dtype=np.float32,
    )
    empty = np.zeros((1, 0, 7), dtype=np.float32)
    outputs = [weak, weak, strong, *([empty] * 5), strong, empty, empty]
    fake_session.run.side_effect = lambda *_args, **_kwargs: [outputs.pop(0)]
    monkeypatch.setattr(detector, "_get_session", lambda: fake_session)

    img_path = tmp_path / "two-birds.jpg"
    from PIL import Image
    Image.new("RGB", (1000, 500), color="green").save(img_path)

    detections = detector.detect_animals(str(img_path))

    assert fake_session.run.call_count == 11
    strong_animals = [
        d for d in detections
        if d["category"] == "animal" and d["confidence"] >= 0.5
    ]
    assert len(strong_animals) == 2, [d["confidence"] for d in detections]
    # Different windows map to distinct full-image regions, so both hits
    # survive as independent subjects.  A stale early stop would have
    # dropped whichever animal only appeared in a post-sixth window.
    ys = sorted(d["box"]["y"] for d in strong_animals)
    assert ys[1] - ys[0] > 0.1, ys


def _crop_detection_output(crop_w, crop_h, box, confidence):
    """Build a fake YOLOv8 output placing ``box`` (crop-normalized) in a crop.

    Inverts ``_preprocess``'s letterbox so ``_postprocess`` recovers exactly
    the crop-relative box requested.
    """
    import detector

    _, info = detector._preprocess(
        np.zeros((crop_h, crop_w, 3), dtype=np.uint8)
    )
    scale, pad_x, pad_y, _, _ = info
    x1 = box[0] * crop_w * scale + pad_x
    y1 = box[1] * crop_h * scale + pad_y
    x2 = box[2] * crop_w * scale + pad_x
    y2 = box[3] * crop_h * scale + pad_y
    return np.array(
        [[[
            (x1 + x2) / 2, (y1 + y2) / 2, x2 - x1, y2 - y1,
            confidence, 0.0, 0.0,
        ]]],
        dtype=np.float32,
    )


def test_tiled_fallback_recovers_a_seam_straddling_subject_from_the_last_crop(
    monkeypatch, tmp_path,
):
    """The reviewer's exact scenario, end to end through ``detect_animals``.

    The sibling test asserts that all nine crops run. This one asserts the
    consequence that matters: a second animal in the bottom band that
    straddles the vertical center line is clipped by the right seam of the
    bottom-left crop and the left seam of the bottom-right crop, so
    ``_map_tile_detection`` rejects it in every window except the
    bottom-center crop -- which runs last. The test pins that premise
    directly rather than trusting crop ordering, so it fails if an early
    stop is ever reintroduced or if the window schedule stops covering this
    seam.
    """
    from unittest.mock import MagicMock

    import detector

    width, height = 1000, 500
    crop_w, crop_h = 600, 300
    windows = list(detector._tile_windows(width, height))
    bottom_center = (200, 200, 800, 500)
    assert windows[-1] == bottom_center

    # Full-image normalized box for the seam-straddling second animal:
    # wider than the corner crops' shared overlap, low in the frame.
    seam_box = (0.35, 0.85, 0.65, 0.95)

    # Premise: every window except the last rejects or cannot see it.
    for window in windows[:-1]:
        w_left, w_top, w_right, w_bottom = window
        tile_w = w_right - w_left
        tile_h = w_bottom - w_top
        tx1 = max(0.0, (seam_box[0] * width - w_left) / tile_w)
        tx2 = min(1.0, (seam_box[2] * width - w_left) / tile_w)
        ty1 = max(0.0, (seam_box[1] * height - w_top) / tile_h)
        ty2 = min(1.0, (seam_box[3] * height - w_top) / tile_h)
        if tx2 <= tx1 or ty2 <= ty1:
            continue  # outside this crop entirely
        clipped = {
            "box": {"x": tx1, "y": ty1, "w": tx2 - tx1, "h": ty2 - ty1},
            "confidence": 0.55,
            "category": "animal",
        }
        assert detector._map_tile_detection(
            clipped, window, width, height,
        ) is None, f"expected seam rejection in {window}"

    tile_box = (
        (seam_box[0] * width - bottom_center[0]) / crop_w,
        (seam_box[1] * height - bottom_center[1]) / crop_h,
        (seam_box[2] * width - bottom_center[0]) / crop_w,
        (seam_box[3] * height - bottom_center[1]) / crop_h,
    )

    fake_session = MagicMock()
    fake_session.get_inputs.return_value = [MagicMock(name="images")]
    weak = np.array(
        [[[320, 320, 100, 100, 0.05, 0.0, 0.0]]], dtype=np.float32,
    )
    strong = np.array(
        [[[320, 320, 100, 100, 0.90, 0.0, 0.0]]], dtype=np.float32,
    )
    empty = np.zeros((1, 0, 7), dtype=np.float32)
    seam_output = _crop_detection_output(crop_w, crop_h, tile_box, 0.55)
    # Fast-path full frame, the full-frame re-run on the larger array, a
    # strong animal in the very first crop, seven quiet crops, then the seam
    # subject in the final bottom-center crop.
    outputs = [weak, weak, strong, *([empty] * 7), seam_output]
    fake_session.run.side_effect = lambda *_args, **_kwargs: [outputs.pop(0)]
    monkeypatch.setattr(detector, "_get_session", lambda: fake_session)

    img_path = tmp_path / "two-birds.jpg"
    from PIL import Image
    Image.new("RGB", (width, height), color="green").save(img_path)

    detections = detector.detect_animals(str(img_path))

    assert fake_session.run.call_count == 2 + len(windows)
    seam_hits = [
        d for d in detections
        if d["category"] == "animal" and abs(d["confidence"] - 0.55) < 1e-4
    ]
    assert seam_hits, [d["confidence"] for d in detections]
    found = seam_hits[0]["box"]
    assert found["x"] == pytest.approx(seam_box[0], abs=1e-3)
    assert found["y"] == pytest.approx(seam_box[1], abs=1e-3)
    assert found["w"] == pytest.approx(seam_box[2] - seam_box[0], abs=1e-3)
    assert found["h"] == pytest.approx(seam_box[3] - seam_box[1], abs=1e-3)


def test_tiled_fallback_retains_detections_between_raw_floor_and_trigger(
    monkeypatch, tmp_path,
):
    """A crop finding an animal between RAW_CONF_FLOOR and the trigger must be
    kept in the artifact so a workspace with a lower confidence threshold can
    surface it at read time. Filtering at write time would defeat the global
    detector cache — matching the invariant the full-frame pass already
    guarantees via RAW_CONF_FLOOR postprocessing.
    """
    from unittest.mock import MagicMock

    import detector

    assert (
        detector.RAW_CONF_FLOOR
        < 0.15
        < detector.TILED_FALLBACK_TRIGGER_CONFIDENCE
    )

    fake_session = MagicMock()
    fake_session.get_inputs.return_value = [MagicMock(name="images")]
    weak = np.array(
        [[[320, 320, 100, 100, 0.05, 0.0, 0.0]]], dtype=np.float32,
    )
    mid = np.array(
        [[[320, 320, 100, 100, 0.15, 0.0, 0.0]]], dtype=np.float32,
    )
    empty = np.zeros((1, 0, 7), dtype=np.float32)
    outputs = [weak, weak, mid, *([empty] * 8)]
    fake_session.run.side_effect = lambda *_args, **_kwargs: [outputs.pop(0)]
    monkeypatch.setattr(detector, "_get_session", lambda: fake_session)

    img_path = tmp_path / "small-bird.jpg"
    from PIL import Image
    Image.new("RGB", (1000, 500), color="green").save(img_path)

    detections = detector.detect_animals(str(img_path))

    # Every crop plus the full-frame pass ran; the tiled fallback runs
    # every seam-safe window unconditionally so the middle-of-image edge
    # seams are always covered.
    assert fake_session.run.call_count == 11
    animals = [d for d in detections if d["category"] == "animal"]
    mid_boxes = [d for d in animals if abs(d["confidence"] - 0.15) < 1e-4]
    assert mid_boxes, [d["confidence"] for d in animals]


def test_weak_raw_reruns_full_frame_on_tiled_rendition(monkeypatch):
    from unittest.mock import MagicMock

    import detector
    import image_loader
    from PIL import Image

    fake_session = MagicMock()
    fake_session.get_inputs.return_value = [MagicMock(name="images")]
    monkeypatch.setattr(detector, "_get_session", lambda: fake_session)

    preview = Image.new("RGB", (1000, 500), color="green")
    source = Image.new("RGB", (1200, 1200), color="green")
    loads = [preview, source]
    monkeypatch.setattr(image_loader, "load_image", lambda *_args, **_kwargs: loads.pop(0))

    calls = []
    preview_detection = {
        "box": {"x": 0.1, "y": 0.1, "w": 0.1, "h": 0.1},
        "confidence": 0.05,
        "category": "animal",
    }
    source_detection = {
        "box": {"x": 0.2, "y": 0.2, "w": 0.1, "h": 0.1},
        "confidence": 0.06,
        "category": "animal",
    }

    def fake_infer(_session, _input_name, image_array, _floor):
        calls.append(image_array.shape[:2])
        return [preview_detection] if len(calls) == 1 else [source_detection]

    monkeypatch.setattr(detector, "_infer_array", fake_infer)
    monkeypatch.setattr(detector, "_tiled_fallback", lambda *_args: [])

    detections = detector.detect_animals("different-renditions.nef")

    assert calls == [(500, 1000), (1200, 1200)]
    assert detections == [source_detection]


def test_detect_animals_skips_tiled_fallback_after_strong_full_pass(
    monkeypatch, tmp_path,
):
    from unittest.mock import MagicMock

    import detector

    fake_session = MagicMock()
    fake_session.get_inputs.return_value = [MagicMock(name="images")]
    strong = np.array(
        [[[320, 320, 100, 100, 0.90, 0.0, 0.0]]], dtype=np.float32,
    )
    fake_session.run.return_value = [strong]
    monkeypatch.setattr(detector, "_get_session", lambda: fake_session)

    img_path = tmp_path / "large-bird.jpg"
    from PIL import Image
    Image.new("RGB", (1000, 500), color="green").save(img_path)

    detections = detector.detect_animals(str(img_path))

    assert fake_session.run.call_count == 1
    assert detections[0]["confidence"] == pytest.approx(0.9)


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
        download_started.wait(timeout=synchronization_timeout(2.0))
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


def test_detect_animals_holds_gpu_lock_for_gpu_session(monkeypatch, tmp_path):
    """``detect_animals`` must take the process-wide GPU semaphore around
    ``session.run`` when the session reports a GPU provider.
    """
    from unittest.mock import MagicMock

    import detector
    import pipeline_locks

    fake_session = MagicMock()
    fake_input = MagicMock()
    fake_input.name = "images"
    fake_session.get_inputs.return_value = [fake_input]
    fake_session.get_providers.return_value = [
        "CUDAExecutionProvider", "CPUExecutionProvider",
    ]

    snapshots = {}

    def fake_run(output_names, feed_dict):
        snapshots["during_run"] = pipeline_locks._GPU_SEMAPHORE._value
        # Return a degenerate empty output that _postprocess can swallow.
        import numpy as np
        return [np.zeros((1, 0, 8), dtype=np.float32)]

    fake_session.run = fake_run
    monkeypatch.setattr(detector, "_get_session", lambda: fake_session)

    # Fake a loadable image so _preprocess succeeds.
    img_path = tmp_path / "stub.jpg"
    from PIL import Image
    Image.new("RGB", (640, 480), color="black").save(img_path)

    baseline = pipeline_locks._GPU_SEMAPHORE._value
    detector.detect_animals(str(img_path))

    assert snapshots["during_run"] == baseline - 1, (
        "GPU lock must be held during session.run for GPU sessions"
    )
    assert pipeline_locks._GPU_SEMAPHORE._value == baseline, (
        "semaphore must be released after detection"
    )


def test_detect_animals_skips_gpu_lock_for_cpu_only_session(monkeypatch, tmp_path):
    """When MegaDetector falls back to CPU (Apple Silicon excludes CoreML
    for external-data ONNX, or CPU-only installs), ``detect_animals``
    must not take the process-wide GPU semaphore — concurrent SAM/DINO
    on the GPU would otherwise stall on detector's CPU work.
    """
    from unittest.mock import MagicMock

    import detector
    import pipeline_locks

    fake_session = MagicMock()
    fake_input = MagicMock()
    fake_input.name = "images"
    fake_session.get_inputs.return_value = [fake_input]
    fake_session.get_providers.return_value = ["CPUExecutionProvider"]

    snapshots = {}

    def fake_run(output_names, feed_dict):
        snapshots["during_run"] = pipeline_locks._GPU_SEMAPHORE._value
        import numpy as np
        return [np.zeros((1, 0, 8), dtype=np.float32)]

    fake_session.run = fake_run
    monkeypatch.setattr(detector, "_get_session", lambda: fake_session)

    img_path = tmp_path / "stub.jpg"
    from PIL import Image
    Image.new("RGB", (640, 480), color="black").save(img_path)

    baseline = pipeline_locks._GPU_SEMAPHORE._value
    detector.detect_animals(str(img_path))

    assert snapshots["during_run"] == baseline, (
        "CPU-only detector session must not take the GPU semaphore"
    )


def test_detect_animals_propagates_resource_wait_cancellation(monkeypatch, tmp_path):
    """Resource cancellation is not reported as an inference failure."""
    import contextlib
    from unittest.mock import MagicMock

    import detector
    import pipeline_locks
    from resource_ledger import ResourceWaitCancelled

    fake_session = MagicMock()
    fake_session.get_inputs.return_value = [MagicMock(name="images")]
    monkeypatch.setattr(detector, "_get_session", lambda: fake_session)

    @contextlib.contextmanager
    def cancelled_lease(_session):
        raise ResourceWaitCancelled("cancelled")
        yield

    monkeypatch.setattr(
        pipeline_locks, "acquire_inference_resources", cancelled_lease,
    )
    img_path = tmp_path / "stub.jpg"
    from PIL import Image
    Image.new("RGB", (640, 480), color="black").save(img_path)

    with pytest.raises(ResourceWaitCancelled):
        detector.detect_animals(str(img_path))
