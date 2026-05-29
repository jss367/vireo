# vireo/tests/test_detection_id.py
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from detection_id import detection_id, positive_int_hash


def test_positive_int_hash_is_deterministic():
    """Same inputs → same hash, every time."""
    a = positive_int_hash("foo", "bar", "baz")
    b = positive_int_hash("foo", "bar", "baz")
    assert a == b


def test_positive_int_hash_differs_for_different_inputs():
    assert positive_int_hash("foo") != positive_int_hash("bar")
    assert positive_int_hash("a", "b") != positive_int_hash("ab")  # separator matters
    assert positive_int_hash("a", "b") != positive_int_hash("b", "a")


def test_positive_int_hash_fits_in_js_safe_integer():
    """52-bit width: every value must be ≤ Number.MAX_SAFE_INTEGER (2^53 - 1).

    Detection IDs ride through JSON to the frontend; values above the JS
    safe integer would silently lose precision on JSON.parse round-trip.
    """
    JS_MAX_SAFE = (1 << 53) - 1  # 9007199254740991
    for parts in [
        ("photo:1", "megadetector-v6", "0.1234", "0.5678", "0.0100", "0.0200", "animal"),
        ("photo:99999999", "x" * 200, "0.9999", "0.9999", "0.9999", "0.9999", "person"),
        ("", "", "", "", "", "", ""),
    ]:
        h = positive_int_hash(*parts)
        assert 0 <= h <= JS_MAX_SAFE, f"{h} exceeds JS safe integer"
        # 52-bit cap: high bit (2^52) and above must be zero.
        assert h < (1 << 52), f"{h} exceeds 52 bits"


def test_detection_id_stable_for_same_box():
    """Same photo, model, box, category → same id."""
    box = (0.1, 0.2, 0.3, 0.4)
    a = detection_id(42, "megadetector-v6", box, "animal")
    b = detection_id(42, "megadetector-v6", box, "animal")
    assert a == b


def test_detection_id_absorbs_sub_quarter_pixel_drift():
    """ONNX float drift between providers is well below 1e-4.

    Two runs producing boxes that differ in the 5th decimal place should
    collapse to the same detection id. Without quantization, identical
    detections from two pipelines would hash to different rows and the
    UPSERT property breaks.
    """
    a = detection_id(42, "megadetector-v6", (0.10001, 0.20001, 0.30001, 0.40001), "animal")
    b = detection_id(42, "megadetector-v6", (0.10002, 0.20002, 0.30002, 0.40002), "animal")
    assert a == b


def test_detection_id_distinguishes_4th_decimal_changes():
    """4th-decimal differences (~0.4 px on 4K image) are real, distinct boxes."""
    a = detection_id(42, "megadetector-v6", (0.1000, 0.2000, 0.3000, 0.4000), "animal")
    b = detection_id(42, "megadetector-v6", (0.1001, 0.2000, 0.3000, 0.4000), "animal")
    assert a != b


def test_detection_id_distinguishes_photo():
    box = (0.1, 0.2, 0.3, 0.4)
    assert detection_id(1, "megadetector-v6", box, "animal") != \
           detection_id(2, "megadetector-v6", box, "animal")


def test_detection_id_distinguishes_detector_model():
    box = (0.1, 0.2, 0.3, 0.4)
    assert detection_id(42, "megadetector-v6", box, "animal") != \
           detection_id(42, "megadetector-v7", box, "animal")


def test_detection_id_distinguishes_category():
    """category is part of model output — different categories on the same
    box are different detections, so DELETE+INSERT retires the stale one.
    """
    box = (0.1, 0.2, 0.3, 0.4)
    assert detection_id(42, "megadetector-v6", box, "animal") != \
           detection_id(42, "megadetector-v6", box, "person")


def test_detection_id_ignores_confidence_drift():
    """confidence is NOT part of the natural key — it drifts between runs."""
    # detection_id signature takes only (photo_id, model, box, category)
    # so confidence isn't even an argument; this test pins the contract.
    import inspect
    sig = inspect.signature(detection_id)
    assert "confidence" not in sig.parameters
    assert "detector_confidence" not in sig.parameters
