# vireo/tests/test_keypoints.py
"""Tests for animal keypoint detection (eye-focus pipeline).

Keypoint-model inference is mocked at the onnxruntime.InferenceSession level
so these tests run without real model weights.
"""
import json
import os
import sys
from unittest.mock import MagicMock

import numpy as np
from PIL import Image

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_decode_simcc_picks_argmax():
    """decode_simcc returns (K, 3) with x, y in input pixels and min(conf_x, conf_y)."""
    from keypoints import decode_simcc

    K = 17
    simcc_x = np.zeros((1, K, 512), dtype=np.float32)
    simcc_y = np.zeros((1, K, 512), dtype=np.float32)
    # Eye 0 at (100, 50) in a 256x256 input image (simcc split ratio = 2.0).
    simcc_x[0, 0, 200] = 1.0  # x = 200 / 2.0 = 100 px
    simcc_y[0, 0, 100] = 1.0  # y = 100 / 2.0 = 50 px

    kps = decode_simcc(simcc_x, simcc_y, input_size=256, simcc_split_ratio=2.0)

    assert kps.shape == (K, 3)
    np.testing.assert_allclose(kps[0, :2], [100.0, 50.0], atol=0.5)
    assert kps[0, 2] == 1.0  # min(max(simcc_x), max(simcc_y))


def test_ensure_keypoint_weights_short_circuits_if_present(tmp_path, monkeypatch):
    """When both model.onnx and config.json exist, no download is triggered."""
    import keypoints as kp

    monkeypatch.setattr(kp, "MODELS_DIR", str(tmp_path))
    model_dir = tmp_path / "rtmpose-animal"
    model_dir.mkdir()
    (model_dir / "model.onnx").write_bytes(b"fake-onnx")
    (model_dir / "config.json").write_text("{}")

    # If the helper attempted a network download, hf_hub_download is what it
    # would reach for. Asserting no call here proves short-circuit behavior.
    called = {"count": 0}

    def _fail_hub(*args, **kwargs):
        called["count"] += 1
        raise AssertionError("Should not download when weights are present")

    monkeypatch.setattr(
        "huggingface_hub.hf_hub_download", _fail_hub, raising=False
    )

    path = kp.ensure_keypoint_weights("rtmpose-animal")
    assert path == str(model_dir / "model.onnx")
    assert called["count"] == 0


def test_ensure_keypoint_weights_missing_files_triggers_download(tmp_path, monkeypatch):
    """When weights are absent, helper calls hf_hub_download for each required file."""
    import keypoints as kp

    monkeypatch.setattr(kp, "MODELS_DIR", str(tmp_path))

    downloaded = []

    def _fake_hub_download(repo_id, filename, subfolder, **kwargs):
        downloaded.append((repo_id, subfolder, filename))
        # Return a path inside tmp_path acting as HF's cache location.
        cache = tmp_path / "_hfcache" / subfolder
        cache.mkdir(parents=True, exist_ok=True)
        dest = cache / filename
        dest.write_bytes(b"fake" if filename == "model.onnx" else b"{}")
        return str(dest)

    import huggingface_hub

    monkeypatch.setattr(
        huggingface_hub, "hf_hub_download", _fake_hub_download
    )

    onnx_path = kp.ensure_keypoint_weights("rtmpose-animal")

    assert onnx_path == str(tmp_path / "rtmpose-animal" / "model.onnx")
    # Both model.onnx and config.json must be fetched.
    names = {name for _, _, name in downloaded}
    assert names == {"model.onnx", "config.json"}
    # Both files land at the expected on-disk location.
    assert (tmp_path / "rtmpose-animal" / "model.onnx").is_file()
    assert (tmp_path / "rtmpose-animal" / "config.json").is_file()


def _write_rtmpose_config(model_dir):
    (model_dir / "config.json").write_text(json.dumps({
        "input_size": [1, 3, 256, 256],
        "mean": [123.675, 116.28, 103.53],
        "std": [58.395, 57.12, 57.375],
        "keypoints": [
            "left_eye", "right_eye", "nose", "neck", "root_of_tail",
            "left_shoulder", "left_elbow", "left_front_paw",
            "right_shoulder", "right_elbow", "right_front_paw",
            "left_hip", "left_knee", "left_back_paw",
            "right_hip", "right_knee", "right_back_paw",
        ],
        "output_type": "simcc",
        "simcc_split_ratio": 2.0,
    }))


def test_detect_keypoints_returns_named_keypoints_in_image_space(tmp_path, monkeypatch):
    """Eye coord lands correctly after crop + resize + pad + reverse transform."""
    import keypoints as kp

    monkeypatch.setattr(kp, "MODELS_DIR", str(tmp_path))
    model_dir = tmp_path / "rtmpose-animal"
    model_dir.mkdir()
    (model_dir / "model.onnx").write_bytes(b"fake")
    _write_rtmpose_config(model_dir)

    # Image is 512x512. Bbox covers the whole image, so scale is 256/512=0.5,
    # input crop is the whole image. Simcc peak at index 256 along each axis
    # => (256/2, 256/2) = (128, 128) in the 256x256 input. Inverse: divide by
    # scale (0.5) => (256, 256) in image space.
    K = 17
    simcc_x = np.zeros((1, K, 512), dtype=np.float32)
    simcc_y = np.zeros((1, K, 512), dtype=np.float32)
    simcc_x[0, 0, 256] = 0.9  # left_eye x
    simcc_y[0, 0, 256] = 0.9  # left_eye y

    mock_sess = MagicMock()
    mock_sess.run.return_value = [simcc_x, simcc_y]
    monkeypatch.setattr(kp, "_load_session", lambda name: mock_sess)

    img = Image.new("RGB", (512, 512), color=(128, 128, 128))
    bbox = (0, 0, 512, 512)

    result = kp.detect_keypoints(img, bbox, "rtmpose-animal")

    # Returns one dict per keypoint in the model's order.
    assert len(result) == K
    names = [k["name"] for k in result]
    assert "left_eye" in names
    left = [k for k in result if k["name"] == "left_eye"][0]
    assert abs(left["x"] - 256) < 5
    assert abs(left["y"] - 256) < 5
    assert left["conf"] > 0.85


def test_detect_keypoints_maps_bbox_offset_into_image_space(tmp_path, monkeypatch):
    """When the bbox is offset within the image, result coords include the offset."""
    import keypoints as kp

    monkeypatch.setattr(kp, "MODELS_DIR", str(tmp_path))
    model_dir = tmp_path / "rtmpose-animal"
    model_dir.mkdir()
    (model_dir / "model.onnx").write_bytes(b"fake")
    _write_rtmpose_config(model_dir)

    # 600x400 image, bbox (100, 50, 356, 306) — a 256x256 square offset by
    # (100, 50). Scale = 256 / 256 = 1.0. Simcc peak at index 100 => input
    # coord (50, 50). Inverse: + bbox origin => image coord (150, 100).
    K = 17
    simcc_x = np.zeros((1, K, 512), dtype=np.float32)
    simcc_y = np.zeros((1, K, 512), dtype=np.float32)
    simcc_x[0, 0, 100] = 0.75
    simcc_y[0, 0, 100] = 0.75

    mock_sess = MagicMock()
    mock_sess.run.return_value = [simcc_x, simcc_y]
    monkeypatch.setattr(kp, "_load_session", lambda name: mock_sess)

    img = Image.new("RGB", (600, 400), color=(200, 200, 200))
    bbox = (100, 50, 356, 306)

    result = kp.detect_keypoints(img, bbox, "rtmpose-animal")
    left = [k for k in result if k["name"] == "left_eye"][0]

    assert abs(left["x"] - 150) < 2
    assert abs(left["y"] - 100) < 2
