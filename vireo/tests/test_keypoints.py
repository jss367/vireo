# vireo/tests/test_keypoints.py
"""Tests for animal keypoint detection (eye-focus pipeline).

Keypoint-model inference is mocked at the onnxruntime.InferenceSession level
so these tests run without real model weights.
"""
import os
import sys

import numpy as np

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
