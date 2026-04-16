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
