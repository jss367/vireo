"""Animal keypoint detection via ONNX Runtime.

Top-down keypoint models take a cropped image (MegaDetector bbox) and return
per-keypoint (x, y, conf) in image-pixel coordinates. Used to localize the
animal's eye for eye-focus scoring.

Models:
    rtmpose-animal        — RTMPose-s on AP-10K, integration spike.
    superanimal-quadruped — DLC 3.x, production mammals.
    superanimal-bird      — DLC 3.x, production birds.

All models load from ~/.vireo/models/<name>/model.onnx with a sibling
config.json describing input size, normalization, and keypoint names.
"""
import logging
import os

import numpy as np

log = logging.getLogger(__name__)

MODELS_DIR = os.path.expanduser("~/.vireo/models")

_sessions = {}
_locks = {}
_download_locks = {}


def decode_simcc(simcc_x, simcc_y, input_size, simcc_split_ratio=2.0):
    """Decode RTMPose simcc-format outputs to (K, 3) keypoints.

    RTMPose emits two 1-D classification maps per keypoint (x and y axes);
    each axis is argmaxed independently and the per-keypoint confidence is
    the minimum of the two peak activations.

    Args:
        simcc_x: ndarray of shape (1, K, size_x).
        simcc_y: ndarray of shape (1, K, size_y).
        input_size: input image side length in pixels (e.g., 256).
        simcc_split_ratio: simcc coordinate scale factor (default 2.0).

    Returns:
        ndarray of shape (K, 3) with columns (x, y, conf) in input-image
        pixel space.
    """
    sx = simcc_x[0]  # (K, size_x)
    sy = simcc_y[0]  # (K, size_y)
    idx_x = np.argmax(sx, axis=1)
    idx_y = np.argmax(sy, axis=1)
    conf_x = sx[np.arange(sx.shape[0]), idx_x]
    conf_y = sy[np.arange(sy.shape[0]), idx_y]
    conf = np.minimum(conf_x, conf_y)
    x = idx_x / simcc_split_ratio
    y = idx_y / simcc_split_ratio
    return np.stack([x, y, conf], axis=1).astype(np.float32)
