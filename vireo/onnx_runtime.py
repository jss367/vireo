"""Shared ONNX Runtime utilities for model inference.

Provides session creation with automatic hardware provider selection,
image preprocessing, and common post-processing operations.
"""

import logging

import numpy as np
from PIL import Image

log = logging.getLogger(__name__)


def get_providers():
    """Return ONNX Runtime execution providers in priority order.

    Checks which providers are actually available in the installed
    onnxruntime package and returns them in preference order:
    CoreML (Apple) > CUDA (NVIDIA) > CPU (fallback).
    """
    import onnxruntime as ort

    available = set(ort.get_available_providers())
    providers = []
    for p in ["CoreMLExecutionProvider", "CUDAExecutionProvider"]:
        if p in available:
            providers.append(p)
    providers.append("CPUExecutionProvider")
    return providers


def create_session(model_path):
    """Create an ONNX Runtime InferenceSession with best available provider.

    Args:
        model_path: path to .onnx file

    Returns:
        ort.InferenceSession
    """
    import onnxruntime as ort

    providers = get_providers()
    log.info("Loading ONNX model: %s (providers: %s)", model_path, providers)
    session = ort.InferenceSession(model_path, providers=providers)
    actual = session.get_providers()
    log.info("ONNX session using: %s", actual)
    return session


def preprocess_image(image, size, mean, std, center_crop=False):
    """Preprocess a PIL Image for ONNX model input.

    Args:
        image: PIL Image
        size: (height, width) tuple
        mean: per-channel mean for normalization (list of 3 floats)
        std: per-channel std for normalization (list of 3 floats)
        center_crop: if True, resize so shortest edge matches then center crop

    Returns:
        numpy float32 array of shape (1, 3, H, W)
    """
    img = image.convert("RGB")

    if center_crop:
        # Resize so shortest edge = target, then center crop
        target_h, target_w = size
        w, h = img.size
        scale = max(target_h / h, target_w / w)
        new_w = int(w * scale + 0.5)
        new_h = int(h * scale + 0.5)
        img = img.resize((new_w, new_h), Image.BICUBIC)
        left = (new_w - target_w) // 2
        top = (new_h - target_h) // 2
        img = img.crop((left, top, left + target_w, top + target_h))
    else:
        target_h, target_w = size
        img = img.resize((target_w, target_h), Image.BICUBIC)

    arr = np.array(img, dtype=np.float32) / 255.0
    mean = np.array(mean, dtype=np.float32).reshape(1, 1, 3)
    std = np.array(std, dtype=np.float32).reshape(1, 1, 3)
    arr = (arr - mean) / std
    arr = arr.transpose(2, 0, 1)  # HWC -> CHW
    return arr[np.newaxis, ...]  # add batch dim


def softmax(logits, axis=-1):
    """Compute softmax probabilities from logits.

    Args:
        logits: numpy array of raw model outputs
        axis: axis along which to compute softmax

    Returns:
        numpy array of probabilities (same shape as input)
    """
    e = np.exp(logits - np.max(logits, axis=axis, keepdims=True))
    return e / e.sum(axis=axis, keepdims=True)


def nms(boxes, scores, iou_threshold=0.5):
    """Non-maximum suppression on bounding boxes.

    Args:
        boxes: numpy array (N, 4) in [x1, y1, x2, y2] format
        scores: numpy array (N,) of confidence scores
        iou_threshold: IoU threshold for suppression

    Returns:
        list of indices to keep
    """
    if len(boxes) == 0:
        return []

    x1 = boxes[:, 0]
    y1 = boxes[:, 1]
    x2 = boxes[:, 2]
    y2 = boxes[:, 3]
    areas = (x2 - x1) * (y2 - y1)

    order = scores.argsort()[::-1]
    keep = []

    while len(order) > 0:
        i = order[0]
        keep.append(i)
        if len(order) == 1:
            break

        xx1 = np.maximum(x1[i], x1[order[1:]])
        yy1 = np.maximum(y1[i], y1[order[1:]])
        xx2 = np.minimum(x2[i], x2[order[1:]])
        yy2 = np.minimum(y2[i], y2[order[1:]])

        inter = np.maximum(0, xx2 - xx1) * np.maximum(0, yy2 - yy1)
        iou = inter / (areas[i] + areas[order[1:]] - inter + 1e-6)

        remaining = np.where(iou <= iou_threshold)[0]
        order = order[remaining + 1]

    return keep
