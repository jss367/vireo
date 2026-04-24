"""Shared ONNX Runtime utilities for model inference.

Provides ONNX session creation with automatic hardware provider selection,
image preprocessing, and common post-processing operations.
"""

import contextlib
import logging
import os

import numpy as np
from PIL import Image

log = logging.getLogger(__name__)


# Substrings that identify onnxruntime load failures rooted in the file
# bytes themselves (corrupt protobuf, truncated graph, missing external
# data sidecar). Seeing one of these in an exception message means the
# on-disk model is unusable and a fresh download is the right remedy.
# Non-matching failures (permission denied, is-a-directory, out-of-memory,
# CUDA init errors) must NOT trigger self-heal.
_CORRUPT_MODEL_MARKERS = (
    "invalid_protobuf",
    "protobuf parsing failed",
    "load model from",
    "model_path must not be empty",
    "external data",
    "failed to load model",
    "no graph",
    "invalid model",
    "invalid_graph",
)


def _looks_like_corrupt_model(err):
    """Return True when an exception from create_session looks like an
    on-disk corruption signal (as opposed to an OS / environment error).

    Purely a string-matching heuristic against the onnxruntime message.
    OSError subclasses (PermissionError, IsADirectoryError, FileNotFoundError)
    are explicitly excluded — those are environment issues, not corruption,
    and blowing away the file would be actively harmful.
    """
    if isinstance(err, OSError):
        return False
    msg = str(err).lower()
    return any(marker in msg for marker in _CORRUPT_MODEL_MARKERS)


def _sibling_paths_to_purge(model_path):
    """Return the set of paths that must be removed alongside ``model_path``
    when self-healing a corrupt model.

    For ONNX graphs that use external data the .onnx file references a
    companion .onnx.data sidecar; purging both ensures the redownload
    starts from a clean slate and no stale bytes from an aborted earlier
    download can survive.
    """
    paths = [model_path]
    sidecar = model_path + ".data"
    if os.path.exists(sidecar):
        paths.append(sidecar)
    return paths


def create_session_with_self_heal(model_path, redownload=None):
    """Load an ONNX session, self-healing on corrupt / truncated model files.

    Wraps :func:`create_session` so that a load failure rooted in the
    on-disk bytes (corrupt protobuf, truncated graph, missing external
    data sidecar) triggers a single recovery attempt: delete the broken
    files, invoke the caller-supplied ``redownload`` callable, then retry
    session creation exactly once. On the second failure raise a
    user-facing :class:`RuntimeError` chained to the underlying
    onnxruntime error — never loop.

    Non-corruption errors (``PermissionError``, ``IsADirectoryError``,
    out-of-memory, CUDA init failures) are re-raised unchanged so the
    user or caller can react appropriately. We never delete the file in
    that path — the bytes are almost certainly fine.

    Args:
        model_path: absolute path to the .onnx file.
        redownload: optional zero-argument callable that replaces the
            removed file(s) with a fresh copy. If ``None``, the wrapper
            has no recovery strategy and re-raises the original error
            without touching the filesystem.

    Returns:
        An ``onnxruntime.InferenceSession`` for ``model_path``.
    """
    try:
        return create_session(model_path)
    except Exception as first_err:
        if not _looks_like_corrupt_model(first_err):
            raise
        if redownload is None:
            # Caller has no recovery strategy (e.g. custom user-supplied
            # model with no known download source). Re-raise the original
            # error so the user isn't silently losing their file.
            raise

        log.warning(
            "ONNX model %s failed to load, looks like corruption: %s. "
            "Deleting on-disk files and triggering redownload.",
            model_path, first_err,
        )

        # Delete the graph and any external-data sidecar BEFORE invoking
        # redownload so a resumable downloader can't mistake the corrupt
        # stub for a partial download to pick up from.
        for path in _sibling_paths_to_purge(model_path):
            with contextlib.suppress(OSError):
                os.unlink(path)
                log.info("Self-heal: removed %s", path)

        try:
            redownload()
        except Exception as redl_err:
            # Download itself failed (network, disk full, HF API down).
            # Re-raise with context so the caller sees both errors.
            raise RuntimeError(
                f"Self-heal of {model_path} failed: redownload raised "
                f"{type(redl_err).__name__}: {redl_err}"
            ) from redl_err

        try:
            return create_session(model_path)
        except Exception as second_err:
            # A second failure means the fresh download is also unusable,
            # or the root cause wasn't actually on-disk corruption. Do
            # NOT loop — surface a clear message chained to the original
            # error so logs show both.
            raise RuntimeError(
                f"Model at {model_path} still failed to load after "
                f"self-heal redownload. Original error: {first_err}. "
                f"Retry error: {second_err}. "
                "Open Settings → Models and click Repair, or check "
                "~/.vireo/vireo.log for details."
            ) from second_err


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
    import os

    import onnxruntime as ort

    providers = get_providers()

    # onnxruntime 1.24+ CoreMLExecutionProvider crashes when loading models
    # that use external data (.onnx.data sidecar files).  Fall back to the
    # remaining providers for these models.
    if str(model_path).endswith(".onnx") and os.path.exists(str(model_path) + ".data"):
        before = list(providers)
        providers = [p for p in providers if p != "CoreMLExecutionProvider"]
        if providers != before:
            log.info(
                "Model %s uses external data (.onnx.data); "
                "excluding CoreMLExecutionProvider to avoid crash",
                model_path,
            )

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
