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
import threading

import numpy as np

log = logging.getLogger(__name__)

MODELS_DIR = os.path.expanduser("~/.vireo/models")

_sessions = {}
_locks = {}
_download_locks = {}


def _model_dir(model_name):
    return os.path.join(MODELS_DIR, model_name)


def ensure_keypoint_weights(model_name, progress_callback=None):
    """Ensure ONNX weights for ``model_name`` are present on disk.

    Returns the path to ``model.onnx`` if both that file and ``config.json``
    already exist under ``MODELS_DIR/<model_name>/``. Otherwise downloads
    both from the ``jss367/vireo-onnx-models`` HuggingFace repo.

    Args:
        model_name: one of 'rtmpose-animal', 'superanimal-quadruped',
            'superanimal-bird'.
        progress_callback: optional callable(phase: str, current: int, total: int)
            invoked before download and after completion.

    Raises:
        RuntimeError: if the download fails; callers should abort or surface
            the error to the user rather than silently running without weights.
    """
    target = _model_dir(model_name)
    onnx_path = os.path.join(target, "model.onnx")
    config_path = os.path.join(target, "config.json")

    if os.path.isfile(onnx_path) and os.path.isfile(config_path):
        return onnx_path

    # Serialize concurrent first-run downloads per-model so two parallel jobs
    # don't both fetch the same weights. Locks are created lazily; the outer
    # setdefault is itself thread-safe for the dict insert.
    lock = _download_locks.setdefault(model_name, threading.Lock())
    with lock:
        if os.path.isfile(onnx_path) and os.path.isfile(config_path):
            return onnx_path

        os.makedirs(target, exist_ok=True)
        if progress_callback:
            progress_callback(
                f"Downloading {model_name} (first run only)...", 0, 1
            )
        log.info("%s weights missing — downloading from Hugging Face", model_name)

        try:
            import shutil

            import huggingface_hub
            from models import ONNX_REPO

            for filename, final_path in (
                ("model.onnx", onnx_path),
                ("config.json", config_path),
            ):
                cached = huggingface_hub.hf_hub_download(
                    repo_id=ONNX_REPO,
                    filename=filename,
                    subfolder=model_name,
                )
                # hf_hub_download returns its cache path; copy into the
                # model dir so the inference code can find it consistently.
                tmp = final_path + ".download"
                shutil.copy2(cached, tmp)
                os.replace(tmp, final_path)
        except Exception as e:
            raise RuntimeError(
                f"Failed to download {model_name} weights: {e}. "
                "Check your network connection and retry, or download manually "
                "from the pipeline models page."
            ) from e

        if not (os.path.isfile(onnx_path) and os.path.isfile(config_path)):
            raise RuntimeError(
                f"{model_name} download completed but files are missing "
                f"under {target}."
            )

        size_mb = round(os.path.getsize(onnx_path) / 1024 / 1024, 1)
        log.info("%s weights downloaded (%s MB)", model_name, size_mb)
        if progress_callback:
            progress_callback(f"{model_name} ready ({size_mb} MB)", 1, 1)

    return onnx_path


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
