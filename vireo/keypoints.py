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
import json
import logging
import os
import threading

import numpy as np

log = logging.getLogger(__name__)

MODELS_DIR = os.path.expanduser("~/.vireo/models")

_sessions = {}
_locks = {}
_download_locks = {}

# Per-model download state for the pipeline page card. Values:
#   "idle"         — never requested, or previous download was acknowledged.
#   "downloading"  — a background thread is actively fetching.
#   "failed"       — most recent attempt raised; weights not on disk.
_download_state = {}


def _model_dir(model_name):
    return os.path.join(MODELS_DIR, model_name)


def weights_status(model_name):
    """Return the current download/readiness state for ``model_name``.

    Priority order:
        1. Files on disk → "ready"
        2. Background thread in flight → "downloading"
        3. Previous failure without files on disk → "failed"
        4. Otherwise → "missing"
    """
    target = _model_dir(model_name)
    onnx_path = os.path.join(target, "model.onnx")
    config_path = os.path.join(target, "config.json")
    if os.path.isfile(onnx_path) and os.path.isfile(config_path):
        return "ready"
    state = _download_state.get(model_name, "idle")
    if state == "downloading":
        return "downloading"
    if state == "failed":
        return "failed"
    return "missing"


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


def decode_heatmaps(heatmaps, input_size):
    """Decode (1, K, H', W') heatmaps to (K, 3) keypoints in input-image pixels.

    Simple argmax + rescale. SuperAnimal heatmap-head models use this path;
    RTMPose goes through decode_simcc instead. Subpixel refinement (quadratic
    fit around argmax) is deferred until real-image tests show the need.
    """
    hm = heatmaps[0]  # (K, H', W')
    K, H, W = hm.shape
    flat = hm.reshape(K, -1)
    idx = np.argmax(flat, axis=1)
    ys = (idx // W).astype(np.float32)
    xs = (idx % W).astype(np.float32)
    conf = flat[np.arange(K), idx]
    xs *= input_size / W
    ys *= input_size / H
    return np.stack([xs, ys, conf], axis=1).astype(np.float32)


def _load_config(model_name):
    """Read config.json sibling to model.onnx."""
    config_path = os.path.join(_model_dir(model_name), "config.json")
    with open(config_path) as f:
        return json.load(f)


def _load_session(model_name):
    """Get the cached onnxruntime session for ``model_name``.

    Loads on first use; subsequent calls return the same session. Raises
    FileNotFoundError if weights are absent — callers should have invoked
    ensure_keypoint_weights() first.
    """
    if model_name in _sessions:
        return _sessions[model_name]
    lock = _locks.setdefault(model_name, threading.Lock())
    with lock:
        if model_name in _sessions:
            return _sessions[model_name]
        import onnxruntime as ort

        onnx_path = os.path.join(_model_dir(model_name), "model.onnx")
        if not os.path.isfile(onnx_path):
            raise FileNotFoundError(
                f"Keypoint model {model_name!r} not found at {onnx_path}. "
                "Call ensure_keypoint_weights() first."
            )
        _sessions[model_name] = ort.InferenceSession(
            onnx_path, providers=["CPUExecutionProvider"]
        )
    return _sessions[model_name]


def detect_keypoints(image, bbox, model_name):
    """Run a keypoint model on the bbox crop; return per-keypoint (x, y, conf).

    Applies aspect-ratio-preserving resize + top-left pad to the model's
    input size, normalizes per the model config, and maps the detected
    input-space keypoints back into the original image's pixel space.

    Args:
        image: PIL.Image (original-resolution RGB).
        bbox: (x0, y0, x1, y1) in image-pixel space (MegaDetector output).
        model_name: one of 'rtmpose-animal', 'superanimal-quadruped',
            'superanimal-bird'.

    Returns:
        list of dicts ``{"name": str, "x": float, "y": float, "conf": float}``,
        one entry per keypoint the model produces. Coordinates are in the
        original image's pixel space.
    """
    from PIL import Image

    cfg = _load_config(model_name)
    input_h = cfg["input_size"][2]
    input_w = cfg["input_size"][3]
    mean = np.array(cfg["mean"], dtype=np.float32).reshape(3, 1, 1)
    std = np.array(cfg["std"], dtype=np.float32).reshape(3, 1, 1)
    keypoint_names = cfg["keypoints"]

    x0, y0, x1, y1 = bbox
    crop = image.crop((x0, y0, x1, y1))
    crop_w, crop_h = crop.size
    scale = min(input_w / crop_w, input_h / crop_h)
    new_w = max(1, int(round(crop_w * scale)))
    new_h = max(1, int(round(crop_h * scale)))
    resized = crop.resize((new_w, new_h), Image.BILINEAR)

    # Top-left aligned pad. Using a constant pad offset (0, 0) keeps the
    # inverse transform trivial: input-space coords map directly through
    # `scale` back to crop-space.
    padded = Image.new("RGB", (input_w, input_h), color=(0, 0, 0))
    padded.paste(resized, (0, 0))

    arr = np.array(padded, dtype=np.float32).transpose(2, 0, 1)  # CHW
    arr = (arr - mean) / std
    arr = arr[np.newaxis, :, :, :]

    session = _load_session(model_name)
    outputs = session.run(None, {"pixel_values": arr})

    output_type = cfg.get("output_type", "heatmap")
    if output_type == "simcc":
        simcc_x, simcc_y = outputs
        kps_input_space = decode_simcc(
            simcc_x,
            simcc_y,
            input_size=input_w,
            simcc_split_ratio=cfg.get("simcc_split_ratio", 2.0),
        )
    else:
        kps_input_space = decode_heatmaps(outputs[0], input_size=input_w)

    result = []
    for i, name in enumerate(keypoint_names):
        x_in, y_in, conf = kps_input_space[i]
        # Inverse-map: input-space → crop-space → image-space.
        x_img = float(x_in / scale) + x0
        y_img = float(y_in / scale) + y0
        result.append({
            "name": name,
            "x": float(x_img),
            "y": float(y_img),
            "conf": float(conf),
        })
    return result
