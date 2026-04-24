"""Wildlife detection using MegaDetector via ONNX Runtime.

Provides bounding boxes around animals in photos for quality scoring.
"""

import logging
import os
import threading

import numpy as np

log = logging.getLogger(__name__)

_session = None
_lock = threading.Lock()
_download_lock = threading.Lock()

# MegaDetector ONNX model path — downloaded to ~/.vireo/models/megadetector-v6/
MEGADETECTOR_ONNX_DIR = os.path.expanduser("~/.vireo/models/megadetector-v6")
MEGADETECTOR_ONNX_PATH = os.path.join(MEGADETECTOR_ONNX_DIR, "model.onnx")

# MegaDetector input size
INPUT_SIZE = 640

# MegaDetector class mapping (index -> label)
CLASS_NAMES = {0: "animal", 1: "person", 2: "vehicle"}

# Raw-confidence hard floor. Every detection at or above this value is stored;
# the user-visible threshold is applied as a read-time filter from the
# workspace-effective config. Filtering at write time would defeat the global
# detection cache — two workspaces with different thresholds over the same
# photo would otherwise need separate detector runs.
RAW_CONF_FLOOR = 0.01


def ensure_megadetector_weights(progress_callback=None):
    """Ensure MegaDetector V6 ONNX weights are present on disk.

    Returns the weights path if already downloaded. Otherwise downloads from
    Hugging Face and copies into MEGADETECTOR_ONNX_DIR. Raises RuntimeError
    on failure so callers can abort rather than silently run without detection.

    Args:
        progress_callback: optional callable(phase: str, current: int, total: int)
            invoked before the download starts and after it completes.
    """
    if os.path.isfile(MEGADETECTOR_ONNX_PATH):
        return MEGADETECTOR_ONNX_PATH

    # Serialize concurrent first-run downloads. Without the lock, two parallel
    # jobs would both start a ~300 MB download; without the atomic replace
    # below, a second caller could also observe a half-copied file at the
    # final path and try to load it as ONNX.
    with _download_lock:
        if os.path.isfile(MEGADETECTOR_ONNX_PATH):
            return MEGADETECTOR_ONNX_PATH

        os.makedirs(MEGADETECTOR_ONNX_DIR, exist_ok=True)

        if progress_callback:
            progress_callback(
                "Downloading MegaDetector V6 (~300 MB, first run only)...", 0, 1
            )
        log.info("MegaDetector weights missing — downloading from Hugging Face")

        tmp_path = MEGADETECTOR_ONNX_PATH + ".download"
        try:
            import shutil

            from huggingface_hub import hf_hub_download
            from models import ONNX_REPO

            cached_path = hf_hub_download(
                repo_id=ONNX_REPO,
                filename="model.onnx",
                subfolder="megadetector-v6",
            )
            # Copy to a sibling temp path then atomically replace so other
            # threads only ever observe either the old (missing) state or a
            # fully written weights file — never a partial copy.
            shutil.copy2(cached_path, tmp_path)
            os.replace(tmp_path, MEGADETECTOR_ONNX_PATH)
        except Exception as e:
            import contextlib

            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise RuntimeError(
                f"Failed to download MegaDetector V6 weights: {e}. "
                "Check your network connection and retry, or download manually "
                "from the pipeline models page."
            ) from e

        if not os.path.isfile(MEGADETECTOR_ONNX_PATH):
            raise RuntimeError(
                "MegaDetector download completed but weights file is missing at "
                f"{MEGADETECTOR_ONNX_PATH}."
            )

        size_mb = round(os.path.getsize(MEGADETECTOR_ONNX_PATH) / 1024 / 1024, 1)
        log.info("MegaDetector weights downloaded (%s MB)", size_mb)
        if progress_callback:
            progress_callback(
                f"MegaDetector V6 ready ({size_mb} MB)", 1, 1
            )

        return MEGADETECTOR_ONNX_PATH


def _get_session():
    """Load MegaDetector ONNX session (cached singleton).

    Uses double-checked locking to ensure only one thread creates the
    session, even when multiple threads call this concurrently.
    """
    global _session
    if _session is not None:
        return _session

    with _lock:
        if _session is None:
            if not os.path.exists(MEGADETECTOR_ONNX_PATH):
                raise RuntimeError(
                    f"MegaDetector ONNX model not found at {MEGADETECTOR_ONNX_PATH}. "
                    "Download it from the Models page in Settings."
                )

            from onnx_runtime import create_session

            _session = create_session(MEGADETECTOR_ONNX_PATH)
            log.info("MegaDetector ONNX model loaded")

    return _session


def _preprocess(image_array):
    """Preprocess image for MegaDetector ONNX input.

    Uses letterbox resize: scale to fit INPUT_SIZE while preserving
    aspect ratio, then center-pad to a square.

    Args:
        image_array: numpy RGB array (H, W, 3) uint8

    Returns:
        (input_tensor, preprocess_info) where:
            input_tensor: numpy float32 array (1, 3, 640, 640)
            preprocess_info: tuple (scale, pad_x, pad_y, orig_w, orig_h)
    """
    h, w = image_array.shape[:2]

    # Letterbox resize: scale to fit INPUT_SIZE, pad to square
    scale = min(INPUT_SIZE / h, INPUT_SIZE / w)
    new_w = int(w * scale + 0.5)
    new_h = int(h * scale + 0.5)

    from PIL import Image

    img = Image.fromarray(image_array).resize((new_w, new_h), Image.BILINEAR)

    # Pad to INPUT_SIZE x INPUT_SIZE (center padding with gray=114)
    pad_x = (INPUT_SIZE - new_w) // 2
    pad_y = (INPUT_SIZE - new_h) // 2

    padded = np.full((INPUT_SIZE, INPUT_SIZE, 3), 114, dtype=np.uint8)
    padded[pad_y : pad_y + new_h, pad_x : pad_x + new_w] = np.array(img)

    # Normalize to 0-1, HWC -> CHW, add batch dim
    arr = padded.astype(np.float32) / 255.0
    arr = arr.transpose(2, 0, 1)[np.newaxis, ...]

    return arr, (scale, pad_x, pad_y, w, h)


def _postprocess(outputs, preprocess_info, confidence_threshold):
    """Post-process ONNX model outputs to detection list.

    Supports two YOLO output layouts (after squeezing the batch dim and
    transposing to (N, C) where N = number of anchor proposals):

    - C == 4 + num_classes (YOLOv8/v9 — no objectness):
        [cx, cy, w, h, cls0, cls1, ...]
        For MegaDetector V6 (YOLOv9c, 3 classes) this is C == 7.
    - C == 5 + num_classes (legacy YOLOv5/v7 with objectness):
        [x1, y1, x2, y2, obj_conf, cls0, cls1, ...]
        With 3 classes that's C == 8.

    The two layouts are distinguished by an exact column count keyed off
    ``CLASS_NAMES``. Earlier code keyed on ``num_cols >= 7``, which sent
    the YOLOv9 7-column output down the with-objectness path and silently
    corrupted both the confidence (animal × max(person, vehicle) ≈ 0)
    and the category labels (off-by-one) on every MegaDetector V6
    detection.

    Args:
        outputs: list of numpy arrays from ONNX session.run()
        preprocess_info: tuple from _preprocess (scale, pad_x, pad_y,
            orig_w, orig_h)
        confidence_threshold: minimum confidence for a detection

    Returns:
        list of detection dicts with keys: box, confidence, category
    """
    from onnx_runtime import nms

    scale, pad_x, pad_y, orig_w, orig_h = preprocess_info
    output = outputs[0]  # primary output tensor

    # Handle different output shapes
    if output.ndim == 3:
        output = output[0]  # remove batch dim -> (N, C) or (C, N)
        # Detect transposed (C, N) format: feature count C is small
        # (typically 7 or 8 for MegaDetector: 4 box + [1 obj] + 3
        # classes) while N is large (thousands of proposals). Transpose
        # when the first dim looks like a feature count (5-20 range) and
        # is smaller than the second dim (detection count).
        n_rows, n_cols = output.shape
        if n_rows < n_cols and 5 <= n_rows <= 20:
            output = output.T  # transpose (C, N) -> (N, C)

    num_cols = output.shape[1]
    num_classes = len(CLASS_NAMES)

    if num_cols == 4 + num_classes:
        # YOLOv8/v9 layout: [cx, cy, w, h, cls0, cls1, ...]
        cx, cy, bw, bh = output[:, 0], output[:, 1], output[:, 2], output[:, 3]
        boxes_raw = np.stack(
            [cx - bw / 2, cy - bh / 2, cx + bw / 2, cy + bh / 2], axis=1
        )
        class_scores = output[:, 4:]
        class_ids = class_scores.argmax(axis=1)
        confidences = class_scores[np.arange(len(class_scores)), class_ids]
    elif num_cols == 5 + num_classes:
        # Legacy YOLOv5/v7 layout with objectness:
        # [x1, y1, x2, y2, obj_conf, cls0, cls1, ...]
        boxes_raw = output[:, :4]
        obj_conf = output[:, 4]
        class_scores = output[:, 5:]
        class_ids = class_scores.argmax(axis=1)
        confidences = obj_conf * class_scores[np.arange(len(class_scores)), class_ids]
    else:
        log.warning(
            "Unexpected ONNX output shape: %s (expected %d or %d columns "
            "for %d classes)",
            output.shape, 4 + num_classes, 5 + num_classes, num_classes,
        )
        return []

    # Filter by confidence
    mask = confidences >= confidence_threshold
    boxes_raw = boxes_raw[mask]
    confidences = confidences[mask]
    class_ids = class_ids[mask]

    if len(boxes_raw) == 0:
        return []

    # NMS
    keep = nms(boxes_raw, confidences, iou_threshold=0.45)
    boxes_raw = boxes_raw[keep]
    confidences = confidences[keep]
    class_ids = class_ids[keep]

    # Convert from padded 640x640 coords back to normalized 0-1
    detections = []
    for i in range(len(boxes_raw)):
        x1, y1, x2, y2 = boxes_raw[i]
        # Remove padding and undo scale
        x1 = (x1 - pad_x) / scale
        y1 = (y1 - pad_y) / scale
        x2 = (x2 - pad_x) / scale
        y2 = (y2 - pad_y) / scale
        # Clip to image bounds and normalize to 0-1
        x1 = max(0, x1) / orig_w
        y1 = max(0, y1) / orig_h
        x2 = max(0, min(orig_w, x2)) / orig_w
        y2 = max(0, min(orig_h, y2)) / orig_h

        # Drop invalid boxes where width or height would be non-positive
        if x2 <= x1 or y2 <= y1:
            continue

        category = CLASS_NAMES.get(int(class_ids[i]), "animal")
        detections.append(
            {
                "box": {
                    "x": float(x1),
                    "y": float(y1),
                    "w": float(x2 - x1),
                    "h": float(y2 - y1),
                },
                "confidence": float(confidences[i]),
                "category": category,
            }
        )

    return detections


def detect_animals(image_path):
    """Detect animals in an image using MegaDetector.

    Returns every detection above ``RAW_CONF_FLOOR``. The user-visible
    confidence threshold is applied as a read-time filter from the
    workspace-effective config — don't filter at write time or we can't
    globally cache detector output across workspaces with different
    thresholds.

    Args:
        image_path: path to the image file

    Returns:
        list of detections, each with:
            box: {x, y, w, h} normalized 0-1
            confidence: float 0-1
            category: str ('animal', 'person', 'vehicle')

        ``[]`` means "ran successfully, no boxes above the raw floor"
        (a real empty scene). ``None`` means "the run itself failed"
        (image decode error, ONNX error, etc.) — callers should NOT
        cache a zero-box result for this case.
    """
    session = _get_session()

    try:
        # Load image ourselves using image_loader which supports RAW formats
        # (NEF, CR2, ARW, etc.).
        from image_loader import load_image

        img = load_image(str(image_path), max_size=1280)
        if img is None:
            log.warning("Could not load image for detection: %s", image_path)
            return None
        img_array = np.array(img.convert("RGB"))

        input_tensor, preprocess_info = _preprocess(img_array)

        input_name = session.get_inputs()[0].name
        outputs = session.run(None, {input_name: input_tensor})

        return _postprocess(outputs, preprocess_info, RAW_CONF_FLOOR)
    except Exception:
        log.warning("Detection failed for %s", image_path, exc_info=True)
        return None


def get_primary_detection(detections):
    """Get the highest-confidence animal detection from a list.

    Returns:
        detection dict or None
    """
    animals = [d for d in detections if d["category"] == "animal"]
    if not animals:
        return None
    return max(animals, key=lambda d: d["confidence"])
