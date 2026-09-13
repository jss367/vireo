"""Wildlife detection using MegaDetector via ONNX Runtime.

Provides bounding boxes around animals in photos for quality scoring.
"""

import logging
import os
import threading

import numpy as np
from resource_ledger import ResourceWaitCancelled

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

# A single 640px letterboxed pass can shrink a small bird to only a handful of
# useful pixels.  When that pass has no ordinary-confidence animal, retry on a
# small overlapping crop grid.  Keeping these values detector-owned (rather
# than workspace config) makes a cached detector artifact mean the same thing
# in every workspace.  Tiled crops themselves are postprocessed at
# ``RAW_CONF_FLOOR`` (not the trigger threshold) so that a workspace lowering
# its user-visible confidence threshold can still surface small-subject boxes
# that the crop grid found — matching the full-frame pass and preserving the
# read-time-filter contract of the global detection cache.  Every seam-safe
# window is always run: ``_map_tile_detection`` rejects proposals clipped by
# internal crop boundaries, so pixel coverage alone is not enough — a subject
# whose bounding box crosses two corner crops' shared internal seam is only
# recovered by the edge-center crop that contains it as an interior box.  An
# early stop after the corner subset would silently blind those seams.
TILED_FALLBACK_TRIGGER_CONFIDENCE = 0.20
TILED_CROP_FRACTION = 0.60
TILED_SOURCE_MAX_SIZE = 2560
TILED_EDGE_MARGIN = 0.01
TILED_NMS_IOU = 0.45
TILED_MAX_ADDITIONS_PER_CATEGORY = 20


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

    from onnx_runtime import acquire_session_cache_lock

    with acquire_session_cache_lock(
        _lock,
        label="MegaDetector session cache",
    ):
        if _session is None:
            if not os.path.exists(MEGADETECTOR_ONNX_PATH):
                raise RuntimeError(
                    f"MegaDetector ONNX model not found at {MEGADETECTOR_ONNX_PATH}. "
                    "Download it from the Models page in Settings."
                )

            from onnx_runtime import create_session
            from resource_ledger import resolve_resource_pure_cancel_check

            # Pure cancel probe: ``create_session``'s ledger.acquire
            # runs while ``_lock`` is still held; the pause-aware probe
            # would park ``wait_if_paused`` under the cache lock and
            # block every unpaused peer waiting on MegaDetector until
            # Resume.
            #
            # Default provider order on purpose: MegaDetector is the one
            # model Vireo runs where CoreML is a large win. It is a CNN, so
            # CoreML takes essentially the whole graph instead of fragmenting
            # it — 0.016s vs 0.176s per frame on an M3 Max, ~11x. Vireo's
            # transformer models go the other way and are pinned to CPU (see
            # ``masking.py`` and the external-data branch in
            # ``onnx_runtime.create_session``); do not "unify" them.
            _session = create_session(
                MEGADETECTOR_ONNX_PATH,
                cancel_check=resolve_resource_pure_cancel_check(),
            )
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


def _tile_starts(length, crop_length):
    """Return overlapping start positions covering both edges and center."""
    last = max(0, length - crop_length)
    return sorted({0, last // 2, last})


def _tile_windows(width, height):
    """Yield center-first fallback crop windows as ``(x1, y1, x2, y2)``."""
    crop_w = max(1, min(width, round(width * TILED_CROP_FRACTION)))
    crop_h = max(1, min(height, round(height * TILED_CROP_FRACTION)))
    xs = _tile_starts(width, crop_w)
    ys = _tile_starts(height, crop_h)
    center_x = xs[len(xs) // 2]
    center_y = ys[len(ys) // 2]
    windows = [
        (center_x, 0),
        (center_x, center_y),
        (0, 0),
        (xs[-1], 0),
        (0, ys[-1]),
        (xs[-1], ys[-1]),
        (0, center_y),
        (xs[-1], center_y),
        (center_x, ys[-1]),
    ]
    seen = set()
    for x, y in windows:
        window = (x, y, x + crop_w, y + crop_h)
        if window not in seen:
            seen.add(window)
            yield window


def _map_tile_detection(detection, window, full_width, full_height):
    """Map one tile-relative box to the full image, rejecting seam boxes.

    A proposal clipped by an *internal* crop boundary is ambiguous and often
    comes from the artificial tile edge.  Overlap gives the same real subject
    another tile where it is interior, so discard the clipped copy.  Boxes on
    the actual outer image boundary remain valid.
    """
    left, top, right, bottom = window
    tile_w = right - left
    tile_h = bottom - top
    box = detection["box"]
    x1 = float(box["x"])
    y1 = float(box["y"])
    x2 = x1 + float(box["w"])
    y2 = y1 + float(box["h"])
    margin = TILED_EDGE_MARGIN
    if (
        (left > 0 and x1 <= margin)
        or (top > 0 and y1 <= margin)
        or (right < full_width and x2 >= 1.0 - margin)
        or (bottom < full_height and y2 >= 1.0 - margin)
    ):
        return None

    mapped_x1 = (left + x1 * tile_w) / full_width
    mapped_y1 = (top + y1 * tile_h) / full_height
    mapped_x2 = (left + x2 * tile_w) / full_width
    mapped_y2 = (top + y2 * tile_h) / full_height
    return {
        "box": {
            "x": float(mapped_x1),
            "y": float(mapped_y1),
            "w": float(mapped_x2 - mapped_x1),
            "h": float(mapped_y2 - mapped_y1),
        },
        "confidence": float(detection["confidence"]),
        "category": detection.get("category", "animal"),
    }


def _box_iou(first, second):
    """Return intersection-over-union for two normalized detection boxes."""
    first_box = first["box"]
    second_box = second["box"]
    first_right = first_box["x"] + first_box["w"]
    first_bottom = first_box["y"] + first_box["h"]
    second_right = second_box["x"] + second_box["w"]
    second_bottom = second_box["y"] + second_box["h"]
    intersection_w = max(
        0.0,
        min(first_right, second_right) - max(first_box["x"], second_box["x"]),
    )
    intersection_h = max(
        0.0,
        min(first_bottom, second_bottom) - max(first_box["y"], second_box["y"]),
    )
    intersection = intersection_w * intersection_h
    union = (
        first_box["w"] * first_box["h"]
        + second_box["w"] * second_box["h"]
        - intersection
    )
    return intersection / union if union > 0 else 0.0


def _merge_detections(full_frame, tiled):
    """Merge tiled detections without dropping full-frame subjects.

    Tiled NMS removes duplicate crop proposals.  A stronger tiled box may
    replace an overlapping full-frame box for the same subject, while novel
    tiled subjects are capped independently.  The cap must never truncate
    unrelated full-frame detections because those raw boxes are reused by
    workspaces with lower read-time confidence thresholds.
    """
    from onnx_runtime import nms

    tiled_after_nms = []
    categories = sorted({d.get("category", "animal") for d in tiled})
    for category in categories:
        candidates = [
            d for d in tiled
            if d.get("category", "animal") == category
        ]
        boxes = np.asarray([
            [
                d["box"]["x"],
                d["box"]["y"],
                d["box"]["x"] + d["box"]["w"],
                d["box"]["y"] + d["box"]["h"],
            ]
            for d in candidates
        ], dtype=np.float32)
        scores = np.asarray(
            [d["confidence"] for d in candidates], dtype=np.float32,
        )
        tiled_after_nms.extend(
            candidates[index]
            for index in nms(boxes, scores, iou_threshold=TILED_NMS_IOU)
        )
    tiled_after_nms.sort(
        key=lambda d: float(d["confidence"]), reverse=True,
    )

    merged = list(full_frame)
    additions_by_category = {}
    for candidate in tiled_after_nms:
        category = candidate.get("category", "animal")
        overlaps = [
            index
            for index, existing in enumerate(merged)
            if existing.get("category", "animal")
            == candidate.get("category", "animal")
            and _box_iou(existing, candidate) >= TILED_NMS_IOU
        ]
        if overlaps:
            closest = max(
                overlaps,
                key=lambda index: _box_iou(merged[index], candidate),
            )
            if candidate["confidence"] > merged[closest]["confidence"]:
                merged[closest] = candidate
            continue
        additions = additions_by_category.get(category, 0)
        if additions < TILED_MAX_ADDITIONS_PER_CATEGORY:
            merged.append(candidate)
            additions_by_category[category] = additions + 1

    merged.sort(key=lambda d: float(d["confidence"]), reverse=True)
    return merged


def _infer_array(session, input_name, image_array, confidence_threshold):
    """Run one preprocessed image array under the shared inference lease."""
    input_tensor, preprocess_info = _preprocess(image_array)
    from pipeline_locks import acquire_inference_resources

    with acquire_inference_resources(session):
        outputs = session.run(None, {input_name: input_tensor})
    return _postprocess(outputs, preprocess_info, confidence_threshold)


def _tiled_fallback(session, input_name, image_array):
    """Run overlapping high-effective-resolution crops for a weak full pass."""
    height, width = image_array.shape[:2]
    tiled = []
    for window in _tile_windows(width, height):
        left, top, right, bottom = window
        crop = image_array[top:bottom, left:right]
        detections = _infer_array(
            session, input_name, crop, RAW_CONF_FLOOR,
        )
        for detection in detections:
            mapped = _map_tile_detection(
                detection, window, width, height,
            )
            if mapped is not None:
                tiled.append(mapped)
    return tiled


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

        input_name = session.get_inputs()[0].name
        detections = _infer_array(
            session, input_name, img_array, RAW_CONF_FLOOR,
        )
        best_animal = max(
            (
                d["confidence"] for d in detections
                if d.get("category", "animal") == "animal"
            ),
            default=0.0,
        )
        if best_animal < TILED_FALLBACK_TRIGGER_CONFIDENCE:
            # Reload only weak photos at a larger source size. Cropping the
            # already-downsampled 1280px fast-path image merely enlarges the
            # same pixels and does not recover fine bird detail.
            tiled_img = load_image(
                str(image_path), max_size=TILED_SOURCE_MAX_SIZE,
            )
            tiled_array = (
                np.array(tiled_img.convert("RGB"))
                if tiled_img is not None else img_array
            )
            # RAW loaders may use an embedded preview for the 1280px pass but
            # demosaic the sensor for the larger request. Those renditions can
            # have different crops or aspect ratios, so their normalized boxes
            # are not safely mergeable. Re-run the full frame from the exact
            # array used for tiling and discard the preview-rendition boxes.
            detections = _infer_array(
                session, input_name, tiled_array, RAW_CONF_FLOOR,
            )
            tiled = _tiled_fallback(session, input_name, tiled_array)
            if tiled:
                detections = _merge_detections(detections, tiled)
        return detections
    except ResourceWaitCancelled:
        raise
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
