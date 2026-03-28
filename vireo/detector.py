"""Wildlife detection using MegaDetector via PytorchWildlife.

Provides bounding boxes around animals in photos for quality scoring.
"""

import logging
import os

import numpy as np

log = logging.getLogger(__name__)

_detector = None


def _get_detector():
    """Load MegaDetector (cached singleton). Auto-downloads weights on first use."""
    global _detector
    if _detector is not None:
        return _detector

    try:
        import torch
        from PytorchWildlife.models import detection as pw_detection

        device = "cuda" if torch.cuda.is_available() else "cpu"
        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            device = "mps"

        log.info(
            "Loading MegaDetector on %s (weights download automatically on first use)...",
            device,
        )

        # PyTorch 2.6+ defaults to weights_only=True which rejects the
        # pickled classes in MegaDetector/ultralytics weights. The loading
        # chain passes through multiple wrappers that capture function
        # references at import time, making monkey-patching unreliable.
        # Use the env var that torch.serialization.load checks directly.
        _prev_no = os.environ.get("TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD")
        _prev_force = os.environ.get("TORCH_FORCE_WEIGHTS_ONLY_LOAD")
        os.environ["TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD"] = "1"
        os.environ.pop("TORCH_FORCE_WEIGHTS_ONLY_LOAD", None)
        try:
            _detector = pw_detection.MegaDetectorV6(
                device=device, pretrained=True, version="MDV6-yolov9-c"
            )
        finally:
            if _prev_no is None:
                os.environ.pop("TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD", None)
            else:
                os.environ["TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD"] = _prev_no
            if _prev_force is not None:
                os.environ["TORCH_FORCE_WEIGHTS_ONLY_LOAD"] = _prev_force

        log.info("MegaDetector loaded")
        return _detector

    except ImportError:
        raise RuntimeError(
            "PytorchWildlife not installed. Run: pip install PytorchWildlife\n"
            "This provides MegaDetector for wildlife detection."
        )


def detect_animals(image_path, confidence_threshold=0.2):
    """Detect animals in an image using MegaDetector.

    Args:
        image_path: path to the image file
        confidence_threshold: minimum detection confidence (0-1)

    Returns:
        list of detections, each with:
            box: {x, y, w, h} normalized 0-1
            confidence: float 0-1
            category: str ('animal', 'person', 'vehicle')
        Returns empty list on failure.
    """
    detector = _get_detector()

    try:
        # Load image ourselves using image_loader which supports RAW formats
        # (NEF, CR2, ARW, etc.). PytorchWildlife's internal loading uses
        # PIL.Image.open() which cannot read RAW files — it either fails
        # or reads the tiny embedded JPEG thumbnail, producing bad detections.
        from image_loader import load_image

        img = load_image(str(image_path), max_size=1280)
        if img is None:
            log.warning("Could not load image for detection: %s", image_path)
            return []
        img_array = np.array(img.convert("RGB"))
        results = detector.single_image_detection(img_array, img_path=str(image_path))
    except Exception:
        log.warning("Detection failed for %s", image_path, exc_info=True)
        return []

    detections = []

    # PytorchWildlife returns a dict with:
    #   detections: supervision.Detections with xyxy, confidence, class_id
    #   normalized_coords: list of [x1, y1, x2, y2] normalized 0-1
    #   labels: list of "animal 0.28" strings
    if not hasattr(results, "keys"):
        log.warning("Unexpected detection result type: %s", type(results))
        return []

    # Prefer normalized_coords (already 0-1, no image size needed)
    norm_coords = results.get("normalized_coords", [])
    det_obj = results.get("detections")

    # Get confidences from the Detections object
    confs = []
    if det_obj is not None and hasattr(det_obj, "confidence") and det_obj.confidence is not None:
        confs = det_obj.confidence

    for i, coords in enumerate(norm_coords):
        conf = float(confs[i]) if i < len(confs) else 0
        if conf < confidence_threshold:
            continue
        x1, y1, x2, y2 = [float(c) for c in coords]
        detections.append(
            {
                "box": {
                    "x": x1,
                    "y": y1,
                    "w": x2 - x1,
                    "h": y2 - y1,
                },
                "confidence": conf,
                "category": "animal",
            }
        )

    return detections


def get_primary_detection(detections):
    """Get the highest-confidence animal detection from a list.

    Returns:
        detection dict or None
    """
    animals = [d for d in detections if d["category"] == "animal"]
    if not animals:
        return None
    return max(animals, key=lambda d: d["confidence"])
