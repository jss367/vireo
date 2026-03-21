"""Wildlife detection using MegaDetector via PytorchWildlife.

Provides bounding boxes around animals in photos for quality scoring.
"""

import logging
import os

import numpy as np
from PIL import Image

log = logging.getLogger(__name__)

_detector = None


def _get_detector():
    """Load MegaDetector (cached singleton). Auto-downloads weights on first use."""
    global _detector
    if _detector is not None:
        return _detector

    try:
        from PytorchWildlife.models import detection as pw_detection
        import torch

        device = "cuda" if torch.cuda.is_available() else "cpu"
        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            device = "mps"

        log.info(
            "Loading MegaDetector on %s (weights download automatically on first use)...",
            device,
        )
        _detector = pw_detection.MegaDetectorV6(
            device=device, pretrained=True, version="MDV6-yolov9-c"
        )
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
        results = detector.single_image_detection(str(image_path))
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
