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
        _detector = pw_detection.MegaDetectorV6(device=device, pretrained=True)
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

    # PytorchWildlife returns results with 'detections' containing boxes
    # The exact format varies by version — handle common patterns
    if hasattr(results, "keys"):
        # Dict-like result
        boxes = results.get("detections", results.get("boxes", []))
        if isinstance(boxes, dict):
            # Some versions return {'xyxy': tensor, 'confidence': tensor, 'class': tensor}
            xyxy = boxes.get("xyxy", [])
            confs = boxes.get("confidence", [])
            classes = boxes.get("class", [])
            for i in range(len(xyxy)):
                conf = float(confs[i]) if i < len(confs) else 0
                if conf < confidence_threshold:
                    continue
                box = xyxy[i]
                # Convert xyxy to xywh normalized
                img = Image.open(str(image_path))
                iw, ih = img.size
                x1, y1, x2, y2 = (
                    float(box[0]),
                    float(box[1]),
                    float(box[2]),
                    float(box[3]),
                )
                detections.append(
                    {
                        "box": {
                            "x": x1 / iw,
                            "y": y1 / ih,
                            "w": (x2 - x1) / iw,
                            "h": (y2 - y1) / ih,
                        },
                        "confidence": conf,
                        "category": "animal",
                    }
                )
        elif hasattr(boxes, "__len__"):
            # List of box objects
            for det in boxes:
                conf = float(det.get("confidence", det.get("conf", 0)))
                if conf < confidence_threshold:
                    continue
                box = det.get("bbox", det.get("box", [0, 0, 0, 0]))
                cat = det.get("category", det.get("class", "animal"))
                # Normalize category
                if isinstance(cat, (int, float)):
                    cat = {1: "animal", 2: "person", 3: "vehicle"}.get(
                        int(cat), "unknown"
                    )
                if cat != "animal":
                    continue
                if len(box) == 4:
                    detections.append(
                        {
                            "box": {
                                "x": float(box[0]),
                                "y": float(box[1]),
                                "w": float(box[2]),
                                "h": float(box[3]),
                            },
                            "confidence": conf,
                            "category": "animal",
                        }
                    )
    else:
        # Try treating as an object with attributes
        try:
            for det in results:
                conf = float(getattr(det, "confidence", getattr(det, "conf", 0)))
                if conf < confidence_threshold:
                    continue
                box = getattr(det, "bbox", getattr(det, "box", [0, 0, 0, 0]))
                detections.append(
                    {
                        "box": {
                            "x": float(box[0]),
                            "y": float(box[1]),
                            "w": float(box[2]),
                            "h": float(box[3]),
                        },
                        "confidence": conf,
                        "category": "animal",
                    }
                )
        except Exception:
            log.warning(
                "Could not parse detection results for %s: %s",
                image_path,
                type(results),
                exc_info=True,
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
