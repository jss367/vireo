"""Focus-window geometry and mask shading; collaborators own image I/O."""

import math

from services import detection_store


def focus_subject_detections(db, photo_id):
    """Prefer the subject whose active mask supplied the stored quality."""
    prompt = db.masks_features.active_prompt_detection(photo_id)
    if prompt is None and db.masks_features.photo_mask_path(photo_id):
        raise ValueError("Active subject association is stale; choose a subject or manual center")
    boxes = [prompt] if prompt is not None else detection_store.get_detections(db, photo_id)
    animals = [box for box in boxes if box["category"] == "animal" and box["detector_model"] != "full-image"]
    if animals and not valid_subject_box(animals[0]):
        raise ValueError("Primary subject coordinates are invalid; choose a subject or manual center")
    return animals


def valid_subject_box(box):
    """Detection coordinates belong to the upright, unedited normalized frame."""
    try:
        x, y, w, h = (float(box["box_" + k]) for k in "xywh")
    except (KeyError, IndexError, TypeError, ValueError):
        return False
    return (all(math.isfinite(v) for v in (x, y, w, h)) and 0 <= x < 1 and 0 <= y < 1
            and w > 0 and h > 0 and x + w <= 1.000001 and y + h <= 1.000001)


def point_in_subject(center, box):
    if not valid_subject_box(box):
        return False
    x, y, w, h = (float(box["box_" + k]) for k in "xywh")
    return x <= center[0] <= x + w and y <= center[1] <= y + h


def parse_center(raw):
    """``(x, y)`` from a ``center=x,y`` query value in normalized frame coordinates.

    A reviewer who has located the eye on the preview asks for the 1:1
    window there. Both values must be finite and within 0..1; anything else
    raises ``ValueError``.
    """
    parts = str(raw).split(",")
    if len(parts) != 2:
        raise ValueError("center must be x,y in normalized frame coordinates")
    try:
        x, y = (float(part) for part in parts)
    except ValueError:
        raise ValueError("center must be x,y in normalized frame coordinates") from None
    if not all(math.isfinite(v) and 0.0 <= v <= 1.0 for v in (x, y)):
        raise ValueError("center coordinates must be between 0 and 1")
    return (x, y)


def focus_point(photo, detections, basis, *, eye_enabled, point=None):
    """Auto follows the scorer; explicit eye/subject requests remain available.

    ``point`` (a parsed ``center``) wins over every basis: the reviewer chose
    where to look.
    """
    if point is not None:
        return point, "point"
    use_eye = basis == "eye" or (basis == "auto" and eye_enabled and photo["eye_tenengrad"] is not None)
    if use_eye and photo["eye_x"] is not None and photo["eye_y"] is not None:
        center = (float(photo["eye_x"]), float(photo["eye_y"]))
        if (all(math.isfinite(v) and 0 <= v <= 1 for v in center)
                and (not detections or point_in_subject(center, detections[0]))):
            return center, "eye"
    if basis in {"auto", "subject"}:
        for box in detections:
            if box["category"] != "animal" or box["detector_model"] == "full-image" or not valid_subject_box(box):
                continue
            center = (box["box_x"] + box["box_w"] / 2, box["box_y"] + box["box_h"] / 2)
            if all(math.isfinite(v) for v in center):
                return center, "subject"
    if basis in {"eye", "subject"}:
        raise ValueError(f"No {basis} location recorded for this photo")
    return (0.5, 0.5), "frame"


def validate_raster_geometry(image_size, photo):
    """A differently cropped raster cannot use stored normalized source locations."""
    try:
        width, height = photo["width"], photo["height"]
    except (KeyError, IndexError, TypeError):
        return
    if not width or not height:
        return
    expected = max(width, height) / min(width, height)
    actual = max(image_size) / min(image_size)
    if abs(actual / expected - 1) > 0.02:
        raise ValueError("Source raster geometry differs from the catalog; place a manual center")


def focus_window(image_size, center, size):
    """Clamp a window in the loader's upright raster without resampling it."""
    iw, ih = image_size
    cx, cy = (min(max(v, 0.0), 1.0) for v in center)
    half = size // 2
    x1 = min(max(int(round(cx * iw)) - half, 0), max(0, iw - size))
    y1 = min(max(int(round(cy * ih)) - half, 0), max(0, ih - size))
    return (x1, y1, min(iw, x1 + size), min(ih, y1 + size)), (cx, cy)


def shade_focus_mask(crop, mask_bytes, image_size, window, image_api):
    """Use the same raster coordinates for the pipeline mask and focus window."""
    import io

    mask = image_api.open(io.BytesIO(mask_bytes)).convert("L")
    mask = mask.resize(image_size, image_api.NEAREST).crop(window)
    mask = mask.point(lambda v: 255 if v > 127 else 0)
    shaded = crop.point(lambda v: int(v * 0.35))
    return image_api.composite(crop, shaded, mask)


def _catalog_long_side(photo):
    """The photo's longest catalog dimension, or None when the catalog has none."""
    try:
        width, height = photo["width"], photo["height"]
    except (KeyError, IndexError, TypeError):
        return None
    if not width or not height:
        return None
    return max(width, height)


def original_scale(image_size, photo):
    """Decoded-raster pixels per original pixel, or None when the catalog has no dimensions.

    Compared on the longest side, so a raster the loader rotated upright
    still measures against the right catalog edge.
    """
    try:
        validate_raster_geometry(image_size, photo)
    except ValueError:
        return None
    long_side = _catalog_long_side(photo)
    if not long_side:
        return None
    return max(image_size) / float(long_side)


def prefer_original(image_size, photo, *, downsized_below=0.98):
    """Whether a working-copy raster is too small to pass for original pixels.

    A raster whose longest side is under ``downsized_below`` of the catalog's
    counts as downsized, and ``source=auto`` goes back to the original for
    the crop. Embedded JPEGs a few pixels short of the sensor (5392 vs 5408)
    are not downsized in any sense a reviewer cares about.
    """
    scale = original_scale(image_size, photo)
    return scale is not None and scale < downsized_below


def focus_headers(basis, center, window, image_size, source, mask_applied, scale=None):
    """Scale 1 means decoded-raster pixels; ``scale`` says how those relate to the original."""
    return {
        "X-Vireo-Focus-Basis": basis,
        "X-Vireo-Focus-Center": f"{center[0]:.4f},{center[1]:.4f}",
        "X-Vireo-Focus-Scale": "1",
        "X-Vireo-Focus-Scale-Reference": "decoded-raster",
        "X-Vireo-Focus-Original-Scale": "unknown" if scale is None else f"{scale:.4f}",
        "X-Vireo-Focus-Source": source,
        "X-Vireo-Focus-Raster-Size": ",".join(str(v) for v in image_size),
        "X-Vireo-Focus-Mask": "1" if mask_applied else "0",
        "X-Vireo-Focus-Window": ",".join(str(v) for v in window),
    }


def focus_evidence_headers(photo, boxes, *, bound=False):
    """Describe the actual crop subject, independently of photo-level scoring."""
    headers = {"X-Vireo-Focus-Eye-Status": "unbound"}
    if boxes:
        box = boxes[0]
        headers["X-Vireo-Focus-Detection-Id"] = str(box["id"])
        headers["X-Vireo-Focus-Subject-Box"] = ",".join(str(box["box_" + k]) for k in "xywh")
        headers["X-Vireo-Focus-Detection-Confidence"] = str(box["detector_confidence"])
    if bound:
        try:
            focus_point(photo, boxes, "eye", eye_enabled=True)
        except (ValueError, TypeError):
            status = photo.get("eye_status")
            headers["X-Vireo-Focus-Eye-Status"] = status if status in {
                "disabled", "model_unavailable", "identity_unavailable", "not_located",
            } else "not_located"
        else:
            headers["X-Vireo-Focus-Eye-Status"] = "located"
    return headers
