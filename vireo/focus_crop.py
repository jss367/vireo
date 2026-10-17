"""Focus-window geometry and mask shading; collaborators own image I/O."""

import math


def focus_point(photo, detections, basis, *, eye_enabled):
    """Auto follows the scorer; explicit eye/subject requests remain available."""
    use_eye = basis == "eye" or (basis == "auto" and eye_enabled and photo["eye_tenengrad"] is not None)
    if use_eye and photo["eye_x"] is not None and photo["eye_y"] is not None:
        center = (float(photo["eye_x"]), float(photo["eye_y"]))
        if all(math.isfinite(v) for v in center):
            return center, "eye"
    if basis in {"auto", "subject"}:
        for box in detections:
            if box["category"] != "animal" or box["detector_model"] == "full-image":
                continue
            center = (box["box_x"] + box["box_w"] / 2, box["box_y"] + box["box_h"] / 2)
            if all(math.isfinite(v) for v in center):
                return center, "subject"
    if basis in {"eye", "subject"}:
        raise ValueError(f"No {basis} location recorded for this photo")
    return (0.5, 0.5), "frame"


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


def focus_headers(basis, center, window, image_size, source, mask_applied):
    """1:1 means decoded-raster pixels; cached/embedded previews may be smaller."""
    return {
        "X-Vireo-Focus-Basis": basis,
        "X-Vireo-Focus-Center": f"{center[0]:.4f},{center[1]:.4f}",
        "X-Vireo-Focus-Scale": "1",
        "X-Vireo-Focus-Scale-Reference": "decoded-raster",
        "X-Vireo-Focus-Original-Scale": "unknown",
        "X-Vireo-Focus-Source": source,
        "X-Vireo-Focus-Raster-Size": ",".join(str(v) for v in image_size),
        "X-Vireo-Focus-Mask": "1" if mask_applied else "0",
        "X-Vireo-Focus-Window": ",".join(str(v) for v in window),
    }
