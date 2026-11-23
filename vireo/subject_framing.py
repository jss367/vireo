"""Crop rectangles that frame one subject box with padding at a chosen shape.

Pure geometry over the upright, unedited frame: subject boxes and recipe crops
are both fractions of it, so a rectangle from here is a valid recipe crop for
a recipe without rotation, flips or straightening. The frame's size must be
the display-oriented one (``render_source.recipe_source_dimensions``); sensor
axes would turn a portrait photo's shape on its side.
"""

import math

#: Shapes ``auto`` chooses between: a wide subject gets the first.
AUTO_WIDE = (3.0, 2.0)
AUTO_TALL = (4.0, 5.0)
MAX_MARGIN = 5.0


def parse_aspect(raw):
    """``None`` for ``auto``, else ``(width, height)`` from ``"W:H"``."""
    text = str(raw or "auto").strip().lower()
    if text == "auto":
        return None
    try:
        width, height = (float(part) for part in text.split(":"))
    except ValueError:
        raise ValueError("aspect must be auto or W:H, such as 3:2") from None
    if not all(math.isfinite(v) and v > 0 for v in (width, height)):
        raise ValueError("aspect must be auto or W:H, such as 3:2")
    return width, height


def frame_subject(box, frame, margin, aspect=None):
    """The crop around ``box`` padded by ``margin`` of its size on each side.

    ``box`` holds fractions ``x, y, w, h``; ``frame`` is the upright pixel
    size. The crop grows to the aspect ratio, shrinks to fit the frame when it
    must, and slides to stay inside it. Returns the crop, its pixel size, the
    subject's pixel size, and whether the frame left less than the requested
    padding or cut the subject itself.
    """
    width, height = frame
    box_w, box_h = box["w"] * width, box["h"] * height
    if not all(math.isfinite(v) and v > 0 for v in (width, height, box_w, box_h)):
        raise ValueError("The subject box or the photo's size is unusable")
    ratio_w, ratio_h = aspect or (AUTO_WIDE if box_w >= box_h else AUTO_TALL)
    ratio = ratio_w / ratio_h
    crop_w, crop_h = box_w * (1 + 2 * margin), box_h * (1 + 2 * margin)
    if crop_w / crop_h < ratio:
        crop_w = crop_h * ratio
    else:
        crop_h = crop_w / ratio
    shrink = min(1.0, width / crop_w, height / crop_h)
    crop_w, crop_h = crop_w * shrink, crop_h * shrink
    left = min(max((box["x"] + box["w"] / 2) * width - crop_w / 2, 0), width - crop_w)
    top = min(max((box["y"] + box["h"] / 2) * height - crop_h / 2, 0), height - crop_h)
    # Sub-pixel slack, so rounding never reads as a cut subject.
    cut = crop_w < box_w - 1 or crop_h < box_h - 1
    return {
        "crop": {"x": round(left / width, 6), "y": round(top / height, 6),
                 "w": round(crop_w / width, 6), "h": round(crop_h / height, 6)},
        "aspect": f"{ratio_w:g}:{ratio_h:g}",
        "output_pixels": [round(crop_w), round(crop_h)],
        "subject_pixels": [round(box_w), round(box_h)],
        "margin_reduced": shrink < 1.0,
        "subject_cut": cut,
    }
