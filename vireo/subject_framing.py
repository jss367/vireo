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
#: The longest shape a crop can be asked for, either way round.
MAX_ASPECT = 100.0


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
    if not 1 / MAX_ASPECT <= width / height <= MAX_ASPECT:
        raise ValueError(f"aspect must be between 1:{MAX_ASPECT:g} and {MAX_ASPECT:g}:1")
    return width, height


def _span(start, length, total):
    """``(start, length)`` as six-place fractions that end inside the frame.

    Rounding each on its own can pass 1.0 when both land on a midpoint, and a
    recipe refuses a crop that does.
    """
    begin = min(round(start / total, 6), 1 - 1e-6)
    size = min(round(length / total, 6), round(1 - begin, 6))
    return begin, max(size, 1e-6)


def frame_subject(box, frame, margin, aspect=None):
    """The crop around ``box`` padded by ``margin`` of its size on each side.

    ``box`` holds fractions ``x, y, w, h``; ``frame`` is the upright pixel
    size. The crop grows to the aspect ratio, shrinks to fit the frame when it
    must, and slides to stay inside it. Returns the crop, its pixel size, the
    subject's pixel size, and whether the frame left less than the requested
    padding on any side or cut the subject itself.
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
    x, w = _span(left, crop_w, width)
    y, h = _span(top, crop_h, height)
    margin_w, margin_h = box_w * margin, box_h * margin
    box_left, box_top = box["x"] * width, box["y"] * height
    # How far the requested padding reaches past each side of the final crop.
    lost = max(left - (box_left - margin_w), top - (box_top - margin_h),
               box_left + box_w + margin_w - (left + crop_w), box_top + box_h + margin_h - (top + crop_h))
    # Sub-pixel slack, so rounding never reads as a cut subject.
    cut = crop_w < box_w - 1 or crop_h < box_h - 1
    return {
        "crop": {"x": x, "y": y, "w": w, "h": h},
        "aspect": f"{ratio_w:g}:{ratio_h:g}",
        "output_pixels": [round(crop_w), round(crop_h)],
        "subject_pixels": [round(box_w), round(box_h)],
        "margin_reduced": lost > 1,
        "subject_cut": cut,
    }
