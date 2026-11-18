"""Nikon AF area in the display-oriented detector coordinate space."""
import json
import math
from enum import IntEnum
from typing import NamedTuple

from exif_orientation import exif_orientation


class FocusMatch(IntEnum):
    NONE = 0
    NEAR = 1
    ADJACENT = 2
    AREA = 3
    CENTER = 4


class FocusArea(NamedTuple):
    x: float
    y: float
    width: float
    height: float

    def match(self, box):
        """Prefer containment, then overlap with the recorded AF area."""
        dx = max(box["x"] - self.x, self.x - box["x"] - box["w"], 0)
        dy = max(box["y"] - self.y, self.y - box["y"] - box["h"], 0)
        if dx == dy == 0:
            return FocusMatch.CENTER
        if dx <= self.width / 2 and dy <= self.height / 2:
            return FocusMatch.AREA
        # A hunting AF point can land on water alongside a swimming bird.
        # Favor nearby proposals only inside the larger retry crop.
        distance = math.hypot(dx, dy)
        if distance <= 0.07:
            return FocusMatch.ADJACENT
        return FocusMatch.NEAR if distance <= 0.14 else FocusMatch.NONE


def focus_point(metadata):
    try:
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        tags = metadata["MakerNotes"]
        if int(tags.get("AFCoordinatesAvailable", 1)) == 0:
            return None
        width, height = float(tags["AFImageWidth"]), float(tags["AFImageHeight"])
        if not all(math.isfinite(v) and v > 0 for v in (width, height)):
            return None
        x = float(tags["AFAreaXPosition"]) / width
        y = float(tags["AFAreaYPosition"]) / height
        w = float(tags.get("AFAreaWidth", 0)) / width
        h = float(tags.get("AFAreaHeight", 0)) / height
        # AF coordinates can be relative to the full sensor even when the
        # NEF records a DX crop. CropHiSpeed reports sensor and crop sizes
        # followed by the top-left offset, in sensor pixels.
        crop = str(tags.get("CropHiSpeed", "")).split()
        if len(crop) == 7:
            _, sw, sh, cw, ch, left, top = map(int, crop)
            if 0 < cw <= sw and 0 < ch <= sh and (width > cw or height > ch):
                x, y = (x * sw - left) / cw, (y * sh - top) / ch
                w, h = w * sw / cw, h * sh / ch
        if not (0 <= x <= 1 and 0 <= y <= 1 and 0 <= w <= 1 and 0 <= h <= 1):
            return None
        orientation = int(exif_orientation(metadata) or 1)
        center = {1: (x, y), 2: (1-x, y), 3: (1-x, 1-y), 4: (x, 1-y),
                  5: (y, x), 6: (1-y, x), 7: (1-y, 1-x), 8: (y, 1-x)}.get(orientation)
        if center:
            return FocusArea(*center, h, w) if orientation >= 5 else FocusArea(*center, w, h)
    except (AttributeError, TypeError, KeyError, ValueError, OverflowError):
        pass
    return None
