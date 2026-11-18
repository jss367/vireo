"""Pixel evaluation of the local, native before/after evidence cited in issue 2521."""
import os
from pathlib import Path

import numpy as np
import pytest
from PIL import Image


def test_visible_bird_and_foreground_reed_coverage():
    directory = os.getenv("VIREO_GROUP35_VISUALS")
    if not directory:
        pytest.skip("Set VIREO_GROUP35_VISUALS to the recorded quality-visuals directory")
    # Independently labeled patches on the visible head, back, flank, and
    # foreground stalks in the unmasked original crop of photo 237576.
    bird = [(299, 139, 322, 158), (244, 184, 273, 207), (248, 222, 270, 244)]
    reed = [(115, 123, 144, 160), (133, 170, 150, 196), (286, 349, 311, 378)]

    def coverage(prefix, dx, dy, patches):
        def luminance(kind):
            with Image.open(Path(directory) / f"{prefix}-{kind}-237576.jpg") as image:
                return np.asarray(image.convert("L"), dtype=float)
        # The overlay preserves selected pixels and dims the rest to 35%.
        # The midpoint tolerates differences from independent JPEG compression.
        selected = luminance("mask") > .675 * luminance("head")
        return np.concatenate([selected[y+dy:y2+dy, x+dx:x2+dx].ravel()
                               for x, y, x2, y2 in patches]).mean()

    # The tighter box moves the 1:1 crop center. Offset measured by matching
    # the same undimmed source patch in the two native crops.
    for patches, before_min, before_max, after_min, after_max in (
        (bird, 0, .1, .9, 1), (reed, .85, 1, 0, .1),
    ):
        assert before_min <= coverage("group35", 0, 0, patches) <= before_max
        assert after_min <= coverage("group35-mask-repair", -31, 48, patches) <= after_max
