"""Which frame an original was decoded to, and which saved masks lie in it.

A subject mask saved today records the identity of the frame it was made in. A
mask or detector box saved before identities existed records none, and is good
only where every decode of the original gives the same frame. That holds for an
ordinary image file, and for a RAW whose embedded camera JPEG is near-full: the
aligned decode reads that JPEG, as the JPEG-first decode behind the older boxes
and masks did at every size they asked for. It does not hold for a RAW whose
preview was rejected or missing, where an older read could have used another crop.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from PIL import Image

_SHARED_RENDITIONS = ("image", "embedded-preview")


@dataclass(frozen=True)
class DecodedFrame:
    """The unscaled frame behind an aligned decode, whatever size was asked for."""

    frame_id: str | None = None
    native_size: tuple[int, int] | None = None
    rendition: str = "image"

    @classmethod
    def of(cls, image: Image.Image) -> DecodedFrame:
        info = getattr(image, "info", {})
        size = info.get("vireo_native_size")
        return cls(
            info.get("vireo_subject_frame_id"), tuple(size) if size else None,
            info.get("vireo_aligned_rendition", "image"),
        )

    @property
    def every_decode_agrees(self) -> bool:
        """Whether a box saved without a frame identity still points at the same pixels."""
        return self.rendition in _SHARED_RENDITIONS

    def holds_mask(self, mask_id: str | None, mask_size: tuple[int, int]) -> bool:
        """Whether a saved mask was made in this frame.

        A mask without an identity must have exactly the size this frame
        scales to: the sensor frame's aspect ratio usually differs enough
        from the preview's to round to another size.
        """
        if mask_id is not None:
            return mask_id == self.frame_id
        return (
            self.every_decode_agrees and self.native_size is not None
            and tuple(mask_size) == _scaled_size(self.native_size, max(mask_size))
        )


def _scaled_size(native: tuple[int, int], longest: int) -> tuple[int, int]:
    """The size ``Image.thumbnail`` gives a ``native`` image in a ``longest`` square."""
    width, height = native
    if max(native) <= longest:
        return native
    if width >= height:
        return longest, _nearest_edge(longest * height / width, lambda n: abs(width / height - longest / n))
    return _nearest_edge(longest * width / height, lambda n: abs(width / height - n / longest)), longest


def _nearest_edge(exact: float, aspect_error: Callable[[int], float]) -> int:
    """The whole short edge around ``exact`` that distorts the aspect ratio least."""
    return min((max(math.floor(exact), 1), max(math.ceil(exact), 1)), key=aspect_error)
