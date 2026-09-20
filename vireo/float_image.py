"""High-precision RGB buffers used between RAW decoding and output encoding.

Pillow's RGB mode is always 8-bit. This small image type carries float32 RGB
through the same geometry operations without putting those pixels in RGB mode.
``linear`` identifies scene-linear sRGB primaries; ``srgb`` identifies rendered,
gamma-encoded pixels. Neither resizing nor copying changes that encoding.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
from PIL import Image, ImageCms


class FloatImage:
    mode = "RGB"

    def __init__(self, pixels, *, encoding="linear"):
        pixels = np.asarray(pixels, dtype=np.float32)
        if pixels.ndim != 3 or pixels.shape[2] != 3:
            raise ValueError("FloatImage requires an H×W×3 RGB array")
        if encoding not in ("linear", "srgb"):
            raise ValueError("Unknown RGB encoding")
        self.pixels = pixels
        self.encoding = encoding
        self.info = {}

    @property
    def size(self):
        return self.pixels.shape[1], self.pixels.shape[0]

    def __array__(self, dtype=None, copy=None):
        return np.array(self.pixels, dtype=dtype, copy=copy)

    def getbands(self):
        return ("R", "G", "B")

    def copy(self):
        return FloatImage(self.pixels.copy(), encoding=self.encoding)

    def close(self):
        self.pixels = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def convert(self, mode):
        """Explicit 8-bit boundary for consumers such as panorama stitching."""
        image = self.to_pil()
        if mode == "RGB":
            return image
        try:
            return image.convert(mode)
        finally:
            image.close()

    def _map_channels(self, operation):
        # Pillow F mode preserves floating point, including values above white.
        pixels = None
        for channel in range(3):
            with Image.fromarray(self.pixels[..., channel]) as plane:
                transformed = operation(plane)
                if pixels is None:
                    pixels = np.empty((transformed.height, transformed.width, 3), dtype=np.float32)
                pixels[..., channel] = np.asarray(transformed)
                transformed.close()
        return FloatImage(pixels, encoding=self.encoding)

    def rotate(self, angle, resample=Image.Resampling.NEAREST, expand=False):
        if angle % 90 == 0 and (expand or self.size[0] == self.size[1] or angle % 180 == 0):
            return FloatImage(np.rot90(self.pixels, int(angle / 90)), encoding=self.encoding)
        return self._map_channels(
            lambda plane: plane.rotate(angle, resample=resample, expand=expand)
        )

    def transpose(self, method):
        if method == Image.Transpose.FLIP_LEFT_RIGHT:
            return FloatImage(self.pixels[:, ::-1], encoding=self.encoding)
        if method == Image.Transpose.FLIP_TOP_BOTTOM:
            return FloatImage(self.pixels[::-1], encoding=self.encoding)
        return self._map_channels(lambda plane: plane.transpose(method))

    def crop(self, box):
        left, top, right, bottom = box
        if 0 <= left < right <= self.size[0] and 0 <= top < bottom <= self.size[1]:
            return FloatImage(self.pixels[top:bottom, left:right], encoding=self.encoding)
        return self._map_channels(lambda plane: plane.crop(box))

    def resize(self, size, resample=Image.Resampling.BICUBIC):
        return self._map_channels(lambda plane: plane.resize(size, resample))

    def thumbnail(self, size, resample=Image.Resampling.LANCZOS):
        def resize(plane):
            plane.thumbnail(size, resample=resample)
            return plane

        if self.size[0] > size[0] or self.size[1] > size[1]:
            self.pixels = self._map_channels(resize).pixels

    def to_pil(self):
        """Quantize only at an explicit display/8-bit output boundary."""
        return Image.fromarray(self._quantized(8))

    def _quantized(self, bits):
        try:
            from .tone import apply_adjustments
        except ImportError:
            from tone import apply_adjustments
        maximum = (1 << bits) - 1
        result = np.empty(self.pixels.shape, dtype=np.uint8 if bits == 8 else np.uint16)
        rows = max(1, 1_000_000 // max(1, self.size[0]))
        for top in range(0, self.size[1], rows):
            tile = self.pixels[top:top + rows]
            if self.encoding == "linear":
                tile = apply_adjustments(tile, input_linear=True)
            result[top:top + rows] = np.clip(tile * maximum + 0.5, 0, maximum)
        return result

    def save(self, output, format=None, **kwargs):
        """Encode rendered pixels; TIFF preserves 16 bits per channel."""
        if format is None and isinstance(output, (str, Path)):
            format = Image.registered_extensions().get(Path(output).suffix.lower())
        profile = ImageCms.ImageCmsProfile(ImageCms.createProfile("sRGB")).tobytes()
        if format and format.upper() in ("TIFF", "TIF"):
            import tifffile

            pixels = self._quantized(16)
            # Deflate uses Python's zlib and needs no imagecodecs dependency.
            tifffile.imwrite(
                output, pixels, photometric="rgb", compression="deflate",
                metadata=None, extratags=[(34675, "B", len(profile), profile, False)],
            )
        else:
            with self.to_pil() as image:
                kwargs.setdefault("icc_profile", profile)
                image.save(output, format=format, **kwargs)
