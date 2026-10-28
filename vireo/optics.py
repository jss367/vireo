"""Manual lens corrections and edge-aware purple/green defringing.

Adapted from LightCraft's crates/pipeline/src/optics.rs under the MIT License.
Copyright (c) 2026 ArtCraft Team and the LightCraft contributors.
See THIRD_PARTY_NOTICES.md for the license. This implementation uses linear
sRGB instead of Rec.2020, overlapping row tiles, and Vireo's image types.
"""

from __future__ import annotations

import cv2
import numpy as np
from PIL import Image

try:
    from .float_image import FloatImage
    from .tone import linear_to_srgb, srgb_to_linear
except ImportError:
    from float_image import FloatImage
    from tone import linear_to_srgb, srgb_to_linear

_TILE_PIXELS = 1_000_000
_REF_LONG = 4000.0

# Standard linear sRGB -> Oklab matrices (D65).
_RGB_LMS = np.array([
    [0.4122214708, 0.5363325363, 0.0514459929],
    [0.2119034982, 0.6806995451, 0.1073969566],
    [0.0883024619, 0.2817188376, 0.6299787005],
], dtype=np.float32)
_LMS_LAB = np.array([
    [0.2104542553, 0.7936177850, -0.0040720468],
    [1.9779984951, -2.4285922050, 0.4505937099],
    [0.0259040371, 0.7827717662, -0.8086757660],
], dtype=np.float32)
_LAB_LMS = np.linalg.inv(_LMS_LAB).astype(np.float32)
_LMS_RGB = np.linalg.inv(_RGB_LMS).astype(np.float32)


def _smoothstep(lo, hi, value):
    t = np.clip((value - lo) / (hi - lo), 0, 1)
    return t * t * (3 - 2 * t)


def _hue_weight(hue, lo, hi):
    distance = np.abs((hue - (lo + hi) / 2 + 180) % 360 - 180)
    half = (hi - lo) / 2
    return 1 - _smoothstep(half, half + 8, distance)


def _defringe(linear, purple, green, radius):
    """Reduce Oklab chroma only at selected hues near strong lightness edges."""
    lab = np.cbrt(np.maximum(linear, 0) @ _RGB_LMS.T) @ _LMS_LAB.T
    lightness = np.ascontiguousarray(lab[..., 0])
    kernel = np.ones((radius * 2 + 1, radius * 2 + 1), np.uint8)
    lo = cv2.erode(lightness, kernel, borderType=cv2.BORDER_REPLICATE)
    hi = cv2.dilate(lightness, kernel, borderType=cv2.BORDER_REPLICATE)
    edge = _smoothstep(0.08, 0.25, hi - lo)
    chroma = np.hypot(lab[..., 1], lab[..., 2])
    hue = np.degrees(np.arctan2(lab[..., 2], lab[..., 1])) % 360
    # LightCraft's default hue windows: purple 276–324°, green 138–162°.
    weight = np.maximum(
        purple / 100 * _hue_weight(hue, 276, 324),
        green / 100 * _hue_weight(hue, 138, 162),
    ) * edge * _smoothstep(0.015, 0.05, chroma)
    lab[..., 1:] *= (1 - weight[..., None])
    corrected = (lab @ _LAB_LMS.T) ** 3 @ _LMS_RGB.T
    # Untargeted colors and negative/above-white source values survive intact.
    return np.where(weight[..., None] > 0, corrected, linear)


def _radius_squared(width, height, top, bottom):
    """Radius normalized to the half diagonal, centered on pixel centers."""
    cx, cy = (width - 1) / 2, (height - 1) / 2
    denominator = max(cx * cx + cy * cy, 1)
    x = np.arange(width, dtype=np.float32) - cx
    y = np.arange(top, bottom, dtype=np.float32) - cy
    return (y[:, None] ** 2 + x[None, :] ** 2) / denominator


def _wrap(pixels, source):
    if isinstance(source, FloatImage):
        return FloatImage(pixels, encoding=source.encoding)
    result = Image.fromarray(pixels)
    result.info.update(source.info)
    return result


def apply_optics(image, adjustments, profile=None):
    """Vignetting gain and defringing before geometry, crop and tone mapping.

    Both effects run in linear light. Output retains the input encoding and
    precision, with alpha unchanged. All neighborhood work has full overlap
    at row boundaries; intermediate arrays are bounded on large exports.
    """
    vignette = adjustments.get("vignetting", 0)
    purple = adjustments.get("defringe_purple", 0)
    green = adjustments.get("defringe_green", 0)
    calibration = (profile or {}).get("vignette")
    if not (vignette or purple or green or calibration):
        return image
    if not isinstance(image, FloatImage) and image.mode not in ("RGB", "RGBA"):
        alpha = "A" in image.getbands() or "transparency" in image.info
        image = image.convert("RGBA" if alpha else "RGB")
    floating = isinstance(image, FloatImage)
    linear_input = floating and image.encoding == "linear"
    source = np.asarray(image)
    output = np.empty_like(source)
    width, height = image.size
    # Width grows with strength, 1–7 px at a 4000 px long edge. Use the
    # uncropped loaded size so cropping cannot change the detection footprint.
    radius = max(1, round((1 + max(purple, green) * 0.06) * max(image.size) / _REF_LONG))
    halo = radius if purple or green else 0
    rows = max(1, _TILE_PIXELS // width)
    for top in range(0, height, rows):
        bottom = min(height, top + rows)
        start, end = max(0, top - halo), min(height, bottom + halo)
        rgb = source[start:end, :, :3].astype(np.float32)
        if not floating:
            rgb /= 255
        if not linear_input:
            rgb = srgb_to_linear(rgb)
        if vignette or calibration:
            # ±1.5 stops at the source corners; center remains unchanged.
            r2 = _radius_squared(width, height, start, end)
            gain = np.exp2(1.5 * vignette / 100 * r2 * r2)
            if calibration:
                gain /= 1 + r2 * np.polynomial.polynomial.polyval(r2, calibration)
            rgb *= gain[..., None]
        if purple or green:
            rgb = _defringe(rgb, purple, green, radius)
        if not linear_input:
            rgb = linear_to_srgb(rgb)
        tile = rgb[top - start:bottom - start]
        output[top:bottom, :, :3] = tile if floating else np.clip(tile * 255 + 0.5, 0, 255).astype(np.uint8)
        if source.shape[2] == 4:
            output[top:bottom, :, 3] = source[top:bottom, :, 3]
    return _wrap(output, image)


def distortion_scale(amount):
    """Inverse-map coefficient and inset keeping every sample inside the image."""
    k = -0.2 * amount / 100
    inset = 1.0
    if k > 0:
        # At the corner, inset * (1 + k * inset²) = 1. The radial
        # derivative is positive across the supported slider range.
        for _ in range(8):
            inset -= (inset + k * inset ** 3 - 1) / (1 + 3 * k * inset ** 2)
    return k, inset


def radial_factor(radius, amount, warp):
    """Combined inverse map: manual correction followed by calibrated distortion."""
    manual = 1 - 0.2 * amount / 100 * radius * radius
    return manual * np.polynomial.polynomial.polyval(radius * manual, warp)


def profile_inset(amount, warp):
    """Conservative inset for the combined map, including non-monotonic ratios."""
    radii = np.linspace(0, 1, 257)
    lower, upper = 0.0, 1.0
    for _ in range(32):
        middle = (lower + upper) / 2
        if middle * np.max(radial_factor(radii * middle, amount, warp)) <= 1:
            lower = middle
        else:
            upper = middle
    return lower


def distort_image(image, amount, profile=None):
    """Inverse radial resample, equally applicable to a photo and its mask.

    Positive values correct barrel distortion; negative values correct
    pincushion distortion. The output dimensions stay fixed; an automatic
    inset avoids empty borders. No 8-bit conversion of float sources occurs.
    """
    warp = (profile or {}).get("warp")
    if not amount and not warp:
        return image
    if not isinstance(image, FloatImage) and image.mode == "P":
        # Palette indexes are labels, not interpolable color samples.
        image = image.convert("RGBA" if "transparency" in image.info else "RGB")
    source = np.asarray(image)
    output = np.empty_like(source)
    width, height = image.size
    cx, cy = (width - 1) / 2, (height - 1) / 2
    k, inset = distortion_scale(amount)
    if warp:
        inset = profile_inset(amount, warp)
    x = (np.arange(width, dtype=np.float32) - cx)[None, :]
    rows = max(1, _TILE_PIXELS // width)
    for top in range(0, height, rows):
        bottom = min(height, top + rows)
        r2 = _radius_squared(width, height, top, bottom)
        factor = inset * (1 + k * inset * inset * r2)
        if warp:
            factor = inset * radial_factor(np.sqrt(r2) * inset, amount, warp)
        y = (np.arange(top, bottom, dtype=np.float32) - cy)[:, None]
        map_x = np.clip(cx + x * factor, 0, width - 1).astype(np.float32)
        map_y = np.clip(cy + y * factor, 0, height - 1).astype(np.float32)
        output[top:bottom] = cv2.remap(
            source, map_x, map_y, cv2.INTER_LINEAR, borderMode=cv2.BORDER_REPLICATE,
        )
    return _wrap(output, image)
