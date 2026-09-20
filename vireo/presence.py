"""Texture, clarity, and dehaze for server previews and saved renders.

Amounts are signed percentages. Spatial radii are in native photo pixels,
scaled to output resolution just like sharpening. Run after tone and resize,
before the existing detail pass. Overlapping row tiles bound export memory.
"""

from __future__ import annotations

import math

import numpy as np
from PIL import Image
from scipy.ndimage import gaussian_filter, minimum_filter

try:
    from .float_image import FloatImage
except ImportError:
    from float_image import FloatImage

try:
    from .tone import LUMA_B, LUMA_G, LUMA_R
except ImportError:
    from tone import LUMA_B, LUMA_G, LUMA_R

_TILE_PIXELS = 1_000_000


def _blur(plane, sigma):
    return gaussian_filter(plane, sigma, mode="mirror", truncate=3.0)


def _luma(rgb):
    return LUMA_R * rgb[..., 0] + LUMA_G * rgb[..., 1] + LUMA_B * rgb[..., 2]


def _airlight(src):
    # Estimate once for the whole render, never separately for each tile.
    # The brightest dark-channel samples favor veiled regions over isolated
    # saturated highlights. Bound both sampling memory and correction gain.
    # Slice the existing source buffer before allocating a float sample.
    # Resizing RGBA with Pillow would first copy the full frame to RGBa.
    step = max(1, math.ceil(max(src.shape[:2]) / 256))
    rgb = src[::step, ::step, :3].reshape(-1, 3).astype(np.float32)
    if src.dtype == np.uint8:
        rgb /= 255.0
    dark = rgb.min(axis=1)
    count = max(1, len(dark) // 100)
    indices = np.argpartition(dark, len(dark) - count)[-count:]
    return np.clip(rgb[indices].mean(axis=0), 0.5, 1.0)


def apply_presence(img, *, texture=0.0, clarity=0.0, dehaze=0.0, scale=1.0):
    """Apply signed presence controls, preserving alpha and neutral bytes.

    Texture boosts a band of fine detail while excluding the finest noise.
    Clarity adjusts broader, midtone-weighted luminance contrast, with a soft
    limit on edge deltas to reduce halos. Dehaze estimates a smooth local
    transmission from the dark channel; negative amounts add atmospheric veil.
    """
    if not (texture or clarity or dehaze):
        return img
    scale = float(scale or 1.0)
    if not math.isfinite(scale) or scale <= 0:
        scale = 1.0
    has_alpha = "A" in img.getbands() or "transparency" in img.info
    if img.mode not in ("RGB", "RGBA"):
        img = img.convert("RGBA" if has_alpha else "RGB")
    elif img.mode == "RGB" and "transparency" in img.info:
        img = img.convert("RGBA")
    # Distinct floors retain a texture band even in small thumbnails.
    texture_sigma = max(0.6, 3.0 * scale)
    fine_sigma = max(0.3, 0.7 * scale)
    clarity_sigma = max(0.3, 12.0 * scale)
    haze_sigma = max(0.3, 8.0 * scale)
    haze_radius = max(1, int(round(7.0 * scale)))
    # Sequential filters need the sum of their supports as overlap.
    halo = 0
    if dehaze:
        halo += haze_radius + math.ceil(3 * haze_sigma)
    if texture:
        halo += math.ceil(3 * texture_sigma)
    if clarity:
        halo += math.ceil(3 * clarity_sigma)
    floating = isinstance(img, FloatImage)
    src = np.asarray(img)
    airlight = _airlight(src) if dehaze else None
    height, width = src.shape[:2]
    out = np.empty_like(src)
    rows = max(1, _TILE_PIXELS // max(1, width))
    for top in range(0, height, rows):
        bottom = min(height, top + rows)
        start, end = max(0, top - halo), min(height, bottom + halo)
        rgb = src[start:end, :, :3].astype(np.float32)
        if not floating:
            rgb /= 255.0
        if dehaze:
            dark = np.min(rgb / airlight, axis=2)
            dark = minimum_filter(dark, size=2 * haze_radius + 1, mode="mirror")
            dark = np.clip(_blur(dark, haze_sigma), 0.0, 1.0)
            amount = float(dehaze) / 100.0
            if amount > 0:
                transmission = np.maximum(0.2, 1.0 - 0.85 * amount * dark)
                rgb = (rgb - airlight) / transmission[..., None] + airlight
            else:
                veil = -0.5 * amount * (1.0 - dark)
                rgb = rgb * (1.0 - veil[..., None]) + airlight * veil[..., None]
            rgb = np.clip(rgb, 0.0, 1.0)
        if texture:
            y = _luma(rgb)
            band = _blur(y, fine_sigma) - _blur(y, texture_sigma)
            rgb = np.clip(rgb + (float(texture) / 100.0 * band)[..., None], 0.0, 1.0)
        if clarity:
            y = _luma(rgb)
            delta = y - _blur(y, clarity_sigma)
            delta = delta / (1.0 + np.abs(delta) / 0.15)
            delta *= (4.0 * y * (1.0 - y)) * (float(clarity) / 100.0)
            rgb = np.clip(rgb + delta[..., None], 0.0, 1.0)
        rgb = rgb[top - start:bottom - start]
        out[top:bottom, :, :3] = (
            rgb if floating else np.clip(rgb * 255.0 + 0.5, 0, 255).astype(np.uint8)
        )
        if src.shape[2] == 4:
            out[top:bottom, :, 3] = src[top:bottom, :, 3]
    return FloatImage(out, encoding="srgb") if floating else Image.fromarray(out)
