"""Neighborhood detail pass (noise reduction + sharpening) for edit recipes.

This is the spatial counterpart to :mod:`tone`: where the tone pipeline is
strictly per-pixel (and therefore scale-invariant and shader-transcribable),
these ops read pixel neighborhoods and are **not** scale-invariant. The
contract, from the detail-pass design doc: a recipe's detail settings describe
the effect on the full-resolution render, spatial parameters are authored in
native photo pixels, and callers rendering at a reduced size pass ``scale``
(output pixels per native pixel) so kernels shrink proportionally. The pass
runs last, in display space, after geometry, tone, and the final resize.

Because the ops are neighborhood reads, they cannot join the lightbox WebGL
live preview (per-pixel shader); the editor page judges them through exact
server renders instead.

Ordering inside the pass follows RAW-editor convention: noise reduction first
(luminance bilateral plus mild chroma smoothing), then luminance-only unsharp
masking — sharpening Y and adding the delta back to RGB avoids the color
fringing of naive per-channel USM.
"""

from __future__ import annotations

import math

import numpy as np
from PIL import Image

try:
    from .tone import LUMA_B, LUMA_G, LUMA_R
except ImportError:
    from tone import LUMA_B, LUMA_G, LUMA_R

# Row-tile budget in pixels, like image_edits._ADJUST_TILE_PIXELS: bounds peak
# memory on full-resolution exports. Tiles overlap by a halo wide enough that
# every written pixel sees only real neighbors, so tiled output is numerically
# identical to a whole-frame pass. Overridable in tests to force many tiles.
_DETAIL_TILE_PIXELS = 4_000_000

# Unsharp-mask strength at amount=100: delta gain of 1.5 is a strong but not
# cartoonish ceiling for high-ISO wildlife detail.
_SHARPEN_GAIN = 1.5

# Bilateral spatial sigma at scale 1.0, and its floor so the filter stays a
# real neighborhood op even for tiny preview scales (the downscale itself has
# already averaged most noise away by then).
_NR_SIGMA_SPATIAL = 1.6
_NR_SIGMA_SPATIAL_MIN = 0.5

# Bilateral range sigma in [0,1] luma units: from ~8 8-bit levels at amount 0+
# to ~31 levels at amount 100. Edges larger than a few sigmas pass through.
_NR_SIGMA_RANGE_BASE = 0.03
_NR_SIGMA_RANGE_SPAN = 0.09

# Chroma noise is smoothed with a plain Gaussian; chroma resolution loss is
# far less visible than luma blur, so the sigma runs a bit larger.
_NR_SIGMA_CHROMA = 1.5
_NR_SIGMA_CHROMA_MIN = 0.5

# Sharpening below this effective sigma would be sub-pixel noise shaping.
_SHARPEN_SIGMA_MIN = 0.3

_BILATERAL_MAX_RADIUS = 3


def _gaussian_kernel(sigma):
    radius = max(1, int(math.ceil(3.0 * sigma)))
    x = np.arange(-radius, radius + 1, dtype=np.float32)
    kernel = np.exp(-(x * x) / np.float32(2.0 * sigma * sigma))
    return kernel / kernel.sum()


def _blur_axis(arr, kernel, axis):
    radius = len(kernel) // 2
    pad = [(0, 0)] * arr.ndim
    pad[axis] = (radius, radius)
    padded = np.pad(arr, pad, mode="reflect")
    out = np.zeros(arr.shape, dtype=np.float32)
    length = arr.shape[axis]
    for i, weight in enumerate(kernel):
        window = [slice(None)] * arr.ndim
        window[axis] = slice(i, i + length)
        out += padded[tuple(window)] * weight
    return out


def _gaussian_blur(arr, sigma):
    """Separable Gaussian over the two spatial axes (2D or HxWxC arrays)."""
    kernel = _gaussian_kernel(sigma)
    return _blur_axis(_blur_axis(arr, kernel, 0), kernel, 1)


def _bilateral(y, sigma_spatial, sigma_range, radius):
    """Fixed-window bilateral filter on a 2D luma plane."""
    height, width = y.shape
    padded = np.pad(y, radius, mode="reflect")
    acc = np.zeros_like(y, dtype=np.float32)
    weights = np.zeros_like(y, dtype=np.float32)
    inv_2ss = np.float32(1.0 / (2.0 * sigma_spatial * sigma_spatial))
    inv_2sr = np.float32(1.0 / (2.0 * sigma_range * sigma_range))
    for dy in range(-radius, radius + 1):
        for dx in range(-radius, radius + 1):
            shifted = padded[
                radius + dy : radius + dy + height,
                radius + dx : radius + dx + width,
            ]
            diff = shifted - y
            weight = np.exp(
                -np.float32(dy * dy + dx * dx) * inv_2ss - diff * diff * inv_2sr
            )
            acc += shifted * weight
            weights += weight
    return acc / weights


def _luma(rgb):
    return LUMA_R * rgb[..., 0] + LUMA_G * rgb[..., 1] + LUMA_B * rgb[..., 2]


def _detail_params(sharpen, sharpen_radius, noise_reduction, scale):
    """Resolve slider values + render scale into concrete filter parameters.

    Returns None when the pass is a no-op. ``halo`` is the number of overlap
    rows a tile needs so every written pixel's (sequential NR -> sharpen)
    neighborhood contains only real image data.
    """
    sharpen_k = max(0.0, float(sharpen or 0.0)) / 100.0
    nr_k = max(0.0, float(noise_reduction or 0.0)) / 100.0
    if sharpen_k <= 0.0 and nr_k <= 0.0:
        return None
    scale = float(scale or 1.0)
    if not math.isfinite(scale) or scale <= 0.0:
        scale = 1.0

    params = {"nr": None, "sharpen": None}
    nr_halo = 0
    sharpen_halo = 0
    if nr_k > 0.0:
        sigma_spatial = max(_NR_SIGMA_SPATIAL_MIN, _NR_SIGMA_SPATIAL * scale)
        bilateral_radius = min(
            _BILATERAL_MAX_RADIUS, max(1, int(math.ceil(2.0 * sigma_spatial)))
        )
        sigma_chroma = max(_NR_SIGMA_CHROMA_MIN, _NR_SIGMA_CHROMA * scale)
        chroma_radius = max(1, int(math.ceil(3.0 * sigma_chroma)))
        params["nr"] = {
            "blend": nr_k,
            "sigma_spatial": sigma_spatial,
            "sigma_range": _NR_SIGMA_RANGE_BASE + _NR_SIGMA_RANGE_SPAN * nr_k,
            "radius": bilateral_radius,
            "sigma_chroma": sigma_chroma,
        }
        nr_halo = max(bilateral_radius, chroma_radius)
    if sharpen_k > 0.0:
        sigma = max(_SHARPEN_SIGMA_MIN, float(sharpen_radius) * scale)
        params["sharpen"] = {"gain": sharpen_k * _SHARPEN_GAIN, "sigma": sigma}
        sharpen_halo = max(1, int(math.ceil(3.0 * sigma)))
    params["halo"] = nr_halo + sharpen_halo
    return params


def _run_detail(rgb, params):
    """Run the detail pass on a float32 (H, W, 3) array in [0, 1]."""
    out = rgb
    nr = params["nr"]
    if nr:
        y = _luma(out)
        chroma = out - y[..., None]
        y_filtered = _bilateral(
            y, nr["sigma_spatial"], nr["sigma_range"], nr["radius"]
        )
        y = y + (y_filtered - y) * np.float32(nr["blend"])
        chroma_filtered = _gaussian_blur(chroma, nr["sigma_chroma"])
        chroma = chroma + (chroma_filtered - chroma) * np.float32(nr["blend"])
        out = np.clip(y[..., None] + chroma, 0.0, 1.0)
    sharpen = params["sharpen"]
    if sharpen:
        y = _luma(out)
        delta = (y - _gaussian_blur(y, sharpen["sigma"])) * np.float32(
            sharpen["gain"]
        )
        out = np.clip(out + delta[..., None], 0.0, 1.0)
    return out


def apply_detail(img, *, sharpen=0.0, sharpen_radius=1.0, noise_reduction=0.0,
                 scale=1.0):
    """Apply the detail pass to a PIL image and return a PIL image.

    ``sharpen`` and ``noise_reduction`` are recipe amounts in [0, 100];
    ``sharpen_radius`` is in native photo pixels and ``scale`` converts it to
    this render's pixels. A no-op returns the input image unchanged.
    """
    params = _detail_params(sharpen, sharpen_radius, noise_reduction, scale)
    if params is None:
        return img

    has_alpha = "A" in img.getbands() or "transparency" in img.info
    if img.mode not in ("RGB", "RGBA"):
        img = img.convert("RGBA" if has_alpha else "RGB")
    elif img.mode == "RGB" and "transparency" in img.info:
        img = img.convert("RGBA")

    src = np.asarray(img)
    height, width = src.shape[:2]
    channels = src.shape[2]
    out8 = np.empty((height, width, channels), dtype=np.uint8)

    halo = params["halo"]
    rows_per_tile = max(1, _DETAIL_TILE_PIXELS // max(1, width))
    for top in range(0, height, rows_per_tile):
        bottom = min(top + rows_per_tile, height)
        ext_top = max(0, top - halo)
        ext_bottom = min(height, bottom + halo)
        tile = src[ext_top:ext_bottom, :, :3].astype(np.float32) / 255.0
        result = _run_detail(tile, params)
        result = result[top - ext_top : bottom - ext_top]
        out8[top:bottom, :, :3] = np.clip(result * 255.0 + 0.5, 0, 255).astype(
            np.uint8
        )
        if channels == 4:
            out8[top:bottom, :, 3] = src[top:bottom, :, 3]

    return Image.fromarray(out8, "RGBA" if channels == 4 else "RGB")
