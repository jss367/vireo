"""Scene-referred tone pipeline for non-destructive photo edits.

This module is the single source of truth for how the editable adjustments
(exposure, white balance, highlights, shadows, whites, blacks, contrast,
vibrance, saturation) map input pixels to output pixels. Geometry
(rotate/flip/straighten/crop) lives in ``image_edits`` and is not part of this
pipeline.

Why this exists
---------------
The original editor multiplied gamma-encoded sRGB bytes (``2 ** ev`` brightness
in 8-bit space) and hard-clipped anything that exceeded white. That is
photometrically wrong: a real exposure change scales *linear* radiance, and
pushing exposure should roll highlights off smoothly rather than clip them to a
flat white. This pipeline therefore:

  1. de-gammas sRGB -> linear light,
  2. applies the scene-referred ops (exposure, white balance, and
     Shadows/Highlights as a local exposure change) in linear,
  3. rolls highlights off with a smooth shoulder (no hard clip to 1.0),
  4. remaps perceptual luminance for Whites/Blacks and reconstructs RGB in
     linear light,
  5. re-encodes linear -> sRGB,
  6. applies the remaining display-referred ops (contrast and color) in sRGB.

RAW inputs use ``input_linear=True`` and retain floating-point radiance,
including negative/out-of-display-gamut and above-white channel values, until
exposure, white balance and Shadows/Highlights have been applied. A luminance
shoulder and gamut compression then map to display range, so Highlights can
pull radiance above display white back into range before the shoulder
compresses it. Rendered buffers stay float through geometry, tone, presence,
and detail; encoding is the quantization boundary. JPEGs and offline working
copies retain the legacy sRGB input path and cannot recover information
already lost before loading. The WebGL shader implements that legacy path
only; RAW adjustments use the server renderer.

Shadows/Highlights
------------------
Each pixel's exposure moves by a number of stops chosen from the brightness of
its neighborhood, not its own: an edge-aware base of scene log luminance (a
fast guided filter whose blur scales with the frame, see :class:`RangeBase`).
A whole dark region lifts by the same gain, so its texture keeps its contrast
the way it does under Exposure, while strong edges keep each side's own
brightness and no halo forms across them.

Live preview
------------
The default scalar path maps onto the legacy GLSL preview, which has no
Shadows/Highlights: those edits use server previews because the neighborhood
base cannot run in the single-pass shader. Whites/Blacks remain point curves.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any

import numpy as np

# --- constants (keep in sync with the live-preview shader) ---------------------

# Linear value at which the highlight shoulder begins. Below the knee the
# pipeline is identity (no tone change); above it, values are compressed toward
# 1.0 so that boosting exposure desaturates and rolls off highlights instead of
# clipping. 0.85 linear ~= 0.94 sRGB, so an un-pushed image is essentially
# untouched and only genuinely bright pixels ever enter the shoulder.
HIGHLIGHT_KNEE = 0.85

# White-balance channel-gain coefficients. Mirrors the original
# ``_apply_white_balance`` so saved recipes keep their intended colour cast;
# the only change is that the gains are now applied in linear light.
WB_TEMP_R = 0.26
WB_TEMP_B = 0.26
WB_TINT_R = 0.06
WB_TINT_G = 0.18
WB_TINT_B = 0.06
WB_MIN_GAIN = 0.05

# Rec. 709 luma weights, used for luma-preserving saturation in display space.
LUMA_R = 0.2126
LUMA_G = 0.7152
LUMA_B = 0.0722

# Whites/Blacks curve constants.  These controls operate on an sRGB-like
# encoding of *linear-light luminance*, then reconstruct linear RGB at the new
# luminance.  This keeps them perceptually placed while preserving
# chromaticity instead of blending each channel toward grey.
BLACK_PIVOT = 0.30
BLACK_LIFT = 0.40
BLACK_DEEPEN = 0.90
# Preserve chromaticity normally, but fade chroma smoothly below four sRGB
# luminance code values when Blacks or Whites raises luminance.  That prevents
# sensor or quantization noise from becoming a saturated speck beside neutral
# black while retaining color once the source has meaningful luminance.
BLACK_CHROMA_FADE = 4.0 / 255.0

# Shadows/Highlights at +/-100 move a fully weighted neighborhood this many
# stops.  The weights are smoothsteps of the base's scene EV over middle grey:
# Shadows fades out by SHADOW_EV[1] and Highlights fades in from
# HIGHLIGHT_EV[0], so mid-grey regions barely move under either control.
RANGE_STOPS = 1.7
MIDDLE_GREY = 0.18
SHADOW_EV = (-4.8, 0.3)
HIGHLIGHT_EV = (-1.0, 2.8)
# Gaussian sigma of the base, as a fraction of the full frame's long edge, so
# a preview and a full-size export see the same neighborhoods.
RANGE_SIGMA_FRACTION = 0.015
# Guided-filter epsilon in stops squared: texture deviating less than about
# 0.6 EV from its surroundings is averaged into the base; larger edges stay.
_RANGE_EPSILON = 0.35
_LOG_FLOOR = 1e-6
# Pixels read per strip while block-averaging a frame for the base grid.
_RANGE_BASE_STRIP_PIXELS = 1_000_000


def range_sigma(frame_long_edge):
    """Base blur sigma, in pixels, for a full frame this many pixels long."""
    return max(1.0, RANGE_SIGMA_FRACTION * float(frame_long_edge))


def _range_grid_step(sigma):
    """Block size of the base's coarse grid: a third of sigma, 1 to 16 px."""
    return int(min(16, max(1, sigma // 3)))


def _log_luminance(lin):
    return np.log2(np.maximum(_luma(lin)[..., 0], np.float32(_LOG_FLOOR)))


def _bilinear_taps(start, count, step, size):
    """Grid rows (or columns) and weights for pixels ``start..start+count``."""
    centers = (np.arange(start, start + count, dtype=np.float64) + 0.5) / step - 0.5
    centers = np.clip(centers, 0.0, size - 1)
    lower = centers.astype(np.intp)
    upper = np.minimum(lower + 1, size - 1)
    return lower, upper, (centers - lower).astype(np.float32)


@dataclass(frozen=True)
class RangeBase:
    """Edge-aware base of a frame's scene log2 luminance.

    The self-guided filter of He et al., Guided Image Filtering (ECCV 2010),
    in its fast form (He and Sun, arXiv:1505.00996): the coefficients ``a``
    and ``b`` are computed on a block-averaged grid and sampled bilinearly at
    each pixel, whose base is ``a * log2(Y) + b`` with its own luminance
    ``Y``. Flat or finely textured areas (``a`` near 0) take their
    neighborhood's mean; across a strong edge (``a`` near 1) a pixel keeps
    its own level, so the two sides never borrow each other's brightness.
    Sampling depends only on a pixel's position in the frame, so each tile of
    a tiled render sees exactly what a whole-frame render does.
    """

    a: Any
    b: Any
    step: int

    @classmethod
    def build(
        cls, rgb, sigma, *, exposure=0.0, white_balance=None,
        input_linear=False, value_scale=1.0,
    ):
        """Base of ``rgb`` (H, W, >=3) after global exposure and white balance.

        ``sigma`` is the blur in ``rgb``'s pixels (see :func:`range_sigma`);
        ``value_scale`` maps stored values to [0, 1] (1/255 for 8-bit). The
        frame is read in strips, so a 45MP source is never copied whole.
        """
        from scipy.ndimage import gaussian_filter

        height, width = rgb.shape[:2]
        step = _range_grid_step(sigma)
        strip = max(step, _RANGE_BASE_STRIP_PIXELS // max(1, width) // step * step)
        row_starts = np.arange(0, height, step)
        col_starts = np.arange(0, width, step)
        gain = np.float32(2.0 ** float(exposure)) * np.array(
            white_balance_gains(white_balance), dtype=np.float32,
        )
        sums = np.empty((len(row_starts), len(col_starts)), dtype=np.float64)
        for top in range(0, height, strip):
            chunk = np.asarray(rgb[top:top + strip, :, :3], dtype=np.float32)
            if value_scale != 1.0:
                chunk = chunk * np.float32(value_scale)
            lin = chunk if input_linear else srgb_to_linear(chunk)
            log_y = _log_luminance(lin * gain).astype(np.float64)
            blocks = np.add.reduceat(
                np.add.reduceat(log_y, col_starts, axis=1),
                np.arange(0, log_y.shape[0], step), axis=0,
            )
            sums[top // step:top // step + blocks.shape[0]] = blocks
        counts = np.outer(
            np.diff(np.append(row_starts, height)),
            np.diff(np.append(col_starts, width)),
        )
        grid = sums / counts

        def blur(plane):
            return gaussian_filter(plane, sigma / step, mode="reflect")

        mean = blur(grid)
        variance = np.maximum(blur(grid * grid) - mean * mean, 0.0)
        a = variance / (variance + _RANGE_EPSILON)
        return cls(
            a=blur(a).astype(np.float32),
            b=blur(mean - a * mean).astype(np.float32),
            step=step,
        )

    def at(self, log_y, top=0, left=0):
        """Base for ``log_y``, the (h, w) log2 luminance at (top, left)."""
        rows, cols = self.a.shape
        y0, y1, fy = _bilinear_taps(top, log_y.shape[0], self.step, rows)
        x0, x1, fx = _bilinear_taps(left, log_y.shape[1], self.step, cols)
        fy = fy[:, None]

        def sample(grid):
            band = grid[y0] * (1.0 - fy) + grid[y1] * fy
            return band[:, x0] * (1.0 - fx) + band[:, x1] * fx

        a = sample(self.a)
        filtered = a * log_y + sample(self.b)
        # Within a blur radius of a strong edge ``a`` is high but short of 1,
        # and the filtered base leans toward the other side: a flat region
        # would gain a visible bright or dark rim along the edge. Fall back
        # to the pixel's own level there, as a point curve would.
        confidence = (1.0 - a) ** 2
        return log_y + confidence * (filtered - log_y)


def _range_base_log(lin, range_base, origin, ev_shift):
    """Base log2 luminance of each pixel of ``lin``; its own without a base.

    ``ev_shift`` is a pixel's local exposure over the global exposure the
    base was built with. The base moves with it, as exposure moves the
    luminance it was built from, so a mask never enters the filter.
    """
    log_y = _log_luminance(lin)
    if range_base is None:
        return log_y
    if ev_shift is None:
        return range_base.at(log_y, *origin)
    return range_base.at(log_y - ev_shift, *origin) + ev_shift


def _range_stops(base_log, shadows, highlights):
    """Stops of exposure Shadows/Highlights give a pixel with this base."""
    ev = (base_log - np.float32(np.log2(MIDDLE_GREY)))[..., None]
    stops = np.zeros_like(ev)
    if np.any(shadows):
        weight = 1.0 - smoothstep(*SHADOW_EV, ev)
        stops = stops + _unit_amount(shadows) * weight * np.sqrt(weight)
    if np.any(highlights):
        stops = stops + _unit_amount(highlights) * smoothstep(*HIGHLIGHT_EV, ev)
    return np.float32(RANGE_STOPS) * stops


def srgb_to_linear(c):
    """sRGB-encoded [0,1] -> linear light. Standard IEC 61966-2-1 transfer."""
    c = np.asarray(c, dtype=np.float32)
    return np.where(c <= 0.04045, c / 12.92, ((c + 0.055) / 1.055) ** 2.4)


def linear_to_srgb(c):
    """Linear light -> sRGB-encoded [0,1]. Inverse of :func:`srgb_to_linear`."""
    c = np.clip(np.asarray(c, dtype=np.float32), 0.0, None)
    return np.where(c <= 0.0031308, c * 12.92, 1.055 * (c ** (1.0 / 2.4)) - 0.055)


def highlight_rolloff(lin, knee=HIGHLIGHT_KNEE):
    """Smoothly compress linear highlights above ``knee`` toward 1.0.

    Identity for ``lin <= knee``; above the knee a rational (Reinhard-style)
    shoulder asymptotes to 1.0 and never reaches or exceeds it, so highlights
    keep gradient instead of flattening to pure white. The two branches share
    value and first derivative at the knee (C1-continuous), so there is no
    visible seam.
    """
    lin = np.asarray(lin, dtype=np.float32)
    headroom = 1.0 - knee
    over = np.maximum(lin - knee, 0.0)
    rolled = knee + headroom * (over / (over + headroom))
    return np.where(lin > knee, rolled, lin)


def smoothstep(edge0, edge1, x):
    """GLSL-compatible smoothstep for scalar or array inputs."""
    x = np.asarray(x, dtype=np.float32)
    t = np.clip((x - edge0) / (edge1 - edge0), 0.0, 1.0)
    return t * t * (3.0 - 2.0 * t)


def white_balance_gains(white_balance):
    """Return per-channel linear gains (r, g, b) for a white-balance dict.

    ``temperature`` and ``tint`` are in [-100, 100]; both default to 0 (no-op
    gains of 1.0). Coefficients match the original sRGB implementation.
    """
    wb = white_balance or {}
    temperature = float(wb.get("temperature") or 0.0) / 100.0
    tint = float(wb.get("tint") or 0.0) / 100.0
    r = max(WB_MIN_GAIN, 1.0 + WB_TEMP_R * temperature + WB_TINT_R * tint)
    g = max(WB_MIN_GAIN, 1.0 - WB_TINT_G * tint)
    b = max(WB_MIN_GAIN, 1.0 - WB_TEMP_B * temperature + WB_TINT_B * tint)
    return r, g, b


def _luma(rgb):
    return (
        LUMA_R * rgb[..., 0]
        + LUMA_G * rgb[..., 1]
        + LUMA_B * rgb[..., 2]
    )[..., None]


def _unit_amount(amount):
    """Return a scalar/array adjustment normalized from [-100, 100]."""
    return np.asarray(amount, dtype=np.float32) / np.float32(100.0)


def _black_level_curve(level, amount):
    """Monotonic black-point/toe curve with a smooth join at BLACK_PIVOT."""
    amount = _unit_amount(amount)
    positive = np.maximum(amount, 0.0)
    negative = np.maximum(-amount, 0.0)
    t = np.clip(level / np.float32(BLACK_PIVOT), 0.0, 1.0)
    shoulder = (1.0 - t) ** 2
    lift = np.float32(BLACK_PIVOT * BLACK_LIFT) * positive * shoulder
    deepen = (
        np.float32(BLACK_PIVOT * BLACK_DEEPEN)
        * negative * t * shoulder
    )
    return np.where(level < BLACK_PIVOT, level + lift - deepen, level)


def _compress_linear_gamut(rgb, luminance):
    """Fit linear RGB into display gamut while retaining target luminance.

    Uniform luminance scaling preserves chromaticity unless a channel exceeds
    display white.  In that case, reduce chroma toward neutral just enough to
    fit, rather than clipping channels independently and shifting hue.
    """
    max_channel = np.max(rgb, axis=-1, keepdims=True)
    denominator = np.maximum(max_channel - luminance, np.float32(1e-7))
    chroma_scale = np.minimum(
        1.0,
        np.maximum(0.0, (1.0 - luminance) / denominator),
    )
    return luminance + (rgb - luminance) * chroma_scale


def _scene_to_display_linear(rgb):
    """Map scene radiance to display range after exposure and white balance.

    Roll luminance off, retaining channel ratios until gamut compression is
    necessary. Unlike the legacy JPEG push-gate, RAW values above display white
    need this mapping even at zero exposure. A negative exposure is applied
    before the shoulder, so bright source detail remains recoverable.
    """
    luminance = np.maximum(_luma(rgb), 0.0)
    mapped = highlight_rolloff(luminance)
    rgb = rgb * (mapped / np.maximum(luminance, np.float32(1e-7)))
    # Compress both upper and lower gamut boundaries toward the same neutral
    # luminance. RAW's float color conversion can produce negative channels.
    chroma = rgb - mapped
    upper = (1.0 - mapped) / np.maximum(np.max(chroma, axis=-1, keepdims=True), 1e-7)
    lower = mapped / np.maximum(-np.min(chroma, axis=-1, keepdims=True), 1e-7)
    return np.clip(mapped + chroma * np.minimum(1.0, np.minimum(upper, lower)), 0.0, 1.0)


def _apply_whites_blacks_linear(linear, *, whites=0.0, blacks=0.0):
    """Monotonic, chromaticity-preserving Whites/Blacks on display linear RGB.

    Each control transforms perceptual luminance with a monotonic scalar
    curve, so composing them stays monotonic at extreme combinations.
    """
    luminance = _luma(linear)
    level = linear_to_srgb(luminance)
    source_level = level

    if np.any(blacks):
        level = _black_level_curve(level, blacks)
    if np.any(whites):
        level = 1.0 - _black_level_curve(
            1.0 - level, -np.asarray(whites, dtype=np.float32)
        )

    target_luminance = srgb_to_linear(np.clip(level, 0.0, 1.0))
    scale = target_luminance / np.maximum(luminance, np.float32(1e-7))
    mapped = linear * scale
    # A truly black input has no chromaticity to preserve.  Blacks > 0 assigns
    # it the requested neutral lifted endpoint; every non-black pixel follows
    # the chromaticity-preserving scale above.
    mapped = np.where(luminance > 1e-7, mapped, target_luminance)
    chroma_retention = smoothstep(0.0, BLACK_CHROMA_FADE, source_level)
    chroma_retention = chroma_retention * chroma_retention
    chroma_retention = np.where(
        target_luminance > luminance, chroma_retention, 1.0
    )
    mapped = target_luminance + (
        mapped - target_luminance
    ) * chroma_retention
    return _compress_linear_gamut(mapped, target_luminance)


def apply_vibrance(rgb, vibrance=0.0):
    """Apply luma-preserving selective saturation."""
    amount = float(vibrance) / 100.0
    if abs(amount) < 1e-9:
        return rgb
    luma = _luma(rgb)
    maxc = np.max(rgb, axis=-1, keepdims=True)
    minc = np.min(rgb, axis=-1, keepdims=True)
    chroma = np.clip(maxc - minc, 0.0, 1.0)
    if amount > 0:
        factor = 1.0 + amount * (1.0 - chroma) * 0.85
    else:
        factor = 1.0 + amount * 0.65
    return np.clip(luma + (rgb - luma) * factor, 0.0, 1.0)


_TONE_CURVE_POINTS = ("black", "shadows", "midtones", "highlights", "white")
_TONE_CURVE_DEFAULTS = np.array([0.0, 0.25, 0.5, 0.75, 1.0], dtype=np.float32)
_HSL_CENTERS = {
    "red": 0.0,
    "orange": 30.0,
    "yellow": 60.0,
    "green": 120.0,
    "aqua": 180.0,
    "blue": 240.0,
    "purple": 275.0,
    "magenta": 315.0,
}


def apply_tone_curve(rgb, curve):
    """Apply a five-point composite RGB curve in display space.

    The recipe stores output levels at fixed 0/25/50/75/100-percent input
    points. Interpolation is piecewise linear, matching the predictable point
    curve photographers expect while remaining cheap enough for tiled renders.
    """
    if not curve:
        return rgb
    outputs = _TONE_CURVE_DEFAULTS.copy()
    for i, name in enumerate(_TONE_CURVE_POINTS):
        if name in curve:
            outputs[i] = np.float32(float(curve[name]) / 100.0)
    src = np.clip(np.asarray(rgb, dtype=np.float32), 0.0, 1.0)
    position = src * np.float32(4.0)
    lower = np.minimum(position.astype(np.int32), 3)
    fraction = position - lower
    return np.clip(
        outputs[lower] * (1.0 - fraction) + outputs[lower + 1] * fraction,
        0.0, 1.0,
    )


def _rgb_to_hsl(rgb):
    src = np.clip(np.asarray(rgb, dtype=np.float32), 0.0, 1.0)
    maxc = np.max(src, axis=-1)
    minc = np.min(src, axis=-1)
    delta = maxc - minc
    light = (maxc + minc) * np.float32(0.5)
    denom = np.maximum(1.0 - np.abs(2.0 * light - 1.0), 1e-7)
    saturation = np.where(delta > 1e-7, delta / denom, 0.0)
    safe_delta = np.where(delta > 1e-7, delta, 1.0)
    red, green, blue = src[..., 0], src[..., 1], src[..., 2]
    hue = np.zeros_like(light)
    red_max = (maxc == red) & (delta > 1e-7)
    green_max = (maxc == green) & (delta > 1e-7) & ~red_max
    blue_max = (delta > 1e-7) & ~red_max & ~green_max
    hue = np.where(red_max, np.mod((green - blue) / safe_delta, 6.0), hue)
    hue = np.where(green_max, (blue - red) / safe_delta + 2.0, hue)
    hue = np.where(blue_max, (red - green) / safe_delta + 4.0, hue)
    return np.mod(hue / 6.0, 1.0), saturation, light


def _hsl_to_rgb(hue, saturation, light):
    h = np.mod(np.asarray(hue, dtype=np.float32), 1.0)
    s = np.clip(np.asarray(saturation, dtype=np.float32), 0.0, 1.0)
    l = np.clip(np.asarray(light, dtype=np.float32), 0.0, 1.0)
    chroma = (1.0 - np.abs(2.0 * l - 1.0)) * s
    hp = h * 6.0
    x = chroma * (1.0 - np.abs(np.mod(hp, 2.0) - 1.0))
    zero = np.zeros_like(chroma)
    sector = np.floor(hp).astype(np.int32) % 6
    r1 = np.select(
        [sector == 0, sector == 1, sector == 2, sector == 3, sector == 4],
        [chroma, x, zero, zero, x], default=chroma,
    )
    g1 = np.select(
        [sector == 0, sector == 1, sector == 2, sector == 3, sector == 4],
        [x, chroma, chroma, x, zero], default=zero,
    )
    b1 = np.select(
        [sector == 0, sector == 1, sector == 2, sector == 3, sector == 4],
        [zero, zero, x, chroma, chroma], default=x,
    )
    m = l - chroma * np.float32(0.5)
    return np.stack((r1 + m, g1 + m, b1 + m), axis=-1).astype(np.float32)


def apply_hsl_mixer(rgb, mixer):
    """Apply smooth, overlapping per-colour HSL adjustments."""
    if not mixer:
        return rgb
    hue, saturation, light = _rgb_to_hsl(rgb)
    hue_delta = np.zeros_like(hue)
    sat_delta = np.zeros_like(hue)
    lum_delta = np.zeros_like(hue)
    weight_sum = np.zeros_like(hue)
    # A 75-degree triangular support avoids seams between the deliberately
    # non-uniform named colour centers (notably yellow -> green).
    support = np.float32(75.0 / 360.0)
    for color, center_deg in _HSL_CENTERS.items():
        settings = mixer.get(color) or {}
        if not settings:
            continue
        center = np.float32(center_deg / 360.0)
        distance = np.abs(np.mod(hue - center + 0.5, 1.0) - 0.5)
        weight = np.clip(1.0 - distance / support, 0.0, 1.0)
        # Achromatic pixels have no meaningful hue. Fade the band selection to
        # zero near grey so changing (say) Red luminance cannot unexpectedly
        # brighten a neutral wall whose mathematical fallback hue is zero.
        weight *= smoothstep(0.01, 0.08, saturation)
        weight_sum += weight
        hue_delta += weight * np.float32(float(settings.get("hue", 0.0)))
        sat_delta += weight * np.float32(float(settings.get("saturation", 0.0)))
        lum_delta += weight * np.float32(float(settings.get("luminance", 0.0)))
    divisor = np.maximum(weight_sum, 1.0)
    # ±100 Hue corresponds to a ±30-degree shift, enough to cross adjacent
    # colour bands without turning the mixer into a full hue rotation.
    hue = np.mod(hue + (hue_delta / divisor) * np.float32(30.0 / 36000.0), 1.0)
    sat_amount = np.clip(sat_delta / divisor / 100.0, -1.0, 1.0)
    saturation = np.where(
        sat_amount >= 0,
        saturation + (1.0 - saturation) * sat_amount,
        saturation * (1.0 + sat_amount),
    )
    lum_amount = np.clip(lum_delta / divisor / 100.0, -1.0, 1.0)
    light = np.where(
        lum_amount >= 0,
        light + (1.0 - light) * lum_amount,
        light * (1.0 + lum_amount),
    )
    return np.clip(_hsl_to_rgb(hue, saturation, light), 0.0, 1.0)


def apply_point_curves(rgb, curves):
    """Piecewise linear interpolation; composite first, then RGB channels."""
    out = np.asarray(rgb, dtype=np.float32).copy()
    for channel in ("rgb", "red", "green", "blue"):
        points = curves.get(channel)
        if not points:
            continue
        points = np.asarray(points, dtype=np.float32) / 100.0
        if channel == "rgb":
            out = np.interp(out, points[:, 0], points[:, 1]).astype(np.float32)
        else:
            index = ("red", "green", "blue").index(channel)
            out[..., index] = np.interp(out[..., index], points[:, 0], points[:, 1])
    return out


def apply_point_color(rgb, samples):
    """Select input HSL ranges with soft edges, then combine sample deltas.

    Selection is evaluated before any Point Color changes, making overlapping
    samples independent of their order. Hue wraps around red; achromatic pixels
    are protected because their hue is undefined. Range values are half-widths.
    """
    active = [item for item in samples if any(item.get(k, 0) for k in ("hue", "saturation", "luminance"))]
    if not active:
        return rgb
    hue, saturation, light = _rgb_to_hsl(rgb)
    hue_delta = np.zeros_like(hue)
    sat_delta = np.zeros_like(hue)
    lum_delta = np.zeros_like(hue)
    weight_sum = np.zeros_like(hue)
    for item in active:
        center_h, center_s, center_l = item["sample"]
        distance = np.abs(np.mod(hue * 360.0 - center_h + 180.0, 360.0) - 180.0)
        weight = 1.0 - smoothstep(0.0, item.get("hue_range", 30), distance)
        weight *= 1.0 - smoothstep(0.0, item.get("saturation_range", 100), np.abs(saturation * 100.0 - center_s))
        weight *= 1.0 - smoothstep(0.0, item.get("luminance_range", 100), np.abs(light * 100.0 - center_l))
        weight *= smoothstep(0.01, 0.08, saturation)
        weight_sum += weight
        hue_delta += weight * item.get("hue", 0)
        sat_delta += weight * item.get("saturation", 0)
        lum_delta += weight * item.get("luminance", 0)
    divisor = np.maximum(weight_sum, 1.0)
    hue = np.mod(hue + hue_delta / divisor / 360.0, 1.0)
    sat_amount = sat_delta / divisor / 100.0
    lum_amount = lum_delta / divisor / 100.0
    saturation = np.where(sat_amount >= 0, saturation + (1.0 - saturation) * sat_amount,
                          saturation * (1.0 + sat_amount))
    light = np.where(lum_amount >= 0, light + (1.0 - light) * lum_amount,
                     light * (1.0 + lum_amount))
    changed = _hsl_to_rgb(hue, saturation, light)
    # Preserve out-of-range pixels exactly, including RGB/HSL roundoff.
    return np.where((weight_sum > 0)[..., None], changed, rgb)


def apply_color_grading(rgb, grading):
    """Tint shadows, midtones, and highlights while preserving luminance."""
    if not grading:
        return rgb
    out = np.asarray(rgb, dtype=np.float32)
    balance = float(grading.get("balance", 0.0)) / 100.0
    lum = np.clip(_luma(out) + np.float32(balance * 0.25), 0.0, 1.0)
    weights = {
        "shadows": 1.0 - smoothstep(0.12, 0.58, lum),
        "highlights": smoothstep(0.42, 0.88, lum),
    }
    weights["midtones"] = np.clip(
        1.0 - weights["shadows"] - weights["highlights"], 0.0, 1.0
    )
    for zone in ("shadows", "midtones", "highlights"):
        settings = grading.get(zone) or {}
        strength = float(settings.get("saturation", 0.0)) / 100.0
        if strength <= 0.0:
            continue
        hue = np.float32(float(settings.get("hue", 0.0)) / 360.0)
        tint = _hsl_to_rgb(hue, np.float32(1.0), np.float32(0.5))
        tint_luma = (
            LUMA_R * tint[..., 0] + LUMA_G * tint[..., 1] + LUMA_B * tint[..., 2]
        )
        chroma = tint - tint_luma[..., None]
        out = out + chroma * weights[zone] * np.float32(strength * 0.35)
    return np.clip(out, 0.0, 1.0)


def _scene_referred_linear(
    rgb, *, exposure, white_balance, input_linear, shadows=0.0, highlights=0.0,
    range_base=None, origin=(0, 0), ev_shift=None,
):
    """Exposure, white balance, Shadows/Highlights and the display shoulder.

    ``exposure``, ``shadows`` and ``highlights`` may be per-pixel maps (local
    adjustments). ``range_base`` is the frame's :class:`RangeBase` and
    ``origin`` this buffer's (top, left) in that frame; without a base each
    pixel is its own neighborhood. ``ev_shift`` is a pixel's local exposure
    over the global exposure the base was built with.
    """
    lin_pre = rgb if input_linear else srgb_to_linear(rgb)
    lin = lin_pre
    if np.ndim(exposure):
        lin = lin * np.exp2(exposure)
        max_gain = 2.0 ** float(np.max(exposure))
    else:
        max_gain = 2.0 ** float(exposure)
        if exposure:
            lin = lin * np.float32(max_gain)
    gr = gg = gb = 1.0
    if white_balance:
        gr, gg, gb = white_balance_gains(white_balance)
        lin = lin * np.array([gr, gg, gb], dtype=np.float32)
    # Shadows/Highlights run on scene radiance, before the shoulder, so
    # Highlights can bring a RAW's above-white detail back into range.
    if np.any(shadows) or np.any(highlights):
        stops = _range_stops(
            _range_base_log(lin, range_base, origin, ev_shift), shadows, highlights,
        )
        lin = lin * np.exp2(stops)
        max_gain *= 2.0 ** float(np.max(stops))
    # RAW always needs a display transform. Legacy display inputs retain the
    # push-only shoulder so an unadjusted JPEG remains a true no-op.
    if input_linear:
        lin = _scene_to_display_linear(lin)
    elif max_gain * max(gr, gg, gb) > 1.0 + 1e-6:
        # The shoulder curve is below identity for inputs in (knee, ∞), so
        # naively rolling every post-gain value above the knee can darken
        # near-white pixels — e.g. a 0.95-linear pixel at +0.1 EV becomes
        # 1.018 post-gain and the shoulder maps that to 0.929. That violates
        # monotonicity in exposure: a positive boost must never lower a
        # pixel's output. Clamp per channel to the channel's "would-be" value
        # without rolloff: that's lin_pre when the channel's net gain >= 1
        # (the channel can only brighten, so its floor is the input), and
        # the post-gain value itself when the channel's net gain < 1 (e.g.
        # a negative-tint blue channel that WB legitimately reduces).
        # ``min(lin_pre, lin)`` collapses to whichever is the natural floor.
        rolled = highlight_rolloff(lin)
        lin = np.maximum(rolled, np.minimum(lin_pre, lin))
    return lin


def _display_referred_ops(
    disp, *, contrast, tone_curve, point_curves, hsl, color_grading,
    vibrance, saturation,
):
    """Contrast, curves, HSL, grading, vibrance and saturation, in sRGB."""
    if contrast:
        c = np.float32(1.0 + float(contrast) / 100.0)
        disp = (disp - 0.5) * c + 0.5
    if tone_curve:
        disp = apply_tone_curve(disp, tone_curve)
    if point_curves:
        disp = apply_point_curves(disp, point_curves)
    if hsl:
        disp = apply_hsl_mixer(disp, hsl)
    if color_grading:
        disp = apply_color_grading(disp, color_grading)
    if vibrance:
        disp = apply_vibrance(disp, vibrance)
    if saturation:
        s = np.float32(max(0.0, 1.0 + float(saturation) / 100.0))
        luma = _luma(disp)
        disp = luma + (disp - luma) * s
    return disp


@dataclass(frozen=True)
class ToneAdjustments:
    """The global tone and color controls of an edit recipe.

    Each field is a recipe ``adjustments`` key; the defaults leave the
    image unchanged. See :func:`apply_adjustments` for what each does.
    """

    exposure: float = 0.0
    white_balance: Mapping[str, Any] | None = None
    highlights: float = 0.0
    shadows: float = 0.0
    whites: float = 0.0
    blacks: float = 0.0
    contrast: float = 0.0
    vibrance: float = 0.0
    saturation: float = 0.0
    tone_curve: Any = None
    point_curves: Any = None
    point_color: Any = None
    hsl: Any = None
    color_grading: Any = None

    @classmethod
    def from_mapping(cls, adjustments: Mapping[str, Any]) -> ToneAdjustments:
        """The controls present in a recipe's ``adjustments`` dict.

        Keys that are not tone controls are ignored; a missing key keeps the
        field's default, the same as ``adjustments.get(name, default)``.
        """
        return cls(**{f.name: adjustments[f.name] for f in fields(cls) if f.name in adjustments})


@dataclass(frozen=True)
class LocalAdjustments:
    """Mask-weighted deltas on top of a :class:`ToneAdjustments`.

    ``weight`` is the per-pixel subject weight shaped like the image's
    spatial dims; ``subject`` / ``background`` map control names (exposure,
    highlights, shadows, contrast, saturation) to deltas applied at weight
    ``w`` and ``1 - w``. With neither delta set the global path runs.
    """

    weight: Any
    subject: Mapping[str, Any] | None = None
    background: Mapping[str, Any] | None = None


def apply_adjustments(
    rgb,
    adjustments: ToneAdjustments | None = None,
    *,
    local: LocalAdjustments | None = None,
    input_linear=False,
    range_sigma=0.0,
    range_base: RangeBase | None = None,
    origin=(0, 0),
):
    """Apply tonal adjustments to an sRGB float image and return sRGB float.

    Args:
        rgb: float array shaped ``(..., 3)``. Normally sRGB-encoded [0,1];
            ``input_linear=True`` accepts scene-linear RAW data above white.
        adjustments: the global controls (``ToneAdjustments``, fields below);
            None leaves every control at its default.
        local: mask-weighted deltas (``LocalAdjustments``), or None for the
            global-only path; see below.
        input_linear: True when ``rgb`` is scene-linear rather than sRGB.
        range_sigma: blur of the Shadows/Highlights base in ``rgb``'s pixels
            (see :func:`range_sigma`), built from ``rgb`` itself; zero makes
            each pixel its own neighborhood.
        range_base: a base already built over the whole frame, for a tile
            of it at ``origin`` (top, left); takes precedence over
            ``range_sigma``.

    ``ToneAdjustments`` fields:
        exposure: stops of exposure (linear gain ``2 ** exposure``).
        white_balance: dict with ``temperature``/``tint`` in [-100, 100], or None.
        highlights: [-100, 100]; local exposure of bright neighborhoods,
            up to ``RANGE_STOPS`` stops.
        shadows: [-100, 100]; local exposure of dark neighborhoods, up to
            ``RANGE_STOPS`` stops.
        whites: [-100, 100]; perceptual-luminance white point adjustment.
        blacks: [-100, 100]; perceptual-luminance black point adjustment.
        contrast: [-100, 100]; a linear contrast around mid-grey (0.5).
        vibrance: [-100, 100]; luma-preserving selective saturation.
        saturation: [-100, 100]; luma-preserving saturation in display space.
        tone_curve: five composite RGB output points at fixed input levels.
        hsl: per-colour hue, saturation, and luminance adjustments.
        point_curves: composite and RGB channel [input, output] control points.
        point_color: sampled HSL colors with selection ranges and adjustments.
        color_grading: shadow/midtone/highlight hue and saturation tints.

    Returns:
        float32 array, same shape, sRGB-encoded and clipped to [0,1].

    Local (mask-weighted) adjustments: ``local.weight`` is a per-pixel
    subject weight shaped ``(..., )`` matching the image's spatial dims;
    ``local.subject`` / ``local.background`` are dicts of *deltas* on top of
    the global values (keys: exposure, highlights, shadows, contrast,
    saturation). Effective per-pixel amounts are
    ``global + subject·w + background·(1−w)``, clamped to each control's
    global range. The weight is an extra per-pixel *input*, not a
    neighborhood op. The Shadows/Highlights base is built from the image
    under the global exposure and white balance, never the mask, while the
    adjustment amounts remain per-pixel. Without local inputs, execution uses
    the global-only path.
    """
    rgb = np.asarray(rgb, dtype=np.float32)
    if adjustments is None:
        adjustments = ToneAdjustments()
    if local is not None and (local.weight is None or not (local.subject or local.background)):
        local = None
    if range_base is None and range_sigma:
        range_base = _build_range_base(
            rgb, adjustments, local, input_linear=input_linear, sigma=range_sigma,
        )

    if local is not None:
        return _apply_adjustments_weighted(
            rgb, adjustments, local,
            input_linear=input_linear,
            range_base=range_base,
            origin=origin,
        )

    # --- scene-referred ops, in linear light ---
    a = adjustments
    lin = _scene_referred_linear(
        rgb, exposure=a.exposure, white_balance=a.white_balance,
        input_linear=input_linear, shadows=a.shadows, highlights=a.highlights,
        range_base=range_base, origin=origin,
    )

    # --- perceptually placed endpoint curves, reconstructed in linear RGB ---
    if a.whites or a.blacks:
        lin = _apply_whites_blacks_linear(lin, whites=a.whites, blacks=a.blacks)

    # --- display-referred ops, in sRGB ---
    disp = linear_to_srgb(lin)
    disp = _display_referred_ops(
        disp,
        contrast=adjustments.contrast,
        tone_curve=adjustments.tone_curve,
        point_curves=adjustments.point_curves,
        hsl=adjustments.hsl,
        color_grading=adjustments.color_grading,
        vibrance=adjustments.vibrance,
        saturation=adjustments.saturation,
    )

    # Point Color samples this fully adjusted color, before spatial detail.
    # Keeping it last lets the picker render its exact input by omitting only
    # point_color and the detail controls from the current recipe.
    if adjustments.point_color:
        disp = apply_point_color(disp, adjustments.point_color)
    return np.clip(disp, 0.0, 1.0)


def _build_range_base(rgb, adjustments, local, *, input_linear, sigma):
    """A base over ``rgb`` when its Shadows/Highlights need one, else None."""
    regions = (local.subject or {}, local.background or {}) if local is not None else ()
    active = adjustments.shadows or adjustments.highlights or any(
        region.get("shadows") or region.get("highlights") for region in regions
    )
    if not active or rgb.ndim != 3:
        return None
    return RangeBase.build(
        rgb, sigma, exposure=adjustments.exposure,
        white_balance=adjustments.white_balance, input_linear=input_linear,
    )


# Global ranges the weighted amounts clamp to — the local composition
# contract is `clamp(global + subject·w + background·(1−w))` per control.
_LOCAL_CLAMPS = {
    "exposure": (-5.0, 5.0),
    "highlights": (-100.0, 100.0),
    "shadows": (-100.0, 100.0),
    "contrast": (-100.0, 100.0),
    "saturation": (-100.0, 100.0),
}


def _apply_adjustments_weighted(
    rgb, adjustments: ToneAdjustments, local: LocalAdjustments, *,
    input_linear=False, range_base=None, origin=(0, 0),
):
    """Local (mask-weighted) variant of :func:`apply_adjustments`.

    Controls without local deltas run the same scalar math as the global
    path; controls with deltas run the identical formulas with a per-pixel
    amount map. Shadows/Highlights share the global path's base, moved by
    each pixel's local exposure.
    """
    a = adjustments
    subject, background = local.subject or {}, local.background or {}
    w = np.asarray(local.weight, dtype=np.float32)[..., None]

    def amount_map(name, global_value):
        """Per-pixel amount for a control, or None to use the scalar path."""
        s = float(subject.get(name) or 0.0)
        b = float(background.get(name) or 0.0)
        if abs(s) < 1e-9 and abs(b) < 1e-9:
            return None
        lo, hi = _LOCAL_CLAMPS[name]
        return np.clip(
            np.float32(global_value) + np.float32(s) * w + np.float32(b) * (1.0 - w),
            lo, hi,
        )

    # --- scene-referred ops, in linear light ---
    ev_map = amount_map("exposure", a.exposure)
    hi_amt = amount_map("highlights", a.highlights)
    sh_amt = amount_map("shadows", a.shadows)
    lin = _scene_referred_linear(
        rgb,
        exposure=a.exposure if ev_map is None else ev_map,
        white_balance=a.white_balance,
        input_linear=input_linear,
        shadows=a.shadows if sh_amt is None else sh_amt,
        highlights=a.highlights if hi_amt is None else hi_amt,
        range_base=range_base,
        origin=origin,
        ev_shift=None if ev_map is None else ev_map[..., 0] - np.float32(a.exposure),
    )

    # --- perceptually placed endpoint curves, reconstructed in linear RGB ---
    if a.whites or a.blacks:
        lin = _apply_whites_blacks_linear(lin, whites=a.whites, blacks=a.blacks)

    # --- display-referred ops, in sRGB ---
    disp = linear_to_srgb(lin)
    c_amt = amount_map("contrast", a.contrast)
    if c_amt is not None:
        disp = (disp - 0.5) * (1.0 + c_amt / 100.0) + 0.5
    elif a.contrast:
        c = np.float32(1.0 + float(a.contrast) / 100.0)
        disp = (disp - 0.5) * c + 0.5
    if a.tone_curve:
        disp = apply_tone_curve(disp, a.tone_curve)
    if a.point_curves:
        disp = apply_point_curves(disp, a.point_curves)
    if a.hsl:
        disp = apply_hsl_mixer(disp, a.hsl)
    if a.color_grading:
        disp = apply_color_grading(disp, a.color_grading)
    if a.vibrance:
        disp = apply_vibrance(disp, a.vibrance)
    s_amt = amount_map("saturation", a.saturation)
    if s_amt is not None:
        s = np.maximum(0.0, 1.0 + s_amt / 100.0).astype(np.float32)
        luma = _luma(disp)
        disp = luma + (disp - luma) * s
    elif a.saturation:
        s = np.float32(max(0.0, 1.0 + float(a.saturation) / 100.0))
        luma = _luma(disp)
        disp = luma + (disp - luma) * s

    # Point Color samples this fully adjusted color, before spatial detail.
    # Keeping it last lets the picker render its exact input by omitting only
    # point_color and the detail controls from the current recipe.
    if a.point_color:
        disp = apply_point_color(disp, a.point_color)
    return np.clip(disp, 0.0, 1.0)
