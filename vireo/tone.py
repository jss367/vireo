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
  2. applies the scene-referred ops (exposure, white balance) in linear,
  3. rolls highlights off with a smooth shoulder (no hard clip to 1.0),
  4. re-encodes linear -> sRGB,
  5. applies the display-referred ops (range controls, contrast, color) in sRGB.

Data ceiling: this operates on whatever 8-bit source it is handed. It produces
a pleasing rolloff but cannot *recover* highlights that were already clipped
upstream (e.g. in a camera JPEG or an auto-brightened RAW decode). Recovering
real highlight detail requires decoding RAW to linear with auto-bright off
(a separate, larger change).

Keep-in-sync contract (Tier 3 / live preview)
---------------------------------------------
Every function here is strictly per-pixel: each output channel depends only on
the input pixel's own channels and the scalar parameters. That means this maps
1:1 onto a GLSL fragment shader for the live WebGL preview. When the preview is
ported, transcribe these functions verbatim and reuse the constants below; the
CSS-``filter`` preview in ``_navbar.html`` cannot express the linear-light
rolloff and only approximates this render.
"""

from __future__ import annotations

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


def _blend_range(rgb, amount, mask):
    """Move masked pixels toward white or black without hard clipping."""
    amount = float(amount) / 100.0
    if abs(amount) < 1e-9:
        return rgb
    mask = mask * abs(amount)
    if amount > 0:
        return rgb + (1.0 - rgb) * mask
    return rgb - rgb * mask


def apply_range_adjustments(
    rgb, *, highlights=0.0, shadows=0.0, whites=0.0, blacks=0.0
):
    """Apply display-space tonal range controls with smooth luma masks."""
    out = np.asarray(rgb, dtype=np.float32)
    lum = _luma(out)

    if shadows:
        out = _blend_range(out, shadows, (1.0 - smoothstep(0.05, 0.65, lum)) * 0.48)
    if highlights:
        out = _blend_range(out, highlights, smoothstep(0.35, 0.95, lum) * 0.42)
    if blacks:
        out = _blend_range(out, blacks, (1.0 - smoothstep(0.00, 0.30, lum)) * 0.34)
    if whites:
        out = _blend_range(out, whites, smoothstep(0.70, 1.00, lum) * 0.34)

    return np.clip(out, 0.0, 1.0)


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


def apply_adjustments(
    rgb,
    *,
    exposure=0.0,
    white_balance=None,
    highlights=0.0,
    shadows=0.0,
    whites=0.0,
    blacks=0.0,
    contrast=0.0,
    vibrance=0.0,
    saturation=0.0,
):
    """Apply tonal adjustments to an sRGB float image and return sRGB float.

    Args:
        rgb: float array shaped ``(..., 3)`` with sRGB-encoded values in [0,1].
        exposure: stops of exposure (linear gain ``2 ** exposure``).
        white_balance: dict with ``temperature``/``tint`` in [-100, 100], or None.
        highlights: [-100, 100]; display-space highlight range adjustment.
        shadows: [-100, 100]; display-space shadow range adjustment.
        whites: [-100, 100]; display-space white point range adjustment.
        blacks: [-100, 100]; display-space black point range adjustment.
        contrast: [-100, 100]; a linear contrast around mid-grey (0.5).
        vibrance: [-100, 100]; luma-preserving selective saturation.
        saturation: [-100, 100]; luma-preserving saturation in display space.

    Returns:
        float32 array, same shape, sRGB-encoded and clipped to [0,1].
    """
    rgb = np.asarray(rgb, dtype=np.float32)

    # --- scene-referred ops, in linear light ---
    lin_pre = srgb_to_linear(rgb)
    lin = lin_pre
    exp_gain = 2.0 ** float(exposure)
    if exposure:
        lin = lin * np.float32(exp_gain)
    gr = gg = gb = 1.0
    if white_balance:
        gr, gg, gb = white_balance_gains(white_balance)
        lin = lin * np.array([gr, gg, gb], dtype=np.float32)
    # Roll highlights off only when something actually pushes values up. With
    # no net gain, nothing can exceed display white, so the shoulder must stay
    # disabled to keep an un-pushed image a true no-op (ev=0 == original).
    if exp_gain * max(gr, gg, gb) > 1.0 + 1e-6:
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

    # --- display-referred ops, in sRGB ---
    disp = linear_to_srgb(lin)
    if highlights or shadows or whites or blacks:
        disp = apply_range_adjustments(
            disp,
            highlights=highlights,
            shadows=shadows,
            whites=whites,
            blacks=blacks,
        )
    if contrast:
        c = np.float32(1.0 + float(contrast) / 100.0)
        disp = (disp - 0.5) * c + 0.5
    if vibrance:
        disp = apply_vibrance(disp, vibrance)
    if saturation:
        s = np.float32(max(0.0, 1.0 + float(saturation) / 100.0))
        luma = _luma(disp)
        disp = luma + (disp - luma) * s

    return np.clip(disp, 0.0, 1.0)
