"""Scene-referred tone pipeline for non-destructive photo edits.

This module is the single source of truth for how the editable adjustments
(exposure, white balance, contrast, saturation) map input pixels to output
pixels. Geometry (rotate/flip/straighten/crop) lives in ``image_edits`` and is
not part of this pipeline.

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
  5. applies the display-referred ops (contrast, saturation) in sRGB.

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


def apply_adjustments(
    rgb,
    *,
    exposure=0.0,
    white_balance=None,
    contrast=0.0,
    saturation=0.0,
):
    """Apply tonal adjustments to an sRGB float image and return sRGB float.

    Args:
        rgb: float array shaped ``(..., 3)`` with sRGB-encoded values in [0,1].
        exposure: stops of exposure (linear gain ``2 ** exposure``).
        white_balance: dict with ``temperature``/``tint`` in [-100, 100], or None.
        contrast: [-100, 100]; a linear contrast around mid-grey (0.5).
        saturation: [-100, 100]; luma-preserving saturation in display space.

    Returns:
        float32 array, same shape, sRGB-encoded and clipped to [0,1].
    """
    rgb = np.asarray(rgb, dtype=np.float32)

    # --- scene-referred ops, in linear light ---
    lin = srgb_to_linear(rgb)
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
        lin = highlight_rolloff(lin)

    # --- display-referred ops, in sRGB ---
    disp = linear_to_srgb(lin)
    if contrast:
        c = np.float32(1.0 + float(contrast) / 100.0)
        disp = (disp - 0.5) * c + 0.5
    if saturation:
        s = np.float32(max(0.0, 1.0 + float(saturation) / 100.0))
        luma = (
            LUMA_R * disp[..., 0]
            + LUMA_G * disp[..., 1]
            + LUMA_B * disp[..., 2]
        )[..., None]
        disp = luma + (disp - luma) * s

    return np.clip(disp, 0.0, 1.0)
