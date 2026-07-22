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

Data ceiling: this operates on whatever RGB source it is handed. JPEGs and
legacy working copies are still 8-bit and cannot *recover* highlights that were
already clipped upstream. New RAW working copies and edited RAW renders ask
``image_loader`` to demosaic with auto-bright disabled and highlight blending
enabled before quantizing to the JPEG working/render cache. That preserves more
RAW highlight headroom for this tone curve, but it is still not a full
scene-linear 16-bit editing pipeline.

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
    """Move masked pixels toward white or black without hard clipping.

    ``amount`` is a scalar in [-100, 100], or a per-pixel array of the same
    (broadcastable) shape for local (mask-weighted) adjustments.
    """
    if np.ndim(amount) > 0:
        amount = np.asarray(amount, dtype=np.float32) / 100.0
        scaled = mask * np.abs(amount)
        return np.where(
            amount > 0, rgb + (1.0 - rgb) * scaled, rgb - rgb * scaled
        )
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

    if np.any(shadows):
        out = _blend_range(out, shadows, (1.0 - smoothstep(0.05, 0.65, lum)) * 0.48)
    if np.any(highlights):
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
    tone_curve=None,
    hsl=None,
    color_grading=None,
    local_weight=None,
    local_subject=None,
    local_background=None,
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
        tone_curve: five composite RGB output points at fixed input levels.
        hsl: per-colour hue, saturation, and luminance adjustments.
        color_grading: shadow/midtone/highlight hue and saturation tints.

    Returns:
        float32 array, same shape, sRGB-encoded and clipped to [0,1].

    Local (mask-weighted) adjustments: ``local_weight`` is a per-pixel
    subject weight shaped ``(..., )`` matching the image's spatial dims;
    ``local_subject`` / ``local_background`` are dicts of *deltas* on top of
    the global values (keys: exposure, highlights, shadows, contrast,
    saturation). Effective per-pixel amounts are
    ``global + subject·w + background·(1−w)``, clamped to each control's
    global range. The weight is an extra per-pixel *input*, not a
    neighborhood op, so this pipeline (and its future shader transcription)
    stays strictly per-pixel. When no local inputs are given, execution
    takes the original global-only path unchanged — byte-identical output.
    """
    rgb = np.asarray(rgb, dtype=np.float32)

    subject = local_subject or {}
    background = local_background or {}
    if local_weight is not None and (subject or background):
        return _apply_adjustments_weighted(
            rgb,
            weight=np.asarray(local_weight, dtype=np.float32),
            subject=subject,
            background=background,
            exposure=exposure,
            white_balance=white_balance,
            highlights=highlights,
            shadows=shadows,
            whites=whites,
            blacks=blacks,
            contrast=contrast,
            vibrance=vibrance,
            saturation=saturation,
            tone_curve=tone_curve,
            hsl=hsl,
            color_grading=color_grading,
        )

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
    if tone_curve:
        disp = apply_tone_curve(disp, tone_curve)
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

    return np.clip(disp, 0.0, 1.0)


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
    rgb, *, weight, subject, background, exposure, white_balance,
    highlights, shadows, whites, blacks, contrast, vibrance, saturation,
    tone_curve, hsl, color_grading,
):
    """Local (mask-weighted) variant of :func:`apply_adjustments`.

    Kept as a separate branch so the global-only path above stays literally
    unchanged (its byte-exactness backs the no-cache-purge guarantee and the
    shader-parity test). Controls without local deltas run the same scalar
    math as the global path; controls with deltas run the identical formulas
    with a per-pixel amount map.
    """
    w = weight[..., None]

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
    lin_pre = srgb_to_linear(rgb)
    lin = lin_pre
    ev_map = amount_map("exposure", exposure)
    if ev_map is not None:
        lin = lin * np.exp2(ev_map)
        max_gain = 2.0 ** float(np.max(ev_map))
    else:
        exp_gain = 2.0 ** float(exposure)
        if exposure:
            lin = lin * np.float32(exp_gain)
        max_gain = exp_gain
    gr = gg = gb = 1.0
    if white_balance:
        gr, gg, gb = white_balance_gains(white_balance)
        lin = lin * np.array([gr, gg, gb], dtype=np.float32)
    # Same push-gate and monotonicity clamp as the global path; with a
    # per-pixel gain the clamp is already per-pixel-correct, and gating on
    # the maximum gain only controls whether the shoulder runs at all.
    if max_gain * max(gr, gg, gb) > 1.0 + 1e-6:
        rolled = highlight_rolloff(lin)
        lin = np.maximum(rolled, np.minimum(lin_pre, lin))

    # --- display-referred ops, in sRGB ---
    disp = linear_to_srgb(lin)
    hi_amt = amount_map("highlights", highlights)
    sh_amt = amount_map("shadows", shadows)
    hi_arg = highlights if hi_amt is None else hi_amt
    sh_arg = shadows if sh_amt is None else sh_amt
    if np.any(hi_arg) or np.any(sh_arg) or whites or blacks:
        disp = apply_range_adjustments(
            disp,
            highlights=hi_arg,
            shadows=sh_arg,
            whites=whites,
            blacks=blacks,
        )
    c_amt = amount_map("contrast", contrast)
    if c_amt is not None:
        disp = (disp - 0.5) * (1.0 + c_amt / 100.0) + 0.5
    elif contrast:
        c = np.float32(1.0 + float(contrast) / 100.0)
        disp = (disp - 0.5) * c + 0.5
    if tone_curve:
        disp = apply_tone_curve(disp, tone_curve)
    if hsl:
        disp = apply_hsl_mixer(disp, hsl)
    if color_grading:
        disp = apply_color_grading(disp, color_grading)
    if vibrance:
        disp = apply_vibrance(disp, vibrance)
    s_amt = amount_map("saturation", saturation)
    if s_amt is not None:
        s = np.maximum(0.0, 1.0 + s_amt / 100.0).astype(np.float32)
        luma = _luma(disp)
        disp = luma + (disp - luma) * s
    elif saturation:
        s = np.float32(max(0.0, 1.0 + float(saturation) / 100.0))
        luma = _luma(disp)
        disp = luma + (disp - luma) * s

    return np.clip(disp, 0.0, 1.0)
