"""Guards that the WebGL live-preview shader matches the server tone pipeline.

The live preview in ``_navbar.html`` (``VireoToneGL``) is a GLSL transcription
of :mod:`tone`. This test mirrors that GLSL arithmetic in numpy and asserts it
reproduces ``tone.apply_adjustments`` for a neutral source with the complete
recipe. Quick adjustments use that same source across saves and re-edits;
browser tests cover the actual GPU pixels through repeated slider changes.

It can't execute the actual shader headlessly, but it locks the *formula*:
sRGB<->linear transfer, the highlight-knee rolloff, the white-balance gains and
push-gate, the perceptual-luminance range curves, and display-space color
controls — in the same order.
"""

import os
import re
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import tone

NAVBAR = os.path.join(
    os.path.dirname(__file__), '..', 'templates', '_navbar.html'
)

KNEE = 0.85  # must equal VireoToneGL KNEE and tone.HIGHLIGHT_KNEE


def test_shader_knee_constant_matches_tone():
    # The local KNEE used by _shader_mirror must equal the server constant...
    assert KNEE == tone.HIGHLIGHT_KNEE
    # ...and the literal baked into the actual shader must match too, so the
    # template can't silently drift away from tone.HIGHLIGHT_KNEE.
    with open(NAVBAR, encoding='utf-8') as f:
        src = f.read()
    match = re.search(r'var\s+KNEE\s*=\s*([0-9.]+)\s*;', src)
    assert match, 'could not find `var KNEE = ...;` in VireoToneGL'
    assert float(match.group(1)) == tone.HIGHLIGHT_KNEE


def test_shader_range_curve_constants_match_tone():
    with open(NAVBAR, encoding='utf-8') as f:
        src = f.read()
    expected_fragments = (
        f"x / {tone.SHADOW_PIVOT:.2f}",
        f"{tone.SHADOW_LIFT:.1f} * positive - "
        f"{tone.SHADOW_DEEPEN:.2f} * negative",
        f"x / {tone.BLACK_PIVOT:.2f}",
        f"{tone.BLACK_PIVOT:.2f} * {tone.BLACK_LIFT:.2f} * positive",
        f"{tone.BLACK_PIVOT:.2f} * {tone.BLACK_DEEPEN:.2f} * negative",
        f"smoothstep(0.0, {tone.BLACK_CHROMA_FADE:.10f}, sourceLevel)",
    )
    for fragment in expected_fragments:
        assert fragment in src, f"shader is missing tone.py constant: {fragment}"


def _shader_mirror(
    c,
    exposure,
    wb_gain,
    highlights,
    shadows,
    whites,
    blacks,
    contrast,
    vibrance,
    saturation,
    rolloff,
):
    """numpy mirror of the GLSL fragment shader in VireoToneGL."""
    def srgb_to_linear(x):
        # GLSL: mix(x/12.92, pow((x+0.055)/1.055, 2.4), step(0.04045, x))
        return np.where(x >= 0.04045, ((x + 0.055) / 1.055) ** 2.4, x / 12.92)

    def linear_to_srgb(x):
        x = np.maximum(x, 0.0)
        return np.where(x >= 0.0031308, 1.055 * x ** (1.0 / 2.4) - 0.055, x * 12.92)

    def roll(x):
        h = 1.0 - KNEE
        o = np.maximum(x - KNEE, 0.0)
        r = KNEE + h * (o / (o + h))
        return np.where(x > KNEE, r, x)

    def shadow_level_curve(x, amount):
        a = amount / 100.0
        positive = max(a, 0.0)
        negative = max(-a, 0.0)
        t = np.clip(x / 0.65, 0.0, 1.0)
        basis = t * (1.0 - t) ** 3
        delta = 0.65 * basis * (3.0 * positive - 0.85 * negative)
        return np.where(x < 0.65, x + delta, x)

    def black_level_curve(x, amount):
        a = amount / 100.0
        positive = max(a, 0.0)
        negative = max(-a, 0.0)
        t = np.clip(x / 0.30, 0.0, 1.0)
        shoulder = (1.0 - t) ** 2
        lift = 0.30 * 0.40 * positive * shoulder
        deepen = 0.30 * 0.90 * negative * t * shoulder
        return np.where(x < 0.30, x + lift - deepen, x)

    def apply_range_curves(range_lin):
        luminance = range_lin @ np.array([0.2126, 0.7152, 0.0722])
        level = linear_to_srgb(luminance)
        source_level = level
        level = shadow_level_curve(level, shadows)
        level = 1.0 - shadow_level_curve(1.0 - level, -highlights)
        level = black_level_curve(level, blacks)
        level = 1.0 - black_level_curve(1.0 - level, -whites)
        target = srgb_to_linear(np.clip(level, 0.0, 1.0))
        mapped = np.where(
            (luminance > 1e-7)[..., None],
            range_lin * (target / np.maximum(luminance, 1e-7))[..., None],
            target[..., None],
        )
        fade_t = np.clip(source_level / (4.0 / 255.0), 0.0, 1.0)
        chroma_retention = fade_t * fade_t * (3.0 - 2.0 * fade_t)
        chroma_retention *= chroma_retention
        chroma_retention = np.where(target > luminance, chroma_retention, 1.0)
        mapped = target[..., None] + (
            mapped - target[..., None]
        ) * chroma_retention[..., None]
        max_channel = np.max(mapped, axis=-1)
        denominator = np.maximum(max_channel - target, 1e-7)
        chroma_scale = np.clip((1.0 - target) / denominator, 0.0, 1.0)
        mapped = target[..., None] + (
            mapped - target[..., None]
        ) * chroma_scale[..., None]
        return np.clip(mapped, 0.0, 1.0)

    def apply_vibrance(rgb, amount):
        a = amount / 100.0
        if abs(a) < 1e-9:
            return rgb
        lum = (rgb @ np.array([0.2126, 0.7152, 0.0722]))[..., None]
        chroma = np.clip(
            np.max(rgb, axis=-1, keepdims=True) - np.min(rgb, axis=-1, keepdims=True),
            0.0,
            1.0,
        )
        factor = np.where(a > 0, 1.0 + a * (1.0 - chroma) * 0.85, 1.0 + a * 0.65)
        return np.clip(lum + (rgb - lum) * factor, 0.0, 1.0)

    lin_pre = srgb_to_linear(c)
    lin = lin_pre * (2.0 ** exposure) * np.asarray(wb_gain)
    if rolloff:
        # Per-channel clamp matches the shader's `max(rolled, min(linPre, lin))`
        # which keeps the shoulder from darkening pixels below their natural
        # (unrolled) value — see the monotonicity note in apply_adjustments.
        lin = np.maximum(roll(lin), np.minimum(lin_pre, lin))
    if shadows or highlights or blacks or whites:
        lin = apply_range_curves(lin)
    disp = linear_to_srgb(lin)
    disp = (disp - 0.5) * contrast + 0.5
    disp = apply_vibrance(disp, vibrance)
    luma = disp @ np.array([0.2126, 0.7152, 0.0722])
    disp = luma[..., None] + (disp - luma[..., None]) * saturation
    return np.clip(disp, 0.0, 1.0)


def test_shader_matches_tone_for_full_recipe():
    rng = np.random.default_rng(1)
    px = rng.random((256, 3)).astype(np.float32)
    cases = [
        (0.0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        (1.5, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        (2.0, 60, -30, -25, 35, 10, -15, 10, 20, 20),
        (-1.0, 0, 0, 20, -30, -25, 18, 40, 0, -50),
        (0.0, 0, 0, 0, 0, 0, 0, 0, 30, -100),
        (3.0, -40, 25, -40, 45, 20, -25, -20, 35, 35),
    ]
    for ev, temp, tint, hi, sh, wh, bl, con, vib, sat in cases:
        wb = {"temperature": temp, "tint": tint} if (temp or tint) else None
        expected = tone.apply_adjustments(
            px[None, :, :],
            exposure=ev,
            white_balance=wb,
            highlights=hi,
            shadows=sh,
            whites=wh,
            blacks=bl,
            contrast=con,
            vibrance=vib,
            saturation=sat,
        )[0]

        # The dispatcher applies the complete values to a neutral source.
        gr, gg, gb = tone.white_balance_gains(wb)
        pushed = (2.0 ** ev) * max(gr, gg, gb) > 1.0 + 1e-6
        got = _shader_mirror(
            px,
            exposure=ev,
            wb_gain=(gr, gg, gb),
            highlights=hi,
            shadows=sh,
            whites=wh,
            blacks=bl,
            contrast=max(0.0, 1.0 + con / 100.0),
            vibrance=vib,
            saturation=max(0.0, 1.0 + sat / 100.0),
            rolloff=pushed,
        )
        max_err = float(np.max(np.abs(got - expected)))
        assert max_err < 2e-4, (ev, temp, tint, hi, sh, wh, bl, con, vib, sat, max_err)
