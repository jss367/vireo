"""Guards that the WebGL live-preview shader matches the server tone pipeline.

The live preview in ``_navbar.html`` (``VireoToneGL``) is a GLSL transcription
of :mod:`tone`. This test mirrors that GLSL arithmetic in numpy and asserts it
reproduces ``tone.apply_adjustments`` for the **first-edit case only** — the
displayed image has no baked adjustments (base == zeros), so the previewed
"delta" equals the full recipe and the two pipelines must agree exactly.

It deliberately does *not* claim parity for re-edits: there the preview applies
a delta on top of already tone-mapped pixels, and the highlight rolloff /
clamping are neither reversible nor associative, so the preview is only an
approximation that snaps to the exact server render after save. We test the
exact case because it's the one the two formulas are supposed to match; if they
drift, that case fails.

It can't execute the actual shader headlessly, but it locks the *formula*:
sRGB<->linear transfer, the highlight-knee rolloff, the white-balance gains and
push-gate, and the display-space range/color controls — in the same order.
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

    def blend_range(rgb, amount, mask):
        a = amount / 100.0
        m = abs(a) * mask
        if a > 0:
            return rgb + (1.0 - rgb) * m
        return rgb - rgb * m

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
    disp = linear_to_srgb(lin)
    range_luma = disp @ np.array([0.2126, 0.7152, 0.0722])
    if shadows:
        disp = blend_range(disp, shadows, (1.0 - tone.smoothstep(0.05, 0.65, range_luma))[..., None] * 0.48)
    if highlights:
        disp = blend_range(disp, highlights, tone.smoothstep(0.35, 0.95, range_luma)[..., None] * 0.42)
    if blacks:
        disp = blend_range(disp, blacks, (1.0 - tone.smoothstep(0.00, 0.30, range_luma))[..., None] * 0.34)
    if whites:
        disp = blend_range(disp, whites, tone.smoothstep(0.70, 1.00, range_luma)[..., None] * 0.34)
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

        # The dispatcher (_lbApplyAdjustmentPreview) derives these from a base of
        # all-zeros for a first edit, so the deltas equal the full values.
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
