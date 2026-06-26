"""Guards that the WebGL live-preview shader matches the server tone pipeline.

The live preview in ``_navbar.html`` (``VireoToneGL``) is a GLSL transcription
of :mod:`tone`. This test mirrors that GLSL arithmetic in numpy and asserts it
reproduces ``tone.apply_adjustments`` for the first-edit case the shader is
exact on (the displayed image has no baked adjustments, so the previewed
"delta" equals the full recipe). If the two pipelines drift, this fails.

It can't execute the actual shader headlessly, but it locks the *formula*:
sRGB<->linear transfer, the highlight-knee rolloff, the white-balance gains and
push-gate, and the display-space contrast/saturation — in the same order.
"""

import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import tone

KNEE = 0.85  # must equal VireoToneGL KNEE and tone.HIGHLIGHT_KNEE


def test_shader_knee_constant_matches_tone():
    assert KNEE == tone.HIGHLIGHT_KNEE


def _shader_mirror(c, exposure, wb_gain, contrast, saturation, rolloff):
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

    lin = srgb_to_linear(c) * (2.0 ** exposure) * np.asarray(wb_gain)
    if rolloff:
        lin = roll(lin)
    disp = linear_to_srgb(lin)
    disp = (disp - 0.5) * contrast + 0.5
    luma = disp @ np.array([0.2126, 0.7152, 0.0722])
    disp = luma[..., None] + (disp - luma[..., None]) * saturation
    return np.clip(disp, 0.0, 1.0)


def test_shader_matches_tone_for_full_recipe():
    rng = np.random.default_rng(1)
    px = rng.random((256, 3)).astype(np.float32)
    cases = [
        (0.0, 0, 0, 0, 0),
        (1.5, 0, 0, 0, 0),
        (2.0, 60, -30, 10, 20),
        (-1.0, 0, 0, 40, -50),
        (0.0, 0, 0, 0, -100),
        (3.0, -40, 25, -20, 35),
    ]
    for ev, temp, tint, con, sat in cases:
        wb = {"temperature": temp, "tint": tint} if (temp or tint) else None
        expected = tone.apply_adjustments(
            px[None, :, :], exposure=ev, white_balance=wb, contrast=con, saturation=sat
        )[0]

        # The dispatcher (_lbApplyAdjustmentPreview) derives these from a base of
        # all-zeros for a first edit, so the deltas equal the full values.
        gr, gg, gb = tone.white_balance_gains(wb)
        pushed = (2.0 ** ev) * max(gr, gg, gb) > 1.0 + 1e-6
        got = _shader_mirror(
            px,
            exposure=ev,
            wb_gain=(gr, gg, gb),
            contrast=max(0.0, 1.0 + con / 100.0),
            saturation=max(0.0, 1.0 + sat / 100.0),
            rolloff=pushed,
        )
        max_err = float(np.max(np.abs(got - expected)))
        assert max_err < 2e-4, (ev, temp, tint, con, sat, max_err)
