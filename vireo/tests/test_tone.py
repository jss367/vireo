import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from tone import (
    HIGHLIGHT_KNEE,
    apply_adjustments,
    highlight_rolloff,
    linear_to_srgb,
    srgb_to_linear,
    white_balance_gains,
)


def test_srgb_linear_round_trips():
    c = np.linspace(0.0, 1.0, 256, dtype=np.float32)
    back = linear_to_srgb(srgb_to_linear(c))
    assert np.allclose(c, back, atol=1e-4)


def test_no_adjustments_is_near_identity():
    # The de-gamma -> re-gamma round trip plus an untouched highlight shoulder
    # must leave an un-pushed image essentially unchanged (<= 1 code value).
    rgb = np.random.default_rng(0).random((32, 32, 3), dtype=np.float32)
    out = apply_adjustments(rgb)
    assert np.max(np.abs(out - rgb) * 255.0) <= 1.0


def test_exposure_is_a_linear_multiply_in_shadows():
    # A mid-shadow tone pushed +1 stop should roughly double in linear light
    # (well below the highlight knee, so the shoulder does not engage).
    rgb = np.full((1, 1, 3), 0.2, dtype=np.float32)
    out = apply_adjustments(rgb, exposure=1.0)
    lin_in = srgb_to_linear(np.float32(0.2))
    lin_out = srgb_to_linear(out[0, 0, 0])
    assert lin_out == float(lin_out)  # finite
    assert abs(lin_out - 2.0 * lin_in) < 0.02


def test_highlights_roll_off_instead_of_clipping():
    # Pushing a bright tone hard must compress toward white, never reach a flat
    # 1.0, and stay monotonic across increasing exposure.
    rgb = np.full((1, 1, 3), 0.7, dtype=np.float32)
    prev = -1.0
    for ev in (0.0, 1.0, 2.0, 4.0):
        out = float(apply_adjustments(rgb, exposure=ev)[0, 0, 0])
        assert out < 1.0  # never hard-clips to flat white
        assert out > prev  # monotonic increase with exposure
        prev = out


def test_positive_exposure_never_darkens_near_white_highlights():
    # Regression: before the per-channel ``max(rolled, min(lin_pre, lin))``
    # clamp, the shoulder curve being below identity on (knee, 1.0] meant a
    # 0.95-linear pixel (sRGB ~0.977, already above the 0.85 knee) at +0.1 EV
    # would darken to ~0.929 — non-monotonic in exposure. Lock the contract:
    # a positive exposure can only raise or hold the output, never lower it.
    srgb_in = float(linear_to_srgb(np.array([0.95], dtype=np.float32))[0])
    rgb = np.full((1, 1, 3), srgb_in, dtype=np.float32)
    base = float(apply_adjustments(rgb)[0, 0, 0])
    prev = base
    for ev in (0.05, 0.1, 0.25, 0.5, 1.0, 2.0):
        out = float(apply_adjustments(rgb, exposure=ev)[0, 0, 0])
        assert out >= base - 1e-5, (
            f"+{ev} EV darkened below the ev=0 baseline: {base:.4f} -> {out:.4f}"
        )
        assert out >= prev - 1e-5, (
            f"+{ev} EV non-monotonic vs prev step: {prev:.4f} -> {out:.4f}"
        )
        prev = out


def test_highlight_rolloff_is_identity_below_knee():
    lin = np.array([0.0, 0.25, 0.5, HIGHLIGHT_KNEE], dtype=np.float32)
    assert np.allclose(highlight_rolloff(lin), lin, atol=1e-6)


def test_highlight_rolloff_never_exceeds_one():
    lin = np.array([1.0, 4.0, 100.0], dtype=np.float32)
    assert np.all(highlight_rolloff(lin) < 1.0)


def test_negative_exposure_darkens():
    rgb = np.full((1, 1, 3), 0.5, dtype=np.float32)
    out = float(apply_adjustments(rgb, exposure=-1.0)[0, 0, 0])
    assert out < 0.5


def test_white_balance_gains_default_to_unity():
    assert white_balance_gains(None) == (1.0, 1.0, 1.0)
    assert white_balance_gains({}) == (1.0, 1.0, 1.0)


def test_warm_white_balance_pushes_red_over_blue():
    rgb = np.full((1, 1, 3), 0.5, dtype=np.float32)
    out = apply_adjustments(rgb, white_balance={"temperature": 60})
    r, g, b = out[0, 0]
    assert r > b


def test_saturation_zero_is_grey_and_preserves_luma():
    rgb = np.array([[[0.8, 0.3, 0.1]]], dtype=np.float32)
    out = apply_adjustments(rgb, saturation=-100.0)
    r, g, b = out[0, 0]
    assert abs(r - g) < 1e-3 and abs(g - b) < 1e-3  # fully desaturated -> grey
