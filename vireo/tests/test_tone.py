import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from tone import (
    HIGHLIGHT_KNEE,
    apply_adjustments,
    apply_color_grading,
    apply_hsl_mixer,
    apply_tone_curve,
    apply_vibrance,
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


def test_shadows_lift_dark_pixels_more_than_midtones():
    rgb = np.array([[[0.12, 0.12, 0.12], [0.55, 0.55, 0.55]]], dtype=np.float32)
    out = apply_adjustments(rgb, shadows=80)
    dark_delta = float(out[0, 0, 0] - rgb[0, 0, 0])
    mid_delta = float(out[0, 1, 0] - rgb[0, 1, 0])
    assert dark_delta > mid_delta
    assert dark_delta > 0


def test_highlights_pull_bright_pixels_more_than_midtones():
    rgb = np.array([[[0.55, 0.55, 0.55], [0.9, 0.9, 0.9]]], dtype=np.float32)
    out = apply_adjustments(rgb, highlights=-80)
    mid_delta = float(rgb[0, 0, 0] - out[0, 0, 0])
    bright_delta = float(rgb[0, 1, 0] - out[0, 1, 0])
    assert bright_delta > mid_delta
    assert bright_delta > 0


def test_whites_and_blacks_target_extreme_ranges():
    rgb = np.array(
        [[[0.05, 0.05, 0.05], [0.30, 0.30, 0.30], [0.85, 0.85, 0.85]]],
        dtype=np.float32,
    )
    out = apply_adjustments(rgb, blacks=-80, whites=80)
    assert out[0, 0, 0] < rgb[0, 0, 0]
    assert abs(float(out[0, 1, 0] - rgb[0, 1, 0])) < 0.02
    assert out[0, 2, 0] > rgb[0, 2, 0]


def test_positive_vibrance_prefers_less_saturated_colors():
    low = np.array([[[0.55, 0.50, 0.48]]], dtype=np.float32)
    high = np.array([[[0.95, 0.20, 0.10]]], dtype=np.float32)
    low_out = apply_vibrance(low, 80)
    high_out = apply_vibrance(high, 80)
    low_gain = float(np.max(low_out) - np.min(low_out)) / float(np.max(low) - np.min(low))
    high_gain = float(np.max(high_out) - np.min(high_out)) / float(np.max(high) - np.min(high))
    assert low_gain > high_gain


def test_neutral_tone_curve_is_identity():
    rgb = np.random.default_rng(21).random((8, 8, 3), dtype=np.float32)
    curve = {"black": 0, "shadows": 25, "midtones": 50, "highlights": 75, "white": 100}
    assert np.allclose(apply_tone_curve(rgb, curve), rgb, atol=1e-6)


def test_tone_curve_moves_control_points_and_interpolates():
    rgb = np.array([[[0.0, 0.25, 0.5], [0.75, 1.0, 0.125]]], dtype=np.float32)
    out = apply_tone_curve(rgb, {"black": 10, "shadows": 35, "midtones": 45, "white": 90})
    assert np.allclose(out[0, 0], [0.10, 0.35, 0.45], atol=1e-6)
    assert abs(float(out[0, 1, 1]) - 0.90) < 1e-6
    assert 0.10 < out[0, 1, 2] < 0.35


def test_hsl_mixer_targets_selected_color():
    rgb = np.array([[[0.9, 0.3, 0.1], [0.1, 0.3, 0.9]]], dtype=np.float32)
    out = apply_hsl_mixer(rgb, {"orange": {"saturation": -100}})
    orange_chroma_before = float(np.ptp(rgb[0, 0]))
    orange_chroma_after = float(np.ptp(out[0, 0]))
    blue_change = float(np.max(np.abs(out[0, 1] - rgb[0, 1])))
    assert orange_chroma_after < orange_chroma_before * 0.2
    assert blue_change < 0.02


def test_hsl_luminance_changes_selected_color_brightness():
    rgb = np.array([[[0.1, 0.3, 0.9], [0.9, 0.3, 0.1]]], dtype=np.float32)
    out = apply_hsl_mixer(rgb, {"blue": {"luminance": -50}})
    assert float(np.mean(out[0, 0])) < float(np.mean(rgb[0, 0]))
    assert np.allclose(out[0, 1], rgb[0, 1], atol=0.02)


def test_hsl_mixer_does_not_treat_neutral_grey_as_red():
    rgb = np.full((1, 1, 3), 0.45, dtype=np.float32)
    out = apply_hsl_mixer(
        rgb,
        {"red": {"hue": 100, "saturation": 100, "luminance": 100}},
    )
    assert np.allclose(out, rgb, atol=1e-6)


def test_color_grading_tints_tonal_ranges_differently():
    rgb = np.array([[[0.15, 0.15, 0.15], [0.85, 0.85, 0.85]]], dtype=np.float32)
    out = apply_color_grading(
        rgb,
        {
            "shadows": {"hue": 240, "saturation": 50},
            "highlights": {"hue": 45, "saturation": 50},
        },
    )
    assert out[0, 0, 2] > out[0, 0, 0]
    assert out[0, 1, 0] > out[0, 1, 2]


def test_advanced_color_runs_with_local_tone_branch():
    rgb = np.array([[[0.2, 0.3, 0.8], [0.2, 0.3, 0.8]]], dtype=np.float32)
    weight = np.array([[1.0, 0.0]], dtype=np.float32)
    out = apply_adjustments(
        rgb,
        local_weight=weight,
        local_subject={"exposure": 0.5},
        tone_curve={"midtones": 60},
        hsl={"blue": {"saturation": -30}},
        color_grading={"shadows": {"hue": 30, "saturation": 10}},
    )
    assert out.shape == rgb.shape
    assert np.all(np.isfinite(out))


# --- local (mask-weighted) tone -------------------------------------------


def _halves_weight(height=4, width=8):
    """Weight 1.0 on the left half, 0.0 on the right."""
    w = np.zeros((height, width), dtype=np.float32)
    w[:, : width // 2] = 1.0
    return w


def test_local_weight_none_matches_global_only():
    rng = np.random.default_rng(9)
    px = rng.random((4, 8, 3)).astype(np.float32)
    a = apply_adjustments(px, exposure=0.7, shadows=20)
    b = apply_adjustments(
        px, exposure=0.7, shadows=20,
        local_weight=None, local_subject=None, local_background=None,
    )
    assert np.array_equal(a, b)


def test_local_exposure_applies_only_inside_weight():
    px = np.full((4, 8, 3), 0.2, dtype=np.float32)
    w = _halves_weight()

    out = apply_adjustments(
        px, local_weight=w, local_subject={"exposure": 1.0},
    )
    global_only = apply_adjustments(px)

    # Right half (w=0) identical to the no-local render.
    assert np.allclose(out[:, 4:], global_only[:, 4:], atol=1e-6)
    # Left half brightened by ~one stop in linear light.
    left_lin = srgb_to_linear(out[:, :4, 0])
    base_lin = srgb_to_linear(px[:, :4, 0])
    assert np.allclose(left_lin, base_lin * 2.0, rtol=0.02)


def test_local_background_applies_to_inverse_weight():
    px = np.full((4, 8, 3), 0.5, dtype=np.float32)
    w = _halves_weight()

    out = apply_adjustments(
        px, local_weight=w, local_background={"exposure": -1.0},
    )

    # Subject half untouched, background half darkened.
    assert np.allclose(out[:, :4], px[:, :4], atol=1e-3)
    assert np.all(out[:, 4:, 0] < 0.4)


def test_local_deltas_compose_with_global():
    # Global +40 shadows, subject delta -40: subject area nets to no shadow
    # lift, background keeps the global lift.
    px = np.full((4, 8, 3), 0.15, dtype=np.float32)
    w = _halves_weight()

    out = apply_adjustments(
        px, shadows=40, local_weight=w, local_subject={"shadows": -40},
    )
    no_lift = apply_adjustments(px)
    lifted = apply_adjustments(px, shadows=40)

    assert np.allclose(out[:, :4], no_lift[:, :4], atol=1e-3)
    assert np.allclose(out[:, 4:], lifted[:, 4:], atol=1e-3)


def test_local_saturation_weighted():
    px = np.zeros((2, 2, 3), dtype=np.float32)
    px[..., 0] = 0.7
    px[..., 1] = 0.3
    px[..., 2] = 0.3
    w = np.array([[1.0, 0.0], [1.0, 0.0]], dtype=np.float32)

    out = apply_adjustments(
        px, local_weight=w, local_subject={"saturation": -100},
    )

    # w=1 column fully desaturated (channels equal), w=0 column unchanged.
    assert np.allclose(out[:, 0, 0], out[:, 0, 1], atol=1e-4)
    assert np.allclose(out[:, 1], px[:, 1], atol=1e-4)


def test_local_fractional_weight_is_between():
    px = np.full((1, 3, 3), 0.25, dtype=np.float32)
    w = np.array([[0.0, 0.5, 1.0]], dtype=np.float32)

    out = apply_adjustments(
        px, local_weight=w, local_subject={"exposure": 1.5},
    )

    assert out[0, 0, 0] < out[0, 1, 0] < out[0, 2, 0]
