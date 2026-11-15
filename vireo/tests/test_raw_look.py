"""The RAW look: how an unadjusted scene-linear decode reaches the display."""

import numpy as np
import pytest
import tone
from tone import (
    MIDDLE_GREY,
    RAW_MIDDLE_GREY,
    ToneAdjustments,
    apply_adjustments,
    linear_to_srgb,
    raw_look_curve,
)

# Median luminance of the camera's own JPEG (8-bit sRGB) at each scene level
# (stops relative to the sensor ceiling of the linear decode), averaged over a
# Nikon D7200, D850 and Z 8: 16 files each, spread across each camera's
# shooting days. The three cameras agree within about 5 levels at every row.
CAMERA_JPEG_LEVELS = [
    (-7.875, 10), (-7.125, 17), (-6.375, 26), (-5.625, 40), (-4.875, 63),
    (-4.125, 94), (-3.375, 136), (-2.625, 178), (-1.875, 215), (-1.125, 241),
    (-0.375, 249), (0.375, 252),
]


def _levels(scene):
    return float(linear_to_srgb(raw_look_curve(scene))) * 255.0


@pytest.mark.parametrize('stops, camera_level', CAMERA_JPEG_LEVELS)
def test_look_follows_the_camera_jpeg_rendering(stops, camera_level):
    assert _levels(2.0 ** stops) == pytest.approx(camera_level, abs=8)


def test_look_is_monotonic_and_stays_below_white():
    scene = 2.0 ** np.linspace(-14, 6, 2000)
    display = raw_look_curve(scene)
    assert raw_look_curve(0.0) == 0
    assert np.all(np.diff(display) > 0)
    assert display.max() < 1.0


def test_look_shows_the_sensor_ceiling_and_blown_areas_as_white():
    # 1.0 is the least amplified channel's ceiling; a rebuilt blown area sits
    # at the largest white-balance multiplier, about 2 in daylight.
    assert 248 < _levels(1.0) < 253
    assert _levels(2.0) > 253


def test_look_keeps_deep_shadows_proportional():
    # Under the toe a tone twice as bright is shown twice as bright in
    # display-linear light: shadow detail is neither crushed nor stretched.
    dark = raw_look_curve(np.array([1e-4, 2e-4]))
    assert dark[1] / dark[0] == pytest.approx(2.0, rel=0.02)
    assert 0.4 < dark[0] / 1e-4 < 0.6


def test_scene_middle_grey_is_shown_at_display_middle_grey():
    assert float(raw_look_curve(RAW_MIDDLE_GREY)) == pytest.approx(MIDDLE_GREY, rel=1e-5)
    # More than a stop under display middle grey: a camera meters for
    # highlight headroom and brightens when it renders.
    assert 1.0 < np.log2(MIDDLE_GREY / RAW_MIDDLE_GREY) < 1.5


def test_unadjusted_raw_renders_through_the_look_and_neutral_stays_neutral():
    scene = np.array([[[0.02] * 3, [RAW_MIDDLE_GREY] * 3, [0.5] * 3, [3.0] * 3]], dtype=np.float32)
    out = apply_adjustments(scene, input_linear=True)
    np.testing.assert_allclose(out, linear_to_srgb(raw_look_curve(scene)), atol=1e-6)
    assert np.ptp(out, axis=-1).max() == 0
    assert out[0, 1, 0] * 255 == pytest.approx(118, abs=0.5)


def test_display_referred_sources_do_not_get_the_look():
    jpeg = np.array([[[0.1, 0.2, 0.3], [0.46, 0.46, 0.46], [0.9, 0.8, 0.7]]], dtype=np.float32)
    np.testing.assert_allclose(apply_adjustments(jpeg), jpeg, atol=1e-6)


def test_look_preserves_hue():
    rng = np.random.default_rng(3)
    scene = rng.uniform(0.0, 1.5, (32, 32, 3)).astype(np.float32)
    out = tone.srgb_to_linear(apply_adjustments(scene, input_linear=True))

    def place(rgb):
        low, high = rgb.min(-1), rgb.max(-1)
        return (np.median(rgb, axis=-1) - low) / (high - low)

    # The channel order and the middle channel's place between the other two
    # are what hue is; the look changes neither.
    assert np.array_equal(np.argsort(out, axis=-1), np.argsort(scene, axis=-1))
    np.testing.assert_allclose(place(out), place(scene), atol=2e-3)


def test_bright_saturated_color_keeps_its_gradation():
    # A red twice as saturated as sRGB can show at this brightness under one
    # luminance gain: its largest channel still rises smoothly toward white
    # and never reaches it.
    red = np.zeros((1, 64, 3), dtype=np.float32)
    red[0, :, 0] = np.linspace(0.2, 2.0, 64)
    red[0, :, 1] = 0.04
    red[0, :, 2] = 0.02
    out = apply_adjustments(red, input_linear=True)
    assert np.all(np.diff(out[0, :, 0]) > 0)
    assert out[0, :, 0].max() < 1.0
    assert np.all(out[0, :, 0] > out[0, :, 1])
    assert np.all(out[0, :, 1] > out[0, :, 2])


def test_out_of_gamut_color_is_fitted_without_changing_its_order():
    scene = np.array([[[2.0, -0.1, 0.2], [0.2, -0.05, 0.1], [-0.01, 0.3, 0.02]]], dtype=np.float32)
    out = apply_adjustments(scene, input_linear=True)
    assert np.isfinite(out).all()
    assert out.min() == 0 and out.max() < 1.0
    assert np.array_equal(np.argsort(out, axis=-1), np.argsort(scene, axis=-1))


def test_exposure_never_darkens_any_channel():
    rng = np.random.default_rng(5)
    scene = rng.uniform(0.0, 2.0, (16, 16, 3)).astype(np.float32)
    previous = apply_adjustments(scene, ToneAdjustments(exposure=-3), input_linear=True)
    for stops in (-2, -1, 0, 1, 2):
        current = apply_adjustments(scene, ToneAdjustments(exposure=stops), input_linear=True)
        assert np.all(current >= previous - 1e-6)
        previous = current


@pytest.mark.parametrize('control', ['shadows', 'highlights'])
def test_range_controls_are_placed_by_displayed_brightness(control):
    # A tone shown at middle grey answers Shadows and Highlights the same way
    # whether it came from a RAW or from a JPEG of the same picture: Shadows
    # leaves it alone, and Highlights has only begun to act on it.
    raw = np.full((8, 8, 3), RAW_MIDDLE_GREY, dtype=np.float32)
    jpeg = np.full((8, 8, 3), float(linear_to_srgb(MIDDLE_GREY)), dtype=np.float32)
    amount = ToneAdjustments(**{control: 100})
    raw_stops = np.log2(
        raw_look_inverse(apply_adjustments(raw, amount, input_linear=True)[0, 0, 0]) / RAW_MIDDLE_GREY
    )
    jpeg_stops = np.log2(
        float(tone.srgb_to_linear(apply_adjustments(jpeg, amount)[0, 0, 0])) / MIDDLE_GREY
    )
    assert raw_stops == pytest.approx(jpeg_stops, abs=0.02)
    if control == 'shadows':
        assert abs(raw_stops) < 0.02


def raw_look_inverse(display_srgb):
    return tone._raw_look_inverse(float(tone.srgb_to_linear(display_srgb)))


def test_shadows_lift_a_dark_raw_region():
    dark = np.full((8, 8, 3), RAW_MIDDLE_GREY / 16, dtype=np.float32)
    plain = apply_adjustments(dark, input_linear=True)
    lifted = apply_adjustments(dark, ToneAdjustments(shadows=100), input_linear=True)
    assert (lifted - plain).min() * 255 > 15
