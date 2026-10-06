import os
import sys

import numpy as np
import pytest
from PIL import Image

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import auto_tone
from tone import apply_adjustments, linear_to_srgb

H, W = 120, 180


def _grey(levels):
    """An H×W sRGB frame whose grey levels follow ``levels`` (H×W)."""
    return np.repeat(np.asarray(levels, dtype=np.float32)[..., None], 3, axis=-1)


def _ramp(lo, hi, seed=0):
    """A textured grey frame spanning [lo, hi] with its median mid-range."""
    rng = np.random.default_rng(seed)
    levels = np.linspace(lo, hi, H * W, dtype=np.float32)
    rng.shuffle(levels)
    return _grey(levels.reshape(H, W))


def _render(rgb, adjustments, input_linear=False):
    adj = {k: v for k, v in adjustments.items() if v}
    return apply_adjustments(rgb, input_linear=input_linear, **adj)


def _luma(rgb):
    return 0.2126 * rgb[..., 0] + 0.7152 * rgb[..., 1] + 0.0722 * rgb[..., 2]


def _box(top, left, height, width):
    mask = np.zeros((H, W), dtype=np.float32)
    mask[top:top + height, left:left + width] = 1.0
    return mask


def test_dark_frame_is_brightened_toward_mid_grey():
    rgb = _ramp(0.02, 0.35)
    result = auto_tone.fit(rgb)
    exposure = result['adjustments']['exposure']
    assert exposure >= 0.5
    assert result['notes'][0] == f'brightened {exposure:.1f} EV'
    before = np.median(_luma(rgb))
    after = np.median(_luma(_render(rgb, {'exposure': exposure})))
    assert abs(after - auto_tone.MID_GREY) < abs(before - auto_tone.MID_GREY)


def test_balanced_full_range_frame_is_left_nearly_alone():
    result = auto_tone.fit(_ramp(0.03, 0.9))
    adj = result['adjustments']
    assert abs(adj['exposure']) <= 0.1
    assert adj['highlights'] == 0
    assert adj['shadows'] == 0
    assert adj['vibrance'] == 0 and adj['saturation'] == 0


def test_fitted_values_are_whole_slider_steps_within_limits():
    rng = np.random.default_rng(3)
    rgb = np.clip(rng.normal(0.3, 0.15, (H, W, 3)), 0, 1).astype(np.float32)
    adj = auto_tone.fit(rgb)['adjustments']
    assert set(adj) == {
        'exposure', 'highlights', 'shadows', 'contrast',
        'whites', 'blacks', 'vibrance', 'saturation',
    }
    assert round(adj['exposure'] * 10) == adj['exposure'] * 10
    for key in set(adj) - {'exposure'}:
        assert adj[key] == int(adj[key])
    assert auto_tone.HIGHLIGHTS_LIMIT <= adj['highlights'] <= 0
    assert 0 <= adj['shadows'] <= auto_tone.SHADOWS_LIMIT
    assert 0 <= adj['contrast'] <= auto_tone.CONTRAST_LIMIT
    assert 0 <= adj['whites'] <= auto_tone.WHITES_LIMIT
    assert auto_tone.BLACKS_LIMIT <= adj['blacks'] <= 0
    assert 0 <= adj['vibrance'] <= auto_tone.VIBRANCE_LIMIT
    assert 0 <= adj['saturation'] <= auto_tone.SATURATION_LIMIT


def test_dark_subject_on_bright_ground_pulls_exposure_a_bounded_amount():
    # A dark swallow over bright sand: metering on the bird alone would blow
    # out the sand, ignoring it would leave the bird a silhouette.
    levels = np.full((H, W), 0.7, dtype=np.float32)
    levels += np.random.default_rng(1).uniform(-0.05, 0.05, (H, W)).astype(np.float32)
    levels[50:70, 80:110] = 0.08
    rgb = _grey(levels)
    frame_only = auto_tone.fit(rgb)
    metered = auto_tone.fit(rgb, subject=_box(50, 80, 20, 30))

    assert frame_only['metering'] == 'frame'
    assert metered['metering'] == 'subject'
    frame_ev = frame_only['adjustments']['exposure']
    subject_ev = metered['adjustments']['exposure']
    assert subject_ev > frame_ev
    assert subject_ev - frame_ev <= auto_tone.SUBJECT_PULL_LIMIT + 0.1
    # Shadows open the bird up instead of exposure blowing the sand.
    assert metered['adjustments']['shadows'] > 0
    rendered = _luma(_render(rgb, metered['adjustments']))
    assert np.median(rendered[50:70, 80:110]) > 0.08 + 0.1


def test_saturated_subject_is_metered_on_its_brightest_channel():
    # A vivid blue bird has low luma without being underexposed; a grey bird
    # of the same luma is genuinely dark and gets more exposure.
    frame = _ramp(0.3, 0.6)
    blue = frame.copy()
    blue[50:70, 80:110] = (0.1, 0.2, 0.9)
    luma = float(_luma(np.array([0.1, 0.2, 0.9], dtype=np.float32)))
    grey = frame.copy()
    grey[50:70, 80:110] = luma
    subject = _box(50, 80, 20, 30)
    blue_ev = auto_tone.fit(blue, subject=subject)['adjustments']['exposure']
    grey_ev = auto_tone.fit(grey, subject=subject)['adjustments']['exposure']
    assert grey_ev > blue_ev


def test_tiny_or_whole_frame_subjects_fall_back_to_frame_metering():
    rgb = _ramp(0.1, 0.5)
    assert auto_tone.fit(rgb, subject=_box(0, 0, 1, 1))['metering'] == 'frame'
    assert auto_tone.fit(rgb, subject=np.ones((H, W)))['metering'] == 'frame'


def test_low_key_frame_keeps_its_black_backdrop():
    # A night flash shot: most of the frame is black on purpose.
    levels = np.full((H, W), 0.01, dtype=np.float32)
    levels[:, :70] = np.linspace(0.2, 0.7, H * 70, dtype=np.float32).reshape(H, 70)
    rgb = _grey(levels)
    result = auto_tone.fit(rgb)
    assert result['adjustments']['shadows'] == 0
    rendered = _luma(_render(rgb, result['adjustments']))
    assert np.median(rendered[:, 90:]) < auto_tone.LOW_KEY_LEVEL


def test_brightening_never_pushes_more_than_one_percent_into_the_shoulder():
    # Mostly dark, with a bright cloud: full mid-grey metering would blow it.
    levels = np.full((H, W), 0.12, dtype=np.float32)
    levels[:6, :] = 0.9
    rgb = _grey(levels)
    exposure = auto_tone.fit(rgb)['adjustments']['exposure']
    exposed = _luma(_render(rgb, {'exposure': exposure}))
    assert np.quantile(exposed, 0.99) <= auto_tone.HIGHLIGHT_GUARD


def test_bright_areas_get_highlight_recovery():
    # A shaded foreground under a bright sky covering a fifth of the frame.
    levels = np.linspace(0.2, 0.5, H * W, dtype=np.float32).reshape(H, W)
    levels[:24] = np.linspace(0.92, 0.96, 24 * W, dtype=np.float32).reshape(24, W)
    rgb = _grey(levels)
    adj = auto_tone.fit(rgb)['adjustments']
    assert adj['highlights'] < 0
    before = _render(rgb, {'exposure': adj['exposure']})
    after = _render(rgb, {'exposure': adj['exposure'], 'highlights': adj['highlights']})
    assert np.mean(_luma(after) >= auto_tone.HIGHLIGHT_CEILING) < np.mean(
        _luma(before) >= auto_tone.HIGHLIGHT_CEILING
    )


def test_overexposed_raw_is_darkened_in_linear_light():
    # Scene-linear values up to two stops over display white.
    rgb = np.full((H, W, 3), 1.0, dtype=np.float32)
    rgb *= np.linspace(0.5, 4.0, W, dtype=np.float32)[None, :, None]
    result = auto_tone.fit(rgb, input_linear=True)
    assert result['adjustments']['exposure'] < 0
    assert result['notes'][0].startswith('darkened')


def test_raw_darkening_stops_once_its_headroom_is_recovered():
    # A bright RAW frame whose top tones sit just one stop past the knee.
    rgb = np.full((H, W, 3), 1.0, dtype=np.float32)
    rgb *= np.linspace(0.8, 2 * 0.85, W, dtype=np.float32)[None, :, None]
    exposure = auto_tone.fit(rgb, input_linear=True)['adjustments']['exposure']
    assert -1.0 <= exposure < 0


def test_bright_raw_within_white_is_not_darkened():
    # Bright, but nothing above the highlight knee: no detail to recover.
    rgb = np.full((H, W, 3), 0.6, dtype=np.float32)
    rgb *= np.linspace(0.9, 1.4, W, dtype=np.float32)[None, :, None]
    assert auto_tone.fit(rgb, input_linear=True)['adjustments']['exposure'] >= 0


def test_bright_display_referred_sky_is_not_darkened_to_grey():
    # A JPEG (or camera-embedded preview) of a bright cyan sky: nothing is
    # clipped, but a display-referred source has no headroom to recover.
    sky = np.zeros((H, W, 3), dtype=np.float32)
    sky[...] = (0.62, 0.9, 0.98)
    sky *= np.random.default_rng(4).uniform(0.95, 1.0, (H, W, 1)).astype(np.float32)
    assert auto_tone.fit(sky)['adjustments']['exposure'] >= 0


def test_dark_bird_on_white_overcast_sky_keeps_the_sky_white():
    # Metering the frame alone would pull the white sky toward mid-grey.
    levels = np.full((H, W), 0.97, dtype=np.float32)
    levels[:40] = 1.0
    levels[55:65, 85:100] = 0.12
    rgb = _grey(levels)
    result = auto_tone.fit(rgb, subject=_box(55, 85, 10, 15))
    assert result['metering'] == 'subject'
    assert result['adjustments']['exposure'] >= 0
    rendered = _luma(_render(rgb, result['adjustments']))
    assert np.median(rendered[70:]) >= 0.9


def test_neutral_frame_gets_no_colour_boost():
    rgb = _ramp(0.1, 0.8) * np.array([1.0, 0.99, 0.98], dtype=np.float32)
    adj = auto_tone.fit(rgb)['adjustments']
    assert adj['vibrance'] == 0 and adj['saturation'] == 0


def test_muted_colour_is_enriched():
    rgb = _ramp(0.2, 0.8)
    rgb[..., 0] *= 1.06
    rgb[..., 2] *= 0.94
    result = auto_tone.fit(np.clip(rgb, 0, 1))
    assert result['adjustments']['vibrance'] > 0
    assert 'enriched muted colour' in result['notes']


def test_white_balance_and_presence_are_rendered_but_never_set():
    rgb = _ramp(0.1, 0.6)
    plain = auto_tone.fit(rgb)
    warm = auto_tone.fit(rgb, white_balance={'temperature': 80})
    assert 'white_balance' not in warm['adjustments']
    assert 'dehaze' not in auto_tone.fit(rgb, presence={'dehaze': 50})['adjustments']
    # Warming lowers blue more than it raises red, so the fit compensates.
    assert warm['adjustments'] != plain['adjustments']


def test_box_refinement_finds_the_bird_against_sky():
    sky = np.zeros((H, W, 3), dtype=np.float32)
    sky[...] = (0.45, 0.6, 0.85)
    sky[55:65, 85:95] = (0.1, 0.08, 0.06)
    ellipse = np.zeros((H, W), dtype=np.float32)
    yy, xx = np.mgrid[:H, :W]
    ellipse[((yy - 60) / 20.0) ** 2 + ((xx - 90) / 20.0) ** 2 <= 1.0] = 1.0
    refined = auto_tone._refine_box_subject(sky, ellipse)
    assert refined[55:65, 85:95].min() > 0.9
    assert refined[45, 90] == 0.0  # sky inside the box


def test_box_refinement_keeps_ellipse_when_subject_matches_backdrop():
    flat = np.full((H, W, 3), 0.5, dtype=np.float32)
    ellipse = _box(40, 60, 40, 60)
    assert np.array_equal(auto_tone._refine_box_subject(flat, ellipse), ellipse)


def test_fit_loaded_image_ignores_tonal_values_and_passes_frame_settings(monkeypatch):
    img = Image.fromarray((_ramp(0.1, 0.6) * 255).astype(np.uint8), 'RGB')
    seen = []
    real_fit = auto_tone.fit

    def spy(rgb, **kwargs):
        seen.append((rgb.shape, kwargs))
        return real_fit(rgb, **kwargs)

    monkeypatch.setattr(auto_tone, 'fit', spy)
    plain = auto_tone.fit_loaded_image(img, {})
    edited = auto_tone.fit_loaded_image(img, {
        'adjustments': {'exposure': 2.0, 'shadows': 60, 'vibrance': 40},
    })
    assert plain == edited

    auto_tone.fit_loaded_image(img, {
        'rotation': 90,
        'crop': {'x': 0.0, 'y': 0.0, 'w': 0.5, 'h': 1.0},
        'adjustments': {'white_balance': {'temperature': 30}, 'clarity': 20},
    })
    shape, kwargs = seen[-1]
    # Rotated 90 (120 wide x 180 tall), then the left half: 60 x 180.
    assert shape[:2] == (180, 60)
    assert kwargs['white_balance'] == {'temperature': 30.0}
    assert kwargs['presence']['clarity'] == 20.0
    assert kwargs['input_linear'] is False


def test_fit_loaded_image_meters_on_a_detection_box():
    levels = np.full((H, W), 0.7, dtype=np.float32)
    levels[50:70, 80:110] = 0.08
    img = Image.fromarray((_grey(levels) * 255).astype(np.uint8), 'RGB')
    box = {'x': 70 / W, 'y': 40 / H, 'w': 50 / W, 'h': 40 / H}
    result = auto_tone.fit_loaded_image(img, {}, box=box)
    assert result['metering'] == 'subject'
    assert result['subject_source'] == 'detection'
    assert auto_tone.fit_loaded_image(img, {})['subject_source'] is None


def test_fit_loaded_image_reads_raw_in_linear_light():
    from float_image import FloatImage

    linear = np.full((H, W, 3), 0.02, dtype=np.float32)
    linear *= np.linspace(0.5, 1.5, W, dtype=np.float32)[None, :, None]
    result = auto_tone.fit_loaded_image(FloatImage(linear, encoding='linear'), {})
    assert result['adjustments']['exposure'] > 0
    # Sanity: the same pixels as display-encoded sRGB are much brighter.
    assert float(linear_to_srgb(0.02)) > 0.1


def _bird_on_sky():
    """A dark bird (rows 55-65) against a bright, slightly textured sky."""
    levels = np.full((H, W), 0.8, dtype=np.float32)
    levels += np.random.default_rng(5).uniform(-0.03, 0.03, (H, W)).astype(np.float32)
    levels[55:65, 80:110] = 0.05
    return _grey(levels), _box(55, 80, 10, 30)


def test_unknown_style_is_rejected():
    with pytest.raises(ValueError):
        auto_tone.fit(_ramp(0.1, 0.6), style='dramatic')


def test_every_result_names_its_style():
    for style in auto_tone.STYLES:
        assert auto_tone.fit(_ramp(0.1, 0.6), style=style)['style'] == style


def test_subject_style_exposes_for_the_subject_and_lets_the_sky_go():
    rgb, subject = _bird_on_sky()
    balanced = auto_tone.fit(rgb, subject=subject)
    exposed = auto_tone.fit(rgb, subject=subject, style='subject')
    assert exposed['metering'] == 'subject'
    assert exposed['adjustments']['exposure'] > balanced['adjustments']['exposure'] + 1.0
    rendered = _luma(_render(rgb, exposed['adjustments']))
    bird = rendered[55:65, 80:110]
    assert np.median(bird) > np.median(_luma(_render(rgb, balanced['adjustments']))[55:65, 80:110])
    # The sky may go to white; the bird itself stays below the guard.
    assert np.quantile(bird, auto_tone.SUBJECT_STYLE_GUARD_QUANTILE) <= auto_tone.HIGHLIGHT_GUARD


def test_subject_style_keeps_a_white_bird_out_of_the_shoulder():
    levels = np.full((H, W), 0.3, dtype=np.float32)
    levels[50:70, 80:110] = 0.93
    rgb = _grey(levels)
    adj = auto_tone.fit(rgb, subject=_box(50, 80, 20, 30), style='subject')['adjustments']
    # Exposure is metered on the bird, which is already bright: no push.
    assert adj['exposure'] <= 0.1
    bird = _luma(_render(rgb, adj))[50:70, 80:110]
    assert np.median(bird) < auto_tone.CLIPPED


def test_subject_style_without_a_subject_meters_like_balanced():
    rgb = _ramp(0.05, 0.4)
    balanced = auto_tone.fit(rgb)
    subject = auto_tone.fit(rgb, style='subject')
    assert subject['adjustments'] == balanced['adjustments']
    assert subject['notes'][0] == 'no subject found, so metered the whole frame as Balanced does'
    assert subject['notes'][1:] == balanced['notes']


def test_gentle_style_moves_every_control_half_as_far():
    rng = np.random.default_rng(6)
    rgb = np.clip(rng.normal(0.22, 0.08, (H, W, 3)), 0, 1).astype(np.float32)
    rgb[..., 0] *= 1.08
    rgb = np.clip(rgb, 0, 1)
    balanced = auto_tone.fit(rgb)['adjustments']
    gentle = auto_tone.fit(rgb, style='gentle')
    assert any(balanced.values())
    for key, value in balanced.items():
        step = 0.1 if key == 'exposure' else 1.0
        half = gentle['adjustments'][key]
        assert abs(half) <= abs(value) / 2 + 1e-9
        assert abs(value) / 2 - abs(half) < step
        assert half == 0 or np.sign(half) == np.sign(value)
    assert gentle['adjustments']['exposure'] == round(gentle['adjustments']['exposure'], 1)
    assert gentle['notes'] == auto_tone._describe(gentle['adjustments'])
