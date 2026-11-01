"""Shadows/Highlights base: detail retention, artifact limits, and tiling."""

import itertools

import image_edits
import numpy as np
import pytest
from float_image import FloatImage
from image_edits import apply_recipe_to_loaded_image
from PIL import Image
from tone import (
    RANGE_STOPS,
    LocalAdjustments,
    RangeBase,
    ToneAdjustments,
    apply_adjustments,
    range_sigma,
    srgb_to_linear,
)


def _texture(center, amplitude=0.005, period=8, width=256, height=96):
    level = center + amplitude * np.sin(np.arange(width) * 2 * np.pi / period)
    return np.broadcast_to(level[None, :, None], (height, width, 3)).astype(np.float32).copy()


def _contrast(rgb):
    return np.std(np.log2(srgb_to_linear(rgb[24:-24, 48:-48, 0])))


def _spatial(src, **settings):
    """Render through a base sized for ``src``, as the renderer sizes it."""
    return apply_adjustments(
        src, ToneAdjustments(**settings), range_sigma=range_sigma(max(src.shape[:2])),
    )


@pytest.mark.parametrize("settings,center", [({"shadows": 100}, 0.15), ({"highlights": -100}, 0.675)])
def test_range_adjustment_preserves_texture_at_same_average_brightness(settings, center):
    src = _texture(center)
    point = apply_adjustments(src, ToneAdjustments(**settings))
    spatial = _spatial(src, **settings)
    # Relative luminance contrast measures texture independently of its new
    # brightness. The region moves as a whole, like an exposure change, so
    # texture keeps its contrast; a per-pixel curve flattens it.
    retention = _contrast(spatial) / _contrast(src)
    assert 0.95 < retention < 1.05
    assert _contrast(spatial) > 2 * _contrast(point)
    assert abs(float(spatial.mean() - point.mean())) < 0.001
    assert abs(float(spatial.mean() - src.mean())) > 0.05


@pytest.mark.parametrize("settings", [{"shadows": 100}, {"highlights": -100}, {"shadows": 100, "highlights": -100}])
def test_hard_subject_edge_has_no_visible_bright_or_dark_rim(settings):
    src = np.full((64, 512, 3), 0.8, dtype=np.float32)
    src[:, :256] = 0.2
    point = apply_adjustments(src, ToneAdjustments(**settings))
    spatial = _spatial(src, **settings)
    # A flat field on either side should take its own gain right up to the
    # edge. Keep the worst edge-adjacent deviation below half a display step.
    assert np.max(np.abs(spatial - point)) < 0.5 / 255


def test_extreme_range_combinations_keep_ramps_ordered_and_black_anchored():
    ramp = np.broadcast_to(np.linspace(0, 1, 1025, dtype=np.float32)[None, :, None], (8, 1025, 3))
    for shadows, highlights in itertools.product((-100, 0, 100), repeat=2):
        out = _spatial(ramp, shadows=shadows, highlights=highlights)
        assert np.isfinite(out).all()
        assert np.min(np.diff(out[0, :, 0])) >= -2e-6
        np.testing.assert_allclose(out[:, 0], 0.0, atol=1e-6)
        if highlights >= 0:
            np.testing.assert_allclose(out[:, -1], 1.0, atol=1e-6)


def test_deep_shadow_noise_gets_its_neighborhoods_gain_and_no_more():
    rng = np.random.default_rng(7)
    src = np.repeat(rng.uniform(0.002, 0.012, (64, 128, 1)), 3, axis=-1).astype(np.float32)
    spatial = _spatial(src, shadows=100)
    gain = srgb_to_linear(spatial[8:-8, 8:-8, 0]) / srgb_to_linear(src[8:-8, 8:-8, 0])
    # One lift for the whole region, like an exposure change: noise keeps its
    # relative amplitude instead of being exaggerated or smoothed away.
    assert 2 ** (0.9 * RANGE_STOPS) < float(np.median(gain)) <= 2 ** RANGE_STOPS + 1e-5
    assert float(np.std(np.log2(gain))) < 0.05


def test_highlights_recover_raw_radiance_above_display_white():
    # A sky ramp from just under display white to two stops over it: without
    # recovery the shoulder squeezes it into the top few display levels.
    sky = np.broadcast_to(
        np.geomspace(0.9, 4.0, 512, dtype=np.float32)[None, :, None], (32, 512, 3),
    ).copy()
    flat = apply_adjustments(sky, input_linear=True)
    recovered = apply_adjustments(
        sky, ToneAdjustments(highlights=-100), input_linear=True,
        range_sigma=range_sigma(512),
    )

    def levels(rgb):
        return len(np.unique(np.round(rgb[16, :, 0] * 255)))

    assert levels(recovered) > 2 * levels(flat)
    assert np.min(np.diff(recovered[16, :, 0])) >= -2e-6


def test_spatial_shadow_lift_preserves_in_gamut_color_ratios():
    linear = srgb_to_linear(_texture(0.325)) * np.array([1.2, 0.9, 0.6], dtype=np.float32)
    out = apply_adjustments(linear, ToneAdjustments(shadows=100), input_linear=True, range_sigma=range_sigma(256))
    mapped = srgb_to_linear(out)
    np.testing.assert_allclose(mapped[..., 0] / mapped[..., 1], linear[..., 0] / linear[..., 1], atol=1e-6)
    np.testing.assert_allclose(mapped[..., 2] / mapped[..., 1], linear[..., 2] / linear[..., 1], atol=1e-6)


def test_local_range_adjustment_leaves_unselected_texture_unchanged():
    src = _texture(0.15)
    weight = np.zeros(src.shape[:2], dtype=np.float32)
    weight[:, :128] = 1
    spatial = apply_adjustments(
        src,
        local=LocalAdjustments(weight=weight, subject={"shadows": 100}),
        range_sigma=range_sigma(256),
    )
    np.testing.assert_allclose(spatial[:, 128:], src[:, 128:], atol=2e-7)
    assert spatial[:, :128].mean() > src[:, :128].mean() + 0.05


def test_local_exposure_moves_the_base_with_the_pixel():
    # A subject brightened two stops by a local exposure is no longer a
    # shadow: Shadows must lift it as the brighter region it now is.
    src = _texture(0.15)
    weight = np.ones(src.shape[:2], dtype=np.float32)
    local = apply_adjustments(
        src, ToneAdjustments(shadows=100),
        local=LocalAdjustments(weight=weight, subject={"exposure": 2.0}),
        range_sigma=range_sigma(256),
    )
    global_ = _spatial(src, exposure=2.0, shadows=100)
    np.testing.assert_allclose(local, global_, atol=1e-5)


@pytest.mark.parametrize("encoding", ["linear", "srgb"])
def test_float_tiled_masked_render_matches_whole_frame_and_preserves_precision(encoding, monkeypatch):
    src = _texture(0.325, width=128, height=173)
    src += np.linspace(-0.02, 0.02, 173, dtype=np.float32)[:, None, None]
    if encoding == "linear":
        src = srgb_to_linear(src)
    image = FloatImage(src, encoding=encoding)
    mask = Image.fromarray(np.tile(np.linspace(0, 255, 128, dtype=np.uint8), (173, 1)))
    recipe = {
        "adjustments": {"shadows": 40, "highlights": -30},
        "local": {
            "mask": {"ref": "0123456789ab", "source_digest": "test-source", "feather": 2},
            "regions": [{"region": "subject", "adjustments": {"shadows": 60}}],
        },
    }
    whole = np.asarray(apply_recipe_to_loaded_image(image, recipe, local_mask=mask))
    monkeypatch.setattr(image_edits, "_ADJUST_TILE_PIXELS", 128 * 7)
    tiled = np.asarray(apply_recipe_to_loaded_image(image, recipe, local_mask=mask))
    np.testing.assert_array_equal(tiled, whole)
    assert len(np.unique(tiled[..., 0])) > 256


def test_range_filter_tracks_loaded_resolution_independently_of_detail_override():
    src = FloatImage(_texture(0.325), encoding="srgb")
    recipe = {"adjustments": {"shadows": 100}}
    normal = apply_recipe_to_loaded_image(src, recipe, native_size=src.size)
    override = apply_recipe_to_loaded_image(src, recipe, native_size=src.size, detail_scale=0.25)
    np.testing.assert_array_equal(np.asarray(normal), np.asarray(override))


def test_scaled_preview_agrees_with_downsampled_full_render():
    src = FloatImage(_texture(0.325, amplitude=0.015, period=32, width=512, height=256), encoding="srgb")
    recipe = {"adjustments": {"shadows": 100}}
    full = apply_recipe_to_loaded_image(src, recipe, native_size=src.size, max_size=256)
    preview_source = src.resize((256, 128), Image.Resampling.LANCZOS)
    preview = apply_recipe_to_loaded_image(preview_source, recipe, native_size=src.size)
    np.testing.assert_allclose(np.asarray(preview), np.asarray(full), atol=0.002)


def test_neutral_range_controls_keep_existing_render_exact():
    src = _texture(0.325)
    for settings in ({}, {"exposure": 0.5}, {"whites": -50, "blacks": 50}):
        np.testing.assert_array_equal(
            apply_adjustments(src, ToneAdjustments(**settings)),
            _spatial(src, **settings),
        )


def test_base_sampled_by_tile_matches_the_whole_frame_base():
    rng = np.random.default_rng(5)
    src = rng.uniform(0, 1, (75, 301, 3)).astype(np.float32)
    base = RangeBase.build(src, range_sigma(301) * 4)
    assert base.step > 1
    log_y = np.log2(np.maximum(rng.uniform(0, 1, (75, 301)), 1e-6)).astype(np.float32)
    whole = base.at(log_y)
    for top, left in ((0, 0), (13, 0), (40, 97), (74, 300)):
        np.testing.assert_array_equal(
            base.at(log_y[top:top + 11, left:left + 23], top, left),
            whole[top:top + 11, left:left + 23],
        )


@pytest.mark.parametrize('encoding', ['linear', 'srgb', 'rgba'])
@pytest.mark.parametrize('masked', [False, True])
def test_rectangular_tiles_preserve_edges_corners_alpha_and_local_weights(encoding, masked, monkeypatch):
    rng = np.random.default_rng(318)
    src = rng.uniform(0, 1, (173, 257, 3)).astype(np.float32)
    # Flat shadows and a strong subject boundary cross horizontal and vertical
    # tile seams, including their intersections and the outer image edges.
    src[:83, :119] = 0.03
    src[83:, 119:] = 0.9
    if encoding == 'rgba':
        rgba = np.concatenate([src * 255, rng.integers(0, 256, (173, 257, 1))], axis=2).astype(np.uint8)
        image = Image.fromarray(rgba)
    else:
        if encoding == 'linear':
            src = src * 2 - 0.05  # RAW negative channels and above-white radiance
        image = FloatImage(src, encoding=encoding)
    before = np.asarray(image).copy()
    recipe = {'adjustments': {'exposure': 0.4, 'shadows': 65, 'highlights': -45}}
    mask = None
    if masked:
        mask = Image.fromarray(rng.integers(0, 256, (173, 257), dtype=np.uint8))
        recipe['local'] = {
            'mask': {'ref': '0123456789ab', 'source_digest': 'test-source'},
            'regions': [
                {'region': 'subject', 'adjustments': {'shadows': 30, 'exposure': -0.5}},
                {'region': 'background', 'adjustments': {'highlights': -25}},
            ],
        }
    whole = np.asarray(apply_recipe_to_loaded_image(image, recipe, local_mask=mask))
    monkeypatch.setattr(image_edits, '_ADJUST_TILE_PIXELS', 31 * 31)
    tiled = np.asarray(apply_recipe_to_loaded_image(image, recipe, local_mask=mask))
    # The base is built once per frame and sampled by frame position, so a
    # strip computes exactly what the whole frame does.
    np.testing.assert_array_equal(tiled, whole)
    np.testing.assert_array_equal(np.asarray(image), before)
    if encoding == 'rgba':
        np.testing.assert_array_equal(tiled[..., 3], rgba[..., 3])


def test_tiles_do_not_overlap(monkeypatch):
    import tone

    # A shallow 8K-wide image at the tile budget, without allocating a full
    # 46MP frame. Count work instead of asserting noisy wall-clock times.
    image = Image.new('RGB', (8288, 300), (50, 80, 110))
    processed_pixels = []

    def counted_tone(rgb, adjustments=None, **kwargs):
        processed_pixels.append(rgb.shape[0] * rgb.shape[1])
        return rgb

    monkeypatch.setattr(tone, 'apply_adjustments', counted_tone)
    rendered = apply_recipe_to_loaded_image(image, {'adjustments': {'shadows': 15}})
    assert sum(processed_pixels) == image.width * image.height
    assert max(processed_pixels) < image_edits._ADVANCED_COLOR_TILE_PIXELS * 1.25
    np.testing.assert_array_equal(np.asarray(rendered), np.asarray(image))
