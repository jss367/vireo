"""Detail retention, artifact limits, and shared-renderer integration."""

import itertools

import image_edits
import numpy as np
import pytest
from float_image import FloatImage
from image_edits import apply_recipe_to_loaded_image
from PIL import Image
from tone import RANGE_RADIUS, apply_adjustments, srgb_to_linear


def _texture(center, amplitude=0.005, period=8, width=256, height=96):
    level = center + amplitude * np.sin(np.arange(width) * 2 * np.pi / period)
    return np.broadcast_to(level[None, :, None], (height, width, 3)).astype(np.float32).copy()


def _contrast(rgb):
    return np.std(np.log2(srgb_to_linear(rgb[24:-24, 48:-48, 0])))


@pytest.mark.parametrize("settings,center", [({"shadows": 100}, 0.325), ({"highlights": -100}, 0.675)])
def test_range_adjustment_preserves_texture_at_same_average_brightness(settings, center):
    src = _texture(center)
    point = apply_adjustments(src, **settings)
    spatial = apply_adjustments(src, range_radius=RANGE_RADIUS, **settings)
    # Relative luminance contrast measures texture independently of its new
    # brightness. Both controls should preserve most of it without sharpening.
    retention = _contrast(spatial) / _contrast(src)
    assert 0.8 < retention < 1.05
    assert _contrast(spatial) > 2 * _contrast(point)
    assert abs(float(spatial.mean() - point.mean())) < 0.001


@pytest.mark.parametrize("settings", [{"shadows": 100}, {"highlights": -100}, {"shadows": 100, "highlights": -100}])
def test_hard_subject_edge_has_no_visible_bright_or_dark_rim(settings):
    src = np.full((64, 512, 3), 0.8, dtype=np.float32)
    src[:, :256] = 0.2
    point = apply_adjustments(src, **settings)
    spatial = apply_adjustments(src, range_radius=RANGE_RADIUS, **settings)
    # A flat field on either side has no texture to restore. Keep even the
    # worst edge-adjacent deviation below a tenth of an 8-bit display step.
    assert np.max(np.abs(spatial - point)) < 0.1 / 255


def test_extreme_range_combinations_keep_ramps_ordered_and_endpoints_anchored():
    ramp = np.broadcast_to(np.linspace(0, 1, 1025, dtype=np.float32)[None, :, None], (1, 1025, 3))
    for shadows, highlights in itertools.product((-100, 0, 100), repeat=2):
        out = apply_adjustments(ramp, shadows=shadows, highlights=highlights, range_radius=RANGE_RADIUS)
        assert np.isfinite(out).all()
        assert np.min(np.diff(out[0, :, 0])) >= -2e-6
        np.testing.assert_allclose(out[:, [0, -1]], ramp[:, [0, -1]], atol=1e-6)


def test_deep_shadow_noise_is_not_boosted_by_detail_restoration():
    rng = np.random.default_rng(7)
    src = np.repeat(rng.uniform(0, 0.012, (64, 128, 1)), 3, axis=-1).astype(np.float32)
    point = apply_adjustments(src, shadows=100)
    spatial = apply_adjustments(src, shadows=100, range_radius=RANGE_RADIUS)
    np.testing.assert_allclose(spatial, point, atol=2e-7)


def test_spatial_shadow_lift_preserves_in_gamut_color_ratios():
    linear = srgb_to_linear(_texture(0.325)) * np.array([1.2, 0.9, 0.6], dtype=np.float32)
    out = apply_adjustments(linear, input_linear=True, shadows=100, range_radius=RANGE_RADIUS)
    mapped = srgb_to_linear(out)
    np.testing.assert_allclose(mapped[..., 0] / mapped[..., 1], linear[..., 0] / linear[..., 1], atol=1e-6)
    np.testing.assert_allclose(mapped[..., 2] / mapped[..., 1], linear[..., 2] / linear[..., 1], atol=1e-6)


def test_local_range_adjustment_leaves_unselected_texture_unchanged():
    src = _texture(0.325)
    weight = np.zeros(src.shape[:2], dtype=np.float32)
    weight[:, :128] = 1
    spatial = apply_adjustments(
        src, local_weight=weight, local_subject={"shadows": 100}, range_radius=RANGE_RADIUS,
    )
    np.testing.assert_allclose(spatial[:, 128:], src[:, 128:], atol=2e-7)
    assert spatial[:, :128].mean() > src[:, :128].mean() + 0.1


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
            apply_adjustments(src, **settings),
            apply_adjustments(src, range_radius=RANGE_RADIUS, **settings),
        )
