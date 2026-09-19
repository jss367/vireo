"""Image-quality and recipe invariants for point curves and sampled colors."""
import image_edits
import numpy as np
import pytest
from image_edits import RecipeError, apply_recipe, normalize_recipe, recipe_to_json
from PIL import Image
from tone import (
    _hsl_to_rgb,
    apply_adjustments,
    apply_point_color,
    apply_point_curves,
    apply_tone_curve,
)


def test_curves_interpolate_arbitrary_points_and_isolate_channels():
    rgb = np.array([[.2, .3, .4], [.6, .7, .8]], dtype=np.float32)
    actual = apply_point_curves(rgb, {"red": [[0, 10], [20, 40], [100, 80]]})
    np.testing.assert_allclose(actual[:, 0], [.4, .6], atol=1e-6)
    np.testing.assert_array_equal(actual[:, 1:], rgb[:, 1:])
    composite = {"rgb": [[0, 0], [100, 50]], "red": [[0, 0], [50, 100], [100, 100]]}
    np.testing.assert_allclose(apply_point_curves(rgb, composite), rgb * [1, .5, .5], atol=1e-6)


def test_promoting_legacy_curve_preserves_appearance():
    rgb = np.random.default_rng(12).random((12, 16, 3), dtype=np.float32)
    legacy = {"black": 4, "shadows": 35, "midtones": 47, "highlights": 83, "white": 96}
    points = [[i * 25, value] for i, value in enumerate(legacy.values())]
    np.testing.assert_allclose(apply_point_curves(rgb, {"rgb": points}), apply_tone_curve(rgb, legacy), atol=1e-7)


def test_point_color_wraps_red_and_preserves_outside_ranges_and_neutrals():
    hues = np.array([359, 1, 120, 0], dtype=np.float32) / 360
    rgb = _hsl_to_rgb(hues, np.array([1, 1, 1, 0], dtype=np.float32), np.full(4, .5))
    actual = apply_point_color(rgb, [{"sample": [0, 100, 50], "hue_range": 20, "hue": 60}])
    assert actual[0, 1] > .9 and actual[1, 1] > .9
    np.testing.assert_array_equal(actual[2:], rgb[2:])


def test_point_color_saturation_and_luminance_ranges_are_selective():
    rgb = _hsl_to_rgb(np.zeros(3), np.array([.8, .2, .8]), np.array([.5, .5, .15]))
    actual = apply_point_color(rgb, [{"sample": [0, 80, 50], "saturation_range": 20, "luminance_range": 20, "hue": 120}])
    assert actual[0, 1] > actual[0, 0]
    np.testing.assert_array_equal(actual[1:], rgb[1:])


def test_overlapping_samples_are_order_independent_and_neutral_samples_do_not_dilute():
    rgb = np.array([[.9, .1, .1]], dtype=np.float32)
    samples = [{"sample": [0, 80, 50], "hue": 30}, {"sample": [2, 75, 45], "luminance": 20}]
    np.testing.assert_allclose(apply_point_color(rgb, samples), apply_point_color(rgb, samples[::-1]))
    np.testing.assert_array_equal(apply_point_color(rgb, samples), apply_point_color(rgb, samples + [{"sample": [0, 80, 50]}]))
    np.testing.assert_array_equal(apply_point_color(rgb, [{"sample": [0, 80, 50]}]), rgb)


def test_new_controls_run_in_local_branch_and_tiled_rgba_exports(monkeypatch):
    adjustments = {"point_curves": {"red": [[0, 0], [35, 55], [100, 95]]},
                   "point_color": [{"sample": [0, 80, 50], "hue": 40, "luminance": -10}]}
    rgb = np.random.default_rng(14).random((13, 17, 3), dtype=np.float32)
    expected = apply_adjustments(rgb, exposure=.4, **adjustments)
    actual = apply_adjustments(rgb, local_weight=np.ones((13, 17)), local_subject={"exposure": .4}, **adjustments)
    np.testing.assert_allclose(actual, expected, atol=1e-6)
    rgba = np.random.default_rng(15).integers(0, 256, (13, 17, 4), dtype=np.uint8)
    image = Image.fromarray(rgba)
    whole = np.asarray(apply_recipe(image, {"adjustments": adjustments}))
    monkeypatch.setattr(image_edits, '_ADVANCED_COLOR_TILE_PIXELS', 17)
    tiled = np.asarray(apply_recipe(image, {"adjustments": adjustments}))
    np.testing.assert_array_equal(tiled, whole)
    np.testing.assert_array_equal(tiled[..., 3], rgba[..., 3])
    assert not np.array_equal(tiled[..., :3], rgba[..., :3])


def test_recipe_roundtrip_and_identity_canonicalization():
    recipe = {"adjustments": {"point_curves": {"rgb": [[0, 0], [35, 35], [100, 100]], "blue": [[0, 10], [100, 90]]},
                              "point_color": [{"sample": [360, 80, 50], "hue_range": 30, "hue": 0}]}}
    normalized = normalize_recipe(recipe)
    assert normalized['adjustments']['point_curves'] == {'blue': [[0, 10], [100, 90]]}
    assert normalized['adjustments']['point_color'] == [{'sample': [0, 80, 50]}]
    assert recipe_to_json(normalized) == recipe_to_json(recipe)
    assert normalize_recipe({'adjustments': {'point_curves': {'rgb': [[0, 0], [100, 100]]}}}) is None


@pytest.mark.parametrize('value', [
    [], {'alpha': [[0, 0], [100, 100]]}, {'red': [[0, 0]]},
    {'rgb': [[1, 0], [100, 100]]}, {'rgb': [[0, 0], [99, 100]]},
    {'rgb': [[0, 0], [50, 20], [50, 30], [100, 100]]},
    {'rgb': [[0, 0], [50, float('nan')], [100, 100]]},
    {'rgb': [[False, 0], [100, 100]]}, {'rgb': [[0, 0], [100, 101]]},
    {'rgb': [[i * 100 / 32, i] for i in range(33)]},
])
def test_rejects_invalid_curves(value):
    with pytest.raises(RecipeError):
        normalize_recipe({'adjustments': {'point_curves': value}})


@pytest.mark.parametrize('value', [
    {}, 'red', [{'sample': [0, 50]}], [{'sample': [0, 50, 50], 'unknown': 1}],
    [{'sample': [0, 50, 50], 'hue_range': 0}], [{'sample': [0, 50, 50], 'saturation_range': 101}],
    [{'sample': [0, 50, 50], 'hue': float('inf')}], [{'sample': [True, 50, 50]}],
    [{'sample': [0, 50, 50]}] * 9,
    *[[{'sample': [0, saturation, 50]}] for saturation in (0, .5, 1, 1.0000004)],
    *[[{'sample': [0, 100, luminance]}] for luminance in (0, .0000004, 99.9999996, 100)],
])
def test_rejects_invalid_samples(value):
    with pytest.raises(RecipeError):
        normalize_recipe({'adjustments': {'point_color': value}})


@pytest.mark.parametrize('local', [False, True])
def test_point_color_selects_after_other_color_adjustments(local):
    rgb = np.array([[[.8, .2, .1], [.15, .55, .1]]], dtype=np.float32)
    base = {'vibrance': 30, 'saturation': -25, 'color_grading': {'midtones': {'hue': 180, 'saturation': 30}}}
    if local:
        base.update(local_weight=np.array([[1, .5]], dtype=np.float32), local_subject={'saturation': -20})
    source = apply_adjustments(rgb, **base)
    samples = [{'sample': [15, 50, 40], 'hue_range': 90, 'hue': 80, 'saturation': 30}]
    np.testing.assert_allclose(apply_adjustments(rgb, point_color=samples, **base),
                               apply_point_color(source, samples), atol=1e-6)
