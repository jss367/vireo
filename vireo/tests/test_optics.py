"""Lens geometry, linear-light gain, fringe isolation, precision and mask alignment."""

import numpy as np
import optics
import pytest
from edit_recipe import RecipeError, normalize_recipe
from float_image import FloatImage
from image_edits import _apply_geometry, apply_recipe_to_loaded_image, local_weight_map
from PIL import Image
from tone import srgb_to_linear


@pytest.mark.parametrize("key", ["distortion", "vignetting", "defringe_purple", "defringe_green"])
def test_optics_recipe_validation_and_noop(key):
    assert normalize_recipe({"adjustments": {key: 0}}) is None
    assert normalize_recipe({"adjustments": {key: 40}})["adjustments"][key] == 40
    for value in (True, float("nan"), float("inf"), 101, -101, "20"):
        with pytest.raises(RecipeError):
            normalize_recipe({"adjustments": {key: value}})
    if key.startswith("defringe"):
        with pytest.raises(RecipeError):
            normalize_recipe({"adjustments": {key: -1}})


def test_optics_disabled_is_exact_noop():
    source = FloatImage(np.random.default_rng(4).normal(size=(20, 30, 3)).astype(np.float32))
    assert optics.apply_optics(source, {}) is source
    assert optics.distort_image(source, 0) is source


@pytest.mark.parametrize("amount", [-100, -50, 50, 100])
def test_distortion_restores_known_radial_grid(amount):
    # Construct the recorded grid analytically by inverting r_d=r*(1+k*r²).
    # Corrected vertical lines must have the same x at every image row.
    width, height = 321, 241
    cx, cy = (width - 1) / 2, (height - 1) / 2
    x, y = np.meshgrid(np.arange(width) - cx, np.arange(height) - cy)
    k = -0.2 * amount / 100
    rd = np.sqrt((x * x + y * y) / (cx * cx + cy * cy))
    ru = rd.copy()
    for _ in range(12):
        ru -= (ru * (1 + k * ru ** 2) - rd) / (1 + 3 * k * ru ** 2)
    ratio = np.divide(ru, rd, out=np.ones_like(rd), where=rd > 0)
    # Smooth grid rather than binary lines keeps interpolation error measurable.
    warped = 0.5 + 0.3 * np.cos(x * ratio / 9)
    source = FloatImage(np.repeat(warped[..., None], 3, axis=-1).astype(np.float32))
    corrected = np.asarray(optics.distort_image(source, amount))[..., 0]
    _, inset = optics.distortion_scale(amount)
    expected = 0.5 + 0.3 * np.cos(x * inset / 9)
    np.testing.assert_allclose(corrected[40:-40, 60:-60], expected[40:-40, 60:-60], atol=0.002)


@pytest.mark.parametrize("size", [(1, 1), (1, 25), (25, 1), (121, 81)])
@pytest.mark.parametrize("amount", [-100, 100])
def test_distortion_has_no_empty_borders_and_preserves_flat_field(size, amount):
    source = Image.new("RGB", size, (150, 100, 50))
    corrected = optics.distort_image(source, amount)
    assert corrected.size == size
    np.testing.assert_array_equal(corrected, source)


def test_vignetting_restores_radial_falloff_in_linear_light():
    r2 = optics._radius_squared(121, 81, 0, 81)
    falloff = np.exp2(-1.5 * r2 * r2)
    source = FloatImage(np.repeat((0.3 * falloff)[..., None], 3, axis=-1))
    corrected = optics.apply_optics(source, {"vignetting": 100})
    np.testing.assert_allclose(corrected, 0.3, atol=1e-7)
    # A rendered image gets the same stop gain after decoding its transfer curve.
    gray = Image.new("RGB", (121, 81), (100, 100, 100))
    corrected = optics.apply_optics(gray, {"vignetting": 100})
    linear = srgb_to_linear(np.asarray(corrected) / 255)
    assert linear[0, 0, 0] / linear[40, 60, 0] == pytest.approx(2 ** 1.5, rel=0.02)
    np.testing.assert_array_equal(np.asarray(corrected)[40, 60], (100, 100, 100))


def _fringed_scene(color):
    image = np.full((70, 121, 3), 0.85, dtype=np.float32)
    image[:, :60] = 0.015
    image[:, 60:61] = color
    image[15:55, 85:115] = color  # actual colored subject, away from edges
    return image


@pytest.mark.parametrize("key,color,other", [
    ("defringe_purple", [0.55, 0.06, 0.8], "defringe_green"),
    ("defringe_green", [0.04, 0.7, 0.2], "defringe_purple"),
])
def test_defringe_reduces_only_targeted_color_near_contrast_edges(key, color, other):
    pixels = _fringed_scene(color)
    source = FloatImage(pixels)
    corrected = np.asarray(optics.apply_optics(source, {key: 100}))
    # Oklab chroma at the fringe falls strongly while its lightness is retained.
    lab_before = np.cbrt(pixels @ optics._RGB_LMS.T) @ optics._LMS_LAB.T
    lab_after = np.cbrt(np.maximum(corrected, 0) @ optics._RGB_LMS.T) @ optics._LMS_LAB.T
    assert np.linalg.norm(lab_after[35, 60, 1:]) < np.linalg.norm(lab_before[35, 60, 1:]) * 0.1
    assert lab_after[35, 60, 0] == pytest.approx(lab_before[35, 60, 0], abs=1e-6)
    np.testing.assert_array_equal(corrected[25:45, 95:105], pixels[25:45, 95:105])
    np.testing.assert_array_equal(corrected[:, :58], pixels[:, :58])
    np.testing.assert_array_equal(optics.apply_optics(source, {other: 100}), pixels)


@pytest.mark.parametrize("encoding", ["linear", "srgb"])
def test_optics_tiles_are_seamless_and_preserve_float_precision(encoding, monkeypatch):
    source = FloatImage(_fringed_scene([0.55, 0.06, 0.8]), encoding=encoding)
    source.pixels[30, 30] = [1.8, 1.80001, 1.80002]
    adjustments = {"vignetting": 25, "defringe_purple": 70, "defringe_green": 30}
    whole = optics.apply_optics(source, adjustments)
    monkeypatch.setattr(optics, "_TILE_PIXELS", 121 * 3)
    tiled = optics.apply_optics(source, adjustments)
    np.testing.assert_array_equal(tiled, whole)
    assert tiled.encoding == encoding
    assert np.asarray(tiled)[30, 30].max() > 1
    assert np.diff(np.asarray(tiled)[30, 30]).min() > 0
    transformed = optics.distort_image(tiled, -30)
    assert isinstance(transformed, FloatImage)
    assert transformed.encoding == encoding
    assert np.asarray(transformed).max() > 1


def test_optics_preserves_alpha_and_supports_palette_transparency():
    source = Image.fromarray(np.random.default_rng(1).integers(30, 220, (30, 40, 4), dtype=np.uint8))
    corrected = optics.apply_optics(source, {"vignetting": 50, "defringe_purple": 100})
    np.testing.assert_array_equal(np.asarray(corrected)[..., 3], np.asarray(source)[..., 3])
    palette = source.convert("P")
    palette.info["transparency"] = 0
    assert optics.apply_optics(palette, {"vignetting": 10}).mode == "RGBA"
    mapped = optics.distort_image(palette, 50)
    expected = optics.distort_image(palette.convert("RGBA"), 50)
    assert mapped.mode == "RGBA"
    np.testing.assert_array_equal(mapped, expected)


def test_subject_mask_and_overlay_follow_lens_geometry_before_crop():
    pixels = np.zeros((80, 120), dtype=np.uint8)
    pixels[15:65, 10:35] = 255
    mask = Image.fromarray(pixels)
    recipe = {"adjustments": {"distortion": 80}, "rotation": 90,
              "flip": {"horizontal": True}, "crop": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8},
              "local": {"mask": {"ref": "abcdef123456", "source_digest": "test"},
                        "regions": [{"region": "subject", "adjustments": {"exposure": 1}}]}}
    overlay = local_weight_map(mask, mask.size, recipe)
    expected = np.asarray(_apply_geometry(mask, recipe)) / 255
    np.testing.assert_allclose(overlay, expected, atol=1e-7)
    image = Image.new("RGB", mask.size, (65, 65, 65))
    actual = np.asarray(apply_recipe_to_loaded_image(image, recipe, local_mask=mask))
    assert actual[expected > 0.99].mean() > 85
    np.testing.assert_array_equal(actual[expected == 0], 65)


def test_vignetting_uses_source_center_before_crop():
    source = FloatImage(np.full((81, 121, 3), 0.1, dtype=np.float32))
    recipe = {"adjustments": {"vignetting": 70, "distortion": -30}}
    full = np.asarray(apply_recipe_to_loaded_image(source, recipe))
    recipe["crop"] = {"x": 0.1, "y": 0.2, "w": 0.5, "h": 0.5}
    crop = apply_recipe_to_loaded_image(source, recipe)
    np.testing.assert_array_equal(crop, full[16:57, 12:73])
