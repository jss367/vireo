"""Subject preservation, background noise reduction, and recipe persistence."""

import copy

import numpy as np
import pytest
from background_denoise import protection_mask, protection_weight
from edit_recipe import RecipeError, normalize_recipe, recipe_to_json
from float_image import FloatImage
from image_edits import apply_recipe_to_loaded_image, local_weight_map
from PIL import Image


def recipe(amount=70):
    return {"local": {
        "mask": {"ref": "a1b2c3d4e5f6", "source_digest": "test"},
        "regions": [], "background_denoise": amount,
    }}


def noisy_image(floating=False):
    pixels = np.random.default_rng(42).normal(120, 14, (160, 240, 3))
    if floating:
        return FloatImage((pixels / 255).astype(np.float32), encoding="srgb")
    return Image.fromarray(np.clip(pixels, 0, 255).astype(np.uint8))


def subject_mask():
    mask = np.zeros((160, 240), np.uint8)
    mask[30:130, 40:120] = 255
    mask[65:85, 70:90] = 0  # Accidental hole over an eye/dark face.
    return Image.fromarray(mask)


def test_strength_alone_retains_mask_and_round_trips():
    import json
    normalized = normalize_recipe(recipe())
    assert normalized["local"]["background_denoise"] == 70
    assert normalize_recipe(json.loads(recipe_to_json(normalized))) == normalized
    assert normalize_recipe(recipe(0)) is None
    with pytest.raises(RecipeError, match="mask"):
        normalize_recipe({"local": {"background_denoise": 50}})


@pytest.mark.parametrize("amount", [-1, 101, True, float("nan"), "50", None])
def test_invalid_strength_rejected(amount):
    with pytest.raises(RecipeError, match="background_denoise"):
        normalize_recipe(recipe(amount))


@pytest.mark.parametrize("floating", [False, True])
def test_preserves_edited_subject_and_reduces_background_noise(floating):
    image = noisy_image(floating)
    mask = subject_mask()
    edits = recipe()
    edits["adjustments"] = {"exposure": 0.2, "sharpen": 25}
    edits["local"]["regions"] = [{"region": "subject", "adjustments": {"saturation": 10}}]
    baseline_edits = copy.deepcopy(edits)
    baseline_edits["local"].pop("background_denoise")
    baseline = apply_recipe_to_loaded_image(image, baseline_edits, local_mask=mask)
    smoothed = apply_recipe_to_loaded_image(image, edits, local_mask=mask)
    before, after = np.asarray(baseline), np.asarray(smoothed)
    # Includes the hole in the automatic mask and the 8px safety margin.
    np.testing.assert_array_equal(before[30:130, 40:128], after[30:130, 40:128])
    assert after[:, 165:].std() < before[:, 165:].std() * 0.9
    assert np.isfinite(after).all()


@pytest.mark.parametrize("mask", [None, Image.new("L", (30, 90), 255), Image.new("L", (240, 160))])
def test_unusable_mask_never_denoises_whole_photo(mask):
    image = noisy_image()
    output = apply_recipe_to_loaded_image(image, recipe(), local_mask=mask)
    np.testing.assert_array_equal(np.asarray(image), np.asarray(output))


def test_holes_filled_before_crop_and_manual_corrections_respected():
    mask = subject_mask()
    assert np.asarray(protection_mask(mask))[75, 80] == 255
    assert np.asarray(protection_mask(mask, corrected=True))[75, 80] == 0
    edits = recipe()
    # Crop through the hole: filling after geometry would leave it open.
    edits["crop"] = {"x": 0, "y": 0, "w": 0.34, "h": 1}
    weight = local_weight_map(mask, mask.size, edits, native_size=mask.size)
    assert weight[75, -1] == 1


def test_outward_transition_scales_without_eroding_core():
    pixels = np.zeros((100, 200), np.uint8)
    pixels[:, :80] = 255
    full = protection_weight(Image.fromarray(pixels), 1)
    half = protection_weight(Image.fromarray(pixels[::2, ::2]), 0.5)
    assert np.all(full[:, :88] == 1)
    assert np.all(full[:, 104:] == 0)
    assert 0 < full[50, 95] < 1
    np.testing.assert_allclose(full[50, ::2], half[25], atol=0.1)
    feathered = protection_weight(Image.fromarray(pixels), 1, feather=50)
    assert np.all(feathered[:, :88] == 1)
    assert feathered[50, 110] > 0


def test_overlay_matches_cropped_rotated_render_weight(monkeypatch):
    import detail
    image = Image.new("RGB", (240, 160), (100, 100, 100))
    edits = recipe(100)
    edits.update(rotation=90, crop={"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8})
    monkeypatch.setattr(detail, "apply_detail", lambda image, **kw: Image.new("RGB", image.size))
    rendered = apply_recipe_to_loaded_image(
        image, edits, local_mask=subject_mask(), native_size=image.size, max_size=120,
    )
    weight = local_weight_map(
        subject_mask(), image.size, edits, native_size=image.size, render_size=rendered.size,
    )
    np.testing.assert_array_equal(np.asarray(rendered)[..., 0], (100 * weight + 0.5).astype(np.uint8))


def test_camera_profile_uses_destination_metadata(monkeypatch):
    import detail
    calls = []
    def denoise(image, **kwargs):
        calls.append(kwargs)
        return image.copy()
    monkeypatch.setattr(detail, "apply_detail", denoise)
    apply_recipe_to_loaded_image(
        noisy_image(), recipe(), local_mask=subject_mask(),
        camera_metadata={"camera_make": "NIKON CORPORATION", "camera_model": "NIKON D850", "iso": 3200},
    )
    assert len(calls) == 1
    assert calls[0]["denoise_mode"] == "camera"
    assert calls[0]["noise_reduction"] == 70
    assert calls[0]["noise_profile"] is not None


@pytest.mark.parametrize("mode", ["L", "RGBA"])
def test_grayscale_and_alpha_survive_background_smoothing(mode):
    image = noisy_image().convert(mode)
    if mode == "RGBA":
        image.putalpha(Image.fromarray(np.random.default_rng(6).integers(0, 256, (160, 240), dtype=np.uint8)))
    rendered = apply_recipe_to_loaded_image(image, recipe(), local_mask=subject_mask())
    expected = np.asarray(image.convert(rendered.mode))
    np.testing.assert_array_equal(np.asarray(rendered)[30:130, 40:128], expected[30:130, 40:128])
    if mode == "RGBA":
        np.testing.assert_array_equal(np.asarray(rendered)[..., 3], expected[..., 3])
