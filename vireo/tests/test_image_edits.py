import os
import sys

import pytest
from PIL import Image

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from image_edits import RecipeError, apply_recipe, normalize_recipe, recipe_to_json


def test_normalize_recipe_drops_noop():
    assert normalize_recipe({}) is None
    assert normalize_recipe({"rotation": 0, "crop": {"x": 0, "y": 0, "w": 1, "h": 1}}) is None


def test_normalize_recipe_rejects_out_of_bounds_crop():
    with pytest.raises(RecipeError, match="crop must fit"):
        normalize_recipe({"crop": {"x": 0.5, "y": 0.5, "w": 0.6, "h": 0.6}})


def test_normalize_recipe_rejects_boolean_crop_coordinates():
    with pytest.raises(RecipeError, match="crop must include numeric"):
        normalize_recipe({"crop": {"x": False, "y": False, "w": True, "h": True}})


def test_normalize_recipe_rejects_crop_that_rounds_out_of_bounds():
    with pytest.raises(RecipeError, match="crop must fit"):
        normalize_recipe({"crop": {"x": 0, "y": 0, "w": 0.0000001, "h": 1}})


def test_normalize_recipe_rejects_fractional_rotation():
    with pytest.raises(RecipeError, match="rotation"):
        normalize_recipe({"rotation": 90.9})


def test_normalize_recipe_accepts_straighten():
    assert normalize_recipe({"straighten": 1.23456}) == {
        "version": 1,
        "straighten": 1.2346,
    }


def test_normalize_recipe_rejects_out_of_range_straighten():
    with pytest.raises(RecipeError, match="straighten"):
        normalize_recipe({"straighten": 46})


def test_normalize_recipe_rejects_non_boolean_flip():
    with pytest.raises(RecipeError, match="flip.horizontal"):
        normalize_recipe({"flip": {"horizontal": "false"}})


@pytest.mark.parametrize(
    ("recipe", "message"),
    [
        ({"flip": []}, "flip must be an object"),
        ({"adjustments": []}, "adjustments must be an object"),
    ],
)
def test_normalize_recipe_rejects_falsey_non_object_sections(recipe, message):
    with pytest.raises(RecipeError, match=message):
        normalize_recipe(recipe)


def test_recipe_json_is_canonical():
    assert recipe_to_json({"flip": {"vertical": True}, "rotation": 90}) == (
        '{"flip":{"vertical":true},"rotation":90,"version":1}'
    )


def test_normalize_recipe_canonicalizes_white_balance():
    assert normalize_recipe(
        {
            "adjustments": {
                "temperature": 20,
                "tint": -10,
                "exposure": 0,
            }
        }
    ) == {
        "version": 1,
        "adjustments": {
            "white_balance": {
                "temperature": 20.0,
                "tint": -10.0,
            },
        },
    }


def test_normalize_recipe_rejects_invalid_white_balance():
    with pytest.raises(RecipeError, match="white_balance.temperature"):
        normalize_recipe({"adjustments": {"white_balance": {"temperature": 200}}})


def test_apply_recipe_rotates_flips_and_crops():
    img = Image.new("RGB", (100, 60), "white")
    edited = apply_recipe(
        img,
        {
            "rotation": 90,
            "flip": {"horizontal": True},
            "crop": {"x": 0.25, "y": 0.25, "w": 0.5, "h": 0.5},
        },
    )

    assert edited.size == (30, 50)


def test_apply_recipe_adjusts_exposure_white_balance_contrast_and_saturation():
    img = Image.new("RGB", (1, 1), (100, 100, 100))
    edited = apply_recipe(
        img,
        {
            "adjustments": {
                "exposure": 1,
                "contrast": 10,
                "white_balance": {"temperature": 60, "tint": -30},
                "saturation": 20,
            },
        },
    )

    r, g, b = edited.getpixel((0, 0))
    assert r > b
    assert g > b
    assert max(r, g, b) > 100


def test_apply_recipe_straightens_in_place():
    img = Image.new("RGB", (100, 60), "white")
    edited = apply_recipe(img, {"straighten": 3.5})

    assert edited.size == (100, 60)


def test_apply_recipe_white_balance_preserves_rgba_alpha():
    img = Image.new("RGBA", (1, 1), (100, 100, 100, 123))
    edited = apply_recipe(img, {"adjustments": {"white_balance": {"temperature": 50}}})

    assert edited.mode == "RGBA"
    assert edited.getpixel((0, 0))[3] == 123


def test_apply_recipe_white_balance_preserves_la_alpha():
    img = Image.new("LA", (1, 1), (100, 123))
    edited = apply_recipe(img, {"adjustments": {"white_balance": {"temperature": 50}}})

    assert edited.mode == "RGBA"
    assert edited.getpixel((0, 0))[3] == 123


def test_apply_recipe_white_balance_preserves_palette_transparency():
    img = Image.new("P", (1, 1), 0)
    img.putpalette([100, 100, 100] + [0, 0, 0] * 255)
    img.info["transparency"] = bytes([123] * 256)

    edited = apply_recipe(img, {"adjustments": {"white_balance": {"temperature": 50}}})

    assert edited.mode == "RGBA"
    assert edited.getpixel((0, 0))[3] == 123
