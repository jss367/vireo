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


def test_normalize_recipe_rejects_fractional_rotation():
    with pytest.raises(RecipeError, match="rotation"):
        normalize_recipe({"rotation": 90.9})


def test_normalize_recipe_rejects_non_boolean_flip():
    with pytest.raises(RecipeError, match="flip.horizontal"):
        normalize_recipe({"flip": {"horizontal": "false"}})


def test_recipe_json_is_canonical():
    assert recipe_to_json({"flip": {"vertical": True}, "rotation": 90}) == (
        '{"flip":{"vertical":true},"rotation":90,"version":1}'
    )


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
