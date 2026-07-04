import os
import sys

import numpy as np
import pytest
from PIL import Image

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import image_edits
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


def test_normalize_recipe_accepts_expanded_adjustments():
    assert normalize_recipe(
        {
            "adjustments": {
                "highlights": -20,
                "shadows": 35,
                "whites": 12,
                "blacks": -8,
                "vibrance": 25,
            }
        }
    ) == {
        "version": 1,
        "adjustments": {
            "highlights": -20.0,
            "shadows": 35.0,
            "whites": 12.0,
            "blacks": -8.0,
            "vibrance": 25.0,
        },
    }


def test_normalize_recipe_accepts_detail_adjustments():
    assert normalize_recipe(
        {
            "adjustments": {
                "sharpen": 40,
                "sharpen_radius": 1.5,
                "noise_reduction": 25,
            }
        }
    ) == {
        "version": 1,
        "adjustments": {
            "sharpen": 40.0,
            "sharpen_radius": 1.5,
            "noise_reduction": 25.0,
        },
    }


def test_normalize_recipe_drops_default_sharpen_radius():
    assert normalize_recipe(
        {"adjustments": {"sharpen": 40, "sharpen_radius": 1.0}}
    ) == {
        "version": 1,
        "adjustments": {"sharpen": 40.0},
    }


def test_normalize_recipe_drops_radius_without_sharpen():
    assert normalize_recipe({"adjustments": {"sharpen_radius": 2.0}}) is None
    assert normalize_recipe(
        {"adjustments": {"sharpen": 0, "sharpen_radius": 2.0}}
    ) is None


def test_normalize_recipe_drops_zero_detail_amounts():
    assert normalize_recipe(
        {"adjustments": {"sharpen": 0, "noise_reduction": 0}}
    ) is None


@pytest.mark.parametrize(
    ("adjustments", "message"),
    [
        ({"sharpen": -1}, "sharpen adjustment"),
        ({"sharpen": 101}, "sharpen adjustment"),
        ({"noise_reduction": 150}, "noise_reduction adjustment"),
        ({"sharpen": 40, "sharpen_radius": 0.4}, "sharpen_radius"),
        ({"sharpen": 40, "sharpen_radius": 3.5}, "sharpen_radius"),
        ({"sharpen": 40, "sharpen_radius": "wide"}, "sharpen_radius"),
        ({"sharpen": 40, "sharpen_radius": True}, "sharpen_radius"),
    ],
)
def test_normalize_recipe_rejects_invalid_detail_adjustments(adjustments, message):
    with pytest.raises(RecipeError, match=message):
        normalize_recipe({"adjustments": adjustments})


def test_normalize_recipe_rejects_invalid_white_balance():
    with pytest.raises(RecipeError, match="white_balance.temperature"):
        normalize_recipe({"adjustments": {"white_balance": {"temperature": 200}}})


def test_normalize_recipe_rejects_invalid_expanded_adjustment():
    with pytest.raises(RecipeError, match="highlights adjustment"):
        normalize_recipe({"adjustments": {"highlights": 200}})


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


def test_apply_recipe_adjusts_expanded_tone_controls():
    img = Image.fromarray(
        np.array(
            [
                [[35, 35, 35], [128, 110, 96], [230, 230, 230]],
            ],
            dtype=np.uint8,
        ),
        "RGB",
    )

    edited = apply_recipe(
        img,
        {
            "adjustments": {
                "shadows": 60,
                "highlights": -60,
                "whites": 30,
                "blacks": -30,
                "vibrance": 40,
            },
        },
    )

    dark, mid, bright = [edited.getpixel((x, 0)) for x in range(3)]
    assert sum(dark) > 35 * 3
    assert sum(bright) < 230 * 3
    assert max(mid) - min(mid) > 128 - 96


@pytest.mark.parametrize("mode", ["RGB", "RGBA"])
def test_apply_recipe_tiling_matches_single_pass(mode, monkeypatch):
    """A multi-tile image must be byte-identical to a whole-frame pass.

    Shrinks the tile budget so a small test image spans many row tiles, then
    compares against a single-pass reference computed directly through the tone
    pipeline. Proves tiling does not change results.
    """
    from tone import apply_adjustments

    width, height = 13, 97
    rng = np.random.default_rng(1234)
    channels = 4 if mode == "RGBA" else 3
    src = rng.integers(0, 256, size=(height, width, channels), dtype=np.uint8)
    img = Image.fromarray(src, mode)

    recipe = {
        "adjustments": {
            "exposure": 1.3,
            "highlights": -30,
            "shadows": 45,
            "whites": 15,
            "blacks": -10,
            "contrast": 15,
            "white_balance": {"temperature": 40, "tint": -20},
            "vibrance": 35,
            "saturation": 25,
        }
    }

    # Force many tiles: at width 13, one row per tile.
    monkeypatch.setattr(image_edits, "_ADJUST_TILE_PIXELS", width)
    tiled = np.asarray(apply_recipe(img, recipe))

    # Single whole-frame reference.
    arr = src.astype(np.float32) / 255.0
    ref_rgb = apply_adjustments(
        arr[..., :3],
        exposure=1.3,
        white_balance={"temperature": 40, "tint": -20},
        highlights=-30,
        shadows=45,
        whites=15,
        blacks=-10,
        contrast=15,
        vibrance=35,
        saturation=25,
    )
    ref8 = np.clip(ref_rgb * 255.0 + 0.5, 0, 255).astype(np.uint8)
    if channels == 4:
        ref8 = np.concatenate([ref8, src[..., 3:4]], axis=-1)

    assert tiled.shape == ref8.shape
    assert np.array_equal(tiled, ref8)


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


def test_edit_math_version_template_constant_matches_python():
    """The client-side `_VIREO_EDIT_MATH_VERSION` in `_navbar.html` is folded
    into the `er` query param on every rendered URL so a math bump busts the
    browser's cached thumbnail/preview bytes (server response is
    `Cache-Control: public, max-age=86400`, so server purges alone are not
    enough). The JS literal must stay in sync with `image_edits.EDIT_MATH_VERSION`
    or the cache key drifts and stale renders survive deploys.
    """
    import re

    navbar = os.path.join(
        os.path.dirname(__file__), '..', 'templates', '_navbar.html',
    )
    with open(navbar, encoding='utf-8') as f:
        src = f.read()
    match = re.search(
        r'var\s+_VIREO_EDIT_MATH_VERSION\s*=\s*([0-9]+)\s*;', src,
    )
    assert match, (
        'could not find `var _VIREO_EDIT_MATH_VERSION = ...;` in _navbar.html'
    )
    assert int(match.group(1)) == image_edits.EDIT_MATH_VERSION


def test_normalize_recipe_accepts_local_section():
    assert normalize_recipe(
        {
            "adjustments": {"exposure": 0.3},
            "local": {
                "mask": {
                    "ref": "a1b2c3d4e5f6",
                    "source_digest": "sha1:0011223344556677",
                    "feather": 12.5,
                },
                "regions": [
                    {
                        "region": "subject",
                        "adjustments": {"exposure": 0.6, "sharpen": 30},
                    },
                    {
                        "region": "background",
                        "adjustments": {"saturation": -15, "noise_reduction": 40},
                    },
                ],
            },
        }
    ) == {
        "version": 1,
        "adjustments": {"exposure": 0.3},
        "local": {
            "mask": {
                "ref": "a1b2c3d4e5f6",
                "source_digest": "sha1:0011223344556677",
                "feather": 12.5,
            },
            # Canonical order: sorted by region name.
            "regions": [
                {
                    "region": "background",
                    "adjustments": {"saturation": -15.0, "noise_reduction": 40.0},
                },
                {
                    "region": "subject",
                    "adjustments": {"exposure": 0.6, "sharpen": 30.0},
                },
            ],
        },
    }


def test_normalize_recipe_drops_empty_local_sections():
    # No regions at all, empty regions, and all-zero regions each drop the
    # entire local block (the shared mask never persists alone).
    mask = {"ref": "a1b2c3d4e5f6", "source_digest": "d"}
    assert normalize_recipe({"local": {}}) is None
    assert normalize_recipe({"local": {"mask": mask, "regions": []}}) is None
    assert normalize_recipe(
        {
            "local": {
                "mask": mask,
                "regions": [
                    {"region": "subject", "adjustments": {"exposure": 0}},
                ],
            }
        }
    ) is None


def test_normalize_recipe_drops_local_feather_at_zero():
    out = normalize_recipe(
        {
            "local": {
                "mask": {
                    "ref": "a1b2c3d4e5f6",
                    "source_digest": "d",
                    "feather": 0,
                },
                "regions": [
                    {"region": "subject", "adjustments": {"exposure": 1}},
                ],
            }
        }
    )
    assert "feather" not in out["local"]["mask"]


@pytest.mark.parametrize(
    ("local", "message"),
    [
        ([], "local must be an object"),
        ({"mask": {"ref": "a1b2c3d4e5f6", "source_digest": "d"},
          "regions": "subject"}, "regions must be an array"),
        ({"regions": [{"region": "subject", "adjustments": {"exposure": 1}}]},
         "local.mask"),
        ({"mask": {"source_digest": "d"},
          "regions": [{"region": "subject", "adjustments": {"exposure": 1}}]},
         "mask.ref"),
        ({"mask": {"ref": "NOT-HEX-12ch", "source_digest": "d"},
          "regions": [{"region": "subject", "adjustments": {"exposure": 1}}]},
         "mask.ref"),
        ({"mask": {"ref": "a1b2c3d4e5f6"},
          "regions": [{"region": "subject", "adjustments": {"exposure": 1}}]},
         "source_digest"),
        ({"mask": {"ref": "a1b2c3d4e5f6", "source_digest": "d", "feather": -1},
          "regions": [{"region": "subject", "adjustments": {"exposure": 1}}]},
         "feather"),
        ({"mask": {"ref": "a1b2c3d4e5f6", "source_digest": "d", "feather": 999},
          "regions": [{"region": "subject", "adjustments": {"exposure": 1}}]},
         "feather"),
        ({"mask": {"ref": "a1b2c3d4e5f6", "source_digest": "d"},
          "regions": [{"region": "sky", "adjustments": {"exposure": 1}}]},
         "region"),
        ({"mask": {"ref": "a1b2c3d4e5f6", "source_digest": "d"},
          "regions": [
              {"region": "subject", "adjustments": {"exposure": 1}},
              {"region": "subject", "adjustments": {"exposure": 2}},
          ]}, "duplicate"),
        ({"mask": {"ref": "a1b2c3d4e5f6", "source_digest": "d"},
          "regions": [{"region": "subject", "adjustments": {"exposure": 99}}]},
         "exposure"),
        ({"mask": {"ref": "a1b2c3d4e5f6", "source_digest": "d"},
          "regions": [{"region": "subject",
                       "adjustments": {"whites": 20}}]},
         "whites"),
    ],
)
def test_normalize_recipe_rejects_invalid_local(local, message):
    with pytest.raises(RecipeError, match=message):
        normalize_recipe({"local": local})


def test_normalize_recipe_accepts_negative_local_detail_deltas():
    # Region values are deltas layered on top of the globals; a background
    # sharpen -70 against global sharpen 70 legitimately zeroes sharpen in the
    # background. Rejecting negative deltas would force users to drop the
    # global sharpen entirely instead of just excluding the background from it.
    out = normalize_recipe(
        {
            "adjustments": {"sharpen": 70, "noise_reduction": 30},
            "local": {
                "mask": {"ref": "a1b2c3d4e5f6", "source_digest": "d"},
                "regions": [
                    {
                        "region": "background",
                        "adjustments": {"sharpen": -70, "noise_reduction": -30},
                    },
                ],
            },
        }
    )
    assert out["local"]["regions"] == [
        {
            "region": "background",
            "adjustments": {"sharpen": -70.0, "noise_reduction": -30.0},
        }
    ]
    # Out-of-range deltas still rejected: -100 is the floor.
    with pytest.raises(RecipeError, match="sharpen"):
        normalize_recipe(
            {
                "local": {
                    "mask": {"ref": "a1b2c3d4e5f6", "source_digest": "d"},
                    "regions": [
                        {
                            "region": "subject",
                            "adjustments": {"sharpen": -150},
                        },
                    ],
                },
            }
        )


def test_local_recipe_json_is_canonical_and_stable():
    recipe = {
        "local": {
            "mask": {"ref": "a1b2c3d4e5f6", "source_digest": "d"},
            "regions": [
                {"region": "subject", "adjustments": {"exposure": 1}},
            ],
        }
    }
    assert recipe_to_json(recipe) == recipe_to_json(normalize_recipe(recipe))
