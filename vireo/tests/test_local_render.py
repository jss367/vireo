"""End-to-end render tests for local (mask-weighted) adjustments.

Synthetic masks through apply_recipe_to_loaded_image: weighted tone,
geometry-transformed weights, feathering, two-branch local detail, and the
all-or-nothing disable when the mask is unusable.
"""

import os
import sys

import numpy as np
from PIL import Image, ImageFilter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import image_edits
from image_edits import apply_recipe_to_loaded_image

MASK_REF = "a1b2c3d4e5f6"


def _left_half_mask(width=80, height=60):
    arr = np.zeros((height, width), dtype=np.uint8)
    arr[:, : width // 2] = 255
    return Image.fromarray(arr, "L")


def _gray(width=80, height=60, level=100):
    return Image.new("RGB", (width, height), (level, level, level))


def _local(regions, feather=None):
    mask = {"ref": MASK_REF, "source_digest": "d"}
    if feather is not None:
        mask["feather"] = feather
    return {"mask": mask, "regions": regions}


def _luma(img):
    arr = np.asarray(img).astype(np.float32)
    return 0.2126 * arr[..., 0] + 0.7152 * arr[..., 1] + 0.0722 * arr[..., 2]


def test_subject_exposure_brightens_only_masked_half():
    img = _gray()
    recipe = {
        "local": _local(
            [{"region": "subject", "adjustments": {"exposure": 1.5}}]
        )
    }

    out = apply_recipe_to_loaded_image(
        img, recipe, native_size=(80, 60), local_mask=_left_half_mask()
    )

    y = _luma(out)
    base = _luma(img)
    assert np.mean(y[:, :35]) > np.mean(base[:, :35]) + 30
    # Unmasked side stays put (small tolerance for float round-trip).
    assert np.max(np.abs(y[:, 45:] - base[:, 45:])) <= 1.0


def test_background_region_uses_inverse_weight():
    img = _gray(level=150)
    recipe = {
        "local": _local(
            [{"region": "background", "adjustments": {"exposure": -2.0}}]
        )
    }

    out = apply_recipe_to_loaded_image(
        img, recipe, native_size=(80, 60), local_mask=_left_half_mask()
    )

    y = _luma(out)
    assert np.mean(y[:, :35]) > 140          # subject half untouched
    assert np.mean(y[:, 45:]) < 90           # background half darkened


def test_weight_follows_recipe_geometry():
    # Flip horizontal: the brightened half must flip with the image.
    img = _gray()
    recipe = {
        "flip": {"horizontal": True},
        "local": _local(
            [{"region": "subject", "adjustments": {"exposure": 1.5}}]
        ),
    }

    out = apply_recipe_to_loaded_image(
        img, recipe, native_size=(80, 60), local_mask=_left_half_mask()
    )

    y = _luma(out)
    assert np.mean(y[:, 45:]) > np.mean(y[:, :35]) + 30


def test_weight_respects_crop():
    # Cropping to the masked half yields a fully brightened result.
    img = _gray()
    recipe = {
        "crop": {"x": 0.0, "y": 0.0, "w": 0.5, "h": 1.0},
        "local": _local(
            [{"region": "subject", "adjustments": {"exposure": 1.5}}]
        ),
    }

    out = apply_recipe_to_loaded_image(
        img, recipe, native_size=(80, 60), local_mask=_left_half_mask()
    )

    y = _luma(out)
    base = _luma(img)
    assert np.min(y) > np.mean(base) + 25


def test_missing_mask_disables_all_local_regions():
    img = _gray()
    recipe = {
        "adjustments": {"contrast": 20},
        "local": _local(
            [
                {"region": "subject", "adjustments": {"exposure": 2.0}},
                {"region": "background", "adjustments": {"exposure": -2.0}},
            ]
        ),
    }

    with_local_but_no_mask = apply_recipe_to_loaded_image(
        img, recipe, native_size=(80, 60), local_mask=None
    )
    global_only = apply_recipe_to_loaded_image(
        img, {"adjustments": {"contrast": 20}}, native_size=(80, 60)
    )

    assert np.array_equal(
        np.asarray(with_local_but_no_mask), np.asarray(global_only)
    )


def test_aspect_mismatched_mask_disables_local():
    img = _gray()
    recipe = {
        "local": _local(
            [{"region": "subject", "adjustments": {"exposure": 2.0}}]
        )
    }
    square_mask = _left_half_mask(width=60, height=60)  # 1:1 vs 4:3 photo

    out = apply_recipe_to_loaded_image(
        img, recipe, native_size=(80, 60), local_mask=square_mask
    )

    assert np.array_equal(np.asarray(out), np.asarray(img))


def test_feather_softens_the_transition():
    img = _gray()
    hard_recipe = {
        "local": _local(
            [{"region": "subject", "adjustments": {"exposure": 2.0}}]
        )
    }
    soft_recipe = {
        "local": _local(
            [{"region": "subject", "adjustments": {"exposure": 2.0}}],
            feather=8.0,
        )
    }

    hard = _luma(apply_recipe_to_loaded_image(
        img, hard_recipe, native_size=(80, 60), local_mask=_left_half_mask()
    ))
    soft = _luma(apply_recipe_to_loaded_image(
        img, soft_recipe, native_size=(80, 60), local_mask=_left_half_mask()
    ))

    # Max adjacent-column jump across the seam is smaller when feathered.
    hard_step = np.max(np.abs(np.diff(np.mean(hard, axis=0))))
    soft_step = np.max(np.abs(np.diff(np.mean(soft, axis=0))))
    assert soft_step < hard_step * 0.6


def test_local_tone_feather_uses_detail_scale_override():
    # The edit-preview endpoint strips crop from the recipe and passes
    # detail_scale computed from the crop-inclusive recipe. The local tone
    # feather must honour that override so the preview's mask falloff matches
    # what the saved (cropped) render will produce; otherwise the tone pass
    # uses the crop-stripped scale while the detail pass uses the saved-render
    # scale and preview/export disagree for cropped feathered local edits.
    img = _gray()
    recipe = {
        "local": _local(
            [{"region": "subject", "adjustments": {"exposure": 2.0}}],
            feather=8.0,
        )
    }

    def seam_step(detail_scale):
        out = apply_recipe_to_loaded_image(
            img, recipe, native_size=(80, 60), local_mask=_left_half_mask(),
            detail_scale=detail_scale,
        )
        y = _luma(out)
        return float(np.max(np.abs(np.diff(np.mean(y, axis=0)))))

    default = seam_step(None)          # scale=1.0 from the uncropped recipe
    small_scale = seam_step(0.25)      # simulates a 4x tighter saved render
    # Smaller scale → narrower feather → harder transition (larger step).
    assert small_scale > default * 1.5


def test_local_detail_two_branch_blend():
    # Soft edges in both halves; subject-only sharpen must raise acutance on
    # the masked half and leave the other half at the global (unsharpened)
    # rendering.
    arr = np.zeros((60, 80, 3), dtype=np.uint8)
    arr[:, 12:20, :] = 200   # edge inside subject half
    arr[:, 52:60, :] = 200   # edge inside background half
    img = Image.fromarray(arr, "RGB").filter(ImageFilter.GaussianBlur(1.2))

    recipe = {
        "local": _local(
            [{"region": "subject", "adjustments": {"sharpen": 90}}]
        )
    }

    out = apply_recipe_to_loaded_image(
        img, recipe, native_size=(80, 60), local_mask=_left_half_mask()
    )

    base = _luma(img)
    y = _luma(out)

    def max_step(y2d, lo, hi):
        return float(np.max(np.abs(np.diff(y2d[:, lo:hi], axis=1))))

    assert max_step(y, 6, 26) > max_step(base, 6, 26) * 1.15
    assert abs(max_step(y, 46, 66) - max_step(base, 46, 66)) < 2.0


def test_local_detail_composes_with_global_detail():
    # Global sharpen everywhere + extra subject NR: background half must keep
    # the global sharpening (not lose it because a local branch ran).
    arr = np.zeros((60, 80, 3), dtype=np.uint8)
    arr[:, 52:60, :] = 200
    img = Image.fromarray(arr, "RGB").filter(ImageFilter.GaussianBlur(1.2))

    recipe = {
        "adjustments": {"sharpen": 70},
        "local": _local(
            [{"region": "subject", "adjustments": {"noise_reduction": 60}}]
        ),
    }
    global_only = {"adjustments": {"sharpen": 70}}

    out = _luma(apply_recipe_to_loaded_image(
        img, recipe, native_size=(80, 60), local_mask=_left_half_mask()
    ))
    ref = _luma(apply_recipe_to_loaded_image(
        img, global_only, native_size=(80, 60)
    ))

    # Background half matches the global-sharpen render closely.
    assert np.max(np.abs(out[:, 46:] - ref[:, 46:])) <= 2.0


def test_weighted_tone_tiling_matches_single_pass(monkeypatch):
    rng = np.random.default_rng(21)
    src = rng.integers(0, 256, size=(90, 17, 3), dtype=np.uint8)
    img = Image.fromarray(src, "RGB")
    mask = _left_half_mask(width=17, height=90)
    recipe = {
        "adjustments": {"exposure": 0.4},
        "local": _local(
            [
                {"region": "subject", "adjustments": {"exposure": 1.0}},
                {"region": "background", "adjustments": {"saturation": -40}},
            ],
            feather=3.0,
        ),
    }

    whole = np.asarray(apply_recipe_to_loaded_image(
        img, recipe, native_size=(17, 90), local_mask=mask
    ))
    monkeypatch.setattr(image_edits, "_ADJUST_TILE_PIXELS", 17)
    tiled = np.asarray(apply_recipe_to_loaded_image(
        img, recipe, native_size=(17, 90), local_mask=mask
    ))

    assert np.array_equal(whole, tiled)
