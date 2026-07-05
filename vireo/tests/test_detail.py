"""Tests for the neighborhood detail pass (noise reduction + sharpening).

Unlike the per-pixel tone pipeline, detail ops depend on pixel neighborhoods,
so these tests exercise spatial behavior: edge acutance, noise statistics,
render-scale weakening, and tiling equivalence.
"""

import os
import sys

import numpy as np
import pytest
from PIL import Image, ImageFilter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import detail
from detail import apply_detail


def _luma(arr):
    rgb = arr[..., :3].astype(np.float32)
    return 0.2126 * rgb[..., 0] + 0.7152 * rgb[..., 1] + 0.0722 * rgb[..., 2]


def _soft_edge_image(width=64, height=32, blur=1.2):
    """A vertical dark->light step edge softened by a Gaussian blur.

    The default softness sits in the band a ~1px-radius unsharp mask acts on;
    pass a larger ``blur`` to test how reduced render scales weaken the
    (proportionally smaller) kernel.
    """
    arr = np.zeros((height, width, 3), dtype=np.uint8)
    arr[:, width // 2:, :] = 200
    arr[:, : width // 2, :] = 55
    img = Image.fromarray(arr, "RGB")
    return img.filter(ImageFilter.GaussianBlur(blur))


def _noisy_flat_image(width=96, height=96, level=120, sigma=14, seed=7):
    rng = np.random.default_rng(seed)
    base = np.full((height, width, 3), level, dtype=np.float32)
    noisy = base + rng.normal(0.0, sigma, size=base.shape)
    return Image.fromarray(
        np.clip(noisy, 0, 255).astype(np.uint8), "RGB"
    )


def _max_horizontal_luma_step(img):
    y = _luma(np.asarray(img))
    return float(np.max(np.abs(np.diff(y, axis=1))))


def test_apply_detail_zero_amounts_is_identity():
    rng = np.random.default_rng(3)
    src = rng.integers(0, 256, size=(20, 30, 3), dtype=np.uint8)
    img = Image.fromarray(src, "RGB")

    out = apply_detail(img, sharpen=0, noise_reduction=0)

    assert np.array_equal(np.asarray(out), src)


def test_sharpen_increases_edge_acutance():
    img = _soft_edge_image()

    sharpened = apply_detail(img, sharpen=60)

    assert _max_horizontal_luma_step(sharpened) > _max_horizontal_luma_step(img) * 1.15


def test_sharpen_amount_is_monotone():
    img = _soft_edge_image()

    mild = apply_detail(img, sharpen=25)
    strong = apply_detail(img, sharpen=90)

    assert _max_horizontal_luma_step(strong) > _max_horizontal_luma_step(mild)


def test_sharpen_effect_weakens_at_reduced_render_scale():
    """Radius is defined in native pixels; a downscaled render must apply a
    proportionally smaller kernel, extracting less contrast from the same
    (already softened) edge."""
    img = _soft_edge_image(blur=4.0)

    full = apply_detail(img, sharpen=60, sharpen_radius=2.0, scale=1.0)
    reduced = apply_detail(img, sharpen=60, sharpen_radius=2.0, scale=0.25)

    delta_full = np.mean(np.abs(_luma(np.asarray(full)) - _luma(np.asarray(img))))
    delta_reduced = np.mean(
        np.abs(_luma(np.asarray(reduced)) - _luma(np.asarray(img)))
    )
    assert delta_full > delta_reduced * 1.5


def test_noise_reduction_smooths_flat_regions():
    img = _noisy_flat_image()

    smoothed = apply_detail(img, noise_reduction=80)

    # Compare interior noise (avoid border effects).
    before = np.std(_luma(np.asarray(img))[8:-8, 8:-8])
    after = np.std(_luma(np.asarray(smoothed))[8:-8, 8:-8])
    assert after < before * 0.75


def test_noise_reduction_preserves_step_edges():
    arr = np.zeros((48, 48, 3), dtype=np.float32)
    arr[:, 24:, :] = 200.0
    arr[:, :24, :] = 40.0
    rng = np.random.default_rng(11)
    arr += rng.normal(0.0, 10.0, size=arr.shape)
    img = Image.fromarray(np.clip(arr, 0, 255).astype(np.uint8), "RGB")

    smoothed = apply_detail(img, noise_reduction=80)

    y = _luma(np.asarray(smoothed))
    left = float(np.mean(y[:, 18:22]))
    right = float(np.mean(y[:, 26:30]))
    # The 160-level step must survive strong NR nearly intact.
    assert right - left > 130


def test_apply_detail_preserves_alpha():
    rng = np.random.default_rng(5)
    src = rng.integers(0, 256, size=(24, 24, 4), dtype=np.uint8)
    img = Image.fromarray(src, "RGBA")

    out = apply_detail(img, sharpen=50, noise_reduction=50)

    assert out.mode == "RGBA"
    assert np.array_equal(np.asarray(out)[..., 3], src[..., 3])


@pytest.mark.parametrize("mode", ["RGB", "RGBA"])
def test_apply_detail_tiling_matches_single_pass(mode, monkeypatch):
    """Row-band tiling with halo overlap must be byte-identical to a
    whole-frame pass."""
    rng = np.random.default_rng(42)
    channels = 4 if mode == "RGBA" else 3
    src = rng.integers(0, 256, size=(90, 17, channels), dtype=np.uint8)
    img = Image.fromarray(src, mode)
    kwargs = {"sharpen": 55, "sharpen_radius": 1.8, "noise_reduction": 45}

    whole = np.asarray(apply_detail(img, **kwargs))

    monkeypatch.setattr(detail, "_DETAIL_TILE_PIXELS", 17 * 4)
    tiled = np.asarray(apply_detail(img, **kwargs))

    assert np.array_equal(whole, tiled)


def test_apply_recipe_to_loaded_image_runs_detail_pass():
    from image_edits import apply_recipe_to_loaded_image

    img = _soft_edge_image()
    recipe = {"adjustments": {"sharpen": 60}}

    plain = apply_recipe_to_loaded_image(img, None)
    edited = apply_recipe_to_loaded_image(img, recipe)

    assert _max_horizontal_luma_step(edited) > _max_horizontal_luma_step(plain) * 1.15


def test_apply_recipe_to_loaded_image_scales_detail_to_native_size():
    """A render at 1/4 of native resolution must apply a ~1/4-size kernel:
    weaker effect than the same recipe rendered as if the image were native."""
    from image_edits import apply_recipe_to_loaded_image

    img = _soft_edge_image(blur=4.0)
    recipe = {"adjustments": {"sharpen": 60, "sharpen_radius": 2.0}}

    at_native = apply_recipe_to_loaded_image(
        img, recipe, native_size=(img.size[0], img.size[1])
    )
    at_quarter = apply_recipe_to_loaded_image(
        img, recipe, native_size=(img.size[0] * 4, img.size[1] * 4)
    )

    base = _luma(np.asarray(img))
    delta_native = np.mean(np.abs(_luma(np.asarray(at_native)) - base))
    delta_quarter = np.mean(np.abs(_luma(np.asarray(at_quarter)) - base))
    assert delta_native > delta_quarter * 1.5


def test_detail_render_scale_accounts_for_crop_and_rotation():
    """Scale = output pixels per native pixel along the long edge, using the
    long edge the recipe would render at native resolution (rotation swaps
    axes, crop shrinks them; straighten keeps dimensions)."""
    from image_edits import detail_render_scale, normalize_recipe

    # Uncropped: 2000-wide render of an 8000-wide native photo.
    plain = normalize_recipe({"adjustments": {"sharpen": 50}})
    assert detail_render_scale((2000, 1500), (8000, 6000), plain) == pytest.approx(
        0.25
    )

    # Half-frame crop: the same 2000-wide output now covers 4000 native
    # pixels, so the scale doubles.
    cropped = normalize_recipe(
        {
            "crop": {"x": 0.25, "y": 0.25, "w": 0.5, "h": 0.5},
            "adjustments": {"sharpen": 50},
        }
    )
    assert detail_render_scale((2000, 1500), (8000, 6000), cropped) == pytest.approx(
        0.5
    )

    # Rotation swaps the axes a crop applies to: a 0.5-wide crop of a rotated
    # 8000x6000 photo spans 0.5 * 6000 horizontally and the full 8000
    # vertically.
    rotated = normalize_recipe(
        {
            "rotation": 90,
            "crop": {"x": 0.25, "y": 0.0, "w": 0.5, "h": 1.0},
            "adjustments": {"sharpen": 50},
        }
    )
    assert detail_render_scale((750, 2000), (8000, 6000), rotated) == pytest.approx(
        0.25
    )

    # Unknown native size falls back to apply-as-authored.
    assert detail_render_scale((2000, 1500), None, plain) == 1.0
    assert detail_render_scale((2000, 1500), (0, 0), plain) == 1.0


def test_apply_recipe_detail_only_recipe_does_not_run_tone_pipeline():
    """A detail-only recipe must leave non-detail pixels' tone untouched:
    sharpening a perfectly flat image is a no-op, byte-exact."""
    from image_edits import apply_recipe

    img = Image.new("RGB", (32, 32), (137, 141, 129))

    out = apply_recipe(img, {"adjustments": {"sharpen": 80, "noise_reduction": 0}})

    assert np.array_equal(np.asarray(out), np.asarray(img))
