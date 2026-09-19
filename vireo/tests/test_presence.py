"""Spatial behavior and recipe integration for Texture, Clarity, and Dehaze."""

import numpy as np
import presence
import pytest
from detail import apply_detail
from image_edits import RecipeError, apply_recipe_to_loaded_image, normalize_recipe
from PIL import Image
from presence import apply_presence

KEYS = ('texture', 'clarity', 'dehaze')


def _pattern(period=16, width=192, height=120):
    x = np.arange(width, dtype=np.float32)
    row = 128 + 30 * np.sin(2 * np.pi * x / period)
    rgb = np.broadcast_to(row[None, :, None], (height, width, 3))
    return Image.fromarray(rgb.astype(np.uint8))


@pytest.mark.parametrize('key', KEYS)
def test_presence_recipe_normalization(key):
    assert normalize_recipe({'adjustments': {key: 0}}) is None
    for value in (-100, 35, 100):
        assert normalize_recipe({'adjustments': {key: value}})['adjustments'] == {key: float(value)}
    for value in (-101, 101, True, '30', float('nan'), float('inf')):
        with pytest.raises(RecipeError, match=key):
            normalize_recipe({'adjustments': {key: value}})


def test_zero_is_byte_exact():
    img = _pattern()
    assert apply_presence(img) is img


@pytest.mark.parametrize(('key', 'period'), [('texture', 12), ('clarity', 64)])
def test_signed_detail_controls_change_contrast_in_both_directions(key, period):
    img = _pattern(period=period)
    base = np.asarray(img).astype(float)[:, 24:-24].std()
    boosted = np.asarray(apply_presence(img, **{key: 80})).astype(float)[:, 24:-24].std()
    softened = np.asarray(apply_presence(img, **{key: -80})).astype(float)[:, 24:-24].std()
    assert boosted > base * 1.1
    assert softened < base * 0.9


def test_texture_targets_finer_detail_than_clarity():
    img = _pattern(period=64)
    base = np.asarray(img).astype(float)
    texture = np.asarray(apply_presence(img, texture=80)).astype(float)
    clarity = np.asarray(apply_presence(img, clarity=80)).astype(float)
    assert np.abs(clarity - base).mean() > np.abs(texture - base).mean() * 2


def test_dehaze_reduces_veil_and_negative_adds_it():
    scene = np.linspace(30, 230, 192, dtype=np.float32)
    hazy = (scene * 0.6 + 230 * 0.4).astype(np.uint8)
    img = Image.fromarray(np.broadcast_to(hazy[None, :, None], (80, 192, 3)))
    base = np.asarray(img).astype(float)
    clear = np.asarray(apply_presence(img, dehaze=70)).astype(float)
    mist = np.asarray(apply_presence(img, dehaze=-70)).astype(float)
    assert clear[:, :64].mean() < base[:, :64].mean()
    assert mist[:, :64].mean() > base[:, :64].mean()
    assert clear.std() > base.std() > mist.std()
    assert np.abs(clear - scene[None, :, None]).mean() < np.abs(base - scene[None, :, None]).mean()


@pytest.mark.parametrize('key', ['texture', 'clarity'])
def test_spatial_kernels_follow_render_scale(key):
    img = _pattern(period=32)
    base = np.asarray(img).astype(float)
    full = np.asarray(apply_presence(img, **{key: 80}, scale=1)).astype(float)
    reduced = np.asarray(apply_presence(img, **{key: 80}, scale=0.25)).astype(float)
    assert np.abs(full - base).mean() > np.abs(reduced - base).mean() * 1.5


@pytest.mark.parametrize('amount', [-80, 80])
def test_tiling_matches_whole_image_and_preserves_alpha(monkeypatch, amount):
    rng = np.random.default_rng(8)
    src = rng.integers(30, 230, (180, 47, 4), dtype=np.uint8)
    img = Image.fromarray(src)
    kwargs = dict(texture=amount, clarity=amount, dehaze=amount, scale=0.5)
    whole = np.asarray(apply_presence(img, **kwargs))
    monkeypatch.setattr(presence, '_TILE_PIXELS', 47 * 5)
    tiled = np.asarray(apply_presence(img, **kwargs))
    np.testing.assert_array_equal(whole, tiled)
    np.testing.assert_array_equal(tiled[..., 3], src[..., 3])


@pytest.mark.parametrize('key', KEYS)
@pytest.mark.parametrize('size', [(1, 1), (1, 9), (9, 1)])
def test_tiny_images_remain_supported(key, size):
    img = Image.new('RGB', size, (100, 130, 150))
    out = apply_presence(img, **{key: 100})
    assert out.size == size
    assert np.isfinite(np.asarray(out)).all()


@pytest.mark.parametrize('key', KEYS)
def test_recipe_runs_presence_at_output_resolution(key):
    img = _pattern()
    resized = img.resize((96, 60), Image.Resampling.LANCZOS)
    expected = apply_presence(resized, **{key: 65}, scale=0.5)
    actual = apply_recipe_to_loaded_image(
        img, {'adjustments': {key: 65}}, max_size=96, native_size=img.size,
    )
    np.testing.assert_array_equal(np.asarray(actual), np.asarray(expected))


def test_local_detail_preserves_global_presence_in_both_branches():
    img = _pattern()
    values = dict(texture=35, clarity=-20, dehaze=25)
    base = apply_presence(img, **values)
    sharpened = apply_detail(base, sharpen=60)
    mask = np.zeros((img.height, img.width), dtype=np.uint8)
    mask[:, :img.width // 2] = 255
    recipe = {
        'adjustments': values,
        'local': {
            'mask': {'ref': 'a1b2c3d4e5f6', 'source_digest': 'd'},
            'regions': [{'region': 'subject', 'adjustments': {'sharpen': 60}}],
        },
    }
    actual = np.asarray(apply_recipe_to_loaded_image(img, recipe, local_mask=Image.fromarray(mask)))
    midpoint = img.width // 2
    np.testing.assert_array_equal(actual[:, :midpoint], np.asarray(sharpened)[:, :midpoint])
    np.testing.assert_array_equal(actual[:, midpoint:], np.asarray(base)[:, midpoint:])
