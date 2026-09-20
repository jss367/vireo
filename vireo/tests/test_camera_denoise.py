"""Camera identity, profile interpolation, and denoising quality regressions."""

import json
import sqlite3

import camera_denoise as denoise
import numpy as np
import pytest
from image_edits import RecipeError, apply_recipe_to_loaded_image, normalize_recipe
from PIL import Image


def _photo(iso=3200, model='NIKON Z 8'):
    return {'camera_make': 'NIKON CORPORATION', 'camera_model': model, 'iso': iso}


def _scene():
    clean = np.full((128, 192, 3), 65, dtype=np.uint8)
    clean[:, 96:] = (175, 185, 195)
    noisy = np.clip(clean.astype(float) + np.random.default_rng(7).normal(0, 12, clean.shape), 0, 255).astype(np.uint8)
    return clean, Image.fromarray(noisy)


@pytest.mark.parametrize('model, short_model', [('NIKON Z 8', 'Z8'), ('NIKON D850', 'D850')])
def test_real_profiles_and_exact_camera_identity(model, short_model):
    assert len(denoise._profiles()) >= 400
    assert denoise.resolve_profile(_photo(model=model))['match'] == 'exact'
    assert denoise.resolve_profile(_photo(model=short_model))['source'] == 'camera'
    assert denoise.resolve_profile(_photo(model='Z 8 II'))['source'] == 'image'
    assert denoise.resolve_profile({**_photo(), 'camera_make': 'Canon'})['source'] == 'image'


def test_nikon_base_iso_profiles():
    assert denoise.resolve_profile(_photo(64))['match'] == 'exact'
    d850 = denoise.resolve_profile(_photo(64, model='NIKON D850'))
    assert d850['source'] == 'camera'
    assert d850['match'] == 'interpolated'
    assert d850['profile_iso'] == [31, 100]


def test_iso_interpolation_and_endpoint_clamping():
    low, high = (denoise.resolve_profile(_photo(iso)) for iso in (1600, 2000))
    middle = denoise.resolve_profile(_photo(1800))
    assert middle['match'] == 'interpolated'
    np.testing.assert_allclose(middle['a'], (np.array(low['a']) + high['a']) / 2)
    np.testing.assert_allclose(middle['b'], (np.array(low['b']) + high['b']) / 2)
    end = denoise.resolve_profile(_photo(10**7))
    assert end['match'] == 'nearest'
    assert end['a'] == denoise.resolve_profile(_photo(end['profile_iso'][0]))['a']


@pytest.mark.parametrize('iso', [None, '', 'invalid', True, -100, 0, float('nan'), float('inf'), [], {}])
def test_bad_iso_falls_back(iso):
    assert denoise.resolve_profile(_photo(iso))['source'] == 'image'


def test_grouped_metadata_and_sqlite_rows():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    exif = json.dumps({'EXIF': {'Make': 'NIKON CORPORATION', 'Model': 'NIKON Z 8', 'ISO': 3200}})
    row = conn.execute('SELECT ? as exif_data', (exif,)).fetchone()
    assert denoise.resolve_profile(row) == denoise.resolve_profile(_photo())
    assert denoise.resolve_profile({'exif_data': '{broken'})['source'] == 'image'
    conn.close()


def test_missing_database_uses_image_estimates(monkeypatch, tmp_path):
    denoise._profiles.cache_clear()
    try:
        monkeypatch.setattr(denoise, '_DATA', tmp_path / 'missing.json')
        assert denoise.resolve_profile(_photo())['source'] == 'image'
    finally:
        denoise._profiles.cache_clear()


@pytest.mark.parametrize('profile', [None, denoise.resolve_profile(_photo())])
def test_reduces_error_and_preserves_edges(profile):
    clean, noisy = _scene()
    out = np.asarray(denoise.apply_camera_denoise(noisy, 85, profile=profile)).astype(float)
    before = np.mean((np.asarray(noisy).astype(float) - clean) ** 2)
    after = np.mean((out - clean) ** 2)
    assert after < before * 0.4
    # Contrast directly across the step should survive, including the edge pixels.
    assert out[:, 96].mean() - out[:, 95].mean() > 100


def test_amount_is_monotone_and_zero_is_exact():
    clean, noisy = _scene()
    assert denoise.apply_camera_denoise(noisy, 0) is noisy
    mild = np.asarray(denoise.apply_camera_denoise(noisy, 25)).astype(float)
    strong = np.asarray(denoise.apply_camera_denoise(noisy, 90)).astype(float)
    assert np.mean((strong - clean) ** 2) < np.mean((mild - clean) ** 2)


def test_clean_flat_image_unchanged_even_with_high_iso_profile():
    img = Image.new('RGB', (80, 80), (88, 136, 182))
    out = denoise.apply_camera_denoise(img, 100, profile=denoise.resolve_profile(_photo(25600)))
    np.testing.assert_array_equal(out, img)


def test_alpha_and_tiles_are_exact(monkeypatch):
    _, noisy = _scene()
    rgba = np.dstack((np.asarray(noisy), np.random.default_rng(2).integers(0, 256, noisy.size[::-1], dtype=np.uint8)))
    img = Image.fromarray(rgba)
    whole = np.asarray(denoise.apply_camera_denoise(img, 80))
    monkeypatch.setattr(denoise, '_TILE_PIXELS', img.width * 19)
    tiled = np.asarray(denoise.apply_camera_denoise(img, 80))
    np.testing.assert_array_equal(whole, tiled)
    np.testing.assert_array_equal(tiled[..., 3], rgba[..., 3])


@pytest.mark.parametrize('size', [(1, 1), (1, 20), (20, 1), (2, 2)])
def test_tiny_images(size):
    img = Image.new('RGB', size, (110, 90, 30))
    assert denoise.apply_camera_denoise(img, 100).size == size


def test_profile_prior_changes_strength_but_cannot_override_measured_noise():
    _, img = _scene()
    rgb = np.asarray(img)
    estimated = denoise._filter_strength(rgb, None, 1)
    low = denoise._filter_strength(rgb, denoise.resolve_profile(_photo(100)), 1)
    high = denoise._filter_strength(rgb, denoise.resolve_profile(_photo(25600)), 1)
    assert np.all(high > low)
    assert np.all(low >= estimated * 0.8 - 1e-5)
    assert np.all(high <= estimated * 1.2 + 1e-5)


def test_recipe_mode_validation_and_legacy_compatibility():
    assert normalize_recipe({'adjustments': {'denoise_mode': 'standard'}}) is None
    assert normalize_recipe({'adjustments': {'denoise_mode': 'camera'}})['adjustments'] == {'denoise_mode': 'camera'}
    for value in ('bogus', True, {}, None):
        with pytest.raises(RecipeError, match='denoise_mode'):
            normalize_recipe({'adjustments': {'denoise_mode': value}})
    _, img = _scene()
    legacy = {'adjustments': {'noise_reduction': 60}}
    standard = {'adjustments': {**legacy['adjustments'], 'denoise_mode': 'standard'}}
    np.testing.assert_array_equal(apply_recipe_to_loaded_image(img, legacy), apply_recipe_to_loaded_image(img, standard))


def test_camera_recipe_uses_destination_metadata_and_sharpens_after_denoise():
    from detail import apply_detail

    _, img = _scene()
    recipe = {'adjustments': {'noise_reduction': 80, 'sharpen': 25, 'denoise_mode': 'camera'}}
    actual = apply_recipe_to_loaded_image(img, recipe, camera_metadata=_photo())
    expected = apply_detail(denoise.apply_camera_denoise(img, 80, profile=denoise.resolve_profile(_photo())), sharpen=25)
    np.testing.assert_array_equal(actual, expected)
    high = apply_recipe_to_loaded_image(img, recipe, camera_metadata=_photo(25600))
    assert not np.array_equal(actual, high)


def test_local_denoise_delta_can_disable_camera_denoise_on_subject():
    _, img = _scene()
    mask = Image.new('L', img.size, 255)
    recipe = {
        'adjustments': {'noise_reduction': 80, 'denoise_mode': 'camera'},
        'local': {'mask': {'ref': 'abcdef012345', 'source_digest': 'test'}, 'regions': [
            {'region': 'subject', 'adjustments': {'noise_reduction': -80}},
        ]},
    }
    out = apply_recipe_to_loaded_image(img, recipe, local_mask=mask, camera_metadata=_photo())
    np.testing.assert_array_equal(out, img)
