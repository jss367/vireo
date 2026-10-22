import numpy as np
from PIL import Image, ImageFilter

from keeper_eval.features import dino_tensor, native_detail


def test_controlled_blur_changes_measurement_not_a_calibrated_label():
    values = np.random.default_rng(42).integers(0, 256, (256, 256, 3), dtype=np.uint8)
    image = Image.fromarray(values)
    clean = native_detail(image)
    blurred = native_detail(image.filter(ImageFilter.GaussianBlur(radius=4)))
    assert clean['median_gradient_energy'] > blurred['median_gradient_energy']
    assert clean['median_laplacian_variance'] > blurred['median_laplacian_variance']
    assert 'usable' not in clean and 'calibrated' not in clean
    assert clean['native_region_size'] == [256, 256]


def test_constant_or_tiny_native_region_does_not_manufacture_relative_focus():
    assert native_detail(Image.new('RGB', (2, 2)))['status'] == 'unavailable'
    constant = native_detail(Image.new('RGB', (40, 80), 'gray'))
    assert constant['median_contrast_normalized_gradient'] is None
    assert constant['median_gradient_energy'] == 0.


def test_embedding_input_recipe_is_deterministic_and_not_a_native_focus_measurement():
    tensor = dino_tensor(Image.new('RGB', (30, 70), 'white'))
    assert tensor.shape == (1, 3, 518, 518)
    assert tensor.dtype == np.float32
    assert np.isfinite(tensor).all()
