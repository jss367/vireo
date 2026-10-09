"""Numerical and boundary contracts for the optional Rust experiment."""

import importlib.util
import json
import os
import subprocess
import sys
from importlib import import_module
from pathlib import Path

import numpy as np
import pytest
from PIL import Image

native = pytest.importorskip('vireo_detail_experiment')
DIRECTORY = Path(__file__).resolve().parent
sys.path.insert(0, str(DIRECTORY.parents[1] / 'vireo'))
detail = import_module('detail')

spec = importlib.util.spec_from_file_location('detail_benchmark', DIRECTORY / 'benchmark.py')
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


@pytest.fixture
def filters():
    return native.Filters(4)


@pytest.mark.parametrize('shape', [(1, 1), (1, 9), (7, 1), (2, 3), (19, 23)])
@pytest.mark.parametrize('radius', [0, 1, 3])
def test_bilateral_preserves_reference_edges_and_input(filters, shape, radius):
    image = np.random.default_rng(42).random(shape, dtype=np.float32)
    original = image.copy()
    expected = detail._bilateral(image, 1.6, .0705, radius)
    actual = filters.bilateral(image, 1.6, .0705, radius)
    np.testing.assert_allclose(actual, expected, rtol=0, atol=4e-7)
    np.testing.assert_array_equal(image, original)


@pytest.mark.parametrize('shape', [(1, 1), (1, 9, 3), (7, 1, 4), (2, 3), (19, 23, 3)])
@pytest.mark.parametrize('sigma', [.3, .5, 1.8, 3.0])
def test_gaussian_matches_reference_at_boundaries(filters, shape, sigma):
    image = np.random.default_rng(21).random(shape, dtype=np.float32)
    original = image.copy()
    actual = filters.gaussian(image, detail._gaussian_kernel(sigma))
    expected = detail._gaussian_blur(image, sigma)
    np.testing.assert_allclose(actual, expected, rtol=0, atol=4e-7)
    np.testing.assert_array_equal(image, original)


@pytest.mark.parametrize('name', ['scipy', 'opencv', 'rust', 'rust-opencv'])
@pytest.mark.parametrize('mode', ['RGB', 'RGBA'])
def test_whole_detail_pass_preserves_alpha_and_tiled_output(name, mode, monkeypatch):
    channels = 4 if mode == 'RGBA' else 3
    data = np.random.default_rng(7).integers(0, 256, (90, 17, channels), dtype=np.uint8)
    image = Image.fromarray(data)
    options = dict(sharpen=55, sharpen_radius=1.8, noise_reduction=45)
    expected = np.asarray(detail.apply_detail(image, **options))
    with benchmark.backend(name, 4):
        actual = np.asarray(detail.apply_detail(image, **options))
        monkeypatch.setattr(detail, '_DETAIL_TILE_PIXELS', 17 * 4)
        tiled = np.asarray(detail.apply_detail(image, **options))
    assert np.max(np.abs(actual.astype(np.int16) - expected.astype(np.int16))) <= 1
    np.testing.assert_array_equal(actual, tiled)
    if channels == 4:
        np.testing.assert_array_equal(actual[..., 3], data[..., 3])
    np.testing.assert_array_equal(np.asarray(image), data)


def test_noncontiguous_views_and_worker_counts(filters):
    data = np.random.default_rng(2).random((21, 17), dtype=np.float32)[::-1, ::2]
    expected = detail._bilateral(data, 1.6, .0705, 3)
    parallel = filters.bilateral(data, 1.6, .0705, 3)
    serial = native.Filters(1).bilateral(data, 1.6, .0705, 3)
    np.testing.assert_allclose(parallel, expected, rtol=0, atol=4e-7)
    np.testing.assert_array_equal(serial, parallel)


@pytest.mark.parametrize('name', ['scipy', 'opencv', 'rust', 'rust-opencv'])
@pytest.mark.parametrize('options', [dict(sharpen=100, sharpen_radius=3, noise_reduction=100),
                                   dict(sharpen=55, sharpen_radius=1.8, noise_reduction=45, scale=.17),
                                   dict(sharpen=100, noise_reduction=0),
                                   dict(sharpen=0, noise_reduction=100)])
def test_float_render_preserves_16bit_precision(name, options):
    from float_image import FloatImage

    pixels = np.random.default_rng(42).random((33, 47, 3), dtype=np.float32)
    image = FloatImage(pixels, encoding='srgb')
    expected = detail.apply_detail(image, **options)
    with benchmark.backend(name, 4):
        actual = detail.apply_detail(image, **options)
    assert benchmark.fidelity(actual.pixels, expected.pixels)['passes']
    np.testing.assert_array_equal(image.pixels, pixels)


def test_fidelity_rejects_precision_loss():
    image = np.full((2, 3, 3), .5, dtype=np.float32)
    assert not benchmark.fidelity(image, image + .001)['passes']


@pytest.mark.parametrize('threads', [0, 5])
def test_rejects_unbounded_worker_counts(threads):
    with pytest.raises(ValueError, match='four'):
        native.Filters(threads)


@pytest.mark.parametrize('data', [np.zeros((0, 3), dtype=np.float32),
                                np.zeros(3, dtype=np.float32),
                                np.full((2, 3), np.nan, dtype=np.float32)])
def test_rejects_invalid_arrays(filters, data):
    with pytest.raises(ValueError):
        filters.bilateral(data, 1.6, .0705, 3)


@pytest.mark.parametrize('sigma,radius', [(0, 3), (-1, 3), (float('nan'), 3), (1e-100, 3), (1.6, 9)])
def test_rejects_invalid_bilateral_parameters(filters, sigma, radius):
    with pytest.raises(ValueError):
        filters.bilateral(np.zeros((2, 3), dtype=np.float32), sigma, .07, radius)


@pytest.mark.parametrize('kernel', [[], [1, 2], [float('nan')], [1] * 131])
def test_rejects_invalid_gaussian_kernels(filters, kernel):
    with pytest.raises(ValueError):
        filters.gaussian(np.zeros((2, 3), dtype=np.float32), np.array(kernel, dtype=np.float32))


def test_experimental_filters_run_in_supervised_preview_worker(tmp_path):
    from vireo.tests.test_raw_precision import write_dng

    source = tmp_path / 'small.dng'
    write_dng(source)
    manifest = tmp_path / 'corpus.json'
    manifest.write_text(json.dumps([{'name': 'synthetic-camera', 'path': source.name}]))
    output = tmp_path / 'report.json'
    environment = dict(os.environ, OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1')
    run = subprocess.run([sys.executable, str(DIRECTORY / 'endpoint.py'), '--manifest', str(manifest),
                          '--output', str(output), '--views', 'native', '--backends', 'original', 'rust',
                          '--threads', '1', '--samples', '1'],
                         env=environment, capture_output=True, text=True, timeout=90)
    assert run.returncode == 0, run.stderr
    report = json.loads(output.read_text())
    assert len(report['results']) == 2
    assert all(row['output_dimensions'] == [512, 64] for row in report['results'])
    assert str(tmp_path) not in output.read_text()
