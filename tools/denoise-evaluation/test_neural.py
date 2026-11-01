"""Geometry and scale checks that catch silent corruption of a RAW comparison."""

import numpy as np
import pytest
from neural import Denoiser, finish_tile, pack, starts, weights


@pytest.mark.parametrize("dy,dx", [(0, 0), (0, 1), (1, 0), (1, 1)])
def test_bayer_phase_and_odd_sizes(dy, dx):
    base = np.array([["R", "G"], ["G", "B"]])
    cell = np.roll(base, (dy, dx), axis=(0, 1))
    image = np.zeros((35, 47), np.float32)
    levels = {"R": 0.1, "G": 0.2, "B": 0.3}
    for y in range(2):
        for x in range(2):
            image[y::2, x::2] = levels[cell[y, x]]
    packed, shift = pack(image, cell)
    assert shift == (dy, dx)
    for plane, value in zip(packed, [0.1, 0.2, 0.2, 0.3], strict=True):
        np.testing.assert_allclose(plane, value)


def test_overlap_has_no_holes_and_preserves_constants():
    n = 1301
    positions = starts(n)
    total = np.zeros(n)
    accumulated = np.zeros(n)
    for i, x in enumerate(positions):
        weight = weights(i > 0, i + 1 < len(positions))
        total[x : x + 512] += weight
        accumulated[x : x + 512] += weight * 0.37
    assert positions[0] == 0 and positions[-1] == n - 512
    assert np.all(total > 0)
    np.testing.assert_allclose(accumulated / total, 0.37)


@pytest.mark.parametrize("dy,dx", [(0, 0), (0, 1), (1, 0), (1, 1)])
def test_packing_keeps_every_original_sensor_coordinate(dy, dx):
    cell = np.roll(np.array([["R", "G"], ["G", "B"]]), (dy, dx), axis=(0, 1))
    mosaic = np.arange(35 * 47, dtype=np.float32).reshape(35, 47)
    packed, (oy, ox) = pack(mosaic, cell)
    recovered = np.empty((packed.shape[1] * 2, packed.shape[2] * 2), np.float32)
    for plane, (y, x) in zip(packed, [(0, 0), (0, 1), (1, 0), (1, 1)], strict=True):
        recovered[y::2, x::2] = plane
    np.testing.assert_array_equal(recovered[oy : oy + 35, ox : ox + 47], mosaic)


def test_gain_match_and_clipped_highlight_protection():
    inp = np.full((4, 512, 512), 0.05, np.float32)
    out = np.full((3, 1024, 1024), 0.05e6, np.float32)
    finish_tile(inp, out, False, False, False, False)
    np.testing.assert_allclose(out, 0.05, atol=1e-7)
    inp[:, 200, 200] = 1
    out.fill(0.05e6)
    finish_tile(inp, out, False, False, False, False)
    np.testing.assert_allclose(out[:, 400:402, 400:402], 1, atol=1e-6)
    assert np.isfinite(out).all()


def test_complete_tiler_preserves_geometry_without_a_model():
    class ConstantModel:
        def run(self, _, inputs):
            value = next(iter(inputs.values()))[0]
            rgb = np.stack([value[0], (value[1] + value[2]) / 2, value[3]])
            return [rgb.repeat(2, 1).repeat(2, 2)[None] * 1e6]

    runner = Denoiser.__new__(Denoiser)
    runner.session = ConstantModel()
    runner.input = "input"
    runner.load_seconds = 0
    mosaic = np.full((1103, 1301), 0.05, np.float32)
    out, _ = runner.run(mosaic, np.array([["G", "B"], ["R", "G"]]))
    assert out.shape == (1103, 1301, 3)
    np.testing.assert_allclose(out, 0.05, atol=1e-6)
