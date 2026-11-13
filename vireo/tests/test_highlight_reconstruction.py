"""Clipped sensels are rebuilt toward neutral before the linear RAW decode."""

import highlight_reconstruction as hr
import numpy as np
import pytest
import tifffile
from float_image import FloatImage
from image_loader import RAW_DECODE_LINEAR, load_image

BLACK = 512
WHITE = 16383
GAINS = [2.0, 1.0, 1.5, 1.0]  # R, G, B, G2 white-balance gains
PLANES = [0, 1, 2, 1]
BLACKS = [BLACK] * 4
PATTERN = np.array([[0, 1], [3, 2]], dtype=np.uint8)


def colors_for(shape):
    return np.tile(PATTERN, (shape[0] // 2, shape[1] // 2))


def balanced(mosaic):
    """The mosaic in white-balanced camera space."""
    colors = colors_for(mosaic.shape)
    return (mosaic.astype(np.float64) - BLACK) * np.asarray(GAINS)[colors]


def rebuild(mosaic):
    levels = hr.clip_levels(mosaic, PATTERN, 4, WHITE)
    count = hr.rebuild_clipped(
        mosaic, colors_for(mosaic.shape), PLANES, GAINS, BLACKS, levels,
    )
    return count, levels


def test_blown_area_becomes_neutral_at_the_highest_ceiling():
    mosaic = np.full((64, 64), WHITE, dtype=np.uint16)
    count, _ = rebuild(mosaic)
    assert count > 0
    value = balanced(mosaic)
    # Red's ceiling is the highest after white balance; green and blue meet it.
    np.testing.assert_allclose(value, (WHITE - BLACK) * 2.0, rtol=2e-3)


def test_unclipped_mosaic_is_left_alone():
    rng = np.random.default_rng(0)
    mosaic = rng.integers(BLACK, 12000, (64, 64), dtype=np.uint16)
    before = mosaic.copy()
    count, levels = rebuild(mosaic)
    assert levels == [None] * 4
    assert count == 0
    np.testing.assert_array_equal(mosaic, before)


def test_clipped_green_takes_the_mean_of_measured_red_and_blue():
    mosaic = np.empty((64, 64), dtype=np.uint16)
    colors = colors_for(mosaic.shape)
    # Balanced red = 22000, blue = 18000; green is stuck at its ceiling.
    mosaic[colors == 0] = BLACK + 11000
    mosaic[colors == 2] = BLACK + 12000
    mosaic[(colors == 1) | (colors == 3)] = WHITE
    before = mosaic.copy()
    rebuild(mosaic)
    value = balanced(mosaic)
    np.testing.assert_allclose(value[(colors == 1) | (colors == 3)], 20000, rtol=2e-3)
    measured = (colors == 0) | (colors == 2)
    np.testing.assert_array_equal(mosaic[measured], before[measured])


def test_a_sensel_is_never_lowered():
    rng = np.random.default_rng(1)
    mosaic = rng.integers(BLACK, WHITE + 1, (96, 96), dtype=np.uint16)
    mosaic[20:60, 20:60] = WHITE
    before = mosaic.copy()
    rebuild(mosaic)
    assert (mosaic >= before).all()


def test_bands_match_a_single_pass(monkeypatch):
    rng = np.random.default_rng(2)
    source = rng.integers(9000, WHITE + 1, (120, 80), dtype=np.uint16)
    source[30:90, 10:70] = WHITE
    whole = source.copy()
    monkeypatch.setattr(hr, "_BAND_ROWS", 1024)
    rebuild(whole)
    banded = source.copy()
    monkeypatch.setattr(hr, "_BAND_ROWS", 6)
    rebuild(banded)
    np.testing.assert_array_equal(banded, whole)
    assert not np.array_equal(whole, source)


def test_a_channel_that_saturates_below_white_uses_its_own_ceiling():
    mosaic = np.full((64, 64), 9000, dtype=np.uint16)
    colors = colors_for(mosaic.shape)
    mosaic[(colors == 1) | (colors == 3)] = 16240
    levels = hr.clip_levels(mosaic, PATTERN, 4, WHITE)
    assert levels == [None, 16240.0, None, 16240.0]


def write_blown_dng(path):
    pixels = np.full((64, 128), 3000, dtype=np.uint16)
    pixels[:, 64:] = WHITE
    tags = [
        (50706, 'B', 4, (1, 4, 0, 0), False),  # DNGVersion
        (50707, 'B', 4, (1, 1, 0, 0), False),
        (50708, 's', 0, 'Vireo highlight test camera', False),
        (33421, 'H', 2, (2, 2), False),  # CFARepeatPatternDim
        (33422, 'B', 4, (0, 1, 1, 2), False),
        (50714, 'I', 1, BLACK, False),  # BlackLevel
        (50717, 'I', 1, WHITE, False),  # WhiteLevel
        (50721, '2i', 9, tuple(v for n in [1, 0, 0, 0, 1, 0, 0, 0, 1]
                              for v in (n, 1)), False),  # ColorMatrix1
        (50728, '2I', 3, (1, 2, 1, 1, 2, 3), False),  # AsShotNeutral
        (50778, 'H', 1, 21, False),  # CalibrationIlluminant1: D65
    ]
    tifffile.imwrite(path, pixels, photometric=32803, metadata=None, extratags=tags)


def test_linear_decode_of_a_blown_area_is_neutral(tmp_path):
    path = tmp_path / 'blown.dng'
    write_blown_dng(path)
    image = load_image(path, max_size=None, raw_decode=RAW_DECODE_LINEAR)
    assert isinstance(image, FloatImage)
    blown = np.asarray(image)[16:48, 88:120].reshape(-1, 3).mean(axis=0)
    # Unreconstructed, each channel stops at its own white-balanced ceiling
    # (green at half of red here), so the area decodes strongly colored.
    assert blown.max() / blown.min() == pytest.approx(1.0, abs=0.03)
    # LibRaw scales the highest ceiling to white; the rebuilt area sits there.
    assert blown.min() == pytest.approx(1.0, abs=0.02)


def test_linear_decode_leaves_unclipped_pixels_unchanged(tmp_path, monkeypatch):
    path = tmp_path / 'blown.dng'
    write_blown_dng(path)
    rebuilt = np.asarray(load_image(path, max_size=None, raw_decode=RAW_DECODE_LINEAR))
    monkeypatch.setattr(hr, "reconstruct_highlights", lambda raw: 0)
    plain = np.asarray(load_image(path, max_size=None, raw_decode=RAW_DECODE_LINEAR))
    np.testing.assert_array_equal(rebuilt[:, :48], plain[:, :48])
    assert not np.allclose(rebuilt[:, 80:], plain[:, 80:], rtol=0.05)
