"""Experimental sensor-to-Vireo bridge; originals are only ever read.

LibRaw does not expose the complete resolved camera/WB matrix through rawpy.
Measure its linear transform with in-memory constant-color sensor patches,
then check independent patches and untouched scene pixels before using it.
This is an experiment adapter, not a replacement production RAW pipeline.
"""

from pathlib import Path

import numpy as np
import rawpy
from image_loader import _postprocess_raw_linear


def sensor(path: Path):
    with rawpy.imread(str(path)) as raw:
        pattern = raw.raw_pattern
        if pattern is None or pattern.shape != (2, 2):
            raise ValueError("This experiment requires a Bayer sensor")
        if raw.sizes.flip != 0:
            raise ValueError("This first experiment requires unrotated RAWs")
        colors = np.array([chr(v) for v in raw.color_desc])
        cell = colors[pattern]
        if sorted(cell.ravel()) != ["B", "G", "G", "R"]:
            raise ValueError("Unsupported Bayer color layout")
        black = np.asarray(raw.black_level_per_channel, dtype=np.float32)
        white = float(raw.white_level)
        mosaic = raw.raw_image_visible.astype(np.float32)
        for y in range(2):
            for x in range(2):
                c = pattern[y, x]
                mosaic[y::2, x::2] = np.maximum(mosaic[y::2, x::2] - black[c], 0) / (white - black[c])
        return mosaic, cell


def load_and_calibrate(path: Path):
    with rawpy.imread(str(path)) as raw:
        baseline = _postprocess_raw_linear(raw)
    # Separate decoder; no pixels in baseline or the source file are changed.
    levels = np.array(
        [
            [0.06, 0.06, 0.06],
            [0.12, 0.06, 0.06],
            [0.06, 0.12, 0.06],
            [0.06, 0.06, 0.12],
            [0.03, 0.07, 0.09],
            [0.15, 0.15, 0.15],
            [0.01, 0.01, 0.01],
            [0.2, 0.08, 0.04],
        ]
    )
    actual = levels.copy()
    with rawpy.imread(str(path)) as raw:
        if raw.sizes.flip != 0:
            raise ValueError("Calibration currently requires unrotated RAWs")
        pattern = raw.raw_pattern
        black = np.array(raw.black_level_per_channel)
        white = raw.white_level
        for i, level in enumerate(levels):
            for dy in range(2):
                for dx in range(2):
                    c = int(pattern[dy, dx])
                    ch = "RGB".index(chr(raw.color_desc[c]))
                    value = round(black[c] + level[ch] * (white - black[c]))
                    raw.raw_image_visible[dy:128:2, i * 128 + dx : (i + 1) * 128 : 2] = value
                    actual[i, ch] = (value - black[c]) / (white - black[c])
        probes = _postprocess_raw_linear(raw)
    if baseline.size != probes.size:
        raise ValueError("Calibration changed image dimensions")
    # A change to LibRaw's scene-dependent scaling invalidates the bridge.
    unchanged_error = float(np.max(np.abs(baseline.pixels[256::97, ::97] - probes.pixels[256::97, ::97])))
    measured = np.array(
        [np.median(probes.pixels[40:88, i * 128 + 40 : i * 128 + 88], axis=(0, 1)) for i in range(len(levels))]
    )
    matrix = np.linalg.lstsq(actual[:4], measured[:4], rcond=None)[0]
    validation_error = float(np.max(np.abs(actual[4:] @ matrix - measured[4:])))
    if unchanged_error > 1e-5 or validation_error > 0.001:
        raise ValueError(f"Color bridge failed: scene={unchanged_error}, patches={validation_error}")
    report = {
        "method": "in-memory sensor probes through Vireo's existing LibRaw decoder",
        "fit_patches": 4,
        "independent_validation_patches": 4,
        "max_untouched_scene_error_linear_rgb": unchanged_error,
        "max_validation_error_linear_rgb": validation_error,
        "validation_limit": 0.001,
        "matrix": matrix.tolist(),
        "limitations": "Linear color/WB validation, not proof of demosaicing equivalence or scene-detail fidelity",
    }
    probes.close()
    return baseline, matrix.astype(np.float32), report
