"""Rebuild clipped sensor channels before a scene-linear RAW decode.

A sensel at its ceiling records only a lower bound. The linear edit decode
keeps channels unclipped so radiance above display white survives, and white
balance then scales each channel's ceiling differently: a blown area comes out
with red and blue above green, which renders pink. This module raises clipped
sensels in the mosaic, in white-balanced camera space, so a blown area is
neutral and a partly clipped one keeps the color its unclipped channels
measured.

The estimate for a clipped sensel is the mean of the other two channels around
it. A neighboring channel that is itself clipped is taken to reach the
brightest channel there, which makes a fully clipped area neutral at the
highest ceiling. A sensel is only ever raised, and the correction fades in
over the last tenth below the ceiling so there is no edge where it starts.
Raised values must fit the 16-bit mosaic, so a sensor whose white level is
already near 65535 has no room and keeps its clipped values.
Detail is not invented: where every channel clipped, the result is flat.
"""

from concurrent.futures import ThreadPoolExecutor

import cv2
import numpy as np

# The correction fades in between these fractions of a channel's ceiling.
_FADE_START = 0.90
_FADE_END = 0.98
# Rows per band. Bounds the float temporaries on a 45 MP mosaic to a few MB
# each; a multiple of the 2- and 6-row filter tiles so every band starts on
# the same CFA phase.
_BAND_ROWS = 240
# NumPy and OpenCV release the GIL, so bands run in parallel; kept small
# because several decodes can run at once.
_WORKERS = 4
_UNCLIPPED = np.float32(1e12)
_SENSEL_MAX = 65535


def _has_terminal_spike(views, peak, white, size):
    """Require a narrow spike above a populated tail, not just bright samples.

    A flat bright field is ambiguous without sensor metadata. Conservatively
    leave it alone unless it reaches the nominal white level. Below white,
    compare the last histogram bin with eight equally wide preceding bins;
    several populated bins must lead into the spike.
    """
    width = max(1, round(white / 4096))
    edges = np.arange(peak - 9 * width, peak + width, width)
    hist = sum((np.histogram(view, bins=edges)[0] for view in views))
    tail = hist[:-1]
    return (np.count_nonzero(tail) >= 4
            and hist[-1] >= max(16, size * 1e-5, 8 * tail.mean()))


def clip_levels(mosaic, pattern, n_colors, white_level):
    """Each sensor color's ceiling in raw units, or None where it never clips.

    ``pattern`` is the repeating color-filter tile at the mosaic's origin.
    Some sensors saturate a channel below the nominal white level (a D850's
    greens stop near 16240 of 16383). Such a channel piles sensels up at its
    own peak, which then stands in for the ceiling only when a narrow terminal
    histogram spike rises above a populated tail. Flat fields and smooth
    highlight peaks below white are insufficient evidence of saturation.
    """
    white = float(white_level)
    rows, cols = pattern.shape
    sites = [[] for _ in range(n_colors)]
    for row in range(rows):
        for col in range(cols):
            sites[int(pattern[row, col])].append(mosaic[row::rows, col::cols])
    levels = []
    for views in sites:
        size = sum(view.size for view in views)
        if not size:
            levels.append(None)
            continue
        peak = float(max(view.max() for view in views))
        if peak >= white:
            levels.append(white)
        elif 0.9 * white <= peak and _has_terminal_spike(views, peak, white, size):
            levels.append(peak)
        else:
            levels.append(None)
    return levels


def _band_estimate(value, soft, plane_map, ksize):
    """Per-sensel mean of the other two channels, trusting unclipped neighbors."""
    mean = np.empty((3,) + value.shape, np.float32)
    clipped = np.empty_like(mean)
    box = {"ksize": (ksize, ksize), "normalize": False, "borderType": cv2.BORDER_REFLECT}
    for plane in range(3):
        member = (plane_map == plane).astype(np.float32)
        count = np.maximum(cv2.boxFilter(member, -1, **box), np.float32(1e-6))
        mean[plane] = cv2.boxFilter(value * member, -1, **box) / count
        clipped[plane] = cv2.boxFilter(soft * member, -1, **box) / count
    ceiling = mean.max(axis=0)
    believed = mean + (ceiling[None] - mean) * clipped
    own = np.take_along_axis(believed, plane_map[None].astype(np.intp), axis=0)[0]
    return (believed.sum(axis=0) - own) * np.float32(0.5)


def _rebuild_band(band, colors, tables, ksize):
    """Rebuilt raw values for one band of the mosaic, or None if none clipped."""
    gain, black, fade_start, fade_span, plane_of = tables
    raw = band.astype(np.float32)
    soft = np.clip((raw - fade_start[colors]) / fade_span[colors], 0.0, 1.0)
    if not soft.any():
        return None
    gain_map = gain[colors]
    black_map = black[colors]
    value = (raw - black_map) * gain_map
    estimate = _band_estimate(value, soft, plane_of[colors], ksize)
    rebuilt = value + (np.maximum(value, estimate) - value) * soft
    out = np.rint(rebuilt / gain_map + black_map)
    return np.clip(out, band, _SENSEL_MAX).astype(band.dtype)


def rebuild_clipped(mosaic, colors, planes, gains, blacks, levels, *, bayer=True):
    """Raise clipped sensels of ``mosaic`` in place; return how many changed.

    ``colors`` gives each sensel's sensor color index; ``planes``, ``gains``,
    ``blacks`` and ``levels`` are indexed by it (the R/G/B plane a color
    belongs to, its white-balance gain, black level, and ceiling from
    :func:`clip_levels`).
    """
    if all(level is None for level in levels):
        return 0
    start = [_UNCLIPPED if lv is None else _FADE_START * lv for lv in levels]
    span = [1.0 if lv is None else (_FADE_END - _FADE_START) * lv for lv in levels]
    tables = (
        np.asarray(gains, np.float32), np.asarray(blacks, np.float32),
        np.asarray(start, np.float32), np.asarray(span, np.float32),
        np.asarray(planes, np.uint8),
    )
    # Every 3×3 Bayer window holds all three colors; other mosaics need 5×5.
    ksize = 3 if bayer else 5
    pad = ksize - 1  # even, so a padded band keeps the CFA phase
    height = mosaic.shape[0]

    def rebuild(top):
        bottom = min(height, top + _BAND_ROWS)
        lead = min(pad, top)
        rows = slice(top - lead, min(height, bottom + pad))
        rebuilt = _rebuild_band(mosaic[rows], colors[rows], tables, ksize)
        return None if rebuilt is None else rebuilt[lead:lead + bottom - top]

    # Every band reads the untouched mosaic (its neighbors' rows included), so
    # nothing is written until all bands are done.
    tops = range(0, height, _BAND_ROWS)
    with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
        cores = list(pool.map(rebuild, tops))
    changed = 0
    for top, core in zip(tops, cores, strict=True):
        if core is None:
            continue
        target = mosaic[top:top + core.shape[0]]
        changed += int(np.count_nonzero(core != target))
        target[:] = core
    return changed


def reconstruct_highlights(raw):
    """Rebuild the clipped sensels of an open rawpy image before postprocess.

    Returns the number of sensels raised. A mosaic this cannot describe (no
    color filter pattern, a fourth color, missing white balance) is left
    untouched, which keeps the decode's unclipped channels as they are.
    """
    import rawpy

    if raw.raw_type != rawpy.RawType.Flat or raw.raw_pattern is None:
        return 0
    mosaic = raw.raw_image_visible
    if mosaic.ndim != 2 or mosaic.dtype != np.uint16:
        return 0
    description = raw.color_desc.decode()
    planes = ["RGB".find(letter) for letter in description]
    if min(planes) < 0:
        return 0
    balance = list(raw.camera_whitebalance)
    if not all(value > 0 for value in balance[:3]):
        balance = list(raw.daylight_whitebalance)
        if not all(value > 0 for value in balance[:3]):
            return 0
    green = balance[1]
    gains = [
        (balance[index] if index < len(balance) and balance[index] > 0 else green) / green
        for index in range(len(description))
    ]
    colors = raw.raw_colors_visible
    tile = raw.raw_pattern.shape
    levels = clip_levels(
        mosaic, colors[:tile[0], :tile[1]], len(description), raw.white_level,
    )
    return rebuild_clipped(
        mosaic, colors, planes, gains, list(raw.black_level_per_channel), levels,
        bayer=raw.raw_pattern.shape == (2, 2),
    )
