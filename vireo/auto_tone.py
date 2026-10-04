"""Auto Tone: fit the Basic tone and colour sliders to one photo.

Every control is fitted by rendering candidate settings through the same
``tone.apply_adjustments`` (and ``presence.apply_presence``) the editor's
previews and exports use, then measuring the rendered result. Nothing here
predicts what a slider will do; a slider value is accepted only after the
real pipeline shows it lands the photo where the rule wants it. RAW sources
are analysed in scene-linear light, so exposure can see (and recover)
highlight headroom that a display-referred preview has already compressed.

Controls are fitted in a fixed order, each with the earlier ones applied:

  exposure -> highlights -> shadows -> contrast -> whites -> blacks
  -> vibrance -> saturation

When the photo has a subject (its active SAM mask, else its primary detection
box), metering weights the subject: exposure blends subject and frame
brightness, highlights also protect bright plumage on the subject, and
shadows lift a dark subject rather than a deliberately dark background.

White balance, texture, clarity, and dehaze are never set: an automatic
neutral would remove a wanted cast such as golden-hour warmth, and the
presence controls are taste. The current white balance and presence values
*are* rendered during the fit, so Auto Tone balances the photo as it will
actually look with them.
"""

from __future__ import annotations

import math

import numpy as np

try:
    from .float_image import FloatImage
    from .tone import (
        LUMA_B,
        LUMA_G,
        LUMA_R,
        apply_adjustments,
        linear_to_srgb,
        srgb_to_linear,
    )
except ImportError:
    from float_image import FloatImage
    from tone import (
        LUMA_B,
        LUMA_G,
        LUMA_R,
        apply_adjustments,
        linear_to_srgb,
        srgb_to_linear,
    )

# Long edge the edit source is decoded at (before crop) for analysis.
SOURCE_LONG_EDGE = 1024
# Long edge, in pixels, of the buffer the fit renders. Large enough for
# stable percentiles and for a small subject to cover a few hundred pixels.
ANALYSIS_LONG_EDGE = 384

# Display level of 18% grey (sRGB-encoded): the exposure target.
MID_GREY = 0.46
# Exposure moves this share of the way to mid-grey (in stops). Darkening is
# damped harder so snow, surf, and white-bird scenes stay bright.
EXPOSURE_BRIGHTEN_SHARE = 0.75
EXPOSURE_DARKEN_SHARE = 0.5
EXPOSURE_LIMIT = 2.5
# Subject share of the exposure when a subject is known, and how far (in
# stops) the subject may pull exposure from what the frame alone would get.
# The limit keeps a dark bird from blowing out the sand or sky around it.
SUBJECT_METERING_SHARE = 0.5
SUBJECT_PULL_LIMIT = 0.75
# Brightening stops before the frame's 99th percentile passes this level:
# auto exposure never pushes more than 1% of the frame into the shoulder.
HIGHLIGHT_GUARD = 0.97
# A frame with at least this share of near-black pixels is low-key (a night
# or flash shot, a dark backdrop): it is metered on its lit part, and its
# shadows are not lifted for the frame's sake.
LOW_KEY_LEVEL = 0.04
LOW_KEY_SHARE = 0.3
# A subject smaller than this share of the frame is too small to meter on
# reliably; larger than the upper share it is the frame.
SUBJECT_MIN_COVERAGE = 0.003
SUBJECT_MAX_COVERAGE = 0.9

# Display luma at which tones count as clipped / crushed.
CLIPPED = 0.995
CRUSHED = 0.005

HIGHLIGHTS_LIMIT = -70.0
# Highlights pulls back until no more than 3% of the recoverable frame (and
# 10% of the subject) sits above this level.
HIGHLIGHT_CEILING = 0.90

SHADOWS_LIMIT = 50.0
SHADOWS_LIMIT_SUBJECT = 70.0
# Lift until the 10th percentile of the frame reaches FRAME_SHADOW_FLOOR, or
# with a subject, its 25th percentile reaches SUBJECT_SHADOW_FLOOR (the frame
# floor relaxes so a dark background behind a lit subject stays dark).
FRAME_SHADOW_FLOOR = 0.10
FRAME_SHADOW_FLOOR_WITH_SUBJECT = 0.05
SUBJECT_SHADOW_FLOOR = 0.20
# How far contrast and blacks may then pull those floors back down.
SHADOW_FLOOR_SLACK = 0.02

CONTRAST_LIMIT = 30.0
# Interquartile range of display luma below which a photo reads as flat.
CONTRAST_TARGET_IQR = 0.24

WHITES_LIMIT = 40.0
WHITE_POINT = 0.94  # 99.5th percentile target
BLACKS_LIMIT = -40.0
BLACK_POINT = 0.04  # 0.5th percentile target
# Endpoint controls may add at most this share of newly clipped pixels.
NEW_CLIP_ALLOWANCE = 0.003
NEW_CRUSH_ALLOWANCE = 0.005
# Contrast may push this share of new pixels to white, and to black.
CONTRAST_CLIP_ALLOWANCE = 0.005
# A frame whose tones span less than this has no endpoints to anchor to.
MIN_TONAL_RANGE = 0.04

VIBRANCE_LIMIT = 25.0
SATURATION_LIMIT = 8.0
# Median chroma (max - min channel) of the coloured midtones to reach.
COLOUR_TARGET = 0.12
# Pixels below this chroma are neutral; a frame with fewer than
# MIN_COLOURED_SHARE coloured midtones is left alone so a near-monochrome
# scene (snow, fog) never has a faint cast amplified.
NEUTRAL_CHROMA = 0.05
MIN_COLOURED_SHARE = 0.15

_BISECT_STEPS = 7


def _luma(rgb):
    return LUMA_R * rgb[..., 0] + LUMA_G * rgb[..., 1] + LUMA_B * rgb[..., 2]


class _Render:
    """One candidate's rendered display buffer and its luma statistics."""

    def __init__(self, rgb, subject):
        self.rgb = rgb
        self.luma = _luma(rgb).ravel()
        self._frame_sorted = np.sort(self.luma)
        self._subject = None
        if subject is not None:
            order = np.argsort(self.luma)
            weights = np.cumsum(subject[order])
            self._subject = (self.luma[order], weights / weights[-1])

    def frame(self, q):
        values = self._frame_sorted
        index = min(len(values) - 1, max(0, int(round(q * (len(values) - 1)))))
        return float(values[index])

    def subject(self, q):
        values, cumulative = self._subject
        index = min(len(values) - 1, int(np.searchsorted(cumulative, q)))
        return float(values[index])

    def share_above(self, level, among=None):
        hits = self.luma >= level
        if among is not None:
            return float(np.count_nonzero(hits & among)) / max(1, np.count_nonzero(among))
        return float(np.count_nonzero(hits)) / len(self.luma)

    def share_below(self, level):
        return float(np.count_nonzero(self.luma <= level)) / len(self.luma)


def _round_away(amount, step):
    steps = amount / step
    return float((math.ceil(steps - 1e-9) if steps > 0 else math.floor(steps + 1e-9)) * step)


def _round_toward(amount, step):
    steps = amount / step
    return float((math.floor(steps + 1e-9) if steps > 0 else math.ceil(steps - 1e-9)) * step)


def _bisect(limit, ok, step=1.0):
    """Smallest-magnitude amount in [0, limit] for which ``ok`` holds.

    ``ok`` must be monotone (false near zero, true further out). Returns 0
    when zero already satisfies it and ``limit`` when nothing does. The
    result is rounded away from zero to a whole slider ``step``, so the
    returned value satisfies ``ok`` whenever any value in range does.
    """
    if ok(0.0):
        return 0.0
    if not ok(limit):
        return limit
    lo, hi = 0.0, 1.0
    for _ in range(_BISECT_STEPS):
        mid = (lo + hi) / 2.0
        if ok(mid * limit):
            hi = mid
        else:
            lo = mid
    return _round_away(hi * limit, step)


def _largest(limit, ok, step=1.0):
    """Largest-magnitude amount in [0, limit] for which ``ok`` still holds."""
    if ok(limit):
        return limit
    if not ok(0.0):
        return 0.0
    lo, hi = 0.0, 1.0
    for _ in range(_BISECT_STEPS):
        mid = (lo + hi) / 2.0
        if ok(mid * limit):
            lo = mid
        else:
            hi = mid
    return _round_toward(lo * limit, step)


class _Fitter:
    def __init__(
        self, rgb, *, input_linear, range_radius, subject, white_balance,
        presence, presence_scale,
    ):
        self.rgb = rgb
        self.input_linear = input_linear
        self.range_radius = range_radius
        self.subject = subject
        self.white_balance = white_balance or None
        self.presence = {k: v for k, v in (presence or {}).items() if v}
        self.presence_scale = presence_scale
        self._cache = {}

    def render(self, adjustments):
        key = tuple(sorted((k, round(float(v), 4)) for k, v in adjustments.items() if v))
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        rgb = apply_adjustments(
            self.rgb,
            white_balance=self.white_balance,
            input_linear=self.input_linear,
            range_radius=self.range_radius,
            **dict(key),
        )
        if self.presence:
            try:
                from .presence import apply_presence
            except ImportError:
                from presence import apply_presence
            rgb = np.asarray(apply_presence(
                FloatImage(rgb, encoding="srgb"),
                scale=self.presence_scale,
                **self.presence,
            ))
        result = _Render(rgb, self.subject)
        self._cache[key] = result
        return result


def _weighted_median(values, weights):
    order = np.argsort(values)
    cumulative = np.cumsum(weights[order])
    index = min(len(values) - 1, int(np.searchsorted(cumulative, cumulative[-1] / 2.0)))
    return float(values[order][index])


def _stops_to_grey(level):
    """Damped exposure (stops) that moves a display level toward mid-grey."""
    level = max(float(level), 1e-3)
    full = math.log2(float(srgb_to_linear(MID_GREY)) / float(srgb_to_linear(level)))
    share = EXPOSURE_BRIGHTEN_SHARE if full > 0 else EXPOSURE_DARKEN_SHARE
    return EXPOSURE_LIMIT * math.tanh(share * full / EXPOSURE_LIMIT)


def _subject_weights(subject, shape):
    """Flattened subject weights, or None when there is no usable subject."""
    if subject is None:
        return None
    weights = np.clip(np.asarray(subject, dtype=np.float32), 0.0, 1.0)
    if weights.shape != shape:
        return None
    coverage = float(weights.mean())
    if not SUBJECT_MIN_COVERAGE <= coverage <= SUBJECT_MAX_COVERAGE:
        return None
    return weights.ravel()


def fit(
    rgb, *, input_linear=False, range_radius=0, subject=None,
    white_balance=None, presence=None, presence_scale=1.0,
):
    """Fit Auto Tone to a pre-tone RGB buffer.

    Args:
        rgb: float array ``(H, W, 3)`` after geometry and crop; sRGB-encoded
            [0, 1], or scene-linear when ``input_linear`` (RAW).
        range_radius: guided-filter radius for Shadows/Highlights at this
            buffer's scale, as the real render would use.
        subject: optional ``(H, W)`` weight map in [0, 1] marking the subject.
        white_balance, presence: the photo's current white balance and
            texture/clarity/dehaze, rendered during the fit but not changed.
        presence_scale: output pixels per native pixel, for presence radii.

    Returns:
        ``{"adjustments": {...}, "metering": "subject"|"frame", "notes": [...]}``
        where ``adjustments`` holds the eight fitted controls (zeros included)
        and ``notes`` lists, in plain words, what the fit changed and why.
    """
    rgb = np.asarray(rgb, dtype=np.float32)
    weights = _subject_weights(subject, rgb.shape[:2])
    fitter = _Fitter(
        rgb,
        input_linear=input_linear,
        range_radius=range_radius,
        subject=weights,
        white_balance=white_balance,
        presence=presence,
        presence_scale=presence_scale,
    )
    has_subject = weights is not None
    subject_mask = weights > 0.5 if has_subject else None
    adj = {}
    notes = []

    # --- exposure: bring the (subject-weighted) median toward mid-grey ---
    base = fitter.render({})
    lit = base.luma > LOW_KEY_LEVEL
    low_key = 1.0 - float(np.count_nonzero(lit)) / len(base.luma) >= LOW_KEY_SHARE
    if low_key and np.any(lit):
        frame_ev = _stops_to_grey(np.median(base.luma[lit]))
    else:
        frame_ev = _stops_to_grey(base.frame(0.5))
    ev = frame_ev
    if has_subject:
        # Meter the subject on its brightest channel: a saturated blue or red
        # bird has low luma but is not underexposed.
        value = base.rgb.reshape(-1, 3).max(axis=1)
        subject_ev = _stops_to_grey(_weighted_median(value, weights))
        ev = SUBJECT_METERING_SHARE * subject_ev + (1.0 - SUBJECT_METERING_SHARE) * frame_ev
        ev = min(frame_ev + SUBJECT_PULL_LIMIT, max(frame_ev - SUBJECT_PULL_LIMIT, ev))
    ev = _round_toward(ev, 0.1)
    if ev > 0:
        ev = _largest(
            ev,
            lambda amount: fitter.render({"exposure": amount}).frame(0.99) <= HIGHLIGHT_GUARD,
            step=0.1,
        )
    adj["exposure"] = round(ev, 1) + 0.0
    if adj["exposure"] >= 0.1:
        notes.append(f"brightened {adj['exposure']:.1f} EV")
    elif adj["exposure"] <= -0.1:
        notes.append(f"darkened {abs(adj['exposure']):.1f} EV")

    exposed = fitter.render(adj)
    # Pixels already at white cannot be pulled back by Highlights (the curve
    # anchors white), so they never decide how far it goes.
    recoverable = exposed.luma < CLIPPED
    has_range = exposed.frame(0.995) - exposed.frame(0.005) > MIN_TONAL_RANGE

    # --- highlights: give bright areas their gradation back ---
    def highlights_ok(amount):
        r = fitter.render({**adj, "highlights": amount})
        if r.share_above(HIGHLIGHT_CEILING, among=recoverable) > 0.03:
            return False
        if has_subject:
            subject_recoverable = recoverable & subject_mask
            if np.any(subject_recoverable) and r.share_above(
                HIGHLIGHT_CEILING, among=subject_recoverable,
            ) > 0.10:
                return False
        return True

    adj["highlights"] = _bisect(HIGHLIGHTS_LIMIT, highlights_ok)
    if adj["highlights"]:
        notes.append("recovered highlights")

    # --- shadows: open up a dark subject or a crushed frame ---
    frame_floor = FRAME_SHADOW_FLOOR_WITH_SUBJECT if has_subject else FRAME_SHADOW_FLOOR
    if low_key:
        frame_floor = 0.0

    def shadows_ok(amount):
        r = fitter.render({**adj, "shadows": amount})
        if r.frame(0.10) < frame_floor:
            return False
        return not has_subject or r.subject(0.25) >= SUBJECT_SHADOW_FLOOR

    adj["shadows"] = _bisect(
        SHADOWS_LIMIT_SUBJECT if has_subject else SHADOWS_LIMIT, shadows_ok,
    )
    if adj["shadows"]:
        notes.append("lifted shadows")

    # Contrast and blacks deepen dark tones. They may not push the shadows
    # back under the floors the Shadows fit worked to reach.
    toned = fitter.render(adj)
    frame_floor_kept = min(frame_floor, toned.frame(0.10)) - SHADOW_FLOOR_SLACK
    subject_floor_kept = (
        min(SUBJECT_SHADOW_FLOOR, toned.subject(0.25)) - SHADOW_FLOOR_SLACK
        if has_subject else None
    )

    def shadows_kept(r):
        if r.frame(0.10) < frame_floor_kept:
            return False
        return not has_subject or r.subject(0.25) >= subject_floor_kept

    # --- contrast: add punch to a flat frame, never clip to get it ---
    clipped0 = toned.share_above(CLIPPED)
    crushed0 = toned.share_below(CRUSHED)

    def contrast_needed(amount):
        r = fitter.render({**adj, "contrast": amount})
        return r.frame(0.75) - r.frame(0.25) >= CONTRAST_TARGET_IQR

    def contrast_safe(amount):
        r = fitter.render({**adj, "contrast": amount})
        return (
            r.share_above(CLIPPED) - clipped0 <= CONTRAST_CLIP_ALLOWANCE
            and r.share_below(CRUSHED) - crushed0 <= CONTRAST_CLIP_ALLOWANCE
            and shadows_kept(r)
        )

    if has_range:
        adj["contrast"] = min(
            _bisect(CONTRAST_LIMIT, contrast_needed),
            _largest(CONTRAST_LIMIT, contrast_safe),
        )
    else:
        adj["contrast"] = 0.0
    if adj["contrast"]:
        notes.append("added contrast")

    # --- whites / blacks: anchor the endpoints without clipping ---
    endpoints = []
    adj["whites"] = 0.0
    adj["blacks"] = 0.0
    if has_range:
        toned = fitter.render(adj)
        clipped0 = toned.share_above(CLIPPED)

        def whites_reach(amount):
            return fitter.render({**adj, "whites": amount}).frame(0.995) >= WHITE_POINT

        def whites_safe(amount):
            r = fitter.render({**adj, "whites": amount})
            return r.share_above(CLIPPED) - clipped0 <= NEW_CLIP_ALLOWANCE

        adj["whites"] = min(
            _bisect(WHITES_LIMIT, whites_reach),
            _largest(WHITES_LIMIT, whites_safe),
        )
        if adj["whites"]:
            endpoints.append("white")

        toned = fitter.render(adj)
        crushed0 = toned.share_below(CRUSHED)

        def blacks_reach(amount):
            return fitter.render({**adj, "blacks": amount}).frame(0.005) <= BLACK_POINT

        def blacks_safe(amount):
            r = fitter.render({**adj, "blacks": amount})
            return (
                r.share_below(CRUSHED) - crushed0 <= NEW_CRUSH_ALLOWANCE
                and shadows_kept(r)
            )

        adj["blacks"] = max(
            _bisect(BLACKS_LIMIT, blacks_reach),
            _largest(BLACKS_LIMIT, blacks_safe),
        )
        if adj["blacks"]:
            endpoints.append("black")
    if endpoints:
        notes.append("set the " + " and ".join(endpoints) + " point" + ("s" if len(endpoints) > 1 else ""))

    # --- vibrance, then saturation: enliven muted colour only ---
    adj["vibrance"] = 0.0
    adj["saturation"] = 0.0
    toned = fitter.render(adj)
    midtones = (toned.luma > 0.08) & (toned.luma < 0.92)

    def colour(r):
        rgb = r.rgb.reshape(-1, 3)[midtones]
        chroma = rgb.max(axis=1) - rgb.min(axis=1)
        coloured = chroma[chroma > NEUTRAL_CHROMA]
        return coloured, len(chroma)

    coloured, mid_count = colour(toned)
    if mid_count and len(coloured) / mid_count >= MIN_COLOURED_SHARE:
        def vivid(extra):
            def ok(amount):
                values, _ = colour(fitter.render({**adj, **extra(amount)}))
                return len(values) and float(np.median(values)) >= COLOUR_TARGET
            return ok

        adj["vibrance"] = _bisect(VIBRANCE_LIMIT, vivid(lambda a: {"vibrance": a}))
        if adj["vibrance"] == VIBRANCE_LIMIT:
            adj["saturation"] = _bisect(
                SATURATION_LIMIT, vivid(lambda a: {"saturation": a}),
            )
        if adj["vibrance"] or adj["saturation"]:
            notes.append("enriched muted colour")

    adjustments = {k: (float(v) + 0.0) for k, v in adj.items()}
    return {
        "adjustments": adjustments,
        "metering": "subject" if has_subject else "frame",
        "notes": notes,
    }


def _subject_image(size, mask=None, box=None):
    """A source-space 'L' subject image from a SAM mask or a detection box.

    The mask is preferred. A box (normalized x/y/w/h) becomes its inscribed
    ellipse, which follows an animal's outline better than the corners do.
    Returns ``(image, source)`` or ``(None, None)``.
    """
    from PIL import Image, ImageDraw

    try:
        from .image_edits import _fit_mask_to_source
    except ImportError:
        from image_edits import _fit_mask_to_source

    if mask is not None:
        fitted = _fit_mask_to_source(mask, size)
        if fitted is not None:
            return fitted, "mask"
    if box:
        width, height = size
        x, y, w, h = (float(box.get(k) or 0.0) for k in ("x", "y", "w", "h"))
        if w > 0 and h > 0:
            image = Image.new("L", size, 0)
            ImageDraw.Draw(image).ellipse(
                (x * width, y * height, (x + w) * width, (y + h) * height),
                fill=255,
            )
            return image, "detection"
    return None, None


def _refine_box_subject(display, ellipse):
    """Narrow a detection ellipse to the pixels that differ from its backdrop.

    A box around a bird in flight is mostly sky. The backdrop colour is the
    median of a ring just outside the ellipse; pixels inside that sit well
    away from it are the animal. Falls back to the plain ellipse when the
    split finds almost nothing (a subject that matches its surroundings).
    """
    from scipy.ndimage import binary_dilation

    inside = ellipse > 0.5
    if not np.any(inside):
        return ellipse
    reach = max(2, int(round(0.15 * math.sqrt(np.count_nonzero(inside)))))
    ring = binary_dilation(inside, iterations=reach) & ~inside
    if np.count_nonzero(ring) < 8:
        return ellipse
    backdrop = np.median(display[ring], axis=0)
    distance = np.sqrt(np.sum((display - backdrop) ** 2, axis=-1))
    refined = ellipse * np.clip((distance - 0.04) / 0.08, 0.0, 1.0)
    if refined.sum() < 0.02 * ellipse.sum():
        return ellipse
    return refined


def fit_loaded_image(img, recipe, *, native_size=None, mask=None, box=None):
    """Fit Auto Tone to a decoded source and the recipe being edited.

    ``img`` is the decoded edit source (PIL image, or a FloatImage — RAW is
    scene-linear). The recipe's geometry and crop select the analysed frame;
    its white balance and presence controls are rendered during the fit;
    every other adjustment (including local edits) is ignored, so the result
    does not depend on the slider values Auto Tone replaces. ``mask`` (the
    active SAM mask, 'L') or ``box`` (primary detection, normalized) marks
    the subject in source space. The result of :func:`fit` gains
    ``subject_source``: "mask", "detection", or None.
    """
    from PIL import Image

    try:
        from .image_edits import (
            _PRESENCE_KEYS,
            _apply_geometry,
            detail_render_scale,
            normalize_recipe,
        )
        from .tone import RANGE_RADIUS
    except ImportError:
        from image_edits import (
            _PRESENCE_KEYS,
            _apply_geometry,
            detail_render_scale,
            normalize_recipe,
        )
        from tone import RANGE_RADIUS

    normalized = normalize_recipe(recipe) or {}
    adjustments = normalized.get("adjustments") or {}

    subject_img, subject_source = _subject_image(img.size, mask=mask, box=box)
    floating = isinstance(img, FloatImage)
    frame = _apply_geometry(img if floating else img.convert("RGB"), normalized)
    frame.thumbnail(
        (ANALYSIS_LONG_EDGE, ANALYSIS_LONG_EDGE),
        resample=Image.Resampling.LANCZOS,
    )
    pixels = np.asarray(frame)[..., :3].astype(np.float32)
    if not floating:
        pixels /= 255.0

    subject = None
    if subject_img is not None:
        subject_frame = _apply_geometry(subject_img, normalized).resize(
            frame.size, Image.Resampling.BILINEAR,
        )
        subject = np.asarray(subject_frame, dtype=np.float32) / 255.0
        if subject_source == "detection":
            display = pixels
            if floating and frame.encoding == "linear":
                display = np.clip(linear_to_srgb(np.clip(pixels, 0.0, 1.0)), 0.0, 1.0)
            subject = _refine_box_subject(display, subject)

    scale = detail_render_scale(frame.size, native_size, normalized)
    result = fit(
        pixels,
        input_linear=floating and frame.encoding == "linear",
        range_radius=max(1, int(round(RANGE_RADIUS * scale))),
        subject=subject,
        white_balance=adjustments.get("white_balance"),
        presence={k: adjustments.get(k) for k in _PRESENCE_KEYS},
        presence_scale=scale,
    )
    result["subject_source"] = subject_source if result["metering"] == "subject" else None
    return result
