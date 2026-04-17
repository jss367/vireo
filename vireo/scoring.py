"""Subject-aware quality scoring for the culling pipeline (Stage 4).

Scores each photo based on subject sharpness, exposure, composition,
subject size, and noise — all computed relative to the bird mask, not
the whole frame.

All weights and thresholds are configurable with defaults from the design doc.
"""

import logging
import math

log = logging.getLogger(__name__)

# Default weights and thresholds
DEFAULTS = {
    # Composite score weights (Section 4.4)
    "w_focus": 0.45,
    "w_exposure": 0.20,
    "w_composition": 0.15,
    "w_area": 0.10,
    "w_noise": 0.10,
    # Composition sub-weights (Section 4.3)
    "w_crop_complete": 0.55,
    "w_bg_separation": 0.45,
    # Hard reject thresholds (Section 4.5)
    "reject_crop_complete": 0.60,
    "reject_focus": 0.35,
    "reject_clip_high": 0.30,  # subject highlight clipping fraction
    "reject_composite": 0.40,
    # Eye-focus reject: fires when an eye is confidently localized but its
    # windowed sharpness ranks poorly. Catches "sharp body, soft eye" frames
    # that would otherwise pass subject-level focus checks.
    "reject_eye_focus": 0.35,
}

EPS = 1e-8


def _sigmoid(x):
    """Standard sigmoid function."""
    return 1.0 / (1.0 + math.exp(-x))


def _percentile_rank(value, values):
    """Compute percentile rank of a value within a list of values.

    Returns float in [0, 1]. If only one value, returns 0.5.
    """
    if len(values) <= 1:
        return 0.5
    below = sum(1 for v in values if v < value)
    equal = sum(1 for v in values if v == value)
    return (below + 0.5 * equal) / len(values)


# -- Individual score components --


def subject_focus_score(subject_tenengrad, bg_tenengrad, encounter_tenegrads):
    """Compute subject focus score F'_i (Section 4.1).

    F_i = 0.70 * percentile_rank(subject_tenengrad, within encounter)
        + 0.30 * sigmoid(log(subject_tenengrad / bg_tenengrad))

    AF metadata bonus is deferred (not yet extracted).

    Args:
        subject_tenengrad: float — subject sharpness
        bg_tenengrad: float — background ring sharpness
        encounter_tenegrads: list of floats — all subject_tenengrad values
                             in this encounter (for percentile ranking)

    Returns:
        float in [0, 1]
    """
    if subject_tenengrad is None or subject_tenengrad == 0:
        return 0.0

    # Percentile rank within encounter
    rank = _percentile_rank(subject_tenengrad, encounter_tenegrads)

    # Background ratio: sharp subject vs sharp background
    ratio = math.log((subject_tenengrad + EPS) / (bg_tenengrad + EPS))
    bg_term = _sigmoid(ratio)

    return 0.70 * rank + 0.30 * bg_term


def exposure_score(clip_high, clip_low, y_median):
    """Compute exposure score E_i (Section 4.2).

    E_i = exp(-6*clip_high - 3*clip_low) * exp(-|Y_med/255 - 0.45| / 0.30)

    Highlight clipping penalized ~2x harder than shadow clipping.

    Args:
        clip_high: fraction of subject pixels > 250
        clip_low: fraction of subject pixels < 5
        y_median: median luminance of subject (0-255 scale)

    Returns:
        float in [0, 1]
    """
    if clip_high is None:
        clip_high = 0.0
    if clip_low is None:
        clip_low = 0.0
    if y_median is None:
        return 0.5  # no data → neutral score

    clip_penalty = math.exp(-6.0 * clip_high - 3.0 * clip_low)
    # Normalize y_median to [0, 1] for the luminance term
    y_norm = y_median / 255.0
    lum_penalty = math.exp(-abs(y_norm - 0.45) / 0.30)

    return clip_penalty * lum_penalty


def composition_score(crop_complete, bg_separation, encounter_bg_seps, config=None):
    """Compute composition score C_i (Section 4.3).

    C_i = 0.55 * crop_complete + 0.45 * normalized_bg_separation

    bg_separation is normalized within the encounter (lower variance = better).

    Args:
        crop_complete: float in [0, 1]
        bg_separation: float — raw background variance
        encounter_bg_seps: list of floats — all bg_separation values in encounter
        config: optional dict overriding DEFAULTS

    Returns:
        float in [0, 1]
    """
    cfg = {**DEFAULTS, **(config or {})}

    cc = crop_complete if crop_complete is not None else 0.5

    # Normalize bg_separation within encounter: lower is better
    if bg_separation is not None and encounter_bg_seps:
        max_sep = max(encounter_bg_seps)
        if max_sep > 0:
            # Invert: low variance → high score
            norm_bg = 1.0 - (bg_separation / max_sep)
        else:
            norm_bg = 1.0
    else:
        norm_bg = 0.5  # no data → neutral

    return cfg["w_crop_complete"] * cc + cfg["w_bg_separation"] * norm_bg


def area_score(subject_size):
    """Subject area fraction score.

    Larger subject in frame is generally better, with diminishing returns.
    Uses sqrt to compress the range (a bird filling 25% of frame shouldn't
    score 5x better than one filling 5%).

    Args:
        subject_size: float — mask pixels / frame pixels

    Returns:
        float in [0, 1]
    """
    if subject_size is None or subject_size <= 0:
        return 0.0
    # sqrt compression, capped at 1.0
    return min(1.0, math.sqrt(subject_size) * 2.0)


def noise_score(bg_tenengrad, encounter_bg_tenegrads, noise_estimate=None, encounter_noise_estimates=None):
    """Noise score based on dedicated noise estimate or background sharpness proxy.

    If noise_estimate is available (Laplacian variance on background ring),
    uses inverted percentile rank of that. Otherwise falls back to
    inverted percentile rank of background Tenengrad.

    Lower noise → higher score.

    Args:
        bg_tenengrad: float — background ring sharpness (fallback)
        encounter_bg_tenegrads: list of floats for percentile ranking (fallback)
        noise_estimate: float — dedicated noise estimate (preferred)
        encounter_noise_estimates: list of floats for percentile ranking (preferred)

    Returns:
        float in [0, 1]
    """
    # Prefer dedicated noise estimate if available
    if noise_estimate is not None and encounter_noise_estimates:
        valid = [v for v in encounter_noise_estimates if v is not None]
        if valid:
            rank = _percentile_rank(noise_estimate, valid)
            return 1.0 - rank

    if bg_tenengrad is None or not encounter_bg_tenegrads:
        return 0.5
    # Lower bg_tenengrad (on smooth background) → less noise → higher score
    rank = _percentile_rank(bg_tenengrad, encounter_bg_tenegrads)
    return 1.0 - rank


# -- Composite quality score --


def composite_quality_score(photo, encounter_photos, config=None):
    """Compute the composite quality score Q_i (Section 4.4).

    Q_i = w_focus * F'_i + w_exposure * E_i + w_composition * C_i
        + w_area * area + w_noise * noise

    Args:
        photo: dict with keys: subject_tenengrad, bg_tenengrad, subject_clip_high,
               subject_clip_low, subject_y_median, crop_complete, bg_separation,
               subject_size
        encounter_photos: list of photo dicts (same encounter, for normalization)
        config: optional dict overriding DEFAULTS

    Returns:
        float in [0, 1]
    """
    cfg = {**DEFAULTS, **(config or {})}

    enc_tenegrads = [
        p.get("subject_tenengrad", 0) or 0 for p in encounter_photos
    ]
    enc_bg_tenegrads = [
        p.get("bg_tenengrad", 0) or 0 for p in encounter_photos
    ]
    enc_bg_seps = [
        p.get("bg_separation", 0) or 0 for p in encounter_photos
    ]

    f = subject_focus_score(
        photo.get("subject_tenengrad"),
        photo.get("bg_tenengrad"),
        enc_tenegrads,
    )
    e = exposure_score(
        photo.get("subject_clip_high"),
        photo.get("subject_clip_low"),
        photo.get("subject_y_median"),
    )
    c = composition_score(
        photo.get("crop_complete"),
        photo.get("bg_separation"),
        enc_bg_seps,
        config=cfg,
    )
    a = area_score(photo.get("subject_size"))
    enc_noise = [p.get("noise_estimate") for p in encounter_photos]
    n = noise_score(
        photo.get("bg_tenengrad"),
        enc_bg_tenegrads,
        noise_estimate=photo.get("noise_estimate"),
        encounter_noise_estimates=enc_noise,
    )

    q = (
        cfg["w_focus"] * f
        + cfg["w_exposure"] * e
        + cfg["w_composition"] * c
        + cfg["w_area"] * a
        + cfg["w_noise"] * n
    )
    return round(q, 4)


# -- Hard reject rules (Section 4.5) --


def hard_reject_reasons(photo, q_score, config=None):
    """Check hard reject rules. Returns list of reasons (empty = no reject).

    Args:
        photo: dict with quality features
        q_score: composite quality score Q_i
        config: optional dict overriding DEFAULTS

    Returns:
        list of reason strings (empty if no reject)
    """
    cfg = {**DEFAULTS, **(config or {})}
    reasons = []

    # Rule 1: No mask (no bird detected)
    if not photo.get("mask_path"):
        reasons.append("no_subject_mask")

    # Rule 2: Subject severely clipped
    cc = photo.get("crop_complete")
    if cc is not None and cc < cfg["reject_crop_complete"]:
        reasons.append(f"crop_incomplete ({cc:.2f} < {cfg['reject_crop_complete']})")

    # Rule 3: Subject badly out of focus
    # Use the focus score relative to encounter — if we don't have encounter
    # context here, skip this rule (it's checked at triage time with encounter)

    # Rule 4: Subject highlight clipping
    clip_h = photo.get("subject_clip_high")
    if clip_h is not None and clip_h > cfg["reject_clip_high"]:
        reasons.append(f"highlight_clipping ({clip_h:.2f} > {cfg['reject_clip_high']})")

    # Rule 5: Composite score floor
    if q_score < cfg["reject_composite"]:
        reasons.append(f"low_quality ({q_score:.3f} < {cfg['reject_composite']})")

    return reasons


def score_encounter(encounter, config=None):
    """Score all photos in an encounter and apply hard reject rules.

    Enriches each photo dict with:
        - quality_composite: float (Q_i)
        - focus_score, exposure_score, composition_score, area_score, noise_score
        - reject_reasons: list (empty if not rejected)
        - label: 'REJECT' or None (KEEP/REVIEW assigned later by MMR)

    Args:
        encounter: dict with 'photos' key (list of photo dicts)
        config: optional dict overriding DEFAULTS

    Returns:
        encounter dict (modified in place)
    """
    cfg = {**DEFAULTS, **(config or {})}
    photos = encounter["photos"]

    # Compute per-encounter normalization data
    enc_tenegrads = [p.get("subject_tenengrad", 0) or 0 for p in photos]
    enc_bg_tenegrads = [p.get("bg_tenengrad", 0) or 0 for p in photos]
    enc_bg_seps = [p.get("bg_separation", 0) or 0 for p in photos]
    enc_noise = [p.get("noise_estimate") for p in photos]
    # Eye cohort: photos with a populated eye_tenengrad form their own
    # ranking group so body-based photos don't skew the eye percentile
    # (Option A from the design doc). A body-only photo compares against
    # its peers' subject_tenengrad; an eye photo compares against its
    # peers' eye_tenengrad. Skipped entirely when eye detection is
    # disabled so stale eye_tenengrad values from prior runs don't
    # influence scoring after the user turns the feature off.
    eye_enabled = cfg.get("eye_detect_enabled", True)
    enc_eye_tenegrads = [
        p["eye_tenengrad"] for p in photos
        if p.get("eye_tenengrad") is not None
    ] if eye_enabled else []

    for photo in photos:
        eye_t = photo.get("eye_tenengrad") if eye_enabled else None
        if eye_t is not None and enc_eye_tenegrads:
            # Eye-based focus: pure percentile rank within the eye cohort.
            # The subject-vs-bg ratio term from subject_focus_score does not
            # translate to a small eye window, so the normalization is just
            # the peer comparison — sharper eye relative to peers = higher.
            f = _percentile_rank(eye_t, enc_eye_tenegrads)
            photo["eye_focus_score"] = round(f, 4)
        else:
            f = subject_focus_score(
                photo.get("subject_tenengrad"),
                photo.get("bg_tenengrad"),
                enc_tenegrads,
            )
        e = exposure_score(
            photo.get("subject_clip_high"),
            photo.get("subject_clip_low"),
            photo.get("subject_y_median"),
        )
        c = composition_score(
            photo.get("crop_complete"),
            photo.get("bg_separation"),
            enc_bg_seps,
            config=cfg,
        )
        a = area_score(photo.get("subject_size"))
        n = noise_score(
            photo.get("bg_tenengrad"),
            enc_bg_tenegrads,
            noise_estimate=photo.get("noise_estimate"),
            encounter_noise_estimates=enc_noise,
        )

        q = (
            cfg["w_focus"] * f
            + cfg["w_exposure"] * e
            + cfg["w_composition"] * c
            + cfg["w_area"] * a
            + cfg["w_noise"] * n
        )
        q = round(q, 4)

        photo["focus_score"] = round(f, 4)
        photo["exposure_score"] = round(e, 4)
        photo["composition_score"] = round(c, 4)
        photo["area_score"] = round(a, 4)
        photo["noise_score"] = round(n, 4)
        photo["quality_composite"] = q

        # Check focus reject using encounter percentile
        reasons = hard_reject_reasons(photo, q, config=cfg)
        if f < cfg.get("reject_focus", 0.35) and photo.get("mask_path"):
            reasons.append(f"out_of_focus (F={f:.3f} < {cfg['reject_focus']})")

        # Eye-soft reject: catches "sharp body, soft eye" frames. Only fires
        # when we have a confidently-localized eye — a null eye_tenengrad
        # means the pipeline stage's gates didn't all pass, and we fall back
        # to out_of_focus on subject_tenengrad (already handled above).
        # Also skipped when eye detection is disabled so stale eye_tenengrad
        # values from prior runs can't reject photos after the user toggles
        # the feature off.
        if (
            eye_enabled
            and photo.get("eye_tenengrad") is not None
            and f < cfg.get("reject_eye_focus", 0.35)
            and photo.get("mask_path")
        ):
            reasons.append(
                f"eye_soft (E={f:.3f} < {cfg['reject_eye_focus']})"
            )

        photo["reject_reasons"] = reasons
        photo["label"] = "REJECT" if reasons else None

    return encounter
