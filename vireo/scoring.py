"""Subject-aware quality scoring for the culling pipeline (Stage 4).

Scores each photo based on subject sharpness, exposure, composition,
subject size, and noise — all computed relative to the bird mask, not
the whole frame.

All weights and thresholds are configurable with defaults from the design doc.
"""

import logging
import math
from collections.abc import Mapping
from typing import Any

from subject_readiness import _measured, explicit_decision, mark_analysis_readiness, subject_analysis_readiness

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
    # Legacy settings retained for config compatibility. Relative eye/subject
    # ranks cannot establish absolute defocus and no longer trigger rejection.
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


def detail_focus_score(subject_detail, subject_tenengrad, bg_tenengrad, encounter_details):
    """Subject focus ranked on the fixed-scale subject detail.

    The same shape as ``subject_focus_score``: 70% percentile rank within
    the encounter's detail cohort plus 30% subject/background gradient
    separation. The rank reads ``subject_detail`` (the subject resampled
    to DETAIL_SCALE_PX, ``quality.compute_subject_detail``) instead of the
    proxy-scale ``subject_tenengrad``, which at a fixed frame scale reads a
    small subject as sharper than a large one. On this catalog's bursts the
    detail rank put the photographer's pick first more often than the
    proxy-scale rank or the eye window (docs/comparable-quality.md). The
    separation term stays on the proxy-scale pair, since it compares the
    subject with its own background at one scale.
    """
    if subject_detail is None or subject_detail == 0:
        return 0.0
    rank = _percentile_rank(subject_detail, encounter_details)
    ratio = math.log(((subject_tenengrad or 0.0) + EPS) / ((bg_tenengrad or 0.0) + EPS))
    return 0.70 * rank + 0.30 * _sigmoid(ratio)


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
        float in [0, 1], or None when required analysis is unavailable
    """
    if not subject_analysis_readiness(photo)["eligible"]:
        return None
    encounter_photos = [p for p in encounter_photos if subject_analysis_readiness(p)["eligible"]]
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
    if not subject_analysis_readiness(photo)["eligible"] or q_score is None:
        return []
    cfg = {**DEFAULTS, **(config or {})}
    reasons = []

    # Rule 2: Subject severely clipped
    cc = photo.get("crop_complete")
    if cc is not None and cc < cfg["reject_crop_complete"]:
        reasons.append(f"crop_incomplete ({cc:.2f} < {cfg['reject_crop_complete']})")

    # Rule 4: Subject highlight clipping
    clip_h = photo.get("subject_clip_high")
    if clip_h is not None and clip_h > cfg["reject_clip_high"]:
        reasons.append(f"highlight_clipping ({clip_h:.2f} > {cfg['reject_clip_high']})")

    # Focus contributes to ordering, never to automatic rejection. Substitute
    # the neutral midpoint only for this counterfactual floor check; it is not
    # a measurement or an absolute sharpness estimate. Ignore stale rounded
    # composite/percentile values by recomputing from the available components.
    floor_score = _rejection_floor_score(photo, cfg)
    if floor_score is not None and floor_score < cfg["reject_composite"]:
        reasons.append(f"low_quality_without_focus ({floor_score:.3f} < {cfg['reject_composite']})")

    return reasons


def _rejection_floor_score(photo, cfg):
    """Evaluate the quality floor independently of group-relative focus."""
    components = (("exposure_score", "w_exposure"), ("composition_score", "w_composition"),
                  ("area_score", "w_area"), ("noise_score", "w_noise"))
    if any(photo.get(key) is None for key, _ in components):
        return None
    return cfg["w_focus"] * 0.5 + sum(photo[key] * cfg[weight] for key, weight in components)


def _normalize_scoring_config(config):
    """Merge nested pipeline settings up so callers can pass either shape.

    Two shapes reach scoring today:

    * A flat pipeline config, e.g. ``effective_cfg["pipeline"]`` — the
      shape ``pipeline_job``'s regroup stage and the test suite pass.
    * A full effective config with pipeline keys nested under
      ``config["pipeline"]`` — the shape ``build_best_batch_response`` and
      the browse-selection batch review path pass.

    Without this normalization, the second shape silently reads pipeline
    keys as absent and falls back to DEFAULTS/False. The most visible
    breakage after defaulting ``eye_detect_enabled`` to False is that
    scoring ignores ``eye_tenengrad`` on those paths even in workspaces
    where the user has enabled eye detection in Settings.
    """
    if not config:
        return {}
    nested = config.get("pipeline")
    if isinstance(nested, dict):
        return {**config, **nested}
    return config


def score_encounter(encounter, config=None):
    """Score all photos in an encounter and apply hard reject rules.

    Enriches each photo dict with:
        - quality_composite: float (Q_i), or None when analysis is unavailable
        - focus_score, exposure_score, composition_score, area_score, noise_score
        - reject_reasons: list (empty if not rejected)
        - analysis_status, analysis_reasons, selection_eligible
        - label: REVIEW for unavailable analysis; REJECT or None otherwise
          (explicit photographer decisions take precedence)

    Args:
        encounter: dict with 'photos' key (list of photo dicts)
        config: optional dict overriding DEFAULTS

    Returns:
        encounter dict (modified in place)
    """
    cfg = {**DEFAULTS, **_normalize_scoring_config(config)}
    photos = [p for p in encounter["photos"] if mark_analysis_readiness(p)]

    # Compute per-encounter normalization data
    enc_bg_tenegrads = [p.get("bg_tenengrad", 0) or 0 for p in photos]
    enc_bg_seps = [p.get("bg_separation", 0) or 0 for p in photos]
    enc_noise = [p.get("noise_estimate") for p in photos]
    eye_enabled = cfg.get("eye_detect_enabled", False)
    cohorts = {basis: [] for basis in FOCUS_BASES}
    for photo in photos:
        cohorts[focus_basis(photo, eye_enabled)].append(photo)
    encounter["focus_cohorts"] = {
        basis: {"scope": "encounter", "basis": basis, "size": len(peers),
                "photo_ids": [p["id"] for p in peers if p.get("id") is not None],
                "unidentified_count": sum(p.get("id") is None for p in peers)}
        for basis, peers in cohorts.items()
    }
    cohort_values = {basis: [_focus_measurement(p, basis) for p in peers] for basis, peers in cohorts.items()}

    for photo in photos:
        basis = focus_basis(photo, eye_enabled)
        _score_photo(photo, cfg, basis, cohort_values[basis], enc_bg_seps, enc_bg_tenegrads, enc_noise)
        _record_focus_evidence(photo, basis, cohort_values[basis])

    return encounter


# The measurement a photo's focus is ranked on within its encounter, in the
# order the scorer prefers them: the eye window when the user has turned
# eye detection on and an eye was located, else the fixed-scale subject
# detail when it has been measured, else the proxy-scale subject Tenengrad.
# Each basis ranks only among peers measured on the same basis.
FOCUS_BASES = ("eye", "detail", "subject")


def focus_basis(photo, eye_enabled):
    """Which of FOCUS_BASES the scorer ranks ``photo`` on."""
    if eye_enabled and _eye_measurement(photo) is not None:
        return "eye"
    if _detail_measurement(photo) is not None:
        return "detail"
    return "subject"


def scored_focus_basis(photo):
    """The basis a scored photo was ranked on, read from its focus evidence.

    A result saved before the evidence record existed carries only the
    eye score, so it is read the way those results were written.
    """
    evidence = photo.get("focus_evidence")
    if isinstance(evidence, dict) and evidence.get("basis") in FOCUS_BASES:
        return evidence["basis"]
    return "eye" if photo.get("eye_focus_score") is not None else "subject"


def _focus_measurement(photo, basis):
    """The raw value ranked for ``basis``: eye window, subject detail or subject Tenengrad."""
    if basis == "eye":
        return _eye_measurement(photo)
    if basis == "detail":
        return _detail_measurement(photo)
    return photo["subject_tenengrad"]


def _score_photo(photo, cfg, basis, cohort_values, enc_bg_seps, enc_bg_tenegrads, enc_noise):
    """Score one encounter photo against its peers and record its reject reasons.

    ``cohort_values`` are the ``basis`` measurements of every peer ranked on
    the same basis, this photo's included.
    """
    photo.pop("eye_focus_score", None)
    if basis == "eye":
        # Eye-based focus: pure percentile rank within the eye cohort.
        # The subject-vs-bg ratio term from subject_focus_score does not
        # translate to a small eye window, so the normalization is just
        # the peer comparison — sharper eye relative to peers = higher.
        f = _percentile_rank(_eye_measurement(photo), cohort_values)
        photo["eye_focus_score"] = round(f, 4)
    elif basis == "detail":
        f = detail_focus_score(
            photo["subject_detail"], photo.get("subject_tenengrad"), photo.get("bg_tenengrad"), cohort_values,
        )
    else:
        f = subject_focus_score(
            photo.get("subject_tenengrad"),
            photo.get("bg_tenengrad"),
            cohort_values,
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
    # The absolute companion to the encounter rank, for comparisons that
    # cross encounters; None until the subject detail is measured.
    photo["comparable_quality"] = comparable_quality(photo)

    photo["reject_reasons"] = hard_reject_reasons(photo, q, config=cfg)
    photo["label"] = explicit_decision(photo) or ("REJECT" if photo["reject_reasons"] else None)


def _eye_measurement(photo):
    """An invalid optional eye measurement falls back to subject scoring."""
    return _nonnegative_measurement(photo.get("eye_tenengrad"))


def _detail_measurement(photo):
    """The subject detail when measured; an unmeasured or invalid value falls back to subject scoring."""
    return _nonnegative_measurement(photo.get("subject_detail"))


def _nonnegative_measurement(value):
    """``value`` when it is a finite number of zero or more, else None."""
    return value if _measured(value) and value >= 0 else None


def _record_focus_evidence(photo, basis, values):
    """Explain the relative score without claiming calibrated absolute focus.

    Cohort membership is stored once on the encounter under focus_cohorts;
    cohort_key refers to that map. Peers have eligible current subject analysis
    and the same measurement basis, including this photo. Flags never exclude
    a measured peer. The scope is the encounter, not the visible review burst.
    On the detail basis the separation term reads the proxy-scale subject
    and background pair, recorded as ``separation_measurement`` and
    ``background_measurement`` so the inspector can show both scales.
    """
    raw = _focus_measurement(photo, basis)
    photo["focus_evidence"] = {
        "basis": basis, "measurement": raw,
        "measurement_name": MEASUREMENT_NAME if basis == "detail" else "Tenengrad",
        "scale_px": DETAIL_SCALE_PX if basis == "detail" else None,
        "separation_measurement": photo["subject_tenengrad"] if basis == "detail" else None,
        "background_measurement": photo["bg_tenengrad"] if basis != "eye" else None,
        "percentile": _percentile_rank(raw, values),
        "relative_score": photo["focus_score"],
        "method": {"eye": "eye_percentile", "detail": "detail_percentile_background_ratio",
                   "subject": "subject_percentile_background_ratio"}[basis],
        "cohort_key": basis, "cohort_size": len(values), "cohort_scope": "encounter",
        "absolute_status": "not_validated",
    }


# -- Cross-encounter quality --
#
# quality_composite ranks a photo against the other frames of its encounter:
# its focus, noise and background terms are percentiles, so two values from
# different encounters say nothing about which photo is better. Culling one
# species across outings needs the opposite: terms that mean the same thing
# for every photo in the catalog. comparable_quality combines absolute terms
# only: the fixed-scale subject detail (quality.compute_subject_detail,
# measured with the subject resampled to DETAIL_SCALE_PX, so distance and
# sensor resolution cancel out) through a fixed log-sigmoid, the exposure
# score, crop_complete and the area score. The relative terms are left out
# rather than faked. The detail mapping's constants were chosen on a catalog
# sample (docs/comparable-quality.md) so photos spread over the range; they
# order photos by detail and set how much a detail difference is worth
# against clipping, a clipped wing or a small subject, and are not a
# calibrated sharpness scale. Texture and lighting still move the detail
# value, so the number is most trustworthy within one species.

# The subject scale, in pixels of the subject box's longer side, that
# subject_detail is measured at. Defined here, beside the composite that
# interprets it, so quality (a member of the pipeline's import cycle) can
# import it without pulling scoring into that cycle.
DETAIL_SCALE_PX = 512

# Detail value mapped to 0.5, and the width of the sigmoid in decades of
# detail. On the catalog sample (docs/comparable-quality.md) picks had a
# median detail near 16,000 and rejects near 7,000; 8,000 is the midpoint
# of their spread and 0.35 decades puts 2,000 near 0.15 and 40,000 near 0.9.
DETAIL_ANCHOR = 8000.0
DETAIL_WIDTH_DECADES = 0.35

# The composite's weights are the encounter scorer's for the same terms,
# renormalized without its relative noise term; composition keeps only the
# crop part of its weight because background separation is relative too.
_TERM_WEIGHTS = {
    "detail": DEFAULTS["w_focus"],
    "exposure": DEFAULTS["w_exposure"],
    "crop": DEFAULTS["w_composition"] * DEFAULTS["w_crop_complete"],
    "area": DEFAULTS["w_area"],
}
WEIGHTS = {name: weight / sum(_TERM_WEIGHTS.values()) for name, weight in _TERM_WEIGHTS.items()}

MEASUREMENT_NAME = f"Tenengrad at {DETAIL_SCALE_PX} px subject scale"

REQUIRED_FIELDS = (
    "subject_detail", "subject_clip_high", "subject_clip_low", "subject_y_median",
    "crop_complete", "subject_size",
)


def detail_score(value: float) -> float:
    """Map a subject detail measurement onto [0, 1]; a zero measurement is 0."""
    if value <= 0:
        return 0.0
    return 1.0 / (1.0 + math.exp(-(math.log10(value) - math.log10(DETAIL_ANCHOR)) / DETAIL_WIDTH_DECADES))


def comparable_quality(photo: Mapping[str, Any]) -> dict[str, Any] | None:
    """The cross-encounter quality of one photo, or None until its inputs are measured.

    Takes the photo's feature fields as the pipeline loads them. The result
    carries every term so a reader can see what the score is made of:
    ``score`` in [0, 1], ``detail`` and ``detail_px`` (the measurement and
    the subject's size in source pixels), ``scale_px`` (the scale it was
    measured at), ``measurement_name``, ``basis`` (``subject``; a head or
    eye basis is a later tier), the four term scores, and ``weights``.
    """
    if any(not _measured(photo.get(field)) for field in REQUIRED_FIELDS):
        return None
    detail = float(photo["subject_detail"])
    terms = {
        "detail": detail_score(detail),
        "exposure": exposure_score(photo["subject_clip_high"], photo["subject_clip_low"], photo["subject_y_median"]),
        "crop": max(0.0, min(1.0, float(photo["crop_complete"]))),
        "area": area_score(photo["subject_size"]),
    }
    score = sum(WEIGHTS[name] * value for name, value in terms.items())
    detail_px = photo.get("subject_detail_px")
    return {
        "score": round(score, 4),
        "detail": detail,
        "detail_px": float(detail_px) if _measured(detail_px) else None,
        "scale_px": DETAIL_SCALE_PX,
        "measurement_name": MEASUREMENT_NAME,
        "basis": "subject",
        "detail_score": round(terms["detail"], 4),
        "exposure_score": round(terms["exposure"], 4),
        "crop_score": round(terms["crop"], 4),
        "area_score": round(terms["area"], 4),
        "weights": {name: round(weight, 4) for name, weight in WEIGHTS.items()},
    }
