"""The cross-encounter quality scalar: absolute terms only, None until measured."""
import math

import pytest
from scoring import (
    DETAIL_ANCHOR,
    DETAIL_SCALE_PX,
    MEASUREMENT_NAME,
    WEIGHTS,
    comparable_quality,
    detail_score,
)


def _photo(**overrides):
    photo = {
        "subject_detail": 16000.0, "subject_detail_px": 900.0,
        "subject_clip_high": 0.0, "subject_clip_low": 0.0, "subject_y_median": 115.0,
        "crop_complete": 1.0, "subject_size": 0.25,
        # Relative terms the scalar must ignore.
        "subject_tenengrad": 30000.0, "bg_tenengrad": 5000.0, "bg_separation": 400.0, "noise_estimate": 9.0,
    }
    photo.update(overrides)
    return photo


def test_weights_are_normalized_and_name_the_absolute_terms_only():
    assert set(WEIGHTS) == {"detail", "exposure", "crop", "area"}
    assert math.isclose(sum(WEIGHTS.values()), 1.0)


def test_detail_score_is_a_fixed_log_sigmoid():
    assert detail_score(DETAIL_ANCHOR) == pytest.approx(0.5)
    assert detail_score(0.0) == 0.0
    assert detail_score(-1.0) == 0.0
    assert detail_score(DETAIL_ANCHOR / 10) < 0.2 < 0.8 < detail_score(DETAIL_ANCHOR * 10)
    assert detail_score(2000.0) < detail_score(8000.0) < detail_score(16000.0) < detail_score(40000.0)


def test_unmeasured_detail_or_missing_inputs_give_none():
    assert comparable_quality(_photo(subject_detail=None)) is None
    assert comparable_quality(_photo(crop_complete=None)) is None
    assert comparable_quality(_photo(subject_detail=float("nan"))) is None
    assert comparable_quality({}) is None


def test_result_carries_every_term_and_the_measurement():
    result = comparable_quality(_photo())
    assert result["detail"] == 16000.0
    assert result["detail_px"] == 900.0
    assert result["scale_px"] == DETAIL_SCALE_PX
    assert result["measurement_name"] == MEASUREMENT_NAME
    assert result["basis"] == "subject"
    assert result["crop_score"] == 1.0
    assert 0.0 < result["exposure_score"] <= 1.0
    assert 0.0 < result["area_score"] <= 1.0
    assert result["weights"] == {name: round(weight, 4) for name, weight in WEIGHTS.items()}
    expected = sum(WEIGHTS[name] * result[key] for name, key in (
        ("detail", "detail_score"), ("exposure", "exposure_score"), ("crop", "crop_score"), ("area", "area_score"),
    ))
    assert result["score"] == pytest.approx(expected, abs=1e-3)
    assert 0.0 <= result["score"] <= 1.0


def test_relative_terms_never_move_the_score():
    base = comparable_quality(_photo())
    moved = comparable_quality(_photo(subject_tenengrad=1.0, bg_tenengrad=90000.0, bg_separation=1.0, noise_estimate=900.0))
    assert moved["score"] == base["score"]


def test_detail_clipping_and_framing_each_lower_the_score():
    base = comparable_quality(_photo())["score"]
    assert comparable_quality(_photo(subject_detail=4000.0))["score"] < base
    assert comparable_quality(_photo(subject_clip_high=0.4))["score"] < base
    assert comparable_quality(_photo(crop_complete=0.5))["score"] < base
    assert comparable_quality(_photo(subject_size=0.002))["score"] < base


def test_a_zero_detail_measurement_scores_zero_detail_but_is_still_scored():
    result = comparable_quality(_photo(subject_detail=0.0, subject_detail_px=None))
    assert result["detail_score"] == 0.0
    assert result["detail_px"] is None
    assert result["score"] == pytest.approx(
        WEIGHTS["exposure"] * result["exposure_score"] + WEIGHTS["crop"] + WEIGHTS["area"] * result["area_score"], abs=1e-3,
    )
