# vireo/tests/test_scoring.py
"""Tests for subject-aware quality scoring (Stage 4)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# -- subject_focus_score --


def test_focus_score_high_sharpness():
    """The sharpest photo in an encounter should score high."""
    from scoring import subject_focus_score

    enc_tenegrads = [100, 200, 300, 400, 500]
    score = subject_focus_score(500, 50, enc_tenegrads)
    assert score > 0.7


def test_focus_score_low_sharpness():
    """The least sharp photo should score low."""
    from scoring import subject_focus_score

    enc_tenegrads = [100, 200, 300, 400, 500]
    score = subject_focus_score(100, 400, enc_tenegrads)
    assert score < 0.4


def test_focus_score_sharp_subject_blurry_bg():
    """Sharp subject + blurry background should boost the score."""
    from scoring import subject_focus_score

    enc = [300, 300, 300]
    # Sharp subject (300) vs very blurry background (10)
    score_good = subject_focus_score(300, 10, enc)
    # Sharp subject (300) vs sharp background (300) — misfocus
    score_bad = subject_focus_score(300, 300, enc)
    assert score_good > score_bad


def test_focus_score_none():
    from scoring import subject_focus_score

    assert subject_focus_score(None, 50, [100, 200]) == 0.0
    assert subject_focus_score(0, 50, [100, 200]) == 0.0


# -- exposure_score --


def test_exposure_midtone():
    """Well-exposed midtone subject should score high."""
    from scoring import exposure_score

    # ~115/255 ≈ 0.45 → minimal luminance penalty
    score = exposure_score(0.0, 0.0, 115)
    assert score > 0.9


def test_exposure_blown_highlights():
    """Heavy highlight clipping should score low."""
    from scoring import exposure_score

    score = exposure_score(0.5, 0.0, 200)
    assert score < 0.3


def test_exposure_deep_shadows():
    """Heavy shadow clipping penalized less than highlights."""
    from scoring import exposure_score

    score_shadow = exposure_score(0.0, 0.5, 10)
    score_highlight = exposure_score(0.5, 0.0, 240)
    # Highlights penalized harder (-6x vs -3x)
    assert score_shadow > score_highlight


def test_exposure_none_median():
    from scoring import exposure_score

    assert exposure_score(0.0, 0.0, None) == 0.5


# -- composition_score --


def test_composition_full_interior_smooth_bg():
    """Full interior bird + smooth background = high composition."""
    from scoring import composition_score

    score = composition_score(1.0, 0.0, [0.0, 100.0, 200.0])
    assert score > 0.9


def test_composition_clipped_busy_bg():
    """Clipped bird + busy background = low composition."""
    from scoring import composition_score

    score = composition_score(0.3, 200.0, [0.0, 100.0, 200.0])
    assert score < 0.3


# -- area_score --


def test_area_score_large_subject():
    from scoring import area_score

    score = area_score(0.25)  # 25% of frame
    assert score > 0.8


def test_area_score_small_subject():
    from scoring import area_score

    score = area_score(0.01)  # 1% of frame
    assert score < 0.3


def test_area_score_none():
    from scoring import area_score

    assert area_score(None) == 0.0
    assert area_score(0) == 0.0


# -- composite_quality_score --


def test_composite_score_good_photo():
    """A well-exposed, sharp, well-composed photo should score high."""
    from scoring import composite_quality_score

    photo = {
        "subject_tenengrad": 500,
        "bg_tenengrad": 50,
        "subject_clip_high": 0.0,
        "subject_clip_low": 0.0,
        "subject_y_median": 115,
        "crop_complete": 1.0,
        "bg_separation": 10.0,
        "subject_size": 0.15,
    }
    encounter = [photo]
    q = composite_quality_score(photo, encounter)
    assert q > 0.5


def test_composite_score_bad_photo():
    """Blurry, clipped, overexposed photo should score low."""
    from scoring import composite_quality_score

    photo = {
        "subject_tenengrad": 10,
        "bg_tenengrad": 500,
        "subject_clip_high": 0.4,
        "subject_clip_low": 0.0,
        "subject_y_median": 250,
        "crop_complete": 0.3,
        "bg_separation": 500.0,
        "subject_size": 0.01,
    }
    encounter = [photo]
    q = composite_quality_score(photo, encounter)
    assert q < 0.4


def test_composite_score_range():
    """Score should always be in [0, 1]."""
    from scoring import composite_quality_score

    for _ in range(20):
        import random

        photo = {
            "subject_tenengrad": random.uniform(0, 1000),
            "bg_tenengrad": random.uniform(0, 1000),
            "subject_clip_high": random.uniform(0, 1),
            "subject_clip_low": random.uniform(0, 1),
            "subject_y_median": random.uniform(0, 255),
            "crop_complete": random.uniform(0, 1),
            "bg_separation": random.uniform(0, 500),
            "subject_size": random.uniform(0, 0.5),
        }
        q = composite_quality_score(photo, [photo])
        assert 0 <= q <= 1, f"Score {q} out of range"


# -- hard_reject_reasons --


def test_reject_no_mask():
    from scoring import hard_reject_reasons

    reasons = hard_reject_reasons({}, 0.5)
    assert any("no_subject_mask" in r for r in reasons)


def test_reject_crop_incomplete():
    from scoring import hard_reject_reasons

    photo = {"mask_path": "/masks/1.png", "crop_complete": 0.4}
    reasons = hard_reject_reasons(photo, 0.8)
    assert any("crop_incomplete" in r for r in reasons)


def test_reject_highlight_clipping():
    from scoring import hard_reject_reasons

    photo = {"mask_path": "/masks/1.png", "subject_clip_high": 0.5}
    reasons = hard_reject_reasons(photo, 0.8)
    assert any("highlight_clipping" in r for r in reasons)


def test_reject_low_composite():
    from scoring import hard_reject_reasons

    photo = {"mask_path": "/masks/1.png", "crop_complete": 0.8}
    reasons = hard_reject_reasons(photo, 0.3)
    assert any("low_quality" in r for r in reasons)


def test_no_reject_good_photo():
    from scoring import hard_reject_reasons

    photo = {
        "mask_path": "/masks/1.png",
        "crop_complete": 0.95,
        "subject_clip_high": 0.01,
    }
    reasons = hard_reject_reasons(photo, 0.8)
    assert len(reasons) == 0


# -- score_encounter --


def test_score_encounter_labels_rejects():
    """score_encounter should label photos that fail hard reject rules."""
    from scoring import score_encounter

    enc = {
        "photos": [
            {  # Good photo
                "subject_tenengrad": 500,
                "bg_tenengrad": 50,
                "subject_clip_high": 0.0,
                "subject_clip_low": 0.0,
                "subject_y_median": 115,
                "crop_complete": 0.95,
                "bg_separation": 10.0,
                "subject_size": 0.1,
                "mask_path": "/masks/1.png",
            },
            {  # Bad photo: no mask
                "subject_tenengrad": None,
                "bg_tenengrad": None,
                "subject_clip_high": None,
                "subject_clip_low": None,
                "subject_y_median": None,
                "crop_complete": None,
                "bg_separation": None,
                "subject_size": None,
                "mask_path": None,
            },
        ],
    }

    score_encounter(enc)
    assert enc["photos"][0]["label"] is None  # not rejected
    assert enc["photos"][0]["quality_composite"] > 0
    assert enc["photos"][1]["label"] == "REJECT"


# ---------------------------------------------------------------------------
# Eye-focus composite replacement (Milestone 7)
# ---------------------------------------------------------------------------

def _make_base_photo(**overrides):
    """Minimal photo dict with fields score_encounter needs."""
    photo = {
        "subject_tenengrad": 500,
        "bg_tenengrad": 50,
        "subject_clip_high": 0.0,
        "subject_clip_low": 0.0,
        "subject_y_median": 115,
        "crop_complete": 0.95,
        "bg_separation": 10.0,
        "subject_size": 0.15,
        "mask_path": "/masks/1.png",
    }
    photo.update(overrides)
    return photo


def test_focus_score_uses_eye_tenengrad_when_populated():
    """When photos carry eye_tenengrad, focus_score must rank on eye sharpness, not body.

    Two photos with opposite body/eye sharpness patterns:
      A: sharp body (50000), soft eye (5000)
      B: soft body (5000), sharp eye (50000)
    Body-based ranking would score A higher; eye-based ranking scores B higher.
    """
    from scoring import score_encounter

    a = _make_base_photo(subject_tenengrad=50000, eye_tenengrad=5000)
    b = _make_base_photo(subject_tenengrad=5000, eye_tenengrad=50000)
    enc = {"photos": [a, b]}

    score_encounter(enc)

    assert b["focus_score"] > a["focus_score"], (
        f"eye-based ranking should put sharp-eye photo ahead; "
        f"got A={a['focus_score']} vs B={b['focus_score']}"
    )


def test_focus_score_falls_back_to_subject_tenengrad_when_eye_null():
    """Photos without an eye signal keep the pre-feature body-ranking behavior."""
    from scoring import score_encounter

    a = _make_base_photo(subject_tenengrad=50000, eye_tenengrad=None)
    b = _make_base_photo(subject_tenengrad=5000, eye_tenengrad=None)
    enc = {"photos": [a, b]}

    score_encounter(enc)

    assert a["focus_score"] > b["focus_score"], (
        "without eye_tenengrad, score_encounter must rank on subject_tenengrad"
    )


def test_focus_score_mixed_eye_and_body_ranks_within_their_group():
    """Mixed encounter: eye-having photos rank against each other; body-only photos
    rank against each other. No cross-contamination that would make the
    percentiles unfair."""
    from scoring import score_encounter

    # Two eye-having photos (eye sharpness: b > a).
    a = _make_base_photo(subject_tenengrad=100, eye_tenengrad=1000)
    b = _make_base_photo(subject_tenengrad=100, eye_tenengrad=10000)
    # Two body-only photos (body sharpness: d > c).
    c = _make_base_photo(subject_tenengrad=1000, eye_tenengrad=None)
    d = _make_base_photo(subject_tenengrad=10000, eye_tenengrad=None)
    enc = {"photos": [a, b, c, d]}

    score_encounter(enc)

    assert b["focus_score"] > a["focus_score"]
    assert d["focus_score"] > c["focus_score"]


def test_reject_eye_soft_fires_when_eye_present_and_below_threshold():
    """reject_eye_soft rule: eye is the weak link even if body is sharp."""
    from scoring import score_encounter

    soft_eye = _make_base_photo(
        subject_tenengrad=50000, eye_tenengrad=1000,
    )
    sharp_eye = _make_base_photo(
        subject_tenengrad=50000, eye_tenengrad=50000,
    )
    enc = {"photos": [soft_eye, sharp_eye]}

    score_encounter(enc, config={"reject_eye_focus": 0.35})

    assert any("eye_soft" in r for r in soft_eye.get("reject_reasons", []))
    assert not any("eye_soft" in r for r in sharp_eye.get("reject_reasons", []))


def test_reject_eye_soft_does_not_fire_when_eye_null():
    """Photos without an eye signal must not trigger eye_soft — even if body is soft."""
    from scoring import score_encounter

    photo = _make_base_photo(subject_tenengrad=1000, eye_tenengrad=None)
    enc = {"photos": [photo]}

    score_encounter(enc, config={"reject_eye_focus": 0.35})

    assert not any("eye_soft" in r for r in photo.get("reject_reasons", []))
