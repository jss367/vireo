"""Relative focus is explainable ranking evidence, never absolute defocus."""

import copy
import json
from pathlib import Path

import numpy as np
import pytest
from pipeline import serialize_results
from quality import _tenengrad
from scoring import hard_reject_reasons, score_encounter
from test_scoring import _make_base_photo

EXAMPLES = json.loads((Path(__file__).parent / "fixtures/relative_focus_examples.json").read_text())["cases"]


@pytest.mark.parametrize("example", EXAMPLES, ids=lambda example: example["photo"]["filename"])
def test_reported_mismatches_are_relative_evidence_not_focus_rejections(example):
    target = _make_base_photo(**example["photo"])
    peers = [_make_base_photo(**peer) for peer in example["reconstructed_eye_cohort"]
             if peer["id"] != target["id"]]
    encounter = {"photos": [target, *peers]}
    score_encounter(encounter, {"eye_detect_enabled": True})
    evidence = target["focus_evidence"]
    assert evidence["basis"] == "eye"
    assert evidence["measurement"] == example["photo"]["eye_tenengrad"]
    assert evidence["percentile"] == pytest.approx(.5 / len(encounter["photos"]))
    assert evidence["cohort_size"] == len(example["reconstructed_eye_cohort"])
    assert evidence["absolute_status"] == "not_validated"
    assert target["label"] is None
    assert target["reject_reasons"] == []

    # Same measured image, different peer set: rank changes without establishing
    # any change in absolute focus. Historical Eared Grebe scope is unknown.
    raw = target["eye_tenengrad"]
    score_encounter({"photos": [target]}, {"eye_detect_enabled": True})
    assert target["focus_score"] == .5
    assert target["eye_tenengrad"] == raw
    assert target["reject_reasons"] == []


def test_saved_grebe_and_finch_percentiles_reconstruct_but_eared_scope_is_unknown():
    for example in EXAMPLES:
        reconstructed = round(.5 / len(example["reconstructed_eye_cohort"]), 4)
        if example["species"] == "Eared Grebe":
            assert reconstructed != example["saved_focus_score"]
        else:
            assert reconstructed == example["saved_focus_score"]


def test_mixed_basis_cohorts_exclude_missing_analysis_and_serialize_membership_once():
    eye = _make_base_photo(id=1, eye_tenengrad=20.)
    eye_peer = _make_base_photo(id=2, eye_tenengrad=40., flag="rejected")
    subject = _make_base_photo(id=3)
    missing = _make_base_photo(id=4, mask_path=None, eye_tenengrad=1., focus_evidence={"stale": True})
    enc = {"photos": [eye, eye_peer, subject, missing]}
    score_encounter(enc, {"eye_detect_enabled": True})
    assert enc["focus_cohorts"]["eye"]["photo_ids"] == [1, 2]
    assert enc["focus_cohorts"]["subject"]["photo_ids"] == [3]
    assert eye["focus_evidence"]["percentile"] == .25
    assert subject["focus_evidence"]["percentile"] == .5
    assert subject["focus_evidence"]["background_measurement"] == 50
    assert "focus_evidence" not in missing
    assert missing["label"] == "REVIEW"
    saved = serialize_results({"encounters": [enc], "photos": enc["photos"], "summary": {}})
    assert saved["encounters"][0]["focus_cohorts"] == enc["focus_cohorts"]
    assert saved["photos"][0]["focus_evidence"] == eye["focus_evidence"]
    assert "photo_ids" not in eye["focus_evidence"]
    json.dumps(saved, allow_nan=False)


@pytest.mark.parametrize("invalid", [None, float("nan"), float("inf"), -1., True])
def test_optional_invalid_eye_falls_back_to_subject_and_clears_prior_eye_rank(invalid):
    photo = _make_base_photo(id=1, eye_tenengrad=invalid, eye_focus_score=.99)
    score_encounter({"photos": [photo]}, {"eye_detect_enabled": True})
    assert photo["focus_evidence"]["basis"] == "subject"
    assert "eye_focus_score" not in photo


def test_disabling_eye_detection_refreshes_measurement_and_cohort():
    photo = _make_base_photo(id=1, eye_tenengrad=3.)
    encounter = {"photos": [photo]}
    score_encounter(encounter, {"eye_detect_enabled": True})
    score_encounter(encounter, {"eye_detect_enabled": False})
    assert photo["focus_evidence"]["basis"] == "subject"
    assert photo["focus_evidence"]["measurement"] == photo["subject_tenengrad"]
    assert "eye_focus_score" not in photo
    assert encounter["focus_cohorts"]["eye"]["size"] == 0


@pytest.mark.parametrize("basis", ["eye", "subject"])
def test_regrouping_focus_alone_cannot_reject_indirectly_through_composite(basis):
    target = _make_base_photo(id=1, subject_tenengrad=10., eye_tenengrad=10.,
                              subject_size=.03, subject_y_median=85., bg_separation=10.)
    # Keep all non-focus normalization features equal, isolate the effect of
    # adding sharper peers, and place the raw composite below the reject floor.
    peers = [_make_base_photo(**{**target, "id": i, "subject_tenengrad": 1000.,
                                "eye_tenengrad": 1000.}) for i in range(2, 102)]
    config = {"eye_detect_enabled": basis == "eye", "reject_composite": .4}
    score_encounter({"photos": [target]}, config)
    before = copy.deepcopy(target)
    score_encounter({"photos": [target, *peers]}, config)
    assert target["quality_composite"] < .4 < before["quality_composite"]
    assert target["focus_score"] < before["focus_score"]
    assert target["reject_reasons"] == before["reject_reasons"] == []


def test_absolute_exposure_and_crop_rejects_remain_supported_without_relative_focus():
    photo = _make_base_photo(crop_complete=.3, subject_clip_high=.5)
    score_encounter({"photos": [photo]})
    assert any(r.startswith("crop_incomplete") for r in photo["reject_reasons"])
    assert any(r.startswith("highlight_clipping") for r in photo["reject_reasons"])


@pytest.mark.parametrize("flag,label", [
    ("flagged", "KEEP"), ("picked", "KEEP"), ("rejected", "REJECT"), ("none", "REVIEW"),
])
def test_focus_policy_preserves_photographer_decisions(flag, label):
    photo = _make_base_photo(flag=flag, flag_confirmation_status="confirmed",
                             eye_tenengrad=1., subject_clip_high=.5)
    score_encounter({"photos": [photo]}, {"eye_detect_enabled": True})
    assert photo["label"] == label
    assert photo["flag"] == flag


def test_unclipped_exposure_gain_changes_gradients_and_rank_without_changing_edge_shape():
    # A fixed edge profile with reduced exposure, without clipping, resampling
    # or blur. Raw Tenengrad scales with gain squared, so it is not calibrated
    # absolute focus evidence. The normalized spatial edge remains identical.
    x = np.linspace(-4, 4, 64)
    image = np.tile(80 + 40 * np.tanh(x), (32, 1))
    darkened = image * .5
    measurement = _tenengrad(image)
    assert _tenengrad(darkened) == pytest.approx(measurement * .25)
    assert np.allclose(darkened / darkened.max(), image / image.max())
    photo = _make_base_photo(id=1, eye_tenengrad=measurement)
    peer = _make_base_photo(id=2, eye_tenengrad=measurement * .5)
    enc = {"photos": [photo, peer]}
    score_encounter(enc, {"eye_detect_enabled": True})
    assert photo["focus_evidence"]["percentile"] == .75
    photo["eye_tenengrad"] = _tenengrad(darkened)
    score_encounter(enc, {"eye_detect_enabled": True})
    assert photo["focus_evidence"]["percentile"] == .25
    assert photo["reject_reasons"] == []
    assert photo["focus_evidence"]["absolute_status"] == "not_validated"


def test_legacy_composite_without_components_cannot_establish_rejection():
    assert hard_reject_reasons(_make_base_photo(), .01) == []
