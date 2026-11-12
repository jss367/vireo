"""The background gate: the rule, its evidence sentence, and what it does to
Highlights bucket membership."""

import background_match
import match_confidence
import numpy as np
import pytest
from classifier import BACKGROUND_PROMPTS, Classifier
from classify_job.classify_pass import _match_stats
from highlights_payload import collect_highlight_buckets
from species_memberships import (
    SECONDARY_SUBJECT_FLOOR,
    bucket_opening_subjects,
    candidate_subjects,
    explain_subject_roles,
    photo_species_memberships,
    subject_evidence_order,
    subject_is_background_like,
)


def test_margin_rule_matches_the_calibration():
    # A hawk crop: best label 0.49, best prompt 0.09.
    assert not background_match.is_background_like(0.49, 0.09)
    # A trunk strip next to it: best label 0.41, "leaves" at 0.27.
    assert background_match.is_background_like(0.41, 0.27)
    # Just inside the margin counts; just outside does not.
    assert background_match.is_background_like(0.5, 0.375)
    assert not background_match.is_background_like(0.5, 0.3)
    assert background_match.background_gap(0.41, 0.27) == pytest.approx(-0.14)


def test_confident_detector_box_is_never_background():
    # A kinglet filling a corner of a tamarisk box: the gap says foliage,
    # the detector at 0.87 says animal, and the detector wins.
    assert not background_match.is_background_like(0.49, 0.35, detector_confidence=0.87)
    assert not background_match.is_background_like(0.49, 0.35, detector_confidence=background_match.CONFIDENT_DETECTION)
    # Below the line, and for the whole-frame row at 0, the gap decides.
    assert background_match.is_background_like(0.49, 0.35, detector_confidence=0.34)
    assert background_match.is_background_like(0.49, 0.35, detector_confidence=0.0)
    # Unknown confidence is not confidence.
    assert background_match.is_background_like(0.49, 0.35, detector_confidence=None)
    assert background_match.is_confident_detection(0.5) and not background_match.is_confident_detection(0.49)
    assert not background_match.is_confident_detection(None)


def test_whole_frame_row_is_never_a_confident_detection():
    # The schema does not constrain the whole-frame row's detector_confidence
    # (legacy rows and some test fixtures carry 1.0). It is still a no-box
    # placeholder, so the override that trusts a confident box over the gap
    # must not fire for it — the gap decides.
    assert background_match.is_background_like(
        0.49, 0.35, detector_confidence=1.0, whole_frame=True,
    )
    assert background_match.is_background_like(
        0.49, 0.35, detector_confidence=background_match.CONFIDENT_DETECTION,
        whole_frame=True,
    )
    assert not background_match.is_confident_detection(1.0, whole_frame=True)
    assert not background_match.is_confident_detection(
        background_match.CONFIDENT_DETECTION, whole_frame=True,
    )
    # A low-confidence whole-frame row still falls to the gap, as before.
    assert background_match.is_background_like(
        0.49, 0.35, detector_confidence=0.0, whole_frame=True,
    )
    # Outside the margin, a whole-frame row stays a subject.
    assert not background_match.is_background_like(
        0.49, 0.09, detector_confidence=1.0, whole_frame=True,
    )


def test_unmeasured_or_foreign_scale_is_never_background():
    assert not background_match.is_background_like(None, 0.5)
    assert not background_match.is_background_like(0.4, None)
    assert background_match.background_gap(None, 0.3) is None
    # A supervised model's logits have no prompt to compare with.
    assert not background_match.is_background_like(2.0, 2.0, score_kind="logit")


def test_assess_background_states_and_sentences():
    verdict = match_confidence.assess_background("cosine", 0.41, 0.37, "tree bark")
    assert verdict["state"] == match_confidence.BACKGROUND
    assert "tree bark" in verdict["explanation"]
    assert "not an identification" in verdict["explanation"]
    assert verdict["gap"] == pytest.approx(-0.04)

    verdict = match_confidence.assess_background("cosine", 0.49, 0.09, "twigs")
    assert verdict["state"] == match_confidence.SUBJECT
    assert "twigs" in verdict["explanation"]

    # Inside the margin but on a confident box: a subject, and the sentence
    # says the detector was trusted over the prompt.
    verdict = match_confidence.assess_background("cosine", 0.49, 0.35, "tamarisk", detector_confidence=0.87)
    assert verdict["state"] == match_confidence.SUBJECT
    assert verdict["detector_confidence"] == 0.87
    assert "detector was 0.87 sure" in verdict["explanation"]
    assert "tamarisk" in verdict["explanation"]
    assert match_confidence.assess_background("cosine", 0.49, 0.35, "tamarisk", detector_confidence=0.34)["state"] == match_confidence.BACKGROUND

    # A whole-frame row with a high stored confidence still goes by the gap:
    # no detector box means no override, even when the stored value sits at 1.0.
    whole_frame_verdict = match_confidence.assess_background(
        "cosine", 0.49, 0.35, "twigs", detector_confidence=1.0, whole_frame=True,
    )
    assert whole_frame_verdict["state"] == match_confidence.BACKGROUND
    assert "detector was" not in whole_frame_verdict["explanation"]

    assert match_confidence.assess_background("cosine", 0.49, None)["state"] == match_confidence.UNAVAILABLE
    assert match_confidence.assess_background("logit", 3.0, 2.9)["state"] == match_confidence.UNAVAILABLE


def test_match_stats_carry_the_background_match():
    stats = _match_stats(
        [{"species": "Elf Owl", "raw_score": 0.41, "background_score": 0.27, "background_prompt": "leaves"},
         {"species": "Hawk", "raw_score": 0.38, "background_score": 0.27, "background_prompt": "leaves"}],
        model_type="bioclip",
    )
    assert stats["background_score"] == pytest.approx(0.27)
    assert stats["background_prompt"] == "leaves"
    # A classifier without prompts leaves both unrecorded, not zero.
    stats = _match_stats([{"species": "Hawk", "raw_score": 0.4}], model_type="timm")
    assert stats["background_score"] is None and stats["background_prompt"] is None


def test_classifier_attaches_its_background_match_to_every_result():
    clf = Classifier.__new__(Classifier)
    clf._mode = "custom"
    clf._classes = ["Hawk", "Owl"]
    clf._label_identities = {}
    # Unit vectors: the image points at the hawk label, and "bark" sits
    # halfway between the hawk and the second axis.
    clf._txt_embeddings = np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32)
    clf._background_prompts = ["bark", "sky"]
    clf._background_embeddings = np.array([[0.6, 0.0], [0.8, 1.0]], dtype=np.float32)
    results, embedding = clf._classify_features(np.array([[1.0, 0.0]], dtype=np.float32), threshold=0)
    assert [r["species"] for r in results] == ["Hawk", "Owl"]
    assert all(r["background_prompt"] == "bark" for r in results)
    assert all(r["background_score"] == pytest.approx(0.6) for r in results)
    assert results[0]["raw_score"] == pytest.approx(1.0)
    assert embedding.tolist() == [1.0, 0.0]


def test_classifier_without_prompts_reports_nothing():
    clf = Classifier.__new__(Classifier)
    clf._mode = "custom"
    clf._classes = ["Hawk"]
    clf._label_identities = {}
    clf._txt_embeddings = np.array([[1.0]], dtype=np.float32)
    results, _ = clf._classify_features(np.array([[1.0]], dtype=np.float32), threshold=0)
    assert "background_score" not in results[0]
    assert clf._background_match(np.array([[1.0]], dtype=np.float32)) == (None, None)


def test_background_prompts_are_plain_distinct_labels():
    assert len(set(BACKGROUND_PROMPTS)) == len(BACKGROUND_PROMPTS)
    assert all(p == p.strip() and p for p in BACKGROUND_PROMPTS)


# --- membership rule ------------------------------------------------------

def _subject(species, confidence, *, detector=0.9, model="megadetector-v6",
             label=0.49, background=0.09, prediction_id=1):
    return {
        "detection_id": prediction_id * 10, "prediction_id": prediction_id,
        "species": species, "confidence": confidence,
        "detector_confidence": detector, "detector_model": model,
        "max_match_score": label, "background_score": background, "score_kind": "cosine",
    }


HAWK = _subject("Red-shouldered Hawk", 0.52, detector=0.49, prediction_id=1)
TRUNK = _subject("Elf Owl", 0.88, detector=0.34, label=0.41, background=0.27, prediction_id=2)
WHOLE = _subject("Orange-breasted Waxbill", 0.98, detector=0.0, model="full-image",
                 label=0.47, background=0.36, prediction_id=3)


def test_subjects_rank_by_region_evidence_not_softmax():
    ordered = candidate_subjects({"subject_predictions": [WHOLE, TRUNK, HAWK]})
    assert [s["species"] for s in ordered] == ["Red-shouldered Hawk", "Elf Owl", "Orange-breasted Waxbill"]
    # Among two clean boxes the detector's confidence decides, then the softmax.
    weak = _subject("Verdin", 0.99, detector=0.3, prediction_id=4)
    strong = _subject("Wrentit", 0.6, detector=0.8, prediction_id=5)
    assert [s["species"] for s in sorted([weak, strong], key=subject_evidence_order)] == ["Wrentit", "Verdin"]


def test_trunk_box_opens_no_bucket_and_hawk_wins_the_photo():
    memberships = photo_species_memberships(
        {"species_names": [], "subject_predictions": [TRUNK, HAWK]}, confidence_threshold=0.5,
    )
    assert [(m.species, m.whole_frame) for m in memberships] == [("Red-shouldered Hawk", False)]
    assert memberships[0].prediction["prediction_id"] == 1


def test_second_real_bird_needs_a_confident_box():
    second = _subject("Yellow-rumped Warbler", 0.9, detector=SECONDARY_SUBJECT_FLOOR, prediction_id=6)
    weak_second = _subject("Yellow-rumped Warbler", 0.9, detector=0.3, prediction_id=7)
    primary = _subject("Lesser Goldfinch", 0.95, detector=0.8, prediction_id=8)
    assert [s["species"] for s in bucket_opening_subjects([primary, second])] == [
        "Lesser Goldfinch", "Yellow-rumped Warbler"]
    assert [s["species"] for s in bucket_opening_subjects([primary, weak_second])] == ["Lesser Goldfinch"]
    # The primary may itself be a weak box: it is still the photo's best region.
    assert bucket_opening_subjects([weak_second]) == [weak_second]


def test_whole_frame_guess_speaks_only_for_a_photo_without_boxes():
    assert bucket_opening_subjects([HAWK, WHOLE]) == [HAWK]
    nuthatch = _subject("Red-breasted Nuthatch", 1.0, detector=0.0, model="full-image",
                        label=0.52, background=0.19, prediction_id=9)
    memberships = photo_species_memberships({"species_names": [], "subject_predictions": [nuthatch]})
    assert [(m.species, m.whole_frame) for m in memberships] == [("Red-breasted Nuthatch", True)]
    # A whole frame of twigs scoring like background opens nothing.
    assert photo_species_memberships({"species_names": [], "subject_predictions": [WHOLE]}) == []
    # ...and neither does a photo whose only box is a trunk.
    assert photo_species_memberships({"species_names": [], "subject_predictions": [TRUNK]}) == []


def test_whole_frame_row_opens_no_bucket_even_when_its_stored_confidence_is_high():
    # The schema does not pin a whole-frame row's detector_confidence at 0,
    # and legacy anchors may already carry a non-zero value. The gap must
    # still decide: a photo whose only "subject" is a twig-frame row stays
    # out of the Highlights bucket however its stored confidence reads.
    legacy_whole = _subject(
        "Red-breasted Nuthatch", 0.98, detector=1.0, model="full-image",
        label=0.47, background=0.36, prediction_id=14,
    )
    assert subject_is_background_like(legacy_whole)
    assert photo_species_memberships(
        {"species_names": [], "subject_predictions": [legacy_whole]},
    ) == []


def test_small_bird_in_foliage_keeps_its_bucket_on_a_confident_box():
    kinglet = _subject("Ruby-crowned Kinglet", 0.9, detector=0.87, label=0.49, background=0.35, prediction_id=12)
    assert not subject_is_background_like(kinglet)
    memberships = photo_species_memberships({"species_names": [], "subject_predictions": [kinglet]})
    assert [m.species for m in memberships] == ["Ruby-crowned Kinglet"]
    # The same scores on a weak box are still a trunk strip.
    strip = _subject("Ruby-crowned Kinglet", 0.9, detector=0.34, label=0.49, background=0.35, prediction_id=13)
    assert subject_is_background_like(strip)
    assert photo_species_memberships({"species_names": [], "subject_predictions": [strip]}) == []
    buckets, unidentified = collect_highlight_buckets([_row(1, [kinglet]), _row(2, [strip])], confidence_threshold=0.5)
    assert [b["species"] for b in buckets] == ["Ruby-crowned Kinglet"]
    assert buckets[0]["photos"][0]["background_like"] is False
    assert [p["id"] for p in unidentified] == [2] and unidentified[0]["background_like"]


def test_runs_without_a_background_score_keep_the_old_behavior():
    old = {"detection_id": 1, "prediction_id": 1, "species": "Robin", "confidence": 0.9}
    memberships = photo_species_memberships({"species_names": [], "subject_predictions": [old]})
    assert [m.species for m in memberships] == ["Robin"]
    single = photo_species_memberships({"predicted_species": "Robin", "predicted_confidence": 0.9, "prediction_id": 1})
    assert [m.species for m in single] == ["Robin"]


def test_keyword_bucket_still_uses_any_matching_subject():
    memberships = photo_species_memberships(
        {"species_names": ["Elf Owl"], "subject_predictions": [TRUNK, HAWK]},
    )
    assert memberships[0].accepted_name == "Elf Owl"
    assert memberships[0].prediction["prediction_id"] == 2
    # The keyword matches a subject, so the hawk box still suggests its species.
    assert [m.species for m in memberships[1:]] == ["Red-shouldered Hawk"]


def _row(pid, subjects):
    return {"id": pid, "filename": f"{pid}.jpg", "quality_score": 0.5, "flag": "none",
            "species_names": [], "subject_predictions": subjects}


def test_whole_frame_buckets_are_badged_and_sorted_last():
    nuthatch = _subject("Red-breasted Nuthatch", 1.0, detector=0.0, model="full-image",
                        label=0.52, background=0.19, prediction_id=9)
    buckets, unidentified = collect_highlight_buckets(
        [_row(1, [nuthatch]), _row(2, [HAWK]), _row(3, [WHOLE]), _row(4, [TRUNK])],
        confidence_threshold=0.5,
    )
    by_species = {b["species"]: b for b in buckets}
    nut = by_species["Red-breasted Nuthatch"]
    assert nut["certainty"] == "whole_frame"
    assert nut["avg_confidence"] is None and nut["whole_frame_count"] == 1
    assert nut["photos"][0]["whole_frame"] is True
    hawk = by_species["Red-shouldered Hawk"]
    assert hawk["certainty"] == "candidate"
    assert hawk["photos"][0]["whole_frame"] is False
    assert hawk["photos"][0]["predicted_detector_confidence"] == pytest.approx(0.49)
    assert [b["species"] for b in buckets][-1] == "Red-breasted Nuthatch"
    # The twig frame and the trunk-only photo fall to Unidentified, and say why.
    assert {p["id"] for p in unidentified} == {3, 4}
    assert all(p["background_like"] for p in unidentified)


def test_mixed_bucket_averages_only_detected_regions():
    guess = _subject("Red-shouldered Hawk", 0.99, detector=0.0, model="full-image",
                     label=0.5, background=0.2, prediction_id=11)
    buckets, _ = collect_highlight_buckets([_row(1, [HAWK]), _row(2, [guess])], confidence_threshold=0.5)
    (bucket,) = buckets
    assert bucket["avg_confidence"] == pytest.approx(0.52)
    assert bucket["whole_frame_count"] == 1
    assert bucket["certainty"] == "candidate"


def test_repository_rank_decides_the_primary_over_the_fallback_order():
    strip = dict(_subject("Common Leopard Gecko", 0.92, detector=0.47, prediction_id=20), subject_rank=2)
    hawk = dict(_subject("Red-shouldered Hawk", 1.0, detector=0.37, prediction_id=21), subject_rank=1)
    assert [s["species"] for s in candidate_subjects({"subject_predictions": [strip, hawk]})] == [
        "Red-shouldered Hawk", "Common Leopard Gecko"]


def test_explain_subject_roles_names_why_a_region_opens_nothing():
    trunk = dict(TRUNK, detection_id=2)
    hawk = dict(HAWK, detection_id=1)
    roles = explain_subject_roles({"species_names": [], "subject_predictions": [trunk, hawk]})
    assert roles == {1: ("bucket", "Red-shouldered Hawk"), 2: ("background", None)}
    tagged = explain_subject_roles({"species_names": ["Red-shouldered Hawk"], "subject_predictions": [trunk, hawk]})
    assert tagged[1] == ("tagged", "Red-shouldered Hawk")
    disagree = explain_subject_roles({"species_names": ["Wrentit"], "subject_predictions": [hawk]})
    assert disagree == {1: ("tags_disagree", None)}
    # A region the user chose is never called background.
    chosen = explain_subject_roles({"species_names": [], "subject_predictions": [dict(trunk, subject_chosen=True)]})
    assert chosen == {2: ("bucket", "Elf Owl")}
