"""A relative winner is a candidate, never positive evidence of a usable animal."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
from keep_safety import (
    KEEP_REVIEW_MESSAGE,
    review_culling_suggestions,
    review_ranked_response,
    review_ranked_suggestions,
)
from pipeline import run_triage, serialize_results
from subject_readiness import automatic_keep_assessment


def measured_photo(photo_id=1, **overrides):
    photo = {"id": photo_id, "mask_path": "/mask.png", "subject_present": True,
             "subject_tenengrad": 500., "bg_tenengrad": 50., "subject_clip_high": 0.,
             "subject_clip_low": 0., "subject_y_median": 115., "crop_complete": .95,
             "bg_separation": 10., "subject_size": .15, "flag": "none"}
    photo.update(overrides)
    return photo


@pytest.mark.parametrize("raw", [0., .001, 500., 1e9])
def test_singleton_ranking_does_not_validate_absolute_usability(raw):
    photo = measured_photo(subject_tenengrad=raw)
    encounters, photos = run_triage([{"photos": [photo]}])
    assert photo["selection_eligible"] is True  # inputs remain useful for comparison
    assert photo["ranking_selected"] is True
    assert photo["quality_composite"] is not None
    assert photo["label"] == "REVIEW"
    assert photo["automatic_keep_eligible"] is False
    assert photo["keep_reasons"] == ["absolute_focus_not_validated", "animal_subject_not_validated"]
    saved = serialize_results({"encounters": encounters, "photos": photos, "summary": {}})
    assert saved["photos"][0]["keep_status"] == "not_validated"


@pytest.mark.parametrize("raw_values", [[.001, .002, .003], [.001, 100000.], [500.]])
def test_all_poor_and_mixed_groups_stay_provisional_without_positive_validation(raw_values):
    photos = [measured_photo(i, subject_tenengrad=raw) for i, raw in enumerate(raw_values, 1)]
    run_triage([{"photos": photos}])
    assert all(p["label"] == "REVIEW" for p in photos)
    assert all(p["quality_composite"] is not None for p in photos)
    assert any(p["ranking_selected"] for p in photos)


def test_classifier_confidence_and_claimed_cached_validation_cannot_authorize_a_keep():
    photo = measured_photo(detection_conf=1., species_top5=[("Osprey", .9999)],
                           confirmed_species="Osprey", focus_evidence={"absolute_status": "validated"},
                           automatic_keep_eligible=True, keep_status="validated")
    run_triage([{"photos": [photo]}])
    assert photo["label"] == "REVIEW"
    assert automatic_keep_assessment(photo)["eligible"] is False


def test_independent_ranker_returns_are_guarded_with_decisions_preserved():
    @review_ranked_response
    def independent_subject_ranker():
        return {"best_photo_id": 1, "alternate_ids": [2, 3], "suggested_reject_ids": [2, 3, 4, 5],
                "cards": [
                    {"id": 1, "rank": 1, "role": "best", "label": "KEEP"},
                    {"id": 2, "rank": 2, "role": "alternate", "label": None},
                    {"id": 3, "rank": 3, "role": "reject", "label": "REVIEW"},
                    {"id": 4, "rank": 4, "role": "reject", "label": "REJECT", "flag": "flagged", "flag_confirmation_status": "confirmed"},
                    {"id": 5, "rank": 5, "role": "best", "label": "KEEP", "flag": "rejected", "flag_confirmation_status": "confirmed"},
                ]}, None

    response, error = independent_subject_ranker()
    assert error is None
    assert response["best_photo_id"] is None
    assert response["ranked_candidate_id"] == 1
    assert response["alternate_ids"] == []
    assert response["suggested_reject_ids"] == []
    assert [c["label"] for c in response["cards"]] == ["REVIEW", "REVIEW", "REVIEW", "KEEP", "REJECT"]
    assert [c["role"] for c in response["cards"]] == ["review", "review", "review", "kept", "reject"]
    assert response["cards"][0]["reasons"][0] == KEEP_REVIEW_MESSAGE
    assert [c["rank"] for c in response["cards"]] == [1, 2, 3, 4, 5]


def test_unavailable_evidence_stays_visible_and_unknown_is_not_photographic_failure():
    result = {"cards": [{"id": 1, "rank": None, "role": "review", "label": "REVIEW",
                         "analysis_status": "not_run", "analysis_reasons": ["detection_not_run"]}]}
    review_ranked_suggestions(result)
    assert result["ranked_candidate_id"] is None
    assert result["cards"][0]["analysis_status"] == "not_run"
    assert result["suggested_reject_ids"] == []


@pytest.mark.parametrize("flag,label", [("flagged", "KEEP"), ("picked", "KEEP"), ("rejected", "REJECT")])
def test_explicit_photographer_decisions_override_unvalidated_evidence(flag, label):
    photo = measured_photo(flag=flag, flag_confirmation_status="confirmed", subject_tenengrad=0.)
    run_triage([{"photos": [photo]}])
    assert photo["label"] == label
    assert photo["automatic_keep_eligible"] is False


@pytest.mark.parametrize("flag", ["flagged", "picked", "rejected"])
@pytest.mark.parametrize("confirmation", [None, "unverified"])
def test_bare_flags_cannot_override_pipeline_or_ranked_review(flag, confirmation):
    photo = measured_photo(flag=flag, flag_confirmation_status=confirmation)
    run_triage([{"photos": [photo]}])
    assert photo["label"] == "REVIEW"
    card = {"id": 1, "rank": 1, "flag": flag, "flag_confirmation_status": confirmation,
            "label": "KEEP", "role": "best"}
    result = {"cards": [card]}
    review_ranked_suggestions(result)
    assert card["label"] == "REVIEW"
    assert result["best_photo_id"] is None
    assert result["suggested_reject_ids"] == []


def test_confirmed_clear_prevents_automatic_rejection():
    photo = measured_photo(flag="none", flag_confirmation_status="confirmed", subject_clip_high=.99)
    run_triage([{"photos": [photo]}])
    assert photo["reject_reasons"]  # Real technical evidence remains available for inspection.
    assert photo["label"] == "REVIEW"
    card = dict(photo, rank=1, role="reject", label="REJECT")
    result = {"cards": [card]}
    review_ranked_suggestions(result)
    assert card["label"] == "REVIEW"
    assert card["role"] == "review"
    assert result["suggested_reject_ids"] == []


def test_error_from_ranker_is_preserved():
    @review_ranked_response
    def unavailable():
        return None, "No current analysis"
    assert unavailable() == (None, "No current analysis")


def test_reported_before_state_winners_and_clear_wildlife_controls():
    fixture = json.loads((Path(__file__).parent / "fixtures/keep_safety_review_examples.json").read_text())
    by_id = {p["id"]: p for p in fixture["photos"]}
    for pid in (213640, 214325, 216521):
        photo = deepcopy(by_id[pid])
        assert photo["label"] == "KEEP" and photo["flag"] == "none"
        run_triage([{"photos": [photo]}])
        assert photo["label"] == "REVIEW"
        assert photo["selection_eligible"] is True
        assert photo["automatic_keep_eligible"] is False
    failures = deepcopy([by_id[pid] for pid in (213640, 214325, 216521)])
    run_triage([{"photos": failures}])
    assert all(p["label"] == "REVIEW" for p in failures)
    # Independently viewed clear wildlife controls retain useful measurements.
    # Clarity by visual review is not a calibrated automatic validator.
    for pid in (214419, 216716):
        photo = deepcopy(by_id[pid])
        run_triage([{"photos": [photo]}])
        assert photo["quality_composite"] is not None
        assert photo["label"] == "REVIEW"  # Historical flags have no recorded photographer attribution.
        assert photo["automatic_keep_eligible"] is False


def test_reject_role_requires_recorded_failure_and_cannot_create_a_keeper():
    result = {"best_photo_id": 1, "summary": {"keep_count": 2, "review_count": 0, "reject_count": 0},
              "cards": [{"id": 1, "rank": 1, "label": "REJECT", "role": "best",
                         "reject_reasons": ["blown_highlights (0.99)"]},
                        {"id": 2, "rank": 2, "label": "REJECT", "role": "reject"}]}
    review_ranked_suggestions(result)
    assert result["best_photo_id"] is None
    assert result["ranked_candidate_id"] == 2
    assert result["suggested_reject_ids"] == [1]
    assert result["cards"][0]["role"] == "reject"
    assert result["cards"][1]["role"] == "review"
    assert result["summary"] == {"keep_count": 0, "review_count": 1, "reject_count": 1}


def test_external_evaluation_report_and_cached_producer_claims_do_not_authorize_runtime_keeps():
    photo = measured_photo(absolute_keeper_evidence={
        'runtime_approved': True, 'risk_requirement_met': True,
        'producer': 'self-claimed validated', 'model_sha256': 'a' * 64,
        'animal_score': 1., 'focus_score': 1.,
    })
    encounters, photos = run_triage([{'photos': [photo]}])
    saved = serialize_results({'encounters': encounters, 'photos': photos, 'summary': {}})
    assert saved['photos'][0]['label'] == 'REVIEW'
    assert saved['photos'][0]['automatic_keep_eligible'] is False
    assert saved['photos'][0]['absolute_keeper_evidence'] == photo['absolute_keeper_evidence']


@pytest.mark.parametrize("group_key", ["scene_groups", "pose_groups"])
@pytest.mark.parametrize("cached_action", ["keep", "reject"])
def test_legacy_culling_cache_rechecks_suggestions_and_current_user_flags(app_and_db, tmp_path, group_key, cached_action):
    from services.photo_review import PhotoReviewService

    app, db = app_and_db
    ids = [p["id"] for p in db.photos.list_page()]
    PhotoReviewService(db).set_flag(ids[0], "flagged")
    PhotoReviewService(db).set_flag(ids[1], "rejected")
    cached = {
        "total_photos": 3, "suggested_keepers": 1, "suggested_rejects": 2,
        "species_groups": [{"keepers": 1, "rejects": 2, "photos_data": [
            {"photo_id": pid, "flag": "flagged", "automatic_keep_eligible": True} for pid in ids
        ], group_key: [{"photos": [
            {"photo_id": pid, "action": cached_action,
             "automatic_keep_eligible": True, "keep_status": "validated", "flag": "flagged"}
            for pid in ids
        ]}]}],
    }
    path = tmp_path / f"culling_results_ws{db.active_workspace_id}.json"
    path.write_text(json.dumps(cached))
    before = path.read_bytes()
    client = app.test_client()
    for _ in range(2):
        response = client.get("/api/culling/results")
        assert response.status_code == 200
        result = response.get_json()
        photos = result["species_groups"][0][group_key][0]["photos"]
        assert [p["action"] for p in photos] == ["keep", "reject", "review"]
        assert all(p["automatic_keep_eligible"] is False for p in photos)
        assert photos[2]["keep_reasons"] == ["absolute_focus_not_validated", "animal_subject_not_validated"]
        assert result["suggested_keepers"] == result["suggested_rejects"] == result["suggested_reviews"] == 1
        assert result["species_groups"][0]["reviews"] == 1
        raw = result["species_groups"][0]["photos_data"]
        assert [p["flag"] for p in raw] == ["flagged", "rejected", "none"]
        assert all(p["automatic_keep_eligible"] is False for p in raw)
    assert path.read_bytes() == before
    assert [db.photos.get(pid)["flag"] for pid in ids] == ["flagged", "rejected", "none"]


@pytest.mark.parametrize("singleton", [False, True])
def test_legacy_culling_reported_failures_remain_review_candidates(singleton):
    from culling import _build_scene_groups

    fixture = json.loads((Path(__file__).parent / "fixtures/keep_safety_review_examples.json").read_text())
    failures = [p for p in fixture["photos"] if p["id"] in (213640, 214325, 216521)]
    ids = [p["id"] for p in failures]
    groups = [[pid] for pid in ids] if singleton else [ids]
    scenes, _, _ = _build_scene_groups(groups, groups, [
        {"photo_id": p["id"], "quality": p["subject_tenengrad"]} for p in failures
    ], {}, {})
    result = {"species_groups": [{"scene_groups": scenes}]}
    review_culling_suggestions(result, {pid: "none" for pid in ids})
    assert result["suggested_keepers"] == result["suggested_rejects"] == 0
    assert result["suggested_reviews"] == 3
    assert all(p["action"] == "review" for s in scenes for p in s["photos"])
    assert sum(p["ranking_selected"] for s in scenes for p in s["photos"]) == (3 if singleton else 1)
    before = deepcopy(result)
    review_culling_suggestions(result, {pid: "none" for pid in ids})
    assert result == before
