"""Missing analysis must stay REVIEW through final selection and serialization."""

from copy import deepcopy

import pytest
from pipeline import run_triage
from scoring import composite_quality_score, hard_reject_reasons, score_encounter
from selection import mmr_select, triage_encounters
from subject_readiness import REQUIRED_QUALITY_FIELDS, SCORE_FIELDS, subject_analysis_readiness


def complete_photo(photo_id=1, **overrides):
    photo = {
        "id": photo_id, "mask_path": "/mask.png", "subject_tenengrad": 500.,
        "bg_tenengrad": 50., "subject_clip_high": 0., "subject_clip_low": 0.,
        "subject_y_median": 115., "crop_complete": .95, "bg_separation": 10.,
        "subject_size": .15,
        "analysis_provenance": {"detection": {"status": "complete", "current": True},
                                "segmentation": {"status": "complete", "current": True}},
    }
    photo.update(overrides)
    return photo


@pytest.mark.parametrize("stage,status", [
    ("detection", "not_run"), ("detection", "failed"),
    ("segmentation", "failed"), ("segmentation", "incomplete"),
])
def test_stage_unavailable_survives_final_selection(stage, status):
    photo = complete_photo(quality_composite=.01, label="REJECT", reject_reasons=["no_subject_mask"])
    photo["analysis_provenance"][stage] = {"status": status}
    _, photos = run_triage([{"photos": [photo], "bursts": [[photo]]}])
    assert photos[0]["label"] == "REVIEW"
    assert photo["analysis_status"] == status
    assert photo["analysis_reasons"] == [f"{stage}_{status}"]
    assert photo["reject_reasons"] == []
    assert all(photo[field] is None for field in SCORE_FIELDS)
    assert mmr_select([photo], .85, 3) == []


@pytest.mark.parametrize("field", ["mask_path", *REQUIRED_QUALITY_FIELDS])
def test_partial_quality_cannot_trigger_any_quality_reject(field):
    photo = complete_photo(**{field: None})
    # Even a bad measured component cannot justify a reject until all required
    # inputs exist; no zero/neutral substitution may create a composite score.
    photo["subject_clip_high"] = .99 if field != "subject_clip_high" else None
    _, photos = run_triage([{"photos": [photo]}])
    assert photos[0]["label"] == "REVIEW"
    assert photo["analysis_reasons"] == [f"missing_{field}" if field != "mask_path" else "missing_subject_mask"]
    assert composite_quality_score(photo, [photo]) is None
    assert hard_reject_reasons(photo, .01) == []


@pytest.mark.parametrize("stage", ["detection", "segmentation"])
def test_stale_stage_is_incomplete(stage):
    photo = complete_photo()
    photo["analysis_provenance"][stage]["current"] = False
    assert subject_analysis_readiness(photo) == {
        "status": "incomplete", "reasons": [f"{stage}_stale"], "eligible": False,
    }


def test_completed_no_subject_is_explicit_review_policy():
    photo = {"id": 1, "subjects": [], "subject_absent": True,
             "analysis_provenance": {"detection": {"status": "complete", "no_qualifying_subject": True}}}
    _, photos = run_triage([{"photos": [photo]}])
    assert photos[0]["analysis_status"] == "complete"
    assert photo["analysis_reasons"] == ["no_qualifying_subject"]
    assert photo["label"] == "REVIEW"
    unknown = {"id": 2, "subjects": []}
    assert subject_analysis_readiness(unknown)["status"] == "not_run"


@pytest.mark.parametrize("eye_enabled", [False, True])
def test_incomplete_frames_do_not_contaminate_any_comparison_cohort(eye_enabled):
    reference = [complete_photo(1, subject_tenengrad=100., eye_tenengrad=10., noise_estimate=2.),
                 complete_photo(2, subject_tenengrad=1000., eye_tenengrad=100., noise_estimate=20.)]
    mixed = deepcopy(reference)
    for i in range(20):
        mixed.append(complete_photo(3+i, subject_tenengrad=0., eye_tenengrad=100000.,
                                   bg_tenengrad=0., bg_separation=100000., noise_estimate=0.,
                                   subject_y_median=None))
    cfg = {"eye_detect_enabled": eye_enabled}
    score_encounter({"photos": reference}, cfg)
    score_encounter({"photos": mixed}, cfg)
    for before, after in zip(reference, mixed[:2], strict=True):
        for field in (*SCORE_FIELDS, "label", "reject_reasons"):
            assert before.get(field) == after.get(field)
        assert composite_quality_score(before, reference) == composite_quality_score(after, mixed)
    assert all(p["label"] == "REVIEW" for p in mixed[2:])


@pytest.mark.parametrize("flag,label", [("flagged", "KEEP"), ("picked", "KEEP"), ("rejected", "REJECT")])
@pytest.mark.parametrize("missing", [True, False])
def test_explicit_decisions_survive_scoring_selection_and_rarity(flag, label, missing):
    photo = complete_photo(flag=flag, crop_complete=None if missing else .01,
                           species_top5=[("Mandarin Duck", .9)])
    _, photos = run_triage([{"photos": [photo]}])
    assert photos[0]["label"] == label
    assert photo["flag"] == flag


def test_readiness_recomputes_when_missing_measurements_arrive():
    photo = complete_photo(subject_y_median=None)
    run_triage([{"photos": [photo]}])
    assert photo["label"] == "REVIEW"
    photo["subject_y_median"] = 115.
    run_triage([{"photos": [photo]}])
    assert photo["label"] == "KEEP"
    assert photo["analysis_reasons"] == []


def test_legacy_selection_eligible_false_cannot_be_promoted():
    photo = {"id": 1, "label": "REVIEW", "selection_eligible": False, "quality_composite": None}
    _, photos = triage_encounters([{"photos": [photo]}])
    assert photos[0]["label"] == "REVIEW"


def test_current_positive_detection_overrides_stale_subject_absent():
    photo = complete_photo(subject_absent=True)
    assert subject_analysis_readiness(photo)["eligible"] is True


def test_selection_without_scoring_rechecks_cached_reject_and_missing_analysis():
    photo = {"id": 1, "label": "REJECT", "quality_composite": .9, "subjects": []}
    _, photos = triage_encounters([{"photos": [photo]}])
    assert photos[0]["label"] == "REVIEW"
    assert photos[0]["analysis_status"] == "not_run"
    assert photos[0]["quality_composite"] is None


def test_cached_mask_and_metrics_do_not_establish_detector_completion():
    photo = complete_photo()
    photo.pop("analysis_provenance")
    assert subject_analysis_readiness(photo)["reasons"] == ["detection_not_run"]


@pytest.fixture
def readiness_photo(db, tmp_path):
    from repositories.masks_features import MaskQuality, PipelineFeatures
    from services import photo_ingest

    folder = db.add_folder(str(tmp_path), name="Readiness")
    pid = photo_ingest.add_photo(db, folder, "duck.jpg", ".jpg", 100, 1.)
    mask_path = tmp_path / "mask.png"
    mask_path.write_bytes(b"scratch-mask")
    quality = {key: complete_photo()[key] for key in REQUIRED_QUALITY_FIELDS}
    db.masks_features.update_pipeline_features(pid, PipelineFeatures(mask_path=str(mask_path), **{k: v for k, v in quality.items() if k != "subject_size"}))
    db.photos.update_quality(pid, subject_size=quality["subject_size"])
    return pid, mask_path, MaskQuality(**quality)


def _loaded_photo(db, pid, **pipeline_settings):
    from pipeline import load_photo_features

    effective = {"detector_confidence": .2, "pipeline": {
        "weak_detection_rescue_enabled": False, "sam2_variant": "sam2-small", **pipeline_settings,
    }}
    return load_photo_features(db, photo_ids=[pid], effective_config=effective)[0]


def _seed_detection_and_mask(db, readiness_photo):
    pid, mask_path, quality = readiness_photo
    db.detections.save(pid, [{"box": {"x": .1, "y": .1, "w": .4, "h": .4},
                              "confidence": .9, "category": "animal"}], detector_model="megadetector-v6")
    db.model_runs.record_detector_run(pid, "megadetector-v6", 1)
    db.masks_features.upsert_mask(pid, "sam2-small", str(mask_path), "megadetector-v6",
                                 (.1, .1, .4, .4), quality)
    db.masks_features.set_active_variant(pid, "sam2-small", .2)


def test_loader_does_not_infer_detection_from_old_mask(db, readiness_photo):
    pid, _, _ = readiness_photo
    photo = _loaded_photo(db, pid)
    assert photo["analysis_provenance"]["detection"]["status"] == "not_run"
    run_triage([{"photos": [photo]}])
    assert photo["label"] == "REVIEW"


def test_loader_empty_completed_detector_and_person_box_are_explicit_no_subject(db, readiness_photo):
    pid, _, _ = readiness_photo
    db.detections.save(pid, [{"box": {"x": .1, "y": .1, "w": .4, "h": .4},
                              "confidence": .9, "category": "person"}], detector_model="megadetector-v6")
    db.model_runs.record_detector_run(pid, "megadetector-v6", 1)
    photo = _loaded_photo(db, pid)
    run_triage([{"photos": [photo]}])
    assert photo["analysis_status"] == "complete"
    assert photo["analysis_reasons"] == ["no_qualifying_subject"]
    assert photo["label"] == "REVIEW"


@pytest.mark.parametrize("changed", ["none", "variant", "prompt", "file", "source", "threshold"])
def test_loader_checks_current_mask_inputs_and_settings(db, readiness_photo, changed):
    from computation_cache import source_input

    pid, mask_path, quality = readiness_photo
    _seed_detection_and_mask(db, readiness_photo)
    settings = {}
    if changed == "variant":
        settings["sam2_variant"] = "sam2-large"
    elif changed == "prompt":
        db.masks_features.upsert_mask(pid, "sam2-small", str(mask_path), "megadetector-v6",
                                     (.2, .1, .4, .4), quality)
    elif changed == "file":
        mask_path.unlink()
    elif changed == "source":
        db.model_runs.record_detector_run(pid, "megadetector-v6", 1, "r", source_input(
            "a" * 64, "vireo-detector-source-v1")[1])
        db.conn.execute("UPDATE photos SET file_hash=? WHERE id=?", ("b" * 64, pid))
    elif changed == "threshold":
        db.conn.execute("UPDATE detections SET detector_confidence=.1 WHERE photo_id=?", (pid,))
    photo = _loaded_photo(db, pid, **settings)
    run_triage([{"photos": [photo]}])
    if changed == "none":
        assert photo["selection_eligible"] is True
        assert photo["label"] == "KEEP"
    else:
        assert photo["selection_eligible"] is False
        assert photo["label"] == "REVIEW"
        assert photo["analysis_reasons"] == (["detection_stale"] if changed == "source" else
                                             ["no_qualifying_subject"] if changed == "threshold" else
                                             ["segmentation_stale"])


def test_readiness_and_review_reason_survive_serialization(db, readiness_photo):
    from pipeline import serialize_results

    pid, _, _ = readiness_photo
    photo = _loaded_photo(db, pid)
    encounters, photos = run_triage([{"photos": [photo]}])
    serialized = serialize_results({"encounters": encounters, "photos": photos, "summary": {}})
    assert serialized["photos"][0]["analysis_status"] == "not_run"
    assert serialized["photos"][0]["analysis_reasons"] == ["detection_not_run"]
    assert serialized["photos"][0]["label"] == "REVIEW"


def test_measured_zero_is_quality_evidence_not_unavailable_analysis():
    photo = complete_photo(subject_tenengrad=0., bg_tenengrad=0.)
    run_triage([{"photos": [photo]}])
    assert photo["analysis_status"] == "complete"
    assert photo["selection_eligible"] is True
    assert photo["label"] == "REJECT"
    assert photo["focus_score"] == 0.
    assert any(reason.startswith("out_of_focus") for reason in photo["reject_reasons"])


def test_current_selected_subject_can_supply_in_memory_mask():
    photo = complete_photo(mask_path=None, subject_mask_available=True, subject_tenengrad=0.)
    run_triage([{"photos": [photo]}])
    assert photo["analysis_status"] == "complete"
    assert photo["selection_eligible"] is True
    assert any(reason.startswith("out_of_focus") for reason in photo["reject_reasons"])
