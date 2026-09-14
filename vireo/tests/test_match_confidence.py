"""Tests for absolute match strength (match_confidence + its storage).

The property under test throughout: a softmax confidence ranks labels WITHIN a
list and can never say whether the list contained the right answer, so the raw
pre-softmax score has to survive to storage intact, and an uncalibrated model
must decline to judge rather than guess.
"""
import match_confidence as mc

_COSINE_CFG = {
    "match_thresholds": {
        "BioCLIP-2.5": {"threshold": 0.25, "score_kind": "cosine"},
    }
}
_DET = {"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.9,
        "category": "animal"}


def _photo(db, filename="DSC_0320.NEF"):
    """A folder + photo + detection, the minimum a prediction needs."""
    fid = db.add_folder("/photos", name="photos")
    pid = db.add_photo(
        folder_id=fid, filename=filename, extension=".nef",
        file_size=25_000_000, file_mtime=1_700_000_000.0,
        timestamp="2026-04-25T10:30:00", width=6000, height=4000,
    )
    det = db.save_detections(pid, [_DET], detector_model="MDV6")[0]
    return pid, det


# --------------------------------------------------------------------------
# assess()
# --------------------------------------------------------------------------

def test_below_threshold_is_unlisted():
    a = mc.assess("BioCLIP-2.5", "cosine", 0.21, 0.03, _COSINE_CFG)
    assert a["state"] == mc.UNLISTED
    assert mc.is_unlisted(a)
    assert "closest available label" in a["explanation"]


def test_above_threshold_is_listed():
    a = mc.assess("BioCLIP-2.5", "cosine", 0.31, 0.03, _COSINE_CFG)
    assert a["state"] == mc.LISTED
    assert not mc.is_unlisted(a)


def test_unconfigured_model_is_not_judged():
    """No threshold must mean no verdict — never a passing one."""
    a = mc.assess("iNat21 (EVA-02 Large)", "logit", 12.0, 1.0, _COSINE_CFG)
    assert a["state"] == mc.UNCALIBRATED
    assert not mc.is_unlisted(a)
    assert a["threshold"] is None


def test_score_kind_mismatch_refuses_to_compare():
    """A cosine floor applied to a logit would mark everything listed.

    12.0 is far above the 0.25 cosine floor, so a naive comparison would pass
    it. The scales are unrelated, so the only correct answer is to abstain.
    """
    a = mc.assess("BioCLIP-2.5", "logit", 12.0, 1.0, _COSINE_CFG)
    assert a["state"] == mc.UNCALIBRATED


def test_missing_score_is_unavailable():
    a = mc.assess("BioCLIP-2.5", "cosine", None, None, _COSINE_CFG)
    assert a["state"] == mc.UNAVAILABLE
    assert not mc.is_unlisted(a)


def test_malformed_threshold_entries_are_ignored():
    for bad in ({}, {"threshold": "high"}, {"threshold": 0.2},
                {"threshold": 0.2, "score_kind": "bananas"}):
        cfg = {"match_thresholds": {"M": bad}}
        assert mc.threshold_for("M", cfg) == (None, None)
        assert mc.assess("M", "cosine", 0.1, None, cfg)["state"] == mc.UNCALIBRATED


def test_non_finite_threshold_is_treated_as_uncalibrated():
    """NaN silently marks every scored run 'listed' (every comparison against
    NaN is false, and the else branch in ``assess`` picks up), +inf marks
    every run 'unlisted', and a hand-edited config carrying either value
    would misjudge every photo instead of abstaining. A model with a
    non-finite floor must degrade to ``uncalibrated`` the same way a missing
    or misspelled score kind does — no verdict beats a wrong verdict.
    """
    for bad in (float("nan"), float("inf"), float("-inf"), "nan", "inf",
                "-inf"):
        cfg = {"match_thresholds": {
            "BioCLIP-2.5": {"threshold": bad, "score_kind": "cosine"},
        }}
        assert mc.threshold_for("BioCLIP-2.5", cfg) == (None, None)
        # Both a would-be 'listed' score (0.9 > any real floor) and a
        # would-be 'unlisted' score (0.001 < any real floor) must land as
        # uncalibrated — the point is that the model is not judged at all.
        for score in (0.001, 0.9):
            assessment = mc.assess("BioCLIP-2.5", "cosine", score, None, cfg)
            assert assessment["state"] == mc.UNCALIBRATED
            assert not mc.is_unlisted(assessment)


def test_oversized_integer_threshold_is_treated_as_uncalibrated():
    """JSON integers are unbounded, and coercing 10**1000 to a C double
    raises ``OverflowError`` rather than returning inf. Without an explicit
    catch, a hand-edited config carrying such a value would turn the
    predictions and pipeline endpoints into 500s. The same "no verdict beats
    a wrong verdict" rule applies: degrade to uncalibrated, do not raise.
    """
    cfg = {"match_thresholds": {
        "BioCLIP-2.5": {"threshold": 10 ** 1000, "score_kind": "cosine"},
    }}
    assert mc.threshold_for("BioCLIP-2.5", cfg) == (None, None)
    assert mc.assess(
        "BioCLIP-2.5", "cosine", 0.5, None, cfg,
    )["state"] == mc.UNCALIBRATED


def test_malformed_threshold_container_is_treated_as_uncalibrated():
    """A hand-edited config or API-accepted workspace override that stores
    ``match_thresholds`` as anything other than a mapping (a string, a list,
    a number) used to raise ``AttributeError`` inside ``threshold_for`` when
    it reached ``.get(model)`` on the non-mapping value. Both the per-photo
    predictions endpoint and the Pipeline Inspector assess match states in
    the request, so an affected workspace turned every read into a 500.
    The right degrade is the same one a missing threshold takes: report
    ``uncalibrated`` and let the caller decide, never raise.
    """
    for bad in ("bad", ["BioCLIP-2.5"], 3, 3.14, True):
        cfg = {"match_thresholds": bad}
        assert mc.threshold_for("BioCLIP-2.5", cfg) == (None, None)
        assert mc.assess(
            "BioCLIP-2.5", "cosine", 0.9, None, cfg,
        )["state"] == mc.UNCALIBRATED


def test_cosine_threshold_outside_unit_interval_is_treated_as_uncalibrated():
    """Cosine similarity is bounded to ``[-1, 1]``; a floor at 5.0 cannot
    have been calibrated on real cosine scores, and applying it as-is
    would mark every real cosine 'unlisted' (5.0 > every valid cosine).
    Refuse the calibration rather than let a data-entry mistake silently
    condemn the whole catalog. Logit thresholds are unbounded on both
    sides — a class logit is routinely well above 1 — so the bound is
    keyed to the score kind, not applied universally.
    """
    bad_cos = {"match_thresholds": {
        "BioCLIP-2.5": {"threshold": 5.0, "score_kind": "cosine"},
    }}
    assert mc.threshold_for("BioCLIP-2.5", bad_cos) == (None, None)
    also_bad = {"match_thresholds": {
        "BioCLIP-2.5": {"threshold": -1.5, "score_kind": "cosine"},
    }}
    assert mc.threshold_for("BioCLIP-2.5", also_bad) == (None, None)

    # But an out-of-unit-interval logit floor is legitimate and stays.
    logit_cfg = {"match_thresholds": {
        "iNat21": {"threshold": 8.5, "score_kind": "logit"},
    }}
    assert mc.threshold_for("iNat21", logit_cfg) == (8.5, "logit")


# --------------------------------------------------------------------------
# summarize()
# --------------------------------------------------------------------------

def test_one_good_match_outvotes_a_bad_one():
    """A model that saw a bad crop must not condemn the whole photo."""
    bad = mc.assess("BioCLIP-2.5", "cosine", 0.10, None, _COSINE_CFG)
    good = mc.assess("BioCLIP-2.5", "cosine", 0.40, None, _COSINE_CFG)
    assert mc.summarize([bad, good])["state"] == mc.LISTED


def test_unanimous_failure_is_unlisted():
    bad = mc.assess("BioCLIP-2.5", "cosine", 0.10, None, _COSINE_CFG)
    summary = mc.summarize([bad, bad])
    assert summary["state"] == mc.UNLISTED
    assert summary["judged_models"] == 2


def test_unjudged_runs_do_not_vote():
    """Uncalibrated runs neither pass nor fail the photo."""
    unjudged = mc.assess("Other", "cosine", 0.10, None, _COSINE_CFG)
    assert mc.summarize([unjudged])["state"] == mc.UNCALIBRATED
    assert mc.summarize([])["state"] == mc.UNAVAILABLE

    good = mc.assess("BioCLIP-2.5", "cosine", 0.40, None, _COSINE_CFG)
    mixed = mc.summarize([good, unjudged])
    assert mixed["state"] == mc.LISTED
    assert mixed["judged_models"] == 1
    assert mixed["unjudged_models"] == 1


def test_uncalibrated_run_blocks_the_photo_wide_failure():
    """An unjudged model must not be conscripted into a unanimous failure.

    One calibrated model below its floor plus one model with no calibrated
    threshold is not "nothing here matches your list" — the uncalibrated model
    may have identified the bird perfectly and nobody looked. The photo-level
    verdict degrades to ``uncalibrated`` so the blanket banner stays off, while
    the failing run is still carried through for a warning that names it.
    """
    bad = mc.assess("BioCLIP-2.5", "cosine", 0.10, None, _COSINE_CFG)
    unjudged = mc.assess("Other", "cosine", 0.10, None, _COSINE_CFG)
    assert bad["state"] == mc.UNLISTED
    assert unjudged["state"] == mc.UNCALIBRATED

    summary = mc.summarize([bad, unjudged])
    assert summary["state"] == mc.UNCALIBRATED
    assert summary["judged_models"] == 1
    assert summary["unlisted_models"] == 1
    assert summary["unjudged_models"] == 1

    # Still unanimous-and-judged when the second model is genuinely absent
    # rather than deliberately unjudged.
    absent = mc.assess("BioCLIP-2.5", "cosine", None, None, _COSINE_CFG)
    assert absent["state"] == mc.UNAVAILABLE
    assert mc.summarize([bad, absent])["state"] == mc.UNLISTED


def test_photo_with_an_uncalibrated_model_keeps_the_per_run_failure():
    """End to end through summarize_photo: no banner, but the warning survives."""
    rows = [
        {"detection_id": 1, "classifier_model": "BioCLIP-2.5",
         "score_kind": "cosine", "max_match_score": 0.10},
        {"detection_id": 1, "classifier_model": "Other",
         "score_kind": "cosine", "max_match_score": 0.10},
    ]
    summary = mc.summarize_photo(rows, _COSINE_CFG)
    assert summary["state"] == mc.UNCALIBRATED
    assert [r["classifier_model"] for r in summary["unlisted_runs"]] == [
        "BioCLIP-2.5",
    ]


# --------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------

def test_prediction_round_trips_match_score(db):
    _, det = _photo(db)
    db.add_prediction(detection_id=det, species="Yellow-breasted Chat",
                           confidence=0.999, model="BioCLIP-2.5",
                           match_score=0.3142)
    row = db.conn.execute(
        "SELECT match_score FROM predictions WHERE detection_id = ?", (det,)
    ).fetchone()
    assert row["match_score"] == 0.3142


def test_match_score_defaults_to_null_not_zero(db):
    """Absent must stay distinguishable from 'matched nothing'."""
    _, det = _photo(db)
    db.add_prediction(detection_id=det, species="House Sparrow",
                           confidence=0.8, model="BioCLIP-2.5")
    row = db.conn.execute(
        "SELECT match_score FROM predictions WHERE detection_id = ?", (det,)
    ).fetchone()
    assert row["match_score"] is None


def test_replay_backfills_null_but_never_overwrites(db):
    """A replayed score (cache materialization, backfill) fills gaps only.

    The stored value is a real measurement this install already recorded; a
    later import re-stating it must not churn it.
    """
    _, det = _photo(db)
    kwargs = dict(detection_id=det, species="House Sparrow", confidence=0.8,
                  model="BioCLIP-2.5")
    db.add_prediction(**kwargs)
    db.add_prediction(**kwargs, match_score=0.30)
    score = db.conn.execute(
        "SELECT match_score FROM predictions WHERE detection_id = ?", (det,)
    ).fetchone()["match_score"]
    assert score == 0.30

    db.add_prediction(**kwargs, match_score=0.99)
    score = db.conn.execute(
        "SELECT match_score FROM predictions WHERE detection_id = ?", (det,)
    ).fetchone()["match_score"]
    assert score == 0.30


def test_fresh_inference_overwrites_a_prior_runtimes_score(db):
    """New weights under the same model name must not leave a stale score.

    The unique key is (detection, model, label list, species) — it does NOT
    include the runtime fingerprint, so a non-reclassify pass that re-infers
    because the runtime changed lands on the existing row. Keeping the old
    per-candidate score would contradict the run-level summary written by the
    new run.
    """
    _, det = _photo(db)
    kwargs = dict(detection_id=det, species="House Sparrow", confidence=0.8,
                  model="BioCLIP-2.5")
    db.add_prediction(**kwargs, match_score=0.30, from_fresh_inference=True)
    db.add_prediction(**kwargs, match_score=0.61, from_fresh_inference=True)
    score = db.conn.execute(
        "SELECT match_score FROM predictions WHERE detection_id = ?", (det,)
    ).fetchone()["match_score"]
    assert score == 0.61

    # ...and a replay afterwards still does not undo the fresh measurement.
    db.add_prediction(**kwargs, match_score=0.30)
    score = db.conn.execute(
        "SELECT match_score FROM predictions WHERE detection_id = ?", (det,)
    ).fetchone()["match_score"]
    assert score == 0.61


def test_match_score_recorded_for_a_run_with_no_predictions(db):
    """The run that matched nothing is the one worth recording.

    classifier_runs deliberately suppresses zero-prediction rows because it
    gates re-classification; classifier_match_scores must not inherit that.
    """
    photo_id, det = _photo(db)
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "fp1", max_match_score=0.11, match_margin=0.004,
        top_species="Rufous-winged Sparrow", label_count=1255,
        score_kind="cosine",
    )
    rows = db.get_match_scores_for_photo(photo_id)
    assert len(rows) == 1
    assert rows[0]["max_match_score"] == 0.11
    assert rows[0]["label_count"] == 1255
    assert db.conn.execute(
        "SELECT COUNT(*) c FROM predictions WHERE detection_id = ?", (det,)
    ).fetchone()["c"] == 0


def test_match_score_is_keyed_per_label_list(db):
    """Two lists on one detection are two rows, not an overwrite."""
    photo_id, det = _photo(db)
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "california", max_match_score=0.19,
        score_kind="cosine")
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "us-wide", max_match_score=0.36,
        score_kind="cosine")
    rows = db.get_match_scores_for_photo(photo_id)
    assert [r["max_match_score"] for r in rows] == [0.36, 0.19]


def test_rerunning_one_list_updates_in_place(db):
    photo_id, det = _photo(db)
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "california", max_match_score=0.19,
        score_kind="cosine")
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "california", max_match_score=0.22,
        score_kind="cosine")
    rows = db.get_match_scores_for_photo(photo_id)
    assert len(rows) == 1
    assert rows[0]["max_match_score"] == 0.22


def test_clear_predictions_clears_match_scores(db):
    """A stale 'nothing matched' verdict must not outlive its predictions."""
    photo_id, det = _photo(db)
    db.add_prediction(detection_id=det, species="House Sparrow",
                           confidence=0.8, model="BioCLIP-2.5",
                           labels_fingerprint="fp1", match_score=0.3)
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "fp1", max_match_score=0.3, score_kind="cosine")
    assert db.get_match_scores_for_photo(photo_id)

    db.clear_predictions()
    assert db.get_match_scores_for_photo(photo_id) == []


def test_clear_predictions_for_one_model_spares_the_other(db):
    photo_id, det = _photo(db)
    for model in ("BioCLIP-2.5", "iNat21 (EVA-02 Large)"):
        db.record_classifier_match_score(
            det, model, "fp1", max_match_score=0.3, score_kind="cosine")

    db.clear_predictions(model="BioCLIP-2.5")
    remaining = db.get_match_scores_for_photo(photo_id)
    assert [r["classifier_model"] for r in remaining] == ["iNat21 (EVA-02 Large)"]


# --------------------------------------------------------------------------
# API surfaces
# --------------------------------------------------------------------------

def test_pipeline_inspector_reports_runs_the_prediction_table_hides(app_and_db):
    """The inspector's own table excludes full-image and sub-threshold runs.

    That is the case this feature exists for — the species a user asks about
    usually came from exactly those runs — so the match-score payload must
    cover them even when `predictions` comes back empty.
    """
    app, db = app_and_db
    photo_id = db.get_photos()[0]["id"]
    weak = db.save_detections(
        photo_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4}, "confidence": 0.04,
          "category": "animal"}],
        detector_model="megadetector-v6",
    )[0]
    full = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.0,
          "category": "animal"}],
        detector_model="full-image",
    )[0]
    db.record_classifier_match_score(
        weak, "BioCLIP-2.5", "california", max_match_score=0.11,
        top_species="Common Nighthawk", score_kind="cosine")
    db.record_classifier_match_score(
        full, "BioCLIP-2.5", "california", max_match_score=0.31,
        top_species="Yellow-breasted Chat", score_kind="cosine")

    data = app.test_client().get(f"/api/photos/{photo_id}/pipeline").get_json()
    assert data["predictions"] == []
    reported = {
        (r["top_species"], r["detector_model"]) for r in data["match_scores"]
    }
    assert ("Yellow-breasted Chat", "full-image") in reported
    assert ("Common Nighthawk", "megadetector-v6") in reported


def test_pipeline_summary_uses_the_best_crop_per_model(app_and_db):
    """A bad crop must not outvote a good one from the same model."""
    app, db = app_and_db
    photo_id = db.get_photos()[0]["id"]
    for conf, score in ((0.04, 0.11), (0.9, 0.31)):
        det = db.save_detections(
            photo_id,
            [{"box": {"x": 0.1, "y": 0.1, "w": 0.3, "h": 0.4},
              "confidence": conf, "category": "animal"}],
            detector_model="megadetector-v6",
        )[0]
        db.record_classifier_match_score(
            det, "BioCLIP-2.5", "california", max_match_score=score,
            score_kind="cosine")

    data = app.test_client().get(f"/api/photos/{photo_id}/pipeline").get_json()
    # Uncalibrated by default, and the summary must say so rather than pass.
    assert data["match_summary"]["state"] == mc.UNCALIBRATED
    assert len(data["match_summary"]["assessments"]) == 1
    assert data["match_summary"]["assessments"][0]["max_match_score"] == 0.31


def test_predictions_api_reports_match_state_per_photo(app_and_db):
    app, db = app_and_db
    photo_id = db.get_photos()[0]["id"]
    det = db.save_detections(photo_id, [_DET], detector_model="MDV6")[0]
    db.add_prediction(detection_id=det, species="House Sparrow",
                      confidence=0.8, model="BioCLIP-2.5", match_score=0.11)
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "legacy", max_match_score=0.11, score_kind="cosine")

    data = app.test_client().get(
        f"/api/predictions?photo_ids={photo_id}"
    ).get_json()
    assert data["match_states"][str(photo_id)]["state"] == mc.UNCALIBRATED


def test_predictions_api_omits_match_states_for_queue_wide_requests(app_and_db):
    """Review asks for the whole workspace; it must not pay for this."""
    app, db = app_and_db
    data = app.test_client().get("/api/predictions").get_json()
    assert "match_states" not in data


def test_calibrated_threshold_reaches_the_api(app_and_db):
    """End-to-end: a configured floor turns into an 'unlisted' verdict."""
    import json as _json

    app, db = app_and_db
    db.conn.execute(
        "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
        (_json.dumps({"match_thresholds": {
            "BioCLIP-2.5": {"threshold": 0.25, "score_kind": "cosine"},
        }}), db._active_workspace_id),
    )
    db.conn.commit()

    photo_id = db.get_photos()[0]["id"]
    det = db.save_detections(photo_id, [_DET], detector_model="MDV6")[0]
    db.add_prediction(detection_id=det, species="Rufous-winged Sparrow",
                      confidence=0.99, model="BioCLIP-2.5", match_score=0.11)
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "legacy", max_match_score=0.11,
        top_species="Rufous-winged Sparrow", score_kind="cosine")

    client = app.test_client()
    pipeline = client.get(f"/api/photos/{photo_id}/pipeline").get_json()
    assert pipeline["match_summary"]["state"] == mc.UNLISTED
    # The 99% confidence is still reported — it is a true statement about the
    # ranking. What changes is that it no longer stands alone.
    assert pipeline["predictions"][0]["confidence"] == 0.99
    assert pipeline["predictions"][0]["match_score"] == 0.11

    preds = client.get(f"/api/predictions?photo_ids={photo_id}").get_json()
    assert preds["match_states"][str(photo_id)]["state"] == mc.UNLISTED


def test_summarize_photo_judges_only_each_models_best_run():
    rows = [
        {"classifier_model": "BioCLIP-2.5", "score_kind": "cosine",
         "max_match_score": 0.10, "match_margin": None},
        {"classifier_model": "BioCLIP-2.5", "score_kind": "cosine",
         "max_match_score": 0.40, "match_margin": None},
    ]
    summary = mc.summarize_photo(rows, _COSINE_CFG)
    assert summary["state"] == mc.LISTED
    assert summary["judged_models"] == 1


def test_summarize_photo_skips_rows_without_a_score():
    rows = [
        {"classifier_model": "BioCLIP-2.5", "score_kind": "cosine",
         "max_match_score": None, "match_margin": None},
    ]
    assert mc.summarize_photo(rows, _COSINE_CFG)["state"] == mc.UNAVAILABLE
    assert mc.summarize_photo([], _COSINE_CFG)["state"] == mc.UNAVAILABLE


# --------------------------------------------------------------------------
# A passing run must never silence a failing one
# --------------------------------------------------------------------------

_TWO_MODEL_CFG = {
    "match_thresholds": {
        "BioCLIP-2.5": {"threshold": 0.25, "score_kind": "cosine"},
        "iNat21": {"threshold": 10.0, "score_kind": "logit"},
    }
}


def test_a_failing_model_keeps_its_verdict_when_another_model_passes():
    """The photo-level rollup may be outvoted; the failing run may not vanish.

    Two models classify the same photo. BioCLIP clears its floor, iNat21 does
    not. The photo is not unidentifiable — so the rollup is `listed` — but the
    iNat21 predictions on screen matched nothing in their list, and rendering
    them beside a 99% with no warning is the exact failure this module exists
    to remove.
    """
    rows = [
        {"detection_id": 7, "classifier_model": "BioCLIP-2.5",
         "score_kind": "cosine", "max_match_score": 0.40},
        {"detection_id": 7, "classifier_model": "iNat21",
         "score_kind": "logit", "max_match_score": 3.0},
    ]
    summary = mc.summarize_photo(rows, _TWO_MODEL_CFG)
    assert summary["state"] == mc.LISTED
    assert summary["unlisted_models"] == 1
    failures = summary["unlisted_runs"]
    assert [(f["detection_id"], f["classifier_model"]) for f in failures] == [
        (7, "iNat21")
    ]
    assert "closest available label" in failures[0]["explanation"]


def test_two_detections_keep_separate_verdicts():
    """A photo can hold two species; one good subject does not vouch for the
    other. The rollup still says `listed` (this model did see something
    clearly), but detection 9's own run is carried through as failing."""
    rows = [
        {"detection_id": 8, "classifier_model": "BioCLIP-2.5",
         "score_kind": "cosine", "max_match_score": 0.40},
        {"detection_id": 9, "classifier_model": "BioCLIP-2.5",
         "score_kind": "cosine", "max_match_score": 0.11},
    ]
    summary = mc.summarize_photo(rows, _COSINE_CFG)
    assert summary["state"] == mc.LISTED
    assert len(summary["runs"]) == 2
    assert [f["detection_id"] for f in summary["unlisted_runs"]] == [9]


def test_unjudged_runs_never_surface_as_failures():
    """Uncalibrated is not a failure any more than it is a pass."""
    rows = [
        {"detection_id": 8, "classifier_model": "BioCLIP-2.5",
         "score_kind": "cosine", "max_match_score": 0.40},
        {"detection_id": 9, "classifier_model": "iNat21",
         "score_kind": "logit", "max_match_score": 3.0},
    ]
    summary = mc.summarize_photo(rows, _COSINE_CFG)
    assert summary["unlisted_runs"] == []
    assert {r["state"] for r in summary["runs"]} == {mc.LISTED, mc.UNCALIBRATED}


# --------------------------------------------------------------------------
# The verdict must describe the label list the predictions came from
# --------------------------------------------------------------------------

def _reclassified_against_two_lists(db):
    """One detection classified against a wide list, then a narrow one."""
    photo_id, det = _photo(db)
    db.add_prediction(detection_id=det, species="Yellow-breasted Chat",
                      confidence=0.99, model="BioCLIP-2.5",
                      labels_fingerprint="us-wide", match_score=0.40)
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "us-wide", max_match_score=0.40,
        top_species="Yellow-breasted Chat", score_kind="cosine")
    db.add_prediction(detection_id=det, species="Rufous-winged Sparrow",
                      confidence=0.99, model="BioCLIP-2.5",
                      labels_fingerprint="california", match_score=0.11)
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "california", max_match_score=0.11,
        top_species="Rufous-winged Sparrow", score_kind="cosine")
    return photo_id, det


def test_obsolete_label_list_cannot_certify_the_current_one(db):
    """The verdict must be built from the same list as the rows on screen.

    ``get_predictions`` pins to the newest fingerprint per (detection, model),
    so the panel shows the California run. Its 0.11 is under the floor. The
    abandoned US-wide run's 0.40 is still in the table — and must not be
    allowed to mark the list the user is actually looking at as matched.
    """
    photo_id, _det = _reclassified_against_two_lists(db)
    rows = db.get_match_scores_for_photo(photo_id)
    assert {r["labels_fingerprint"]: r["is_current"] for r in rows} == {
        "us-wide": 0, "california": 1,
    }
    summary = mc.summarize_photo(rows, _COSINE_CFG)
    assert summary["state"] == mc.UNLISTED
    assert [r["labels_fingerprint"] for r in summary["runs"]] == ["california"]


def test_superseded_runs_stay_visible_to_the_inspector(db):
    """History is not dropped — the per-run table exists to show it."""
    photo_id, _det = _reclassified_against_two_lists(db)
    rows = db.get_match_scores_for_photo(photo_id)
    assert len(rows) == 2


def test_a_run_with_no_predictions_is_still_current(db):
    """The zero-prediction run has nothing in ``predictions`` to pin against.

    It is also the single most informative run this table records, so it falls
    back to its own recency rather than being written off as superseded.
    """
    photo_id, det = _photo(db)
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "california", max_match_score=0.11,
        score_kind="cosine")
    rows = db.get_match_scores_for_photo(photo_id)
    assert [r["is_current"] for r in rows] == [1]
    assert mc.summarize_photo(rows, _COSINE_CFG)["state"] == mc.UNLISTED


def test_hand_built_rows_default_to_current():
    """Absent ``is_current`` means current, so a caller with no fingerprint
    context summarizes what it was given instead of silently nothing."""
    assert mc.is_current_row({"classifier_model": "BioCLIP-2.5"})
    assert not mc.is_current_row({"is_current": 0})


def test_pipeline_reports_superseded_runs_with_the_current_verdict(app_and_db):
    """End-to-end: the inspector lists both runs, the verdict follows the
    current one, and the failing run is attached to its detection."""
    import json as _json

    app, db = app_and_db
    db.conn.execute(
        "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
        (_json.dumps({"match_thresholds": {
            "BioCLIP-2.5": {"threshold": 0.25, "score_kind": "cosine"},
        }}), db._active_workspace_id),
    )
    db.conn.commit()
    photo_id = db.get_photos()[0]["id"]
    det = db.save_detections(photo_id, [_DET], detector_model="MDV6")[0]
    db.add_prediction(detection_id=det, species="Yellow-breasted Chat",
                      confidence=0.99, model="BioCLIP-2.5",
                      labels_fingerprint="us-wide", match_score=0.40)
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "us-wide", max_match_score=0.40,
        top_species="Yellow-breasted Chat", score_kind="cosine")
    db.add_prediction(detection_id=det, species="Rufous-winged Sparrow",
                      confidence=0.99, model="BioCLIP-2.5",
                      labels_fingerprint="california", match_score=0.11)
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "california", max_match_score=0.11,
        top_species="Rufous-winged Sparrow", score_kind="cosine")

    data = app.test_client().get(
        f"/api/photos/{photo_id}/pipeline"
    ).get_json()
    assert len(data["match_scores"]) == 2
    assert data["match_summary"]["state"] == mc.UNLISTED
    assert [r["detection_id"] for r in data["match_summary"]["unlisted_runs"]] \
        == [det]

    preds = app.test_client().get(
        f"/api/predictions?photo_ids={photo_id}"
    ).get_json()
    failures = preds["match_states"][str(photo_id)]["unlisted_runs"]
    assert [(f["detection_id"], f["classifier_model"]) for f in failures] == [
        (det, "BioCLIP-2.5")
    ]


def test_current_prediction_without_score_blocks_blanket_unlisted():
    """A prediction from a model with no match score must not be silenced.

    A migrated catalog carries current predictions from models that ran
    before ``classifier_match_scores`` existed. The panel still shows those
    predictions, so a "no label in this list matches" banner rendered over
    them would speak for a run that was never judged. The unscored current
    pair blocks the blanket ``unlisted`` verdict the same way an
    ``uncalibrated`` run does.
    """
    scored_rows = [
        {"detection_id": 7, "classifier_model": "BioCLIP-2.5",
         "score_kind": "cosine", "max_match_score": 0.11,
         "labels_fingerprint": "california"},
    ]
    unscored = [
        {"detection_id": 7, "classifier_model": "iNat21-legacy",
         "labels_fingerprint": "unknown"},
    ]
    summary = mc.summarize_photo(
        scored_rows, _COSINE_CFG, unscored_current_runs=unscored,
    )
    assert summary["state"] == mc.UNCALIBRATED
    # The failing run is still on the record for the UI to warn on
    assert [r["classifier_model"] for r in summary["runs"]] == [
        "BioCLIP-2.5", "iNat21-legacy",
    ]


def test_unscored_current_pair_alone_summarizes_as_unavailable():
    """A photo whose only run is a pre-migration prediction is not judged.

    No scored run has said "unlisted" — the panel just shows predictions from
    a model that was never judged. The state is ``unavailable`` rather than
    ``uncalibrated`` because nothing was measured: the pill for this photo has
    to say "match strength not recorded", not "recorded, not judged".
    """
    unscored = [
        {"detection_id": 7, "classifier_model": "iNat21-legacy",
         "labels_fingerprint": "unknown"},
    ]
    summary = mc.summarize_photo([], _COSINE_CFG, unscored_current_runs=unscored)
    assert summary["state"] == mc.UNAVAILABLE
    assert summary["unjudged_models"] == 1


def test_unscored_pair_ignored_when_it_duplicates_a_scored_row():
    """An overzealous caller that hands us both must not double-count."""
    scored = [
        {"detection_id": 7, "classifier_model": "BioCLIP-2.5",
         "score_kind": "cosine", "max_match_score": 0.40,
         "labels_fingerprint": "california"},
    ]
    unscored = [
        {"detection_id": 7, "classifier_model": "BioCLIP-2.5",
         "labels_fingerprint": "california"},
    ]
    summary = mc.summarize_photo(
        scored, _COSINE_CFG, unscored_current_runs=unscored,
    )
    assert summary["state"] == mc.LISTED
    assert [r["classifier_model"] for r in summary["runs"]] == [
        "BioCLIP-2.5",
    ]


def test_unscored_pair_blocks_blanket_verdict_even_when_model_has_scored_run():
    """A different-detection unscored run must still block the blanket verdict.

    A photo with two detections classified by the same model — one detection
    scored and failing, one detection carried over from before match scoring
    existed — used to summarize as ``unlisted`` because the model was already
    in ``best`` from the scored run. Browse would then paint its "no label
    matches" banner over the legacy prediction that no one ever judged
    (Codex P1 on f074d0c). Each unscored current run stands for its own
    detection and must block the rollup regardless of which other detections
    of the same model got a score.
    """
    scored = [
        {"detection_id": 7, "classifier_model": "BioCLIP-2.5",
         "score_kind": "cosine", "max_match_score": 0.11,
         "labels_fingerprint": "california"},
    ]
    unscored = [
        {"detection_id": 8, "classifier_model": "BioCLIP-2.5",
         "labels_fingerprint": "california"},
    ]
    summary = mc.summarize_photo(
        scored, _COSINE_CFG, unscored_current_runs=unscored,
    )
    assert summary["state"] == mc.UNCALIBRATED
    assert summary["unjudged_models"] == 1
    # Both runs must show up so the UI can attach a warning to the failing
    # scored detection and refuse to over-report the unscored one.
    assert sorted(
        (r["detection_id"], r["state"]) for r in summary["runs"]
    ) == [(7, mc.UNLISTED), (8, mc.UNAVAILABLE)]


def test_get_unscored_current_prediction_runs_matches_migrated_shape(db):
    """The DB helper names exactly the pairs summarize_photo must be told about.

    A migrated catalog leaves prior predictions in the table with no matching
    ``classifier_match_scores`` row. That is the query this helper answers.
    """
    photo_id, det = _photo(db)
    db.add_prediction(
        detection_id=det, species="Rufous-winged Sparrow",
        confidence=0.99, model="Legacy-Model",
        labels_fingerprint="pre-migration",
    )
    # A scored row on a different model / detection must not appear.
    db.add_prediction(
        detection_id=det, species="Yellow-breasted Chat",
        confidence=0.99, model="BioCLIP-2.5",
        labels_fingerprint="california", match_score=0.40,
    )
    db.record_classifier_match_score(
        det, "BioCLIP-2.5", "california", max_match_score=0.40,
        top_species="Yellow-breasted Chat", score_kind="cosine",
    )
    pairs = db.get_unscored_current_prediction_runs(photo_id)
    assert [(p["detection_id"], p["classifier_model"]) for p in pairs] == [
        (det, "Legacy-Model")
    ]


# --------------------------------------------------------------------------
# The invariant itself, rather than one more face of it
# --------------------------------------------------------------------------

# Every distinguishable shape a match-score row can take. Each gets its own
# detection so no entry de-dupes against another.
_ROW_SHAPES = [
    # Current, scored, failing — the only shape that may vote ``unlisted``.
    ("scored_fail", {
        "detection_id": 1, "classifier_model": "BioCLIP-2.5",
        "score_kind": "cosine", "max_match_score": 0.11,
        "labels_fingerprint": "california",
    }),
    # Current, scored, passing.
    ("scored_pass", {
        "detection_id": 2, "classifier_model": "BioCLIP-2.5",
        "score_kind": "cosine", "max_match_score": 0.40,
        "labels_fingerprint": "california",
    }),
    # Current and measured, but with no floor to judge the scale against.
    ("uncalibrated", {
        "detection_id": 3, "classifier_model": "iNat21",
        "score_kind": "logit", "max_match_score": 12.0,
        "labels_fingerprint": "california",
    }),
    # Current, displayed, and never measured: the row exists but carries no
    # ``max_match_score``.
    ("row_without_score", {
        "detection_id": 4, "classifier_model": "BioCLIP-2.5",
        "score_kind": "cosine", "max_match_score": None,
        "labels_fingerprint": "california",
    }),
    # Superseded: the label list these ran against is gone from the panel, so
    # neither shape may vote or block.
    ("superseded_fail", {
        "detection_id": 5, "classifier_model": "BioCLIP-2.5",
        "score_kind": "cosine", "max_match_score": 0.11,
        "labels_fingerprint": "arizona", "is_current": 0,
    }),
    ("superseded_without_score", {
        "detection_id": 6, "classifier_model": "BioCLIP-2.5",
        "score_kind": "cosine", "max_match_score": None,
        "labels_fingerprint": "arizona", "is_current": 0,
    }),
]

# A displayed run with no ``classifier_match_scores`` row at all.
_UNSCORED_PAIR = {
    "detection_id": 7, "classifier_model": "Legacy-Model",
    "labels_fingerprint": "pre-migration",
}


def test_unlisted_requires_every_displayed_run_to_be_scored_and_failed():
    """The invariant, over every mix — not one more case for one more face.

    "No label in this list matches this photo" is painted as a banner across
    every prediction on screen, so it is honest only when every run whose
    predictions are on screen was measured, judged, and failed. Three rounds
    of review have each found a different way for an unjudged run to go
    missing from the rollup: uncalibrated runs, unscored siblings on another
    detection, and rows that exist with a NULL score. All three are the same
    bug — absence of a verdict rendering as a verdict. Asserting the property
    over the whole cross-product of row shapes, rather than enumerating
    faces, is what makes face number four fail here instead of shipping.
    """
    import itertools

    for mask in itertools.product((False, True), repeat=len(_ROW_SHAPES)):
        for with_unscored_pair in (False, True):
            rows = [
                dict(row)
                for (_name, row), keep in zip(_ROW_SHAPES, mask, strict=True) if keep
            ]
            unscored = [dict(_UNSCORED_PAIR)] if with_unscored_pair else []
            summary = mc.summarize_photo(
                rows, _COSINE_CFG, unscored_current_runs=unscored,
            )
            # ``runs`` is exactly what the panel is displaying: current rows
            # plus current predictions that have no row.
            displayed = summary["runs"]
            selected = [
                name for (name, _row), keep in zip(_ROW_SHAPES, mask, strict=True) if keep
            ]
            context = f"rows={selected} unscored_pair={with_unscored_pair}"

            if summary["state"] == mc.UNLISTED:
                assert displayed, f"unlisted with nothing displayed ({context})"
                for run in displayed:
                    assert run["max_match_score"] is not None, (
                        f"blanket unlisted over an unmeasured run ({context})"
                    )
                    assert run["state"] == mc.UNLISTED, (
                        f"blanket unlisted over a {run['state']} run ({context})"
                    )

            # The other half. Blocking correctly is only part of the job:
            # refusing to speak when every displayed run was measured, judged
            # and failed would be its own kind of dishonesty.
            if displayed and all(r["state"] == mc.UNLISTED for r in displayed):
                assert summary["state"] == mc.UNLISTED, (
                    f"every displayed run failed but the photo is "
                    f"{summary['state']} ({context})"
                )


def test_row_present_without_a_score_blocks_the_blanket_verdict():
    """The face of the invariant found in round 8.

    ``get_unscored_current_prediction_runs`` finds displayed runs with NO
    ``classifier_match_scores`` row. A row that exists but carries a NULL
    ``max_match_score`` — an older model given a row later, or a run
    materialized from an artifact published without a measurement — satisfies
    that query's ``NOT EXISTS`` and is invisible to it. ``summarize_photo``
    used to skip the row outright, so a failing sibling carried the photo to
    ``unlisted`` and Browse painted "no label matches" over a prediction
    nobody had judged (Codex P1 on ecb275c).
    """
    rows = [
        {"detection_id": 1, "classifier_model": "BioCLIP-2.5",
         "score_kind": "cosine", "max_match_score": 0.11,
         "labels_fingerprint": "california"},
        {"detection_id": 2, "classifier_model": "BioCLIP-2.5",
         "score_kind": "cosine", "max_match_score": None,
         "labels_fingerprint": "california"},
    ]
    summary = mc.summarize_photo(rows, _COSINE_CFG)
    assert summary["state"] != mc.UNLISTED
    assert summary["unjudged_models"] == 1
    # The measured failure is still carried through for the UI to warn on.
    assert [r["detection_id"] for r in summary["unlisted_runs"]] == [1]


def test_superseded_row_without_a_score_does_not_block():
    """Blocking is for displayed runs only.

    A row from a label list the user has replaced produced nothing that is on
    screen, so its missing score is not evidence about the list that is.
    Treating it as a blocker would mute the blanket verdict permanently on
    any detection that has ever been re-classified.
    """
    rows = [
        {"detection_id": 1, "classifier_model": "BioCLIP-2.5",
         "score_kind": "cosine", "max_match_score": 0.11,
         "labels_fingerprint": "california"},
        {"detection_id": 1, "classifier_model": "BioCLIP-2.5",
         "score_kind": "cosine", "max_match_score": None,
         "labels_fingerprint": "arizona", "is_current": 0},
    ]
    assert mc.summarize_photo(rows, _COSINE_CFG)["state"] == mc.UNLISTED


def test_unmeasured_only_photo_says_not_recorded_not_uncalibrated():
    """The two "unjudged" pills are different claims and must not be swapped.

    ``uncalibrated`` renders as "match strength recorded, not judged" and
    offers the calibration script. For a run whose strength was never
    recorded both halves are false: calibrating changes nothing, and the
    honest statement is that nothing was measured.
    """
    unscored = [
        {"detection_id": 7, "classifier_model": "Legacy-Model",
         "labels_fingerprint": "pre-migration"},
    ]
    assert mc.summarize_photo(
        [], _COSINE_CFG, unscored_current_runs=unscored,
    )["state"] == mc.UNAVAILABLE
    # A run that WAS measured but has no floor is the other claim, and keeps
    # the uncalibrated pill that points at the calibration script.
    rows = [
        {"detection_id": 3, "classifier_model": "iNat21",
         "score_kind": "logit", "max_match_score": 12.0,
         "labels_fingerprint": "california"},
    ]
    assert mc.summarize_photo(rows, _COSINE_CFG)["state"] == mc.UNCALIBRATED
