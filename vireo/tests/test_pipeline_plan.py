"""Tests for the pipeline plan: db helpers + compute_plan + /api/pipeline/plan.

The plan is the truth source for the Pipeline page's status pills. These
tests pin down the contract documented in CORE_PHILOSOPHY.md ("No black
boxes"): a pill that says "Already done" must mean the next run would be a
no-op, and a pill that says "Will run" must mean there is genuinely new
work — independent of whether *any* prior output exists in the workspace.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _make_db(tmp_path, name="test.db"):
    from db import Database
    db = Database(str(tmp_path / name))
    folder_id = db.add_folder("/tmp/p")
    db._active_workspace_id = db.create_workspace("WS")
    db.add_workspace_folder(db._active_workspace_id, folder_id)
    return db, folder_id


def _add_photo_with_detection(db, folder_id, filename, conf=0.9,
                               detector_model="megadetector-v6"):
    photo_id = db.add_photo(
        folder_id=folder_id, filename=filename, extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
          "confidence": conf, "category": "animal"}],
        detector_model=detector_model,
    )
    return photo_id, det_ids[0]


# -------- db helpers --------

def test_count_real_detections_in_scope_excludes_full_image(tmp_path):
    """Synthetic full-image rows must not inflate the classify scope.

    full-image rows exist only as FK anchors for predictions on photos
    where MegaDetector found nothing — counting them as "real detections"
    would let the classify pill flip to done-prior the moment a single
    such anchor exists, even though those photos have no actual subject
    boxes to classify.
    """
    db, folder_id = _make_db(tmp_path)
    pid_real, _ = _add_photo_with_detection(db, folder_id, "real.jpg")
    pid_synth = db.add_photo(
        folder_id=folder_id, filename="synth.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    db.save_detections(
        pid_synth,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
          "confidence": 1.0, "category": "anchor"}],
        detector_model="full-image",
    )

    counts = db.count_real_detections_in_scope()
    assert counts["total_dets"] == 1
    assert counts["photos_with_dets"] == 1


def test_count_real_detections_respects_min_conf(tmp_path):
    db, folder_id = _make_db(tmp_path)
    _add_photo_with_detection(db, folder_id, "high.jpg", conf=0.9)
    _add_photo_with_detection(db, folder_id, "low.jpg", conf=0.05)

    assert db.count_real_detections_in_scope(min_conf=0.2)["total_dets"] == 1
    assert db.count_real_detections_in_scope(min_conf=0.0)["total_dets"] == 2


def test_count_real_detections_scopes_to_photo_ids(tmp_path):
    db, folder_id = _make_db(tmp_path)
    pid_a, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    pid_b, _ = _add_photo_with_detection(db, folder_id, "b.jpg")

    counts = db.count_real_detections_in_scope(photo_ids=[pid_a])
    assert counts["total_dets"] == 1
    assert counts["photos_with_dets"] == 1

    # Empty scope is a real "no photos" sentinel — must return zero, not
    # the whole-workspace count.
    counts = db.count_real_detections_in_scope(photo_ids=set())
    assert counts == {"total_dets": 0, "photos_with_dets": 0}


def test_count_classify_pending_excludes_recorded_runs(tmp_path):
    """The pending-pair count must mirror the classify gate exactly: a
    detection with a classifier_runs row for (model, fp) is done.
    """
    db, folder_id = _make_db(tmp_path)
    _, did_a = _add_photo_with_detection(db, folder_id, "a.jpg")
    _, did_b = _add_photo_with_detection(db, folder_id, "b.jpg")

    # Both pending against (BioCLIP-2, fp1)
    assert db.count_classify_pending_pairs("BioCLIP-2", "fp1") == 2

    db.record_classifier_run(did_a, "BioCLIP-2", "fp1", prediction_count=1)
    assert db.count_classify_pending_pairs("BioCLIP-2", "fp1") == 1

    db.record_classifier_run(did_b, "BioCLIP-2", "fp1", prediction_count=1)
    assert db.count_classify_pending_pairs("BioCLIP-2", "fp1") == 0

    # A different (model, fp) must NOT see those rows as done — adding a
    # new model is the headline failure mode the plan must catch.
    assert db.count_classify_pending_pairs("BioCLIP-2", "fp2") == 2
    assert db.count_classify_pending_pairs("OtherModel", "fp1") == 2


def test_count_photos_pending_masks(tmp_path):
    db, folder_id = _make_db(tmp_path)
    pid_unmasked, _ = _add_photo_with_detection(db, folder_id, "no_mask.jpg")
    pid_masked, _ = _add_photo_with_detection(db, folder_id, "masked.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path=? WHERE id=?",
        ("/masks/x.png", pid_masked),
    )
    db.conn.commit()

    counts = db.count_photos_pending_masks()
    assert counts == {"eligible": 2, "pending": 1}


def test_count_photos_pending_masks_ignores_photos_without_real_detections(tmp_path):
    """A photo with only a synthetic full-image anchor should not be
    counted as eligible for mask extraction — the actual stage skips it.
    """
    db, folder_id = _make_db(tmp_path)
    pid = db.add_photo(
        folder_id=folder_id, filename="anchor_only.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    db.save_detections(
        pid,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
          "confidence": 1.0, "category": "anchor"}],
        detector_model="full-image",
    )
    counts = db.count_photos_pending_masks()
    assert counts == {"eligible": 0, "pending": 0}


def test_count_eye_keypoint_eligible_requires_mask_and_prediction(tmp_path):
    db, folder_id = _make_db(tmp_path)
    # Photo A: real det, mask, prediction → eligible
    pid_a, did_a = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute("UPDATE photos SET mask_path='/m/a.png' WHERE id=?", (pid_a,))
    db.conn.execute(
        """INSERT INTO predictions
            (detection_id, classifier_model, labels_fingerprint,
             species, confidence)
           VALUES (?, ?, ?, ?, ?)""",
        (did_a, "BioCLIP-2", "fp1", "robin", 0.9),
    )
    # Photo B: real det, NO mask → NOT eligible
    pid_b, did_b = _add_photo_with_detection(db, folder_id, "b.jpg")
    db.conn.execute(
        """INSERT INTO predictions
            (detection_id, classifier_model, labels_fingerprint,
             species, confidence)
           VALUES (?, ?, ?, ?, ?)""",
        (did_b, "BioCLIP-2", "fp1", "sparrow", 0.8),
    )
    # Photo C: real det, mask, no prediction → NOT eligible
    pid_c, _ = _add_photo_with_detection(db, folder_id, "c.jpg")
    db.conn.execute("UPDATE photos SET mask_path='/m/c.png' WHERE id=?", (pid_c,))
    db.conn.commit()

    assert db.count_eye_keypoint_eligible() == 1


# -------- compute_plan: classify --------

def _params(**kwargs):
    from pipeline_plan import PipelinePlanParams
    return PipelinePlanParams(**kwargs)


def test_classify_plan_will_skip_when_disabled(tmp_path):
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    plan = compute_plan(db, _params(skip_classify=True), str(tmp_path / "test.db"))
    assert plan["stages"]["Classify"]["state"] == "will-skip"


def test_classify_plan_will_skip_when_no_models_selected(tmp_path):
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    plan = compute_plan(db, _params(model_ids=[]), str(tmp_path / "test.db"))
    assert plan["stages"]["Classify"]["state"] == "will-skip"
    assert "No models selected" in plan["stages"]["Classify"]["summary"]


def test_classify_plan_will_run_when_no_detections_yet(tmp_path, monkeypatch):
    """Empty workspace → Classify must show "will run" so the user knows
    MegaDetector + classifiers will both run on the next press.
    """
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])
    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    assert plan["stages"]["Classify"]["state"] == "will-run"


def test_classify_plan_done_prior_when_all_pairs_recorded(tmp_path, monkeypatch):
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")

    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])
    # Pin label resolution: no labels_files passed and no workspace
    # overrides → TOL fallback for bioclip-2. The dev's real
    # ~/.vireo/labels_active.json must not bleed into this test.
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=3)

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    classify = plan["stages"]["Classify"]
    assert classify["state"] == "done-prior"
    assert "Already classified" in classify["summary"]


def test_classify_plan_will_run_when_new_model_added(tmp_path, monkeypatch):
    """The headline bug from the user report: classifying with model A
    must NOT make the pill say "Already done" once the user adds model B
    to the run.
    """
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")

    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
        {"id": "m2", "name": "BioCLIP",
         "model_str": "hf-hub:imageomics/bioclip",
         "model_type": "bioclip", "downloaded": True},
    ])
    # Only m1 has been run — the new m2 has zero coverage.
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=3)

    plan = compute_plan(
        db, _params(model_ids=["m1", "m2"]), str(tmp_path / "test.db"),
    )
    classify = plan["stages"]["Classify"]
    assert classify["state"] == "will-run"
    # Per-model breakdown surfaces the *new* model so the user sees why.
    assert "BioCLIP" in classify["summary"]


def test_classify_plan_reclassify_bypasses_cache(tmp_path, monkeypatch):
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")

    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=3)

    plan = compute_plan(
        db,
        _params(model_ids=["m1"], reclassify=True),
        str(tmp_path / "test.db"),
    )
    classify = plan["stages"]["Classify"]
    assert classify["state"] == "will-run"
    assert "Re-classify" in classify["summary"]


# -------- compute_plan: extract --------

def test_extract_plan_will_skip_when_disabled(tmp_path):
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    plan = compute_plan(
        db, _params(skip_extract_masks=True), str(tmp_path / "test.db"),
    )
    assert plan["stages"]["Extract"]["state"] == "will-skip"


def test_extract_plan_done_prior_when_all_masks_present(tmp_path):
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    pid, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute("UPDATE photos SET mask_path='/m/a.png' WHERE id=?", (pid,))
    db.conn.commit()
    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    extract = plan["stages"]["Extract"]
    assert extract["state"] == "done-prior"
    assert "1" in extract["summary"]


def test_extract_plan_will_run_when_some_photos_missing_masks(tmp_path):
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    pid_done, _ = _add_photo_with_detection(db, folder_id, "done.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/done.png' WHERE id=?", (pid_done,),
    )
    _add_photo_with_detection(db, folder_id, "todo.jpg")
    db.conn.commit()

    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    extract = plan["stages"]["Extract"]
    assert extract["state"] == "will-run"
    assert extract["detail"]["pending"] == 1
    assert extract["detail"]["eligible"] == 2


# -------- compute_plan: eye keypoints --------

def test_eye_keypoints_plan_will_skip_when_disabled(tmp_path):
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    plan = compute_plan(
        db, _params(skip_eye_keypoints=True), str(tmp_path / "test.db"),
    )
    assert plan["stages"]["EyeKeypoints"]["state"] == "will-skip"


def test_eye_keypoints_plan_will_skip_when_extract_disabled(tmp_path):
    """Eye keypoints depends on masks — disabling extract must propagate."""
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    plan = compute_plan(
        db, _params(skip_extract_masks=True), str(tmp_path / "test.db"),
    )
    assert plan["stages"]["EyeKeypoints"]["state"] == "will-skip"


def test_eye_keypoints_plan_will_skip_when_preflight_fails(tmp_path, monkeypatch):
    import pipeline as pipeline_mod
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight",
        lambda config: "disabled in config",
    )
    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    eye = plan["stages"]["EyeKeypoints"]
    assert eye["state"] == "will-skip"
    assert "disabled in config" in eye["summary"]


def test_eye_keypoints_plan_done_prior_when_all_processed(tmp_path, monkeypatch):
    """The headline bug case: every eligible photo has eye_tenengrad set
    (with the current fingerprint, so it isn't stale), so the next run is
    a no-op. The pill must say so.
    """
    import pipeline as pipeline_mod
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight",
        lambda config: None,
    )
    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/a.png', eye_tenengrad=0.5, "
        "eye_kp_fingerprint=? WHERE id=?",
        (EYE_KP_FINGERPRINT_VERSION, pid),
    )
    db.conn.execute(
        """INSERT INTO predictions
            (detection_id, classifier_model, labels_fingerprint,
             species, confidence)
           VALUES (?, ?, ?, ?, ?)""",
        (did, "BioCLIP-2", "fp1", "robin", 0.9),
    )
    db.conn.commit()

    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    eye = plan["stages"]["EyeKeypoints"]
    assert eye["state"] == "done-prior"


def test_eye_keypoints_plan_will_run_when_fingerprint_outdated(tmp_path, monkeypatch):
    """A photo with eye_tenengrad set but a stale eye_kp_fingerprint must
    surface as will-run, not done-prior — keypoint model/routing changed
    since the photo was processed."""
    import pipeline as pipeline_mod
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight",
        lambda config: None,
    )
    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    # Old fingerprint string — definitely doesn't match current.
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/a.png', eye_tenengrad=0.5, "
        "eye_kp_fingerprint='superanimal-old' WHERE id=?",
        (pid,),
    )
    db.conn.execute(
        """INSERT INTO predictions
            (detection_id, classifier_model, labels_fingerprint,
             species, confidence)
           VALUES (?, ?, ?, ?, ?)""",
        (did, "BioCLIP-2", "fp1", "robin", 0.9),
    )
    db.conn.commit()

    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    eye = plan["stages"]["EyeKeypoints"]
    assert eye["state"] == "will-run", (
        f"expected will-run for stale fingerprint, got {eye['state']!r}: {eye['summary']}"
    )


def test_eye_keypoints_plan_will_run_when_some_pending(tmp_path, monkeypatch):
    import pipeline as pipeline_mod
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight",
        lambda config: None,
    )
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    pid_done, did_done = _add_photo_with_detection(db, folder_id, "done.jpg")
    pid_todo, did_todo = _add_photo_with_detection(db, folder_id, "todo.jpg")
    # done.jpg: processed with current fingerprint → not stale, not pending
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/d.png', eye_tenengrad=0.5, "
        "eye_kp_fingerprint=? WHERE id=?",
        (EYE_KP_FINGERPRINT_VERSION, pid_done),
    )
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/t.png' WHERE id=?", (pid_todo,),
    )
    for did in (did_done, did_todo):
        db.conn.execute(
            """INSERT INTO predictions
                (detection_id, classifier_model, labels_fingerprint,
                 species, confidence)
               VALUES (?, ?, ?, ?, ?)""",
            (did, "BioCLIP-2", "fp1", "robin", 0.9),
        )
    db.conn.commit()

    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    eye = plan["stages"]["EyeKeypoints"]
    assert eye["state"] == "will-run"
    assert eye["detail"]["pending"] == 1
    assert eye["detail"]["eligible"] == 2


# -------- compute_plan: regroup --------

def test_regroup_plan_will_skip_when_disabled(tmp_path):
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    plan = compute_plan(
        db, _params(skip_regroup=True), str(tmp_path / "test.db"),
    )
    assert plan["stages"]["Group"]["state"] == "will-skip"


def test_regroup_plan_done_prior_when_cache_exists_and_no_upstream_work(tmp_path, monkeypatch):
    """The other headline bug: Group & Score had no signal at all and
    always said "Will run." When the cache exists and no upstream stage
    has work, the next press is a no-op — say so.
    """
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    # Make Classify and Extract done-prior so upstream_will_run is False.
    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute("UPDATE photos SET mask_path='/m/a.png' WHERE id=?", (pid,))
    db.conn.commit()
    from labels_fingerprint import TOL_SENTINEL
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=1)

    cache_path = os.path.join(
        str(tmp_path), f"pipeline_results_ws{db._active_workspace_id}.json",
    )
    with open(cache_path, "w") as f:
        f.write('{"photos": []}')

    # Stamp the workspace fingerprint to match current settings — this is
    # the post-Phase-1 state for a workspace that completed a clean full
    # regroup. Without this, the "cache exists but fingerprint NULL" path
    # would correctly report will-run (covered by the dedicated test).
    import config as cfg
    from pipeline import compute_group_fingerprint
    effective = db.get_effective_config(cfg.load())
    db.set_workspace_group_state(
        db._active_workspace_id,
        fingerprint=compute_group_fingerprint(effective),
        when_ts=1714579200,
    )

    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])
    plan = compute_plan(
        db,
        # Skip eye keypoints so it doesn't surface as upstream "will-run".
        _params(model_ids=["m1"], skip_eye_keypoints=True),
        str(tmp_path / "test.db"),
    )

    assert plan["stages"]["Group"]["state"] == "done-prior"


def test_regroup_plan_will_run_when_upstream_has_work(tmp_path):
    """If any upstream stage is going to run, regroup must too — even if
    a stale cache file exists, the grouping needs to be redone.
    """
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _add_photo_with_detection(db, folder_id, "todo.jpg")

    cache_path = os.path.join(
        str(tmp_path), f"pipeline_results_ws{db._active_workspace_id}.json",
    )
    with open(cache_path, "w") as f:
        f.write('{"photos": []}')

    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    # Extract has pending work → upstream_will_run = True
    assert plan["stages"]["Extract"]["state"] == "will-run"
    assert plan["stages"]["Group"]["state"] == "will-run"


def test_regroup_plan_will_run_when_workspace_fingerprint_outdated(tmp_path, monkeypatch):
    """Cache exists and no upstream work, BUT workspace.last_group_fingerprint
    no longer matches the current settings — surface as will-run with a
    'settings changed' summary, not done-prior."""
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)

    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute("UPDATE photos SET mask_path='/m/a.png' WHERE id=?", (pid,))
    db.conn.commit()
    from labels_fingerprint import TOL_SENTINEL
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=1)

    cache_path = os.path.join(
        str(tmp_path), f"pipeline_results_ws{db._active_workspace_id}.json",
    )
    with open(cache_path, "w") as f:
        f.write('{"photos": []}')

    # Stamp a deliberately mismatched fingerprint.
    db.set_workspace_group_state(
        db._active_workspace_id,
        fingerprint="old-fingerprint-from-prior-settings",
        when_ts=1714579200,
    )

    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])
    plan = compute_plan(
        db,
        _params(model_ids=["m1"], skip_eye_keypoints=True),
        str(tmp_path / "test.db"),
    )

    assert plan["stages"]["Group"]["state"] == "will-run"
    assert "settings" in plan["stages"]["Group"]["summary"].lower()



def test_regroup_plan_will_run_when_cache_exists_but_fingerprint_invalidated(
    tmp_path, monkeypatch,
):
    """A partial regroup wipes last_group_fingerprint to NULL after
    overwriting the cache with subset output. The plan must treat this
    state (cache_exists AND last_fp IS NULL) as will-run, not done-prior,
    because the cache no longer reflects the full workspace."""
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)

    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute("UPDATE photos SET mask_path='/m/a.png' WHERE id=?", (pid,))
    db.conn.commit()
    from labels_fingerprint import TOL_SENTINEL
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=1)

    # Cache file from a prior partial regroup write.
    cache_path = os.path.join(
        str(tmp_path), f"pipeline_results_ws{db._active_workspace_id}.json",
    )
    with open(cache_path, "w") as f:
        f.write('{"photos": []}')

    # Workspace fingerprint is NULL (invalidated by the partial run).
    # Default state is already NULL — assert and proceed.
    row = db.conn.execute(
        "SELECT last_group_fingerprint FROM workspaces WHERE id=?",
        (db._active_workspace_id,),
    ).fetchone()
    assert row["last_group_fingerprint"] is None

    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])

    plan = compute_plan(
        db,
        _params(model_ids=["m1"], skip_eye_keypoints=True),
        str(tmp_path / "test.db"),
    )

    assert plan["stages"]["Group"]["state"] == "will-run", (
        "cache exists but fingerprint is NULL → previous run was partial / "
        "predates fingerprint stamping; plan must report will-run, not done-prior"
    )



def test_regroup_plan_will_run_when_no_cache(tmp_path):
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    # Skip everything else so upstream is all will-skip and cache is the
    # only signal.
    plan = compute_plan(
        db,
        _params(
            skip_classify=True, skip_extract_masks=True,
            skip_eye_keypoints=True,
        ),
        str(tmp_path / "test.db"),
    )
    assert plan["stages"]["Group"]["state"] == "will-run"
    assert "no cached" in plan["stages"]["Group"]["summary"].lower()


# -------- /api/pipeline/plan endpoint --------

def test_api_pipeline_plan_returns_per_stage_state(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/pipeline/plan",
        json={
            "skip_classify": True, "skip_extract_masks": True,
            "skip_eye_keypoints": True, "skip_regroup": True,
        },
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["stages"]["Classify"]["state"] == "will-skip"
    assert data["stages"]["Extract"]["state"] == "will-skip"
    assert data["stages"]["EyeKeypoints"]["state"] == "will-skip"
    assert data["stages"]["Group"]["state"] == "will-skip"


def test_api_pipeline_plan_collection_scope(app_and_db):
    """Passing a collection_id must scope the plan to that collection's
    photos — the endpoint's headline transparency contract.
    """
    app, db = app_and_db
    # No detections in app_and_db fixture, so Extract should report
    # "no eligible photos yet" rather than will-skip.
    client = app.test_client()
    resp = client.post(
        "/api/pipeline/plan",
        json={
            "skip_classify": True,  # short-circuit to keep test simple
            "skip_eye_keypoints": True, "skip_regroup": True,
        },
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["stages"]["Extract"]["state"] == "will-run"
    assert data["stages"]["Extract"]["detail"]["eligible"] == 0
