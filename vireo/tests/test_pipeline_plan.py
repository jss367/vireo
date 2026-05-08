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

def test_photos_by_paths_returns_known_and_omits_unknown(tmp_path):
    """photos_by_paths is the lookup that lets import-mode plan split a
    preview file list into 'already in DB' vs 'truly new'. Known paths
    must come back keyed by absolute path; unknown paths must be absent.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid_a = db.add_folder("/cards/a")
    fid_b = db.add_folder("/cards/b")
    pid_a1 = db.add_photo(folder_id=fid_a, filename="IMG_001.NEF",
                          extension=".nef", file_size=1, file_mtime=1.0)
    pid_b1 = db.add_photo(folder_id=fid_b, filename="IMG_900.JPG",
                          extension=".jpg", file_size=1, file_mtime=1.0)

    result = db.photos_by_paths([
        "/cards/a/IMG_001.NEF",         # known
        "/cards/a/IMG_NEW.NEF",         # unknown (new file, same folder)
        "/cards/b/IMG_900.JPG",         # known (different folder)
        "/cards/never/IMG_000.JPG",     # unknown (folder not in DB)
    ])
    assert result == {
        "/cards/a/IMG_001.NEF": pid_a1,
        "/cards/b/IMG_900.JPG": pid_b1,
    }


def test_photos_by_paths_handles_empty_input(tmp_path):
    """Empty list must short-circuit, not run a SELECT with zero
    placeholders (which is malformed SQL)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    assert db.photos_by_paths([]) == {}


def test_photos_by_paths_ignores_workspace_membership(tmp_path):
    """Photos are global; the lookup must find a known photo even when
    its folder is not in the active workspace. Otherwise re-importing
    into a different workspace would mis-classify already-imported
    files as 'new'.
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/cards/a")
    pid = db.add_photo(folder_id=fid, filename="x.jpg",
                       extension=".jpg", file_size=1, file_mtime=1.0)
    # Active workspace has no folders (the import target is a fresh ws).
    db._active_workspace_id = db.create_workspace("Other")

    assert db.photos_by_paths(["/cards/a/x.jpg"]) == {"/cards/a/x.jpg": pid}


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


def test_count_classify_stale_zero_when_no_runs(tmp_path):
    from labels_fingerprint import TOL_SENTINEL
    db, folder_id = _make_db(tmp_path)
    _add_photo_with_detection(db, folder_id, "a.jpg")
    assert db.count_classify_stale("BioCLIP-2", TOL_SENTINEL) == 0


def test_count_classify_stale_zero_when_current_run_present(tmp_path):
    """A detection with a row matching current (model, fp) is done,
    not stale — even if older rows under different fingerprints exist."""
    from labels_fingerprint import TOL_SENTINEL
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.record_classifier_run(did, "BioCLIP-2", "fp_old", prediction_count=1)
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=1)
    assert db.count_classify_stale("BioCLIP-2", TOL_SENTINEL) == 0


def test_count_classify_stale_counts_old_only_runs(tmp_path):
    """A detection with a row for current model under a stale fingerprint
    AND no row matching current fingerprint is stale."""
    from labels_fingerprint import TOL_SENTINEL
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.record_classifier_run(did, "BioCLIP-2", "fp_old", prediction_count=1)
    assert db.count_classify_stale("BioCLIP-2", TOL_SENTINEL) == 1


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


def test_count_eye_keypoint_stale_zero_when_all_current(tmp_path):
    """No stale photos when every eligible row's eye_kp_fingerprint
    matches the current EYE_KP_FINGERPRINT_VERSION."""
    from labels_fingerprint import TOL_SENTINEL
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    db, folder_id = _make_db(tmp_path)
    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/a.png', "
        "eye_tenengrad=12.0, eye_kp_fingerprint=? WHERE id=?",
        (EYE_KP_FINGERPRINT_VERSION, pid),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) VALUES (?, ?, ?, ?, ?)",
        (did, "BioCLIP-2", TOL_SENTINEL, "robin", 0.9),
    )
    db.conn.commit()
    assert db.count_eye_keypoint_stale() == 0


def test_count_eye_keypoint_stale_counts_old_fingerprint(tmp_path):
    """A photo with eye_tenengrad set under an old fingerprint counts
    as stale; the planner will use this to flip Outdated."""
    from labels_fingerprint import TOL_SENTINEL
    db, folder_id = _make_db(tmp_path)
    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/a.png', "
        "eye_tenengrad=12.0, eye_kp_fingerprint='superanimal-old' WHERE id=?",
        (pid,),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) VALUES (?, ?, ?, ?, ?)",
        (did, "BioCLIP-2", TOL_SENTINEL, "robin", 0.9),
    )
    db.conn.commit()
    assert db.count_eye_keypoint_stale() == 1


def test_count_eye_keypoint_stale_ignores_never_processed(tmp_path):
    """Photos with eye_tenengrad IS NULL are 'never processed', not stale.
    Stale specifically means 'previously processed under different
    settings that no longer match'."""
    from labels_fingerprint import TOL_SENTINEL
    db, folder_id = _make_db(tmp_path)
    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/a.png' WHERE id=?", (pid,),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) VALUES (?, ?, ?, ?, ?)",
        (did, "BioCLIP-2", TOL_SENTINEL, "robin", 0.9),
    )
    db.conn.commit()
    assert db.count_eye_keypoint_stale() == 0


def test_count_extract_stale_zero_when_no_masks(tmp_path):
    db, folder_id = _make_db(tmp_path)
    _add_photo_with_detection(db, folder_id, "a.jpg")
    assert db.count_extract_stale("sam2-small") == 0


def test_count_extract_stale_zero_when_prompt_matches(tmp_path):
    """A mask whose stored prompt matches the photo's primary detection
    is fresh — not stale."""
    db, folder_id = _make_db(tmp_path)
    pid, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    # Stored prompt matches the detection's box (0.1, 0.1, 0.5, 0.5)
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '/m/a.png', 0, 'megadetector-v6', "
        "0.1, 0.1, 0.5, 0.5)",
        (pid,),
    )
    db.conn.execute("UPDATE photos SET mask_path='/m/a.png' WHERE id=?", (pid,))
    db.conn.commit()
    assert db.count_extract_stale("sam2-small") == 0


def test_count_extract_stale_counts_prompt_mismatch(tmp_path):
    """A mask whose stored prompt does NOT match the photo's primary
    detection (e.g., re-detection produced a different bbox) is stale."""
    db, folder_id = _make_db(tmp_path)
    pid, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '/m/a.png', 0, 'megadetector-v6', "
        "0.9, 0.9, 0.05, 0.05)",  # bbox mismatches detection (0.1,0.1,0.5,0.5)
        (pid,),
    )
    db.conn.execute("UPDATE photos SET mask_path='/m/a.png' WHERE id=?", (pid,))
    db.conn.commit()
    assert db.count_extract_stale("sam2-small") == 1


def test_count_extract_stale_filters_by_variant(tmp_path):
    """Stale masks for a different variant don't count toward the
    configured variant's staleness."""
    db, folder_id = _make_db(tmp_path)
    pid, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-large', '/m/a.png', 0, 'megadetector-v6', "
        "0.9, 0.9, 0.05, 0.05)",
        (pid,),
    )
    db.conn.execute("UPDATE photos SET mask_path='/m/a.png' WHERE id=?", (pid,))
    db.conn.commit()
    assert db.count_extract_stale("sam2-small") == 0
    assert db.count_extract_stale("sam2-large") == 1


def test_count_extract_stale_excludes_photos_with_null_mask_path(tmp_path):
    """A photo in an interrupted state — photo_masks row inserted but
    photos.mask_path still NULL — is already counted as ``pending`` by
    ``count_photos_pending_masks``. Counting it again as stale here
    would double-count when ``_extract_plan`` does ``pending + stale``,
    pushing ``detail.pending`` past ``eligible`` and producing a wrong
    "N to redo" + progress bar in the pipeline UI.
    """
    db, folder_id = _make_db(tmp_path)
    pid, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '/m/a.png', 0, 'megadetector-v6', "
        "0.9, 0.9, 0.05, 0.05)",  # mismatches the detection
        (pid,),
    )
    # Note: mask_path on photos is NOT set — represents an interrupted
    # extract run that inserted a photo_masks row before updating
    # photos.mask_path.
    db.conn.commit()
    assert db.count_photos_pending_masks()["pending"] == 1
    assert db.count_extract_stale("sam2-small") == 0


def test_count_extract_stale_ignores_photos_without_primary_detection(tmp_path):
    """Photos with only ``full-image`` detections aren't eligible for
    extract — a stale ``photo_masks`` row left over from a prior detector
    run must not inflate ``count_extract_stale``, otherwise the stage
    stays flagged Outdated/Will run indefinitely in mixed workspaces."""
    db, folder_id = _make_db(tmp_path)
    pid = db.add_photo(
        folder_id=folder_id, filename="anchor.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    db.save_detections(
        pid,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1},
          "confidence": 1.0, "category": "anchor"}],
        detector_model="full-image",
    )
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '/m/a.png', 0, 'megadetector-v6', "
        "0.1, 0.1, 0.5, 0.5)",
        (pid,),
    )
    db.conn.commit()
    assert db.count_extract_stale("sam2-small") == 0


def test_count_extract_stale_ignores_photos_below_confidence_floor(tmp_path):
    """A photo whose only non-full-image detection sits below the
    workspace ``detector_confidence`` floor is not eligible for extract
    (extraction skips it). A leftover mask on such a photo shouldn't
    count as stale work to redo — the pipeline wouldn't redo it."""
    db, folder_id = _make_db(tmp_path)
    pid, _ = _add_photo_with_detection(db, folder_id, "a.jpg", conf=0.05)
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '/m/a.png', 0, 'megadetector-v6', "
        "0.9, 0.9, 0.05, 0.05)",
        (pid,),
    )
    db.conn.execute("UPDATE photos SET mask_path='/m/a.png' WHERE id=?", (pid,))
    db.conn.commit()
    # detector_confidence floor of 0.2 means the 0.05-conf detection is
    # invisible; the photo has no eligible primary, so no stale work.
    assert db.count_extract_stale("sam2-small", detector_confidence=0.2) == 0
    # Lower the floor and the same photo's prompt mismatch reappears.
    assert db.count_extract_stale("sam2-small", detector_confidence=0.0) == 1


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


def test_classify_plan_exposes_pending_and_eligible_done_prior(tmp_path, monkeypatch):
    """Every classify return path must expose detail.pending + detail.eligible
    so the UI's pill formatter doesn't need per-stage count knowledge.
    Done-prior path: eligible = total_dets * num_models, pending = 0."""
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
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=3)

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["pending"] == 0
    assert detail["eligible"] == 1  # 1 detection × 1 model


def test_classify_plan_exposes_pending_and_eligible_will_run(tmp_path, monkeypatch):
    """will-run path with new model added: eligible counts pairs across
    all unblocked models, pending counts the unfinished ones."""
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
        {"id": "m2", "name": "BioCLIP",
         "model_str": "hf-hub:imageomics/bioclip",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=3)

    plan = compute_plan(
        db, _params(model_ids=["m1", "m2"]), str(tmp_path / "test.db"),
    )
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["eligible"] == 2  # 1 detection × 2 models
    assert detail["pending"] == 1   # only m2 is unrun


def test_classify_plan_exposes_pending_and_eligible_reclassify(tmp_path, monkeypatch):
    """Reclassify path: pending == eligible (everything will redo)."""
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
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["eligible"] == 1
    assert detail["pending"] == 1  # reclassify forces all pairs to redo


def test_classify_plan_exposes_pending_and_eligible_no_detections(tmp_path, monkeypatch):
    """No detections cached yet: eligible=0, pending=0. Bar will hide."""
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)

    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["eligible"] == 0
    assert detail["pending"] == 0


def test_classify_plan_exposes_pending_and_eligible_blocked_only(tmp_path, monkeypatch):
    """Blocked-only path (model needs labels): eligible=0 since no model can run.
    pending=0. UI hides the bar; the summary explains the block."""
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")

    import labels as labels_mod
    import models as models_mod
    # timm models without labels are blocked. Use a non-bioclip model_str
    # so the TOL fallback doesn't kick in.
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "SomeTimmModel",
         "model_str": "hf-hub:other/model",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["eligible"] == 0  # no unblocked models
    assert detail["pending"] == 0



def test_classify_plan_exposes_pending_and_eligible_mixed_blocked_and_done(
    tmp_path, monkeypatch,
):
    """Mixed path: one unblocked model is fully cached AND another model is
    blocked-needs-labels. The blocked-only return branch fires (pending_total
    is 0 from the cached model + the blocked one was skipped via continue).
    eligible must be 0/0 since the stage is functionally blocked — returning
    eligible>0 with pending=0 here would let the UI render "Resume (0 left)"
    which is contradictory with the "Blocked" summary text."""
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
        {"id": "m2", "name": "OtherModel",
         "model_str": "hf-hub:other/model",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])
    # m1 is fully cached under the TOL fallback. m2 is blocked (no labels,
    # not a TOL-supported model_str).
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=3)

    plan = compute_plan(
        db, _params(model_ids=["m1", "m2"]), str(tmp_path / "test.db"),
    )
    classify = plan["stages"]["Classify"]
    assert classify["state"] == "will-run"
    assert "Blocked" in classify["summary"]
    detail = classify["detail"]
    assert detail["pending"] == 0
    assert detail["eligible"] == 0, (
        "blocked-only branch must return eligible=0 even when some unblocked "
        "models happen to be fully cached — otherwise pending=0/eligible>0 "
        "lets the UI show 'Resume (0 left)' against a stage that's actually "
        "blocked on missing labels"
    )


def test_classify_plan_emits_fingerprint_outdated_when_stale(
    tmp_path, monkeypatch,
):
    """A detection classified under fp_old + no current-fp row → outdated."""
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.record_classifier_run(did, "BioCLIP-2", "fp_old", prediction_count=1)

    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["stale"] == 1
    assert detail["fingerprint_outdated"] is True


def test_classify_plan_no_outdated_flag_when_current(
    tmp_path, monkeypatch,
):
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=1)

    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["stale"] == 0
    assert not detail.get("fingerprint_outdated")


def test_classify_plan_reclassify_suppresses_outdated(
    tmp_path, monkeypatch,
):
    """Reclassify is a user override, not a settings-change signal —
    don't render as 'Outdated' even though all pairs will redo."""
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.record_classifier_run(did, "BioCLIP-2", "fp_old", prediction_count=1)

    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])

    plan = compute_plan(
        db, _params(model_ids=["m1"], reclassify=True),
        str(tmp_path / "test.db"),
    )
    detail = plan["stages"]["Classify"]["detail"]
    assert not detail.get("fingerprint_outdated"), (
        "reclassify is user-explicit; outdated flag should stay off so "
        "pill says 'Re-classify' not 'Outdated'"
    )


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


def test_extract_plan_emits_fingerprint_outdated_when_stale(tmp_path):
    """When the configured sam2_variant has stale masks (prompt mismatch),
    surface fingerprint_outdated + stale count so the UI shows Outdated."""
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    pid, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    # Mask under default sam2_variant ('sam2-small') with mismatched prompt.
    # mask_path is set so the photo is "complete" — without it the photo
    # would already be in `pending` and stale wouldn't apply (see
    # test_count_extract_stale_excludes_photos_with_null_mask_path).
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '/m/a.png', 0, 'megadetector-v6', "
        "0.9, 0.9, 0.05, 0.05)",
        (pid,),
    )
    db.conn.execute("UPDATE photos SET mask_path='/m/a.png' WHERE id=?", (pid,))
    db.conn.commit()
    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    detail = plan["stages"]["Extract"]["detail"]
    assert detail["stale"] == 1
    assert detail["fingerprint_outdated"] is True


def test_extract_plan_pending_includes_stale_when_all_masked(tmp_path):
    """When every eligible photo already has an active mask but some
    stored prompts no longer match the current detection, ``detail.pending``
    must reflect the stale work the stage will redo. Otherwise the pill UI
    (``pending || eligible``) collapses to ``eligible`` and the progress
    bar (``eligible - pending``) renders 100% — both incorrect for a
    stage that is about to re-extract masks for the stale photos.
    """
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    # Two eligible photos: one fresh, one stale. Both already have an
    # active mask (mask_path set), so count_photos_pending_masks reports
    # pending=0.
    pid_fresh, _ = _add_photo_with_detection(db, folder_id, "fresh.jpg")
    pid_stale, _ = _add_photo_with_detection(db, folder_id, "stale.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/fresh.png' WHERE id=?",
        (pid_fresh,),
    )
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/stale.png' WHERE id=?",
        (pid_stale,),
    )
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '/m/fresh.png', 0, 'megadetector-v6', "
        "0.1, 0.1, 0.5, 0.5)",  # matches detection
        (pid_fresh,),
    )
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '/m/stale.png', 0, 'megadetector-v6', "
        "0.9, 0.9, 0.05, 0.05)",  # mismatched prompt
        (pid_stale,),
    )
    db.conn.commit()

    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    extract = plan["stages"]["Extract"]
    assert extract["state"] == "will-run"
    detail = extract["detail"]
    assert detail["eligible"] == 2
    assert detail["stale"] == 1
    assert detail["pending"] == 1, (
        "stale work must be reflected in pending so pill text + bar are "
        "accurate; got "
        f"pending={detail['pending']}, stale={detail['stale']}"
    )
    assert detail["fingerprint_outdated"] is True


def test_extract_plan_pending_does_not_double_count_interrupted_state(tmp_path):
    """Interrupted extract: a ``photo_masks`` row was inserted but
    ``photos.mask_path`` is still NULL. The photo is one unit of work,
    not two — ``detail.pending`` must not exceed ``eligible`` and the
    progress bar must stay sane.

    Before the disjoint-by-construction fix, ``count_extract_stale``
    counted this photo and ``count_photos_pending_masks`` also counted
    it, so ``work = pending + stale`` produced ``pending=2`` for one
    eligible photo — a 0/2 progress bar and "2 to redo" pill on a
    single-photo workspace.
    """
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    pid, _ = _add_photo_with_detection(db, folder_id, "interrupted.jpg")
    # photo_masks row exists with stale prompt, but mask_path is still NULL.
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '/m/i.png', 0, 'megadetector-v6', "
        "0.9, 0.9, 0.05, 0.05)",
        (pid,),
    )
    db.conn.commit()

    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    extract = plan["stages"]["Extract"]
    assert extract["state"] == "will-run"
    detail = extract["detail"]
    assert detail["eligible"] == 1
    assert detail["pending"] == 1, (
        "interrupted photo must count as one unit of work, not two; got "
        f"pending={detail['pending']}, stale={detail['stale']}"
    )
    assert detail["pending"] <= detail["eligible"], (
        "detail.pending must never exceed eligible (would render "
        f">100% progress); got pending={detail['pending']}, "
        f"eligible={detail['eligible']}"
    )
    # The photo is in pending (mask_path NULL), not stale — so the
    # planner reports stale=0 for this case.
    assert detail["stale"] == 0


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


def test_eye_keypoints_plan_emits_fingerprint_outdated_when_stale(
    tmp_path, monkeypatch,
):
    """The planner must surface fingerprint_outdated + a stale count when
    any eligible photo has a non-current eye_kp_fingerprint. PR #748's
    pill formatter renders this as 'Outdated (N to redo)' + amber bar."""
    import pipeline as pipeline_mod
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/a.png', "
        "eye_tenengrad=12.0, eye_kp_fingerprint='superanimal-old' WHERE id=?",
        (pid,),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) VALUES (?, ?, ?, ?, ?)",
        (did, "BioCLIP-2", TOL_SENTINEL, "robin", 0.9),
    )
    db.conn.commit()
    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    detail = plan["stages"]["EyeKeypoints"]["detail"]
    assert detail["stale"] == 1
    assert detail["fingerprint_outdated"] is True


def test_eye_keypoints_plan_no_outdated_flag_when_all_current(
    tmp_path, monkeypatch,
):
    """No stale photos → flag absent (or False), pill stays 'Already done'."""
    import pipeline as pipeline_mod
    from labels_fingerprint import TOL_SENTINEL
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/a.png', "
        "eye_tenengrad=12.0, eye_kp_fingerprint=? WHERE id=?",
        (EYE_KP_FINGERPRINT_VERSION, pid),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) VALUES (?, ?, ?, ?, ?)",
        (did, "BioCLIP-2", TOL_SENTINEL, "robin", 0.9),
    )
    db.conn.commit()
    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    detail = plan["stages"]["EyeKeypoints"]["detail"]
    assert detail["stale"] == 0
    assert not detail.get("fingerprint_outdated")


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


# -------- compute_plan: import-mode honesty --------
#
# These tests pin the core bug fix: when the user is about to import a
# brand-new SD card, every per-photo stage must report "will run" with a
# count that reflects the about-to-be-imported files — *not* "Already
# done" derived from the active workspace's existing photos. A pill that
# says "Already done" must mean the next run is genuinely a no-op.

def _import_params(paths, **kwargs):
    """Helper: compute_plan params for an import run with the given paths.

    Defaults skip eye-keypoints + regroup so tests can focus on the
    classify/extract scope without setting up label fixtures."""
    from pipeline_plan import PipelinePlanParams
    return PipelinePlanParams(
        source_paths=list(paths),
        skip_eye_keypoints=kwargs.pop("skip_eye_keypoints", True),
        skip_regroup=kwargs.pop("skip_regroup", True),
        **kwargs,
    )


def _stub_models(monkeypatch):
    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])


def test_import_plan_all_new_files_flips_classify_to_will_run(tmp_path, monkeypatch):
    """The bug we're fixing: workspace has fully-classified detections,
    user points at a brand-new folder. Classify must say "will run", not
    "Already done"."""
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _stub_models(monkeypatch)
    # Existing scope: one detection, already classified — would be
    # "done-prior" without imports.
    _, did = _add_photo_with_detection(db, folder_id, "existing.jpg")
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=1)

    new_paths = ["/cards/A/IMG_001.NEF", "/cards/A/IMG_002.NEF"]
    plan = compute_plan(
        db,
        _import_params(new_paths, model_ids=["m1"]),
        str(tmp_path / "test.db"),
    )
    classify = plan["stages"]["Classify"]
    assert classify["state"] == "will-run"
    assert classify["detail"]["new_photos"] == 2
    assert classify["detail"]["pending"] == 2
    assert "2 new" in classify["summary"]


def test_import_plan_all_new_files_flips_extract_to_will_run(tmp_path):
    """Extract pill on a brand-new import: every file is a new photo that
    will need a mask once the detector produces detections, so the pill
    must show "Will run (N)" with N = the import count, not 0 or "Already
    done"."""
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    # Background: workspace has a fully-masked photo. Without source_paths
    # the plan would report this as "done-prior". With source_paths, the
    # scope is the import only — so the existing photo doesn't pollute the
    # numbers (it's not what this run is touching).
    pid, _ = _add_photo_with_detection(db, folder_id, "existing.jpg")
    db.conn.execute("UPDATE photos SET mask_path='/m/x.png' WHERE id=?", (pid,))
    db.conn.commit()

    plan = compute_plan(
        db,
        _import_params(
            ["/cards/A/IMG_001.NEF", "/cards/A/IMG_002.NEF",
             "/cards/A/IMG_003.NEF"],
            skip_classify=True,
        ),
        str(tmp_path / "test.db"),
    )
    extract = plan["stages"]["Extract"]
    assert extract["state"] == "will-run"
    assert extract["detail"]["new_photos"] == 3
    # Scope = import (3 new) only. The unrelated existing photo isn't
    # being processed by this run and must not be included.
    assert extract["detail"]["eligible"] == 3
    assert extract["detail"]["pending"] == 3


def test_import_plan_known_files_with_pending_masks_combine(tmp_path):
    """Mixed scope: the user re-points at a partly-imported folder where
    one already-known photo has a detection but no mask yet, plus a
    new file. Extract must report the combined work — masks for the
    pending known + masks for the new — so the pill is honest about
    what the run will actually do.
    """
    from pipeline_plan import compute_plan
    db, _folder = _make_db(tmp_path)
    fid_card = db.add_folder("/cards/A")
    db.add_workspace_folder(db._active_workspace_id, fid_card)
    pid_known, _ = _add_photo_with_detection(db, fid_card, "IMG_001.NEF")
    # IMG_001.NEF: known, has detection, no mask → eligible=1, pending=1.
    paths = ["/cards/A/IMG_001.NEF", "/cards/A/IMG_002.NEF"]
    plan = compute_plan(
        db,
        _import_params(paths, skip_classify=True),
        str(tmp_path / "test.db"),
    )
    extract = plan["stages"]["Extract"]
    assert extract["state"] == "will-run"
    assert extract["detail"]["eligible"] == 2  # 1 known + 1 new
    assert extract["detail"]["pending"] == 2   # both need masks
    assert extract["detail"]["new_photos"] == 1


def test_import_plan_does_not_leak_active_workspace_state(tmp_path):
    """The headline bug: active workspace has tons of "done" work, and
    the user starts an import. Without source_paths the plan describes
    the active workspace (→ "Already done"). With source_paths the plan
    describes the import — every per-photo stage flips to will-run.

    This is the regression guard for the user-reported bug.
    """
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    # Fully-classified detection + masked photo in the active workspace.
    pid, did = _add_photo_with_detection(db, folder_id, "old.jpg")
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=1)
    db.conn.execute("UPDATE photos SET mask_path='/m/old.png' WHERE id=?", (pid,))
    db.conn.commit()

    # Without source_paths → plan is whole-workspace (the old buggy
    # behaviour), so Extract reports done-prior.
    baseline = compute_plan(
        db,
        _params(skip_classify=True, skip_eye_keypoints=True, skip_regroup=True),
        str(tmp_path / "test.db"),
    )
    assert baseline["stages"]["Extract"]["state"] == "done-prior"

    # With source_paths for a fresh import → plan is scoped to those
    # paths, Extract flips to will-run.
    fixed = compute_plan(
        db,
        _import_params(
            ["/cards/A/IMG_001.NEF", "/cards/A/IMG_002.NEF"],
            skip_classify=True,
        ),
        str(tmp_path / "test.db"),
    )
    assert fixed["stages"]["Extract"]["state"] == "will-run"
    assert fixed["stages"]["Extract"]["detail"]["pending"] == 2


def test_import_plan_partial_overlap_counts_only_truly_new(tmp_path, monkeypatch):
    """If half the preview's files are already in the DB (the user
    re-pointed at an SD card they previously imported), the plan must
    count only the truly-new ones as new work — re-importing an existing
    file is a no-op."""
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _stub_models(monkeypatch)
    # Two existing photos in DB at /cards/A/...
    fid_card = db.add_folder("/cards/A")
    db.add_photo(folder_id=fid_card, filename="IMG_001.NEF",
                 extension=".nef", file_size=1, file_mtime=1.0)
    db.add_photo(folder_id=fid_card, filename="IMG_002.NEF",
                 extension=".nef", file_size=1, file_mtime=1.0)

    paths = [
        "/cards/A/IMG_001.NEF",  # known
        "/cards/A/IMG_002.NEF",  # known
        "/cards/A/IMG_003.NEF",  # new
        "/cards/A/IMG_004.NEF",  # new
    ]
    plan = compute_plan(
        db,
        _import_params(paths, model_ids=["m1"]),
        str(tmp_path / "test.db"),
    )
    assert plan["scope"]["new_count"] == 2
    assert plan["scope"]["known_count"] == 2
    assert plan["stages"]["Scan"]["state"] == "will-run"
    assert plan["stages"]["Scan"]["detail"]["new_photos"] == 2
    assert plan["stages"]["Scan"]["detail"]["already_known"] == 2


def test_import_plan_all_known_scan_is_done_prior(tmp_path):
    """If every preview file is already in the DB, Scan reports done-prior
    (the run will be a no-op for the scan step). Other stages reflect
    real status of those known photos as scope."""
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    fid_card = db.add_folder("/cards/A")
    db.add_photo(folder_id=fid_card, filename="IMG_001.NEF",
                 extension=".nef", file_size=1, file_mtime=1.0)

    plan = compute_plan(
        db,
        _import_params(
            ["/cards/A/IMG_001.NEF"],
            skip_classify=True,
        ),
        str(tmp_path / "test.db"),
    )
    scan = plan["stages"]["Scan"]
    assert scan["state"] == "done-prior"
    assert scan["detail"]["new_photos"] == 0
    assert scan["detail"]["already_known"] == 1


def test_import_plan_emits_scan_only_for_import_mode(tmp_path):
    """Scan plan entry is import/new-images specific. Whole-workspace and
    collection-scoped plans must not emit it (the JS renders a default
    'will run' for unknown stages, which is fine for those modes)."""
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)

    plan = compute_plan(
        db,
        _params(
            skip_classify=True, skip_extract_masks=True,
            skip_eye_keypoints=True, skip_regroup=True,
        ),
        str(tmp_path / "test.db"),
    )
    assert "Scan" not in plan["stages"]
    assert plan["scope"]["new_count"] == 0


def test_api_pipeline_plan_rejects_oversized_source_paths(app_and_db):
    """Defense in depth at the API boundary — a misbehaving client that
    POSTs hundreds of thousands of paths should get 400, not silent
    truncation (which would mis-classify the dropped files as new)."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/pipeline/plan",
        json={
            "source_paths": ["/x/" + str(i) for i in range(50001)],
            "skip_classify": True, "skip_extract_masks": True,
            "skip_eye_keypoints": True, "skip_regroup": True,
        },
    )
    assert resp.status_code == 400


def test_api_pipeline_plan_rejects_non_list_source_paths(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/pipeline/plan",
        json={
            "source_paths": "/single/path.nef",  # str, not list
            "skip_classify": True, "skip_extract_masks": True,
            "skip_eye_keypoints": True, "skip_regroup": True,
        },
    )
    assert resp.status_code == 400


def test_api_pipeline_plan_rejects_non_string_source_paths_elements(app_and_db):
    """A payload like {"source_paths": [123]} would otherwise reach
    photos_by_paths and crash os.path.dirname with TypeError, surfacing
    as a 500. Catch it at the boundary as a 400 instead."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/pipeline/plan",
        json={
            "source_paths": ["/ok/path.nef", 123],  # mixed types
            "skip_classify": True, "skip_extract_masks": True,
            "skip_eye_keypoints": True, "skip_regroup": True,
        },
    )
    assert resp.status_code == 400
