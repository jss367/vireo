"""Tests for the pipeline plan: db helpers + compute_plan + /api/pipeline/plan.

The plan is the truth source for the Pipeline page's status pills. These
tests pin down the contract documented in CORE_PHILOSOPHY.md ("No black
boxes"): a pill that says "Already done" must mean the next run would be a
no-op, and a pill that says "Will run" must mean there is genuinely new
work — independent of whether *any* prior output exists in the workspace.
"""
import os
import sys

import pytest

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


def _mark_sam_done(db, photo_id, path, variant="sam2-small"):
    db.upsert_photo_mask(
        photo_id, variant, path,
        detector_model="megadetector-v6",
        prompt_x=0.1, prompt_y=0.1, prompt_w=0.5, prompt_h=0.5,
    )
    db.set_active_mask_variant(photo_id, variant)


def _tol_weights(tmp_path, name="bioclip-2"):
    """Create a fake ToL weights dir with the artifact stubs so
    `tree_of_life_ready()` treats the model as ready. The label-free ToL
    gate is disk-aware — it checks `tol_embeddings.npy` and
    `tol_classes.json` on disk — so tests that mock a ToL-supported model
    must ship a real dir with the stubs, otherwise the planner blocks the
    Classify stage. Reused across tests via `exist_ok=True`."""
    weights = tmp_path / name
    weights.mkdir(exist_ok=True)
    (weights / "tol_embeddings.npy").write_bytes(b"stub")
    (weights / "tol_classes.json").write_bytes(b"[]")
    return str(weights)


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


def test_workspace_unlinked_folder_count_counts_unlinked(tmp_path):
    """The helper must report folders not linked to the active workspace.

    Three cases must all count as "unlinked":
      - folder row exists but is linked only to a different workspace,
      - folder row exists and is linked to no workspace,
      - folder row does not exist at all (a brand-new directory).
    """
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    ws_other = db.create_workspace("Other")
    ws_active = db.create_workspace("Active")

    fid_other = db.add_folder("/cards/other-ws-only")
    db.add_workspace_folder(ws_other, fid_other)
    db.add_folder("/cards/orphan")  # no workspace link at all
    fid_shared = db.add_folder("/cards/shared")
    db.add_workspace_folder(ws_other, fid_shared)
    db.add_workspace_folder(ws_active, fid_shared)

    db._active_workspace_id = ws_active

    assert db.workspace_unlinked_folder_count([
        "/cards/other-ws-only",  # linked only to Other
        "/cards/orphan",         # linked to nothing
        "/cards/shared",         # linked to Active
        "/cards/never-seen",     # no folder row at all
    ]) == 3


def test_workspace_unlinked_folder_count_dedupes_and_handles_empty(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    db._active_workspace_id = db.create_workspace("Active")
    fid = db.add_folder("/cards/a")
    db.add_workspace_folder(db._active_workspace_id, fid)

    assert db.workspace_unlinked_folder_count([]) == 0
    assert db.workspace_unlinked_folder_count(["/cards/a", "/cards/a"]) == 0
    assert db.workspace_unlinked_folder_count(["/cards/x", "/cards/x"]) == 1


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


def test_primary_classify_counts_ignore_secondary_detections(tmp_path):
    """Pipeline classify uses one primary detection per photo.

    Secondary boxes may exist in the detector cache, but they are not work the
    streaming pipeline will classify, so primary-scoped plan helpers must not
    count them as pending or stale.
    """
    db, folder_id = _make_db(tmp_path)
    photo_id = db.add_photo(
        folder_id=folder_id, filename="multi.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_ids = db.save_detections(
        photo_id,
        [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
             "confidence": 0.95, "category": "animal"},
            {"box": {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.2},
             "confidence": 0.85, "category": "animal"},
        ],
        detector_model="megadetector-v6",
    )
    db.record_classifier_run(det_ids[0], "BioCLIP-2", "fp1", prediction_count=1)
    db.record_classifier_run(det_ids[1], "BioCLIP-2", "fp_old", prediction_count=1)

    assert db.count_real_detections_in_scope()["total_dets"] == 2
    assert db.count_primary_detections_in_scope()["total_dets"] == 1
    assert db.count_primary_classify_pending_pairs("BioCLIP-2", "fp1") == 0
    assert db.count_primary_classify_stale("BioCLIP-2", "fp1") == 0


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


def test_count_photos_pending_masks_is_variant_aware(tmp_path):
    db, folder_id = _make_db(tmp_path)
    p1, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    p2, _ = _add_photo_with_detection(db, folder_id, "b.jpg")
    for pid in (p1, p2):
        db.upsert_photo_mask(
            pid, "sam2-large", f"/m/{pid}.large.png",
            detector_model="megadetector-v6",
            prompt_x=0.1, prompt_y=0.1, prompt_w=0.5, prompt_h=0.5,
        )
        db.set_active_mask_variant(pid, "sam2-large")

    assert db.count_photos_pending_masks() == {"eligible": 2, "pending": 0}
    assert db.count_photos_pending_masks(
        sam2_variant="sam2-small",
    ) == {"eligible": 2, "pending": 2}


def test_count_photos_pending_masks_selected_variant_requires_cache_row(tmp_path):
    db, folder_id = _make_db(tmp_path)
    p_done, _ = _add_photo_with_detection(db, folder_id, "done.jpg")
    p_legacy, _ = _add_photo_with_detection(db, folder_id, "legacy.jpg")

    db.upsert_photo_mask(
        p_done, "sam2-small", "/m/done.small.png",
        detector_model="megadetector-v6",
        prompt_x=0.1, prompt_y=0.1, prompt_w=0.5, prompt_h=0.5,
    )
    db.set_active_mask_variant(p_done, "sam2-small")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/legacy.png' WHERE id=?",
        (p_legacy,),
    )
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'unknown', '/m/legacy.png', 0, "
        "'unknown', -1, -1, -1, -1)",
        (p_legacy,),
    )
    db.conn.commit()

    assert db.count_photos_pending_masks(
        sam2_variant="sam2-small",
    ) == {"eligible": 2, "pending": 1}


def test_count_photos_pending_masks_treats_empty_variant_path_as_pending(tmp_path):
    db, folder_id = _make_db(tmp_path)
    pid, _ = _add_photo_with_detection(db, folder_id, "empty.jpg")
    db.conn.execute("UPDATE photos SET mask_path='/m/empty.png' WHERE id=?", (pid,))
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '', 0, "
        "'megadetector-v6', 0.1, 0.1, 0.5, 0.5)",
        (pid,),
    )
    db.conn.commit()

    assert db.count_photos_pending_masks(
        sam2_variant="sam2-small",
    ) == {"eligible": 1, "pending": 1}


def test_extract_plan_warns_when_other_sam_variant_has_coverage(tmp_path):
    from pipeline_plan import PipelinePlanParams, compute_plan

    db, folder_id = _make_db(tmp_path)
    p1, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    p2, _ = _add_photo_with_detection(db, folder_id, "b.jpg")
    for pid in (p1, p2):
        db.upsert_photo_mask(
            pid, "sam2-large", f"/m/{pid}.large.png",
            detector_model="megadetector-v6",
            prompt_x=0.1, prompt_y=0.1, prompt_w=0.5, prompt_h=0.5,
        )
        db.set_active_mask_variant(pid, "sam2-large")

    plan = compute_plan(db, PipelinePlanParams(), str(tmp_path / "test.db"))
    extract = plan["stages"]["Extract"]
    assert extract["state"] == "will-run"
    assert extract["detail"]["pending"] == 2
    warning = extract["detail"]["sam_variant_warning"]
    assert warning["selected_variant"] == "sam2-small"
    assert warning["alternate_variant"] == "sam2-large"
    assert warning["target_count"] == 2


def test_extract_plan_variant_warning_ignores_incomplete_masks(tmp_path):
    from pipeline_plan import PipelinePlanParams, compute_plan

    db, folder_id = _make_db(tmp_path)
    p1, _ = _add_photo_with_detection(db, folder_id, "interrupted.jpg")
    p2, _ = _add_photo_with_detection(db, folder_id, "missing_path.jpg")
    # p1 simulates an interrupted run: the variant row exists, but
    # photos.mask_path was never activated.
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-large', '/m/interrupted.png', 0, "
        "'megadetector-v6', 0.1, 0.1, 0.5, 0.5)",
        (p1,),
    )
    # p2 has an active-looking photo row, but the per-variant row has no
    # file path, so it should not count as alternate variant coverage.
    db.conn.execute("UPDATE photos SET mask_path='/m/missing.png' WHERE id=?", (p2,))
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-large', '', 0, "
        "'megadetector-v6', 0.1, 0.1, 0.5, 0.5)",
        (p2,),
    )
    db.conn.commit()

    plan = compute_plan(db, PipelinePlanParams(), str(tmp_path / "test.db"))

    assert plan["stages"]["Extract"]["detail"].get("sam_variant_warning") is None


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


def test_count_extract_stale_excludes_incomplete_variant_paths(tmp_path):
    """Incomplete selected-variant rows are pending, not stale.

    A migrated/interrupted row can have ``photos.mask_path`` set but no
    selected-variant row, or an empty ``photo_masks.path`` for the configured
    variant. The planner counts those as pending, so stale must not count
    them again.
    """
    db, folder_id = _make_db(tmp_path)
    pid_missing, _ = _add_photo_with_detection(db, folder_id, "missing-row.jpg")
    pid_empty, _ = _add_photo_with_detection(db, folder_id, "empty-path.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/missing-row.png' WHERE id=?",
        (pid_missing,),
    )
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/empty-path.png' WHERE id=?",
        (pid_empty,),
    )
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '', 0, 'megadetector-v6', "
        "0.9, 0.9, 0.05, 0.05)",
        (pid_empty,),
    )
    db.conn.commit()

    counts = db.count_photos_pending_masks(sam2_variant="sam2-small")
    assert counts["eligible"] == 2
    assert counts["pending"] == 2
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
         "model_type": "bioclip", "downloaded": True,
         "weights_path": _tol_weights(tmp_path)},
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
         "model_type": "bioclip", "downloaded": True,
         "weights_path": _tol_weights(tmp_path)},
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


def test_classify_plan_timm_intrinsic_uses_runtime_fingerprint(
    tmp_path, monkeypatch,
):
    """timm/iNat21 has a fixed class head and runtime records it as `tol`.

    The planner must use the same fingerprint or cached iNat21 rows render as
    stale/outdated even though the next pipeline run will skip them.
    """
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan

    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")

    import labels as labels_mod
    import models as models_mod

    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "inat", "name": "iNat21 (EVA-02 Large)",
         "model_str": "hf-hub:timm/eva02_large_patch14_clip_336.merged2b_ft_inat21",
         "model_type": "timm", "downloaded": True},
    ])
    # Label files do not apply to timm models; keep the environment out of it.
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])
    db.record_classifier_run(
        did, "iNat21 (EVA-02 Large)", TOL_SENTINEL, prediction_count=1,
    )

    plan = compute_plan(
        db, _params(model_ids=["inat"]), str(tmp_path / "test.db"),
    )
    classify = plan["stages"]["Classify"]
    assert classify["state"] == "done-prior"
    assert classify["detail"]["pending"] == 0
    assert classify["detail"]["stale"] == 0
    assert classify["detail"]["fingerprint_outdated"] is False


def test_classify_plan_counts_primary_detections_only(tmp_path, monkeypatch):
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan

    db, folder_id = _make_db(tmp_path)
    photo_id = db.add_photo(
        folder_id=folder_id, filename="multi.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    det_ids = db.save_detections(
        photo_id,
        [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
             "confidence": 0.95, "category": "animal"},
            {"box": {"x": 0.5, "y": 0.5, "w": 0.2, "h": 0.2},
             "confidence": 0.85, "category": "animal"},
        ],
        detector_model="megadetector-v6",
    )

    import labels as labels_mod
    import models as models_mod

    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True,
         "weights_path": _tol_weights(tmp_path)},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])
    db.record_classifier_run(det_ids[0], "BioCLIP-2", TOL_SENTINEL, 1)

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    classify = plan["stages"]["Classify"]
    assert classify["state"] == "done-prior"
    assert classify["detail"]["eligible"] == 1
    assert classify["detail"]["pending"] == 0
    assert classify["detail"]["stale"] == 0


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
         "model_type": "bioclip", "downloaded": True,
         "weights_path": _tol_weights(tmp_path, "bioclip-2")},
        {"id": "m2", "name": "BioCLIP",
         "model_str": "hf-hub:imageomics/bioclip",
         "model_type": "bioclip", "downloaded": True,
         "weights_path": _tol_weights(tmp_path, "bioclip")},
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
         "model_type": "bioclip", "downloaded": True,
         "weights_path": _tol_weights(tmp_path)},
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
         "model_type": "bioclip", "downloaded": True,
         "weights_path": _tol_weights(tmp_path)},
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
         "model_type": "bioclip", "downloaded": True,
         "weights_path": _tol_weights(tmp_path, "bioclip-2")},
        {"id": "m2", "name": "BioCLIP",
         "model_str": "hf-hub:imageomics/bioclip",
         "model_type": "bioclip", "downloaded": True,
         "weights_path": _tol_weights(tmp_path, "bioclip")},
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
         "model_type": "bioclip", "downloaded": True,
         "weights_path": _tol_weights(tmp_path)},
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
    classify = plan["stages"]["Classify"]
    assert classify["state"] == "blocked"
    assert classify["detail"]["blocked_models"] == ["SomeTimmModel"]
    detail = classify["detail"]
    assert detail["eligible"] == 0  # no unblocked models
    assert detail["pending"] == 0


def test_classify_plan_blocked_when_no_detections_and_no_labels(tmp_path, monkeypatch):
    """Fresh install: a label-needing model with no labels and no detections
    cached yet must report "blocked", not "will-run". This is the exact
    first-run state that previously slipped through the total_dets==0
    early-return and let the user launch a pipeline that crashes at classify.
    """
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)

    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP ViT-B-16",
         "model_str": "ViT-B-16",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    classify = plan["stages"]["Classify"]
    assert classify["state"] == "blocked"
    assert "Settings" in classify["summary"]
    assert classify["detail"]["blocked_models"] == ["BioCLIP ViT-B-16"]


def test_classify_plan_mixed_blocked_with_no_detections_emits_blocked(
    tmp_path, monkeypatch,
):
    """Mixed scope, no detections cached yet: one label-free model can run,
    another model is blocked on missing labels. The unblocked_count==0 guard
    doesn't fire (one model is runnable), but classify_job iterates every
    selected model and the blocked one will fail at _load_labels once
    MegaDetector creates detections. The planner must emit "blocked" (gates
    Start), not "will-run" — otherwise Start stays enabled, the user
    launches, and the pipeline records the same missing-labels classify
    failure this PR is meant to prevent.
    """
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)

    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        # Label-free TOL model — unblocked.
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True,
         "weights_path": _tol_weights(tmp_path)},
        # Label-needing model with no active labels — blocked.
        {"id": "m2", "name": "BioCLIP ViT-B-16",
         "model_str": "ViT-B-16",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])

    plan = compute_plan(
        db, _params(model_ids=["m1", "m2"]), str(tmp_path / "test.db"),
    )
    classify = plan["stages"]["Classify"]
    assert classify["state"] == "blocked", (
        "mixed scope with no detections + one blocked model must emit "
        "'blocked', not slip through the total_dets==0 'will-run' branch"
    )
    assert classify["detail"]["blocked_models"] == ["BioCLIP ViT-B-16"]
    assert classify["detail"]["pending"] == 0
    assert classify["detail"]["eligible"] == 0


def test_classify_plan_mixed_blocked_with_pending_emits_blocked(
    tmp_path, monkeypatch,
):
    """Mixed scope with cached detections: one unblocked model has pending
    classify work AND another selected model is blocked on missing labels.
    Previously the `blocked and not pending_total` guard skipped this case
    (pending_total > 0 from the runnable model) and the planner fell through
    to "will-run" with blocked_models only buried in detail. The Start gate
    only disables `state === "blocked"`, so the user clicked Start, the
    classify job iterated every selected resolved spec, and the blocked
    model failed at _load_labels — the exact missing-labels failure this PR
    is meant to prevent. Must emit "blocked" so Start stays disabled until
    the user either adds labels or deselects the blocked model.
    """
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _add_photo_with_detection(db, folder_id, "a.jpg")

    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        # Label-free TOL model — unblocked, has pending work (no
        # record_classifier_run was called for it, so the detection is
        # uncached and pending_total > 0 for this model).
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True,
         "weights_path": _tol_weights(tmp_path)},
        # Label-needing model with no active labels — blocked.
        {"id": "m2", "name": "BioCLIP ViT-B-16",
         "model_str": "ViT-B-16",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])

    plan = compute_plan(
        db, _params(model_ids=["m1", "m2"]), str(tmp_path / "test.db"),
    )
    classify = plan["stages"]["Classify"]
    assert classify["state"] == "blocked", (
        "any blocked model in a mixed selection must emit 'blocked' (gates "
        "Start) — falling through to 'will-run' when other models have "
        "pending work let the missing-labels classify failure through on "
        "launch"
    )
    assert classify["detail"]["blocked_models"] == ["BioCLIP ViT-B-16"]
    assert classify["detail"]["pending"] == 0
    assert classify["detail"]["eligible"] == 0


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
    assert classify["state"] == "blocked", (
        "mixed blocked/done shape has no runnable work, so it must emit "
        "'blocked' (gates Start) — not 'will-run', which left Start enabled "
        "behind a 'Blocked' summary and reached the unlabeled model"
    )
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
    """A detection classified under fp_old + no current-fp row needs the
    current label set, and the plan should explain that without implying
    the old classifications or user's keywords are bad.
    """
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.record_classifier_run(did, "BioCLIP-2", "fp_old", prediction_count=1)

    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True,
         "weights_path": _tol_weights(tmp_path)},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    detail = plan["stages"]["Classify"]["detail"]
    assert "Current label set differs" in plan["stages"]["Classify"]["summary"]
    assert detail["stale"] == 1
    assert detail["fingerprint_outdated"] is True
    assert detail["fingerprint_reason"] == "label_set_changed"


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


# -------- db helpers: thumbnails & previews --------

def test_count_photos_missing_thumb_basic(tmp_path):
    """One photo with thumb_path set, one without → eligible=2, pending=1."""
    db, folder_id = _make_db(tmp_path)
    pid_done = db.add_photo(
        folder_id=folder_id, filename="done.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    db.add_photo(
        folder_id=folder_id, filename="todo.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    db.conn.execute(
        "UPDATE photos SET thumb_path='done.jpg' WHERE id=?", (pid_done,),
    )
    db.conn.commit()
    assert db.count_photos_missing_thumb() == {"eligible": 2, "pending": 1}


def test_count_photos_missing_thumb_empty_workspace(tmp_path):
    db, _ = _make_db(tmp_path)
    assert db.count_photos_missing_thumb() == {"eligible": 0, "pending": 0}


def test_count_photos_missing_preview_basic(tmp_path):
    """preview_cache row at the requested size means 'already cached'."""
    db, folder_id = _make_db(tmp_path)
    pid_a = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    db.add_photo(
        folder_id=folder_id, filename="b.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    db.preview_cache_insert(pid_a, 1920, 100)
    assert db.count_photos_missing_preview(1920) == {
        "eligible": 2, "pending": 1,
    }


def test_count_photos_missing_preview_size_specific(tmp_path):
    """A photo cached at 1280px is still pending at 1920px — the planner
    must not count cross-size cache rows toward the configured size.
    """
    db, folder_id = _make_db(tmp_path)
    pid = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    db.preview_cache_insert(pid, 1280, 100)
    assert db.count_photos_missing_preview(1920) == {
        "eligible": 1, "pending": 1,
    }
    assert db.count_photos_missing_preview(1280) == {
        "eligible": 1, "pending": 0,
    }


def test_count_photos_missing_thumb_or_preview_union(tmp_path):
    """Union helper counts a photo once whether it's missing its thumb,
    its preview, or both. ``max(thumb_pending, preview_pending)``
    undercounts whenever the missing-sets aren't strict subsets — this
    pins the union behaviour the Thumbnails & Previews pill needs.
    """
    db, folder_id = _make_db(tmp_path)
    pid_thumb_only_missing = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    pid_preview_only_missing = db.add_photo(
        folder_id=folder_id, filename="b.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    pid_both_done = db.add_photo(
        folder_id=folder_id, filename="c.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    pid_both_missing = db.add_photo(
        folder_id=folder_id, filename="d.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    # b and c have a thumb on disk; a and d do not.
    db.conn.execute(
        "UPDATE photos SET thumb_path=filename WHERE id IN (?, ?)",
        (pid_preview_only_missing, pid_both_done),
    )
    db.conn.commit()
    # a and c have a 1920px preview cached; b and d do not.
    db.preview_cache_insert(pid_thumb_only_missing, 1920, 100)
    db.preview_cache_insert(pid_both_done, 1920, 100)

    # max(thumb_pending=2, preview_pending=2) = 2 — but the real union is
    # {a, b, d} = 3 photos the next run will touch.
    assert db.count_photos_missing_thumb()["pending"] == 2
    assert db.count_photos_missing_preview(1920)["pending"] == 2
    assert db.count_photos_missing_thumb_or_preview(1920) == {
        "eligible": 4, "pending": 3,
    }


def test_previews_plan_pending_uses_union_not_max(tmp_path):
    """compute_plan must surface the union pending count, not the max of
    substages. Two photos — one missing only the thumb, one missing only
    the 1920px preview — should report ``pending=2``, not 1.
    """
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    pid_thumb_only_missing = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    pid_preview_only_missing = db.add_photo(
        folder_id=folder_id, filename="b.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    db.conn.execute(
        "UPDATE photos SET thumb_path=filename WHERE id=?",
        (pid_preview_only_missing,),
    )
    db.conn.commit()
    db.preview_cache_insert(pid_thumb_only_missing, 1920, 100)

    plan = compute_plan(
        db, _params(preview_max_size=1920), str(tmp_path / "test.db"),
    )
    previews = plan["stages"]["Previews"]
    assert previews["state"] == "will-run"
    assert previews["detail"]["thumb_pending"] == 1
    assert previews["detail"]["preview_pending"] == 1
    # Both photos genuinely have work, so pending must be 2 — not 1.
    assert previews["detail"]["pending"] == 2


# -------- compute_plan: previews --------

def test_previews_plan_done_prior_when_all_cached(tmp_path):
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    pid_a = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    pid_b = db.add_photo(
        folder_id=folder_id, filename="b.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    db.conn.execute(
        "UPDATE photos SET thumb_path=filename WHERE id IN (?, ?)",
        (pid_a, pid_b),
    )
    db.conn.commit()
    db.preview_cache_insert(pid_a, 1920, 100)
    db.preview_cache_insert(pid_b, 1920, 100)
    plan = compute_plan(
        db, _params(preview_max_size=1920), str(tmp_path / "test.db"),
    )
    previews = plan["stages"]["Previews"]
    assert previews["state"] == "done-prior"
    assert "2 photos" in previews["summary"]
    assert "1920px" in previews["summary"]
    assert previews["detail"]["thumb_pending"] == 0
    assert previews["detail"]["preview_pending"] == 0


def test_previews_plan_will_run_with_pending_counts(tmp_path):
    """Mixed scope: some cached, some not. Pill summary names both
    substages so the user sees the breakdown, not a single conflated number.
    """
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    pid_done = db.add_photo(
        folder_id=folder_id, filename="done.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    db.add_photo(
        folder_id=folder_id, filename="todo.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    db.conn.execute(
        "UPDATE photos SET thumb_path='done.jpg' WHERE id=?", (pid_done,),
    )
    db.conn.commit()
    db.preview_cache_insert(pid_done, 1920, 100)
    plan = compute_plan(
        db, _params(preview_max_size=1920), str(tmp_path / "test.db"),
    )
    previews = plan["stages"]["Previews"]
    assert previews["state"] == "will-run"
    assert previews["detail"]["thumb_pending"] == 1
    assert previews["detail"]["preview_pending"] == 1
    assert "1 thumbnail" in previews["summary"]
    assert "1 1920px preview" in previews["summary"]


def test_previews_plan_done_prior_when_full_resolution_and_thumbs_cached(tmp_path):
    """preview_max_size=0 → previews substage skipped. With all thumbs
    cached, the card is honestly Already done — only the thumbnail
    substage had work, and it has none left.
    """
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    pid = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    db.conn.execute(
        "UPDATE photos SET thumb_path='a.jpg' WHERE id=?", (pid,),
    )
    db.conn.commit()
    plan = compute_plan(
        db, _params(preview_max_size=0), str(tmp_path / "test.db"),
    )
    previews = plan["stages"]["Previews"]
    assert previews["state"] == "done-prior"
    assert previews["detail"]["previews_skipped"] is True
    assert "previews skipped" in previews["summary"]


def test_previews_plan_full_resolution_will_run_when_thumbs_pending(tmp_path):
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    plan = compute_plan(
        db, _params(preview_max_size=0), str(tmp_path / "test.db"),
    )
    previews = plan["stages"]["Previews"]
    assert previews["state"] == "will-run"
    assert previews["detail"]["thumb_pending"] == 1
    assert previews["detail"]["preview_pending"] == 0
    assert "1 thumbnail" in previews["summary"]
    # No "Npx preview" mention when previews are skipped.
    assert "preview" not in previews["summary"].replace("previews", "")


def test_previews_plan_size_change_invalidates_done_prior(tmp_path):
    """Library cached at 1280px should NOT report Already done when the
    user has the picker on 1920px — the next run will generate fresh
    1920px files. This is the core of the user's question on
    transparency: the pill must answer for the *current* selection.
    """
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    pid = db.add_photo(
        folder_id=folder_id, filename="a.jpg", extension=".jpg",
        file_size=1, file_mtime=1.0,
    )
    db.conn.execute(
        "UPDATE photos SET thumb_path='a.jpg' WHERE id=?", (pid,),
    )
    db.conn.commit()
    db.preview_cache_insert(pid, 1280, 100)
    # 1280 selected → done-prior.
    plan_1280 = compute_plan(
        db, _params(preview_max_size=1280), str(tmp_path / "test.db"),
    )
    assert plan_1280["stages"]["Previews"]["state"] == "done-prior"
    # 1920 selected → will-run, even though "some prior preview output exists".
    plan_1920 = compute_plan(
        db, _params(preview_max_size=1920), str(tmp_path / "test.db"),
    )
    previews_1920 = plan_1920["stages"]["Previews"]
    assert previews_1920["state"] == "will-run"
    assert previews_1920["detail"]["preview_pending"] == 1


def test_previews_plan_empty_workspace_will_run_no_count(tmp_path):
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    plan = compute_plan(
        db, _params(preview_max_size=1920), str(tmp_path / "test.db"),
    )
    previews = plan["stages"]["Previews"]
    assert previews["state"] == "will-run"
    assert previews["detail"]["eligible"] == 0
    assert "no photos in scope" in previews["summary"]


def test_previews_plan_import_mode_counts_new_files(tmp_path):
    """Import mode: N new paths must be reflected as up-to-N new photos
    needing thumbnails+previews, even when the active workspace is empty.
    """
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    plan = compute_plan(
        db,
        _params(
            source_paths=["/cards/a/IMG_001.JPG", "/cards/a/IMG_002.JPG"],
            preview_max_size=1920,
        ),
        str(tmp_path / "test.db"),
    )
    previews = plan["stages"]["Previews"]
    assert previews["state"] == "will-run"
    assert previews["detail"]["new_photos"] == 2
    assert "up to 2 new" in previews["summary"]


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
    db.upsert_photo_mask(
        pid, "sam2-small", "/m/a.png",
        detector_model="megadetector-v6",
        prompt_x=0.1, prompt_y=0.1, prompt_w=0.5, prompt_h=0.5,
    )
    db.set_active_mask_variant(pid, "sam2-small")
    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    extract = plan["stages"]["Extract"]
    assert extract["state"] == "done-prior"
    assert "1" in extract["summary"]


def test_extract_plan_will_run_when_some_photos_missing_masks(tmp_path):
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    pid_done, _ = _add_photo_with_detection(db, folder_id, "done.jpg")
    db.upsert_photo_mask(
        pid_done, "sam2-small", "/m/done.png",
        detector_model="megadetector-v6",
        prompt_x=0.1, prompt_y=0.1, prompt_w=0.5, prompt_h=0.5,
    )
    db.set_active_mask_variant(pid_done, "sam2-small")
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
    _mark_sam_done(db, pid, "/m/a.png")
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
    _mark_sam_done(db, pid, "/m/a.png")
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


def _stub_models(monkeypatch, tmp_path=None):
    import labels as labels_mod
    import models as models_mod
    model = {
        "id": "m1", "name": "BioCLIP-2",
        "model_str": "hf-hub:imageomics/bioclip-2",
        "model_type": "bioclip", "downloaded": True,
    }
    if tmp_path is not None:
        # Label-free ToL is now disk-aware — the planner checks that the
        # ToL artifacts exist under weights_path. Tests that expect
        # BioCLIP-2 to run label-free must ship a real weights dir with
        # the artifact stubs, otherwise the Classify stage blocks.
        model["weights_path"] = _tol_weights(tmp_path)
    monkeypatch.setattr(models_mod, "get_models", lambda: [model])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])


def test_import_plan_all_new_files_flips_classify_to_will_run(tmp_path, monkeypatch):
    """The bug we're fixing: workspace has fully-classified detections,
    user points at a brand-new folder. Classify must say "will run", not
    "Already done"."""
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _stub_models(monkeypatch, tmp_path)
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
    _mark_sam_done(db, pid, "/m/old.png")

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


def test_import_plan_deduplicates_source_paths(tmp_path, monkeypatch):
    """Overlapping source roots can land the same path in source_paths
    twice. Counting the duplicate as 'new' would inflate Scan/Classify/
    Extract estimates and could flip Scan from done-prior to will-run
    misleadingly. Dedup must happen before the new-vs-known split.
    """
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _stub_models(monkeypatch)
    fid_card = db.add_folder("/cards/A")
    db.add_photo(folder_id=fid_card, filename="IMG_001.NEF",
                 extension=".nef", file_size=1, file_mtime=1.0)

    # All-known with duplicates: same known path repeated must NOT count
    # as new work; Scan stays done-prior.
    plan_all_known = compute_plan(
        db,
        _import_params(
            ["/cards/A/IMG_001.NEF", "/cards/A/IMG_001.NEF"],
            model_ids=["m1"],
        ),
        str(tmp_path / "test.db"),
    )
    assert plan_all_known["scope"]["new_count"] == 0
    assert plan_all_known["scope"]["known_count"] == 1
    assert plan_all_known["stages"]["Scan"]["state"] == "done-prior"

    # Mixed with duplicates: 1 unique known + 1 unique new (each repeated).
    # Without dedup, new_count would be 4 - 1 = 3 (wrong).
    plan_mixed = compute_plan(
        db,
        _import_params(
            [
                "/cards/A/IMG_001.NEF",   # known
                "/cards/A/IMG_001.NEF",   # known dup
                "/cards/A/IMG_NEW.NEF",   # new
                "/cards/A/IMG_NEW.NEF",   # new dup
            ],
            model_ids=["m1"],
        ),
        str(tmp_path / "test.db"),
    )
    assert plan_mixed["scope"]["new_count"] == 1
    assert plan_mixed["scope"]["known_count"] == 1
    assert plan_mixed["stages"]["Scan"]["detail"]["new_photos"] == 1
    assert plan_mixed["stages"]["Scan"]["detail"]["already_known"] == 1


@pytest.mark.skip(reason="retired pipeline local-processing import planner mode")
def test_import_plan_all_duplicates_reports_zero_new_photos(
    tmp_path, monkeypatch
):
    """The re-inserted SD card (local-processing mode): every selected
    file is already in the library via the hash/metadata duplicate gate,
    so the run will import 0 new photos and every per-photo stage executes
    over an empty set — the post-ingest scan walks the local staging root,
    which stays empty when everything deduplicated.
    The summaries must say that outright — "no photos in scope yet" and
    "MegaDetector will run first" read as "work is coming" when none is.
    Group must report "Will skip": with 0 collected photos the job never
    creates a collection, and regroup_stage skips on `not collection_id` —
    so any "Will run" claim (upstream work, no cached grouping, stale
    cache) would promise a Group run the job cannot perform."""
    import pipeline as pipeline_mod
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    _stub_models(monkeypatch, tmp_path)
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    paths = ["/cards/SD/IMG_001.NEF", "/cards/SD/IMG_002.NEF"]
    plan = compute_plan(
        db,
        _import_params(
            paths,
            model_ids=["m1"],
            hash_duplicate_paths=list(paths),
            local_processing=True,
            skip_eye_keypoints=False,
            skip_regroup=False,
        ),
        str(tmp_path / "test.db"),
    )
    assert plan["scope"]["new_count"] == 0
    assert plan["scope"]["known_count"] == 2
    for suffix in ("Previews", "Classify", "Extract", "EyeKeypoints"):
        stage = plan["stages"][suffix]
        assert stage["state"] == "will-run", (suffix, stage)
        assert "0 new photos to import" in stage["summary"], (suffix, stage)
        assert stage["detail"]["import_no_new"] is True, (suffix, stage)
    group = plan["stages"]["Group"]
    assert group["state"] == "will-skip", group
    assert "nothing to group" in group["summary"], group
    assert "0 new photos to import" in group["summary"], group
    assert group["detail"]["import_no_new"] is True, group
    assert group["detail"]["upstream_will_run"] is False, group
    assert "cache_exists" in group["detail"], group


def test_import_plan_all_duplicates_copy_mode_keeps_forward_summaries(
    tmp_path, monkeypatch
):
    """Contrast case for the above: same all-duplicates selection but in
    plain copy mode (local_processing off). Here "nothing to …" would
    overclaim: with no copied paths the post-ingest scan runs with
    restrict=None over the REAL destination, and scanner.scan fires the
    photo callback for existing cataloged rows there — so downstream
    workspace-scoped stages can still find real work among
    previously-unprocessed destination photos. The plan must keep the
    pre-existing forward-looking summaries and let Group see upstream
    as will-run."""
    import pipeline as pipeline_mod
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    _stub_models(monkeypatch, tmp_path)
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    paths = ["/cards/SD/IMG_001.NEF", "/cards/SD/IMG_002.NEF"]
    plan = compute_plan(
        db,
        _import_params(
            paths,
            model_ids=["m1"],
            hash_duplicate_paths=list(paths),
            skip_eye_keypoints=False,
            skip_regroup=False,
        ),
        str(tmp_path / "test.db"),
    )
    assert plan["scope"]["new_count"] == 0
    assert plan["scope"]["known_count"] == 2
    for suffix in ("Previews", "Classify", "Extract", "EyeKeypoints"):
        stage = plan["stages"][suffix]
        assert stage["state"] == "will-run", (suffix, stage)
        assert "0 new photos to import" not in stage["summary"], (suffix, stage)
        assert not stage["detail"].get("import_no_new"), (suffix, stage)
    assert "no photos in scope yet" in plan["stages"]["Previews"]["summary"]
    group = plan["stages"]["Group"]
    assert group["state"] == "will-run"
    assert "upstream stages have new work" in group["summary"], group


@pytest.mark.skip(reason="retired pipeline local-processing import planner mode")
def test_import_plan_with_new_files_keeps_forward_looking_summaries(
    tmp_path, monkeypatch
):
    """Contrast case: an import that DOES bring new files must keep the
    counting summaries and never claim "0 new photos to import" — even in
    local-processing mode, where the all-duplicates wording is allowed."""
    import pipeline as pipeline_mod
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    _stub_models(monkeypatch, tmp_path)
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    plan = compute_plan(
        db,
        _import_params(
            ["/cards/SD/IMG_001.NEF", "/cards/SD/IMG_002.NEF"],
            model_ids=["m1"],
            local_processing=True,
            skip_eye_keypoints=False,
            skip_regroup=False,
        ),
        str(tmp_path / "test.db"),
    )
    assert plan["scope"]["new_count"] == 2
    for suffix in ("Previews", "Classify", "Extract", "EyeKeypoints"):
        stage = plan["stages"][suffix]
        assert stage["state"] == "will-run", (suffix, stage)
        assert "0 new photos to import" not in stage["summary"], (suffix, stage)
        assert not stage["detail"].get("import_no_new"), (suffix, stage)
    assert plan["stages"]["Group"]["state"] == "will-run"


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
    assert scan["detail"]["unlinked_folders"] == 0


def test_import_plan_all_known_but_folder_unlinked_to_active_workspace(tmp_path):
    """Files known globally but whose folder is not yet linked to the
    active workspace must NOT report Scan as done-prior.

    Re-importing files that were indexed in another workspace lands in
    this branch. ``scanner.scan`` calls ``_ensure_folder`` for every
    walked directory, and ``add_folder`` auto-links to the active
    workspace via ``workspace_folders`` — so the next pipeline run will
    mutate workspace state by attaching the folder (and thereby making
    those photos visible here). Claiming "Already done" hides that work
    from the user, exactly the misleading-pill failure
    CORE_PHILOSOPHY.md prohibits.
    """
    from db import Database
    from pipeline_plan import compute_plan
    db = Database(str(tmp_path / "test.db"))

    # Photos imported under workspace A.
    ws_a = db.create_workspace("WorkspaceA")
    db._active_workspace_id = ws_a
    fid = db.add_folder("/cards/A")  # auto-linked to WorkspaceA
    db.add_photo(folder_id=fid, filename="IMG_001.NEF",
                 extension=".nef", file_size=1, file_mtime=1.0)

    # Switch active workspace to a fresh B that has no link to /cards/A.
    db._active_workspace_id = db.create_workspace("WorkspaceB")

    plan = compute_plan(
        db,
        _import_params(
            ["/cards/A/IMG_001.NEF"],
            skip_classify=True, skip_extract_masks=True,
            skip_eye_keypoints=True, skip_regroup=True,
        ),
        str(tmp_path / "test.db"),
    )
    scan = plan["stages"]["Scan"]
    # Scan must run — it will INSERT INTO workspace_folders for /cards/A.
    assert scan["state"] == "will-run", scan
    assert scan["detail"]["new_photos"] == 0
    assert scan["detail"]["already_known"] == 1
    assert scan["detail"]["unlinked_folders"] == 1
    # Summary must name the linking work, not pretend it's a no-op.
    assert "elsewhere" in scan["summary"].lower()
    assert "link" in scan["summary"].lower()


def test_import_plan_all_known_with_folder_linked_stays_done_prior(tmp_path):
    """Same workspace re-import: folder is linked to the active workspace,
    so scan really is a no-op. The unlinked-folder check must not flip
    these into will-run.
    """
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    fid = db.add_folder("/cards/A")  # auto-links to active WS
    db.add_photo(folder_id=fid, filename="IMG_001.NEF",
                 extension=".nef", file_size=1, file_mtime=1.0)

    plan = compute_plan(
        db,
        _import_params(
            ["/cards/A/IMG_001.NEF"],
            skip_classify=True, skip_extract_masks=True,
            skip_eye_keypoints=True, skip_regroup=True,
        ),
        str(tmp_path / "test.db"),
    )
    scan = plan["stages"]["Scan"]
    assert scan["state"] == "done-prior", scan
    assert scan["detail"]["unlinked_folders"] == 0


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


def test_api_pipeline_plan_rejects_retired_import_fields(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/pipeline/plan",
        json={"source_paths": [], "local_processing": True},
    )
    assert resp.status_code == 400
    assert "import/archive fields" in resp.get_json()["error"]


def test_import_plan_empty_source_paths_is_no_op(tmp_path, monkeypatch):
    """Import / new-images mode with every preview file deselected.

    Per-photo stages (Classify, Extract, ...) are genuine no-ops because
    they only operate on imported photos and the import set is empty —
    they must not leak active-workspace state.

    Scan, however, must NOT report "will-skip": the runtime still walks
    the source/destination tree regardless of selection, so claiming a
    no-op there violates CORE_PHILOSOPHY.md's "show what's happening"
    rule. Scan reads as "will-run" with a summary explaining that
    selection only affects which files import.
    """
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _stub_models(monkeypatch)
    # Active-workspace state that would otherwise dominate the plan:
    # one fully-classified, fully-masked photo. Without our fix this
    # would render as "done-prior" for Classify and Extract.
    pid, did = _add_photo_with_detection(db, folder_id, "old.jpg")
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=1)
    db.conn.execute("UPDATE photos SET mask_path='/m/old.png' WHERE id=?", (pid,))
    db.conn.commit()

    # Empty source_paths = import mode, all files deselected.
    plan = compute_plan(
        db,
        _import_params([], model_ids=["m1"]),
        str(tmp_path / "test.db"),
    )
    # Per-photo stages are no-ops — none leak active-workspace state.
    for name in ("Classify", "Extract"):
        assert plan["stages"][name]["state"] == "will-skip", (
            f"{name} leaked workspace state: {plan['stages'][name]}"
        )
        assert "no files selected" in plan["stages"][name]["summary"].lower()
    # Scan honestly reports it will still run (directory walk + hashing),
    # not "will-skip", since the runtime doesn't actually no-op here.
    scan = plan["stages"]["Scan"]
    assert scan["state"] == "will-run", scan
    assert "scan" in scan["summary"].lower()
    assert plan["scope"]["new_count"] == 0
    assert plan["scope"]["known_count"] == 0
    assert plan["scope"]["photo_count"] == 0


def test_compute_plan_distinguishes_none_from_empty_source_paths(tmp_path, monkeypatch):
    """source_paths=None (whole-workspace) and source_paths=[] (empty
    import) must produce different plans. Conflating them re-introduces
    the cross-workspace status leak the import-mode plan exists to fix.
    """
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import PipelinePlanParams, compute_plan
    db, folder_id = _make_db(tmp_path)
    _stub_models(monkeypatch)
    pid, did = _add_photo_with_detection(db, folder_id, "old.jpg")
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=1)
    _mark_sam_done(db, pid, "/m/old.png")

    # source_paths=None → whole-workspace fallback (Extract sees the
    # masked photo and reports "done-prior").
    whole_ws = compute_plan(
        db,
        PipelinePlanParams(
            source_paths=None, model_ids=["m1"],
            skip_eye_keypoints=True, skip_regroup=True,
        ),
        str(tmp_path / "test.db"),
    )
    assert "Scan" not in whole_ws["stages"]  # not import mode
    assert whole_ws["stages"]["Extract"]["state"] == "done-prior"

    # source_paths=[] → import mode. Scan emitted as will-run (runtime
    # still walks); Extract is "will-skip — No files selected", NOT
    # "done-prior" (would leak active-workspace state).
    empty_import = compute_plan(
        db,
        PipelinePlanParams(
            source_paths=[], model_ids=["m1"],
            skip_eye_keypoints=True, skip_regroup=True,
        ),
        str(tmp_path / "test.db"),
    )
    assert "Scan" in empty_import["stages"]
    assert empty_import["stages"]["Scan"]["state"] == "will-run"
    assert empty_import["stages"]["Extract"]["state"] == "will-skip"


def test_api_pipeline_plan_empty_source_paths_does_not_fall_back(app_and_db):
    """API-level guard: posting an explicit empty source_paths list (the
    frontend's signal for "import mode, every preview file deselected")
    must produce the no-op import plan, not whole-workspace fallback.

    Codex flagged this exact regression — dropping the field client-side
    when no files are selected made /api/pipeline/plan describe the
    unrelated active workspace, which is the misleading behaviour the
    import-mode scope is meant to prevent.
    """
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/pipeline/plan",
        json={
            "source_paths": [],  # explicit empty == import mode, no files
            "skip_classify": True, "skip_extract_masks": True,
            "skip_eye_keypoints": True, "skip_regroup": True,
        },
    )
    assert resp.status_code == 200
    data = resp.get_json()
    # Scan stage emitted (= import mode active). It reads as "will-run"
    # because the runtime still walks the tree regardless of selection;
    # claiming "will-skip" would lie about substantial directory work.
    assert "Scan" in data["stages"]
    assert data["stages"]["Scan"]["state"] == "will-run"
    assert data["scope"]["new_count"] == 0
    assert data["scope"]["known_count"] == 0


def test_api_pipeline_plan_missing_source_paths_uses_whole_workspace(app_and_db):
    """The mirror of the empty-list case: when the client doesn't send
    source_paths at all (e.g. collection or workspace mode), the API
    must fall through to whole-workspace scope. No Scan entry is
    emitted because the run isn't an import.
    """
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/pipeline/plan",
        json={
            # no source_paths key at all
            "skip_classify": True, "skip_extract_masks": True,
            "skip_eye_keypoints": True, "skip_regroup": True,
        },
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert "Scan" not in data["stages"]


# -------- compute_plan: copy-mode hash-dedup honesty --------
#
# Copy-mode imports run with skip_duplicates=True dedupe by file_hash via
# ingest(), not by source path. The frontend pre-computes hash matches via
# /api/import/check-duplicates and passes them back in hash_duplicate_paths.
# These paths must count as "already known", not "new" — otherwise pills
# overstate work for files ingest() will silently skip (the common case:
# a card was already imported once into a different destination folder).

def test_import_plan_hash_duplicates_count_as_known(tmp_path, monkeypatch):
    """Hash-matched source paths shift from new_count to known_count, so
    Scan/Classify/Extract estimates reflect what ingest will actually do.

    Setup mirrors a real copy-mode re-import: the library has one photo at
    a destination path; the user is re-importing two source files, one of
    which is byte-identical to the existing photo (same hash, different
    source path) and one of which is genuinely new. The frontend's prior
    check-duplicates pass has flagged the matching source path.
    """
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    _stub_models(monkeypatch)

    plan = compute_plan(
        db,
        _import_params(
            ["/cards/SD/IMG_DUP.NEF", "/cards/SD/IMG_NEW.NEF"],
            hash_duplicate_paths=["/cards/SD/IMG_DUP.NEF"],
            model_ids=["m1"],
        ),
        str(tmp_path / "test.db"),
    )
    assert plan["scope"]["new_count"] == 1, plan["scope"]
    assert plan["scope"]["known_count"] == 1, plan["scope"]
    scan = plan["stages"]["Scan"]
    assert scan["detail"]["new_photos"] == 1
    assert scan["detail"]["already_known"] == 1


def test_import_plan_hash_duplicates_outside_selection_ignored(tmp_path, monkeypatch):
    """``_duplicateResults`` survives source-folder edits; a path in
    hash_duplicate_paths that's no longer in source_paths is a stale
    cache entry and must NOT influence counts.
    """
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    _stub_models(monkeypatch)

    plan = compute_plan(
        db,
        _import_params(
            ["/cards/SD/IMG_NEW.NEF"],  # one selected, all new
            hash_duplicate_paths=[
                "/cards/SD/IMG_NEW.NEF",      # NOT a dup but in cache (stale)
                "/cards/SD/IMG_OLD.NEF",      # not even in source_paths
            ],
            model_ids=["m1"],
        ),
        str(tmp_path / "test.db"),
    )
    # IMG_NEW is in source_paths AND in hash_duplicate_paths, so it's
    # counted as known. IMG_OLD isn't in source_paths so it's ignored.
    assert plan["scope"]["new_count"] == 0
    assert plan["scope"]["known_count"] == 1


def test_import_plan_hash_dup_also_known_at_path_not_double_counted(tmp_path, monkeypatch):
    """A file already known at its exact source path (re-pointing at an
    already-imported folder) is also a hash match. It must count toward
    known_count exactly once — double-counting would push known_count
    past the total file count.
    """
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)
    _stub_models(monkeypatch)
    fid = db.add_folder("/cards/A")
    db.add_photo(folder_id=fid, filename="IMG_001.NEF",
                 extension=".nef", file_size=1, file_mtime=1.0)

    plan = compute_plan(
        db,
        _import_params(
            ["/cards/A/IMG_001.NEF"],
            hash_duplicate_paths=["/cards/A/IMG_001.NEF"],  # also a hash match
            model_ids=["m1"],
        ),
        str(tmp_path / "test.db"),
    )
    assert plan["scope"]["new_count"] == 0
    assert plan["scope"]["known_count"] == 1  # not 2


def test_import_plan_hash_duplicate_does_not_link_source_folder(tmp_path, monkeypatch):
    """In copy mode, ingest skips a hash-matched source file entirely —
    its source folder is never walked by the post-import scan (only the
    destination is). So Scan must NOT report the source folder as
    "needs linking" for hash-dup-only paths.

    BUT: scan is still not a no-op. The post-ingest scan walks the
    destination folders that hold the existing copies (and walks the
    entire destination root if those copies live elsewhere), which can
    link previously-unseen destination folders to the active workspace.
    The plan must report will-run with a summary that names this
    destination-side work — claiming "done-prior" would lie about real
    state changes the user is about to trigger.
    """
    from db import Database
    from pipeline_plan import compute_plan
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.create_workspace("WorkspaceA")
    db._active_workspace_id = ws_a
    # Photo lives at /library/2025 in WorkspaceA. Folder /cards/SD has
    # never been seen by the DB, so it would normally count as "unlinked".
    fid = db.add_folder("/library/2025")
    db.add_photo(folder_id=fid, filename="dest.NEF",
                 extension=".nef", file_size=1, file_mtime=1.0)
    db._active_workspace_id = db.create_workspace("WorkspaceB")

    plan = compute_plan(
        db,
        _import_params(
            ["/cards/SD/IMG_HASH.NEF"],  # only file is a hash dup
            hash_duplicate_paths=["/cards/SD/IMG_HASH.NEF"],
            skip_classify=True, skip_extract_masks=True,
            skip_eye_keypoints=True, skip_regroup=True,
        ),
        str(tmp_path / "test.db"),
    )
    scan = plan["stages"]["Scan"]
    # Source folder isn't walked (ingest skips the file entirely), so
    # ``unlinked_folders`` for source paths stays 0.
    assert scan["detail"]["unlinked_folders"] == 0, scan
    # But the destination is walked, so plan must surface will-run.
    assert scan["state"] == "will-run", scan
    assert scan["detail"]["hash_duplicates"] == 1, scan
    assert "hash-duplicate" in scan["summary"], scan
    assert "destination" in scan["summary"], scan


def test_import_plan_hash_duplicate_with_unlinked_source_folder(tmp_path, monkeypatch):
    """Mixed signal: some import paths are hash dups (copy mode skips them
    + walks destination), and one path resolves to an existing photo whose
    source folder isn't linked to the active workspace yet (scan will link
    it). The plan must surface BOTH reasons in the summary so the user
    sees what mutates and isn't told a misleading "Already done".
    """
    from db import Database
    from pipeline_plan import compute_plan
    db = Database(str(tmp_path / "test.db"))
    ws_a = db.create_workspace("WorkspaceA")
    db._active_workspace_id = ws_a
    # Existing photo at /cards/A/IMG_OLD.NEF — its folder is in WorkspaceA
    # only. Re-importing into WorkspaceB will link /cards/A here.
    fid_a = db.add_folder("/cards/A")
    db.add_photo(folder_id=fid_a, filename="IMG_OLD.NEF",
                 extension=".nef", file_size=1, file_mtime=1.0)
    db._active_workspace_id = db.create_workspace("WorkspaceB")

    plan = compute_plan(
        db,
        _import_params(
            ["/cards/A/IMG_OLD.NEF", "/cards/B/IMG_HASH.NEF"],
            hash_duplicate_paths=["/cards/B/IMG_HASH.NEF"],
            skip_classify=True, skip_extract_masks=True,
            skip_eye_keypoints=True, skip_regroup=True,
        ),
        str(tmp_path / "test.db"),
    )
    scan = plan["stages"]["Scan"]
    assert scan["state"] == "will-run", scan
    assert scan["detail"]["unlinked_folders"] >= 1, scan
    assert scan["detail"]["hash_duplicates"] == 1, scan
    # Summary must mention both the source-link work and the
    # destination-walk work, so the user knows what's about to mutate.
    assert "hash-duplicate" in scan["summary"], scan
    assert "destination" in scan["summary"], scan


def test_api_pipeline_plan_rejects_oversized_hash_duplicate_paths(app_and_db):
    """Same 50k cap as source_paths. Without this, a misbehaving client
    could OOM the server by passing arbitrarily large hash-dup lists."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/pipeline/plan",
        json={
            "source_paths": ["/x/0"],
            "hash_duplicate_paths": ["/x/" + str(i) for i in range(50001)],
            "skip_classify": True, "skip_extract_masks": True,
            "skip_eye_keypoints": True, "skip_regroup": True,
        },
    )
    assert resp.status_code == 400


def test_api_pipeline_plan_rejects_non_list_hash_duplicate_paths(app_and_db):
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/pipeline/plan",
        json={
            "source_paths": ["/x/0"],
            "hash_duplicate_paths": "/single/path.nef",
            "skip_classify": True, "skip_extract_masks": True,
            "skip_eye_keypoints": True, "skip_regroup": True,
        },
    )
    assert resp.status_code == 400


def test_api_pipeline_plan_rejects_non_string_hash_duplicate_paths_elements(app_and_db):
    """Mixed-type list must be rejected at the API boundary; otherwise
    set membership in compute_plan would behave unpredictably for
    non-hashable / non-string entries.
    """
    app, _ = app_and_db
    client = app.test_client()
    resp = client.post(
        "/api/pipeline/plan",
        json={
            "source_paths": ["/x/0"],
            "hash_duplicate_paths": ["/ok/path.nef", 123],
            "skip_classify": True, "skip_extract_masks": True,
            "skip_eye_keypoints": True, "skip_regroup": True,
        },
    )
    assert resp.status_code == 400


def test_exclusions_apply_in_whole_workspace_mode(tmp_path):
    """The running job filters exclude_photo_ids in every mode; the plan
    previously honored them only for collections, overstating pending
    counts for whole-workspace runs."""
    from pipeline_plan import PipelinePlanParams, compute_plan

    db, folder_id = _make_db(tmp_path)
    p1, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    p2, _ = _add_photo_with_detection(db, folder_id, "b.jpg")

    base = compute_plan(db, PipelinePlanParams(), str(tmp_path / "test.db"))
    assert base["stages"]["Extract"]["detail"]["pending"] == 2

    plan = compute_plan(
        db,
        PipelinePlanParams(exclude_photo_ids=[p2]),
        str(tmp_path / "test.db"),
    )
    assert plan["stages"]["Extract"]["detail"]["pending"] == 1
    assert plan["scope"]["photo_count"] == 1
