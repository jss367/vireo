"""Subject detection for a classify run.

``_detect_subjects`` runs MegaDetector over the collection (reusing cached
detector runs), analyzes the retained subjects, and narrows the detections to
the classifier's candidates.
"""

import math
import os
import sqlite3
import time

from classify_job.common import compute_sharpness, detect_animals, get_primary_detection, log
from resource_ledger import ResourceWaitCancelled


def _stored_detection_list(db, photo_id, min_conf=None):
    """Return a photo's stored detection rows as ``detection_map`` entries.

    ``min_conf`` is forwarded to ``db.get_detections`` (``None`` means the
    workspace's ``detector_confidence``). A read failure yields ``[]``.
    """
    try:
        rows = (
            db.get_detections(photo_id) if min_conf is None
            else db.get_detections(photo_id, min_conf=min_conf)
        )
    except Exception:
        log.warning(
            "Could not read stored detections for photo %s; treating it as having none",
            photo_id, exc_info=True,
        )
        return []
    return [{
        "id": d["id"],
        "box_x": d["box_x"],
        "box_y": d["box_y"],
        "box_w": d["box_w"],
        "box_h": d["box_h"],
        "confidence": d["detector_confidence"],
        "category": d["category"],
        # sqlite3.Row supports [key] but lacks .get(), and ``key in row`` is
        # not supported either; .keys() is the documented contains-check.
        # Fallback to None so test mocks (plain dicts without
        # detector_model) don't crash this path.
        "detector_model": (
            d["detector_model"]
            if "detector_model" in d.keys()  # noqa: SIM118
            else None
        ),
    } for d in rows]


class _DetectBatchRun:
    """Run-wide state of one ``_detect_batch`` call.

    ``det_conf_threshold`` is resolved lazily by ``_resolve_threshold`` and
    read back by the reclassify subject sync and the subject-analysis pass,
    so both see the value the detection loop settled on.
    """

    def __init__(self, photos, folders, job, reclassify, db,
                 det_conf_threshold, already_detected_ids, cached_detections):
        self.photos = photos
        self.folders = folders
        self.job = job
        self.reclassify = reclassify
        self.db = db
        self.det_conf_threshold = det_conf_threshold
        self.already_detected_ids = already_detected_ids
        self.cached_detections = cached_detections
        self.detected = 0
        self.detection_map = {}
        self.processed_ids: set[int] = set()

    def sync_reclassified_subjects(self):
        # Reclassification already committed detection replacements (or a
        # clear in the standalone caller). Cleanup is required even on Stop;
        # it performs no image decoding or additional inference.
        if not self.reclassify:
            return
        from pipeline_locks import acquire_photo_mask
        from subjects import sync_primary
        for photo in self.photos:
            with acquire_photo_mask(photo["id"]):
                sync_primary(self.db, photo["id"], min_conf=self.det_conf_threshold)
                self.db.commit_with_retry()

    def analyze_subjects(self, runner, vireo_dir):
        """Analyze every retained subject after the detection loop.

        Includes cached detector runs from older libraries.
        Detection/classification results remain usable if a source goes
        offline; missing analyses are retried on the next run.
        """
        job = self.job
        # Cancellation participates cooperatively: the outer detect loop honors
        # ``runner.is_cancelled`` and re-classify has already cleared the prior
        # detection rows for photos the user asked to stop. Committing more
        # subject analyses after cancel would silently publish work past the
        # stop button, so probe before each photo and inside ``analyze_photo``'s
        # lock-guarded commit points via the checkpoint callback.
        def _subject_analysis_checkpoint():
            if runner is not None and runner.is_cancelled(job["id"]):
                raise ResourceWaitCancelled(
                    "Cancelled during subject analysis"
                )

        try:
            _analyze_subjects(
                self.photos, self.folders, self.db, self.reclassify,
                self.det_conf_threshold, self.cached_detections,
                self.processed_ids, vireo_dir,
                _subject_analysis_checkpoint,
            )
        except ResourceWaitCancelled as exc:
            # Detection rows for these photos are already committed, so the
            # caller must still see them as processed: a reclassify Stop
            # rebuilds predictions for exactly the photos whose boxes were
            # replaced, and the in-flight photo is one of them.
            _attach_partial_detect_result(
                exc, self.detection_map, self.detected, self.processed_ids,
            )
            raise

    def mark_cached_photos_for_analysis(self):
        """Queue cached detector runs for subject analysis without a detector.

        Detector module unavailable. Skip the detection call itself,
        but the subject-analysis backfill below can still run over
        photos that already have cached detection rows — analyzing
        cached boxes doesn't require detect_animals/get_primary_detection,
        so cached-only installations must still receive subject
        quality/crop/exposure data (Codex r4056698679).
        """
        for photo in self.photos:
            if photo["id"] in self.cached_detections:
                continue
            try:
                if (photo["id"] in self.already_detected_ids
                        or self.db.get_detections(photo["id"], min_conf=0)):
                    # Empty and now-subthreshold cached runs still need
                    # to clear outputs from their former primary.
                    self.processed_ids.add(photo["id"])
            except sqlite3.Error:
                log.warning(
                    "Could not read cached detections for photo %s; "
                    "skipping its subject-analysis backfill",
                    photo["id"], exc_info=True,
                )

    def detect_photo(self, photo):
        """Detect one photo, or reuse its prior detector run."""
        folder_path = self.folders.get(photo["folder_id"], "")
        image_path = os.path.join(folder_path, photo["filename"])

        # Skip if already detected (unless reclassifying). After the
        # detector_runs migration, `already_detected_ids` includes
        # empty-scene photos (box_count=0) — we must not re-invoke
        # MegaDetector for them either. Either the detector produced
        # rows (reuse them) or it ran and found nothing (skip entirely).
        if not self.reclassify and photo["id"] in self.already_detected_ids:
            self._reuse_prior_detections(photo)
            return

        self._resolve_threshold()

        detections = detect_animals(image_path)

        if detections is None:
            # Detector run itself failed (image decode error, ONNX
            # error, etc.). Do NOT clear prior detections and do NOT
            # record a run — otherwise future non-reclassify passes
            # would skip the photo permanently, leaving it without
            # detections unless the user forces --reclassify.
            # The photo stays out of processed_ids so the caller
            # treats it as "will be retried next pass".
            return

        # Persist detection rows and record the detector run atomically —
        # `write_detection_batch` wraps both writes in one transaction so a
        # crash between them can't leave a torn state (detections without a
        # matching detector_runs row, or the reverse for empty scenes).
        # Failures from `detect_animals` were handled by the ``is None``
        # early-continue above and must not poison the skip set.
        detector_runtime = self.job.get("_detector_runtime_fingerprint") or "legacy"
        portable_input, portable_photo_hash = self._portable_identity(
            photo, detector_runtime,
        )
        det_ids = self.db.write_detection_batch(
            photo["id"], "megadetector-v6", detections,
            runtime_fingerprint=detector_runtime,
            input_fingerprint=portable_input,
            force_runtime_replace=self.reclassify,
        )

        if portable_input is not None:
            self._publish_portable_result(
                photo, detector_runtime, portable_photo_hash,
            )

        if detections:
            self.detected += 1
            self.detection_map[photo["id"]] = self._detection_list(
                photo, detections, det_ids,
            )

            # Mark as processed immediately after detection rows are committed
            # so that even if the quality-scoring calls below raise, the
            # reclassify purge in pipeline_job correctly removes the now-stale
            # pre-run detection rows for this photo rather than leaving them in
            # place and allowing future non-reclassify runs to reuse them.
            self.processed_ids.add(photo["id"])

            self._score_subject_quality(photo, image_path, detections)

        self.processed_ids.add(photo["id"])

    def _reuse_prior_detections(self, photo):
        # Prefer cached detections from an earlier model in this
        # same pipeline run so that model 2+ is bound to the
        # detection rows just produced, not stale rows from a
        # prior pipeline pass that db.get_detections() would
        # return when old rows haven't been cleared.
        if self.cached_detections is not None and photo["id"] in self.cached_detections:
            det_list = self.cached_detections[photo["id"]]
            if det_list:
                self.detection_map[photo["id"]] = det_list
                self.detected += 1
            self.processed_ids.add(photo["id"])
            return
        # Pull cached rows if any; an empty result means this photo
        # was scanned and had no animals, which is still a skip.
        # (Task 20 will add a min_conf filter to get_detections.)
        det_list = _stored_detection_list(self.db, photo["id"])
        if det_list:
            self.detection_map[photo["id"]] = det_list
            self.detected += 1
        self.processed_ids.add(photo["id"])

    def _resolve_threshold(self):
        # Resolve workspace-effective threshold lazily on first actual
        # detection call so a batch where every photo hits the
        # cached/already-detected short-circuit doesn't need a working
        # config/db at all (the cached-detections short-circuit test
        # relies on this).
        #
        # The threshold is NOT passed to detect_animals — detector writes
        # everything above RAW_CONF_FLOOR so results can be globally
        # cached. The effective threshold is applied as a read-time
        # filter by get_detections / stats queries (Tasks 20-22).
        if self.det_conf_threshold is None:
            import config as cfg
            effective_cfg = self.db.get_effective_config(cfg.load())
            self.det_conf_threshold = effective_cfg.get("detector_confidence", 0.2)

    def _portable_identity(self, photo, detector_runtime):
        """Return ``(input_fingerprint, photo_hash)`` for a portable run.

        Both are ``None`` for the legacy runtime, companion-backed photos,
        and photos whose hash cannot fingerprint a source input.
        """
        portable_input = None
        portable_photo_hash = None
        if detector_runtime != "legacy":
            identity_row = self.db.model_runs.detector_source_identity(
                photo["id"],
            )
            if (
                identity_row is not None
                and not identity_row["companion_path"]
            ):
                try:
                    from computation_cache import source_input

                    portable_photo_hash = identity_row["file_hash"]
                    _input_block, portable_input = source_input(
                        portable_photo_hash, "vireo-detector-source-v1",
                    )
                except (TypeError, ValueError):
                    portable_input = None
                    portable_photo_hash = None
        return portable_input, portable_photo_hash

    def _publish_portable_result(self, photo, detector_runtime, portable_photo_hash):
        """Publish the committed detector result as a portable artifact.

        Publication follows the database commit.  A store failure must
        never turn successful inference into a failed detector run.

        ``write_detection_batch`` returns early (leaving the stored
        detector_runs.runtime_fingerprint at its OLD value) when a
        runtime change is proposed against a review-pinned run and
        ``force_runtime_replace`` is False.  Publishing with the new
        ``detector_runtime`` in that case would attach a foreign
        runtime identity to boxes that were actually produced by an
        older runtime — the artifact is content-addressed and
        exportable, so downstream catalogs would trust that lie.
        Read back the persisted runtime and only publish when it
        equals the runtime we intended to write.
        """
        db = self.db
        try:
            from computation_cache import publish_detection_artifact

            persisted_runtime = db.model_runs.detector_runtime_fingerprint(
                photo["id"], "megadetector-v6",
            )
            if persisted_runtime == detector_runtime:
                normalized_detections = [{
                    "box": {
                        "x": row["box_x"], "y": row["box_y"],
                        "w": row["box_w"], "h": row["box_h"],
                    },
                    "confidence": row["detector_confidence"],
                    "category": row["category"],
                } for row in sorted(db.get_detections(
                    photo["id"], min_conf=0,
                    detector_model="megadetector-v6",
                ), key=lambda row: (-row["detector_confidence"], row["id"]))]
                # Reconstruct the configured ArtifactStore from
                # the path stashed by ``run_classify_job`` /
                # ``run_pipeline_job`` so newly published detector
                # artifacts honor ``COMPUTATION_CACHE_DIR``
                # instead of landing in the default location.
                from computation_cache import ArtifactStore
                _cache_dir = self.job.get("_computation_cache_dir")
                _publish_store = (
                    ArtifactStore(_cache_dir) if _cache_dir else None
                )
                publish_detection_artifact(
                    portable_photo_hash,
                    "megadetector-v6",
                    detector_runtime,
                    normalized_detections,
                    store=_publish_store,
                )
        except Exception:
            log.warning(
                "Could not publish portable detector result for photo %s",
                photo["id"], exc_info=True,
            )

    def _detection_list(self, photo, detections, det_ids):
        """Build the ``detection_map`` entries with database IDs.

        Content-addressed IDs can collapse near-duplicate detector outputs
        into one persisted row, so fall back to the DB rows if the returned
        ID count no longer matches the raw detector output count.
        """
        det_list = []
        if len(det_ids) == len(detections):
            for det, det_id in zip(detections, det_ids, strict=True):
                det_list.append({
                    "id": det_id,
                    "box_x": det["box"]["x"],
                    "box_y": det["box"]["y"],
                    "box_w": det["box"]["w"],
                    "box_h": det["box"]["h"],
                    "confidence": det["confidence"],
                    "category": det.get("category", "animal"),
                    "detector_model": "megadetector-v6",
                })
        else:
            for det in self.db.get_detections(
                photo["id"], min_conf=0,
                detector_model="megadetector-v6",
            ):
                det_list.append({
                    "id": det["id"],
                    "box_x": det["box_x"],
                    "box_y": det["box_y"],
                    "box_w": det["box_w"],
                    "box_h": det["box_h"],
                    "confidence": det["detector_confidence"],
                    "category": det["category"],
                    "detector_model": det["detector_model"],
                })
        return det_list

    def _score_subject_quality(self, photo, image_path, detections):
        """Score subject quality from the primary detection."""
        db = self.db
        # Use highest-confidence detection as primary for quality scoring
        primary = get_primary_detection(detections)
        if primary and primary["confidence"] < self.det_conf_threshold:
            # Top detection is below the workspace's detector_confidence
            # threshold — it's noise, not a real subject. Skip scoring
            # and clear any stale quality fields from a prior run so
            # noise photos don't float to the top of highlights with a
            # giant ``subject_size`` from a whole-frame noise box.
            db.photos.update_quality(photo["id"])
            primary = None
        if not primary:
            return
        det_box = primary["box"]
        subject_size = det_box["w"] * det_box["h"]

        if compute_sharpness is None:
            db.photos.update_quality(
                photo["id"],
            )
            return

        overall_sharpness = compute_sharpness(image_path)
        subject_sharpness = None
        quality = 0

        try:
            from PIL import Image

            img = Image.open(image_path)
            try:
                iw, ih = img.size
                px = int(det_box["x"] * iw)
                py = int(det_box["y"] * ih)
                pw = int(det_box["w"] * iw)
                ph = int(det_box["h"] * ih)
                subject_sharpness = compute_sharpness(
                    image_path, region=(px, py, pw, ph)
                )
            finally:
                img.close()
        except Exception:
            log.debug(
                "Subject-region sharpness failed for photo %s; "
                "using whole-frame sharpness",
                photo["id"], exc_info=True,
            )
            subject_sharpness = overall_sharpness

        if subject_sharpness is not None and subject_size is not None:
            norm_sharp = min(1.0, math.log1p(subject_sharpness) / 10.0)
            norm_size = min(1.0, subject_size * 4)
            quality = round(0.7 * norm_sharp + 0.3 * norm_size, 4)

        db.photos.update_quality(
            photo["id"],
            subject_sharpness=subject_sharpness,
            subject_size=subject_size,
            quality_score=quality,
            sharpness=overall_sharpness,
        )


def _detect_batch(photos, folders, runner, job, reclassify, db,
                   det_conf_threshold=None, already_detected_ids=None,
                   cached_detections=None, vireo_dir=None):
    """Run MegaDetector on a batch of photos.

    Same interface as _detect_subjects but designed to be called with
    partial photo lists for interleaved detect+classify in the streaming
    pipeline.  Does NOT push progress events — that is the caller's
    responsibility.

    Args:
        det_conf_threshold: Detection confidence threshold. If None,
            loaded from config (fallback for callers that don't pre-load).
        already_detected_ids: Set of photo IDs that already have detections
            in the database. Used for skip-if-already-detected logic.
        cached_detections: Optional dict {photo_id: [detection_dicts]}
            produced by a prior model in the same pipeline run. When
            provided and a photo is in already_detected_ids, the cached
            entries are used instead of db.get_detections() so that
            model 2+ binds to the exact detection rows from this run,
            not stale rows from a previous pipeline pass.
        vireo_dir: optional path to ~/.vireo/; when set, the subject
            analysis loop resolves each photo through the working-copy
            JPEG when its source folder is offline, matching the
            on-demand ``/api/photos/<id>/subjects/analyze`` route.
            Without this, an offline NAS would leave cached-detection
            photos permanently missing their subject quality, crop and
            exposure data.

    Returns:
        (detection_map, detected_count, processed_ids) where detection_map
        is {photo_id: [list_of_detection_dicts]}, detected_count is total
        photos with at least one detection, and processed_ids is the set
        of photo IDs whose per-photo iteration completed without raising
        (callers use this to distinguish "ran and found nothing" from
        "never reached because an earlier photo raised mid-loop").
    """
    if already_detected_ids is None:
        already_detected_ids = set()
    if cached_detections is None:
        cached_detections = {}
    run = _DetectBatchRun(
        photos, folders, job, reclassify, db, det_conf_threshold,
        already_detected_ids, cached_detections,
    )

    try:
        if detect_animals is None or get_primary_detection is None:
            run.mark_cached_photos_for_analysis()
            detection_photos: list = []
        else:
            detection_photos = photos

        for photo in detection_photos:
            run.detect_photo(photo)

    except ResourceWaitCancelled as exc:
        # Cooperative cancellation during a MegaDetector inference-lease
        # wait — the reclassify Stop path. Must propagate: the caller
        # already called ``db.detections.clear(photo["id"])`` for this
        # photo on the reclassify path (classify_job.py:1073), so if
        # this cancel were swallowed under the broad ``RuntimeError``
        # arm below, the classify recovery would then rebuild
        # predictions using the full-image fallback and land a
        # committed catalog change even though the user pressed Stop.
        # ``ResourceWaitCancelled`` subclasses ``RuntimeError``, so
        # this narrow arm MUST precede the broad one.
        run.sync_reclassified_subjects()
        _attach_partial_detect_result(
            exc, run.detection_map, run.detected, run.processed_ids,
        )
        raise
    except (ImportError, RuntimeError) as e:
        # Detection unavailable (missing weights/backend) — non-fatal, the
        # caller degrades to full-image classification. Previously silenced
        # entirely, which let the detect stage report success while the
        # batch's remaining photos were silently skipped.
        log.warning("Detection unavailable for batch (non-fatal): %s", e)
    except Exception:
        log.warning("Detection failed for batch (non-fatal)", exc_info=True)

    run.sync_reclassified_subjects()
    run.analyze_subjects(runner, vireo_dir)

    return run.detection_map, run.detected, run.processed_ids


def _attach_partial_detect_result(exc, detection_map, detected, processed_ids):
    """Record ``_detect_batch``'s committed work on a propagating cancel.

    A ``ResourceWaitCancelled`` unwinds past the ``return``, so without this
    the caller loses the photos whose detections were already rewritten.
    Callers read it back as ``exc.partial_detect_result``, the same
    ``(detection_map, detected_count, processed_ids)`` triple a normal
    return yields.
    """
    exc.partial_detect_result = (detection_map, detected, processed_ids)


def _analyze_subjects(photos, folders, db, reclassify, det_conf_threshold,
                      cached_detections, processed_ids, vireo_dir,
                      checkpoint):
    """Subject-analysis pass of ``_detect_batch`` (see its comments)."""
    from subjects import analyze_photo

    # Resolve the working-copy JPEG first when the caller supplied a
    # ``vireo_dir``: cached-detection photos on an offline NAS still
    # have a usable local preview, and the on-demand Analyze route
    # already reads from it. Building only the source folder path
    # would fail ``os.stat`` inside ``analyze_photo`` and leave those
    # photos permanently without subject quality/crop/exposure data.
    if vireo_dir:
        from image_loader import get_canonical_image_path

    for photo in photos:
        if photo["id"] in cached_detections:
            continue
        # ``processed_ids`` only tracks photos whose detection loop reached
        # ``processed_ids.add(...)`` — i.e. detection ran to completion or
        # produced an empty scene. In a reclassify batch, ``_detect_subjects``
        # calls ``db.detections.clear(photo["id"])`` *before* calling us, so a
        # photo whose ``detect_animals()`` returned None (decode failure) or
        # raised a swallowed error is now absent from ``processed_ids`` AND
        # has no detections in the DB, yet its old mask, DINO embedding,
        # eye_* fields, and ``photo_subject_state`` still point at the
        # deleted subject. Skipping subject analysis for those photos would
        # let that stale state affect subsequent review/scoring under the
        # full-image classifier fallback. Fall through to ``analyze_photo``
        # for reclassified-but-undetected photos: its ``if not detections``
        # branch runs before ``os.stat``, so an offline source doesn't
        # crash, and its ``sync_primary`` clears the stale derived state
        # (Codex r4056646676).
        if photo["id"] not in processed_ids and not reclassify:
            continue
        checkpoint()
        if vireo_dir:
            image_path = get_canonical_image_path(photo, vireo_dir, folders)
        else:
            image_path = os.path.join(folders.get(photo["folder_id"], ""), photo["filename"])
        try:
            analyze_photo(db, photo["id"], image_path,
                          min_conf=det_conf_threshold, force=reclassify,
                          checkpoint=checkpoint)
        except ResourceWaitCancelled:
            raise
        except Exception:
            db.rollback()
            log.warning("Subject analysis unavailable for photo %s", photo["id"], exc_info=True)


class _DetectSubjectsRun:
    """Run-wide state of one ``_detect_subjects`` call.

    ``detection_map`` and ``detected`` accumulate across the per-photo
    ``_detect_batch`` calls; ``det_conf_threshold`` is resolved before the
    first batch and read back when narrowing to classifier candidates.
    """

    def __init__(self, photos, folders, runner, job, reclassify, db, vireo_dir):
        self.photos = photos
        self.folders = folders
        self.runner = runner
        self.job = job
        self.reclassify = reclassify
        self.db = db
        self.vireo_dir = vireo_dir
        self.total = len(photos)
        self.already_detected_ids = set()

        # Track photos whose detections this run rewrote (write_detection_batch
        # in reclassify mode). Declared up here so the except handlers and early
        # returns can stash whatever was accumulated before the failure. The caller
        # reads ``job["_detect_processed_ids"]`` to decide which photos to classify
        # on a post-detect cancel — using ``detection_map.keys()`` alone would miss
        # empty-scene photos whose retired boxes took their predictions with them.
        self.processed_for_rebuild: set[int] = set()
        # Reclassify photos whose redetection failed and whose stored boxes
        # were put in ``detection_map`` instead. Their detections were never
        # rewritten, so a post-detect cancel must not rebuild them.
        self.reused_stored_ids: set[int] = set()
        self.det_conf_threshold = None
        # Bound before the ``try`` so a Stop raised during setup (before the
        # loop assigns them) still reaches the cancel arm with a defined result.
        self.detection_map = {}
        self.detected = 0

    def resolve_cached_detector_runs(self):
        # Resolve cached-detection state before running MegaDetector so we can skip
        # the weight download entirely when every photo already has a detector_runs
        # row (including empty-scene rows with box_count=0).
        reclassify = self.reclassify
        detector_runtime = None
        if not reclassify:
            try:
                from computation_cache import megadetector_runtime_fingerprint

                detector_runtime = megadetector_runtime_fingerprint()
            except (OSError, ValueError):
                detector_runtime = None
        self.already_detected_ids = (
            self.db.model_runs.get_detector_run_photo_ids(
                "megadetector-v6", runtime_fingerprint=detector_runtime,
            )
            if not reclassify and detector_runtime is not None
            else self.db.model_runs.get_detector_run_photo_ids("megadetector-v6")
            if not reclassify
            else set()
        )
        self.job["_detector_runtime_fingerprint"] = detector_runtime

    def reset_job_detect_state(self):
        job = self.job
        job["_detect_reused_ids"] = self.reused_stored_ids
        job["_detect_cancelled"] = False
        job["_non_animal_photo_ids"] = set()

    def backfill_cached_subject_analysis(self):
        """Run ``_detect_batch``'s detector-less cached-row pass."""
        import config as cfg
        effective_cfg = self.db.get_effective_config(cfg.load())
        self.det_conf_threshold = effective_cfg.get("detector_confidence", 0.2)
        _detect_batch(
            self.photos, self.folders, self.runner, self.job, self.reclassify,
            self.db,
            det_conf_threshold=self.det_conf_threshold,
            already_detected_ids=self.already_detected_ids,
            vireo_dir=self.vireo_dir,
        )

    def ensure_detector_weights(self):
        """Download the MegaDetector weights when a photo needs detection.

        Returns False when a Stop arrived before the download started.
        """
        runner, job = self.runner, self.job
        # Require at least one photo — a no-op reclassify over 0 photos should
        # not trigger a ~300 MB MegaDetector download.
        needs_fresh_detection = bool(self.photos) and (
            self.reclassify or any(
                p["id"] not in self.already_detected_ids for p in self.photos
            )
        )
        if not needs_fresh_detection:
            return True
        from detector import ensure_megadetector_weights

        # Gate the download. The classifier-init phase (run by the
        # caller before _detect_subjects) has no internal cancel check,
        # so a cancel during model load would otherwise land here only
        # to be ignored: hf_hub_download can't be interrupted once it
        # starts, so the per-photo cancel check below runs too late.
        if runner.is_cancelled(job["id"]):
            log.info(
                "Classify job cancelled before MegaDetector weights download"
            )
            return False

        weights_path = ensure_megadetector_weights(
            progress_callback=self._weights_download_progress,
        )
        from computation_cache import megadetector_runtime_fingerprint

        detector_runtime = megadetector_runtime_fingerprint(weights_path)
        job["_detector_runtime_fingerprint"] = detector_runtime
        if not self.reclassify:
            self.already_detected_ids = self.db.model_runs.get_detector_run_photo_ids(
                "megadetector-v6", runtime_fingerprint=detector_runtime,
            )
        return True

    def _weights_download_progress(self, phase, current, total_steps):
        self.runner.push_event(
            self.job["id"],
            "progress",
            {
                "current": current,
                "total": total_steps,
                "current_file": "",
                "phase": f"Step 4/5: {phase}",
            },
        )

    def detect_photos(self):
        """Detect each photo in turn, reporting per-photo progress."""
        runner, job, db = self.runner, self.job, self.db
        runner.push_event(
            job["id"],
            "progress",
            {
                "current": 0,
                "total": self.total,
                "current_file": "Loading MegaDetector...",
                "rate": 0,
                "phase": "Step 4/5: Detecting subjects",
            },
        )

        # Load config once for the entire detection loop. Use the
        # workspace-effective config so per-workspace overrides apply.
        import config as cfg
        effective_cfg = db.get_effective_config(cfg.load())
        self.det_conf_threshold = effective_cfg.get("detector_confidence", 0.2)

        # Process one photo at a time so we can report per-photo progress
        self.detection_map = {}
        self.detected = 0
        skipped_det = 0
        start_time = job.get("_start_time", time.time())

        for i, photo in enumerate(self.photos):
            if runner.is_cancelled(job["id"]):
                log.info(
                    "Classify job cancelled during detection (%d/%d)",
                    i, self.total,
                )
                break
            if self._detect_photo(i, photo, start_time):
                skipped_det += 1

        log.info(
            "Detection done: %d animals detected out of %d photos (%d skipped, already detected)",
            self.detected,
            self.total,
            skipped_det,
        )

    def _detect_photo(self, i, photo, start_time):
        """Detect one photo; truthy when a cached detector run was reused."""
        runner, job, reclassify = self.runner, self.job, self.reclassify
        runner.update_step(
            job["id"], "detect",
            progress={"current": i + 1, "total": self.total},
        )
        runner.push_event(
            job["id"],
            "progress",
            {
                "current": i + 1,
                "total": self.total,
                "current_file": photo["filename"],
                "rate": round(
                    (i + 1) / max(time.time() - start_time, 0.01), 1
                ),
                "phase": "Step 4/5: Detecting subjects",
            },
        )

        was_cached = (
            not reclassify
            and photo["id"] in self.already_detected_ids
        )

        # No reclassify pre-clear. ``detections.clear`` is global: its
        # cascade took every classifier model's predictions and every
        # workspace's review state with it, before this run had written
        # anything to replace them, so a Stop anywhere after this point
        # stranded the photo. ``write_detection_batch`` with
        # ``force_runtime_replace`` replaces this photo's MegaDetector
        # rows atomically when the new result lands, and a detector
        # failure (``detect_animals`` returning None) leaves the old
        # rows untouched. The per-model predictions purge stays in the
        # classification loop, which only reaches photos it rebuilds.
        try:
            batch_map, batch_detected, batch_processed = _detect_batch(
                [photo], self.folders, runner, job, reclassify, self.db,
                det_conf_threshold=self.det_conf_threshold,
                already_detected_ids=self.already_detected_ids,
                vireo_dir=self.vireo_dir,
            )
        except ResourceWaitCancelled as exc:
            # A Stop during this photo's subject analysis lands after
            # its detections were committed. Keep that committed result
            # so the cancel recovery still rebuilds the photo's
            # predictions instead of stranding its replaced boxes.
            partial = getattr(exc, "partial_detect_result", None)
            if partial is not None:
                batch_map, batch_detected, batch_processed = partial
                if reclassify:
                    self.processed_for_rebuild.update(batch_processed)
                self.detection_map.update(batch_map)
                self.detected += batch_detected
            raise
        if reclassify:
            self.processed_for_rebuild.update(batch_processed)
            if photo["id"] not in batch_processed:
                batch_map = self._with_stored_boxes(photo, batch_map)
        self.detection_map.update(batch_map)
        self.detected += batch_detected

        return was_cached and batch_detected

    def _with_stored_boxes(self, photo, batch_map):
        # Redetection failed for this photo (``detect_animals``
        # returned None, or ``_detect_batch`` swallowed an
        # error) and its stored boxes were left untouched.
        # Classify those boxes rather than letting the photo
        # fall through to a full-image pass, whose unscoped
        # predictions clear would wipe the boxes' cached
        # predictions and the review state hanging off them.
        stored = _stored_detection_list(
            self.db, photo["id"], min_conf=self.det_conf_threshold,
        )
        if stored:
            batch_map = {**batch_map, photo["id"]: stored}
            self.reused_stored_ids.add(photo["id"])
        return batch_map

    def report_detection_unavailable(self, e):
        """Record an ImportError/RuntimeError and drop to full-image classification."""
        runner, job = self.runner, self.job
        msg = str(e)
        if "ONNX model not available" in msg or "not found" in msg:
            log.warning(
                "MegaDetector weights not available — detection skipped; classifying full images. "
                "Download the MegaDetector V6 ONNX model from the pipeline models page to enable "
                "subject detection, cropped classification, and mask extraction."
            )
            job["errors"].append(
                "MegaDetector weights not downloaded — detection skipped. Classification ran on full "
                "images (less accurate) and no detections were stored, which also prevents the mask "
                "extraction stage from producing subject masks. Download MegaDetector V6 from the "
                "pipeline models page to fix."
            )
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": self.total,
                    "current_file": "",
                    "phase": "Step 4/5: Detection skipped — MegaDetector weights not downloaded",
                },
            )
        else:
            log.warning("Detection unavailable: %s — classifying full images", e)
            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": self.total,
                    "current_file": "",
                    "phase": f"Step 4/5: Detection failed — {msg[:120]}",
                },
            )
            job["errors"].append(f"Detection unavailable: {msg[:200]}")
        self.detection_map = {}
        self.detected = 0

    def report_detection_failed(self, e):
        """Record an unexpected detection failure and drop to full-image classification."""
        self.runner.push_event(
            self.job["id"],
            "progress",
            {
                "current": 0,
                "total": self.total,
                "current_file": "",
                "phase": f"Step 4/5: Detection failed — {str(e)[:120]}",
            },
        )
        self.job["errors"].append(f"Detection failed: {str(e)[:200]}")
        self.detection_map = {}
        self.detected = 0


def _detect_subjects(photos, folders, runner, job, reclassify, db, vireo_dir=None):
    """Run MegaDetector on photos, storing quality metrics.

    Wraps _detect_batch with progress reporting for the standalone classify job.

    When ``reclassify`` is True, each photo's MegaDetector rows are replaced
    by the new run's ``write_detection_batch`` rather than cleared first.
    Detection ids are content-addressed, so a box the detector finds again
    keeps its row and, with it, every classifier model's predictions and
    every workspace's review state; only boxes the new run no longer
    produces are retired. The per-model predictions purge is handled by
    ``_classify_photos`` for the photos it actually reaches, so a cancel at
    any point leaves the unreached photos' predictions intact. A photo whose
    redetection fails keeps its stored boxes, and those boxes (not the full
    image) go into ``detection_map`` so its box predictions are rebuilt
    under this run's fingerprint instead of being wiped.

    ``detection_map`` holds only classifier candidates: MegaDetector animal
    boxes at or above the workspace's ``detector_confidence``, the same set
    the cached path and the pipeline classify. Photos whose only confident
    boxes are people or vehicles are listed in
    ``job["_non_animal_photo_ids"]`` so ``_classify_photos`` skips them
    instead of classifying the whole frame as a species.

    A Stop that arrives while a detector inference slot is being awaited
    (``ResourceWaitCancelled``) keeps the boxes detected so far and sets
    ``job["_detect_cancelled"]``; it is not reported as "Detection
    unavailable" and does not degrade the run to full-image classification.

    ``vireo_dir``: forwarded to ``_detect_batch`` so the subject analysis
    loop resolves through the working-copy JPEG when the source folder is
    offline.

    Returns:
        (detection_map, detected_count) where detection_map is
        {photo_id: [list_of_detection_dicts]} and detected_count is the
        number of photos with at least one classifier candidate.
    """
    run = _DetectSubjectsRun(
        photos, folders, runner, job, reclassify, db, vireo_dir,
    )
    run.resolve_cached_detector_runs()
    run.reset_job_detect_state()

    try:
        if detect_animals is None or get_primary_detection is None:
            # Cached-detection rows still deserve subject-analysis
            # backfill even when the detector module is unavailable —
            # analyzing cached boxes doesn't require detect_animals or
            # get_primary_detection. ``_detect_batch`` handles this
            # detector-less branch internally by iterating only cached
            # rows, so route through it before signalling the missing
            # detector to the outer handler (Codex r4056724369).
            run.backfill_cached_subject_analysis()
            raise ImportError(
                "MegaDetector ONNX model not available — cannot run detection"
            )

        # Inside the try so a weights-download failure (e.g. network down)
        # degrades to full-image classification like every other detection
        # failure, instead of failing the whole job — which on a reclassify
        # run would strike after predictions/detections were already purged.
        if not run.ensure_detector_weights():
            job["_detect_processed_ids"] = run.processed_for_rebuild
            return {}, 0

        run.detect_photos()
    except ResourceWaitCancelled:
        # Stop pressed while this photo waited for an inference slot.
        # ``ResourceWaitCancelled`` subclasses ``RuntimeError``, so this arm
        # must precede the "Detection unavailable" one: that arm would log a
        # false error, discard the boxes already committed for earlier
        # photos, and send them to full-image classification alongside
        # their real detections.
        log.info("Classify job cancelled during detection")
        job["_detect_cancelled"] = True
    except (ImportError, RuntimeError) as e:
        run.report_detection_unavailable(e)
    except Exception as e:
        log.warning(
            "Detection failed (non-fatal) — classifying full images", exc_info=True
        )
        run.report_detection_failed(e)

    detection_map, detected = run.detection_map, run.detected
    if detection_map:
        detection_map, non_animal_ids = _classifier_candidates(
            detection_map, run.det_conf_threshold,
        )
        job["_non_animal_photo_ids"] = non_animal_ids
        detected = len(detection_map)

    job["_detect_processed_ids"] = run.processed_for_rebuild
    return detection_map, detected


def _classifier_candidates(detection_map, floor):
    """Narrow ``detection_map`` to the boxes the classifier should see.

    A fresh ``_detect_batch`` run returns every box above the detector's
    storage floor (``RAW_CONF_FLOOR``) so raw output stays globally cached,
    while a cached run reads back only rows above ``detector_confidence``.
    Classifying the raw list gave noise, person, and vehicle boxes species
    predictions on a fresh pass and not on a rerun. Keep what the pipeline
    classifies (``pipeline_job._cached_classify_detections``): MegaDetector
    animal boxes at or above ``floor``.

    Returns ``(filtered_map, non_animal_ids)``. Photos with no candidate are
    dropped from the map so they fall through to the full-image classifier,
    except those holding a confident person/vehicle box: those are returned
    in ``non_animal_ids`` and must not be classified at all, matching the
    pipeline's guard against tagging a human frame as a species.
    """
    filtered = {}
    non_animal_ids = set()
    for photo_id, detections in detection_map.items():
        confident = [
            d for d in detections
            if d.get("detector_model") != "full-image"
            and (d.get("confidence") or 0) >= floor
        ]
        animals = [
            d for d in confident
            if (d.get("category") or "animal") == "animal"
        ]
        if animals:
            filtered[photo_id] = animals
        elif confident:
            non_animal_ids.add(photo_id)
    return filtered, non_animal_ids
