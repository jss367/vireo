"""Detection stages for the streaming photo pipeline."""

import logging
import time

from pipeline_stages.context import PipelineRun

log = logging.getLogger(__name__)


def detect_stage(
    run: PipelineRun,
    *,
    _filter_excluded,
    collection_ready,
    computation_cache_dir,
    detect_state,
    effective_vireo_dir,
    loaded_models,
    models_ready,
):
    """Run MegaDetector across every collection photo once, ahead of any
    classification. Populates detect_state so each per-model classify
    step can pull cached detections rather than re-running MegaDetector.

    Splitting detect out (it used to run interleaved with model 1's
    classify loop) lets users see detection as its own row in the jobs
    view, and lets a multi-model run amortize one detection pass across
    every classifier.
    """
    collection_ready.wait()
    models_ready.wait()

    has_models_to_try = (
        "clf" in loaded_models
        or loaded_models.get("resolved_specs")
    )
    if (
        run.params.skip_classify
        or run.abort.is_set()
        or not run.collection_id
        or not has_models_to_try
    ):
        run.stages["detect"]["status"] = "skipped"
        run.runner.update_step(run.job["id"], "detect", status="completed",
                           summary="Skipped")
        run.update_stages(run.runner, run.job["id"], run.stages)
        return

    run.stages["detect"]["status"] = "running"
    # Also mark the aggregate classify stage as running so the pipeline
    # wizard's "Classify" card (which predates the detect/classify split)
    # shows activity during the detect pre-pass instead of waiting for
    # the first per-model classify step to start.
    run.stages["classify"]["status"] = "running"
    run.runner.update_step(run.job["id"], "detect", status="running")
    run.update_stages(run.runner, run.job["id"], run.stages)

    detect = _DetectPass(
        run,
        filter_excluded=_filter_excluded,
        computation_cache_dir=computation_cache_dir,
        detect_state=detect_state,
        effective_vireo_dir=effective_vireo_dir,
        loaded_models=loaded_models,
    )
    try:
        detect.run_all()
    except Exception as e:
        run.errors.append(f"[detect] Fatal: {e}")
        log.exception("Pipeline detect stage failed")
        run.abort.set()
        run.stages["detect"]["status"] = "failed"
        run.runner.update_step(run.job["id"], "detect", status="failed",
                           error=str(e))

    run.update_stages(run.runner, run.job["id"], run.stages)


class _DetectPass:
    """State shared by the phases of one detect stage run."""

    def __init__(
        self,
        run,
        *,
        filter_excluded,
        computation_cache_dir,
        detect_state,
        effective_vireo_dir,
        loaded_models,
    ):
        self.run = run
        self.filter_excluded = filter_excluded
        self.computation_cache_dir = computation_cache_dir
        self.detect_state = detect_state
        self.effective_vireo_dir = effective_vireo_dir
        self.loaded_models = loaded_models

    # -- the run -----------------------------------------------------

    def run_all(self):
        from classify_job import _BATCH_SIZE, _detect_batch

        self.batch_size = _BATCH_SIZE
        self.detect_batch = _detect_batch

        self._load_collection()
        self._apply_local_cache()
        self._seed_already_detected()
        self._ensure_detector_weights()
        self._detect_batches()

        self.detect_state["total_detected"] = self.total_detected
        # The stale-detection purge is DEFERRED to classify_stage and
        # only fires after the first model successfully classifies.
        # Deleting the pre-run detection rows here would cascade through
        # the predictions FK and destroy prior results in the case where
        # every classifier ends up failing to load — leaving the user
        # with no detections AND no predictions. See classify_stage for
        # the actual delete.

        if not self.run.params.reclassify:
            self._reapply_local_cache()

        self._finish()

    def _load_collection(self):
        run = self.run
        thread_db = run.database_factory(run.db_path)
        thread_db.set_active_workspace(run.workspace_id)
        self.thread_db = thread_db

        photos = self.filter_excluded(
            thread_db.get_collection_photos(run.collection_id, per_page=999999)
        )
        photo_ids = [p["id"] for p in photos]
        kept_ids = set(thread_db.filter_out_wildlife_excluded(photo_ids))
        skipped_wildlife = len(photos) - len(kept_ids)
        if skipped_wildlife:
            log.info(
                "Skipping %d photo(s) marked not wildlife",
                skipped_wildlife,
            )
        photos = [p for p in photos if p["id"] in kept_ids]
        folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
        self.photos = photos
        self.folders = folders
        self.total = len(photos)
        self.detect_state["photos"] = photos
        self.detect_state["folders"] = folders
        self.detect_state["ran"] = True

    def _apply_local_cache(self):
        try:
            from computation_cache import (
                ArtifactStore,
                materialize_local_store,
            )

            cache_store = (
                ArtifactStore(self.computation_cache_dir)
                if self.computation_cache_dir else None
            )
            reused = (
                materialize_local_store(self.thread_db, store=cache_store)
                if not self.run.params.reclassify else {}
            )
            self.detect_state["portable_reused"] = reused
        except Exception:
            log.warning(
                "Could not apply local computation cache", exc_info=True,
            )
            self.detect_state["portable_reused"] = {}

    def _seed_already_detected(self):
        """Reclassify semantics (see prior interleaved implementation for
        history): start with an empty already_detected so EVERY photo
        is re-detected; snapshot pre-run detection IDs so we can purge
        them after this detect pass completes. On a non-reclassify run,
        pre-seed already_detected from detector_runs so _detect_batch
        reuses rows instead of re-invoking MegaDetector — including
        empty-scene photos (box_count=0) which would otherwise be
        re-detected forever by a legacy detections-only seed.
        """
        run = self.run
        thread_db = self.thread_db
        if run.params.reclassify:
            already_detected: set = set()
            detector_runtime = None
            photo_ids_list = [p["id"] for p in self.photos]
            detections = getattr(thread_db, "detections", None)
            pre_run_det_ids: dict = (
                detections.get_ids_for_photos(photo_ids_list)
                if detections is not None else {}
            )
        else:
            try:
                from computation_cache import megadetector_runtime_fingerprint

                detector_runtime = megadetector_runtime_fingerprint()
            except (OSError, ValueError):
                detector_runtime = None
            if detector_runtime is not None:
                already_detected = set(
                    thread_db.get_detector_run_photo_ids(
                        "megadetector-v6",
                        runtime_fingerprint=detector_runtime,
                    )
                )
            else:
                already_detected = set(
                    thread_db.get_detector_run_photo_ids("megadetector-v6")
                )
            pre_run_det_ids = {}
        self.already_detected = already_detected
        self.detector_runtime = detector_runtime
        run.job["_detector_runtime_fingerprint"] = detector_runtime
        self.detect_state["pre_run_det_ids"] = pre_run_det_ids

    def _ensure_detector_weights(self):
        """Ensure MegaDetector weights only when we actually need fresh
        detection work — an offline rerun over already-detected photos
        should not trigger a ~300 MB download.
        """
        run = self.run
        needs_fresh_detection = bool(self.photos) and (
            run.params.reclassify
            or any(p["id"] not in self.already_detected for p in self.photos)
        )
        if not needs_fresh_detection:
            return
        from detector import ensure_megadetector_weights

        def _dl_progress(phase, current, total_steps):
            # Weight download is a sub-phase of detect; don't treat
            # its bytes as detect's stage-level counter or the bar
            # jumps ahead before any photo has been detected.
            run.emit_progress(
                run.runner, run.job["id"], run.stages, "detect", phase,
            )

        weights_path = ensure_megadetector_weights(
            progress_callback=_dl_progress,
        )
        from computation_cache import megadetector_runtime_fingerprint

        self.detector_runtime = megadetector_runtime_fingerprint(weights_path)
        run.job["_detector_runtime_fingerprint"] = self.detector_runtime
        if not run.params.reclassify and self.detector_runtime is not None:
            self.already_detected = set(
                self.thread_db.get_detector_run_photo_ids(
                    "megadetector-v6",
                    runtime_fingerprint=self.detector_runtime,
                )
            )

    def _detect_batches(self):
        run = self.run
        photos = self.photos
        total = self.total
        already_detected = self.already_detected
        this_run_detections: dict = self.detect_state["detections"]
        processed_ids: set = self.detect_state["processed_ids"]
        self.this_run_detections = this_run_detections
        self.processed_ids = processed_ids
        self.total_detected = 0
        start_time = time.time()

        for batch_start in range(0, total, self.batch_size):
            if run.control.should_abort(run.abort):
                break
            batch = photos[batch_start:batch_start + self.batch_size]
            batch_idx = batch_start + len(batch)

            run.stages["detect"]["count"] = batch_idx
            run.stages["detect"]["total"] = total
            run.emit_progress(
                run.runner, run.job["id"], run.stages, "detect", "Detecting subjects",
                rate=round(
                    batch_idx / max(time.time() - start_time, 0.01) * 60,
                    1,
                ),
            )
            run.runner.update_step(
                run.job["id"], "detect",
                progress={"current": batch_idx, "total": total},
            )

            # GPU serialisation lives inside detector.detect_animals()
            # — wrapping the whole batch here would hold the semaphore
            # across DB writes and CPU sharpness/quality work, blocking
            # a concurrent pipeline's GPU stages on this pipeline's
            # non-GPU work.
            det_map, det_count, det_processed = self.detect_batch(
                batch, self.folders, run.runner, run.job,
                run.params.reclassify, self.thread_db,
                already_detected_ids=already_detected,
                cached_detections=None,
                vireo_dir=self.effective_vireo_dir,
            )
            self.total_detected += det_count
            already_detected.update(det_processed)
            for pid, dets in det_map.items():
                this_run_detections.setdefault(pid, dets)
            for pid in det_processed:
                this_run_detections.setdefault(pid, [])
            processed_ids.update(det_processed)

    # -- post-detect cache reapply -----------------------------------

    def _reapply_local_cache(self):
        """Reapply the local computation cache now that fresh
        detector_runs exist. Classification artifacts whose
        detector dependency was absent at the pre-detection
        materialize call get a second chance to land here, so
        bundles carrying only classifications still surface.

        We ALSO pre-create synthetic full-image detector rows
        for every empty-scene photo before the reapply. The
        classify stage below creates those rows lazily
        per-photo, so without pre-creating them here a cached
        full_image classification artifact has no anchor to
        attach to at reapply time — the classify stage then
        runs the classifier itself even though the answer is
        already sitting in the local store.
        """
        try:
            import config as cfg
            from computation_cache import (
                CacheFormatError,
                full_image_runtime_fingerprint,
                materialize_local_store,
                source_input,
            )

            # Pre-create full-image anchors for empty-scene
            # photos, and promote any legacy full-image
            # ``detector_runs`` row to the portable runtime
            # so imported full-image classifications
            # actually attach on materialize instead of
            # being deferred by the runtime-fingerprint
            # gate.
            full_runtime = full_image_runtime_fingerprint()
            empty_scene_ids = self._full_image_anchor_ids(cfg)
            for photo_id in empty_scene_ids:
                self._ensure_full_image_anchor(
                    photo_id, full_runtime, source_input, CacheFormatError,
                )

            known_runtimes = {full_runtime}
            if self.detector_runtime is not None:
                known_runtimes.add(self.detector_runtime)
            known_classifier_runtimes = self._known_classifier_runtimes(
                full_runtime,
            )
            from computation_cache import ArtifactStore

            reapply_store = (
                ArtifactStore(self.computation_cache_dir)
                if self.computation_cache_dir else None
            )
            post = materialize_local_store(
                self.thread_db, store=reapply_store,
                known_runtimes=known_runtimes,
                known_classifier_runtimes=(
                    known_classifier_runtimes or None
                ),
            )
            if post.get("classifier_runs_applied"):
                prior = self.detect_state.get("portable_reused") or {}
                prior_applied = prior.get(
                    "classifier_runs_applied", 0,
                )
                merged = dict(prior)
                merged["classifier_runs_applied"] = (
                    prior_applied
                    + post["classifier_runs_applied"]
                )
                self.detect_state["portable_reused"] = merged
        except Exception:
            log.warning(
                "Could not reapply local computation cache after detect",
                exc_info=True,
            )

    def _full_image_anchor_ids(self, cfg):
        """Also broaden the anchor set to include photos
        whose only detections are noise (< detector_
        confidence) so cached full-image classifiers
        attach on this materialize instead of forcing
        the classify stage's lazy per-photo anchor
        creation — which happens AFTER materialize and
        re-runs inference for an answer already sitting
        in the local store. The runtime fallback at
        ``classify_stage`` fires under the same
        predicate (no usable animal box at the strict
        threshold, no confident non-animal box), so
        mirror it exactly here to avoid pre-creating
        anchors the fallback would refuse to use.
        """
        thread_db = self.thread_db
        photos = self.photos
        try:
            _effective_cfg = thread_db.get_effective_config(
                cfg.load()
            )
            _det_conf = _effective_cfg.get(
                "detector_confidence", 0.2,
            )
        except Exception:
            log.warning(
                "Could not read workspace detector settings; using defaults",
                exc_info=True,
            )
            _effective_cfg = {}
            _det_conf = 0.2

        _pipeline_cfg = _effective_cfg.get("pipeline", {})
        _weak_enabled = _pipeline_cfg.get(
            "weak_detection_rescue_enabled", True,
        )
        _weak_conf = _pipeline_cfg.get(
            "weak_detection_confidence", 0.12,
        )
        _contextual_weak_ids = set()
        if (
            _weak_enabled
            and _weak_conf < _det_conf
            and photos
        ):
            from weak_detections import (
                contextual_weak_photo_ids,
            )

            _raw_mdv6 = thread_db.get_detections_for_photos(
                [p["id"] for p in photos],
                min_conf=_weak_conf,
                detector_model="megadetector-v6",
            )
            _contextual_weak_ids = contextual_weak_photo_ids(
                photos,
                _raw_mdv6,
                detector_confidence=_det_conf,
                weak_confidence=_weak_conf,
                max_gap=_pipeline_cfg.get(
                    "burst_time_gap", 3.0,
                ),
            )

        return [
            photo["id"] for photo in photos
            if photo["id"] in self.processed_ids
            and self._needs_full_image_anchor(
                photo["id"], _det_conf, _contextual_weak_ids,
            )
        ]

    def _needs_full_image_anchor(self, pid, det_conf, contextual_weak_ids):
        if pid in contextual_weak_ids:
            return False
        dets = self.this_run_detections.get(pid) or []
        if not dets:
            return True
        has_usable_animal = any(
            d.get("detector_model") != "full-image"
            and d.get("category", "animal") == "animal"
            and d.get(
                "confidence",
                d.get("detector_confidence", 0),
            ) >= det_conf
            for d in dets
        )
        if has_usable_animal:
            return False
        confident_non_animal = any(
            d.get("detector_model") != "full-image"
            and d.get("category", "animal") != "animal"
            and d.get(
                "confidence",
                d.get("detector_confidence", 0),
            ) >= det_conf
            for d in dets
        )
        return not confident_non_animal

    def _ensure_full_image_anchor(
        self, photo_id, full_runtime, source_input, cache_format_error,
    ):
        thread_db = self.thread_db
        identity = thread_db.conn.execute(
            """SELECT file_hash, companion_path
               FROM photos WHERE id = ?""",
            (photo_id,),
        ).fetchone()
        full_input = None
        if (
            identity is not None
            and not identity["companion_path"]
        ):
            try:
                _block, full_input = source_input(
                    identity["file_hash"],
                    "vireo-detector-source-v1",
                )
            except (ValueError, cache_format_error):
                # source_input raises CacheFormatError on
                # NULL / non-canonical file_hash values.
                # Falling through to the outer
                # ``except Exception`` would abandon the
                # post-detect reapply for the whole
                # detect stage; keep full_input=None so
                # the surrounding write still runs.
                full_input = None
        existing_full = thread_db.get_detections(
            photo_id, detector_model="full-image",
            min_conf=0,
        )
        if not existing_full:
            thread_db.save_detections(
                photo_id,
                [{
                    "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                    "confidence": 0,
                    "category": "animal",
                }],
                detector_model="full-image",
                runtime_fingerprint=full_runtime,
            )
        else:
            # Promote a legacy full-image detection so
            # its runtime_fingerprint matches the run
            # we're about to record. exportable_artifacts
            # reads the detector runtime from
            # ``detections`` — leaving it at 'legacy'
            # makes future exports emit an empty
            # full-image detection artifact for this
            # photo, dropping the attached classifier
            # run.
            thread_db.conn.execute(
                """UPDATE detections
                      SET runtime_fingerprint = ?
                    WHERE photo_id = ?
                      AND detector_model = 'full-image'
                      AND (runtime_fingerprint IS NULL
                           OR runtime_fingerprint != ?)""",
                (full_runtime, photo_id, full_runtime),
            )
            thread_db.conn.commit()
        existing_run = thread_db.conn.execute(
            """SELECT runtime_fingerprint FROM detector_runs
               WHERE photo_id = ?
                 AND detector_model = 'full-image'""",
            (photo_id,),
        ).fetchone()
        if (
            existing_run is None
            or existing_run["runtime_fingerprint"]
            != full_runtime
        ):
            thread_db.record_detector_run(
                photo_id, "full-image", box_count=1,
                runtime_fingerprint=full_runtime,
                input_fingerprint=full_input,
            )

    def _known_classifier_runtimes(self, full_runtime):
        """Compute expected classifier runtimes for any
        classifier the model_loader stage already
        resolved. Without this the classifier-runtime
        quarantine drops bundle classifications even
        when this install carries the exact matching
        classifier. Multi-classifier pipelines only
        get the first classifier's runtime here;
        later classifiers land during their own
        classify_stage invocations of the cache.
        """
        loaded_models = self.loaded_models
        known_classifier_runtimes = set()
        pl_identity = loaded_models.get(
            "classifier_model_identity",
        )
        pl_fp_full = loaded_models.get(
            "labels_fingerprint_full",
        )
        if (
            pl_identity
            and isinstance(pl_fp_full, str)
            and len(pl_fp_full) == 64
        ):
            try:
                from computation_cache import (
                    classifier_runtime_fingerprint,
                )

                pl_tax_identity = loaded_models.get(
                    "taxonomy_identity", "no-tax",
                )
                for det_rt in (self.detector_runtime, full_runtime):
                    if not det_rt:
                        continue
                    crt = classifier_runtime_fingerprint(
                        pl_identity, pl_fp_full, det_rt,
                        taxonomy_identity=pl_tax_identity,
                    )
                    if crt:
                        known_classifier_runtimes.add(crt)
            except Exception:
                log.warning(
                    "Could not compute classifier runtime fingerprints; "
                    "reapplying without a classifier filter",
                    exc_info=True,
                )
                known_classifier_runtimes = set()
        return known_classifier_runtimes

    # -- outcome -----------------------------------------------------

    def _finish(self):
        run = self.run
        total = self.total
        run.stages["detect"]["status"] = "completed"
        run.runner.update_step(
            run.job["id"], "detect", status="completed",
            summary=(
                f"{self.total_detected} animals detected in {total} photos"
                if total else "No photos to detect"
            ),
        )
        run.result["stages"]["detect"] = {
            "total": total,
            "detected": self.total_detected,
            "processed": len(self.processed_ids),
        }
