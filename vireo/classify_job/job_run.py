"""``_ClassifyJobRun``: the state and phases of one classify job."""

import contextlib
import time

from classify_job.cache_coverage import _all_photos_cache_satisfied
from classify_job.cached_only import _finalize_cached_only, _finalize_remaining_steps, _finalize_summary
from classify_job.classify_pass import (
    _classify_photos,
    _classify_summary_parts,
    _publish_classifier_runs_for_raw_results,
)
from classify_job.common import ClassificationCancelled, Classifier, TimmClassifier, UnusableLabelsError, log
from classify_job.detection import _detect_subjects
from classify_job.label_loading import _load_labels, _record_labels_fingerprint, _reuse_saved_label_embeddings
from classify_job.label_source import _sources_from_metas, describe_label_source
from classify_job.run_helpers import _CachePeek, _empty_classify_result, _rearm_label_desc_heal
from classify_job.store_predictions import _store_grouped_predictions
from models import get_active_model, get_models


class _ClassifyJobRun:
    """Run-wide state for one ``run_classify_job`` call.

    Each phase method reads what earlier phases left on the instance:
    the filtered photo list, the resolved model, the label set and its
    fingerprints, the classifier, and the detection results.
    """

    def __init__(
        self, job, runner, db, params, vireo_dir, computation_cache_dir,
    ):
        self.job = job
        self.runner = runner
        self.db = db
        self.params = params
        self.vireo_dir = vireo_dir
        self.computation_cache_dir = computation_cache_dir
        self.classifier_cache_handle = None
        self.finish_cleared_only = False

    def _progress(self, current, total, current_file, phase, **extra):
        self.runner.push_event(
            self.job["id"],
            "progress",
            {
                "current": current,
                "total": total,
                "current_file": current_file,
                "rate": 0,
                "phase": phase,
                **extra,
            },
        )

    def cancelled_before_start(self, step_ids):
        """Close the not-yet-run steps and report a run that stored nothing."""
        _finalize_remaining_steps(
            self.runner, self.job["id"], step_ids,
            status="cancelled", summary="Cancelled before start",
        )
        return _empty_classify_result(self.total)

    # -- Phase 1: photos -----------------------------------------------

    def load_photos(self):
        """Phase 1: Get photos from collection.

        Runs before model resolution so that a collection fully filtered
        out by the subject-skip gate short-circuits without ever attempting
        to load (or fail to load) a classifier model. Otherwise users with
        no model downloaded would see "No model available" for jobs that
        have zero work to do.
        """
        job, runner, thread_db = self.job, self.runner, self.db
        runner.update_step(job["id"], "load_photos", status="running")
        self._progress(
            0, 0, "Loading collection photos...", "Step 1/5: Loading photos",
        )
        photos = thread_db.get_collection_photos(
            self.params.collection_id, per_page=999999,
        )

        photo_ids = [p["id"] for p in photos]
        pre_count = len(photos)
        kept_raw = getattr(
            thread_db, "filter_out_wildlife_excluded", lambda ids: ids
        )(photo_ids)
        if not isinstance(kept_raw, list | tuple | set):
            kept_raw = photo_ids
        kept_ids = set(kept_raw)
        photos = [p for p in photos if p["id"] in kept_ids]
        skipped_wildlife = pre_count - len(photos)
        if skipped_wildlife:
            log.info(
                "Skipping %d photo(s) marked not wildlife",
                skipped_wildlife,
            )
            self._progress(
                0, len(photos),
                f"Skipped {skipped_wildlife} photo(s) marked not wildlife",
                "Step 1/5: Loading photos",
                skipped_wildlife_excluded=skipped_wildlife,
            )

        # Skip photos already tagged with a 'subject' keyword (per workspace
        # config). reclassify=True bypasses so users can verify existing tags.
        if not self.params.reclassify:
            subject_types = thread_db.get_subject_types()
            if subject_types:
                pre_count = len(photos)
                kept_ids = set(thread_db.filter_out_subject_tagged(
                    [p["id"] for p in photos], subject_types,
                ))
                photos = [p for p in photos if p["id"] in kept_ids]
                skipped_subject = pre_count - len(photos)
                if skipped_subject:
                    log.info(
                        "Skipping %d photo(s) with subject keywords (types=%s)",
                        skipped_subject, sorted(subject_types),
                    )
                    self._progress(
                        0, len(photos),
                        f"Skipped {skipped_subject} already-identified "
                        f"photo(s)",
                        "Step 1/5: Loading photos",
                        skipped_subject=skipped_subject,
                    )

        self.photos = photos
        self.total = len(photos)
        job["progress"]["total"] = self.total
        runner.update_step(
            job["id"], "load_photos", status="completed",
            summary=f"{self.total} photos",
        )

    def finish_without_photos(self):
        """Skip every later step when filtering left no photos."""
        log.info(
            "Classify job: no photos to process after filtering; "
            "skipping model resolution and detection",
        )
        self._progress(
            0, 0, "No photos to classify", "Step 1/5: Loading photos",
        )
        _finalize_remaining_steps(
            self.runner, self.job["id"],
            ["load_taxonomy", "load_model", "detect", "classify",
             "finalize"],
            status="completed", summary="Skipped (no photos to classify)",
        )
        return _empty_classify_result(0)

    def apply_local_computation_cache(self):
        """Apply the local computation cache before model resolution.

        Photos may have been cataloged after a bundle import. Apply any
        matching stored objects before model resolution so their raw
        results participate in the ordinary database cache gates.
        """
        try:
            from computation_cache import ArtifactStore, materialize_local_store

            cache_store = (
                ArtifactStore(self.computation_cache_dir)
                if self.computation_cache_dir else None
            )
            reused = (
                materialize_local_store(self.db, store=cache_store)
                if not self.params.reclassify else {}
            )
            if reused.get("detector_runs_applied") or reused.get(
                "classifier_runs_applied"
            ):
                log.info(
                    "Portable cache applied %d detector and %d classifier runs",
                    reused.get("detector_runs_applied", 0),
                    reused.get("classifier_runs_applied", 0),
                )
        except Exception:
            log.warning("Could not apply local computation cache", exc_info=True)

    # -- cached-only shortcut ------------------------------------------

    def peek_cache_constraints(self):
        """Resolve what cached results must match to stand in for this run."""
        peek = _CachePeek()
        self._peek_requested_classifier(peek)

        # An unknown model_id must never fall through to the cache
        # shortcut. Without this, ``desired_classifier_model`` stays
        # ``None`` and ``_all_photos_cache_satisfied`` accepts runs from
        # any classifier, silently reporting success for a request that
        # would otherwise raise "not found or not downloaded" below.
        if peek.model_id_missing:
            raise RuntimeError(
                f"Model '{self.params.model_id}' not found or not downloaded."
            )
        self._peek_labels(peek)
        self._peek_model_identity(peek)

        import config as cfg

        cache_effective_cfg = self.db.get_effective_config(cfg.load())
        peek.detector_confidence = cache_effective_cfg.get(
            "detector_confidence", 0.2,
        )
        # Peek the local taxonomy identity so the expected classifier
        # runtimes computed inside ``_all_photos_cache_satisfied`` include
        # the taxonomy axis. Otherwise the check would compute expected
        # runtimes with "no-tax" while installed classifier_runs carry
        # the real taxonomy digest — the cached-only shortcut would then
        # never fire on installs that use taxonomy. ``load_local_taxonomy``
        # is cached, so the actual load in Phase 2 below is free.
        from computation_cache import (
            local_taxonomy_identity as _peek_tax_identity,
        )

        peek.taxonomy_identity = _peek_tax_identity()

        _rearm_label_desc_heal(peek.peek_model)
        return peek

    def _peek_requested_classifier(self, peek):
        """Resolve the requested classifier, downloaded or not."""
        params = self.params
        try:
            if params.model_id:
                peek.peek_model = next(
                    (m for m in get_models() if m["id"] == params.model_id),
                    None,
                )
                if peek.peek_model:
                    # Preserve the explicitly requested model identity
                    # even when the weights aren't downloaded yet.
                    # Otherwise ``_all_photos_cache_satisfied`` would
                    # treat any classifier's cached runs as valid for
                    # this explicit request and silently return
                    # wrong-model results on a fresh install.
                    peek.desired_classifier_model = (
                        params.model_name or peek.peek_model.get("name")
                    )
                else:
                    peek.model_id_missing = True
            elif params.model_name:
                peek.desired_classifier_model = params.model_name
                peek.peek_model = next(
                    (
                        m for m in get_models()
                        if m.get("name") == params.model_name
                    ),
                    None,
                )
            else:
                peek.peek_model = get_active_model()
                if peek.peek_model:
                    peek.desired_classifier_model = (
                        params.model_name or peek.peek_model.get("name")
                    )
        except Exception:
            # A raising get_models() here left both peek and constraints
            # unresolved. Clearing ``model_id_missing`` or
            # ``desired_classifier_model`` unconditionally would let
            # ``_all_photos_cache_satisfied`` silently accept cached
            # runs from a classifier the caller did not select. Preserve
            # the explicit request so either the guard below propagates
            # the lookup failure or the cache filter still rejects
            # wrong-model runs.
            log.warning(
                "Could not resolve the requested classifier before the cache check",
                exc_info=True,
            )
            peek.peek_model = None
            if params.model_id:
                peek.desired_classifier_model = None
                peek.model_id_missing = True
            elif params.model_name:
                peek.desired_classifier_model = params.model_name
                peek.model_id_missing = False
            else:
                peek.desired_classifier_model = None
                peek.model_id_missing = False

    def _peek_labels(self, peek):
        """Compute labels_fingerprint from the user-selected label sources.

        Done so a cache built from a DIFFERENT label set does not silently
        pass as satisfied.  Uses the same _load_labels path that later
        produces the authoritative fingerprint, but tolerates failure
        (missing weights on a fresh install, TOL path unavailable) — in
        that case desired_labels_fingerprint stays None and the check falls
        back to model-only filtering.
        """
        peek_model = peek.peek_model
        try:
            from labels_fingerprint import (
                compute_fingerprint,
                compute_full_fingerprint,
            )

            peek.peek_labels, peek.peek_use_tol, peek.peek_label_metas = (
                _load_labels(
                    model_type=(peek_model or {}).get("model_type", "bioclip"),
                    model_str=(peek_model or {}).get("model_str", ""),
                    labels_file=self.params.labels_file,
                    labels_files=self.params.labels_files,
                    db=self.db,
                    model_dir=(peek_model or {}).get("weights_path"),
                )
            )
            peek.peek_succeeded = True
            peek.desired_labels_fingerprint = compute_fingerprint(
                peek.peek_labels,
            )
            fp_full_peek = compute_full_fingerprint(peek.peek_labels)
            if isinstance(fp_full_peek, str) and len(fp_full_peek) == 64:
                peek.desired_labels_fingerprint_full = fp_full_peek
        except UnusableLabelsError:
            # Same reasoning as the model_id_missing raise above: without
            # this, the cache-only shortcut would accept runs from any
            # previous label space and finish "successfully" for a
            # selection the authoritative load below rejects.
            raise
        except Exception:
            log.warning(
                "Could not load labels before the cache check; "
                "cache reuse falls back to model-only filtering",
                exc_info=True,
            )
            peek.desired_labels_fingerprint = None

    def _peek_model_identity(self, peek):
        """Resolve the installed classifier's portable identity.

        Done so the cache check can filter classifier_runs by
        runtime_fingerprint. Without this, a stale run (weights or preprocessing changed under
        the same classifier_model, unchanged label fingerprint) still
        satisfies the coverage join and the caller shortcuts to
        ``_finalize_cached_only`` on wrong-runtime results. Falling back to
        ``None`` keeps the fresh-install case (undownloaded weights,
        missing revision file) working as before.
        """
        peek_model = peek.peek_model
        try:
            from computation_cache import classifier_model_identity
            from computation_cache import fingerprint as identity_fingerprint

            if peek_model and peek_model.get("weights_path"):
                peek.desired_model_identity = classifier_model_identity(
                    peek_model,
                )
            if (
                peek.desired_labels_fingerprint_full is None
                and peek.peek_use_tol
                and peek.desired_model_identity
            ):
                # Tree-of-Life mode uses a synthetic labels_full that
                # does not require ``peek_labels`` to be non-empty.
                # Mirrors the fp_full fallback later in the job.
                peek.desired_labels_fingerprint_full = identity_fingerprint({
                    "label_space": "tree-of-life",
                    "model": peek.desired_model_identity,
                })
        except (OSError, ValueError):
            peek.desired_model_identity = None

    def cache_satisfied(self, peek):
        return _all_photos_cache_satisfied(
            self.db, [p["id"] for p in self.photos],
            classifier_model=peek.desired_classifier_model,
            labels_fingerprint=peek.desired_labels_fingerprint,
            detector_confidence=peek.detector_confidence,
            model_identity=peek.desired_model_identity,
            labels_fingerprint_full=peek.desired_labels_fingerprint_full,
            taxonomy_identity=peek.taxonomy_identity,
        )

    def finalize_cached_only(self, peek):
        return _finalize_cached_only(
            self.db, self.runner, self.job, self.params, self.photos,
            classifier_model=peek.desired_classifier_model,
            labels_fingerprint=peek.desired_labels_fingerprint,
            peek_model=peek.peek_model,
            peek_labels=peek.peek_labels,
            peek_use_tol=peek.peek_use_tol,
            peek_label_metas=peek.peek_label_metas,
            peek_succeeded=peek.peek_succeeded,
            detector_confidence=peek.detector_confidence,
        )

    # -- Phases 2-4: model, taxonomy, labels, classifier ---------------

    def resolve_model(self):
        """Resolve model (deferred until we know there is work to do)."""
        params = self.params
        if params.model_id:
            all_models = get_models()
            active_model = next(
                (m for m in all_models if m["id"] == params.model_id and m["downloaded"]),
                None,
            )
            if not active_model:
                raise RuntimeError(
                    f"Model '{params.model_id}' not found or not downloaded."
                )
        else:
            active_model = get_active_model()
        if not active_model:
            raise RuntimeError("No model available. Download one in Settings.")

        self.active_model = active_model
        self.model_str = active_model["model_str"]
        self.weights_path = active_model["weights_path"]
        self.effective_name = active_model["name"]
        self.model_type = active_model.get("model_type", "bioclip")
        self.model_name = params.model_name or self.effective_name

        self.folders = {f["id"]: f["path"] for f in self.db.get_folder_tree()}

    def load_taxonomy_and_labels(self):
        """Phase 2: Load taxonomy. Phase 3: Load labels and fingerprint them."""
        job, runner, thread_db = self.job, self.runner, self.db
        runner.update_step(job["id"], "load_taxonomy", status="running")
        self._progress(
            0, self.total, "Loading taxonomy...", "Step 2/5: Loading taxonomy",
        )
        from taxonomy import load_local_taxonomy
        tax = self.tax = load_local_taxonomy()

        # Phase 3: Load labels (uses model_type/model_str from above)
        labels, use_tol, label_metas = _load_labels(
            model_type=self.model_type,
            model_str=self.model_str,
            labels_file=self.params.labels_file,
            labels_files=self.params.labels_files,
            db=thread_db,
            model_dir=self.weights_path,
        )
        self.labels, self.use_tol, self.label_metas = labels, use_tol, label_metas
        # Compute a content-addressable fingerprint for the active label set.
        # Kept in scope so downstream classifier_runs writes can record the
        # exact (classifier_model, labels_fingerprint) that produced a result.
        from labels_fingerprint import compute_fingerprint, compute_full_fingerprint
        fp = self.fp = compute_fingerprint(labels)
        fp_full = compute_full_fingerprint(labels)
        if len(fp_full) != 64:
            fp_full = None
        try:
            from computation_cache import classifier_model_identity, fingerprint

            classifier_identity = classifier_model_identity(self.active_model)
            if fp_full is None and use_tol and classifier_identity:
                fp_full = fingerprint({
                    "label_space": "tree-of-life",
                    "model": classifier_identity,
                })
        except (OSError, ValueError):
            classifier_identity = None
        self.fp_full = fp_full
        self.classifier_identity = classifier_identity
        # Derive the fingerprint's source paths from the metadata
        # ``_load_labels`` actually consumed, not by re-resolving from
        # ``params`` a second time. If a configured labels_file was deleted
        # after the classifier initialized, the fresh resolve would name
        # the stale path while ``labels`` came from the workspace fallback.
        label_sources = _sources_from_metas(label_metas)
        _record_labels_fingerprint(
            thread_db, fp, labels, sources=label_sources,
            full_fingerprint=fp_full,
        )
        job["_labels_fingerprint_full"] = fp_full
        job["_classifier_model_identity"] = classifier_identity
        from computation_cache import taxonomy_identity as _taxonomy_identity
        job["_taxonomy_identity"] = _taxonomy_identity(tax)

        tax_summary = "Taxonomy loaded" if tax else "No taxonomy"
        labels_summary = f"{len(labels)} labels" if labels else ("Tree of Life" if use_tol else "no labels")
        runner.update_step(
            job["id"], "load_taxonomy", status="completed",
            summary=f"{tax_summary}, {labels_summary}",
        )

        log.info(
            "Classifying %d photos with '%s' (%s)",
            self.total, self.effective_name, self.model_str,
        )

    def _factory_cancel_check(self):
        """Pure-cancel probe for the classifier factory.

        The factory runs under ``ModelCache._Entry.load_lock``; parking
        there on pause would strand every concurrent unpaused sibling that
        acquires the same cache key until Resume (they poll the load_lock
        at ``model_cache.py:198`` without their own pause boundary). Using
        the non-parking probe lets construction continue through a pause
        request — the lock releases as soon as the factory returns, then
        the outer classify loop honors the pause at the next boundary.
        Cancel still aborts immediately.
        """
        probe = getattr(self.runner, "cancellation_requested", None)
        if probe is not None:
            return probe(self.job["id"])
        return self.runner.is_cancelled(self.job["id"])

    def _factory_pause_check(self):
        """Non-parking pause probe for the same factory.

        When it reports a pause, the classifier checkpoints the label
        embeddings finished so far and raises ``ClassifierLoadPaused``
        instead of parking under ``load_lock``; the retry loop in
        ``load_classifier`` parks at this job's own boundary and re-enters
        construction on Resume.
        """
        probe = getattr(self.runner, "pause_requested", None)
        return bool(probe is not None and probe(self.job["id"]))

    def _emb_progress(self, current, emb_total):
        self.runner.update_step(
            self.job["id"], "load_model",
            progress={"current": current, "total": emb_total},
        )
        self._progress(
            current, emb_total,
            f"Computing label embeddings ({current}/{emb_total})...",
            "Step 3/5: Computing embeddings",
        )

    def _cancel_model_load(self):
        self.runner.update_step(
            self.job["id"], "load_model",
            status="cancelled", summary="Cancelled",
        )
        return self.cancelled_before_start(["detect", "classify", "finalize"])

    def _classifier_factory(self, _factory_cancel_check):
        """The ``factory`` ``acquire_cached_classifier`` builds on a cache miss."""
        model_type, model_str = self.model_type, self.model_str
        weights_path, labels, use_tol = self.weights_path, self.labels, self.use_tol
        tax, thread_db = self.tax, self.db
        if model_type == "timm":
            def _construct_classifier():
                if _factory_cancel_check():
                    raise ClassificationCancelled("classification cancelled")
                return TimmClassifier(model_str, taxonomy=tax)
        else:
            def _construct_classifier():
                if _factory_cancel_check():
                    raise ClassificationCancelled("classification cancelled")
                if not use_tol:
                    _reuse_saved_label_embeddings(thread_db, model_str, weights_path, labels, _factory_cancel_check)
                return Classifier(
                    labels=None if use_tol else labels,
                    model_str=model_str,
                    pretrained_str=weights_path,
                    embedding_progress_callback=self._emb_progress,
                    cancel_check=_factory_cancel_check,
                    pause_check=self._factory_pause_check,
                )
        return _construct_classifier

    def load_classifier(self):
        """Phase 4: Initialize classifier.

        Returns the job result when a cancel lands before the classifier
        is ready, else ``None``.

        The reclassify purge (destructive clears of detections +
        predictions + cascaded review state) is deferred until AFTER the
        classifier initializes. Running it before model load means any
        weight-load failure leaves affected photos with no predictions AND
        no detections AND no replacement results — shared-folder
        workspaces lose their cached state too. Deferring preserves the
        cache on setup failure; users see a clean error and their
        workspace is unchanged.
        """
        job, runner = self.job, self.runner
        model_type, model_str = self.model_type, self.model_str
        weights_path, labels, use_tol = self.weights_path, self.labels, self.use_tol
        tax = self.tax
        effective_name = self.effective_name
        runner.update_step(job["id"], "load_model", status="running")
        if model_type == "timm":
            phase_msg = f"Loading {effective_name} timm model..."
        elif use_tol:
            phase_msg = f"Loading {effective_name} Tree of Life classifier..."
        else:
            phase_msg = f"Loading {effective_name} model and computing label embeddings..."

        self._progress(0, self.total, phase_msg, "Step 3/5: Loading model")

        _factory_cancel_check = self._factory_cancel_check
        if model_type == "timm" and runner.is_cancelled(job["id"]):
            return self._cancel_model_load()
        _construct_classifier = self._classifier_factory(_factory_cancel_check)

        try:
            from classifier import ClassifierLoadPaused
            from classifier_cache import acquire_cached_classifier
            from computation_cache import taxonomy_identity
            from resource_ledger import ResourceWaitCancelled

            while True:
                try:
                    self.classifier_cache_handle = acquire_cached_classifier(
                        model_type=model_type,
                        model_str=model_str,
                        weights_path=weights_path,
                        labels=None if use_tol else labels,
                        factory=_construct_classifier,
                        files=self.active_model.get("files"),
                        # Optional-artifact presence must flip the
                        # fingerprint too. timm declares
                        # label_descriptions.json as optional and
                        # TimmClassifier reads it to translate to common
                        # names, so after a Repair downloads it we must
                        # not reuse the pre-repair classifier still
                        # emitting scientific names. bioclip-2.5's ToL
                        # artifacts are declared the same way.
                        optional_files=self.active_model.get("optional_files"),
                        taxonomy_fingerprint=(
                            taxonomy_identity(tax)
                            if model_type == "timm" else None
                        ),
                        cancel_check=_factory_cancel_check,
                    )
                    self.clf = self.classifier_cache_handle.__enter__()
                    break
                except ClassifierLoadPaused:
                    # The factory stepped out of the shared load lock with
                    # its label embeddings checkpointed. Park here, where
                    # no lock is held, then construct again: the resumed
                    # computation continues from the checkpoint. A Cancel
                    # during the pause surfaces on the retry through the
                    # factory's own cancel probe.
                    log.info(
                        "Classifier load paused; parking until Resume",
                    )
                    runner.is_cancelled(job["id"])
                    continue
        except (ClassificationCancelled, ResourceWaitCancelled):
            # Two cancellation shapes reach here: ``ClassificationCancelled``
            # from ``ModelCache``'s waiter-local cancel probe, and
            # ``ResourceWaitCancelled`` from the ONNX construction lease
            # inside ``create_session`` (``onnx_runtime.py:337``) when
            # the job-bound resource cancel probe fires mid-load. Both
            # mean the same thing to this job: cancel arrived before
            # inference could start. Finalize the step tree the same
            # way so JobRunner does not persist ``load_model`` as still
            # running with the later steps stuck at ``pending``.
            return self._cancel_model_load()
        return None

    def record_loaded_classifier(self):
        """Close the load step and stamp what the classifier actually read."""
        job, runner, clf = self.job, self.runner, self.clf
        runner.update_step(
            job["id"], "load_model", status="completed",
            summary=self.effective_name,
        )

        # Name the label space on the classify step: "Classify species" alone
        # doesn't say whether these photos are being matched against the
        # active regional lists, the full Tree of Life, or a timm model's
        # fixed head — and that decides what the predictions can even be.
        # Pass ``label_metas`` — the metadata ``_load_labels`` actually
        # consumed — so the displayed name matches ``labels`` even if the
        # on-disk sources shifted between here and rendering.
        label_source_text = describe_label_source(
            self.params, self.db,
            labels=self.labels,
            use_tol=self.use_tol,
            model_type=self.model_type,
            class_count=getattr(clf, "label_space_size", None),
            label_metas=self.label_metas,
        )
        if label_source_text:
            runner.update_step(
                job["id"], "classify", label_source=label_source_text,
            )

        # Restamp the portable model identity with the
        # label_descriptions.json this classifier actually consumed.
        # ``classifier_model_identity`` above probed the disk before the
        # classifier was built, and TimmClassifier's background heal
        # publishes that file during normal operation — so the probe can
        # disagree with what the instance read in either direction. Runs
        # published below must carry the mapping that produced their
        # species names; otherwise a pre-heal run is recorded under the
        # post-heal identity and every later cache check accepts it,
        # skipping the reclassify that replaces raw binomials.
        from computation_cache import with_consumed_label_descriptions
        self.classifier_identity = with_consumed_label_descriptions(
            self.classifier_identity, clf,
        )
        job["_classifier_model_identity"] = self.classifier_identity

    # -- Phase 5: detection --------------------------------------------

    def detect_subjects(self):
        """Phase 5: Detect subjects.

        Returns the job result when a cancel ends the run here, else
        ``None``.
        """
        job, runner, params = self.job, self.runner, self.params
        runner.update_step(job["id"], "detect", status="running")
        self.detection_map, self.detected = _detect_subjects(
            photos=self.photos,
            folders=self.folders,
            runner=runner,
            job=job,
            reclassify=params.reclassify,
            db=self.db,
            vireo_dir=self.vireo_dir,
        )
        detection_map, detected = self.detection_map, self.detected
        cancelled_after_detect = (
            runner.is_cancelled(job["id"])
            or bool(job.get("_detect_cancelled"))
        )
        # In reclassify mode, ``_detect_subjects`` rewrote each processed
        # photo's detections, and any box the new run no longer produces
        # took its predictions with it. Bailing out here would leave those
        # photos with fresh detections but no predictions until a manual
        # rerun. Fall through to classify just that subset so cancel
        # preserves the rebuild work already in flight. The unprocessed
        # tail is dropped — its old state was never touched, so dropping
        # it keeps the cached predictions intact.
        #
        # The processed subset comes from ``job["_detect_processed_ids"]``,
        # which is the union of (a) photos with at least one detection and
        # (b) empty-scene photos that recorded a detector_runs row but
        # added nothing to ``detection_map``. Using ``detection_map``
        # alone would miss case (b), stranding empty-scene reclassified
        # photos with cleared predictions and no replacement. Older test
        # fakes for ``_detect_subjects`` that don't stash this key fall
        # back to ``detection_map.keys()`` for backwards compatibility.
        self.finish_cleared_only = False
        if not cancelled_after_detect:
            runner.update_step(
                job["id"], "detect", status="completed",
                summary=f"{detected} animals detected in {self.total} photos",
            )
            return None
        runner.update_step(
            job["id"], "detect", status="cancelled",
            summary=f"Cancelled ({detected} animals detected so far)",
        )
        processed_ids = job.get("_detect_processed_ids")
        if processed_ids is None:
            processed_ids = set(detection_map.keys())
        else:
            processed_ids = set(processed_ids) | set(detection_map.keys())
        processed_ids -= set(job.get("_detect_reused_ids") or ())
        if params.reclassify and processed_ids:
            processed = [p for p in self.photos if p["id"] in processed_ids]
            if processed:
                self.finish_cleared_only = True
                self.photos = processed
                self.total = len(processed)
                job["progress"]["total"] = self.total
                return None
        _finalize_remaining_steps(
            runner, job["id"], ["classify", "finalize"],
            status="cancelled", summary="Cancelled",
        )
        return _empty_classify_result(self.total, detected=detected)

    def reapply_local_computation_cache(self):
        """Reapply the local computation cache now that detection has run.

        Classification artifacts whose detector dependency was absent at
        the pre-detection materialize call get a second chance to land
        here, so a bundle containing only classifications still populates
        predictions instead of being silently dropped.

        We ALSO pre-create synthetic full-image detector rows for every
        empty-scene photo before this reapply.  The classify loop below
        creates those rows lazily per-photo, so without pre-creating
        them here a cached full_image classification artifact has no
        anchor to attach to at reapply time — the classify loop then
        runs the classifier itself (or, on a fresh machine, fails with
        "No model available") even though the answer is already sitting
        in the local store.
        """
        try:
            from computation_cache import (
                full_image_runtime_fingerprint,
                materialize_local_store,
                megadetector_runtime_fingerprint,
                source_input,
            )

            try:
                local_runtime = megadetector_runtime_fingerprint()
            except (OSError, ValueError):
                local_runtime = None
            known_runtimes = {full_image_runtime_fingerprint()}
            if local_runtime:
                known_runtimes.add(local_runtime)

            full_runtime = full_image_runtime_fingerprint()
            self._anchor_empty_scene_photos(full_runtime, source_input)

            known_classifier_runtimes = self._known_classifier_runtimes(
                local_runtime, full_runtime,
            )

            from computation_cache import ArtifactStore

            reapply_store = (
                ArtifactStore(self.computation_cache_dir)
                if self.computation_cache_dir else None
            )
            reapplied = materialize_local_store(
                self.db,
                store=reapply_store,
                known_runtimes=known_runtimes,
                known_classifier_runtimes=(
                    known_classifier_runtimes or None
                ),
            )
            if reapplied.get("classifier_runs_applied"):
                log.info(
                    "Portable cache added %d classifier runs after detect",
                    reapplied["classifier_runs_applied"],
                )
        except Exception:
            log.warning(
                "Could not reapply local computation cache after detect",
                exc_info=True,
            )

    def _anchor_empty_scene_photos(self, full_runtime, source_input):
        """Pre-create full-image anchors for empty-scene photos.

        For photos that already carry a legacy full-image detection
        (upgraded catalogs where the detection predates the
        portable-runtime columns), also promote the ``detector_runs`` row
        to the current portable runtime.  Without the promotion,
        materialize's classifier gate would defer any imported full-image
        classification because the stored runtime fingerprint doesn't
        match ``full_runtime`` — and the classify loop would fall back to
        real inference (or fail with "No model available" on a fresh
        machine).
        """
        thread_db = self.db
        empty_scene_ids = [
            photo["id"] for photo in self.photos
            if not self.detection_map.get(photo["id"])
        ]
        # Import CacheFormatError locally so a NULL / non-canonical
        # file_hash escapes into full_input=None instead of the
        # outer except Exception (which would abandon reapply).
        from computation_cache import CacheFormatError
        for photo_id in empty_scene_ids:
            identity = thread_db.model_runs.detector_source_identity(photo_id)
            full_input = None
            if identity is not None and not identity["companion_path"]:
                try:
                    _block, full_input = source_input(
                        identity["file_hash"],
                        "vireo-detector-source-v1",
                    )
                except (ValueError, CacheFormatError):
                    full_input = None
            existing_full = thread_db.get_detections(
                photo_id, detector_model="full-image", min_conf=0,
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
                # Promote a legacy full-image detection row so its
                # ``runtime_fingerprint`` matches the run we're
                # about to record.  ``exportable_artifacts`` reads
                # the detector runtime from ``detections`` — if we
                # leave the row at ``'legacy'`` an export skips
                # the attached classifier run and emits an empty
                # full-image detection artifact for this photo.
                thread_db.detections.promote_full_image_runtime(
                    photo_id, full_runtime,
                )
                thread_db.commit()
            existing_runtime = thread_db.model_runs.detector_runtime_fingerprint(
                photo_id, "full-image",
            )
            if (
                existing_runtime is None
                or existing_runtime != full_runtime
            ):
                thread_db.model_runs.record_detector_run(
                    photo_id, "full-image", box_count=1,
                    runtime_fingerprint=full_runtime,
                    input_fingerprint=full_input,
                )

    def _known_classifier_runtimes(self, local_runtime, full_runtime):
        """Compute the classifier runtimes this job would produce.

        Done so cached classifications from other machines that match are
        accepted at reapply time. Without this the classifier-runtime
        quarantine would drop them even though this install just proved
        it can reproduce the exact same runtime.
        """
        known_classifier_runtimes = set()
        job_tax_identity = self.job.get("_taxonomy_identity", "no-tax")
        if self.fp_full and self.classifier_identity:
            try:
                from computation_cache import (
                    classifier_runtime_fingerprint,
                )

                for det_runtime in (local_runtime, full_runtime):
                    if not det_runtime:
                        continue
                    crt = classifier_runtime_fingerprint(
                        self.classifier_identity, self.fp_full, det_runtime,
                        taxonomy_identity=job_tax_identity,
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

    # -- Phases 6-7: classification and storage ------------------------

    def classify_photos(self):
        """Phase 6: Classify each photo.

        The per-detection classifier_runs gate inside _classify_photos
        skips already-done detections and still surfaces their cached
        predictions into raw_results, so a photo-level short-circuit is
        both unnecessary and actively harmful (it hides newly-surfaced
        detections after the user lowers detector_confidence).
        """
        job, runner, thread_db = self.job, self.runner, self.db
        existing_preds = set()

        job["_start_time"] = time.time()  # reset rate timer for classification phase

        import config as cfg
        effective_cfg = thread_db.get_effective_config(cfg.load())
        top_k = effective_cfg.get("top_k_predictions", 5)

        runner.update_step(job["id"], "classify", status="running")
        # ``finish_cleared_only`` runs after Cancel deliberately: detection
        # already cascaded away the old predictions, so this pass rebuilds
        # them to avoid leaving the processed subset empty. The bound
        # cancellation probe is already True here, and CPU inference now
        # consults it, so leaving the binding in place would make every
        # ``_flush_batch`` raise ``ResourceWaitCancelled`` and drop the
        # replacement predictions. Suspend the binding for this preservation
        # pass so inference completes; the JobRunner still owns any hard
        # shutdown signal via the runner-side deadline.
        from resource_ledger import bind_resource_cancel_check
        cancel_binding = (
            bind_resource_cancel_check(None)
            if self.finish_cleared_only
            else contextlib.nullcontext()
        )
        with cancel_binding:
            raw_results, failed, skipped_existing = _classify_photos(
                photos=self.photos,
                folders=self.folders,
                detection_map=self.detection_map,
                existing_preds=existing_preds,
                clf=self.clf,
                model_type=self.model_type,
                model_name=self.model_name,
                runner=runner,
                job=job,
                db=thread_db,
                top_k=top_k,
                vireo_dir=self.vireo_dir,
                labels_fingerprint=self.fp,
                reclassify=self.params.reclassify,
                finish_cleared_only=self.finish_cleared_only,
            )
        self.raw_results = raw_results
        self.failed = failed
        self.skipped_existing = skipped_existing
        parts = _classify_summary_parts(raw_results, skipped_existing, failed)
        cancelled_mid_classify = (
            runner.is_cancelled(job["id"])
            or bool(job.get("_classify_cancelled"))
        )
        if cancelled_mid_classify:
            # Fall through to finalize: raw_results holds real classifications
            # for the photos completed before the cancel — storing them
            # preserves that work (and matches the per-detection cache, which
            # is already committed).
            if self.finish_cleared_only:
                summary = (
                    "Cancelled mid-detect — finished "
                    + ", ".join(parts)
                    + " for already-cleared photos"
                )
            else:
                summary = "Cancelled (" + ", ".join(parts) + ")"
            runner.update_step(
                job["id"], "classify", status="cancelled",
                summary=summary,
            )
        else:
            runner.update_step(
                job["id"], "classify", status="completed",
                summary=", ".join(parts),
            )

    def store_predictions(self):
        """Phase 7: Group and store predictions, then build the job result."""
        job, runner, thread_db = self.job, self.runner, self.db
        total, raw_results = self.total, self.raw_results
        runner.update_step(job["id"], "finalize", status="running")
        self._progress(
            total, total, "Grouping bursts and computing consensus...",
            "Finalizing results",
        )

        group_result = _store_grouped_predictions(
            raw_results=raw_results,
            job_id=job["id"],
            model_name=self.model_name,
            grouping_window=self.params.grouping_window,
            similarity_threshold=self.params.similarity_threshold,
            tax=self.tax,
            db=thread_db,
            labels_fingerprint=self.fp,
        )
        # promote_and_publish reads persisted predictions written by
        # _store_grouped_predictions above; running it inside
        # _record_batch_classifier_runs would no-op because those rows
        # don't exist yet, leaving fresh classifier_runs stranded on
        # runtime_fingerprint = 'legacy' and out of bundle exports.
        # Reconstruct the configured ArtifactStore from the path stashed
        # by ``run_classify_job`` so the classifier artifacts land in the
        # same cache the status / export paths use, matching
        # ``publish_detection_artifact``'s behavior above.
        from computation_cache import ArtifactStore
        _publish_cache_dir = job.get("_computation_cache_dir")
        _publish_store = (
            ArtifactStore(_publish_cache_dir) if _publish_cache_dir else None
        )
        _publish_classifier_runs_for_raw_results(
            thread_db, raw_results, self.model_name, self.fp,
            labels_fingerprint_full=job.get("_labels_fingerprint_full"),
            model_identity=job.get("_classifier_model_identity"),
            taxonomy_identity=job.get("_taxonomy_identity", "no-tax"),
            store=_publish_store,
        )
        runner.update_step(
            job["id"], "finalize", status="completed",
            summary=_finalize_summary(group_result),
        )

        log.info(
            "Classification complete: %d photos processed, %d predictions stored, "
            "%d already classified, %d already labeled, %d failed",
            total,
            group_result["predictions_stored"],
            self.skipped_existing,
            group_result["already_labeled"],
            self.failed,
        )

        return {
            "total": total,
            "predictions_stored": group_result["predictions_stored"],
            "burst_groups": group_result["burst_groups"],
            "already_classified": self.skipped_existing,
            "already_labeled": group_result["already_labeled"],
            "detected": self.detected,
            "failed": self.failed,
        }
