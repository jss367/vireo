"""Classification stages for the streaming photo pipeline.

``classify_stage`` runs every resolved classifier over the detections the
detect stage produced. ``_ClassifyPass`` holds the state shared across
models (the run's config, outage latch and rollup totals); ``_SpecPass``
holds one model's counters and pending inference batch.
"""

import contextlib
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime as dt

import numpy as np
from pipeline_stages.context import PipelineRun

log = logging.getLogger(__name__)

# Photos per progress batch: the UI hears from classify at this boundary.
_PROGRESS_BATCH_SIZE = 32


def classify_stage(
    run: PipelineRun,
    *,
    _MAX_SOURCE_OFFLINE_PAUSES,
    _cached_classify_detections,
    _classification_eta_progress,
    _load_model_bundle,
    _record_unattempted_cache_hit,
    _release_classifier_cache_handle,
    _remove_attempted_cache_hits,
    _source_offline_reason,
    detect_state,
    effective_model_ids,
    loaded_models,
    source_offline_state,
):
    """Run one classifier per model against the pre-computed detections.

    Each model drives its own `classify:<model_id>` step so users see
    per-model progress, duration, and summary instead of an aggregate.
    A model that fails to load is marked `failed` on its own row — the
    run continues with remaining models.
    """
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
        _finish_without_classifying(
            run, loaded_models, effective_model_ids,
            _release_classifier_cache_handle,
        )
        return

    run.stages["classify"]["status"] = "running"
    run.update_stages(run.runner, run.job["id"], run.stages)

    classify = _ClassifyPass(
        run,
        max_source_offline_pauses=_MAX_SOURCE_OFFLINE_PAUSES,
        cached_classify_detections=_cached_classify_detections,
        classification_eta_progress=_classification_eta_progress,
        load_model_bundle=_load_model_bundle,
        record_unattempted_cache_hit=_record_unattempted_cache_hit,
        release_classifier_cache_handle=_release_classifier_cache_handle,
        remove_attempted_cache_hits=_remove_attempted_cache_hits,
        source_offline_reason=_source_offline_reason,
        detect_state=detect_state,
        loaded_models=loaded_models,
        source_offline_state=source_offline_state,
    )
    try:
        classify.run_all()
    except Exception as e:
        # fail() logs via log.exception, records the fatal-stage status
        # and sets run.abort; we deliberately swallow rather than
        # re-raise so the finally block still releases the classifier
        # cache handle and the stages update below still runs.
        classify.fail(e)
    finally:
        # Release the held classifier so subsequent pipelines can reuse
        # the cached session (or the idle timer can reclaim VRAM). Runs
        # whether classify completed cleanly, errored mid-loop, or hit
        # the fatal-exception path above.
        _release_classifier_cache_handle(loaded_models)

    run.update_stages(run.runner, run.job["id"], run.stages)


def _finish_without_classifying(
    run, loaded_models, effective_model_ids, release_classifier_cache_handle,
):
    """Close every per-model row when classify will not run at all."""
    # Distinguish loader-driven abort (model resolution or preload
    # failure) from benign skips (skip_classify, user cancellation,
    # missing collection): rows for the former must surface as
    # 'failed' so the per-model failure is visible on the job tree
    # — the whole point of splitting classify into per-model rows.
    loader_failed = run.stages["model_loader"]["status"] == "failed"
    loader_err = next(
        (e for e in run.errors if e.startswith("[model_loader] Fatal:")),
        None,
    )
    row_status = "failed" if loader_failed else "completed"
    row_summary = (
        "Model load failed" if loader_failed else "Skipped"
    )
    run.stages["classify"]["status"] = (
        "failed" if loader_failed else "skipped"
    )

    specs_for_step_ids = loaded_models.get("resolved_specs") or []
    step_model_ids = (
        [spec["id"] for spec in specs_for_step_ids]
        or effective_model_ids
        or ["__unresolved__"]
    )
    for mid in step_model_ids:
        run.runner.update_step(
            run.job["id"], f"classify:{mid}",
            status=row_status, summary=row_summary,
            error=loader_err if loader_failed else None,
        )
    # model_loader may have already loaded the first classifier
    # before this early-return path was hit (e.g. abort.is_set()).
    # Release its cache handle so a same-key reload can be a hit
    # and idle eviction can reclaim VRAM.
    release_classifier_cache_handle(loaded_models)
    run.update_stages(run.runner, run.job["id"], run.stages)


def _stored_animal_boxes(thread_db, photo_id, min_conf, detector_model=None):
    """Read a photo's real animal boxes from the database as classify inputs."""
    return [
        {
            "id": d["id"],
            "box_x": d["box_x"],
            "box_y": d["box_y"],
            "box_w": d["box_w"],
            "box_h": d["box_h"],
            "confidence": d["detector_confidence"],
            "category": d["category"],
        }
        for d in thread_db.get_detections(
            photo_id, min_conf=min_conf, detector_model=detector_model,
        )
        if d["detector_model"] != "full-image"
        and d["category"] == "animal"
    ]


def _detection_confidence(detection):
    return detection.get(
        "confidence", detection.get("detector_confidence", 0),
    )


@dataclass
class _SpecPass:
    """One model's pass over the collection: its identity and counters."""

    index: int
    spec: dict
    step_id: str
    clf: object
    model_type: str
    model_name: str
    # The fingerprint for THIS model's label set — pinned by
    # model_loader_stage for the first model and by _load_model_bundle
    # for subsequent ones. Keys the classifier_runs gate so a repeat
    # pass over the same (detection, model, fingerprint) skips work
    # instead of re-running inference.
    fp: str
    # Preflight's estimate of photos the cache will satisfy. In a
    # multi-model run each model owns its own Jobs step, so the stage
    # accumulates per-spec estimates while this keeps the spec's own
    # value for its ETA.
    cached_est: int = 0
    # The full preflight-cached photo id set (not just its count) so
    # overcount attribution can be scoped: recording an overcount for a
    # photo the preflight never counted would deflate the projected
    # cache-hit rate for unrelated photos and prematurely inflate
    # ``remaining_uncached`` (Codex #1468 P2).
    preflight_cached_ids: set = field(default_factory=set)
    # Photos whose runtime path deterministically skips inference
    # because they have no eligible animal target but do have a
    # confident non-animal box. Without subtracting them, a
    # person/vehicle tail inflates the ETA even though runtime traverses
    # it without model work.
    preflight_unclassifiable_ids: set = field(default_factory=set)
    raw_results: list = field(default_factory=list)
    failed: int = 0
    # Photos we never got to look at because the source volume was
    # offline. Kept apart from ``failed`` so the summary never reports
    # an unreachable share as broken photos.
    source_skipped: int = 0
    skipped_existing: int = 0
    full_image_fallbacks: int = 0
    # Photo-scoped bookkeeping for ``count`` (inferred) and ``cached``.
    # ``total`` and ``cached_estimate`` count PHOTOS (× specs), so
    # multi-subject photos with several qualifying detections must not
    # each add multiple ticks to ``count`` / ``cached`` — otherwise the
    # UI's ``inferred · cached / total`` line can read ``2 inferred / 1``
    # on a two-subject photo, and the cached-preflight would understate
    # remaining work when only one of the photo's detections is cached.
    # A photo lands in ``photos_cached`` on its first cache-hit
    # detection and is promoted to ``photos_inferred`` (decrementing
    # ``cached``, incrementing ``count``) the moment any of its
    # detections actually runs inference. Per spec, so a photo can be
    # counted once per (photo × spec) — matching ``total``.
    photos_cached: set = field(default_factory=set)
    photos_inferred: set = field(default_factory=set)
    # Includes failed inference attempts as well as successful ones.
    # ETA throughput is about completed model work, not only
    # predictions that happened to persist successfully.
    photos_attempted: set = field(default_factory=set)
    # Preflight's ``cached_est`` counts any photo whose qualifying
    # detections all have classifier_runs rows, but the runtime
    # cache-hit predicate additionally requires actual predictions to
    # exist. When we hit the fall-through case (run key present,
    # predictions missing — see ``_use_cached_result``) the photo will
    # end up in ``photos_inferred``, so subtract it from the preflight
    # estimate in the ETA calculation instead of leaving a phantom
    # future cache hit that collapses ``remaining_uncached`` to zero.
    photos_cache_overcounted: set = field(default_factory=set)
    # Photos observed to hit the confident-non-animal skip branch this
    # spec. Paired with ``preflight_unclassifiable_ids`` so the ETA can
    # subtract the unvisited share from remaining work instead of
    # treating a fast tail as inference work (Codex #1468 P2).
    photos_unclassifiable: set = field(default_factory=set)
    # Photos whose per-photo iteration was actually entered in THIS
    # spec. Scopes the reclassify clear so it never wipes predictions
    # for photos this spec never opened — on a mount-scoped give-up we
    # break out of the batch loop before reaching later photos, and on a
    # folder-scoped outage every photo under the missing folder is
    # skipped without ever producing a replacement prediction. Without
    # this, ``clear_predictions`` on a ``reclassify=True`` run would
    # delete the prior predictions for those photos even though nothing
    # in this run had a chance to rewrite them (Codex #1388 P1).
    reached_photo_ids: set = field(default_factory=set)
    # Source skips scoped to just THIS spec's reads. Codex #1388 P2
    # (r3663642360): if the folder was unreachable for model A but
    # returned before model B, the run-wide skipped set would still
    # exclude that photo from model B's reclassify clear — leaving model
    # B's stale prediction in place. Because ``add_prediction`` uses
    # ``INSERT OR IGNORE``, that stale row can win over the fresh result
    # and retain the wrong species/confidence. The run-wide set still
    # drives the outage rollup at end-of-run.
    source_skipped_photo_ids: set = field(default_factory=set)
    # Photos that iterated past the per-photo abort check IN THIS spec.
    # Used for the per-spec ``runner.update_step`` progress (bounded by
    # ``total``, not the multi-spec stage total) and the batch-end rate.
    # Captures every branch — cache hit, successful inference,
    # no-detection, image decode fail, inference fail — so spec-level
    # progress reaches ``total`` at the end of the spec regardless of
    # outcome mix.
    processed: int = 0
    # Set when the photo loop starts, after the preflight queries.
    start_time: float = 0.0
    # Wall time since ``start_time`` includes cache-lookup and
    # result-building for the cache-hit prefix, which can dwarf actual
    # model work on cache-heavy collections. Dividing
    # ``inference_attempts`` by that walltime yields an artificially low
    # rate and inflates the ETA for the uncached tail. Accumulate the
    # seconds spent preparing images and inside ``_flush_batch`` here so
    # the ETA rate reflects inference throughput only (Codex #1468 P2).
    inference_seconds: float = 0.0
    inference_batch: list = field(default_factory=list)
    has_flushed: bool = False


class _ClassifyPass:
    """State shared by every model's pass in one classify stage run."""

    def __init__(
        self,
        run,
        *,
        max_source_offline_pauses,
        cached_classify_detections,
        classification_eta_progress,
        load_model_bundle,
        record_unattempted_cache_hit,
        release_classifier_cache_handle,
        remove_attempted_cache_hits,
        source_offline_reason,
        detect_state,
        loaded_models,
        source_offline_state,
    ):
        self.run = run
        self.max_source_offline_pauses = max_source_offline_pauses
        self.cached_classify_detections = cached_classify_detections
        self.classification_eta_progress = classification_eta_progress
        self.load_model_bundle = load_model_bundle
        self.record_unattempted_cache_hit = record_unattempted_cache_hit
        self.release_classifier_cache_handle = release_classifier_cache_handle
        self.remove_attempted_cache_hits = remove_attempted_cache_hits
        self.source_offline_reason = source_offline_reason
        self.detect_state = detect_state
        self.loaded_models = loaded_models
        self.source_offline_state = source_offline_state

        # Track which per-model rows have reached a terminal state so a
        # fatal error raised by one model doesn't overwrite the status of
        # already-completed models (P2 from the Codex review). Set here,
        # before anything can raise, so ``fail`` can always read them.
        self.completed_step_ids: set = set()
        self.failed_step_ids: set = set()

    # -- setup -------------------------------------------------------

    def _setup(self):
        import classify_job
        import config as cfg

        self.classify_job = classify_job
        self.thread_db = self.run.database_factory(self.run.db_path)
        self.thread_db.set_active_workspace(self.run.workspace_id)

        user_cfg = self.thread_db.get_effective_config(cfg.load())
        self.grouping_window = user_cfg.get("grouping_window_seconds", 5)
        self.similarity_threshold = user_cfg.get("similarity_threshold", 0.85)
        self.detector_confidence = user_cfg.get("detector_confidence", 0.2)
        self.pipeline_cfg = user_cfg.get("pipeline", {})
        self.raw_session = None
        self.input_recipe = None
        if self.run.params.raw_subject_analysis:
            from raw_analysis import RECIPE, RawAnalysisSession

            self.input_recipe = RECIPE
            self.raw_session = RawAnalysisSession(
                max_size=self.pipeline_cfg.get("proxy_longest_edge") or 1536,
                sam2_variant=self.pipeline_cfg.get("sam2_variant") or "sam2-small",
                preserve_detail=True,
            )
        self.weak_detection_confidence = self.pipeline_cfg.get(
            "weak_detection_confidence", 0.12,
        )

        self.tax = self.loaded_models["tax"]
        # Fingerprint for the FIRST model is preloaded by model_loader_stage.
        # Each subsequent spec reloads its own bundle (with its own fp), so
        # loaded_models["labels_fingerprint"] is read per spec rather than
        # captured once here.
        self.specs = self.loaded_models.get("resolved_specs") or [
            self.loaded_models["active_model"]
        ]

        self.photos = self.detect_state["photos"]
        self.folders = self.detect_state["folders"]
        self.cached_detections = self.detect_state["detections"]
        self.total = len(self.photos)
        self.contextual_weak_ids = self._find_contextual_weak_ids()

        self.total_predictions_stored = 0
        self.total_full_image_fallbacks = 0
        self.total_failed = 0
        self.total_skipped_existing = 0
        # Unique photo IDs that failed in any model, so the rollup
        # message always produces a valid X-of-N ratio. total_failed sums
        # per-model failures and can exceed total in multi-model runs.
        self.failed_photo_ids: set = set()
        # Photos we never opened because their containing folder was
        # unreachable at read time. Kept apart from ``failed`` (those
        # ARE actual per-photo decode failures) so the rollup can name
        # unreachable photos honestly, and kept as unique photo IDs so
        # a folder outage that hits the same photos across every model
        # in a multi-model run doesn't multiply-count.
        self.source_skipped_photo_ids: set = set()

        self.skipped_model_names: list = []
        self.models_succeeded = 0
        # Photo IDs actually processed by the first successful model's
        # classify loop, so the stale-detection purge is scoped to
        # reclassified photos only. Using detect_state["processed_ids"]
        # (all detected photos) would incorrectly delete detections for
        # photos that weren't reached if the job was aborted mid-classify.
        self.first_model_photo_ids: set = set()
        self.fresh_full_image_ids_by_photo: dict = {}

        # ``offline_reason`` latches only when classify is giving up for
        # good, so the remaining models in a multi-model run skip
        # themselves instead of re-discovering the same dead share.
        # ``offline_pauses`` bounds the reconnect ping-pong: a user who
        # resumes without actually fixing the mount gets a few tries,
        # not an infinite pause/retry loop.
        self.offline_reason = None
        self.offline_pauses = 0
        # The previous spec's results, dropped before the next model loads.
        self.previous_raw_results = None

    def _find_contextual_weak_ids(self):
        """Pick the weak detections worth classifying from their context.

        A low-confidence box is not globally promoted. Only select weak
        runs bracketed by ordinary detections in one tightly timed
        sequence. This gives the classifier a chance to validate
        threshold-cliff frames without making every weak MegaDetector
        result eligible throughout the library.
        """
        if not (
            self.pipeline_cfg.get("weak_detection_rescue_enabled", True)
            and self.weak_detection_confidence < self.detector_confidence
            and self.photos
        ):
            return set()
        from weak_detections import contextual_weak_photo_ids

        raw_mdv6_detections = self.thread_db.get_detections_for_photos(
            [p["id"] for p in self.photos],
            min_conf=self.weak_detection_confidence,
            detector_model="megadetector-v6",
        )
        contextual_weak_ids = contextual_weak_photo_ids(
            self.photos,
            raw_mdv6_detections,
            detector_confidence=self.detector_confidence,
            weak_confidence=self.weak_detection_confidence,
            max_gap=self.pipeline_cfg.get("burst_time_gap", 3.0),
        )
        if contextual_weak_ids:
            log.info(
                "Classification: rescuing %d contextual "
                "weak-detection photo(s)",
                len(contextual_weak_ids),
            )
        return contextual_weak_ids

    def _prepare_pipeline_image(self, photo, detection):
        kwargs = {"raw_analysis": self.raw_session} if self.raw_session is not None else {}
        img, folder_path, image_path = self.classify_job._prepare_image(
            photo, self.folders, detection, **kwargs,
        )
        if img is not None and self.raw_session is not None and detection is not None:
            report = img.info.get("_vireo_raw_analysis")
            if report is not None:
                self.thread_db.masks_features.save_subject_raw_analysis(detection["id"], report)
        return img, folder_path, image_path

    def _should_stop(self):
        return self.run.control.should_abort(self.run.abort) or self.offline_reason

    def _uses_cache(self):
        """Whether classifier_runs may satisfy work instead of inference."""
        return not self.run.params.reclassify and not self.run.params.raw_subject_analysis

    def _portable_identity(self):
        """The loaded model's identity for classifier runtime fingerprints."""
        return (
            self.loaded_models.get("labels_fingerprint_full"),
            self.loaded_models.get("classifier_model_identity"),
            self.loaded_models.get("taxonomy_identity", "no-tax"),
        )

    # -- the run -----------------------------------------------------

    def run_all(self):
        self._setup()
        for spec_idx, active_spec in enumerate(self.specs):
            self._classify_with_spec(spec_idx, active_spec)
        self._finalize()

    def _classify_with_spec(self, spec_idx, active_spec):
        run = self.run
        step_id = f"classify:{active_spec['id']}"
        if run.control.should_abort(run.abort):
            # Anything not yet touched stays pending; mark it skipped
            # so the job tree finalizes cleanly.
            run.runner.update_step(run.job["id"], step_id,
                                   status="completed",
                                   summary="Skipped (cancelled)")
            return
        if self.offline_reason:
            # The share died during an earlier model. Every read for
            # this one would fail the same way, so say so instead of
            # running up a second identical failure count (the
            # incident's "0 predictions, 865 failed" second model).
            run.runner.update_step(
                run.job["id"], step_id, status="completed",
                summary=f"Skipped (source {self.offline_reason})",
            )
            return

        run.runner.update_step(run.job["id"], step_id, status="running")
        model = self._activate_model(spec_idx, active_spec, step_id)
        if model is None:
            return
        clf, model_type, model_name = model

        # Name the label space on the row. Set for both branches of
        # ``_activate_model``: the first model's bundle was built by
        # model_loader_stage and merged into loaded_models there.
        if self.loaded_models.get("label_source"):
            run.runner.update_step(
                run.job["id"], step_id,
                label_source=self.loaded_models["label_source"],
            )

        # The reclassify clear (wipes prior predictions for this
        # model+fingerprint) is intentionally deferred to
        # ``_store_results`` — clearing here and then cancelling
        # mid-classify would leave the predictions table empty for this
        # model with no replacement, erasing the user's prior
        # classifications instead of preserving them.
        spec = _SpecPass(
            index=spec_idx,
            spec=active_spec,
            step_id=step_id,
            clf=clf,
            model_type=model_type,
            model_name=model_name,
            fp=self.loaded_models.get("labels_fingerprint", "legacy"),
        )
        self._preflight(spec)
        # Set total BEFORE the pre-flight event so the UI's
        # ``stageTotal - stageCachedEst`` subtraction renders the
        # real "to classify" count on the first event the user
        # sees, not 0.
        run.stages["classify"]["total"] = self.total * len(self.specs)
        run.emit_progress(
            run.runner, run.job["id"], run.stages, "classify",
            f"Classifying with {active_spec['name']}",
            step_id=step_id,
        )
        self.previous_raw_results = spec.raw_results
        run.stages["classify"].setdefault("cached", 0)
        run.stages["classify"].setdefault("seen", 0)
        spec.start_time = time.time()

        self._classify_photos(spec)
        self._drain_pending_inference(spec)

        # Skip the grouping/storage finalization on cancel — it can
        # take a minute on large collections and the user has already
        # asked us to stop. Per-photo counters are accurate, no
        # corrective fixup needed.
        if run.control.should_abort(run.abort):
            self._finish_cancelled_spec(spec)
            return

        preds = self._store_results(spec)
        self.total_predictions_stored += preds
        self.total_full_image_fallbacks += spec.full_image_fallbacks
        self.total_failed += spec.failed
        self.total_skipped_existing += spec.skipped_existing
        self.models_succeeded += 1
        # A per-model row is "completed" only when it actually
        # finished classifying every photo it was asked to. If
        # the mount died (offline_reason latched) or a folder
        # outage left photos unread (spec.source_skipped_photo_ids),
        # the row should be ``failed`` — the Jobs page reads
        # ``step.status`` directly and auto-collapses ``completed``
        # rows without warnings, so leaving this ``completed`` would
        # show a failed job with a green, collapsed classifier row
        # (Codex #1388 P2 r3664058179). The later stage-status
        # rollup in ``_finalize`` isn't mapped back to the
        # ``classify:<model>`` step, so the fix has to land here.
        spec_failed = bool(self.offline_reason or spec.source_skipped_photo_ids)
        if spec_failed:
            self.failed_step_ids.add(step_id)
        else:
            self.completed_step_ids.add(step_id)

        if run.params.reclassify and self.models_succeeded == 1:
            self._purge_stale_detections()

        self._finish_spec_step(spec, preds, spec_failed)

    def _activate_model(self, spec_idx, active_spec, step_id):
        """Load this spec's classifier; ``None`` when the spec is skipped."""
        run = self.run
        loaded_models = self.loaded_models
        if spec_idx == 0 and "clf" in loaded_models:
            # First model preloaded by model_loader_stage.
            return (
                loaded_models["clf"],
                loaded_models["model_type"],
                loaded_models["model_name"],
            )

        # Don't reset stages["classify"]["count"] here — it accumulates
        # real inferences per-photo; jumping it to spec_idx * total would
        # silently double-count the cached hits from prior specs. total
        # is left at its multi-spec value (set by the batch-end push of
        # the previous spec, or unchanged on first entry); explicitly
        # restate it so the "Loading next model..." event always carries
        # the multi-spec total.
        run.stages["classify"]["total"] = self.total * len(self.specs)
        run.emit_progress(
            run.runner, run.job["id"], run.stages, "classify", f"Loading {active_spec['name']}...",
            step_id=step_id,
        )
        # Drop the prior model's per-photo payload BEFORE loading
        # the next bundle so we don't hold old results + new model
        # weights concurrently. Without this, multi-model runs on
        # large collections can hit transient OOMs.
        if self.previous_raw_results is not None:
            self.previous_raw_results.clear()
        # Release the previous spec's cache handle so its
        # refcount drops and a same-cache-key reload below (or
        # in another pipeline) can be a hit. Must happen
        # BEFORE popping ``clf`` so we don't lose the only
        # reference to the bundle that owns the handle.
        self.release_classifier_cache_handle(loaded_models)
        for k in ("clf", "model_type", "model_name", "model_str",
                  "labels", "label_source", "use_tol",
                  "active_model"):
            loaded_models.pop(k, None)
        try:
            bundle = self.load_model_bundle(
                active_spec, self.tax, self.thread_db, progress_step=step_id,
            )
        except Exception as model_err:
            is_classification_cancelled = (
                model_err.__class__.__name__ == "ClassificationCancelled"
            )
            if (
                run.control.should_abort(run.abort)
                or run.control.cancellation_requested()
                or is_classification_cancelled
                or str(model_err) == "classification cancelled"
            ):
                run.abort.set()
                run.runner.update_step(
                    run.job["id"], step_id,
                    status="completed",
                    summary="Skipped (cancelled)",
                )
                self.completed_step_ids.add(step_id)
                return None
            log.warning(
                "Skipping model %s: %s",
                active_spec["name"], model_err,
            )
            self.skipped_model_names.append(active_spec["name"])
            run.runner.update_step(
                run.job["id"], step_id,
                status="failed",
                error=str(model_err),
                summary=f"Failed to load: {model_err}",
            )
            self.failed_step_ids.add(step_id)
            return None
        loaded_models.update(bundle)
        return bundle["clf"], bundle["model_type"], bundle["model_name"]

    # -- preflight ---------------------------------------------------

    def _preflight(self, spec):
        """Estimate cached and unclassifiable photos before inference starts.

        One indexed query so the UI can display "~M cached, ~K to
        classify" before the first inference runs and ETAs are honest
        from the start. The estimate may overcount if a run key exists
        but no cached predictions do (e.g. a prior pass wrote
        ``category == 'match'`` with no predictions); the live
        ``cached`` counter reflects actual skips, and
        ``spec.photos_cache_overcounted`` lets
        ``_classification_eta_progress`` reconcile the estimate from
        observed misses so ``remaining_uncached`` doesn't collapse to
        zero on collections dominated by these rows.

        Contextual-weak photos use the lower ``weak_detection_confidence``
        floor to match the runtime cache-hit predicate; otherwise the
        cached weak tail is omitted from ``cached_est`` and the ETA
        overstates remaining time (Codex #1468 P2).

        Whenever detection ran, pass its in-memory map so both estimates
        use the exact candidates the classify loop will consume. Rows
        from another detector model can remain in the DB after either a
        reclassify or an ordinary runtime-fingerprint miss; letting
        those stale rows into preflight can invent work or hide a fresh
        cache hit (Codex #1468 P2). ``processed_ids`` is what
        ``_detect_batch`` actually completed this run. Photos absent from
        it are ones the detector raised on and runtime will DB-fallback
        for, so they must be evaluated against DB rows here too instead
        of treated as "no fresh animal" (Codex #1468 P2).

        The cache estimate is skipped on reclassify runs because the
        cache gate is bypassed and every photo is re-inferred.
        """
        detect_ran = self.detect_state.get("ran")
        candidate_scope = {
            "contextual_weak_photo_ids": self.contextual_weak_ids,
            "weak_confidence": (
                self.weak_detection_confidence
                if self.contextual_weak_ids
                else None
            ),
            "fresh_detections_by_photo": (
                self.detect_state["detections"] if detect_ran else None
            ),
            "fresh_processed_photo_ids": (
                self.detect_state["processed_ids"] if detect_ran else None
            ),
        }
        spec.preflight_unclassifiable_ids = (
            self.thread_db.get_unclassifiable_photos(
                [p["id"] for p in self.photos], **candidate_scope,
            )
        )
        if not self._uses_cache():
            return
        spec.preflight_cached_ids = (
            self.thread_db.get_classifier_run_cache_hits(
                [p["id"] for p in self.photos],
                spec.model_name,
                spec.fp,
                **candidate_scope,
                expected_classifier_runtime_by_detector_runtime=(
                    self._expected_runtime_by_detector_runtime()
                ),
            )
        )
        spec.cached_est = len(spec.preflight_cached_ids)
        stage = self.run.stages["classify"]
        stage["cached_estimate"] = stage.get("cached_estimate", 0) + spec.cached_est

    def _expected_runtime_by_detector_runtime(self):
        """Map each detector runtime in this batch to its classifier runtime.

        Passing this map lets the preflight reject obsolete-runtime
        classifier_runs rows the same way the per-detection
        ``get_classifier_run_key_gate`` does at runtime; without it, the
        preflight overcounts rows whose runtime rolled since the prior
        classify pass, and no observation can correct the estimate until
        those rows are visited — so the UI reads "finishing…" while
        inference is still pending (Codex #1468 P2).
        """
        labels_full, model_identity, tax_identity = self._portable_identity()
        if not (labels_full and model_identity):
            return None
        from computation_cache import classifier_runtime_fingerprint

        detector_runtimes: set = set()
        photo_id_list = [p["id"] for p in self.photos]
        det_chunk = 500
        for i in range(0, len(photo_id_list), det_chunk):
            chunk = photo_id_list[i:i + det_chunk]
            placeholders = ",".join("?" * len(chunk))
            rows = self.thread_db.conn.execute(
                f"SELECT DISTINCT runtime_fingerprint "
                f"FROM detections "
                f"WHERE photo_id IN ({placeholders})",
                chunk,
            ).fetchall()
            for row in rows:
                detector_runtimes.add(row["runtime_fingerprint"])
        expected = {}
        for det_rt in detector_runtimes:
            if det_rt is None:
                # Detector runtime not recorded — matches
                # ``classifier_runtime_for_detection``'s None return;
                # permissive entry preserves the unfiltered fallback.
                expected[det_rt] = None
            else:
                expected[det_rt] = classifier_runtime_fingerprint(
                    model_identity,
                    labels_full,
                    det_rt,
                    taxonomy_identity=tax_identity,
                )
        return expected

    # -- the photo loop ----------------------------------------------

    def _classify_photos(self, spec):
        for batch_start in range(0, self.total, _PROGRESS_BATCH_SIZE):
            if self._should_stop():
                break
            batch = self.photos[batch_start:batch_start + _PROGRESS_BATCH_SIZE]
            for photo in batch:
                # Per-photo abort check so cancel takes effect within
                # one inference (~seconds) instead of waiting for the
                # next batch boundary (~32 photos). The outer batch
                # loop's check at the top of the next iteration will
                # then break out of the batch loop entirely.
                # ``offline_reason`` rides the same boundary: once the
                # share is gone there is nothing left to read, so stop
                # pulling photos rather than spending the rest of the
                # batch collecting instant EIOs.
                if self._should_stop():
                    break
                self._classify_photo(spec, photo)

            # Batch boundary: surface the per-photo accumulated
            # count + cached to the UI. Replaces the old per-batch
            # pre-advance which lied about progress when batches
            # contained cache hits.
            self._drain_pending_inference(spec)
            self._emit_batch_progress(spec)

    def _classify_photo(self, spec, photo):
        run = self.run
        spec.processed += 1
        run.stages["classify"]["seen"] = run.stages["classify"].get("seen", 0) + 1
        # Photo entered THIS spec's per-photo body; scopes the reclassify
        # clear so unreached photos keep their prior predictions.
        spec.reached_photo_ids.add(photo["id"])
        # Record this photo as classify-processed for the first
        # successful model. Used by the stale-detection purge to
        # restrict deletions to photos actually reclassified.
        if self.models_succeeded == 0:
            self.first_model_photo_ids.add(photo["id"])

        targets = self._detections_to_classify(spec, photo)
        if targets is None:
            return
        detections, full_image_fallback = targets

        for detection in detections:
            if self._uses_cache() and self._use_cached_result(
                spec, photo, detection, len(detections),
            ):
                continue
            img, folder_path, image_path, gave_up = self._read_image(
                spec, photo, None if full_image_fallback else detection,
            )
            if gave_up:
                break
            if img is None:
                continue
            spec.inference_batch.append({
                "input_recipe": self.input_recipe,
                "photo": photo,
                "detection_id": detection["id"],
                "folder_path": folder_path,
                "image_path": image_path,
                "img": img,
            })
            # Flush the first real inference immediately. That
            # preserves the existing cancel checkpoint after model
            # warm-up, then later images batch normally for GPU
            # throughput.
            if (
                not spec.has_flushed
                or len(spec.inference_batch) >= self.classify_job._BATCH_SIZE
            ):
                self._flush_pending_inference(spec)

    def _detections_to_classify(self, spec, photo):
        """Pick the boxes to classify as ``(detections, full_image_fallback)``.

        Pull every qualifying detection for this photo from the
        detect-stage cache. Fall back to db.get_detections() only for
        photos whose per-photo detect iteration never completed (e.g.
        mid-batch exception, or the detect stage was skipped for an
        already-detected non-reclassify run — in which case the DB holds
        the authoritative rows). If MegaDetector produced no real rows at
        all, synthesize a full-image anchor so classifiers still get one
        attempt and future reruns can hit classifier_runs for that
        attempt. ``None`` means the photo is skipped without inference.
        """
        photo_id = photo["id"]
        is_contextual_weak = photo_id in self.contextual_weak_ids
        detection_floor = (
            self.weak_detection_confidence
            if is_contextual_weak
            else self.detector_confidence
        )
        if photo_id in self.cached_detections:
            # cached_detections from _detect_batch can include
            # full-image rows when an earlier pass synthesized
            # them (legacy db state); filter to match the
            # fallback-query branch below so classifiers only
            # see real, qualifying animal boxes. _detect_batch's
            # fresh cache contains raw low-confidence boxes too,
            # while DB reads normally apply this threshold.
            photo_dets = self.cached_classify_detections(
                self.cached_detections[photo_id],
                detection_floor,
                contextual_weak=is_contextual_weak,
            )
        else:
            photo_dets = _stored_animal_boxes(
                self.thread_db, photo_id, detection_floor,
                # Contextual rescue is defined only from MegaDetector
                # V6 evidence. Keep the detector-failure DB fallback
                # on that same candidate set so a stale,
                # higher-confidence foreign row cannot diverge from
                # the cache preflight's selected crop (Codex #1468 P2).
                detector_model=(
                    "megadetector-v6" if is_contextual_weak else None
                ),
            )
        if is_contextual_weak and not photo_dets:
            # detect_state intentionally caches only rows passing the
            # ordinary workspace threshold on reuse runs. Recover the raw
            # weak row from the database for this explicitly selected
            # bridge.
            photo_dets = _stored_animal_boxes(
                self.thread_db, photo_id, self.weak_detection_confidence,
                detector_model="megadetector-v6",
            )
        if is_contextual_weak and photo_dets:
            # One best weak crop is enough to validate the bridge.
            # Classifying every low-confidence box would multiply work
            # and false-positive risk.
            #
            # Explicit ``detector_confidence DESC, id ASC`` tie-break so
            # the runtime picks the same detection the preflight's
            # cache-hit query picks. ``cached_detections`` comes back
            # from ``_detect_batch`` in raw detector/NMS output order —
            # no ID tie-break — while ``get_classifier_run_cache_hits``
            # ranks by ``detector_confidence DESC, id ASC``. Without this
            # sort, two equal-confidence weak boxes can leave runtime
            # inferring the higher-ID box while the preflight marked the
            # photo cached via a portable-cache run on the lower-ID box;
            # the overcount tracker then can't correct it because the
            # runtime-selected detection has no run key of its own
            # (Codex #1468 P2).
            photo_dets = sorted(
                photo_dets,
                key=lambda d: (
                    -float(_detection_confidence(d) or 0),
                    d.get("id", 0),
                ),
            )[:1]
        if photo_dets:
            return photo_dets, False

        # No animal box is usable at either the ordinary threshold or
        # the contextual weak-rescue floor. Treat this the same as a true
        # no-detection photo and give the classifier a full-image
        # attempt. Raw detector output is retained for diagnostics/cache
        # reuse, but a 1% noise box must not suppress classification of
        # an otherwise visible subject.
        #
        # Exception: MegaDetector emitted a confident non-animal
        # (person/vehicle) box at or above ``detector_confidence``.
        # Sending the entire human/vehicle frame to the wildlife
        # classifier would persist a spurious species prediction. Skip
        # the fallback in that case and leave the photo without a
        # classifier run, matching the old raw-detection guard's intent
        # while still rescuing sub-threshold animal photos.
        if self._has_confident_non_animal(photo_id):
            # This is the only no-target path that skips inference.
            # Track it so the ETA does not project confident person/
            # vehicle tails as pending model work.
            spec.photos_unclassifiable.add(photo_id)
            return None

        full_det_id = self._full_image_detection_id(photo_id)
        spec.full_image_fallbacks += 1
        self.fresh_full_image_ids_by_photo.setdefault(
            photo_id, set(),
        ).add(full_det_id)
        return [{
            "id": full_det_id,
            "box_x": 0,
            "box_y": 0,
            "box_w": 1,
            "box_h": 1,
            "confidence": 0,
            "category": "animal",
            "detector_model": "full-image",
        }], True

    def _has_confident_non_animal(self, photo_id):
        if photo_id in self.cached_detections:
            return any(
                d.get("detector_model") != "full-image"
                and d.get("category", "animal") != "animal"
                and _detection_confidence(d) >= self.detector_confidence
                for d in self.cached_detections[photo_id]
            )
        return any(
            d["detector_model"] != "full-image"
            and d["category"] != "animal"
            for d in self.thread_db.get_detections(
                photo_id, min_conf=self.detector_confidence,
            )
        )

    def _full_image_detection_id(self, photo_id):
        """Reuse or write the synthetic full-image detection for a photo."""
        existing_full = self.thread_db.get_detections(
            photo_id,
            detector_model="full-image",
            min_conf=0,
        )
        if existing_full and not self.run.params.reclassify:
            return existing_full[0]["id"]

        from computation_cache import (
            full_image_runtime_fingerprint,
            source_input,
        )

        full_runtime = full_image_runtime_fingerprint()
        identity = self.thread_db.conn.execute(
            """SELECT file_hash, companion_path
               FROM photos WHERE id = ?""",
            (photo_id,),
        ).fetchone()
        full_input = None
        if identity is not None and not identity["companion_path"]:
            try:
                _block, full_input = source_input(
                    identity["file_hash"],
                    "vireo-detector-source-v1",
                )
            except ValueError:
                full_input = None
        # Combine save_detections + record_detector_run into one
        # transaction via write_detection_batch so a crash between them
        # can't leave a full-image detection row without its matching
        # detector_runs row — mirroring the invariant documented for
        # write_detection_batch.
        full_det_ids = self.thread_db.write_detection_batch(
            photo_id, "full-image",
            [{
                "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                "confidence": 0,
                "category": "animal",
            }],
            runtime_fingerprint=full_runtime,
            input_fingerprint=full_input,
        )
        return full_det_ids[0]

    def _use_cached_result(self, spec, photo, detection, n_detections):
        """Apply the classifier-run gate; ``True`` when no inference is needed.

        Skip work when this exact (detection, classifier_model,
        labels_fingerprint) triple was already classified. Reclassify
        bypasses the gate so users can force a fresh pass. When gated,
        surface the cached top-1 prediction into raw_results so
        downstream grouping/storage sees it — otherwise the cached
        detection would silently drop out of the grouping pipeline.
        """
        thread_db = self.thread_db
        expected_classifier_runtime = None
        labels_full, model_identity, tax_identity = self._portable_identity()
        if labels_full and model_identity:
            from computation_cache import classifier_runtime_for_detection

            expected_classifier_runtime = classifier_runtime_for_detection(
                thread_db,
                detection["id"],
                model_identity,
                labels_full,
                taxonomy_identity=tax_identity,
            )
        # Fetch both accepted and gate-rejected keys in one query.
        # ``rejected_keys`` is the "existed but the runtime_fingerprint
        # rule rejected it" set — the preflight ``count_classifier_runs``
        # ignores that rule, so without recording these as overcounts
        # below, a photo whose only key is gate-rejected sits in
        # ``cached_est`` forever and ETA prematurely reads "finishing…"
        # on runs where the detector fingerprint has rolled since the
        # prior classifier pass (Codex #1468 P2).
        if expected_classifier_runtime is not None:
            run_keys, rejected_keys = thread_db.model_runs.get_classifier_run_key_gate(
                detection["id"],
                expected_classifier_runtime,
            )
        else:
            # No expected runtime fingerprint means portable identity
            # isn't wired up (legacy path); fall back to the unfiltered
            # gate. Nothing to reconcile because the preflight and the
            # gate then agree on which rows count.
            run_keys = thread_db.model_runs.get_classifier_run_keys(detection["id"])
            rejected_keys = set()

        run_key = (spec.model_name, spec.fp)
        if run_key in run_keys:
            cached = thread_db.get_predictions_for_detection(
                detection["id"],
                classifier_model=spec.model_name,
                labels_fingerprint=spec.fp,
                min_classifier_conf=0,
            )
            if cached:
                self._count_cache_hit(spec, photo)
                spec.raw_results.append(self._cached_raw_result(
                    spec, photo, detection, cached[0], n_detections,
                ))
                return True
            # No cached prediction rows: honor a measured
            # ``classifier_match_scores`` summary for the same triple as
            # a completed zero-candidate run, mirroring the boxed and
            # full-image gates in ``classify_job.py``. Without this, a
            # partially cached combined-pipeline run re-classifies
            # detections whose no-match verdict was already recorded and
            # can replace an authoritative "nothing in this list fits"
            # outcome with a fresh inference (Codex P2 on a1be510). The
            # preflight already counted this photo as cached because the
            # classifier-run key exists, so no overcount to reconcile.
            if thread_db.model_runs.has_classifier_match_score(
                detection["id"], spec.model_name, spec.fp,
            ):
                self._count_cache_hit(spec, photo)
                return True
            # Run key with no cached rows (e.g. prior pass stored
            # `category == 'match'` so the prediction was intentionally
            # not written). Fall through to re-classify instead of
            # stranding the detection. Reconcile the preflight's cache
            # estimate: it counted this photo based solely on the run key
            # existing, but runtime will actually infer. Only record the
            # overcount when the preflight actually counted this photo —
            # otherwise a multi-detection photo whose other detection
            # lacked any run key was never in ``cached_est`` to begin
            # with, and treating this fall-through as a failed preflight
            # prediction would deflate the projection for unrelated
            # cache-heavy tails (Codex #1468 P2).
            if photo["id"] in spec.preflight_cached_ids:
                spec.photos_cache_overcounted.add(photo["id"])
        elif run_key in rejected_keys:
            # A classifier_runs row exists but its
            # ``runtime_fingerprint`` no longer matches (typically the
            # detector was re-run since the prior classify). Preflight
            # counted this photo as cached; runtime will infer. Record
            # the overcount so ``_classification_eta_progress`` can
            # deflate its projected future cache hits — otherwise a
            # collection made entirely of these rows sees
            # ``remaining_uncached`` collapse to zero after the first
            # uncached batch and the UI reports "finishing…" while most
            # photos still need inference (Codex #1468 P2). Same
            # preflight-membership guard: an unrelated detection without
            # any run key on this photo means the preflight never
            # counted it, so this branch isn't a preflight miss to
            # reconcile.
            if photo["id"] in spec.preflight_cached_ids:
                spec.photos_cache_overcounted.add(photo["id"])
        return False

    def _count_cache_hit(self, spec, photo):
        spec.skipped_existing += 1
        # Photo-scoped ``cached`` bucket: only the FIRST cached
        # detection per photo (per spec) ticks the counter. A subsequent
        # inferred detection on the same photo will promote it into
        # ``count`` in ``_flush_pending_inference``.
        if self.record_unattempted_cache_hit(
            photo["id"],
            spec.photos_inferred,
            spec.photos_attempted,
            spec.photos_cached,
        ):
            self.run.stages["classify"]["cached"] += 1

    def _cached_raw_result(self, spec, photo, detection, top, n_detections):
        folder_path = self.folders.get(photo["folder_id"], "")
        timestamp = None
        if photo["timestamp"]:
            with contextlib.suppress(ValueError, TypeError):
                timestamp = dt.fromisoformat(photo["timestamp"])
        embedding = None
        if spec.model_type != "timm":
            # Prefer the per-detection embedding so multi-subject cache
            # reruns don't reuse a single last-wins photo-level vector
            # for every detection. Fall back to the photo-level entry
            # only when the photo has a single qualifying detection —
            # there the photo-level row unambiguously belongs to it, so
            # legacy data (classified before per-detection variants were
            # written) still refines correctly.
            emb_blob = self.thread_db.masks_features.get_embedding(
                photo["id"], spec.model_name,
                variant=f"det:{detection['id']}",
            )
            if not emb_blob and n_detections == 1:
                emb_blob = self.thread_db.masks_features.get_embedding(
                    photo["id"], spec.model_name,
                )
            if emb_blob:
                embedding = np.frombuffer(emb_blob, dtype=np.float32)
        return {
            "photo": photo,
            "detection_id": detection["id"],
            "folder_path": folder_path,
            "image_path": os.path.join(folder_path, photo["filename"]),
            "prediction": top["species"],
            "confidence": top["confidence"],
            "timestamp": timestamp,
            "filename": photo["filename"],
            "embedding": embedding,
            "taxonomy": self.classify_job._cached_prediction_taxonomy(top),
            "_existing": True,
        }

    def _read_image(self, spec, photo, detection):
        """Load the crop, waiting out a dropped source.

        Returns ``(img, folder_path, image_path, gave_up)``. ``img`` is
        ``None`` when the photo is broken or its folder is unreachable;
        ``gave_up`` is ``True`` when classify stopped on a dead source.

        Before blaming the photo, check whether the source itself
        vanished. A dropped share fails every remaining read instantly,
        so counting these as per-photo failures would report the
        untouched remainder of the collection as broken images.

        Loop rather than one-shot pause+retry so a mount that stays dead
        across a resume also gets latched when this happens to be the
        last photo (or last detection) needing an image read: silently
        continuing on the failed retry would leave ``offline_reason``
        unset, abort clear, and extract_masks / eye_keypoints would
        still walk every detected photo reissuing reads against the dead
        share (Codex #1388 P1 r3663278142). ``_handle_source_offline``
        bounds the total pause/retry cycles via
        ``_MAX_SOURCE_OFFLINE_PAUSES``, so this can't loop forever.

        Image preparation time is tracked apart from the pause loop and
        folded into ``spec.inference_seconds`` only when the photo
        actually enters the inference batch. Cache-hit and
        image-decode-fail paths never contribute to
        ``inference_attempts``, so their prep time must not count toward
        the rate; a photo whose retries eventually succeed DOES count, so
        accumulate across every ``_prepare_image`` call for this
        detection (Codex #1468 P2). Without it, RAW/JPEG-heavy or
        slow-storage runs publish a rate that only reflects GPU work and
        understate the ETA.
        """
        prep_seconds = 0.0
        prep_started = time.time()
        img, folder_path, image_path = self._prepare_pipeline_image(photo, detection)
        prep_seconds += max(time.time() - prep_started, 0.0)
        while img is None:
            offline = self.source_offline_reason(folder_path, image_path)
            if offline is None:
                # The source is reachable; this one file is genuinely
                # broken.
                spec.failed += 1
                self.failed_photo_ids.add(photo["id"])
                return None, folder_path, image_path, False
            scope, reason = offline
            if scope == "folder":
                # A single missing folder is not evidence the whole
                # source is offline; skip this photo as unreachable and
                # let later photos in healthy folders keep processing.
                # Guard the counter with the per-spec skipped-photo set
                # so a multi-subject photo (N qualifying detections)
                # counts once, not N times — the per-spec ``total`` and
                # step summary are photo-scoped, and without this the row
                # could report e.g. ``3 unreachable`` out of ``1`` photo
                # (Codex #1388 P2 r3664348763).
                if photo["id"] not in spec.source_skipped_photo_ids:
                    spec.source_skipped += 1
                self._mark_source_skipped(spec, photo)
                return None, folder_path, image_path, False
            # Mount-scoped: park and wait for the user to reconnect. On
            # give-up, _handle_source_offline latches ``offline_reason``
            # and returns False — the caller then stops this photo and
            # the remaining photos short-circuit the same way (and the
            # finalization rollup sets abort so downstream stages skip
            # the dead source).
            if not self._handle_source_offline(reason, spec.step_id):
                # Give-up: image never loaded, so this photo is
                # unreached — same bucket as the folder-scoped skip
                # above. Keeps the reclassify clear from wiping its prior
                # prediction (Codex #1388 P1 r3663159360).
                self._mark_source_skipped(spec, photo)
                return None, folder_path, image_path, True
            # Resumed. Retry the same read before advancing — silently
            # skipping the paused-during photo would leave it
            # unclassified even though the source came back, and on a
            # reclassify run finalization would clear its old prediction
            # with no replacement (Codex #1388 P2). If the retry also
            # fails, the loop re-probes and either pauses again
            # (bounded), degrades to folder-scope, or gives up.
            prep_started = time.time()
            img, folder_path, image_path = self._prepare_pipeline_image(
                photo, detection,
            )
            prep_seconds += max(time.time() - prep_started, 0.0)
            if img is not None:
                # Successful recovery: refund the pause budget so an
                # unrelated outage later in the run — a second share that
                # drops, or the same share dropping again hours later —
                # still gets its own bounded retry window. Without this,
                # three separately-recovered outages exhaust the budget
                # and the fourth outage immediately takes the give-up
                # branch without ever offering Resume (Codex #1388 P2
                # r3663816327). The pause bound is meant to protect
                # against a user who resumes WITHOUT actually remounting
                # the share — a successful read is proof the share IS
                # back, so the counter can honestly reset.
                self.offline_pauses = 0
        spec.inference_seconds += prep_seconds
        return img, folder_path, image_path, False

    def _mark_source_skipped(self, spec, photo):
        self.source_skipped_photo_ids.add(photo["id"])
        spec.source_skipped_photo_ids.add(photo["id"])

    # -- source outages ----------------------------------------------

    def _handle_source_offline(self, reason, step_id=None):
        """Park on a dead source; report whether we may continue.

        Returns True when the run was paused and then resumed, so
        the caller should keep classifying (the user reconnected
        the share). Returns False when classify should stop: the
        runner can't park, the job was cancelled, or we've already
        paused for this too many times.
        """
        run = self.run
        if self.offline_pauses >= self.max_source_offline_pauses:
            self.offline_reason = reason
            return False
        self.offline_pauses += 1
        # Don't push a pause message into ``errors``: a successful
        # reconnect+resume leaves classify completing normally, but
        # templates/pipeline.html treats every ``[classify]`` error
        # as a failed stage and suppresses the success redirect
        # (Codex #1388 P1). ``pause_job`` below already flips the
        # job state to ``pausing``/``paused`` (that's what the UI
        # renders while parked), and the give-up path in ``_finalize``
        # appends its own ``[classify] Fatal:`` entry — so a run
        # that never comes back still surfaces a real terminal
        # error, and a run that does come back does not.
        log.warning("Classify paused: source offline (%s)", reason)
        # Publish BEFORE calling pause_job so the reason is already
        # in job['progress'] by the time the UI reacts to the
        # ``pausing`` status flip. Otherwise the banner would only
        # appear after the next progress event, which for a fully
        # blocked read might be minutes away.
        self._publish_pause_reason(reason, step_id)

        pause = getattr(run.runner, "pause_job", None)
        paused = False
        if pause is not None:
            try:
                paused = bool(pause(run.job["id"]))
            except Exception:
                log.warning("Could not pause classify for an offline source", exc_info=True)
        if not paused:
            # ``pause_job`` returns False in two very different
            # cases: (a) the job is not pausable at all — a
            # non-pausable pipeline or a direct
            # ``run_pipeline_job`` call in a test — genuinely
            # nothing to park on, and (b) the user already
            # pressed Pause while this network read was blocked,
            # so the job's public state is already ``pausing``
            # and a second pause request is a no-op. Treating (b)
            # as "can't pause" would waste the user's Pause
            # click by giving up instead of parking (Codex
            # #1388 P2 r3663383518); check
            # ``pause_requested`` and fall through to the
            # checkpoint so the run parks on the user's
            # already-in-flight request.
            pause_probe = getattr(run.runner, "pause_requested", None)
            already_pausing = False
            if pause_probe is not None:
                try:
                    already_pausing = bool(pause_probe(run.job["id"]))
                except Exception:
                    log.warning("Could not read the job's pause request", exc_info=True)
            if not already_pausing:
                # Nothing to park on. Stop rather than spin
                # through the rest of the collection
                # collecting instant EIOs.
                self.offline_reason = reason
                return False

        # Blocks here until the user resumes or cancels. Classify
        # is a registered pause participant, so this is the same
        # safe boundary the per-photo cancel check already uses.
        if run.control.pause_checkpoint():
            self.offline_reason = reason
            return False
        # Successful resume: drop the stale "reconnect and Resume"
        # banner before the caller retries the read.
        self._clear_pause_reason(step_id)
        return True

    def _publish_pause_reason(self, reason, step_id):
        """Surface the offline reason on transient progress/pause state.

        Codex #1388 P1 r3663383513: the pause itself was only
        reported via server-side log; the jobs UI showed a generic
        ``paused`` state with no indication of *why* classify
        parked, so users could not tell they needed to remount a
        named volume and might keep pressing Resume until the
        bounded retry budget converted a recoverable outage into a
        failed run. Publish the reason via the progress event
        (mirrored onto ``job['progress']`` for /api/jobs polls and
        the SSE stream alike) and set the classify step's
        ``current_file`` so the reason renders directly beneath
        the paused pipeline step, right next to the Resume button.
        Kept off ``errors`` so a successful resume still finalizes
        as ``completed``.
        """
        run = self.run
        message = (
            f"Paused: source {reason}. Reconnect it and press "
            f"Resume to keep classifying."
        )
        if step_id is not None:
            try:
                run.runner.update_step(
                    run.job["id"], step_id, current_file=message,
                )
            except Exception:
                log.warning("Could not publish the classify pause reason", exc_info=True)
        run.emit_progress(
            run.runner, run.job["id"], run.stages, "classify",
            message,
            step_id=step_id,
            pause_reason=message,
        )

    def _clear_pause_reason(self, step_id):
        """Drop the pause banner once the user has resumed.

        Leaving a stale ``pause_reason`` on ``job['progress']``
        after a successful reconnect would keep the "reconnect
        and Resume" banner visible while classify happily runs.
        """
        run = self.run
        if step_id is not None:
            try:
                run.runner.update_step(
                    run.job["id"], step_id, current_file="",
                )
            except Exception:
                log.warning("Could not clear the classify pause reason", exc_info=True)
        run.emit_progress(
            run.runner, run.job["id"], run.stages, "classify",
            "Resumed classification",
            step_id=step_id,
            pause_reason=None,
        )

    # -- inference batches -------------------------------------------

    def _drain_pending_inference(self, spec):
        """Run the pending batch, or discard it if the job was cancelled."""
        if self.run.control.should_abort(self.run.abort):
            self._close_pending_inference(spec)
        else:
            self._flush_pending_inference(spec)

    def _close_pending_inference(self, spec):
        for entry in spec.inference_batch:
            # Releasing decoded images; a close failure frees nothing more.
            with contextlib.suppress(Exception):
                entry["img"].close()
        spec.inference_batch.clear()

    def _flush_pending_inference(self, spec):
        if not spec.inference_batch:
            return
        stage = self.run.stages["classify"]
        raw_results = spec.raw_results

        pending = list(spec.inference_batch)
        spec.inference_batch.clear()
        spec.has_flushed = True
        attempted_photo_ids = {entry["photo"]["id"] for entry in pending}
        spec.photos_attempted.update(attempted_photo_ids)
        removed_cache_hits = self.remove_attempted_cache_hits(
            attempted_photo_ids,
            spec.photos_cached,
        )
        if removed_cache_hits:
            stage["cached"] = max(0, stage.get("cached", 0) - removed_cache_hits)
        pre_len = len(raw_results)
        # GPU serialisation lives inside _flush_batch around the
        # inference call so the DB upserts/result-building afterward
        # don't hold the semaphore while the GPU is idle.
        flush_started = time.time()
        n_batch_failed = self.classify_job._flush_batch(
            pending, spec.clf, spec.model_type, spec.model_name,
            self.thread_db, raw_results,
        )
        spec.inference_seconds += max(time.time() - flush_started, 0.0)
        spec.failed += n_batch_failed

        successful_det_ids = {
            r.get("detection_id") for r in raw_results[pre_len:]
        }
        if n_batch_failed:
            for entry in pending:
                if entry.get("detection_id") not in successful_det_ids:
                    self.failed_photo_ids.add(entry["photo"]["id"])

        self.classify_job._record_batch_classifier_runs(
            self.thread_db, pending, spec.model_name, spec.fp, raw_results,
            pre_len,
            labels_fingerprint_full=self.loaded_models.get(
                "labels_fingerprint_full"
            ),
            model_identity=self.loaded_models.get(
                "classifier_model_identity"
            ),
        )

        # Photo-scoped ``count`` bookkeeping: each distinct photo whose
        # flush yielded at least one successful classification counts
        # once. If it was previously bucketed as fully-cached (an earlier
        # detection hit the cache), migrate it — decrement ``cached`` and
        # add it to ``count`` — so ``count + cached`` stays bounded by
        # the (photo-scoped) ``total``.
        new_photo_ids = {r["photo"]["id"] for r in raw_results[pre_len:]}
        promoted = new_photo_ids & spec.photos_cached
        if promoted:
            spec.photos_cached.difference_update(promoted)
            stage["cached"] = max(0, stage.get("cached", 0) - len(promoted))
        newly_inferred = new_photo_ids - spec.photos_inferred
        if newly_inferred:
            spec.photos_inferred.update(newly_inferred)
            stage["count"] = stage.get("count", 0) + len(newly_inferred)

    def _emit_batch_progress(self, spec):
        run = self.run
        n_specs = len(self.specs)
        run.stages["classify"]["total"] = self.total * n_specs
        elapsed = max(time.time() - spec.start_time, 0.01)
        run.emit_progress(
            run.runner, run.job["id"], run.stages, "classify",
            f"Classifying with {spec.spec['name']}"
            + (f" ({spec.index + 1}/{n_specs})" if n_specs > 1 else ""),
            step_id=spec.step_id,
            rate=round(spec.processed / elapsed * 60, 1),
        )
        run.runner.update_step(
            run.job["id"], spec.step_id,
            progress={
                "current": spec.processed,
                "total": self.total,
                **self.classification_eta_progress(
                    total=self.total,
                    seen=spec.processed,
                    cached_estimate=spec.cached_est,
                    cache_hits=len(spec.photos_cached),
                    inference_attempts=len(spec.photos_attempted),
                    classified=len(spec.photos_inferred),
                    # Inference-active seconds only. Wall time since
                    # ``start_time`` includes the cache-traversal prefix
                    # and would deflate the per-attempt rate on
                    # cache-heavy runs (Codex #1468 P2).
                    elapsed=spec.inference_seconds,
                    cache_overcount=len(spec.photos_cache_overcounted),
                    unclassifiable_estimate=len(
                        spec.preflight_unclassifiable_ids
                    ),
                    unclassifiable_seen=len(spec.photos_unclassifiable),
                ),
            },
        )

    # -- finishing a spec --------------------------------------------

    def _finish_cancelled_spec(self, spec):
        run = self.run
        run.emit_progress(
            run.runner, run.job["id"], run.stages, "classify",
            f"Cancelled — {spec.processed} of "
            f"{self.total} processed",
            step_id=spec.step_id,
        )
        run.runner.update_step(
            run.job["id"], spec.step_id,
            status="completed",
            progress={
                "current": spec.processed,
                "total": self.total,
            },
            summary=(
                f"Cancelled "
                f"({spec.processed} of {self.total} processed)"
            ),
        )

    def _store_results(self, spec):
        """Persist this spec's predictions; returns how many were stored."""
        thread_db = self.thread_db
        # Reclassify clear, deferred until classification finished so a
        # mid-batch cancel leaves the user's prior predictions intact
        # (Codex P1 review on #710). Scope by labels_fingerprint so
        # reclassifying one workspace's label set doesn't wipe another
        # workspace's cached predictions on the same photos under its own
        # fingerprint (shared-folder setups). ``clear_run_keys=False``
        # because the per-photo ``record_classifier_run`` calls in the
        # loop already wrote fresh classifier_runs rows for processed
        # detections — wiping them here would strand the gate and force
        # the next non-reclassify pass to re-infer everything.
        #
        # Scope to photos this spec actually reached AND wasn't skipped
        # as source-offline: an unreached photo (mount-scoped give-up, or
        # every photo under a vanished folder) is one this run had no
        # chance to rewrite, so wiping its prior prediction would leave
        # it with nothing at all (Codex #1388 P1). Use the per-spec
        # skipped set rather than the run-wide one so a photo skipped for
        # an earlier spec but successfully reached this spec still has
        # its stale prior prediction cleared before
        # ``_store_grouped_predictions`` writes the fresh row —
        # ``add_prediction`` is ``INSERT OR IGNORE`` and would otherwise
        # keep the stale species/confidence (Codex #1388 P2
        # r3663642360). Fall back to the collection-wide clear when no
        # source outage hit this spec — that's still the desired
        # behavior on a clean reclassify.
        if self.run.params.reclassify:
            if spec.source_skipped_photo_ids:
                clear_ids = list(
                    spec.reached_photo_ids - spec.source_skipped_photo_ids
                )
            else:
                clear_ids = [p["id"] for p in self.photos]
            if clear_ids:
                thread_db.clear_predictions(
                    model=spec.model_name,
                    collection_photo_ids=clear_ids,
                    labels_fingerprint=spec.fp,
                    clear_run_keys=False,
                )

        group_result = self.classify_job._store_grouped_predictions(
            spec.raw_results, self.run.job["id"], spec.model_name,
            self.grouping_window, self.similarity_threshold, self.tax,
            thread_db,
            labels_fingerprint=spec.fp,
        )
        # promote_and_publish reads persisted predictions written by
        # _store_grouped_predictions above; running it inside
        # _record_batch_classifier_runs would no-op because those rows
        # don't exist yet, leaving fresh classifier_runs stranded on
        # runtime_fingerprint = 'legacy' and out of bundle exports.
        # Reconstruct the configured ArtifactStore from the path stashed
        # by ``run_pipeline_job`` so classifier artifacts published here
        # land in the same cache the status / export /
        # catalog-reapplication paths use when ``COMPUTATION_CACHE_DIR``
        # is overridden.
        from computation_cache import ArtifactStore

        publish_cache_dir = self.run.job.get("_computation_cache_dir")
        labels_full, model_identity, tax_identity = self._portable_identity()
        self.classify_job._publish_classifier_runs_for_raw_results(
            thread_db, spec.raw_results, spec.model_name, spec.fp,
            labels_fingerprint_full=labels_full,
            model_identity=model_identity,
            taxonomy_identity=tax_identity,
            store=(
                ArtifactStore(publish_cache_dir)
                if publish_cache_dir else None
            ),
        )
        return group_result["predictions_stored"]

    def _purge_stale_detections(self):
        """Drop pre-run detections the reclassify did not reproduce.

        Only runs after the FIRST successful model has written fresh
        predictions, so a run where every model fails to load leaves
        prior detections (and their cascaded predictions) intact. Photos
        whose detect iteration never completed keep their old rows.
        """
        pre_ids = self.detect_state["pre_run_det_ids"]
        if not pre_ids:
            return
        # Scope the purge to photos whose detect AND classify iterations
        # both completed in this run. Using only classify coverage would
        # delete rows for photos that hit the db.get_detections()
        # fallback (i.e. never got a fresh detect). Using only detect
        # coverage would delete rows for photos the classifier never
        # reached. The intersection guarantees there's a replacement
        # detection AND that the classifier considered it.
        #
        # Also subtract source-skipped photos: ``first_model_photo_ids``
        # is added to at the TOP of the per-photo body (before the image
        # read), so a photo whose read later failed with the source
        # offline is still in that set. Without this subtraction, the
        # purge below would delete any pre-run detection ids whose boxes
        # differ from the fresh boxes even for photos we never classified
        # — cascading through their prior predictions despite the
        # ``clear_predictions`` exclusion that already spares them (Codex
        # #1388 P1 r3663922709).
        purge_ids = (
            (self.first_model_photo_ids & self.detect_state["processed_ids"])
            - self.source_skipped_photo_ids
        )
        # Delete only pre-run ids the current run did NOT re-produce.
        # Detection ids are content-addressed (vireo/detection_id.py), so
        # re-detecting the same boxes yields the SAME ids as the pre-run
        # snapshot, and write_detection_batch UPSERTs them with the
        # freshly written predictions now hanging off them. Deleting
        # every pre-run id unconditionally would cascade-delete those
        # predictions (and the live detection rows) for every photo whose
        # boxes didn't change — the common reclassify case. Compare
        # against the ids THIS run actually re-detected
        # (detect_state["detections"] is the in-memory map _detect_batch
        # built). A no-detection photo may also have a freshly used
        # synthetic full-image anchor from the fallback path; preserve
        # that id so the purge does not cascade-delete the new fallback
        # prediction. Other pre-run rows on empty photos are stale and
        # get purged (write_detection_batch([]) already cleared the
        # MegaDetector rows at the data layer — this is the
        # belt-and-suspenders pass and cross-model cleanup). A photo
        # re-detected with the same boxes has its ids in the fresh set,
        # so they survive.
        fresh_by_photo = self.detect_state["detections"]
        stale_ids = [
            det_id
            for photo_id, id_set in pre_ids.items()
            if photo_id in purge_ids
            for det_id in id_set
            if det_id not in (
                {d["id"] for d in fresh_by_photo.get(photo_id, [])}
                | self.fresh_full_image_ids_by_photo.get(photo_id, set())
            )
        ]
        if stale_ids:
            getattr(
                self.thread_db,
                "delete_detections_by_ids",
                lambda _: None,
            )(stale_ids)
            log.debug(
                "reclassify: purged %d stale detection rows for "
                "%d photos (%d not in purge scope, rows preserved)",
                len(stale_ids),
                len(purge_ids & pre_ids.keys()),
                len(pre_ids) - len(purge_ids & pre_ids.keys()),
            )

    def _finish_spec_step(self, spec, preds, spec_failed):
        total = self.total
        parts = [f"{preds} predictions"]
        if spec.skipped_existing:
            parts.append(f"{spec.skipped_existing} cached")
        if spec.full_image_fallbacks:
            parts.append(f"{spec.full_image_fallbacks} full-image fallback")
        if self.contextual_weak_ids:
            parts.append(
                f"{len(self.contextual_weak_ids)} weak detections rescued"
            )
        if spec.failed:
            parts.append(f"{spec.failed} failed")
        if spec.source_skipped:
            parts.append(
                f"{spec.source_skipped} unreachable (source offline)"
            )
        if self.offline_reason:
            # Name the photos we never reached rather than folding
            # them into a count that reads as "classified".
            parts.append(
                f"stopped after {spec.processed} of {total} — "
                f"source {self.offline_reason}"
            )
        # Attach the outage as ``error=`` on the failed row so the Jobs
        # page shows a human-readable reason next to the collapsed step,
        # matching the shape of the model-load-failure row in
        # ``_activate_model``.
        step_error = None
        if spec_failed:
            if self.offline_reason:
                step_error = (
                    f"Stopped after {spec.processed} of "
                    f"{total} — source {self.offline_reason}"
                )
            else:
                step_error = (
                    f"{len(spec.source_skipped_photo_ids)} of "
                    f"{total} photos unreachable "
                    "(source offline)"
                )
        self.run.runner.update_step(
            self.run.job["id"], spec.step_id,
            status="failed" if spec_failed else "completed",
            summary=", ".join(parts),
            error=step_error,
        )

    # -- finishing the stage -----------------------------------------

    def _finalize(self):
        run = self.run
        total = self.total
        # Cancellation takes precedence over the all-models-failed-to-load
        # signal: if the user cancelled mid-classify after a prior model
        # had already been added to skipped_model_names, raising here
        # would misclassify the cancel as a fatal load failure and
        # overwrite the per-model 'Cancelled' summary in ``fail``.
        if (
            self.models_succeeded == 0
            and self.skipped_model_names
            and not run.control.should_abort(run.abort)
        ):
            raise RuntimeError(
                f"All {len(self.skipped_model_names)} model(s) failed to load: "
                + ", ".join(self.skipped_model_names)
            )

        # Roll up per-photo failures into a single classify stage status
        # + errors[] entry, matching the pattern in #562. Per-model step
        # rows already carry their own summary; the stage status reflects
        # the whole classify pass.  error_count uses unique failed photo
        # IDs (not per-model attempt count) so the badge can never
        # exceed total photos.
        n_failed_photos = len(self.failed_photo_ids)
        n_source_skipped_photos = len(self.source_skipped_photo_ids)
        # Publish the source-skipped set to downstream stages BEFORE
        # deciding to set ``abort``. extract_masks and eye_keypoints
        # need this even on the folder-scoped path (abort deliberately
        # stays clear so healthy folders keep processing) — otherwise
        # they still walk every detected photo in the missing folder
        # and re-issue reads against the offline share (Codex #1388
        # P2 r3664058173).
        self.source_offline_state["skipped_photo_ids"] = (
            set(self.source_skipped_photo_ids)
        )
        if self.offline_reason:
            # Must carry the "[classify] Fatal:" prefix: the end-of-run
            # rollup picks the job's headline error by that marker and
            # would otherwise fall back to errors[0] — likely an
            # unrelated per-photo warning logged much earlier.
            run.errors.append(
                f"[classify] Fatal: source "
                f"{self.offline_reason}. Reconnect it and run "
                f"Process again to classify the rest — photos already "
                f"classified will be skipped."
            )
            # extract_masks_stage / eye_keypoints_stage only gate on
            # abort.is_set(); with the source dead there is nothing
            # for them to read either, so without this they walk every
            # detected photo and reproduce the exact "N failed"
            # pattern this PR fixes — just one stage later, before
            # the failed-stage rollup at the end of the pipeline gets
            # a chance to short-circuit them.
            run.abort.set()
        elif n_source_skipped_photos > 0:
            # Folder-scoped outage: some photos were unreachable but
            # the source as a whole is not necessarily gone (other
            # folders in the collection may still be healthy). Do NOT
            # abort — later stages can still make progress on the
            # reachable photos — but the classify pass did not open
            # every photo it was asked to, so surface a fatal-prefixed
            # error so the end-of-run rollup names the outage instead
            # of letting the run finish silent-green.
            run.errors.append(
                f"[classify] Fatal: {n_source_skipped_photos} of "
                f"{total} photos unreachable (source offline). "
                f"Reconnect the missing folder(s) and run Process "
                f"again to classify the rest."
            )
        # A run that gave up on a dead source, or that skipped photos
        # because their folder was unreachable, did NOT classify the
        # whole collection, so it must not land on the job tree as a
        # clean green stage — "completed" here would tell the user
        # their photos were processed when some were never opened.
        run.stages["classify"]["status"] = (
            "failed"
            if (
                self.total_failed > 0
                or self.offline_reason
                or n_source_skipped_photos > 0
            )
            else "completed"
        )
        if self.total_failed > 0:
            run.errors.append(
                f"[classify] {n_failed_photos} of {total} photos "
                "failed to classify"
            )
        run.result["stages"]["classify"] = {
            "total": total,
            "predictions_stored": self.total_predictions_stored,
            "detected": self.detect_state["total_detected"],
            "failed": self.total_failed,
            "source_offline": self.offline_reason,
            "source_skipped": n_source_skipped_photos,
            "already_classified": self.total_skipped_existing,
            "full_image_fallbacks": self.total_full_image_fallbacks,
            "weak_detection_rescues": len(self.contextual_weak_ids),
            "model_count": len(self.specs),
            "models_succeeded": self.models_succeeded,
            "models_skipped": len(self.skipped_model_names),
            "skipped_model_names": self.skipped_model_names,
        }

    def fail(self, e):
        run = self.run
        run.errors.append(f"[classify] Fatal: {e}")
        log.exception("Pipeline classify stage failed")
        run.abort.set()
        run.stages["classify"]["status"] = "failed"
        # Only surface the fatal error on rows that haven't already
        # reached a terminal state. Without this, a late-loop exception
        # would overwrite the 'completed' status of earlier models that
        # finished successfully, misreporting per-model outcomes.
        specs_for_step_ids = self.loaded_models.get("resolved_specs") or []
        for spec in specs_for_step_ids:
            sid = f"classify:{spec['id']}"
            if sid in self.completed_step_ids or sid in self.failed_step_ids:
                continue
            run.runner.update_step(
                run.job["id"], sid,
                status="failed", error=str(e),
            )
