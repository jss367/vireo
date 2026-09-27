"""Classification stages for the streaming photo pipeline."""

import contextlib
import logging
import os
import time

import numpy as np
from pipeline_stages.context import PipelineRun

log = logging.getLogger(__name__)


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
        if specs_for_step_ids:
            for spec in specs_for_step_ids:
                run.runner.update_step(
                    run.job["id"], f"classify:{spec['id']}",
                    status=row_status, summary=row_summary,
                    error=loader_err if loader_failed else None,
                )
        else:
            for mid in (effective_model_ids or ["__unresolved__"]):
                run.runner.update_step(
                    run.job["id"], f"classify:{mid}",
                    status=row_status, summary=row_summary,
                    error=loader_err if loader_failed else None,
                )
        # model_loader may have already loaded the first classifier
        # before this early-return path was hit (e.g. abort.is_set()).
        # Release its cache handle so a same-key reload can be a hit
        # and idle eviction can reclaim VRAM.
        _release_classifier_cache_handle(loaded_models)
        run.update_stages(run.runner, run.job["id"], run.stages)
        return

    run.stages["classify"]["status"] = "running"
    run.update_stages(run.runner, run.job["id"], run.stages)

    # Track which per-model rows have reached a terminal state so a
    # fatal error raised by one model doesn't overwrite the status of
    # already-completed models (P2 from the Codex review). Defined
    # outside the try so the except handler can read them.
    completed_step_ids: set = set()
    failed_step_ids: set = set()

    try:
        import config as cfg
        from classify_job import (
            _BATCH_SIZE,
            _cached_prediction_taxonomy,
            _flush_batch,
            _prepare_image,
            _publish_classifier_runs_for_raw_results,
            _record_batch_classifier_runs,
            _store_grouped_predictions,
        )

        thread_db = run.database_factory(run.db_path)
        thread_db.set_active_workspace(run.workspace_id)

        user_cfg = thread_db.get_effective_config(cfg.load())
        grouping_window = user_cfg.get("grouping_window_seconds", 5)
        similarity_threshold = user_cfg.get("similarity_threshold", 0.85)
        detector_confidence = user_cfg.get("detector_confidence", 0.2)
        pipeline_cfg = user_cfg.get("pipeline", {})
        raw_session = None
        if run.params.raw_subject_analysis:
            from raw_analysis import RECIPE, RawAnalysisSession

            raw_session = RawAnalysisSession(
                max_size=pipeline_cfg.get("proxy_longest_edge") or 1536,
                sam2_variant=pipeline_cfg.get("sam2_variant") or "sam2-small",
                preserve_detail=True,
            )

        def prepare_pipeline_image(photo, folders, detection):
            kwargs = {"raw_analysis": raw_session} if raw_session is not None else {}
            img, folder_path, image_path = _prepare_image(photo, folders, detection, **kwargs)
            if img is not None and raw_session is not None and detection is not None:
                report = img.info.get("_vireo_raw_analysis")
                if report is not None:
                    thread_db.save_subject_raw_analysis(detection["id"], report)
            return img, folder_path, image_path

        weak_rescue_enabled = pipeline_cfg.get(
            "weak_detection_rescue_enabled", True,
        )
        weak_detection_confidence = pipeline_cfg.get(
            "weak_detection_confidence", 0.12,
        )

        tax = loaded_models["tax"]
        # Fingerprint for the FIRST model is preloaded by model_loader_stage.
        # Each subsequent iteration reloads its own bundle (with its own fp)
        # inside the loop, so we read loaded_models["labels_fingerprint"]
        # per-spec below rather than capturing a single value here.
        resolved_specs_local = loaded_models.get("resolved_specs") or [
            loaded_models["active_model"]
        ]

        photos = detect_state["photos"]
        folders = detect_state["folders"]
        cached_detections = detect_state["detections"]
        total = len(photos)

        # A low-confidence box is not globally promoted. Only select
        # weak runs bracketed by ordinary detections in one tightly
        # timed sequence. This gives the classifier a chance to
        # validate threshold-cliff frames without making every weak
        # MegaDetector result eligible throughout the library.
        contextual_weak_ids = set()
        if (
            weak_rescue_enabled
            and weak_detection_confidence < detector_confidence
            and photos
        ):
            from weak_detections import contextual_weak_photo_ids

            raw_mdv6_detections = thread_db.get_detections_for_photos(
                [p["id"] for p in photos],
                min_conf=weak_detection_confidence,
                detector_model="megadetector-v6",
            )
            contextual_weak_ids = contextual_weak_photo_ids(
                photos,
                raw_mdv6_detections,
                detector_confidence=detector_confidence,
                weak_confidence=weak_detection_confidence,
                max_gap=pipeline_cfg.get("burst_time_gap", 3.0),
            )
            if contextual_weak_ids:
                log.info(
                    "Classification: rescuing %d contextual "
                    "weak-detection photo(s)",
                    len(contextual_weak_ids),
                )

        total_predictions_stored = 0
        total_full_image_fallbacks = 0
        total_failed = 0
        total_skipped_existing = 0
        # Track unique photo IDs that failed in any model so the rollup
        # message always produces a valid X-of-N ratio. total_failed sums
        # per-model failures and can exceed total in multi-model runs.
        failed_photo_ids: set = set()
        # Photos we never opened because their containing folder was
        # unreachable at read time. Kept apart from ``failed`` (those
        # ARE actual per-photo decode failures) so the rollup can name
        # unreachable photos honestly, and kept as unique photo IDs so
        # a folder outage that hits the same photos across every model
        # in a multi-model run doesn't multiply-count.
        source_skipped_photo_ids: set = set()

        skipped_model_names: list = []
        models_succeeded = 0
        # Track photo IDs actually processed by the first successful
        # model's classify loop so the stale-detection purge is scoped
        # to reclassified photos only. Using detect_state["processed_ids"]
        # (all detected photos) would incorrectly delete detections for
        # photos that weren't reached if the job was aborted mid-classify.
        first_model_photo_ids: set = set()
        fresh_full_image_ids_by_photo: dict = {}

        # ``reason`` latches only when classify is giving up for good,
        # so the remaining models in a multi-model run skip themselves
        # instead of re-discovering the same dead share. ``pauses``
        # bounds the reconnect ping-pong: a user who resumes without
        # actually fixing the mount gets a few tries, not an infinite
        # pause/retry loop.
        source_offline: dict = {"reason": None, "pauses": 0}

        def _publish_pause_reason(reason, step_id):
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

        def _clear_pause_reason(step_id):
            """Drop the pause banner once the user has resumed.

            Leaving a stale ``pause_reason`` on ``job['progress']``
            after a successful reconnect would keep the "reconnect
            and Resume" banner visible while classify happily runs.
            """
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

        def _handle_source_offline(reason, step_id=None):
            """Park on a dead source; report whether we may continue.

            Returns True when the run was paused and then resumed, so
            the caller should keep classifying (the user reconnected
            the share). Returns False when classify should stop: the
            runner can't park, the job was cancelled, or we've already
            paused for this too many times.
            """
            if source_offline["pauses"] >= _MAX_SOURCE_OFFLINE_PAUSES:
                source_offline["reason"] = reason
                return False
            source_offline["pauses"] += 1
            # Don't push a pause message into ``errors``: a successful
            # reconnect+resume leaves classify completing normally, but
            # templates/pipeline.html treats every ``[classify]`` error
            # as a failed stage and suppresses the success redirect
            # (Codex #1388 P1). ``pause_job`` below already flips the
            # job state to ``pausing``/``paused`` (that's what the UI
            # renders while parked), and the give-up path below
            # appends its own ``[classify] Fatal:`` entry — so a run
            # that never comes back still surfaces a real terminal
            # error, and a run that does come back does not.
            log.warning("Classify paused: source offline (%s)", reason)
            # Publish BEFORE calling pause_job so the reason is already
            # in job['progress'] by the time the UI reacts to the
            # ``pausing`` status flip. Otherwise the banner would only
            # appear after the next progress event, which for a fully
            # blocked read might be minutes away.
            _publish_pause_reason(reason, step_id)

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
                    source_offline["reason"] = reason
                    return False

            # Blocks here until the user resumes or cancels. Classify
            # is a registered pause participant, so this is the same
            # safe boundary the per-photo cancel check already uses.
            if run.control.pause_checkpoint():
                source_offline["reason"] = reason
                return False
            # Successful resume: drop the stale "reconnect and Resume"
            # banner before the caller retries the read.
            _clear_pause_reason(step_id)
            return True

        from datetime import datetime as dt

        for spec_idx, active_spec in enumerate(resolved_specs_local):
            step_id = f"classify:{active_spec['id']}"
            if run.control.should_abort(run.abort):
                # Anything not yet touched stays pending; mark it skipped
                # so the job tree finalizes cleanly.
                run.runner.update_step(run.job["id"], step_id,
                                   status="completed",
                                   summary="Skipped (cancelled)")
                continue
            if source_offline["reason"]:
                # The share died during an earlier model. Every read for
                # this one would fail the same way, so say so instead of
                # running up a second identical failure count (the
                # incident's "0 predictions, 865 failed" second model).
                run.runner.update_step(
                    run.job["id"], step_id, status="completed",
                    summary=(
                        f"Skipped (source {source_offline['reason']})"
                    ),
                )
                continue

            run.runner.update_step(run.job["id"], step_id, status="running")

            if spec_idx == 0 and "clf" in loaded_models:
                # First model preloaded by model_loader_stage.
                clf = loaded_models["clf"]
                model_type = loaded_models["model_type"]
                model_name = loaded_models["model_name"]
            else:
                # Don't reset stages["classify"]["count"] here — it now
                # accumulates real inferences per-photo (Task 3); jumping
                # it to spec_idx * total would silently double-count the
                # cached hits from prior specs.  total is left at its
                # multi-spec value (set by the batch-end push of the
                # previous spec, or unchanged on first entry); explicitly
                # restate it so the "Loading next model..." event always
                # carries the multi-spec total.
                run.stages["classify"]["total"] = total * len(resolved_specs_local)
                run.emit_progress(
                    run.runner, run.job["id"], run.stages, "classify", f"Loading {active_spec['name']}...",
                    step_id=step_id,
                )
                # Drop the prior model's per-photo payload BEFORE loading
                # the next bundle so we don't hold old results + new model
                # weights concurrently. Without this, multi-model runs on
                # large collections can hit transient OOMs.
                with contextlib.suppress(NameError, UnboundLocalError):
                    raw_results.clear()  # noqa: F821 — bound in prior iter
                # Release the previous spec's cache handle so its
                # refcount drops and a same-cache-key reload below (or
                # in another pipeline) can be a hit. Must happen
                # BEFORE popping ``clf`` so we don't lose the only
                # reference to the bundle that owns the handle.
                _release_classifier_cache_handle(loaded_models)
                for k in ("clf", "model_type", "model_name", "model_str",
                          "labels", "label_source", "use_tol",
                          "active_model"):
                    loaded_models.pop(k, None)
                clf = None
                try:
                    bundle = _load_model_bundle(active_spec, tax, thread_db, progress_step=step_id)
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
                        completed_step_ids.add(step_id)
                        continue
                    log.warning(
                        "Skipping model %s: %s",
                        active_spec["name"], model_err,
                    )
                    skipped_model_names.append(active_spec["name"])
                    run.runner.update_step(
                        run.job["id"], step_id,
                        status="failed",
                        error=str(model_err),
                        summary=f"Failed to load: {model_err}",
                    )
                    failed_step_ids.add(step_id)
                    continue
                loaded_models.update(bundle)
                clf = bundle["clf"]
                model_type = bundle["model_type"]
                model_name = bundle["model_name"]

            # Name the label space on the row. Set for both branches
            # here: the first model's bundle was built by
            # model_loader_stage and merged into loaded_models there.
            if loaded_models.get("label_source"):
                run.runner.update_step(
                    run.job["id"], step_id,
                    label_source=loaded_models["label_source"],
                )

            # The fingerprint for THIS model's label set — pinned by
            # model_loader_stage for the first model and by _load_model_bundle
            # for subsequent ones. Used to key the classifier_runs gate so
            # a repeat pass over the same (detection, model, fingerprint)
            # skips work instead of re-running inference. Hoisted above
            # the reclassify clear so the clear can scope by fingerprint.
            spec_fp = loaded_models.get("labels_fingerprint", "legacy")

            # The reclassify clear (wipes prior predictions for this
            # model+fingerprint) is intentionally deferred to just before
            # _store_grouped_predictions below — clearing here and then
            # cancelling mid-classify would leave the predictions table
            # empty for this model with no replacement, erasing the
            # user's prior classifications instead of preserving them.

            # No photo-level short-circuit: it would hide detections
            # that newly cross the workspace's detector_confidence
            # threshold on photos that already had a cached prediction
            # for some other detection. The per-detection
            # classifier_runs gate below handles skipping correctly
            # and still surfaces cached results into raw_results so
            # grouping sees them.
            # Pre-flight cache estimate. One indexed query so the UI
            # can display "~M cached, ~K to classify" before the first
            # inference runs and ETAs are honest from the start. The
            # estimate may overcount if a run key exists but no cached
            # predictions do (e.g. a prior pass wrote
            # ``category == 'match'`` with no predictions); the live
            # ``cached`` counter reflects actual skips, and
            # ``photos_cache_overcounted_in_spec`` (populated in the
            # fall-through branch below) lets ``_classification_eta_progress``
            # reconcile the estimate from observed misses so
            # ``remaining_uncached`` doesn't collapse to zero on
            # collections dominated by these rows.
            #
            # We keep the full preflight-cached photo id set (not
            # just its count) so overcount attribution can be
            # scoped: recording an overcount for a photo the
            # preflight never counted would deflate the projected
            # cache-hit rate for unrelated photos and prematurely
            # inflate ``remaining_uncached`` (Codex #1468 P2).
            # Contextual-weak photos use the lower
            # ``weak_detection_confidence`` floor to match the
            # runtime cache-hit predicate; otherwise the cached
            # weak tail is omitted from ``cached_est`` and the
            # ETA overstates remaining time (Codex #1468 P2).
            #
            # Skipped on reclassify runs because the cache gate below
            # is bypassed and every photo is re-inferred. In a
            # multi-model run each model owns its own Jobs step, so
            # accumulate the per-spec estimates for the stage while
            # retaining this spec's value for its ETA.
            cached_est = 0
            preflight_cached_ids: set = set()
            # Photos whose runtime path deterministically skips
            # inference because they have no eligible animal target
            # but do have a confident non-animal box. Without
            # subtracting them, a person/vehicle tail inflates the ETA
            # even though runtime traverses it without model work.
            #
            # Whenever detection ran, pass its in-memory map so both
            # this skip estimate and the cache estimate below use the
            # exact candidates the classify loop will consume. Rows
            # from another detector model can remain in the DB after
            # either a reclassify or an ordinary runtime-fingerprint
            # miss; letting those stale rows into preflight can invent
            # work or hide a fresh cache hit (Codex #1468 P2).
            preflight_unclassifiable_ids: set = (
                thread_db.get_unclassifiable_photos(
                    [p["id"] for p in photos],
                    contextual_weak_photo_ids=contextual_weak_ids,
                    weak_confidence=(
                        weak_detection_confidence
                        if contextual_weak_ids
                        else None
                    ),
                    fresh_detections_by_photo=(
                        detect_state["detections"]
                        if detect_state.get("ran")
                        else None
                    ),
                    # ``processed_ids`` is what ``_detect_batch``
                    # actually completed this run. Photos absent
                    # from it are ones the detector raised on and
                    # runtime will DB-fallback for (see
                    # ``photo_dets`` else branch below), so they
                    # must be evaluated against DB rows here too
                    # instead of treated as "no fresh animal"
                    # (Codex #1468 P2).
                    fresh_processed_photo_ids=(
                        detect_state["processed_ids"]
                        if detect_state.get("ran")
                        else None
                    ),
                )
            )
            if not run.params.reclassify and not run.params.raw_subject_analysis:
                # Precompute the classifier runtime_fingerprint the
                # runtime gate will accept for each distinct
                # ``detections.runtime_fingerprint`` present in this
                # batch. Passing this map lets the preflight reject
                # obsolete-runtime classifier_runs rows the same way
                # the per-detection ``get_classifier_run_key_gate``
                # does at runtime; without it, the preflight
                # overcounts rows whose runtime rolled since the
                # prior classify pass, and no observation can correct
                # the estimate until those rows are visited — so the
                # UI reads "finishing…" while inference is still
                # pending (Codex #1468 P2).
                preflight_expected_rt_map = None
                portable_labels_full = loaded_models.get(
                    "labels_fingerprint_full"
                )
                portable_model_identity = loaded_models.get(
                    "classifier_model_identity"
                )
                portable_tax_identity = loaded_models.get(
                    "taxonomy_identity", "no-tax",
                )
                if portable_labels_full and portable_model_identity:
                    from computation_cache import (
                        classifier_runtime_fingerprint,
                    )
                    detector_runtimes: set = set()
                    photo_id_list = [p["id"] for p in photos]
                    det_chunk = 500
                    for i in range(0, len(photo_id_list), det_chunk):
                        chunk = photo_id_list[i:i + det_chunk]
                        placeholders = ",".join("?" * len(chunk))
                        rows = thread_db.conn.execute(
                            f"SELECT DISTINCT runtime_fingerprint "
                            f"FROM detections "
                            f"WHERE photo_id IN ({placeholders})",
                            chunk,
                        ).fetchall()
                        for row in rows:
                            detector_runtimes.add(
                                row["runtime_fingerprint"]
                            )
                    preflight_expected_rt_map = {}
                    for det_rt in detector_runtimes:
                        if det_rt is None:
                            # Detector runtime not recorded — matches
                            # ``classifier_runtime_for_detection``'s
                            # None return; permissive entry preserves
                            # the unfiltered fallback.
                            preflight_expected_rt_map[det_rt] = None
                        else:
                            preflight_expected_rt_map[det_rt] = (
                                classifier_runtime_fingerprint(
                                    portable_model_identity,
                                    portable_labels_full,
                                    det_rt,
                                    taxonomy_identity=(
                                        portable_tax_identity
                                    ),
                                )
                            )
                preflight_cached_ids = (
                    thread_db.get_classifier_run_cache_hits(
                        [p["id"] for p in photos],
                        model_name,
                        spec_fp,
                        contextual_weak_photo_ids=(
                            contextual_weak_ids
                        ),
                        weak_confidence=(
                            weak_detection_confidence
                            if contextual_weak_ids
                            else None
                        ),
                        fresh_detections_by_photo=(
                            detect_state["detections"]
                            if detect_state.get("ran")
                            else None
                        ),
                        fresh_processed_photo_ids=(
                            detect_state["processed_ids"]
                            if detect_state.get("ran")
                            else None
                        ),
                        expected_classifier_runtime_by_detector_runtime=(
                            preflight_expected_rt_map
                        ),
                    )
                )
                cached_est = len(preflight_cached_ids)
                run.stages["classify"]["cached_estimate"] = (
                    run.stages["classify"].get("cached_estimate", 0) + cached_est
                )
            # Set total BEFORE the pre-flight event so the UI's
            # ``stageTotal - stageCachedEst`` subtraction renders the
            # real "to classify" count on the first event the user
            # sees, not 0.
            run.stages["classify"]["total"] = total * len(resolved_specs_local)
            run.emit_progress(
                run.runner, run.job["id"], run.stages, "classify",
                f"Classifying with {active_spec['name']}",
                step_id=step_id,
            )
            raw_results: list = []
            failed = 0
            # Photos we never got to look at because the source volume
            # was offline. Kept apart from ``failed`` so the summary
            # never reports an unreachable share as broken photos.
            source_skipped = 0
            skipped_existing = 0
            full_image_fallbacks = 0
            run.stages["classify"].setdefault("cached", 0)
            run.stages["classify"].setdefault("seen", 0)
            # Photo-scoped bookkeeping for ``count`` (inferred) and
            # ``cached``. ``total`` and ``cached_estimate`` count PHOTOS
            # (× specs), so multi-subject photos with several qualifying
            # detections must not each add multiple ticks to ``count`` /
            # ``cached`` — otherwise the UI's ``inferred · cached /
            # total`` line can read ``2 inferred / 1`` on a two-subject
            # photo, and the cached-preflight would understate remaining
            # work when only one of the photo's detections is cached.
            # A photo lands in ``photos_cached_in_spec`` on its first
            # cache-hit detection and is promoted to
            # ``photos_inferred_in_spec`` (decrementing ``cached``,
            # incrementing ``count``) the moment any of its detections
            # actually runs inference. Reset per spec so a photo can be
            # counted once per (photo × spec) — matching ``total``.
            photos_cached_in_spec: set = set()
            photos_inferred_in_spec: set = set()
            # Includes failed inference attempts as well as successful
            # ones. ETA throughput is about completed model work, not
            # only predictions that happened to persist successfully.
            photos_attempted_in_spec: set = set()
            # Preflight's ``cached_est`` counts any photo whose
            # qualifying detections all have classifier_runs rows,
            # but the runtime cache-hit predicate additionally
            # requires actual predictions to exist. When we hit the
            # fall-through case (run key present, predictions
            # missing — see the classify loop below) the photo will
            # end up in ``photos_inferred_in_spec``, so subtract it
            # from the preflight estimate in the ETA calculation
            # instead of leaving a phantom future cache hit that
            # collapses ``remaining_uncached`` to zero.
            photos_cache_overcounted_in_spec: set = set()
            # Photos observed to hit the confident-non-animal skip
            # branch this spec. Paired with
            # ``preflight_unclassifiable_ids`` so the ETA can
            # subtract the unvisited share from remaining work
            # instead of treating a fast tail as inference work
            # (Codex #1468 P2).
            photos_unclassifiable_in_spec: set = set()
            # Per-spec tracking of photos whose per-photo iteration
            # was actually entered in THIS spec. Used by the
            # reclassify clear below so it never wipes predictions
            # for photos this spec never opened — on a mount-scoped
            # give-up we break out of the batch loop before reaching
            # later photos, and on a folder-scoped outage every photo
            # under the missing folder is skipped without ever
            # producing a replacement prediction. Without this,
            # ``clear_predictions`` on a ``reclassify=True`` run
            # would delete the prior predictions for those photos
            # even though nothing in this run had a chance to
            # rewrite them (Codex #1388 P1).
            spec_reached_photo_ids: set = set()
            # Per-spec source skips, scoped to just THIS spec's
            # reads. Codex #1388 P2 (r3663642360): if the folder
            # was unreachable for model A but returned before
            # model B, the aggregate ``source_skipped_photo_ids``
            # would still exclude that photo from model B's
            # reclassify clear — leaving model B's stale
            # prediction in place. Because ``add_prediction``
            # uses ``INSERT OR IGNORE``, that stale row can win
            # over the fresh result and retain the wrong
            # species/confidence. Track skips per spec for the
            # clear; the aggregate set below still drives the
            # outage rollup at end-of-run.
            spec_source_skipped_photo_ids: set = set()
            # Photos that iterated past the inner abort check IN THIS spec.
            # Used for the per-spec ``runner.update_step`` progress (which
            # is bounded by ``total``, not the multi-spec stage total) and
            # for the batch-end rate calc.  Captures every branch — cache
            # hit, successful inference, no-detection, image decode fail,
            # inference fail — so spec-level progress reaches ``total`` at
            # the end of the spec regardless of outcome mix.
            processed_in_spec = 0
            start_time = time.time()
            # Wall time since ``start_time`` includes cache-lookup and
            # result-building for the cache-hit prefix, which can dwarf
            # actual model work on cache-heavy collections. Dividing
            # ``inference_attempts`` by that walltime yields an
            # artificially low rate and inflates the ETA for the
            # uncached tail. Accumulate the seconds spent inside
            # ``_flush_batch`` here so the ETA rate reflects
            # inference throughput only (Codex #1468 P2).
            inference_seconds = 0.0
            batch_size = 32  # classification batch granularity
            inference_batch_size = _BATCH_SIZE
            inference_batch: list = []
            has_flushed_in_spec = False

            def _close_pending_inference(inference_batch=inference_batch):
                for entry in inference_batch:
                    # Releasing decoded images; a close failure frees nothing more.
                    with contextlib.suppress(Exception):
                        entry["img"].close()
                inference_batch.clear()

            def _flush_pending_inference(
                inference_batch=inference_batch,
                raw_results=raw_results,
                clf=clf,
                model_type=model_type,
                model_name=model_name,
                spec_fp=spec_fp,
                photos_cached_in_spec=photos_cached_in_spec,
                photos_inferred_in_spec=photos_inferred_in_spec,
                photos_attempted_in_spec=photos_attempted_in_spec,
                photos_cache_overcounted_in_spec=(
                    photos_cache_overcounted_in_spec
                ),
            ):
                nonlocal failed, has_flushed_in_spec, inference_seconds
                if not inference_batch:
                    return

                pending = list(inference_batch)
                inference_batch.clear()
                has_flushed_in_spec = True
                attempted_photo_ids = {
                    entry["photo"]["id"] for entry in pending
                }
                photos_attempted_in_spec.update(attempted_photo_ids)
                removed_cache_hits = _remove_attempted_cache_hits(
                    attempted_photo_ids,
                    photos_cached_in_spec,
                )
                if removed_cache_hits:
                    run.stages["classify"]["cached"] = max(
                        0,
                        run.stages["classify"].get("cached", 0)
                        - removed_cache_hits,
                    )
                pre_len = len(raw_results)
                # GPU serialisation lives inside _flush_batch around the
                # inference call so the DB upserts/result-building afterward
                # don't hold the semaphore while the GPU is idle.
                _flush_started = time.time()
                n_batch_failed = _flush_batch(
                    pending, clf, model_type, model_name,
                    thread_db, raw_results,
                )
                inference_seconds += max(
                    time.time() - _flush_started, 0.0,
                )
                failed += n_batch_failed

                successful_det_ids = {
                    r.get("detection_id") for r in raw_results[pre_len:]
                }
                if n_batch_failed:
                    for entry in pending:
                        if entry.get("detection_id") not in successful_det_ids:
                            failed_photo_ids.add(entry["photo"]["id"])

                _record_batch_classifier_runs(
                    thread_db, pending, model_name, spec_fp, raw_results,
                    pre_len,
                    labels_fingerprint_full=loaded_models.get(
                        "labels_fingerprint_full"
                    ),
                    model_identity=loaded_models.get(
                        "classifier_model_identity"
                    ),
                )

                # Photo-scoped ``count`` bookkeeping: each distinct
                # photo whose flush yielded at least one successful
                # classification counts once. If it was previously
                # bucketed as fully-cached (an earlier detection hit
                # the cache), migrate it — decrement ``cached`` and
                # add it to ``count`` — so ``count + cached`` stays
                # bounded by the (photo-scoped) ``total``.
                new_photo_ids = {
                    r["photo"]["id"] for r in raw_results[pre_len:]
                }
                promoted = new_photo_ids & photos_cached_in_spec
                if promoted:
                    photos_cached_in_spec.difference_update(promoted)
                    run.stages["classify"]["cached"] = max(
                        0,
                        run.stages["classify"].get("cached", 0)
                        - len(promoted),
                    )
                newly_inferred = new_photo_ids - photos_inferred_in_spec
                if newly_inferred:
                    photos_inferred_in_spec.update(newly_inferred)
                    run.stages["classify"]["count"] = (
                        run.stages["classify"].get("count", 0)
                        + len(newly_inferred)
                    )

            for batch_start in range(0, total, batch_size):
                if run.control.should_abort(run.abort) or source_offline["reason"]:
                    break
                batch = photos[batch_start:batch_start + batch_size]

                for photo in batch:
                    # Per-photo abort check so cancel takes effect within
                    # one inference (~seconds) instead of waiting for the
                    # next batch boundary (~32 photos). The outer batch
                    # loop's check at the top of the next iteration will
                    # then break out of the batch loop entirely.
                    # ``source_offline`` rides the same boundary: once
                    # the share is gone there is nothing left to read,
                    # so stop pulling photos rather than spending the
                    # rest of the batch collecting instant EIOs.
                    if run.control.should_abort(run.abort) or source_offline["reason"]:
                        break
                    processed_in_spec += 1
                    run.stages["classify"]["seen"] = (
                        run.stages["classify"].get("seen", 0) + 1
                    )
                    # Photo entered THIS spec's per-photo body; scopes
                    # the reclassify clear below so unreached photos
                    # keep their prior predictions.
                    spec_reached_photo_ids.add(photo["id"])
                    # Record this photo as classify-processed for the first
                    # successful model. Used by the stale-detection purge to
                    # restrict deletions to photos actually reclassified.
                    if models_succeeded == 0:
                        first_model_photo_ids.add(photo["id"])

                    # Pull every qualifying detection for this photo from the
                    # detect-stage cache. Fall back to db.get_detections()
                    # only for photos whose per-photo detect iteration
                    # never completed (e.g. mid-batch exception, or the
                    # detect stage was skipped for an already-detected
                    # non-reclassify run — in which case the DB holds the
                    # authoritative rows). If MegaDetector produced no
                    # real rows at all, synthesize a full-image anchor so
                    # classifiers still get one attempt and future reruns
                    # can hit classifier_runs for that attempt.
                    full_image_fallback = False
                    is_contextual_weak = (
                        photo["id"] in contextual_weak_ids
                    )
                    detection_floor = (
                        weak_detection_confidence
                        if is_contextual_weak
                        else detector_confidence
                    )
                    if photo["id"] in cached_detections:
                        # cached_detections from _detect_batch can include
                        # full-image rows when an earlier pass synthesized
                        # them (legacy db state); filter to match the
                        # fallback-query branch below so classifiers only
                        # see real, qualifying animal boxes. _detect_batch's
                        # fresh cache contains raw low-confidence boxes too,
                        # while DB reads normally apply this threshold.
                        photo_dets = _cached_classify_detections(
                            cached_detections[photo["id"]],
                            detection_floor,
                            contextual_weak=is_contextual_weak,
                        )
                    else:
                        photo_dets = [
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
                                photo["id"], min_conf=detection_floor,
                                # Contextual rescue is defined only
                                # from MegaDetector V6 evidence. Keep
                                # the detector-failure DB fallback on
                                # that same candidate set so a stale,
                                # higher-confidence foreign row cannot
                                # diverge from the cache preflight's
                                # selected crop (Codex #1468 P2).
                                detector_model=(
                                    "megadetector-v6"
                                    if is_contextual_weak
                                    else None
                                ),
                            )
                            if d["detector_model"] != "full-image"
                            and d["category"] == "animal"
                        ]
                    if is_contextual_weak and not photo_dets:
                        # detect_state intentionally caches only rows
                        # passing the ordinary workspace threshold on
                        # reuse runs. Recover the raw weak row from the
                        # database for this explicitly selected bridge.
                        photo_dets = [
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
                                photo["id"],
                                min_conf=weak_detection_confidence,
                                detector_model="megadetector-v6",
                            )
                            if d["category"] == "animal"
                        ]
                    if is_contextual_weak and photo_dets:
                        # One best weak crop is enough to validate the
                        # bridge. Classifying every low-confidence box
                        # would multiply work and false-positive risk.
                        #
                        # Explicit ``detector_confidence DESC, id ASC``
                        # tie-break so the runtime picks the same
                        # detection the preflight's cache-hit query
                        # picks. ``cached_detections`` comes back from
                        # ``_detect_batch`` in raw detector/NMS output
                        # order — no ID tie-break — while
                        # ``get_classifier_run_cache_hits`` ranks by
                        # ``detector_confidence DESC, id ASC``. Without
                        # this sort, two equal-confidence weak boxes
                        # can leave runtime inferring the higher-ID box
                        # while the preflight marked the photo cached
                        # via a portable-cache run on the lower-ID box;
                        # the overcount tracker then can't correct it
                        # because the runtime-selected detection has
                        # no run key of its own (Codex #1468 P2).
                        photo_dets = sorted(
                            photo_dets,
                            key=lambda d: (
                                -float(
                                    d.get(
                                        "confidence",
                                        d.get("detector_confidence", 0),
                                    ) or 0
                                ),
                                d.get("id", 0),
                            ),
                        )[:1]
                    detections_to_classify = photo_dets
                    if not detections_to_classify:
                        # No animal box is usable at either the
                        # ordinary threshold or the contextual
                        # weak-rescue floor. Treat this the same as a
                        # true no-detection photo and give the
                        # classifier a full-image attempt. Raw
                        # detector output is retained for
                        # diagnostics/cache reuse, but a 1% noise box
                        # must not suppress classification of an
                        # otherwise visible subject.
                        #
                        # Exception: MegaDetector emitted a confident
                        # non-animal (person/vehicle) box at or above
                        # ``detector_confidence``. Sending the entire
                        # human/vehicle frame to the wildlife
                        # classifier would persist a spurious species
                        # prediction. Skip the fallback in that case
                        # and leave the photo without a classifier
                        # run, matching the old raw-detection guard's
                        # intent while still rescuing sub-threshold
                        # animal photos.
                        confident_non_animal = False
                        if photo["id"] in cached_detections:
                            confident_non_animal = any(
                                d.get("detector_model") != "full-image"
                                and d.get("category", "animal") != "animal"
                                and d.get(
                                    "confidence",
                                    d.get("detector_confidence", 0),
                                ) >= detector_confidence
                                for d in cached_detections[photo["id"]]
                            )
                        else:
                            confident_non_animal = any(
                                d["detector_model"] != "full-image"
                                and d["category"] != "animal"
                                for d in thread_db.get_detections(
                                    photo["id"],
                                    min_conf=detector_confidence,
                                )
                            )
                        if confident_non_animal:
                            # This is the only no-target path that
                            # skips inference. Track it so the ETA
                            # does not project confident person/
                            # vehicle tails as pending model work.
                            photos_unclassifiable_in_spec.add(
                                photo["id"],
                            )
                            continue

                        existing_full = thread_db.get_detections(
                            photo["id"],
                            detector_model="full-image",
                            min_conf=0,
                        )
                        if existing_full and not run.params.reclassify:
                            full_det_id = existing_full[0]["id"]
                        else:
                            from computation_cache import (
                                full_image_runtime_fingerprint,
                                source_input,
                            )

                            full_runtime = full_image_runtime_fingerprint()
                            identity = thread_db.conn.execute(
                                """SELECT file_hash, companion_path
                                   FROM photos WHERE id = ?""",
                                (photo["id"],),
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
                            # Combine save_detections + record_detector_run
                            # into one transaction via write_detection_batch
                            # so a crash between them can't leave a
                            # full-image detection row without its matching
                            # detector_runs row — mirroring the invariant
                            # documented for write_detection_batch.
                            full_det_ids = thread_db.write_detection_batch(
                                photo["id"], "full-image",
                                [{
                                    "box": {"x": 0, "y": 0, "w": 1, "h": 1},
                                    "confidence": 0,
                                    "category": "animal",
                                }],
                                runtime_fingerprint=full_runtime,
                                input_fingerprint=full_input,
                            )
                            full_det_id = full_det_ids[0]
                        detections_to_classify = [{
                            "id": full_det_id,
                            "box_x": 0,
                            "box_y": 0,
                            "box_w": 1,
                            "box_h": 1,
                            "confidence": 0,
                            "category": "animal",
                            "detector_model": "full-image",
                        }]
                        full_image_fallback = True
                        full_image_fallbacks += 1
                        fresh_full_image_ids_by_photo.setdefault(
                            photo["id"], set(),
                        ).add(full_det_id)

                    for detection in detections_to_classify:
                        # Classifier-run gate: skip work when this exact
                        # (detection, classifier_model, labels_fingerprint)
                        # triple was already classified. Reclassify bypasses
                        # the gate so users can force a fresh pass. When
                        # gated, surface the cached top-1 prediction into
                        # raw_results so downstream grouping/storage sees
                        # it — otherwise the cached detection would silently
                        # drop out of the grouping pipeline.
                        if not run.params.reclassify and not run.params.raw_subject_analysis:
                            expected_classifier_runtime = None
                            portable_labels_full = loaded_models.get(
                                "labels_fingerprint_full"
                            )
                            portable_model_identity = loaded_models.get(
                                "classifier_model_identity"
                            )
                            portable_tax_identity = loaded_models.get(
                                "taxonomy_identity", "no-tax",
                            )
                            if portable_labels_full and portable_model_identity:
                                from computation_cache import (
                                    classifier_runtime_for_detection,
                                )

                                expected_classifier_runtime = (
                                    classifier_runtime_for_detection(
                                        thread_db,
                                        detection["id"],
                                        portable_model_identity,
                                        portable_labels_full,
                                        taxonomy_identity=(
                                            portable_tax_identity
                                        ),
                                    )
                                )
                            # Fetch both accepted and gate-rejected
                            # keys in one query. ``rejected_keys`` is
                            # the "existed but the runtime_fingerprint
                            # rule rejected it" set — the preflight
                            # ``count_classifier_runs`` ignores that
                            # rule, so without recording these as
                            # overcounts below, a photo whose only key
                            # is gate-rejected sits in ``cached_est``
                            # forever and ETA prematurely reads
                            # "finishing…" on runs where the detector
                            # fingerprint has rolled since the prior
                            # classifier pass (Codex #1468 P2).
                            if expected_classifier_runtime is not None:
                                run_keys, rejected_keys = (
                                    thread_db
                                    .get_classifier_run_key_gate(
                                        detection["id"],
                                        expected_classifier_runtime,
                                    )
                                )
                            else:
                                # No expected runtime fingerprint means
                                # portable identity isn't wired up
                                # (legacy path); fall back to the
                                # unfiltered gate. Nothing to reconcile
                                # because the preflight and the gate
                                # then agree on which rows count.
                                run_keys = (
                                    thread_db.get_classifier_run_keys(
                                        detection["id"],
                                    )
                                )
                                rejected_keys = set()
                            if (model_name, spec_fp) in run_keys:
                                cached = thread_db.get_predictions_for_detection(
                                    detection["id"],
                                    classifier_model=model_name,
                                    labels_fingerprint=spec_fp,
                                    min_classifier_conf=0,
                                )
                                if cached:
                                    skipped_existing += 1
                                    # Photo-scoped ``cached`` bucket:
                                    # only the FIRST cached detection
                                    # per photo (per spec) ticks the
                                    # counter. A subsequent inferred
                                    # detection on the same photo will
                                    # promote it into ``count`` in the
                                    # flush path above.
                                    if _record_unattempted_cache_hit(
                                        photo["id"],
                                        photos_inferred_in_spec,
                                        photos_attempted_in_spec,
                                        photos_cached_in_spec,
                                    ):
                                        run.stages["classify"]["cached"] += 1
                                    top = cached[0]
                                    folder_path = folders.get(photo["folder_id"], "")
                                    image_path = os.path.join(
                                        folder_path, photo["filename"],
                                    )
                                    timestamp = None
                                    if photo["timestamp"]:
                                        with contextlib.suppress(ValueError, TypeError):
                                            timestamp = dt.fromisoformat(
                                                photo["timestamp"]
                                            )
                                    embedding = None
                                    if model_type != "timm":
                                        # Prefer the per-detection
                                        # embedding so multi-subject
                                        # cache reruns don't reuse a
                                        # single last-wins photo-level
                                        # vector for every detection.
                                        # Fall back to the photo-level
                                        # entry only when the photo has
                                        # a single qualifying detection
                                        # — there the photo-level row
                                        # unambiguously belongs to it,
                                        # so legacy data (classified
                                        # before per-detection variants
                                        # were written) still refines
                                        # correctly.
                                        emb_blob = thread_db.get_photo_embedding(
                                            photo["id"], model_name,
                                            variant=f"det:{detection['id']}",
                                        )
                                        if not emb_blob and len(
                                            detections_to_classify
                                        ) == 1:
                                            emb_blob = thread_db.get_photo_embedding(
                                                photo["id"], model_name,
                                            )
                                        if emb_blob:
                                            embedding = np.frombuffer(
                                                emb_blob, dtype=np.float32,
                                            )
                                    raw_results.append({
                                        "photo": photo,
                                        "detection_id": detection["id"],
                                        "folder_path": folder_path,
                                        "image_path": image_path,
                                        "prediction": top["species"],
                                        "confidence": top["confidence"],
                                        "timestamp": timestamp,
                                        "filename": photo["filename"],
                                        "embedding": embedding,
                                        "taxonomy": _cached_prediction_taxonomy(top),
                                        "_existing": True,
                                    })
                                    continue
                                # No cached prediction rows: honor a
                                # measured ``classifier_match_scores``
                                # summary for the same triple as a
                                # completed zero-candidate run,
                                # mirroring the boxed and full-image
                                # gates in ``classify_job.py``.
                                # Without this, a partially cached
                                # combined-pipeline run re-classifies
                                # detections whose no-match verdict
                                # was already recorded and can
                                # replace an authoritative "nothing
                                # in this list fits" outcome with a
                                # fresh inference (Codex P2 on
                                # a1be510). The preflight already
                                # counted this photo as cached
                                # because the classifier-run key
                                # exists, so no overcount to
                                # reconcile.
                                if thread_db.has_classifier_match_score(
                                    detection["id"], model_name, spec_fp,
                                ):
                                    skipped_existing += 1
                                    if _record_unattempted_cache_hit(
                                        photo["id"],
                                        photos_inferred_in_spec,
                                        photos_attempted_in_spec,
                                        photos_cached_in_spec,
                                    ):
                                        run.stages["classify"]["cached"] += 1
                                    continue
                                # Run key with no cached rows (e.g.
                                # prior pass stored `category == 'match'`
                                # so the prediction was intentionally not
                                # written). Fall through to re-classify
                                # instead of stranding the detection.
                                # Reconcile the preflight's cache
                                # estimate: it counted this photo based
                                # solely on the run key existing, but
                                # runtime will actually infer. Only
                                # record the overcount when the
                                # preflight actually counted this
                                # photo — otherwise a multi-detection
                                # photo whose other detection lacked
                                # any run key was never in
                                # ``cached_est`` to begin with, and
                                # treating this fall-through as a
                                # failed preflight prediction would
                                # deflate the projection for
                                # unrelated cache-heavy tails
                                # (Codex #1468 P2).
                                if (
                                    photo["id"] in preflight_cached_ids
                                ):
                                    photos_cache_overcounted_in_spec.add(
                                        photo["id"],
                                    )
                            elif (
                                model_name, spec_fp,
                            ) in rejected_keys:
                                # A classifier_runs row exists but its
                                # ``runtime_fingerprint`` no longer
                                # matches (typically the detector was
                                # re-run since the prior classify).
                                # Preflight counted this photo as
                                # cached; runtime will infer. Record
                                # the overcount so
                                # ``_classification_eta_progress`` can
                                # deflate its projected future cache
                                # hits — otherwise a collection made
                                # entirely of these rows sees
                                # ``remaining_uncached`` collapse to
                                # zero after the first uncached batch
                                # and the UI reports "finishing…"
                                # while most photos still need
                                # inference (Codex #1468 P2). Same
                                # preflight-membership guard: an
                                # unrelated detection without any
                                # run key on this photo means the
                                # preflight never counted it, so
                                # this branch isn't a preflight
                                # miss to reconcile.
                                if (
                                    photo["id"] in preflight_cached_ids
                                ):
                                    photos_cache_overcounted_in_spec.add(
                                        photo["id"],
                                    )

                        # Track image preparation time separately from
                        # the offline pause loop below so it can be
                        # folded into ``inference_seconds`` only when
                        # the photo actually enters the inference
                        # batch. Cache-hit and image-decode-fail paths
                        # exit the loop above without contributing to
                        # ``inference_attempts``, so their prep time
                        # (if any) must not count toward the rate;
                        # a photo whose retries eventually succeed
                        # DOES count, so accumulate across every
                        # ``_prepare_image`` call for this detection
                        # (Codex #1468 P2).
                        _det_prep_seconds = 0.0
                        _prep_started = time.time()
                        img, folder_path, image_path = prepare_pipeline_image(
                            photo, folders,
                            None if full_image_fallback else detection,
                        )
                        _det_prep_seconds += max(
                            time.time() - _prep_started, 0.0,
                        )
                        # Before blaming the photo, check whether the
                        # source itself vanished. A dropped share
                        # fails every remaining read instantly, so
                        # counting these as per-photo failures would
                        # report the untouched remainder of the
                        # collection as broken images.
                        #
                        # Loop rather than one-shot pause+retry so a
                        # mount that stays dead across a resume also
                        # gets latched when this happens to be the
                        # last photo (or last detection) needing an
                        # image read: silently continuing on the
                        # failed retry would leave source_offline
                        # ["reason"] unset, abort clear, and
                        # extract_masks / eye_keypoints would still
                        # walk every detected photo reissuing reads
                        # against the dead share (Codex #1388 P1
                        # r3663278142). _handle_source_offline
                        # bounds the total pause/retry cycles via
                        # _MAX_SOURCE_OFFLINE_PAUSES, so this
                        # can't loop forever.
                        gave_up_on_source = False
                        while img is None:
                            offline = _source_offline_reason(
                                folder_path, image_path,
                            )
                            if offline is None:
                                # The source is reachable; this one
                                # file is genuinely broken.
                                failed += 1
                                failed_photo_ids.add(photo["id"])
                                break
                            scope, reason = offline
                            if scope == "folder":
                                # A single missing folder is not
                                # evidence the whole source is
                                # offline; skip this photo as
                                # unreachable and let later photos
                                # in healthy folders keep processing.
                                # Guard the counter with the per-spec
                                # skipped-photo set so a multi-subject
                                # photo (N qualifying detections)
                                # counts once, not N times — the
                                # per-spec ``total`` and step summary
                                # are photo-scoped, and without this
                                # the row could report e.g. ``3
                                # unreachable`` out of ``1`` photo
                                # (Codex #1388 P2 r3664348763).
                                if (
                                    photo["id"]
                                    not in spec_source_skipped_photo_ids
                                ):
                                    source_skipped += 1
                                source_skipped_photo_ids.add(
                                    photo["id"]
                                )
                                spec_source_skipped_photo_ids.add(
                                    photo["id"]
                                )
                                break
                            # Mount-scoped: park and wait for the
                            # user to reconnect. On give-up,
                            # _handle_source_offline latches
                            # source_offline["reason"] and returns
                            # False — we then break the per-photo
                            # loop so remaining photos short-circuit
                            # the same way (and the finalization
                            # rollup sets abort so downstream stages
                            # skip the dead source).
                            if not _handle_source_offline(
                                reason, step_id=step_id,
                            ):
                                # Give-up: image never loaded, so
                                # this photo is unreached — same
                                # bucket as the folder-scoped skip
                                # above. Keeps the reclassify clear
                                # below from wiping its prior
                                # prediction (Codex #1388 P1
                                # r3663159360).
                                source_skipped_photo_ids.add(
                                    photo["id"]
                                )
                                spec_source_skipped_photo_ids.add(
                                    photo["id"]
                                )
                                gave_up_on_source = True
                                break
                            # Resumed. Retry the same read before
                            # advancing — silently skipping the
                            # paused-during photo would leave it
                            # unclassified even though the source
                            # came back, and on a reclassify run
                            # finalization would clear its old
                            # prediction with no replacement
                            # (Codex #1388 P2). If the retry also
                            # fails, the loop re-probes and either
                            # pauses again (bounded), degrades to
                            # folder-scope, or gives up.
                            _prep_started = time.time()
                            img, folder_path, image_path = (
                                prepare_pipeline_image(
                                    photo, folders,
                                    None if full_image_fallback
                                    else detection,
                                )
                            )
                            _det_prep_seconds += max(
                                time.time() - _prep_started, 0.0,
                            )
                            if img is not None:
                                # Successful recovery: refund the
                                # pause budget so an unrelated
                                # outage later in the run — a
                                # second share that drops, or the
                                # same share dropping again hours
                                # later — still gets its own
                                # bounded retry window. Without
                                # this, three separately-recovered
                                # outages exhaust the budget and
                                # the fourth outage immediately
                                # takes the give-up branch without
                                # ever offering Resume (Codex
                                # #1388 P2 r3663816327). The
                                # ``pauses`` bound is meant to
                                # protect against a user who
                                # resumes WITHOUT actually
                                # remounting the share — a
                                # successful read is proof the
                                # share IS back, so the counter
                                # can honestly reset.
                                source_offline["pauses"] = 0
                        if gave_up_on_source:
                            break
                        if img is None:
                            continue
                        # This detection is about to enter the flush
                        # batch — attribute its full preparation cost
                        # to the rate denominator so ``_prepare_image``
                        # time (open + decode + crop + resize) shows
                        # up alongside ``_flush_batch`` time. Without
                        # this, RAW/JPEG-heavy or slow-storage runs
                        # publish a rate that only reflects GPU work
                        # and understate the ETA (Codex #1468 P2).
                        inference_seconds += _det_prep_seconds
                        inference_batch.append({
                            "input_recipe": RECIPE if run.params.raw_subject_analysis else None,
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
                            not has_flushed_in_spec
                            or len(inference_batch) >= inference_batch_size
                        ):
                            _flush_pending_inference()

                # Batch boundary: surface the per-photo accumulated
                # count + cached to the UI. Replaces the old per-batch
                # pre-advance which lied about progress when batches
                # contained cache hits.
                if run.control.should_abort(run.abort):
                    _close_pending_inference()
                else:
                    _flush_pending_inference()
                run.stages["classify"]["total"] = total * len(resolved_specs_local)
                elapsed = max(time.time() - start_time, 0.01)
                run.emit_progress(
                    run.runner, run.job["id"], run.stages, "classify",
                    f"Classifying with {active_spec['name']}"
                    + (
                        f" ({spec_idx + 1}/{len(resolved_specs_local)})"
                        if len(resolved_specs_local) > 1 else ""
                    ),
                    step_id=step_id,
                    rate=round(processed_in_spec / elapsed * 60, 1),
                )
                run.runner.update_step(
                    run.job["id"], step_id,
                    progress={
                        "current": processed_in_spec,
                        "total": total,
                        **_classification_eta_progress(
                            total=total,
                            seen=processed_in_spec,
                            cached_estimate=cached_est,
                            cache_hits=len(photos_cached_in_spec),
                            inference_attempts=len(
                                photos_attempted_in_spec
                            ),
                            classified=len(photos_inferred_in_spec),
                            # Inference-active seconds only. Wall time
                            # since ``start_time`` includes the cache-
                            # traversal prefix and would deflate the
                            # per-attempt rate on cache-heavy runs
                            # (Codex #1468 P2).
                            elapsed=inference_seconds,
                            cache_overcount=len(
                                photos_cache_overcounted_in_spec
                            ),
                            unclassifiable_estimate=len(
                                preflight_unclassifiable_ids
                            ),
                            unclassifiable_seen=len(
                                photos_unclassifiable_in_spec
                            ),
                        ),
                    },
                )

            if run.control.should_abort(run.abort):
                _close_pending_inference()
            else:
                _flush_pending_inference()

            # Skip the grouping/storage finalization on cancel — it can
            # take a minute on large collections and the user has already
            # asked us to stop. Per-photo counters are accurate, no
            # corrective fixup needed.
            if run.control.should_abort(run.abort):
                run.emit_progress(
                    run.runner, run.job["id"], run.stages, "classify",
                    f"Cancelled — {processed_in_spec} of "
                    f"{total} processed",
                    step_id=step_id,
                )
                run.runner.update_step(
                    run.job["id"], step_id,
                    status="completed",
                    progress={
                        "current": processed_in_spec,
                        "total": total,
                    },
                    summary=(
                        f"Cancelled "
                        f"({processed_in_spec} of {total} processed)"
                    ),
                )
                continue

            # Reclassify clear, deferred from the top of the per-spec body
            # so a mid-batch cancel above leaves the user's prior
            # predictions intact (Codex P1 review on #710).  Scope by
            # labels_fingerprint so reclassifying one workspace's label
            # set doesn't wipe another workspace's cached predictions on
            # the same photos under its own fingerprint (shared-folder
            # setups).  ``clear_run_keys=False`` because the per-photo
            # ``record_classifier_run`` calls inside the loop above
            # already wrote fresh classifier_runs rows for processed
            # detections — wiping them here would strand the gate and
            # force the next non-reclassify pass to re-infer everything.
            #
            # Scope to photos this spec actually reached AND wasn't
            # skipped as source-offline: an unreached photo (mount-
            # scoped give-up, or every photo under a vanished folder)
            # is one this run had no chance to rewrite, so wiping
            # its prior prediction would leave it with nothing at
            # all (Codex #1388 P1). Use the per-spec skipped set
            # rather than the aggregate one so a photo skipped for
            # an earlier spec but successfully reached this spec
            # still has its stale prior prediction cleared before
            # ``_store_grouped_predictions`` writes the fresh row —
            # ``add_prediction`` is ``INSERT OR IGNORE`` and would
            # otherwise keep the stale species/confidence (Codex
            # #1388 P2 r3663642360). Fall back to the collection-
            # wide clear when no source outage hit this spec —
            # that's still the desired behavior on a clean
            # reclassify.
            if run.params.reclassify:
                if spec_source_skipped_photo_ids:
                    clear_ids = list(
                        spec_reached_photo_ids
                        - spec_source_skipped_photo_ids
                    )
                else:
                    clear_ids = [p["id"] for p in photos]
                if clear_ids:
                    thread_db.clear_predictions(
                        model=model_name,
                        collection_photo_ids=clear_ids,
                        labels_fingerprint=spec_fp,
                        clear_run_keys=False,
                    )

            group_result = _store_grouped_predictions(
                raw_results, run.job["id"], model_name,
                grouping_window, similarity_threshold, tax, thread_db,
                labels_fingerprint=spec_fp,
            )
            # promote_and_publish reads persisted predictions written
            # by _store_grouped_predictions above; running it inside
            # _record_batch_classifier_runs would no-op because those
            # rows don't exist yet, leaving fresh classifier_runs
            # stranded on runtime_fingerprint = 'legacy' and out of
            # bundle exports.
            # Reconstruct the configured ArtifactStore from the
            # path stashed by ``run_pipeline_job`` so classifier
            # artifacts published here land in the same cache the
            # status / export / catalog-reapplication paths use
            # when ``COMPUTATION_CACHE_DIR`` is overridden.
            from computation_cache import ArtifactStore
            _publish_cache_dir = run.job.get("_computation_cache_dir")
            _publish_store = (
                ArtifactStore(_publish_cache_dir)
                if _publish_cache_dir else None
            )
            _publish_classifier_runs_for_raw_results(
                thread_db, raw_results, model_name, spec_fp,
                labels_fingerprint_full=loaded_models.get(
                    "labels_fingerprint_full"
                ),
                model_identity=loaded_models.get(
                    "classifier_model_identity"
                ),
                taxonomy_identity=loaded_models.get(
                    "taxonomy_identity", "no-tax",
                ),
                store=_publish_store,
            )
            preds = group_result["predictions_stored"]
            total_predictions_stored += preds
            total_full_image_fallbacks += full_image_fallbacks
            total_failed += failed
            total_skipped_existing += skipped_existing
            models_succeeded += 1
            # A per-model row is "completed" only when it actually
            # finished classifying every photo it was asked to. If
            # the mount died (source_offline["reason"] latched) or
            # a folder outage left photos unread
            # (spec_source_skipped_photo_ids), the row should be
            # ``failed`` — the Jobs page reads ``step.status``
            # directly and auto-collapses ``completed`` rows without
            # warnings, so leaving this ``completed`` would show a
            # failed job with a green, collapsed classifier row
            # (Codex #1388 P2 r3664058179). The later stage-status
            # rollup at the end of the function isn't mapped back
            # to the ``classify:<model>`` step, so the fix has to
            # land here.
            spec_gave_up = bool(source_offline["reason"])
            spec_had_skips = bool(spec_source_skipped_photo_ids)
            spec_step_status = (
                "failed" if (spec_gave_up or spec_had_skips)
                else "completed"
            )
            if spec_step_status == "failed":
                failed_step_ids.add(step_id)
            else:
                completed_step_ids.add(step_id)

            # Reclassify stale-row purge: only fires after the FIRST
            # successful model has written fresh predictions, so a run
            # where every model fails to load leaves prior detections
            # (and their cascaded predictions) intact. Photos whose
            # detect iteration never completed keep their old rows.
            if (
                run.params.reclassify
                and models_succeeded == 1
                and detect_state["pre_run_det_ids"]
            ):
                pre_ids = detect_state["pre_run_det_ids"]
                # Scope the purge to photos whose detect AND classify
                # iterations both completed in this run. Using only
                # classify coverage would delete rows for photos that
                # hit the db.get_detections() fallback (i.e. never got
                # a fresh detect). Using only detect coverage would
                # delete rows for photos the classifier never reached.
                # The intersection guarantees there's a replacement
                # detection AND that the classifier considered it.
                #
                # Also subtract source-skipped photos: ``first_model_
                # photo_ids`` is added to at the TOP of the per-photo
                # body (before the image read), so a photo whose read
                # later failed with the source offline is still in
                # that set. Without this subtraction, the purge below
                # would delete any pre-run detection ids whose boxes
                # differ from the fresh boxes even for photos we
                # never classified — cascading through their prior
                # predictions despite the ``clear_predictions``
                # exclusion that already spares them (Codex #1388
                # P1 r3663922709).
                purge_ids = (
                    (first_model_photo_ids
                     & detect_state["processed_ids"])
                    - source_skipped_photo_ids
                )
                # Delete only pre-run ids the current run did NOT
                # re-produce. Detection ids are content-addressed
                # (vireo/detection_id.py), so re-detecting the same boxes
                # yields the SAME ids as the pre-run snapshot, and
                # write_detection_batch UPSERTs them with the freshly
                # written predictions now hanging off them. Deleting every
                # pre-run id unconditionally would cascade-delete those
                # predictions (and the live detection rows) for every photo
                # whose boxes didn't change — the common reclassify case.
                # Compare against the ids THIS run actually re-detected
                # (detect_state["detections"] is the in-memory map
                # _detect_batch built). A no-detection photo may also
                # have a freshly used synthetic full-image anchor from
                # the fallback path; preserve that id so the purge does
                # not cascade-delete the new fallback prediction. Other
                # pre-run rows on empty photos are stale and get purged
                # (write_detection_batch([]) already cleared the
                # MegaDetector rows at the data layer — this is the
                # belt-and-suspenders pass and cross-model cleanup). A
                # photo re-detected with the same boxes has its ids in
                # the fresh set, so they survive.
                fresh_by_photo = detect_state["detections"]
                stale_ids = [
                    det_id
                    for photo_id, id_set in pre_ids.items()
                    if photo_id in purge_ids
                    for det_id in id_set
                    if det_id not in (
                        {
                            d["id"]
                            for d in fresh_by_photo.get(photo_id, [])
                        }
                        | fresh_full_image_ids_by_photo.get(photo_id, set())
                    )
                ]
                if stale_ids:
                    getattr(
                        thread_db,
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

            parts = [f"{preds} predictions"]
            if skipped_existing:
                parts.append(f"{skipped_existing} cached")
            if full_image_fallbacks:
                parts.append(f"{full_image_fallbacks} full-image fallback")
            if contextual_weak_ids:
                parts.append(
                    f"{len(contextual_weak_ids)} weak detections rescued"
                )
            if failed:
                parts.append(f"{failed} failed")
            if source_skipped:
                parts.append(
                    f"{source_skipped} unreachable (source offline)"
                )
            if source_offline["reason"]:
                # Name the photos we never reached rather than folding
                # them into a count that reads as "classified".
                parts.append(
                    f"stopped after {processed_in_spec} of {total} — "
                    f"source {source_offline['reason']}"
                )
            # Attach the outage as ``error=`` on the failed row so the
            # Jobs page shows a human-readable reason next to the
            # collapsed step, matching the shape of the
            # model-load-failure branch above (line 4271).
            step_error = None
            if spec_step_status == "failed":
                if spec_gave_up:
                    step_error = (
                        f"Stopped after {processed_in_spec} of "
                        f"{total} — source {source_offline['reason']}"
                    )
                else:
                    step_error = (
                        f"{len(spec_source_skipped_photo_ids)} of "
                        f"{total} photos unreachable "
                        "(source offline)"
                    )
            run.runner.update_step(
                run.job["id"], step_id, status=spec_step_status,
                summary=", ".join(parts),
                error=step_error,
            )

        # Cancellation takes precedence over the all-models-failed-to-load
        # signal: if the user cancelled mid-classify after a prior model
        # had already been added to skipped_model_names, raising here
        # would misclassify the cancel as a fatal load failure and
        # overwrite the per-model 'Cancelled' summary in the exception
        # handler.
        if (
            models_succeeded == 0
            and skipped_model_names
            and not run.control.should_abort(run.abort)
        ):
            raise RuntimeError(
                f"All {len(skipped_model_names)} model(s) failed to load: "
                + ", ".join(skipped_model_names)
            )

        # Roll up per-photo failures into a single classify stage status
        # + errors[] entry, matching the pattern in #562. Per-model step
        # rows already carry their own summary; the stage status reflects
        # the whole classify pass.  error_count uses unique failed photo
        # IDs (not per-model attempt count) so the badge can never
        # exceed total photos.
        n_failed_photos = len(failed_photo_ids)
        n_source_skipped_photos = len(source_skipped_photo_ids)
        # Publish the source-skipped set to downstream stages BEFORE
        # deciding to set ``abort``. extract_masks and eye_keypoints
        # need this even on the folder-scoped path (abort deliberately
        # stays clear so healthy folders keep processing) — otherwise
        # they still walk every detected photo in the missing folder
        # and re-issue reads against the offline share (Codex #1388
        # P2 r3664058173).
        source_offline_state["skipped_photo_ids"] = (
            set(source_skipped_photo_ids)
        )
        if source_offline["reason"]:
            # Must carry the "[classify] Fatal:" prefix: the end-of-run
            # rollup picks the job's headline error by that marker and
            # would otherwise fall back to errors[0] — likely an
            # unrelated per-photo warning logged much earlier.
            run.errors.append(
                f"[classify] Fatal: source "
                f"{source_offline['reason']}. Reconnect it and run "
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
                total_failed > 0
                or source_offline["reason"]
                or n_source_skipped_photos > 0
            )
            else "completed"
        )
        if total_failed > 0:
            run.errors.append(
                f"[classify] {n_failed_photos} of {total} photos "
                "failed to classify"
            )
        run.result["stages"]["classify"] = {
            "total": total,
            "predictions_stored": total_predictions_stored,
            "detected": detect_state["total_detected"],
            "failed": total_failed,
            "source_offline": source_offline["reason"],
            "source_skipped": n_source_skipped_photos,
            "already_classified": total_skipped_existing,
            "full_image_fallbacks": total_full_image_fallbacks,
            "weak_detection_rescues": len(contextual_weak_ids),
            "model_count": len(resolved_specs_local),
            "models_succeeded": models_succeeded,
            "models_skipped": len(skipped_model_names),
            "skipped_model_names": skipped_model_names,
        }
    except Exception as e:
        run.errors.append(f"[classify] Fatal: {e}")
        log.exception("Pipeline classify stage failed")
        run.abort.set()
        run.stages["classify"]["status"] = "failed"
        # Only surface the fatal error on rows that haven't already
        # reached a terminal state. Without this, a late-loop exception
        # would overwrite the 'completed' status of earlier models that
        # finished successfully, misreporting per-model outcomes.
        specs_for_step_ids = loaded_models.get("resolved_specs") or []
        for spec in specs_for_step_ids:
            sid = f"classify:{spec['id']}"
            if sid in completed_step_ids or sid in failed_step_ids:
                continue
            run.runner.update_step(
                run.job["id"], sid,
                status="failed", error=str(e),
            )
    finally:
        # Release the held classifier so subsequent pipelines can reuse
        # the cached session (or the idle timer can reclaim VRAM). Runs
        # whether classify completed cleanly, errored mid-loop, or hit
        # the fatal-exception path above.
        _release_classifier_cache_handle(loaded_models)

    run.update_stages(run.runner, run.job["id"], run.stages)
