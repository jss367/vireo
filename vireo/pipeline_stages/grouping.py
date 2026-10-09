"""Grouping stages for the streaming photo pipeline."""

import logging
import os
import time

from pipeline_stages.context import PipelineRun

log = logging.getLogger(__name__)


def regroup_stage(run: PipelineRun):
    """Run pipeline grouping + scoring + triage from cached features."""
    if run.params.skip_regroup:
        # Only take the species-review save path when the caller
        # explicitly asked for it (the identify preset sets
        # ``review_mode="species"`` via process_strategies). A
        # classify-only run without that opt-in — Advanced/Custom
        # on the Process page, or an API client sending
        # ``skip_regroup: true`` — must NOT overwrite
        # ``pipeline_results_ws*.json`` with all-REVIEW species
        # output, since that would silently reintroduce the
        # culling-pipeline downgrade the reviewer flagged (the
        # user just wanted to refresh classifications, not turn
        # the workspace cache into a species-review cache).
        do_species = (
            run.params.review_mode == "species"
            and not run.abort.is_set()
            and run.collection_id
            and not run.params.skip_classify
        )
        if not do_species:
            run.stages["regroup"]["status"] = "skipped"
            run.runner.update_step(run.job["id"], "regroup", status="completed",
                               summary="Skipped")
            # Emit a progress event so the SSE stream (and tests
            # asserting on the last progress payload) can see the
            # stage's terminal "skipped" state. Without this, a
            # downstream miss_stage that also short-circuits
            # leaves the last stages dict stuck at whatever the
            # detect/classify stage emitted last.
            run.update_stages(run.runner, run.job["id"], run.stages)
            return
        try:
            import config as cfg
            from pipeline import (
                load_photo_features,
                run_species_review_pipeline,
                save_results,
            )

            thread_db = run.database_factory(run.db_path)
            thread_db.set_active_workspace(run.workspace_id)

            effective_cfg = thread_db.get_effective_config(cfg.load())
            pipeline_cfg = effective_cfg.get("pipeline", {})

            photos = load_photo_features(
                thread_db,
                collection_id=run.collection_id,
                config=effective_cfg,
            )
            if run.params.exclude_photo_ids:
                photos = [
                    p for p in photos
                    if p["id"] not in run.params.exclude_photo_ids
                ]
            if not photos:
                run.result["stages"]["review"] = {
                    "error": "No photos with pipeline features found.",
                }
            else:
                results = run_species_review_pipeline(
                    photos,
                    config=pipeline_cfg,
                    emit_trace=True,
                )
                cache_dir = os.path.dirname(run.db_path)
                # Don't preserve miss_computed_at from any prior
                # full run. The identify strategy skips the miss
                # stage entirely, so the cache we're writing has
                # no misses of its own; carrying the old marker
                # forward would make Pipeline Review call
                # /api/misses?since=<old marker> and render miss
                # rows from the previous full run as if they were
                # produced by this identify pass.
                save_results(
                    results,
                    cache_dir,
                    run.workspace_id,
                    preserve_miss_marker=False,
                )
                # The species-only pipeline overwrites
                # pipeline_results_ws*.json with review-only output
                # (no burst/keep/reject scoring). If a prior full
                # regroup left a valid last_group_fingerprint stamped
                # on the workspace, pipeline_plan._group_plan would
                # match it against current settings and report
                # "done-prior" — silently letting an advanced Group
                # & Score run be skipped even though the cache no
                # longer contains any triage output. Invalidate the
                # stamp so a subsequent full run is correctly shown
                # as will-run.
                thread_db.workspaces.set_group_state(
                    workspace_id=run.workspace_id,
                    fingerprint=None,
                    when_ts=None,
                )
                run.result["stages"]["review"] = results.get("summary", {})

            run.stages["regroup"]["status"] = "completed"
            run.runner.update_step(
                run.job["id"], "regroup",
                status="completed",
                summary="Review results ready" if photos else "No photos to group",
            )
        except Exception as e:
            run.errors.append(f"[review] Fatal: {e}")
            log.exception("Pipeline species-review stage failed")
            run.stages["regroup"]["status"] = "failed"
            run.runner.update_step(
                run.job["id"], "regroup", status="failed", error=str(e),
            )
        run.update_stages(run.runner, run.job["id"], run.stages)
        return

    if run.abort.is_set() or not run.collection_id:
        run.stages["regroup"]["status"] = "skipped"
        run.runner.update_step(run.job["id"], "regroup", status="completed",
                           summary="Skipped")
        return

    run.stages["regroup"]["status"] = "running"
    run.runner.update_step(run.job["id"], "regroup", status="running")
    run.update_stages(run.runner, run.job["id"], run.stages)

    # The per-workspace regroup lock is now acquired by the
    # orchestrator (see run_pipeline_job body) so it spans BOTH
    # regroup_stage and miss_stage atomically — the inner lock
    # that used to live here would deadlock against the outer one
    # (Python locks aren't reentrant). The deferred-update pattern
    # likewise goes away: runner.update_step is fine to call here
    # because nothing under JobRunner._lock acquires the workspace
    # regroup lock, so there's no cycle to invert.
    try:
        import config as cfg
        from pipeline import load_photo_features, run_full_pipeline, save_results

        thread_db = run.database_factory(run.db_path)
        thread_db.set_active_workspace(run.workspace_id)

        effective_cfg = thread_db.get_effective_config(cfg.load())
        pipeline_cfg = dict(effective_cfg.get("pipeline", {}))

        # Mirror the eye_keypoints_stage per-run override so scoring
        # honors the same explicit intent. Without carrying the
        # override into pipeline_cfg here, ``run_full_pipeline``
        # reloads workspace config with ``eye_detect_enabled=False``
        # (the new default) and ``score_encounter`` ignores the
        # ``eye_tenengrad`` values the eye stage just wrote — so the
        # visible checkbox would affect only the expensive keypoint
        # pass, not the culling result the user actually sees.
        # Gated on ``eye_detect_override`` (an explicit per-run
        # signal), NOT on ``not skip_eye_keypoints``, because the
        # latter is False by default in ``the "Full" saved process``
        # too — using it would force eye scoring on for any chained
        # ``full`` run regardless of workspace Settings.
        if run.params.eye_detect_override is not None:
            pipeline_cfg["eye_detect_enabled"] = run.params.eye_detect_override

        photos = load_photo_features(thread_db, collection_id=run.collection_id, config=effective_cfg)
        if run.params.exclude_photo_ids:
            photos = [p for p in photos if p["id"] not in run.params.exclude_photo_ids]
        if not photos:
            run.result["stages"]["regroup"] = {"error": "No photos with pipeline features found."}
            run.stages["regroup"]["status"] = "completed"
            run.runner.update_step(
                run.job["id"], "regroup",
                status="completed", summary="No photos to group",
            )
        else:
            results = run_full_pipeline(photos, config=pipeline_cfg, emit_trace=True)
            cache_dir = os.path.dirname(run.db_path)
            save_results(results, cache_dir, run.workspace_id)

            # Stamp the grouping fingerprint + timestamp BEFORE marking
            # the step completed, so a partial regroup that crashes
            # between here and update_step doesn't end up labeled "fresh"
            # with a stale fp.
            #
            # Only stamp when the regroup actually covered the whole
            # workspace — if it ran on a filtered subset (a
            # sub-collection, or with exclude_photo_ids set) some
            # workspace photos were intentionally not regrouped, so
            # claiming workspace-level freshness would let the pipeline
            # page hide a real stale state.
            from pipeline import (
                collection_covers_workspace,
                compute_group_fingerprint,
            )
            # A per-run eye override that differs from the
            # workspace's own effective ``eye_detect_enabled`` means
            # this run's KEEP/REJECT decisions came from scoring
            # settings the workspace's normal state wouldn't produce.
            # ``compute_group_fingerprint`` reads only encounter/burst
            # keys — not ``eye_detect_enabled`` — so stamping it here
            # would mark eye-scored (or eye-disabled) results as
            # settings-fresh for a later plan run against the
            # workspace's real settings. Treat that as a partial run
            # so ``pipeline_plan`` reports the cache as needing to
            # re-run instead of hiding the mismatch.
            workspace_eye_setting = bool(
                effective_cfg.get("pipeline", {}).get(
                    "eye_detect_enabled", False,
                )
            )
            per_run_eye_override_differs = (
                run.params.eye_detect_override is not None
                and bool(run.params.eye_detect_override) != workspace_eye_setting
            )
            covered_full_workspace = (
                not run.params.exclude_photo_ids
                and collection_covers_workspace(
                    thread_db, run.workspace_id, run.collection_id,
                )
                and not per_run_eye_override_differs
            )
            if covered_full_workspace:
                thread_db.workspaces.set_group_state(
                    workspace_id=run.workspace_id,
                    fingerprint=compute_group_fingerprint(effective_cfg),
                    when_ts=int(time.time()),
                )
            else:
                # Partial run — save_results just clobbered
                # pipeline_results_ws*.json with subset output, so any
                # pre-existing fingerprint now points at a cache that
                # no longer reflects the full workspace. Invalidate so
                # the pipeline page surfaces the staleness as will-run
                # instead of falsely reporting done-prior.
                thread_db.workspaces.set_group_state(
                    workspace_id=run.workspace_id,
                    fingerprint=None,
                    when_ts=None,
                )

            run.stages["regroup"]["status"] = "completed"
            summary_info = results.get("summary", {})
            groups = summary_info.get("groups", "")
            run.runner.update_step(
                run.job["id"], "regroup",
                status="completed",
                summary=f"{groups} groups" if groups else "Done",
            )
            run.result["stages"]["regroup"] = summary_info
    except Exception as e:
        run.errors.append(f"[regroup] Fatal: {e}")
        log.exception("Pipeline regroup stage failed")
        run.stages["regroup"]["status"] = "failed"
        run.runner.update_step(
            run.job["id"], "regroup", status="failed", error=str(e),
        )
    run.update_stages(run.runner, run.job["id"], run.stages)


def miss_stage(run: PipelineRun):
    """Compute miss-detection flags for the workspace after regroup.

    Runs last so burst_id is available. Uses only per-photo features
    already computed by earlier stages — no model inference.
    """
    # Skip when classify was skipped: classify_miss depends on fresh
    # detections/classifications written by the classify stage, and
    # without them it would mass-flag "no_subject" on photos whose
    # subjects simply weren't re-evaluated this run.
    #
    # Also skip when regroup failed: miss classification depends on
    # regroup's burst_id output, so running here after a regroup
    # failure would overwrite miss_* flags with stale context during
    # an already-failing job. regroup_stage marks itself "failed"
    # without setting abort, so check the stage status explicitly.
    if (
        run.params.skip_regroup
        or run.params.skip_classify
        or run.abort.is_set()
        or not run.collection_id
        or run.stages["regroup"].get("status") == "failed"
    ):
        run.stages["misses"]["status"] = "skipped"
        run.runner.update_step(run.job["id"], "misses", status="completed",
                           summary="Skipped")
        return

    # Hoisted from the try: block below so the miss_enabled guard can
    # read effective config before the transient "running" status is
    # written.
    try:
        from datetime import UTC, datetime

        import config as cfg
        from misses import compute_misses_for_workspace
        from pipeline import load_results_raw, save_results_raw

        thread_db = run.database_factory(run.db_path)
        thread_db.set_active_workspace(run.workspace_id)

        effective_cfg = thread_db.get_effective_config(cfg.load())
        pipeline_cfg = effective_cfg.get("pipeline", {})
    except Exception as e:
        # Mark the stage failed BEFORE returning. The transient
        # "running" write is *below* this guard, so without stamping
        # "failed" here the stage would stay "pending"; the pipeline
        # finalizer treats absence-of-failed as success and would
        # wrongly mark the whole job completed despite a fatal setup
        # error (cfg.load, Database(...), or any import raising).
        run.stages["misses"]["status"] = "failed"
        run.runner.update_step(run.job["id"], "misses", status="failed",
                           error=str(e))
        run.errors.append(f"[misses] Fatal: {e}")
        log.exception("Pipeline miss-detection setup failed")
        run.update_stages(run.runner, run.job["id"], run.stages)
        return

    # Effective miss_enabled: per-run PipelineParams override wins
    # over workspace config, mirroring how other skip_* flags
    # override workspace defaults. Inject the effective value into
    # pipeline_cfg *before* the guard so both branches — the
    # short-circuit skip AND the fall-through to compute — see the
    # same value: compute_misses_for_workspace reads
    # pipeline_cfg["miss_enabled"] itself, so a strategy that
    # enables misses on a workspace where they're disabled would
    # otherwise get a silent 0 from compute.
    if run.params.miss_enabled is not None:
        pipeline_cfg = {**pipeline_cfg,
                        "miss_enabled": run.params.miss_enabled}
    miss_enabled = pipeline_cfg.get("miss_enabled", True)
    if not miss_enabled:
        # Do NOT fall through to compute_misses_for_workspace: it
        # returns 0 when disabled and the completion path would then
        # stamp "0 photos evaluated", which reads as "misses ran and
        # found none" rather than "misses were disabled". Skipping
        # here also leaves the miss_computed_at cache marker
        # unstamped, which pipeline_review's "current-run misses"
        # shortcut depends on.
        run.stages["misses"]["status"] = "skipped"
        run.runner.update_step(run.job["id"], "misses", status="completed",
                           summary="Skipped")
        run.update_stages(run.runner, run.job["id"], run.stages)
        return

    run.stages["misses"]["status"] = "running"
    run.runner.update_step(run.job["id"], "misses", status="running")
    run.update_stages(run.runner, run.job["id"], run.stages)

    try:
        # Share one timestamp between the DB write and the saved
        # pipeline-results cache so pipeline_review's "Review misses"
        # shortcut can gate on actual recomputation in this run and
        # scope /misses?since=... to exactly what was just written.
        now_ts = datetime.now(UTC).isoformat(timespec="microseconds")

        n = compute_misses_for_workspace(
            thread_db,
            pipeline_cfg,
            collection_id=run.collection_id,
            exclude_photo_ids=run.params.exclude_photo_ids,
            now=now_ts,
        )

        run.stages["misses"]["status"] = "completed"
        run.stages["misses"]["count"] = n
        run.runner.update_step(run.job["id"], "misses", status="completed",
                           summary=f"{n} photos evaluated")
        run.result["stages"]["misses"] = {"evaluated": n}

        # Mark the cached results so the review UI knows misses
        # were actually recomputed this run. Without this, the
        # shortcut would surface stale miss flags from a prior
        # run as "current-run misses" whenever miss_enabled=False
        # or the stage was skipped.
        if miss_enabled:
            cache_dir = os.path.dirname(run.db_path)
            cached = load_results_raw(cache_dir, run.workspace_id)
            if cached is not None:
                cached["miss_computed_at"] = now_ts
                save_results_raw(cached, cache_dir, run.workspace_id)
    except Exception as e:
        run.errors.append(f"[misses] Fatal: {e}")
        log.exception("Pipeline miss-detection stage failed")
        run.stages["misses"]["status"] = "failed"
        run.runner.update_step(run.job["id"], "misses", status="failed", error=str(e))

    run.update_stages(run.runner, run.job["id"], run.stages)
