"""Job-control endpoints and the self-contained background-job launchers.

Job *control* (list, status, cancel, pause, resume, stream, history) talks
only to the ``JobRunner``. The launchers here are the ones whose request
handling needs nothing from the legacy application module beyond what every
blueprint already receives: the request database, ``json_error``, the runner,
the database path and the thumbnail cache directory. Launchers that lean on
import/pipeline/settings helpers stay in ``app.py`` until their domain moves.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import time

from db import Database
from flask import Blueprint, Response, jsonify, request
from web.background_jobs import make_background_job
from web.request_args import reject_visual_collection
from werkzeug.exceptions import BadRequest

log = logging.getLogger(__name__)


def _strip_heavy_for_list(job):
    """Drop heavy fields from a job dict for the polling response.

    ``GET /api/jobs`` is fetched every few seconds by the navbar and the
    Jobs page. Some jobs (notably duplicate-scan, which stores every
    proposal) accumulate ``result`` payloads of tens of MB — re-shipping
    that on every poll freezes the browser and starves the Flask thread
    pool.

    Callers that need the full result fetch ``GET /api/jobs/<id>`` (active
    jobs) or ``GET /api/jobs/history`` (history rows), which keep the full
    payload. ``has_result`` lets the UI tell "no result yet" apart from
    "result available, fetch on demand" without round-tripping.
    """
    trimmed = {k: v for k, v in job.items() if k != "result"}
    trimmed["has_result"] = job.get("result") is not None
    return trimmed


def _rate(job, current):
    return round(current / max(time.time() - job["_start_time"], 0.01), 1)


def create_jobs_blueprint(
    get_db,
    json_error,
    get_runner,
    db_path,
    get_thumb_cache_dir,
):
    """Build the jobs blueprint.

    ``get_thumb_cache_dir`` is a callable rather than a path because the
    launchers read ``app.config["THUMB_CACHE_DIR"]`` when the job runs, not
    when the app is built. The collection-scoped launchers call
    ``web.request_args.reject_visual_collection`` before starting work so
    a visual-only collection gets a 400 instead of a widened scope.
    """
    blueprint = Blueprint("jobs", __name__)
    background_job = make_background_job(get_runner, get_db, db_path, Database)

    def vireo_dir():
        return os.path.dirname(get_thumb_cache_dir())

    # -- Job control -------------------------------------------------------

    @blueprint.get("/api/jobs")
    def api_jobs_list():
        from embedding_cache import get_embedding_cache_diagnostics
        from resource_ledger import get_resource_ledger

        runner = get_runner()
        db = get_db()
        active = [_strip_heavy_for_list(j) for j in runner.list_jobs()]
        history = [_strip_heavy_for_list(j) for j in runner.get_history(db, limit=10)]
        ws_rows = db.get_workspaces()
        ws_names = {w["id"]: w["name"] for w in ws_rows}
        return jsonify({
            "active": active,
            "history": history,
            # Vireo is overriding the system's idle-sleep setting while a
            # job runs (issue #1397). Surfacing it keeps that visible
            # rather than silent — see CORE_PHILOSOPHY "no black boxes".
            "keeping_awake": runner.sleep_blocker.active,
            "resource_budget": get_resource_ledger().snapshot(),
            "workload_metrics": {
                "embedding_cache": get_embedding_cache_diagnostics(),
            },
            "active_workspace_id": db._active_workspace_id,
            "workspace_names": ws_names,
        })

    @blueprint.post("/api/jobs/runtime-warning/dismiss")
    def api_job_runtime_warning_dismiss():
        body = request.get_json(silent=True) or {}
        warning_id = body.get("id")
        if not warning_id:
            return json_error("id required")
        log.info("Client dismissed runtime warning: %s", warning_id)
        return jsonify({"ok": True, "id": str(warning_id)})

    @blueprint.get("/api/jobs/history")
    def api_job_history():
        db = get_db()
        limit = min(max(1, request.args.get("limit", 10, type=int)), 1000)
        return jsonify(get_runner().get_history(db, limit=limit))

    @blueprint.get("/api/jobs/<job_id>")
    def api_job_status(job_id):
        job = get_runner().get(job_id)
        if not job:
            return json_error("job not found", 404)
        return jsonify(job)

    @blueprint.post("/api/jobs/<job_id>/cancel")
    def api_job_cancel(job_id):
        """Request cancellation of a running, pausing, paused, or queued job.

        Returns 200 if the live job accepted cancellation, or 404 if the job
        does not exist or has already reached a terminal state.
        """
        runner = get_runner()
        if runner.cancel_job(job_id):
            return jsonify({"cancelled": True, "job_id": job_id})
        job = runner.get(job_id)
        if job is None:
            return json_error("job not found", 404)
        return json_error(f"job is not running (status={job['status']})", 404)

    @blueprint.post("/api/jobs/<job_id>/pause")
    def api_job_pause(job_id):
        """Pause supported work at its next safe checkpoint."""
        runner = get_runner()
        if runner.pause_job(job_id):
            return jsonify({
                "pause_requested": True,
                "job_id": job_id,
                "status": "pausing",
            })
        job = runner.get(job_id)
        if job is None:
            return json_error("job not found", 404)
        if not job.get("pausable"):
            return json_error("job does not support pausing", 409)
        return json_error(
            f"job cannot be paused (status={job['status']})", 409,
        )

    @blueprint.post("/api/jobs/<job_id>/resume")
    def api_job_resume(job_id):
        """Resume a pausing or paused job."""
        runner = get_runner()
        if runner.resume_job(job_id):
            return jsonify({
                "resumed": True,
                "job_id": job_id,
                "status": "running",
            })
        job = runner.get(job_id)
        if job is None:
            return json_error("job not found", 404)
        return json_error(
            f"job cannot be resumed (status={job['status']})", 409,
        )

    @blueprint.post("/api/jobs/cancel-queued")
    def api_jobs_cancel_queued():
        """Bulk-cancel every queued pipeline in a workspace.

        Body (optional): ``{"workspace_id": <id>}``. Default scope is
        the active workspace — matching what the user sees on the Jobs
        page. Running pipelines and queued pipelines in OTHER
        workspaces are untouched.

        Returns ``{"cancelled": [job_ids...]}``.
        """
        runner = get_runner()
        db = get_db()
        raw_body = request.get_data(cache=True)
        if request.is_json and raw_body:
            try:
                body = request.get_json()
            except BadRequest:
                return json_error("Malformed JSON body", 400)
        elif raw_body.strip():
            body = request.get_json(silent=True)
        else:
            body = {}
        if not isinstance(body, dict):
            return json_error("JSON body must be an object", 400)

        if "workspace_id" in body:
            ws_id = body["workspace_id"]
            if isinstance(ws_id, bool) or not isinstance(ws_id, int):
                return json_error("workspace_id must be an integer", 400)
        else:
            ws_id = db._active_workspace_id
            if ws_id is None:
                return json_error("workspace_id is required", 400)

        cancelled = runner.cancel_queued_jobs(workspace_id=ws_id)
        return jsonify({"cancelled": cancelled})

    @blueprint.get("/api/jobs/<job_id>/stream")
    def api_job_stream(job_id):
        """SSE stream of job progress events."""
        runner = get_runner()
        job = runner.get(job_id)
        if not job:
            return json_error("job not found", 404)

        q = runner.subscribe(job_id)

        def generate():
            try:
                while True:
                    try:
                        event = q.get(timeout=1)
                        yield f"event: {event['type']}\ndata: {json.dumps(event['data'])}\n\n"
                        if event["type"] == "complete":
                            break
                    except queue.Empty:
                        # Send keepalive
                        yield ": keepalive\n\n"
                        # Check if job is done (in case we missed the complete event).
                        # Include "cancelled" as a terminal state so cancelled jobs
                        # close the SSE stream instead of looping indefinitely.
                        j = runner.get(job_id)
                        if j is None:
                            # Job was pruned from finished jobs dict; true terminal
                            # status is unknown (could have been completed, failed, or
                            # cancelled before pruning).  Emit "expired" so callers do
                            # not incorrectly execute success-only code paths.
                            yield f"event: complete\ndata: {json.dumps({'status': 'expired', 'result': None, 'errors': ['job expired from server memory before stream could read final status']})}\n\n"
                            break
                        if j["status"] in ("completed", "failed", "cancelled"):
                            yield f"event: complete\ndata: {json.dumps({'status': j['status'], 'result': j['result'], 'errors': j['errors']})}\n\n"
                            break
            finally:
                runner.unsubscribe(job_id, q)

        return Response(
            generate(),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # -- Maintenance launchers ---------------------------------------------

    @blueprint.post("/api/jobs/thumbnails")
    @background_job
    def api_job_thumbnails(ctx):
        def work(job):
            from thumbnails import generate_all

            thread_db = ctx.thread_db()

            def progress_cb(current, total):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {"current": current, "total": total, "rate": _rate(job, current)},
                )

            job["_start_time"] = time.time()
            thumb_cache_dir = get_thumb_cache_dir()
            return generate_all(
                thread_db, thumb_cache_dir, progress_callback=progress_cb,
                vireo_dir=os.path.dirname(thumb_cache_dir),
                cancel_check=lambda: ctx.runner.is_cancelled(job["id"]),
            )

        return ctx.start("thumbnails", work, pausable=True)

    @blueprint.post("/api/duplicates/scan")
    @background_job
    def api_duplicates_scan(ctx):
        """Start a background duplicate-detection job.

        Returns immediately with the job id. The job walks every file_hash
        group with 2+ rows (both unresolved AND already-auto-resolved
        groups) and proposes a winner/losers per group. Resolved groups
        are flagged ``status='resolved'`` so the UI can surface them in a
        separate section for disk cleanup.

        The UI polls /api/jobs/<id> to fetch the proposals from job.result
        and lets the user apply unresolved groups via /api/duplicates/apply
        or trash already-resolved loser files via
        /api/duplicates/delete-loser-files.
        """

        def work(job):
            from duplicate_scan import run_duplicate_scan
            thread_db = ctx.thread_db()
            try:
                return run_duplicate_scan(
                    job, thread_db, include_resolved=True,
                    cancel_check=lambda: ctx.runner.is_cancelled(job["id"]),
                )
            finally:
                thread_db.conn.close()

        return ctx.start("duplicate-scan", work, pausable=True)

    @blueprint.post("/api/jobs/verify-hashes")
    @background_job
    def api_job_verify_hashes(ctx):
        """Re-hash every photo file and compare against the stored SHA-256.

        Background job: flags silent corruption (content changed, mtime
        unchanged), external edits (content + mtime changed), unreadable
        files, and baselines photos that have no stored hash yet. Results
        land in photos.hash_status; the audit page reads them via
        /api/audit/integrity.
        """

        def work(job):
            from audit import verify_hashes

            thread_db = ctx.thread_db()

            def progress_cb(current, total, filename):
                # Throttle SSE events; hashing is per-file fast on small
                # files and the stream doesn't need every one.
                if current % 10 != 0 and current not in (1, total):
                    return
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                        "current_file": filename,
                        "phase": "Verifying file hashes",
                    },
                )

            return verify_hashes(
                thread_db,
                progress_cb=progress_cb,
                should_cancel=lambda: ctx.runner.cancellation_requested(job["id"]),
                pause_requested=lambda: ctx.runner.pause_requested(job["id"]),
                pause_callback=lambda: ctx.runner.is_cancelled(job["id"]),
            )

        return ctx.start("verify-hashes", work, pausable=True)

    @blueprint.post("/api/jobs/capture-time")
    @background_job
    def api_job_capture_time(ctx):
        """Adjust capture timestamps and timezone offsets for selected photos."""
        body = request.get_json(silent=True)
        if body is None:
            body = {}
        elif not isinstance(body, dict):
            return json_error("request body must be a JSON object", 400)
        raw_ids = body.get("photo_ids", [])
        if not isinstance(raw_ids, list):
            return json_error("photo_ids must be a list", 400)
        if not raw_ids:
            return json_error("photo_ids required", 400)
        if len(raw_ids) > 50000:
            return json_error("too many photo_ids", 400)

        photo_ids = []
        seen = set()
        for raw in raw_ids:
            if isinstance(raw, bool) or not isinstance(raw, int):
                return json_error("photo_ids must be integers", 400)
            if raw in seen:
                continue
            seen.add(raw)
            photo_ids.append(raw)

        mode = body.get("mode", "preserve_instant")
        target_offset = body.get("target_offset")
        shift_minutes = body.get("shift_minutes")
        keep_backups = bool(body.get("keep_backups", True))

        def work(job):
            from capture_time import adjust_capture_time

            thread_db = ctx.thread_db()
            job["_start_time"] = time.time()
            job["progress"]["total"] = len(photo_ids)

            def progress_cb(current, total, filename):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = filename
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                        "current_file": filename,
                        "rate": _rate(job, current),
                        "phase": "Adjusting capture time",
                    },
                )

            return adjust_capture_time(
                thread_db,
                photo_ids,
                mode=mode,
                target_offset=target_offset,
                shift_minutes=shift_minutes,
                keep_backups=keep_backups,
                progress_callback=progress_cb,
                cancel_check=lambda: ctx.runner.is_cancelled(job["id"]),
            )

        return ctx.start(
            "capture-time",
            work,
            pausable=True,
            config={
                "photo_count": len(photo_ids),
                "photo_ids_sample": photo_ids[:20],
                "mode": mode,
                "target_offset": target_offset,
                "shift_minutes": shift_minutes,
                "keep_backups": keep_backups,
            },
        )

    # -- Model and taxonomy downloads --------------------------------------

    @blueprint.post("/api/jobs/download-model")
    @background_job
    def api_job_download_model(ctx):
        body = request.get_json(silent=True) or {}
        model_id = body.get("model_id")
        if not model_id:
            return json_error("model_id required")

        def work(job):
            from models import download_model

            def progress_cb(msg, current=0, total=0, rate=0):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = msg
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                        "current_file": msg,
                        "rate": rate,
                        "phase": "Downloading model",
                    },
                )

            path = download_model(model_id, progress_callback=progress_cb)
            return {"model_id": model_id, "weights_path": path}

        return ctx.start("download-model", work, config={"model_id": model_id})

    @blueprint.post("/api/jobs/verify-all-models")
    @background_job
    def api_job_verify_all_models(ctx):
        """Run SHA256 verification on every installed known model.

        Launches a background job that iterates get_models(), hashes each
        model's LFS files, and writes a .verify_failed sentinel into any
        directory whose files don't match HuggingFace's reported hashes.
        The UI can then show Repair for the bad ones via the existing
        state classifier path.
        """

        def work(job):
            import model_verify

            def progress_cb(msg):
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": 0,
                        "total": 0,
                        "current_file": msg,
                        "rate": 0,
                        "phase": "Verifying models",
                    },
                )

            results = model_verify.verify_all_models(
                progress_callback=progress_cb, pause_callback=lambda: ctx.checkpoint(job),
            )
            return {
                "verified": len(results),
                "failed": [mid for mid, r in results.items() if not r.ok],
                "ok": [mid for mid, r in results.items() if r.ok],
            }

        return ctx.start("verify-models", work, pausable=True)

    @blueprint.post("/api/jobs/download-hf-model")
    @background_job
    def api_job_download_hf_model(ctx):
        body = request.get_json(silent=True) or {}
        repo_id = body.get("repo_id", "").strip()
        if not repo_id:
            return json_error("repo_id required")

        def work(job):
            from models import download_hf_model

            def progress_cb(msg):
                job["progress"]["current_file"] = msg
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": 0,
                        "total": 0,
                        "current_file": msg,
                        "rate": 0,
                    },
                )

            result = download_hf_model(repo_id, progress_callback=progress_cb)
            return result

        return ctx.start("download-model", work, config={"repo_id": repo_id})

    @blueprint.post("/api/jobs/download-taxonomy")
    @background_job
    def api_job_download_taxonomy(ctx):
        def work(job):
            from taxonomy import (
                TAXONOMY_JSON_PATH,
                download_taxonomy,
                load_local_taxonomy,
                populate_taxa_db_from_json,
                seed_informal_groups,
            )

            def progress_cb(msg):
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": 0,
                        "total": 0,
                        "current_file": msg,
                        "rate": 0,
                    },
                )

            download_taxonomy(TAXONOMY_JSON_PATH, progress_callback=progress_cb)

            bg_db = ctx.thread_db()

            # Populate the SQLite taxa table from the same DWCA data so
            # add_keyword's auto-detect (which queries the DB, not the JSON)
            # can type newly-imported keywords as 'taxonomy' going forward.
            # Roll back and fail the job on error — populate_taxa_db_from_json
            # issues many INSERTs within a single open transaction, and
            # letting the subsequent mark_species_keywords call commit
            # would flush the partial writes onto disk and leave the taxa
            # table silently inconsistent.
            try:
                populate_taxa_db_from_json(
                    bg_db, TAXONOMY_JSON_PATH, progress_callback=progress_cb,
                )
                seed_informal_groups(bg_db)
            except Exception:
                log.error("Post-download taxa DB population failed", exc_info=True)
                bg_db.conn.rollback()
                raise

            # Retype existing keywords that match the new taxonomy so the
            # user sees the effect immediately, without restarting the app.
            # Roll back and fail the job on error: mark_species_keywords
            # accumulates UPDATEs before its own commit, so a mid-flight
            # failure (e.g., transient "database is locked") would leave
            # a pending transaction that a later commit could flush, and
            # reporting success would hide the retype failure from the UI.
            # The download + populate + seed steps already committed, so
            # the user keeps that progress — retrying the download re-runs
            # retype for free (it's idempotent).
            progress_cb("Retyping existing keywords...")
            try:
                # Go through load_local_taxonomy() rather than constructing
                # Taxonomy directly: it drops the cached instance before
                # parsing the replacement. Constructing here bypassed that,
                # so a refresh held the old ~2.8GB parse alive alongside the
                # new one and could OOM. It also seeds the cache, so the
                # next compare/accept request reuses this parse instead of
                # paying for its own.
                #
                # Pin it to the file we just downloaded. Without path=, a
                # transient failure parsing the new file would fall back to
                # a legacy copy, and we would retype keywords from the old
                # taxonomy while the taxa tables hold the new one — mixing
                # versions and reporting success.
                tax = load_local_taxonomy(path=TAXONOMY_JSON_PATH)
                if tax is None:
                    raise RuntimeError(
                        f"taxonomy unreadable after download: {TAXONOMY_JSON_PATH}"
                    )
                updated = bg_db.mark_species_keywords(tax)
                bg_db.repair_duplicate_photo_species()
                log.info("Retyped %d existing keywords as taxonomy after download", updated)
            except Exception:
                log.error("Post-download keyword retype failed", exc_info=True)
                bg_db.conn.rollback()
                raise
            return {"ok": True, "keywords_retyped": updated}

        return ctx.start("download-taxonomy", work)

    # -- Collection-scoped analysis launchers ------------------------------

    @blueprint.post("/api/jobs/sharpness")
    @background_job
    def api_job_sharpness(ctx):
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")
        db = get_db()
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err

        working_dir = vireo_dir()

        def work(job):
            from sharpness import score_collection_photos

            thread_db = ctx.thread_db()
            job["_start_time"] = time.time()

            ctx.runner.set_steps(job["id"], [
                {"id": "score", "label": "Score sharpness"},
                {"id": "save", "label": "Save results & auto-flag"},
            ])
            ctx.runner.update_step(job["id"], "score", status="running")

            def progress_cb(current, total, msg):
                job["progress"]["current"] = current
                job["progress"]["total"] = total
                job["progress"]["current_file"] = msg
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": current,
                        "total": total,
                        "current_file": msg,
                        "rate": _rate(job, current),
                        "phase": "Scoring sharpness",
                    },
                )

            result = score_collection_photos(
                thread_db,
                collection_id,
                progress_callback=progress_cb,
                vireo_dir=working_dir,
                pause_callback=lambda: ctx.checkpoint(job),
            )
            ctx.runner.update_step(job["id"], "score", status="completed",
                                   summary=f"{len(result['results'])} scored")

            # Save scores to database
            ctx.runner.update_step(job["id"], "save", status="running")
            ctx.runner.push_event(
                job["id"],
                "progress",
                {
                    "current": 0,
                    "total": len(result["results"]),
                    "current_file": "Saving scores to database...",
                    "rate": 0,
                    "phase": "Saving results",
                },
            )
            for r in result["results"]:
                ctx.checkpoint(job)
                thread_db.update_photo_sharpness(r["photo_id"], r["sharpness"])

            # Auto-flag: flag best in each group, suggest reject for worst
            best_count = 0
            for r in result["results"]:
                ctx.checkpoint(job)
                if r["group_size"] > 1 and r["is_best"]:
                    thread_db.update_photo_flag(r["photo_id"], "flagged",
                                                verify_workspace=False)
                    best_count += 1

            result["auto_flagged"] = best_count
            ctx.runner.update_step(job["id"], "save", status="completed",
                                   summary=f"{best_count} flagged")
            # Don't return the full results list (could be huge)
            del result["results"]
            return result

        return ctx.start(
            "sharpness",
            work,
            pausable=True,
            config={
                "collection_id": collection_id,
            },
        )

    @blueprint.post("/api/jobs/cull")
    @background_job
    def api_job_cull(ctx):
        """Run culling analysis as a background job."""
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")
        db = get_db()
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err
        separate_file_types = body.get("separate_file_types", True)
        time_window = body.get("time_window", 60)
        phash_threshold = body.get("phash_threshold", 19)
        cross_bucket_merge = body.get("cross_bucket_merge", False)

        def work(job):
            from culling import analyze_for_culling

            thread_db = ctx.thread_db()

            def progress_cb(msg):
                ctx.runner.push_event(
                    job["id"],
                    "progress",
                    {
                        "current": 0,
                        "total": 0,
                        "current_file": msg,
                        "rate": 0,
                        "phase": "Culling analysis",
                    },
                )

            progress_cb("Analyzing photos for culling...")

            result = analyze_for_culling(
                thread_db,
                collection_id=collection_id,
                separate_file_types=separate_file_types,
                time_window=time_window,
                phash_threshold=phash_threshold,
                cross_bucket_merge=cross_bucket_merge,
                progress_callback=progress_cb,
                vireo_dir=vireo_dir(),
                pause_callback=lambda: ctx.checkpoint(job),
            )

            # Store culling results in a temporary cache for the UI
            ctx.checkpoint(job)
            cache_path = os.path.join(
                os.path.dirname(db_path), f"culling_results_ws{ctx.workspace_id}.json"
            )
            with open(cache_path, "w") as f:
                json.dump(result, f)

            return {
                "total_photos": result["total_photos"],
                "suggested_keepers": result["suggested_keepers"],
                "suggested_rejects": result["suggested_rejects"],
                "species_count": len(result["species_groups"]),
                "photos_missing_phash": result.get("photos_missing_phash", 0),
            }

        return ctx.start(
            "cull", work, pausable=True, config={"collection_id": collection_id},
        )

    @blueprint.post("/api/jobs/regroup")
    @background_job
    def api_job_regroup(ctx):
        """Run pipeline stages 2-6 (grouping + scoring + triage) from cached features.

        This is fast (seconds) — no model inference, just math on stored features.
        Requires extract-masks to have been run first.
        """
        body = request.get_json(silent=True) or {}
        collection_id = body.get("collection_id")
        db = get_db()
        err = reject_visual_collection(db, collection_id, json_error=json_error)
        if err is not None:
            return err

        import config as cfg

        effective_cfg = db.get_effective_config(cfg.load())
        pipeline_cfg = effective_cfg.get("pipeline", {})

        def work(job):
            from pipeline import (
                load_photo_features,
                run_full_pipeline,
                save_results,
            )

            thread_db = ctx.thread_db()

            ctx.runner.set_steps(job["id"], [
                {"id": "load", "label": "Load features"},
                {"id": "group", "label": "Group encounters & bursts"},
                {"id": "save", "label": "Save results"},
            ])

            ctx.runner.update_step(job["id"], "load", status="running")
            ctx.runner.push_event(
                job["id"],
                "progress",
                {"phase": "Loading features from database", "current": 0, "total": 3},
            )

            photos = load_photo_features(thread_db, collection_id=collection_id, config=effective_cfg)
            if not photos:
                ctx.runner.update_step(job["id"], "load", status="failed",
                                       error="No photos with pipeline features found")
                return {"error": "No photos with pipeline features found. Run extract-masks first."}
            ctx.runner.update_step(job["id"], "load", status="completed",
                                   summary=f"{len(photos)} photos")

            ctx.runner.update_step(job["id"], "group", status="running")
            ctx.runner.push_event(
                job["id"],
                "progress",
                {"phase": "Grouping encounters and bursts", "current": 1, "total": 3},
            )

            # emit_trace=True so the pipeline-review sidebar's algorithm-trace
            # panel can show per-cut-point details for each encounter on the
            # very first load (not only after the user drags a live-tuning
            # slider). Cost is negligible (~300B per adjacent pair).
            if ctx.runner.is_cancelled(job["id"]):
                return {}
            results = run_full_pipeline(photos, config=pipeline_cfg, emit_trace=True)
            summary = results.get("summary", {})
            ctx.runner.update_step(job["id"], "group", status="completed",
                                   summary=f"{summary.get('encounters', 0)} encounters")

            ctx.runner.update_step(job["id"], "save", status="running")
            ctx.runner.push_event(
                job["id"],
                "progress",
                {"phase": "Saving results", "current": 2, "total": 3},
            )

            if ctx.runner.is_cancelled(job["id"]):
                return {}
            cache_dir = os.path.dirname(db_path)
            save_results(results, cache_dir, ctx.workspace_id)
            ctx.runner.update_step(job["id"], "save", status="completed")

            return results["summary"]

        return ctx.start("regroup", work, config={"pipeline": pipeline_cfg}, pausable=True)

    return blueprint
