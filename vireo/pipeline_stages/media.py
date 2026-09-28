"""Media stages for the streaming photo pipeline."""

import contextlib
import logging
import os
import queue
import sqlite3
import time

from artifact_flight import ArtifactProducerFailed
from camera_denoise import cache_matches as _camera_cache_matches
from db import commit_with_retry
from pipeline_stages.context import PipelineRun
from preview_materializer import (
    PreviewMaterializationError,
    PreviewSourceUnavailable,
    materialize_preview,
)
from render_source import (
    has_current_working_copy_failure as _has_current_working_copy_failure,
)
from render_source import (
    recipe_source_dimensions as _recipe_source_dimensions,
)

log = logging.getLogger(__name__)


def thumbnail_stage(
    run: PipelineRun,
    *,
    _RAW_EXTENSIONS,
    _SENTINEL,
    _filter_excluded,
    _recipe_render_source,
    _retry_thumbnail_with_companion,
    _retry_thumbnail_with_working_copy,
    _thumb_min_source_size_kwargs,
    _thumb_raw_decode_kwargs,
    effective_thumb_cache_dir,
    effective_vireo_dir,
    scan_to_thumb,
    skip_scan,
):
    run.stages["thumbnails"]["status"] = "running"
    run.runner.update_step(run.job["id"], "thumbnails", status="running")
    run.update_stages(run.runner, run.job["id"], run.stages)
    try:
        from thumbnails import (
            _is_working_copy_source,
            _retry_thumbnail_after_working_copy_eviction,
            generate_thumbnail,
        )

        thread_db = run.database_factory(run.db_path)
        thread_db.set_active_workspace(run.workspace_id)

        import config as cfg
        effective_cfg = thread_db.get_effective_config(cfg.load())
        thumb_size = effective_cfg.get("display", {}).get("thumbnail_size", 300)

        # Write thumbnails to the configured cache dir so custom
        # --thumb-dir layouts receive the files the Flask serve
        # route (reading from app.config["THUMB_CACHE_DIR"]) will
        # look for. Falls back to <db_dir>/thumbnails only when the
        # caller passed no explicit override.
        cache_dir = effective_thumb_cache_dir
        os.makedirs(cache_dir, exist_ok=True)

        generated = 0
        skipped = 0
        failed = 0
        failed_photos = []
        failure_detail_limit = 100

        def _record_failure(photo_id, photo_path, reason):
            if len(failed_photos) >= failure_detail_limit:
                return
            failed_photos.append({
                "id": photo_id,
                "filename": os.path.basename(photo_path or ""),
                "reason": reason,
            })

        # Mark photos.thumb_path so the dashboard's coverage query
        # (`thumb_path IS NOT NULL`) reflects each freshly-generated or
        # already-cached thumbnail. Batched so the writer lock isn't held
        # per-row under sustained scan throughput.
        THUMB_PATH_BATCH = 25
        pending_thumb_paths = []

        def _flush_thumb_paths():
            if pending_thumb_paths:
                thread_db.conn.executemany(
                    "UPDATE photos SET thumb_path=? WHERE id=?",
                    pending_thumb_paths,
                )
                commit_with_retry(thread_db.conn)
                pending_thumb_paths.clear()

        while True:
            # Continue draining after cancellation, but park before
            # taking another item when the whole pipeline is paused.
            run.control.pause_checkpoint()
            try:
                item = scan_to_thumb.get(timeout=1.0)
            except queue.Empty:
                # Keep draining even if abort is set -- we want thumbnails
                # for any photos already scanned. Only stop on sentinel.
                if run.control.should_abort(run.abort) and scan_to_thumb.empty():
                    break
                continue
            if item is _SENTINEL:
                break
            photo_id, photo_path = item
            try:
                thumb_path = os.path.join(cache_dir, f"{photo_id}.jpg")
                already_exists = os.path.exists(thumb_path)
                recipe = thread_db.get_photo_edit_recipe(photo_id)
                detail_photo = None
                if recipe:
                    detail_photo = thread_db.get_photo(photo_id)
                    if detail_photo:
                        folder_row = thread_db.get_folder(detail_photo["folder_id"])
                        folders = (
                            {folder_row["id"]: folder_row["path"]}
                            if folder_row else {}
                        )
                        photo_path = _recipe_render_source(
                            detail_photo,
                            recipe,
                            thumb_size,
                            effective_vireo_dir,
                            folders,
                        )
                        if (
                            os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                            and _has_current_working_copy_failure(
                                detail_photo,
                                effective_vireo_dir,
                                trust_existing_working_copy=False,
                                live_source_path=photo_path,
                                folder_path=folders.get(detail_photo["folder_id"]),
                            )
                        ):
                            failed += 1
                            _record_failure(
                                photo_id, photo_path,
                                "RAW decode previously failed and no "
                                "acceptable fallback is available",
                            )
                            run.stages["thumbnails"]["count"] = (
                                generated + skipped + failed
                            )
                            continue
                recipe_kwargs = {"recipe": recipe} if recipe else {}
                if recipe:
                    recipe_kwargs["camera_metadata"] = detail_photo
                    recipe_kwargs["native_size"] = (
                        _recipe_source_dimensions(detail_photo)
                    )
                raw_decode_kwargs = _thumb_raw_decode_kwargs(
                    detail_photo, recipe,
                )
                min_size_kwargs = _thumb_min_source_size_kwargs(
                    detail_photo, recipe, thumb_size, photo_path,
                )
                result_path = generate_thumbnail(
                    photo_id,
                    photo_path,
                    cache_dir,
                    size=thumb_size,
                    **recipe_kwargs,
                    **raw_decode_kwargs,
                    **min_size_kwargs,
                )
                if (
                    result_path is None
                    and detail_photo is not None
                    and _is_working_copy_source(
                        detail_photo,
                        photo_path,
                        effective_vireo_dir,
                    )
                ):
                    result_path, photo_path = (
                        _retry_thumbnail_after_working_copy_eviction(
                            detail_photo,
                            photo_path,
                            cache_dir,
                            thumb_size,
                            85,
                            recipe,
                            folders.get(detail_photo["folder_id"]),
                            effective_vireo_dir,
                        )
                    )
                if (
                    result_path is None
                    and detail_photo is not None
                    and os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                ):
                    result_path = _retry_thumbnail_with_companion(
                        thread_db, generate_thumbnail, detail_photo,
                        photo_id, photo_path, cache_dir, thumb_size,
                        recipe, folders.get(detail_photo["folder_id"]),
                    )
                if (
                    result_path is None
                    and detail_photo is not None
                    and os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                ):
                    result_path = _retry_thumbnail_with_working_copy(
                        thread_db, generate_thumbnail, detail_photo,
                        photo_id, photo_path, cache_dir, thumb_size,
                        recipe, effective_vireo_dir,
                    )
                if result_path is None:
                    failed += 1
                    _record_failure(
                        photo_id, photo_path,
                        "No acceptable thumbnail render source",
                    )
                elif already_exists:
                    skipped += 1
                    pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                else:
                    generated += 1
                    pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                if len(pending_thumb_paths) >= THUMB_PATH_BATCH:
                    _flush_thumb_paths()
            except Exception as exc:
                failed += 1
                _record_failure(photo_id, photo_path, str(exc))
                log.debug(
                    "Thumbnail failed for photo %s", photo_id,
                    exc_info=True,
                )
            # Include failed in the progress counter so the dashboard
            # reflects all work attempted, not just successes. Mixed
            # success/failure must not hide behind a 0/N progress bar.
            run.stages["thumbnails"]["count"] = generated + skipped + failed
            processed = generated + skipped + failed
            # Use scan count directly regardless of whether scan has
            # completed yet — this avoids the total staying at 0/? when
            # the thumbnail worker catches up with scan before scan's
            # status flips to "completed".
            scan_total = run.stages["scan"].get("count", 0)
            run.stages["thumbnails"]["total"] = scan_total
            run.runner.update_step(run.job["id"], "thumbnails",
                               current_file=os.path.basename(photo_path),
                               progress={"current": processed, "total": scan_total})
            elapsed = time.time() - run.job["_start_time"]
            rate = round(processed / max(elapsed, 0.01) * 60, 1)
            run.emit_progress(
                run.runner, run.job["id"], run.stages, "thumbnails", "Generating thumbnails",
                current_file=os.path.basename(photo_path),
                rate=rate,
            )

        # Collection mode: the scanner is skipped so the queue above was
        # empty. Iterate the collection's photos directly — mirrors the
        # pattern used by previews_stage — so replays against an existing
        # collection still regenerate any missing thumbs.
        if skip_scan and run.collection_id:
            coll_photos = _filter_excluded(
                thread_db.get_collection_photos(run.collection_id, per_page=999999)
            )
            folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
            total = len(coll_photos)
            for photo in coll_photos:
                if run.control.should_abort(run.abort):
                    break
                photo_id = photo["id"]
                folder_path = folders.get(photo["folder_id"], "")
                photo_path = os.path.join(folder_path, photo["filename"])
                thumb_path = os.path.join(cache_dir, f"{photo_id}.jpg")
                already_exists = os.path.exists(thumb_path)
                try:
                    recipe = thread_db.get_photo_edit_recipe(photo_id)
                    detail_photo = None
                    if recipe:
                        detail_photo = thread_db.get_photo(photo_id) or photo
                        photo_path = _recipe_render_source(
                            detail_photo,
                            recipe,
                            thumb_size,
                            effective_vireo_dir,
                            folders,
                        )
                        if (
                            os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                            and _has_current_working_copy_failure(
                                detail_photo,
                                effective_vireo_dir,
                                trust_existing_working_copy=False,
                                live_source_path=photo_path,
                                folder_path=folders.get(detail_photo["folder_id"]),
                            )
                        ):
                            failed += 1
                            _record_failure(
                                photo_id, photo_path,
                                "RAW decode previously failed and no "
                                "acceptable fallback is available",
                            )
                            run.stages["thumbnails"]["count"] = (
                                generated + skipped + failed
                            )
                            continue
                    recipe_kwargs = {"recipe": recipe} if recipe else {}
                    if recipe:
                        recipe_kwargs["camera_metadata"] = detail_photo
                        recipe_kwargs["native_size"] = (
                            _recipe_source_dimensions(detail_photo)
                        )
                    raw_decode_kwargs = _thumb_raw_decode_kwargs(
                        detail_photo, recipe,
                    )
                    min_size_kwargs = _thumb_min_source_size_kwargs(
                        detail_photo, recipe, thumb_size, photo_path,
                    )
                    result_path = generate_thumbnail(
                        photo_id,
                        photo_path,
                        cache_dir,
                        size=thumb_size,
                        **recipe_kwargs,
                        **raw_decode_kwargs,
                        **min_size_kwargs,
                    )
                    if (
                        result_path is None
                        and detail_photo is not None
                        and _is_working_copy_source(
                            detail_photo,
                            photo_path,
                            effective_vireo_dir,
                        )
                    ):
                        result_path, photo_path = (
                            _retry_thumbnail_after_working_copy_eviction(
                                detail_photo,
                                photo_path,
                                cache_dir,
                                thumb_size,
                                85,
                                recipe,
                                folder_path,
                                effective_vireo_dir,
                            )
                        )
                    if (
                        result_path is None
                        and os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                    ):
                        fallback_photo = detail_photo or photo
                        result_path = _retry_thumbnail_with_companion(
                            thread_db, generate_thumbnail, fallback_photo,
                            photo_id, photo_path, cache_dir, thumb_size,
                            recipe, folder_path,
                        )
                    if (
                        result_path is None
                        and detail_photo is not None
                        and os.path.splitext(photo_path)[1].lower() in _RAW_EXTENSIONS
                    ):
                        result_path = _retry_thumbnail_with_working_copy(
                            thread_db, generate_thumbnail, detail_photo,
                            photo_id, photo_path, cache_dir, thumb_size,
                            recipe, effective_vireo_dir,
                        )
                    if result_path is None:
                        failed += 1
                        _record_failure(
                            photo_id, photo_path,
                            "No acceptable thumbnail render source",
                        )
                    elif already_exists:
                        skipped += 1
                        pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                    else:
                        generated += 1
                        pending_thumb_paths.append((f"{photo_id}.jpg", photo_id))
                    if len(pending_thumb_paths) >= THUMB_PATH_BATCH:
                        _flush_thumb_paths()
                except Exception as exc:
                    failed += 1
                    _record_failure(photo_id, photo_path, str(exc))
                    log.debug(
                        "Thumbnail failed for photo %s", photo_id,
                        exc_info=True,
                    )
                run.stages["thumbnails"]["count"] = generated + skipped + failed
                run.stages["thumbnails"]["total"] = total
                processed = generated + skipped + failed
                run.runner.update_step(
                    run.job["id"], "thumbnails",
                    current_file=os.path.basename(photo_path),
                    progress={"current": processed, "total": total},
                )
                elapsed = time.time() - run.job["_start_time"]
                rate = round(processed / max(elapsed, 0.01) * 60, 1)
                run.emit_progress(
                    run.runner, run.job["id"], run.stages, "thumbnails", "Generating thumbnails",
                    current_file=os.path.basename(photo_path),
                    rate=rate,
                )

        # Flush any thumb_path updates from the final partial batch.
        _flush_thumb_paths()

        from thumbnails import format_summary as thumb_summary
        thumb_result = {
            "generated": generated,
            "skipped": skipped,
            "failed": failed,
        }
        if failed_photos:
            thumb_result["failed_photos"] = failed_photos
        if failed > len(failed_photos):
            thumb_result["failed_photos_truncated"] = (
                failed - len(failed_photos)
            )
        processed = generated + skipped + failed
        # Per-photo failures leave coverage gaps but do not invalidate
        # thumbnails that were generated successfully. Keep the stage
        # terminal and expose the affected photos as repair details;
        # fatal setup/runtime exceptions still take the except path.
        run.stages["thumbnails"]["status"] = "completed"
        run.stages["thumbnails"]["error_count"] = failed
        thumb_rollup = (
            f"{failed} of {processed} thumbnails need attention"
            if failed > 0 else None
        )
        if thumb_rollup:
            run.result.setdefault("warnings", []).append(
                f"[thumbnails] {thumb_rollup}"
            )
        run.runner.update_step(run.job["id"], "thumbnails", status="completed",
                           summary=thumb_summary(thumb_result),
                           error_count=failed,
                           error=thumb_rollup,
                           progress={"current": processed, "total": processed})
        run.result["stages"]["thumbnails"] = thumb_result
    except Exception as e:
        run.errors.append(f"[thumbnails] Fatal: {e}")
        log.exception("Pipeline thumbnail stage failed")
        run.stages["thumbnails"]["status"] = "failed"
        run.runner.update_step(run.job["id"], "thumbnails", status="failed", error=str(e))
        # This stage is the sole consumer of scan_to_thumb. Anything
        # above can raise BEFORE the drain loop (import, Database(),
        # cfg.load(), os.makedirs) — if we just returned, the scanner
        # would eventually block forever in put() once the queue fills,
        # wedging threads["scanner"].join() and leaking a pipeline slot
        # until restart. Set abort so the scanner stops producing, then
        # drain whatever is already queued. Stop at the sentinel, or
        # when the queue stays empty (the sentinel may already have
        # been consumed by the main loop before a late failure; with
        # abort set, photo_cb no longer blocks, so breaking on Empty
        # is safe).
        run.abort.set()
        while True:
            try:
                item = scan_to_thumb.get(timeout=1.0)
            except queue.Empty:
                break
            if item is _SENTINEL:
                break
    run.update_stages(run.runner, run.job["id"], run.stages)


def previews_stage(
    run: PipelineRun,
    *,
    _filter_excluded,
    effective_vireo_dir,
    skip_scan,
):
    """Generate preview images for browsed photos."""
    if run.abort.is_set():
        run.stages["previews"]["status"] = "skipped"
        run.runner.update_step(run.job["id"], "previews", status="completed",
                           summary="Skipped")
        return

    run.stages["previews"]["status"] = "running"
    run.runner.update_step(run.job["id"], "previews", status="running")
    run.update_stages(run.runner, run.job["id"], run.stages)

    try:
        import config as cfg

        thread_db = run.database_factory(run.db_path)
        thread_db.set_active_workspace(run.workspace_id)

        effective = thread_db.get_effective_config(cfg.load())
        raw_size = (
            run.params.preview_max_size
            if run.params.preview_max_size is not None
            else effective.get("preview_max_size", 1920)
        )
        if raw_size == 0:
            # "Full resolution" — /full redirects to /original, so
            # there's no size-suffixed file to warm. Skip rather
            # than produce untracked {id}.jpg files.
            run.runner.update_step(
                run.job["id"], "previews", status="completed",
                summary="Skipped (preview_max_size=0 → serves originals)",
            )
            run.stages["previews"]["status"] = "completed"
            return
        max_size = int(raw_size or 1920)
        preview_quality = effective.get("preview_quality", 90)
        # Must match the Flask serve convention — app.py reads, reaps,
        # and evicts previews under dirname(THUMB_CACHE_DIR)/previews.
        # Using dirname(db_path) here would, with a custom --thumb-dir,
        # warm previews (and preview_cache rows) under a root the app
        # never serves from.
        base_dir = effective_vireo_dir
        preview_dir = os.path.join(base_dir, "previews")
        os.makedirs(preview_dir, exist_ok=True)

        if run.collection_id:
            photos = _filter_excluded(thread_db.get_collection_photos(run.collection_id, per_page=999999))
        elif not skip_scan:
            # Scan ran but produced no photos — skip previews to avoid
            # unexpectedly processing the entire workspace.
            run.runner.update_step(run.job["id"], "previews", status="completed",
                               summary="Skipped (no photos scanned)")
            run.stages["previews"]["status"] = "completed"
            return
        else:
            photos = thread_db.get_photos(per_page=999999)

        folders = {f["id"]: f["path"] for f in thread_db.get_folder_tree()}
        total = len(photos)
        generated = 0
        skipped = 0
        failed = 0

        for i, photo in enumerate(photos):
            if run.control.should_abort(run.abort):
                break
            detail_photo = thread_db.get_photo(photo["id"]) or photo
            cache_path = os.path.join(preview_dir, f'{photo["id"]}_{max_size}.jpg')
            recipe = thread_db.get_photo_edit_recipe(photo["id"])
            if os.path.exists(cache_path):
                cache_row = None
                try:
                    cache_row = thread_db.preview_cache_get(photo["id"], max_size)
                except sqlite3.Error:
                    # Treated as untracked: an edited photo's preview is
                    # then re-rendered rather than trusted.
                    log.warning(
                        "Could not read the preview cache row for photo %s",
                        photo["id"], exc_info=True,
                    )
                if recipe and (cache_row is None or not _camera_cache_matches(cache_path, detail_photo, recipe)):
                    with contextlib.suppress(OSError):
                        os.remove(cache_path)
                    if os.path.exists(cache_path):
                        skipped += 1
                        log.info(
                            "Skipping untracked edited preview for photo %s; "
                            "existing cache file could not be removed",
                            photo["id"],
                        )
                        continue
                else:
                    skipped += 1
                    try:
                        if cache_row is None:
                            thread_db.preview_cache_insert(
                                photo["id"],
                                max_size,
                                os.path.getsize(cache_path),
                            )
                    except (sqlite3.Error, OSError):
                        # The photo (FK) or the file may have been deleted
                        # mid-pipeline; the preview itself is already on disk.
                        log.debug(
                            "Could not track existing preview for photo %s",
                            photo["id"], exc_info=True,
                        )
                    continue
            if not os.path.exists(cache_path):
                folder_path = folders.get(detail_photo["folder_id"])
                try:
                    materialized = materialize_preview(
                        thread_db,
                        detail_photo,
                        folder_path,
                        size=max_size,
                        vireo_dir=base_dir,
                        preview_quality=preview_quality,
                        recipe=recipe,
                        cache_path=cache_path,
                    )
                except PreviewSourceUnavailable as exc:
                    skipped += 1
                    log.info(
                        "Skipping pipeline preview for photo %s: %s",
                        photo["id"], exc,
                    )
                except ArtifactProducerFailed as exc:
                    if isinstance(exc.__cause__, PreviewSourceUnavailable):
                        skipped += 1
                        log.info(
                            "Skipping pipeline preview for photo %s: %s",
                            photo["id"], exc.__cause__,
                        )
                    else:
                        failed += 1
                except PreviewMaterializationError:
                    # image_loader logged the source failure; count it
                    # here so it remains visible in the stage rollup.
                    failed += 1
                else:
                    if materialized.generated:
                        generated += 1
                    else:
                        skipped += 1

            run.stages["previews"]["count"] = i + 1
            run.stages["previews"]["total"] = total
            run.runner.update_step(run.job["id"], "previews",
                               current_file=photo["filename"],
                               progress={"current": i + 1, "total": total})
            run.emit_progress(
                run.runner, run.job["id"], run.stages, "previews", "Generating previews",
                current_file=photo["filename"],
                rate=round(
                    (i + 1) / max(time.time() - run.job["_start_time"], 0.01) * 60, 1
                ),
            )

        # One eviction pass after the stage so preview_cache_max_mb is
        # enforced even when the pipeline is the only producer (e.g.
        # first-run ingest). Writes happen per-photo above to avoid
        # per-row fsyncs.
        from preview_cache import evict_if_over_quota
        evict_if_over_quota(thread_db, base_dir)

        run.result["stages"]["previews"] = {
            "generated": generated, "skipped": skipped, "failed": failed, "total": total
        }
        final_status = "failed" if failed > 0 else "completed"
        run.stages["previews"]["status"] = final_status
        previews_rollup = (
            f"[previews] {failed} of {total} previews failed to generate"
            if failed > 0 else None
        )
        if previews_rollup:
            run.errors.append(previews_rollup)
        summary_parts = [f"{generated} generated"]
        if skipped:
            summary_parts.append(f"{skipped} cached")
        if failed:
            summary_parts.append(f"{failed} failed")
        run.runner.update_step(run.job["id"], "previews", status=final_status,
                           summary=", ".join(summary_parts),
                           error_count=failed,
                           error=previews_rollup)
    except Exception as e:
        run.errors.append(f"[previews] Fatal: {e}")
        log.exception("Pipeline previews stage failed")
        run.stages["previews"]["status"] = "failed"
        run.runner.update_step(run.job["id"], "previews", status="failed", error=str(e))

    run.update_stages(run.runner, run.job["id"], run.stages)
