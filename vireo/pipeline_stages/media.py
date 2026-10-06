"""Media stages for the streaming photo pipeline."""

import contextlib
import logging
import os
import queue
import sqlite3
import time
from dataclasses import dataclass

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
    thumbs = _ThumbPass(
        run,
        raw_extensions=_RAW_EXTENSIONS,
        sentinel=_SENTINEL,
        filter_excluded=_filter_excluded,
        recipe_render_source=_recipe_render_source,
        retry_thumbnail_with_companion=_retry_thumbnail_with_companion,
        retry_thumbnail_with_working_copy=_retry_thumbnail_with_working_copy,
        thumb_min_source_size_kwargs=_thumb_min_source_size_kwargs,
        thumb_raw_decode_kwargs=_thumb_raw_decode_kwargs,
        effective_thumb_cache_dir=effective_thumb_cache_dir,
        effective_vireo_dir=effective_vireo_dir,
        scan_to_thumb=scan_to_thumb,
    )
    try:
        thumbs.setup()
        thumbs.drain_scan_queue()

        # Collection mode: the scanner is skipped so the queue above was
        # empty. Iterate the collection's photos directly — mirrors the
        # pattern used by previews_stage — so replays against an existing
        # collection still regenerate any missing thumbs.
        if skip_scan and run.collection_id:
            thumbs.thumbnail_collection()

        thumbs.finish()
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


# Mark photos.thumb_path so the dashboard's coverage query
# (`thumb_path IS NOT NULL`) reflects each freshly-generated or
# already-cached thumbnail. Batched so the writer lock isn't held
# per-row under sustained scan throughput.
_THUMB_PATH_BATCH = 25
_FAILURE_DETAIL_LIMIT = 100


@dataclass
class _ThumbPhoto:
    """One photo being thumbnailed: its render source and edit recipe."""

    photo_id: int
    photo_path: str
    already_exists: bool = False
    recipe: object = None
    detail_photo: object = None
    folders: dict = None


class _ThumbPass:
    """State shared across one thumbnails stage run."""

    def __init__(
        self,
        run,
        *,
        raw_extensions,
        sentinel,
        filter_excluded,
        recipe_render_source,
        retry_thumbnail_with_companion,
        retry_thumbnail_with_working_copy,
        thumb_min_source_size_kwargs,
        thumb_raw_decode_kwargs,
        effective_thumb_cache_dir,
        effective_vireo_dir,
        scan_to_thumb,
    ):
        self.run = run
        self.raw_extensions = raw_extensions
        self.sentinel = sentinel
        self.filter_excluded = filter_excluded
        self.recipe_render_source = recipe_render_source
        self.retry_thumbnail_with_companion = retry_thumbnail_with_companion
        self.retry_thumbnail_with_working_copy = retry_thumbnail_with_working_copy
        self.thumb_min_source_size_kwargs = thumb_min_source_size_kwargs
        self.thumb_raw_decode_kwargs = thumb_raw_decode_kwargs
        self.effective_thumb_cache_dir = effective_thumb_cache_dir
        self.effective_vireo_dir = effective_vireo_dir
        self.scan_to_thumb = scan_to_thumb

        self.generated = 0
        self.skipped = 0
        self.failed = 0
        self.failed_photos = []
        self.pending_thumb_paths = []

    def setup(self):
        from thumbnails import (
            _is_working_copy_source,
            _retry_thumbnail_after_working_copy_eviction,
            generate_thumbnail,
        )

        self.is_working_copy_source = _is_working_copy_source
        self.retry_thumbnail_after_working_copy_eviction = (
            _retry_thumbnail_after_working_copy_eviction
        )
        self.generate_thumbnail = generate_thumbnail

        self.thread_db = self.run.database_factory(self.run.db_path)
        self.thread_db.set_active_workspace(self.run.workspace_id)

        import config as cfg
        effective_cfg = self.thread_db.get_effective_config(cfg.load())
        self.thumb_size = effective_cfg.get("display", {}).get("thumbnail_size", 300)

        # Write thumbnails to the configured cache dir so custom
        # --thumb-dir layouts receive the files the Flask serve
        # route (reading from app.config["THUMB_CACHE_DIR"]) will
        # look for. Falls back to <db_dir>/thumbnails only when the
        # caller passed no explicit override.
        self.cache_dir = self.effective_thumb_cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

    # -- bookkeeping -------------------------------------------------

    def _record_failure(self, photo_id, photo_path, reason):
        if len(self.failed_photos) >= _FAILURE_DETAIL_LIMIT:
            return
        self.failed_photos.append({
            "id": photo_id,
            "filename": os.path.basename(photo_path or ""),
            "reason": reason,
        })

    def _flush_thumb_paths(self):
        if self.pending_thumb_paths:
            self.thread_db.conn.executemany(
                "UPDATE photos SET thumb_path=? WHERE id=?",
                self.pending_thumb_paths,
            )
            commit_with_retry(self.thread_db.conn)
            self.pending_thumb_paths.clear()

    def _report_progress(self, photo_path, total):
        # Include failed in the progress counter so the dashboard
        # reflects all work attempted, not just successes. Mixed
        # success/failure must not hide behind a 0/N progress bar.
        processed = self.generated + self.skipped + self.failed
        self.run.stages["thumbnails"]["count"] = processed
        self.run.stages["thumbnails"]["total"] = total
        self.run.runner.update_step(
            self.run.job["id"], "thumbnails",
            current_file=os.path.basename(photo_path),
            progress={"current": processed, "total": total},
        )
        elapsed = time.time() - self.run.job["_start_time"]
        rate = round(processed / max(elapsed, 0.01) * 60, 1)
        self.run.emit_progress(
            self.run.runner, self.run.job["id"], self.run.stages, "thumbnails",
            "Generating thumbnails",
            current_file=os.path.basename(photo_path),
            rate=rate,
        )

    # -- scan queue --------------------------------------------------

    def drain_scan_queue(self):
        run = self.run
        while True:
            # Continue draining after cancellation, but park before
            # taking another item when the whole pipeline is paused.
            run.control.pause_checkpoint()
            try:
                item = self.scan_to_thumb.get(timeout=1.0)
            except queue.Empty:
                # Keep draining even if abort is set -- we want thumbnails
                # for any photos already scanned. Only stop on sentinel.
                if run.control.should_abort(run.abort) and self.scan_to_thumb.empty():
                    break
                continue
            if item is self.sentinel:
                break
            photo_id, photo_path = item
            thumb = _ThumbPhoto(photo_id, photo_path)
            try:
                if not self._thumbnail_scanned_photo(thumb):
                    continue
            except Exception as exc:
                self.failed += 1
                self._record_failure(thumb.photo_id, thumb.photo_path, str(exc))
                log.debug(
                    "Thumbnail failed for photo %s", thumb.photo_id,
                    exc_info=True,
                )
            # Use scan count directly regardless of whether scan has
            # completed yet — this avoids the total staying at 0/? when
            # the thumbnail worker catches up with scan before scan's
            # status flips to "completed".
            scan_total = run.stages["scan"].get("count", 0)
            self._report_progress(thumb.photo_path, scan_total)

    def _still_owns(self, photo_id, canonical_path):
        """True if the catalog's current row at ``photo_id`` still names
        the folder + filename behind ``canonical_path``.

        SQLite reuses the row ids of deleted photos. A paired JPEG's
        transient row is inserted during one scanner invocation and
        then deleted by pairing at end-of-scan; _scan_in_place iterates
        sources with one do_scan per source, so a later invocation can
        insert an unrelated photo under the same reused id. Comparing
        only the basename would miss the case where the replacement
        photo happens to share the companion's filename but lives in
        a different folder, so this validates folder + filename
        together — the two columns that identify a row for pairing
        and for ``_canonical_photo_path`` at queue time. ``canonical``
        here means the owner's path (the RAW path for pairs), captured
        before any render-source mutation; the retry paths rewrite
        ``thumb.photo_path`` to a companion or working-copy path and
        validating against that would false-reject a live row.
        """
        filenames = self.thread_db.get_photo_filenames([photo_id])
        entry = filenames.get(photo_id)
        if entry is None:
            return False
        folder_id, filename = entry
        if os.path.basename(canonical_path) != filename:
            return False
        folder = self.thread_db.get_folder(folder_id)
        if folder is None:
            return False
        # ``Database.get_folder`` returns a ``sqlite3.Row`` whose
        # columns are the SELECT list; use subscript access (``Row``
        # supports ``__getitem__`` but not ``dict.get``). A missing or
        # NULL path column — unusual, but survivable — leaves the
        # catalog without a way to reconstruct the owner path, so
        # treat it as not-owned rather than silently proceed.
        try:
            folder_path = folder["path"]
        except (KeyError, IndexError):
            return False
        if not folder_path:
            return False
        catalog_path = os.path.normpath(os.path.join(folder_path, filename))
        return os.path.normpath(canonical_path) == catalog_path

    def _thumbnail_scanned_photo(self, thumb):
        """Thumbnail one photo taken from the scan queue.

        Returns False when the queue entry is stale (its photo_id was
        reused by a different file after the entry was queued, or
        before/after thumbnail generation) or when the photo was
        already counted as failed, so the per-photo progress update
        must be skipped.
        """
        # The retry paths (`_resolve_recipe_source`,
        # `_retry_after_working_copy_eviction`) rewrite
        # ``thumb.photo_path`` to a companion or working-copy render
        # source. Capture the canonical owner path once up front so
        # the pre- and post-generation ownership checks both compare
        # against what was queued, not what is being rendered — else
        # the post-check would delete a valid thumbnail whenever a
        # retry path diverged from the owner's filename.
        canonical_path = thumb.photo_path
        # Pre-generation ownership guard: a stale (id, companion_path)
        # entry queued by a transient JPEG row that pairing then
        # deleted must not touch the cache. Caching {id}.jpg from the
        # stale companion's bytes under a reused id would pin the
        # deleted companion's pixels to the new photo and cause the
        # new row's own queue entry to skip on a pre-populated cache
        # file.
        if not self._still_owns(thumb.photo_id, canonical_path):
            return False
        thumb_path = os.path.join(self.cache_dir, f"{thumb.photo_id}.jpg")
        thumb.already_exists = os.path.exists(thumb_path)
        thumb.recipe = self.thread_db.get_photo_edit_recipe(thumb.photo_id)
        if thumb.recipe:
            thumb.detail_photo = self.thread_db.get_photo(thumb.photo_id)
            if thumb.detail_photo:
                folder_row = self.thread_db.get_folder(thumb.detail_photo["folder_id"])
                thumb.folders = (
                    {folder_row["id"]: folder_row["path"]}
                    if folder_row else {}
                )
                if not self._resolve_recipe_source(thumb, thumb.folders):
                    return False
        result_path = self._generate(thumb)
        detail_photo = thumb.detail_photo
        if (
            result_path is None
            and detail_photo is not None
            and self.is_working_copy_source(
                detail_photo,
                thumb.photo_path,
                self.effective_vireo_dir,
            )
        ):
            result_path = self._retry_after_working_copy_eviction(
                thumb, thumb.folders.get(detail_photo["folder_id"]),
            )
        if (
            result_path is None
            and detail_photo is not None
            and self._is_raw(thumb.photo_path)
        ):
            result_path = self._retry_with_companion(
                thumb, detail_photo, thumb.folders.get(detail_photo["folder_id"]),
            )
        result_path = self._retry_with_working_copy(thumb, result_path)
        # Post-generation ownership re-check against the ORIGINAL
        # canonical path, not ``thumb.photo_path`` (which the retry
        # paths may have rewritten). Ownership can change during
        # ``generate_thumbnail`` — pairing commits a row delete and
        # the next scanner invocation inserts under the reused id —
        # so if the row at this id no longer names the owner we
        # queued, delete the cache file we just published; the new
        # row's own queue entry will regenerate from the correct
        # source.
        if (
            result_path is not None
            and not self._still_owns(thumb.photo_id, canonical_path)
        ):
            with contextlib.suppress(OSError):
                os.remove(result_path)
            return False
        self._tally(thumb, result_path)
        return True

    # -- collection replay -------------------------------------------

    def thumbnail_collection(self):
        run = self.run
        coll_photos = self.filter_excluded(
            self.thread_db.get_collection_photos(run.collection_id, per_page=999999)
        )
        folders = {f["id"]: f["path"] for f in self.thread_db.get_folder_tree()}
        total = len(coll_photos)
        for photo in coll_photos:
            if run.control.should_abort(run.abort):
                break
            photo_id = photo["id"]
            folder_path = folders.get(photo["folder_id"], "")
            thumb = _ThumbPhoto(photo_id, os.path.join(folder_path, photo["filename"]))
            thumb_path = os.path.join(self.cache_dir, f"{photo_id}.jpg")
            thumb.already_exists = os.path.exists(thumb_path)
            try:
                if not self._thumbnail_collection_photo(thumb, photo, folders, folder_path):
                    continue
            except Exception as exc:
                self.failed += 1
                self._record_failure(thumb.photo_id, thumb.photo_path, str(exc))
                log.debug(
                    "Thumbnail failed for photo %s", thumb.photo_id,
                    exc_info=True,
                )
            self._report_progress(thumb.photo_path, total)

    def _thumbnail_collection_photo(self, thumb, photo, folders, folder_path):
        """Thumbnail one photo of the replayed collection.

        Returns False when the photo was already counted as failed and
        the per-photo progress update must be skipped.
        """
        thumb.recipe = self.thread_db.get_photo_edit_recipe(thumb.photo_id)
        if thumb.recipe:
            thumb.detail_photo = self.thread_db.get_photo(thumb.photo_id) or photo
            if not self._resolve_recipe_source(thumb, folders):
                return False
        result_path = self._generate(thumb)
        detail_photo = thumb.detail_photo
        if (
            result_path is None
            and detail_photo is not None
            and self.is_working_copy_source(
                detail_photo,
                thumb.photo_path,
                self.effective_vireo_dir,
            )
        ):
            result_path = self._retry_after_working_copy_eviction(
                thumb, folder_path,
            )
        if (
            result_path is None
            and self._is_raw(thumb.photo_path)
        ):
            fallback_photo = detail_photo or photo
            result_path = self._retry_with_companion(
                thumb, fallback_photo, folder_path,
            )
        result_path = self._retry_with_working_copy(thumb, result_path)
        self._tally(thumb, result_path)
        return True

    # -- per-photo render --------------------------------------------

    def _is_raw(self, path):
        return os.path.splitext(path)[1].lower() in self.raw_extensions

    def _resolve_recipe_source(self, thumb, folders):
        """Point an edited photo at its recipe render source.

        Returns False, after counting the photo as failed, when that
        source is a RAW whose decode previously failed with no
        acceptable fallback.
        """
        thumb.photo_path = self.recipe_render_source(
            thumb.detail_photo,
            thumb.recipe,
            self.thumb_size,
            self.effective_vireo_dir,
            folders,
        )
        if (
            self._is_raw(thumb.photo_path)
            and _has_current_working_copy_failure(
                thumb.detail_photo,
                self.effective_vireo_dir,
                trust_existing_working_copy=False,
                live_source_path=thumb.photo_path,
                folder_path=folders.get(thumb.detail_photo["folder_id"]),
            )
        ):
            self.failed += 1
            self._record_failure(
                thumb.photo_id, thumb.photo_path,
                "RAW decode previously failed and no "
                "acceptable fallback is available",
            )
            self.run.stages["thumbnails"]["count"] = (
                self.generated + self.skipped + self.failed
            )
            return False
        return True

    def _generate(self, thumb):
        recipe = thumb.recipe
        detail_photo = thumb.detail_photo
        recipe_kwargs = {"recipe": recipe} if recipe else {}
        if recipe:
            recipe_kwargs["camera_metadata"] = detail_photo
            recipe_kwargs["native_size"] = (
                _recipe_source_dimensions(detail_photo)
            )
        raw_decode_kwargs = self.thumb_raw_decode_kwargs(
            detail_photo, recipe,
        )
        min_size_kwargs = self.thumb_min_source_size_kwargs(
            detail_photo, recipe, self.thumb_size, thumb.photo_path,
        )
        return self.generate_thumbnail(
            thumb.photo_id,
            thumb.photo_path,
            self.cache_dir,
            size=self.thumb_size,
            **recipe_kwargs,
            **raw_decode_kwargs,
            **min_size_kwargs,
        )

    def _retry_after_working_copy_eviction(self, thumb, folder_path):
        result_path, thumb.photo_path = (
            self.retry_thumbnail_after_working_copy_eviction(
                thumb.detail_photo,
                thumb.photo_path,
                self.cache_dir,
                self.thumb_size,
                85,
                thumb.recipe,
                folder_path,
                self.effective_vireo_dir,
            )
        )
        return result_path

    def _retry_with_companion(self, thumb, fallback_photo, folder_path):
        return self.retry_thumbnail_with_companion(
            self.thread_db, self.generate_thumbnail, fallback_photo,
            thumb.photo_id, thumb.photo_path, self.cache_dir, self.thumb_size,
            thumb.recipe, folder_path,
        )

    def _retry_with_working_copy(self, thumb, result_path):
        if (
            result_path is None
            and thumb.detail_photo is not None
            and self._is_raw(thumb.photo_path)
        ):
            result_path = self.retry_thumbnail_with_working_copy(
                self.thread_db, self.generate_thumbnail, thumb.detail_photo,
                thumb.photo_id, thumb.photo_path, self.cache_dir, self.thumb_size,
                thumb.recipe, self.effective_vireo_dir,
            )
        return result_path

    def _tally(self, thumb, result_path):
        if result_path is None:
            self.failed += 1
            self._record_failure(
                thumb.photo_id, thumb.photo_path,
                "No acceptable thumbnail render source",
            )
        elif thumb.already_exists:
            self.skipped += 1
            self.pending_thumb_paths.append((f"{thumb.photo_id}.jpg", thumb.photo_id))
        else:
            self.generated += 1
            self.pending_thumb_paths.append((f"{thumb.photo_id}.jpg", thumb.photo_id))
        if len(self.pending_thumb_paths) >= _THUMB_PATH_BATCH:
            self._flush_thumb_paths()

    # -- finish ------------------------------------------------------

    def finish(self):
        run = self.run
        # Flush any thumb_path updates from the final partial batch.
        self._flush_thumb_paths()

        generated, skipped, failed = self.generated, self.skipped, self.failed
        failed_photos = self.failed_photos
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
