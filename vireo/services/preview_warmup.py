"""Standalone preview warmup with tracked failures and worker connection cleanup.

The HTTP layer validates the selection and injects a runner plus a database
factory bound to the launching workspace. Rendering and cache adoption keep
using the same materializer and eviction path as before.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from collections.abc import Callable, Sequence
from contextlib import closing
from typing import TYPE_CHECKING

from artifact_flight import ArtifactProducerFailed
from config import config_value
from preview_cache import evict_if_over_quota as evict_preview_cache_if_over_quota
from preview_materializer import PreviewMaterializationError, materialize_preview

if TYPE_CHECKING:
    from sqlite3 import Row

    from db import Database
    from jobs import JobRunner
    from services.service_types import JobDict, JsonDict

log = logging.getLogger(__name__)

# Preserve a sample for job details without growing every checkpoint/history
# payload with the library. errors_total keeps the full count for the runner.
_MAX_ERROR_DETAILS = 50


def warm_previews(
    job: JobDict, vireo_dir: str, *, runner: JobRunner,
    open_db: Callable[[], Database], collection_id: int | None = None,
    photo_ids: Sequence[int] | None = None,
) -> JsonDict:
    """Warm workspace previews and close the worker database on every exit.

    ``open_db`` must open a new connection on the worker thread, bound to the
    launching workspace. No Flask application or request context is needed.
    """
    with closing(open_db()) as db:
        return _PreviewWarmup(
            job, db, vireo_dir, runner=runner,
            collection_id=collection_id, photo_ids=photo_ids,
        ).run()


class _PreviewWarmup:
    """The ``/api/jobs/previews`` work function and its run-wide state."""

    def __init__(
        self, job: JobDict, db: Database, vireo_dir: str, *, runner: JobRunner,
        collection_id: int | None, photo_ids: Sequence[int] | None,
    ) -> None:
        self.runner = runner
        self.job = job
        self.thread_db = db
        self.vireo_dir = vireo_dir
        self.collection_id = collection_id
        self.requested_photo_ids = photo_ids
        self.generated = 0
        self.skipped = 0
        self.failed = 0
        self.errors: list[JsonDict] = []

    def run(self) -> JsonDict:
        import config as cfg

        job = self.job
        runner = self.runner
        thread_db = self.thread_db
        vireo_dir = self.vireo_dir
        # Use workspace-effective config so per-workspace preview_max_size
        # overrides are honored — otherwise precompute warms the wrong
        # size and /photos/<id>/full (which uses workspace overrides) still
        # misses on first view.
        effective = thread_db.get_effective_config(cfg.load())
        raw_size = effective.get("preview_max_size")
        if raw_size == 0:
            # "Full resolution" — /full redirects to /original so
            # there's no size-suffixed file to warm. Skip precompute
            # rather than produce untracked {id}.jpg files.
            job["_start_time"] = time.time()
            return {**self._result(0), "note": "skipped (preview_max_size=0)"}
        self.max_size = int(raw_size or 1920)

        self.preview_quality = config_value(effective, "preview_quality")
        self.preview_dir = os.path.join(vireo_dir, "previews")
        os.makedirs(self.preview_dir, exist_ok=True)

        photos = self._photos()

        self.folders = {f["id"]: f["path"] for f in thread_db.folders.tree()}
        total = len(photos)
        job["_start_time"] = time.time()

        for i, photo in enumerate(photos):
            if runner.is_cancelled(job["id"]):
                break
            if not self._warm_photo(photo):
                continue

            runner.push_event(
                job["id"],
                "progress",
                {
                    "current": i + 1,
                    "total": total,
                    "current_file": photo["filename"],
                    "rate": round(
                        (i + 1) / max(time.time() - job["_start_time"], 0.01), 1
                    ),
                    "phase": "Generating previews",
                },
            )

        # Park a pause requested during the final iteration before the
        # eviction pass, which can unlink many previews and rewrite their
        # database rows. Without this checkpoint the accepted request
        # would remain ``pausing`` while eviction ran, and a cancel from
        # that state could leave the cache mid-eviction.
        if runner.is_cancelled(job["id"]):
            return self._result(total)

        # Run a single eviction pass at the end so the batch doesn't
        # fsync after every photo.
        evict_preview_cache_if_over_quota(thread_db, vireo_dir)

        return self._result(total)

    def _photos(self) -> list[Row]:
        thread_db = self.thread_db
        if self.requested_photo_ids is not None:
            photos: list[Row] = []
            for photo_id in self.requested_photo_ids:
                scoped_photo = thread_db.photos.get(
                    photo_id, verify_workspace=True,
                )
                if scoped_photo is not None:
                    photos.append(scoped_photo)
            return photos
        if self.collection_id:
            return thread_db.collections.get_photos(self.collection_id, per_page=999999)
        return thread_db.photos.list_page(per_page=999999)

    def _warm_photo(self, photo: Row) -> bool:
        """Warm one photo's preview.

        Returns False when the photo is finished without a progress event
        (an existing preview that is kept).
        """
        thread_db = self.thread_db
        detail_photo = thread_db.photos.get(photo["id"]) or photo
        cache_path = os.path.join(
            self.preview_dir, f'{photo["id"]}_{self.max_size}.jpg',
        )
        recipe = thread_db.get_photo_edit_recipe(photo["id"])
        if os.path.exists(cache_path) and self._keep_existing_preview(
            photo, cache_path, recipe,
        ):
            return False

        if not os.path.exists(cache_path):
            self._render_preview(photo, detail_photo, cache_path, recipe)
        return True

    def _keep_existing_preview(self, photo: Row, cache_path: str, recipe: JsonDict | None) -> bool:
        """Settle a preview file already on disk.

        Returns True when it is kept (counted skipped); False when an
        untracked edited preview was removed and must be re-rendered.
        """
        import contextlib

        thread_db = self.thread_db
        max_size = self.max_size
        cache_row = None
        try:
            cache_row = thread_db.caches.preview_get(photo["id"], max_size)
        except sqlite3.Error:
            # Treated as untracked: an edited photo's preview is
            # then re-rendered rather than trusted.
            log.warning(
                "Could not read the preview cache row for photo %s",
                photo["id"], exc_info=True,
            )
        if recipe and cache_row is None:
            with contextlib.suppress(OSError):
                os.remove(cache_path)
            if os.path.exists(cache_path):
                self.skipped += 1
                log.info(
                    "Skipping untracked edited preview for photo %s; "
                    "existing cache file could not be removed",
                    photo["id"],
                )
                return True
            return False
        self.skipped += 1
        # Adopt untracked unedited files so precompute output is
        # visible to eviction and /api/preview-cache.
        # Best-effort: photo may be deleted mid-job (FK error).
        try:
            if cache_row is None:
                thread_db.caches.preview_insert(
                    photo["id"],
                    max_size,
                    os.path.getsize(cache_path),
                )
        except (sqlite3.Error, OSError):
            log.debug(
                "Could not track existing preview for photo %s",
                photo["id"], exc_info=True,
            )
        return True

    def _render_preview(self, photo: Row, detail_photo: Row, cache_path: str, recipe: JsonDict | None) -> None:
        folder_path = self.folders.get(detail_photo["folder_id"])
        try:
            materialized = materialize_preview(
                self.thread_db,
                detail_photo,
                folder_path,
                size=self.max_size,
                vireo_dir=self.vireo_dir,
                preview_quality=self.preview_quality,
                recipe=recipe,
                cache_path=cache_path,
            )
        except (ArtifactProducerFailed, PreviewMaterializationError) as exc:
            # A source we could not read is a failure, not a
            # cache hit; counting it as skipped made an
            # all-unreadable run look fully cached.
            self.failed += 1
            if len(self.errors) < _MAX_ERROR_DETAILS:
                self.errors.append({
                    "photo_id": photo["id"], "filename": photo["filename"],
                    "error": str(exc),
                })
            log.warning(
                "Preview warmup failed for photo %s: %s",
                photo["id"], exc, exc_info=True,
            )
        else:
            if materialized.generated:
                self.generated += 1
            else:
                self.skipped += 1

    def _result(self, total: int) -> JsonDict:
        """Use the runner's verdict convention while retaining bounded details."""
        return {
            "generated": self.generated, "skipped": self.skipped,
            "failed": self.failed, "total": total,
            "ok": self.failed == 0, "errors": self.errors,
            "errors_total": self.failed,
        }
