"""Dropping cached renders after an edit recipe changes.

When a photo's non-destructive edit recipe changes (edit, reset, undo/redo),
every cached derivative rendered from the old recipe is stale: thumbnails and
their paired-source / regeneration sidecars, sized previews, the
full-resolution originals cache, and external-edit caches.
``RenderCache.invalidate_photo_render_cache`` removes them. A preview file
that cannot be unlinked (a persistent lock) is recorded twice: in the
in-process ``invalid_preview_cache_paths`` set and in the durable
``preview_cache_invalidations`` table, so the preview route in ``web.media_previews``
neither serves nor lazily re-adopts it. That route clears the marker once it
has rewritten the file, via ``clear_preview_cache_invalid``.

``RenderCache`` is the one stateful piece (it owns that set), so
``create_app`` builds a single instance and injects its bound methods.
``queue_edit_recipe_sync`` queues the recipe for XMP sync and is stateless.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

import preview_cache
from app_paths import media_root

if TYPE_CHECKING:
    from db import Database

log = logging.getLogger(__name__)


def queue_edit_recipe_sync(db: Database, photo_id: int, recipe_json: str | None) -> None:
    """Queue the current non-destructive edit recipe for XMP sync (no commit)."""
    db.remove_pending_changes(
        photo_id, "edit_recipe", workspace_id=db.require_workspace_id(),
    )
    db.queue_change(
        photo_id, "edit_recipe", recipe_json or "",
        workspace_id=db.require_workspace_id(),
    )


def _drop_stale_thumbnails(db: Database, pid: int, thumb_dir: str) -> None:
    """Unlink ``pid``'s thumbnail, paired-source variants and regen sidecars."""
    thumb_cache = os.path.join(thumb_dir, f"{pid}.jpg")
    clear_thumb_path = True
    try:
        if os.path.exists(thumb_cache):
            os.remove(thumb_cache)
    except OSError:
        clear_thumb_path = not os.path.exists(thumb_cache)
        log.warning(
            "Failed to remove stale thumbnail cache %s", thumb_cache,
            exc_info=True,
        )
    for source in ("raw", "jpeg"):
        variant = os.path.join(thumb_dir, f"{pid}_{source}.jpg")
        try:
            if os.path.exists(variant):
                os.remove(variant)
        except OSError:
            log.warning(
                "Failed to remove stale paired-source thumbnail %s",
                variant,
                exc_info=True,
            )
    # ``<pid>_regen.jpg`` / ``<pid>_raw_regen.jpg`` /
    # ``<pid>_jpeg_regen.jpg`` are the sidecars ``serve_thumbnail``
    # falls back to when the default couldn't be unlinked (a
    # persistent lock). Without this pass, editing the recipe
    # while the default stays locked leaves the sidecar carrying
    # the pre-edit pixels; the freshness gate there compares
    # against the unchanged source mtime and re-serves them.
    for stem in (f"{pid}", f"{pid}_raw", f"{pid}_jpeg"):
        sidecar = os.path.join(thumb_dir, f"{stem}_regen.jpg")
        try:
            if os.path.exists(sidecar):
                os.remove(sidecar)
        except OSError:
            log.warning(
                "Failed to remove stale regeneration sidecar %s",
                sidecar,
                exc_info=True,
            )
    if clear_thumb_path:
        db.caches.clear_thumbnail_path(pid)


def _drop_stale_originals_and_external_edits(pid: int, originals_dir: str, external_edits_dir: str) -> None:
    """Unlink ``pid``'s full-resolution renders and external-editor handoff."""
    original_paths = [
        os.path.join(originals_dir, f"{pid}.jpg"),
        *Path(originals_dir).glob(f"{pid}_*.jpg"),
    ]
    for original_path in original_paths:
        try:
            if os.path.exists(original_path):
                os.remove(original_path)
        except OSError:
            log.warning(
                "Failed to remove stale original cache %s",
                original_path,
            )
    external_cache = os.path.join(external_edits_dir, f"{pid}.jpg")
    external_meta = os.path.join(external_edits_dir, f"{pid}.json")
    for path in (external_cache, external_meta):
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            log.warning(
                "Failed to remove stale external edit cache %s",
                path, exc_info=True,
            )


class RenderCache:
    """Per-app render-cache invalidation state and the routines that write it.

    ``config`` is the app's config mapping; ``THUMB_CACHE_DIR`` is read on
    every call so a test that repoints it after ``create_app`` is honored.
    """

    # Any: the Flask app config holds heterogeneous settings values.
    def __init__(self, config: Mapping[str, Any]) -> None:
        self._config = config
        self.invalid_preview_cache_paths: set[str] = set()

    # Canonical marker definitions live in preview_cache so the recycled-rowid
    # purge (which runs in the scanner, outside the app factory) writes the
    # same marker consulted by the lazy-adoption branch in
    # web.media_previews._PreviewServer._serve_preview.
    @staticmethod
    def mark_preview_cache_invalid(db: Database, photo_id: int, size: int | str) -> None:
        """Record the "do not adopt" marker (no commit: the caller's unit owns it)."""
        preview_cache.mark_preview_cache_invalid(db, photo_id, size)

    @staticmethod
    def clear_preview_cache_invalid(db: Database, photo_id: int, size: int | str) -> None:
        """Clear the "do not adopt" marker in its own unit of work."""
        with db.transaction():
            db.caches.clear_preview_invalidation(photo_id, size)

    def invalidate_photo_render_cache(self, db: Database, photo_ids: Iterable[int]) -> None:
        """Drop cached rendered derivatives after an edit recipe changes."""
        vireo_dir = media_root(self._config["THUMB_CACHE_DIR"])
        thumb_dir = self._config["THUMB_CACHE_DIR"]
        preview_dir = os.path.join(vireo_dir, "previews")
        originals_dir = os.path.join(vireo_dir, "originals")
        external_edits_dir = os.path.join(vireo_dir, "external-edits")
        with db.transaction():
            for pid in photo_ids:
                _drop_stale_thumbnails(db, pid, thumb_dir)
                self._drop_stale_previews(db, pid, preview_dir)
                _drop_stale_originals_and_external_edits(
                    pid, originals_dir, external_edits_dir,
                )

    def _drop_stale_previews(self, db: Database, pid: int, preview_dir: str) -> None:
        """Unlink ``pid``'s sized previews, marking any that can't be removed."""
        tracked_sizes: set[str] = set()
        removed_preview_rows: list[tuple[int, int]] = []
        for row in db.caches.preview_sizes(pid):
            size_value = row["size"]
            tracked_sizes.add(str(size_value))
            path = os.path.join(preview_dir, f"{pid}_{size_value}.jpg")
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError:
                if os.path.exists(path):
                    self.invalid_preview_cache_paths.add(path)
                    self.mark_preview_cache_invalid(db, pid, size_value)
                log.warning(
                    "Failed to remove stale preview cache %s",
                    path, exc_info=True,
                )
            else:
                removed_preview_rows.append((pid, size_value))
                db.caches.clear_preview_invalidation(pid, size_value)
        try:
            for name in os.listdir(preview_dir):
                if not (name.startswith(f"{pid}_") and name.endswith(".jpg")):
                    continue
                size_part = name[len(f"{pid}_"):-4]
                if size_part in tracked_sizes:
                    continue
                path = os.path.join(preview_dir, name)
                try:
                    os.remove(path)
                except OSError:
                    if os.path.exists(path):
                        self.invalid_preview_cache_paths.add(path)
                        self.mark_preview_cache_invalid(db, pid, size_part)
                    log.warning(
                        "Failed to remove stale preview cache %s",
                        path, exc_info=True,
                    )
                else:
                    db.caches.clear_preview_invalidation(pid, size_part)
        except FileNotFoundError:
            pass
        if removed_preview_rows:
            db.caches.delete_preview_rows(removed_preview_rows)
