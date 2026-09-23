"""Dropping cached renders after an edit recipe changes.

When a photo's non-destructive edit recipe changes (edit, reset, undo/redo),
every cached derivative rendered from the old recipe is stale: thumbnails and
their paired-source / regeneration sidecars, sized previews, the
full-resolution originals cache, and external-edit caches.
``RenderCache.invalidate_photo_render_cache`` removes them. A preview file
that cannot be unlinked (a persistent lock) is recorded twice: in the
in-process ``invalid_preview_cache_paths`` set and in the durable
``preview_cache_invalidations`` table, so ``web.media``'s preview route
neither serves nor lazily re-adopts it. That route clears the marker once it
has rewritten the file, via ``clear_preview_cache_invalid``.

``RenderCache`` is the one stateful piece (it owns that set), so
``create_app`` builds a single instance and injects its bound methods.
``queue_edit_recipe_sync`` queues the recipe for XMP sync and is stateless.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import preview_cache

log = logging.getLogger(__name__)


def queue_edit_recipe_sync(db, photo_id, recipe_json, *, _commit=True):
    """Queue the current non-destructive edit recipe for XMP sync."""
    db.remove_pending_changes(
        photo_id, "edit_recipe", workspace_id=db._ws_id(), _commit=False,
    )
    db.queue_change(
        photo_id, "edit_recipe", recipe_json or "",
        workspace_id=db._ws_id(), _commit=False,
    )
    if _commit:
        db.conn.commit()


class RenderCache:
    """Per-app render-cache invalidation state and the routines that write it.

    ``config`` is the app's config mapping; ``THUMB_CACHE_DIR`` is read on
    every call so a test that repoints it after ``create_app`` is honored.
    """

    def __init__(self, config):
        self._config = config
        self.invalid_preview_cache_paths = set()

    # Canonical marker definitions live in preview_cache so the recycled-rowid
    # purge (which runs in the scanner, outside the app factory) writes the
    # same marker web.media's _serve_preview lazy-adoption branch consults.
    @staticmethod
    def mark_preview_cache_invalid(db, photo_id, size, *, commit=True):
        preview_cache.mark_preview_cache_invalid(db, photo_id, size, commit=commit)

    @staticmethod
    def clear_preview_cache_invalid(db, photo_id, size, *, commit=True):
        preview_cache.ensure_preview_cache_invalidations_table(db)
        db.conn.execute(
            "DELETE FROM preview_cache_invalidations WHERE photo_id=? AND size=?",
            (photo_id, size),
        )
        if commit:
            db.conn.commit()

    def invalidate_photo_render_cache(self, db, photo_ids):
        """Drop cached rendered derivatives after an edit recipe changes."""
        vireo_dir = os.path.dirname(self._config["THUMB_CACHE_DIR"])
        thumb_dir = self._config["THUMB_CACHE_DIR"]
        preview_dir = os.path.join(vireo_dir, "previews")
        originals_dir = os.path.join(vireo_dir, "originals")
        external_edits_dir = os.path.join(vireo_dir, "external-edits")
        for pid in photo_ids:
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
                db.conn.execute(
                    "UPDATE photos SET thumb_path = NULL WHERE id = ?", (pid,),
                )
            tracked_sizes = set()
            removed_preview_rows = []
            for row in db.conn.execute(
                "SELECT size FROM preview_cache WHERE photo_id = ?",
                (pid,),
            ).fetchall():
                size_value = row["size"]
                tracked_sizes.add(str(size_value))
                path = os.path.join(preview_dir, f"{pid}_{size_value}.jpg")
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except OSError:
                    if os.path.exists(path):
                        self.invalid_preview_cache_paths.add(path)
                        self.mark_preview_cache_invalid(
                            db, pid, size_value, commit=False,
                        )
                    log.warning(
                        "Failed to remove stale preview cache %s",
                        path, exc_info=True,
                    )
                else:
                    removed_preview_rows.append((pid, size_value))
                    self.clear_preview_cache_invalid(
                        db, pid, size_value, commit=False,
                    )
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
                            self.mark_preview_cache_invalid(
                                db, pid, size_part, commit=False,
                            )
                        log.warning(
                            "Failed to remove stale preview cache %s",
                            path, exc_info=True,
                        )
                    else:
                        self.clear_preview_cache_invalid(
                            db, pid, size_part, commit=False,
                        )
            except FileNotFoundError:
                pass
            if removed_preview_rows:
                db.conn.executemany(
                    "DELETE FROM preview_cache WHERE photo_id = ? AND size = ?",
                    removed_preview_rows,
                )
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
        db.conn.commit()
