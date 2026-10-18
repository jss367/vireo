"""Dropping a photo's derived files when its source bytes change."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Collection

    from db import Database
    from scanner.scan_types import StrPath

log = logging.getLogger(__name__)


def _invalidate_raw_display_cache(vireo_dir: StrPath, photo_id: int) -> None:
    display_file = os.path.join(
        vireo_dir, "originals", f"{photo_id}.display.jpg",
    )
    if os.path.exists(display_file):
        try:
            os.remove(display_file)
        except OSError:
            log.debug(
                "Could not delete stale RAW display rendition %s",
                display_file,
                exc_info=True,
            )


def _invalidate_cached_thumbnails(
    db: Database, vireo_dir: StrPath, photo_id: int, thumb_cache_dir: StrPath | None,
) -> None:
    """Delete the photo's thumbnails and clear ``thumb_path`` to match."""
    thumb_dir = thumb_cache_dir or os.path.join(vireo_dir, "thumbnails")
    thumb_path = os.path.join(thumb_dir, f"{photo_id}.jpg")
    thumb_removed = False
    if os.path.exists(thumb_path):
        try:
            os.remove(thumb_path)
            thumb_removed = True
        except OSError:
            log.debug("Could not delete stale thumbnail %s", thumb_path, exc_info=True)
    else:
        # File already gone but the column may still point at it (e.g.
        # external cache wipe). Keep the column in sync so the pipeline
        # planner's "thumb_path IS NULL" gate matches disk reality.
        thumb_removed = True
    # Explicit RAW/JPEG pair views use source-specific thumbnail names. They
    # derive from the same source bytes and must be invalidated alongside the
    # legacy thumbnail, especially when a hash change preserves file mtime.
    for source in ("raw", "jpeg"):
        variant_path = os.path.join(thumb_dir, f"{photo_id}_{source}.jpg")
        if not os.path.exists(variant_path):
            continue
        try:
            os.remove(variant_path)
        except OSError:
            log.debug(
                "Could not delete stale paired thumbnail %s",
                variant_path,
                exc_info=True,
            )
    if thumb_removed:
        # Mirror the working_copy_path / preview_cache cleanup in
        # ``_invalidate_derived_caches``: any
        # path that drops the cached thumbnail file must also clear
        # photos.thumb_path so count_photos_missing_thumb (used by the
        # Thumbnails & Previews plan card) doesn't see a phantom "done"
        # state for a row whose JPEG no longer exists on disk.
        db.caches.clear_thumbnail_path(photo_id)


def _invalidate_derived_caches(
    db: Database, vireo_dir: StrPath | None, photo_id: int, thumb_cache_dir: StrPath | None = None,
) -> None:
    """Delete cached thumbnail / working copy / display / preview for a photo.

    Called when the scanner detects that an existing photo's source content
    has changed (different file_hash). Thumbnails, working copies, unedited
    full-resolution display renditions, and preview-pyramid sizes are all
    derived from the source bytes, so they're stale as soon as the source
    changes.

    Scope is intentionally O(1) per photo — untracked preview files
    (no preview_cache row) are handled by
    ``_sweep_untracked_previews_for_photos`` once at the end of
    ``scan()`` instead, so large rescans don't re-enumerate previews/
    for every invalidated photo (O(N × M) work).

    Also clears ``working_copy_path`` in the database so the scanner's
    working-copy extraction pass at the end of ``scan()`` picks this row
    back up and rebuilds the working copy.

    Requires an explicit ``vireo_dir``: DB path and cache root are
    independently configurable (--db vs --thumb-dir), so we can't guess
    the cache location from the DB. No-op when the caller omits it.

    ``thumb_cache_dir`` overrides the thumbnail location. ``--thumb-dir``
    may point to any directory name — it is not constrained to
    ``vireo_dir/thumbnails``. Callers that have the configured value
    (Flask routes, audit entry points) should pass it here or stale
    thumbs survive; ``previews/`` and ``working/`` are always siblings
    of ``vireo_dir`` by convention and need no override.
    """
    if not vireo_dir:
        return

    _invalidate_cached_thumbnails(db, vireo_dir, photo_id, thumb_cache_dir)

    wc_file = os.path.join(vireo_dir, "working", f"{photo_id}.jpg")
    if os.path.exists(wc_file):
        try:
            os.remove(wc_file)
        except OSError:
            log.debug("Could not delete stale working copy %s", wc_file, exc_info=True)
    # Also clear any failure markers: a content change is a meaningful
    # input change so the next backfill / scan extraction pass should retry,
    # not be permanently locked out by a stale failure record.
    db.working_copies.reset(photo_id)

    _invalidate_raw_display_cache(vireo_dir, photo_id)

    # Preview pyramid + its LRU accounting. Only drop a preview_cache row
    # for sizes whose file was successfully removed (or was already
    # missing): if unlink fails (e.g. Windows file lock) and we drop the
    # row anyway, the serve path's lazy-adoption shortcut re-adopts the
    # stranded file and hands out stale pre-change bytes. Mirrors the
    # self-healing semantics in preview_cache.evict_if_over_quota.
    preview_dir = os.path.join(vireo_dir, "previews")
    rows = db.caches.preview_sizes(photo_id)
    deleted_sizes = []
    for row in rows:
        size = row["size"]
        path = os.path.join(preview_dir, f"{photo_id}_{size}.jpg")
        try:
            os.remove(path)
        except FileNotFoundError:
            deleted_sizes.append(size)
        except OSError:
            log.debug("Could not delete stale preview %s", path, exc_info=True)
        else:
            deleted_sizes.append(size)

    if deleted_sizes:
        db.caches.delete_preview_rows([(photo_id, s) for s in deleted_sizes])


def _sweep_untracked_previews_for_photos(db: Database, vireo_dir: StrPath | None, photo_ids: Collection[int]) -> None:
    """Batched sweep of preview files with no preview_cache row.

    Legacy / orphan preview files (written by older code paths or left
    over from interrupted inserts) would be lazy-adopted on the next
    ``/photos/<id>/preview`` request and served as valid cache hits.
    After a content change that's stale data — the app serves
    pre-change bytes. We sweep them.

    Runs once per ``scan()`` call, enumerating ``previews/`` at most
    one time regardless of how many photos were invalidated. Files
    whose ``(photo_id, size)`` still has a live preview_cache row are
    preserved (row-driven cleanup in ``_invalidate_derived_caches``
    keeps rows when unlink fails, and we must not orphan those files
    here either).
    """
    if not vireo_dir or not photo_ids:
        return
    preview_dir = os.path.join(vireo_dir, "previews")
    if not os.path.isdir(preview_dir):
        return

    photo_ids_set = {int(p) for p in photo_ids}
    ids_list = list(photo_ids_set)
    # Chunk to stay under SQLITE_MAX_VARIABLE_NUMBER (default 999 on
    # older builds). Without this, a rescan that invalidates thousands
    # of photos crashes scan post-processing with "too many SQL variables".
    _CHUNK = 900
    still_tracked: set[tuple[int, int]] = set()
    for i in range(0, len(ids_list), _CHUNK):
        chunk = ids_list[i : i + _CHUNK]
        rows = db.caches.preview_keys_for_photos(chunk)
        still_tracked.update((r["photo_id"], r["size"]) for r in rows)

    try:
        entries = os.listdir(preview_dir)
    except OSError:
        return
    for fname in entries:
        if not fname.endswith(".jpg"):
            continue
        stem = fname[: -len(".jpg")]
        parts = stem.rsplit("_", 1)
        if len(parts) != 2:
            continue
        try:
            pid = int(parts[0])
            size = int(parts[1])
        except ValueError:
            continue
        if pid not in photo_ids_set:
            continue
        if (pid, size) in still_tracked:
            continue
        path = os.path.join(preview_dir, fname)
        try:
            os.remove(path)
        except OSError:
            log.debug("Could not delete untracked preview %s", path, exc_info=True)
