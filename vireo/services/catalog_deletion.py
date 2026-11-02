"""Removing photo and folder rows from the catalog.

``delete_photos`` deletes photo rows (optionally with their RAW+JPEG
companions) and everything that hangs off them, ``delete_folder`` deletes a
folder subtree and its photos unless another workspace still links to it, and
``prune_pipeline_cache_for_ids`` drops deleted ids from every workspace's
on-disk pipeline results. None of them touches files on disk other than that
cache: each returns the file list its caller's cleanup needs
(``cleanup_cached_files_for_deleted_photos``, Trash). ``delete_photos`` prunes
that cache itself only when its own unit of work committed the delete; inside
a caller's unit the caller prunes once that unit commits, with the ids of
every chunk it deleted.

The new-images cache is ``Database``'s process cache, so the invalidations go
through the façade's public invalidators, ordered against the commit:
``delete_photos`` invalidates the affected folders'
workspaces in a ``finally`` after the repository delete (so a rollback still
clears it), and ``delete_folder`` invalidates the active workspace after the
whole subtree delete has committed.
"""

from __future__ import annotations

import logging
import os
from functools import partial
from typing import TYPE_CHECKING, Any

from services import workspace_folders

if TYPE_CHECKING:
    from collections.abc import Iterable

    from db import Database
    from services.service_types import JsonDict

log = logging.getLogger(__name__)


def prune_pipeline_cache_for_ids(db: Database, ids: Iterable[int] | None) -> None:
    """Remove globally deleted ``ids`` from every workspace's results.

    Separate from ``delete_photos`` so chunked callers can defer it to
    after their outer transaction commits — pruning the on-disk cache
    is not transactional, so running it per-chunk would leave the cache
    permanently stripped of rows that a later rollback restores.
    """
    if not ids or db._db_path == ":memory:":
        return
    try:
        from services.result_files import prune_photo_results
        prune_photo_results(os.path.dirname(db._db_path), ids,
                            [workspace["id"] for workspace in db.workspaces.list_all()])
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, GeneratorExit)):
            raise
        log.exception("Failed to prune pipeline cache after delete")


def delete_photos(
    db: Database, photo_ids: Iterable[int], include_companions: bool = False,
) -> JsonDict:
    """Delete photos and all associated data, as one unit of work.

    Returns dict with 'deleted' count, 'ids' list of deleted photo IDs
    (post-companion-resolution), and 'files' list of
    {photo_id, folder_path, filename, companion_path} for file cleanup.

    Called with no unit open, this call owns its unit: it commits the
    delete and then prunes the pipeline review cache. Inside a caller's
    unit it is a savepoint, so a chunked caller's later failure rolls every
    chunk back together, and the non-DB cache prune is the caller's, once
    its unit has committed (``prune_pipeline_cache_for_ids`` with the
    returned ``ids``), so a rolled-back chunk never leaves a
    permanently-mutated cache file.
    """
    resolved = db.photos.resolve_for_delete(
        photo_ids, include_companions=include_companions,
    )
    rows = resolved.pop("_rows")
    if not rows:
        return {"deleted": 0, "ids": [], "files": []}
    all_ids = resolved["ids"]
    files = resolved["files"]

    # Count photos per folder for decrementing
    folder_counts: dict[int, int] = {}
    deleted_stems_by_folder: dict[int, set[str]] = {}
    folder_paths: dict[int, str] = {}
    for row in rows:
        folder_counts[row["folder_id"]] = folder_counts.get(row["folder_id"], 0) + 1
        deleted_stems_by_folder.setdefault(row["folder_id"], set()).add(
            os.path.splitext(row["filename"])[0]
        )
        folder_paths[row["folder_id"]] = row["folder_path"]

    # Collect affected folder ids BEFORE the delete so we can invalidate the
    # new-images cache even if the delete raises. In "Remove from Vireo"
    # mode the on-disk files stay put, so they become eligible for new-image
    # detection again the moment the photo rows are gone; without an
    # invalidation here, ``/api/workspaces/active/new-images`` would keep
    # serving the stale pre-delete ``new_count`` until the TTL expired.
    affected_folder_ids = list(folder_counts.keys())

    owns_unit = not db.in_unit_of_work
    try:
        with db.transaction():
            db.photos.delete(
                all_ids,
                folder_counts,
                deleted_stems_by_folder,
                folder_paths,
                workspace_id_fn=db.require_workspace_id,
            )
    finally:
        # Always invalidate — even on rollback we may have partially dirtied
        # state, and on success the removed rows mean untracked on-disk
        # files should re-surface as "new" on the next read.
        if affected_folder_ids:
            db.invalidate_new_images_cache_for_folders(affected_folder_ids)

    # Prune the pipeline review cache so deleted photos don't render as
    # blank cards on the pipeline review page, once the delete is durable.
    if owns_unit:
        prune_pipeline_cache_for_ids(db, all_ids)
    return {"deleted": len(all_ids), "ids": all_ids, "files": files}


def delete_folder(db: Database, folder_id: int) -> JsonDict:
    """Delete a folder, its descendant folders, and all their photos/data.

    Must cover the whole subtree: folders.parent_id has no ON DELETE
    action and the scanner registers every subdirectory with parent_id
    set, so deleting a non-leaf folder row alone trips the FK — and
    descendants of a deleted folder would be unreachable anyway. The
    subtree is collected by path prefix (``folders.subtree_ids_by_path``),
    not a parent_id walk, so legacy rows whose parent_id is NULL but
    whose path lives under the target are deleted too — the same shape
    the workspace link/unlink paths already handle.

    A folder with any ``workspace_folders`` link from a workspace
    other than the active one is never deleted — it is only unlinked
    from the active workspace. A foreign link, root or
    scanner-materialized, means the folder is still reachable in that
    workspace (an is_root = 0 link is always covered by a root
    ancestor there). That applies to the target folder itself (in
    which case nothing is deleted at all: the folder, its subtree,
    and its photos survive untouched and only the active workspace's
    links are removed) and to any descendant (the descendant's
    subtree and photos are preserved; its head is reparented to NULL
    because the parent row is going away). Deleting in one workspace
    must not destroy data still reachable in another.

    Returns dict with 'deleted_photos' count and 'files' (list from
    delete_photos) so the caller can remove cached thumbnails, previews,
    and working copies — the FK cascade drops preview_cache rows but
    leaves the on-disk files, which would otherwise become untracked
    orphans that eviction can't reclaim. When the target is protected
    by another workspace's link, that's {'deleted_photos': 0,
    'files': []}.
    """
    active_ws = db.require_workspace_id()
    if (active_ws is not None
            and not workspace_folders.workspace_has_folder_link(db, folder_id, active_ws)
            and any(
                row["id"] == active_ws
                for row in db.workspace_folders.list_workspaces_for_folder(folder_id)
            )):
        # Synthetic photo-grant membership cannot authorize a cascade.
        # Preserve catalog cleanup/unlink callers with no visible claim,
        # including physical ancestor aliases protected by foreign links.
        raise ValueError("Folder is not linked to the active workspace")

    def delete_chunk(chunk: Iterable[int], **kwargs: Any) -> JsonDict:
        return delete_photos(db, chunk, **kwargs)

    # One unit: a failure part-way cannot commit the photo deletes while
    # leaving the folder rows behind.
    with db.transaction():
        deleted_ids, files = db.folders.delete(
            folder_id,
            active_ws,
            delete_photos=delete_chunk,
            remember_removals=partial(workspace_folders.remember_workspace_folder_removals, db),
        )
    prune_pipeline_cache_for_ids(db, deleted_ids)

    # delete_photos' finally-clause invalidation only covers folders that
    # had photo rows. Photo-less deleted folders (whose untracked on-disk
    # files counted as "new") and kept subtrees unlinked from the active
    # workspace above would otherwise keep serving a stale new-images
    # count until the TTL expires. Deleted folders are only ever linked
    # in the active workspace (foreign links protect from deletion), so
    # invalidating it post-commit covers every affected workspace.
    if active_ws is not None:
        db.invalidate_new_images_cache_for_workspace(active_ws)

    return {"deleted_photos": len(deleted_ids), "files": files}
