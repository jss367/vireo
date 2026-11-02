"""Relocating a folder row, and merging a missing folder into the row that tracks its path.

A merge collapses two catalogs' views of the same files: each photo of the
missing folder either folds into the target's photo of the same name, moves
to the target folder, or (when its file is gone) is dropped as a phantom. A
fold carries everything that keys on the losing photo id onto the survivor,
across domains, in this order:

1. the shared identity (``identity_alternatives.prepare_photo_merge``), which
   can refuse with ``IdentityMergeConflict``;
2. the GPS-review decisions (``db.moves_merge.transfer_gps_review``);
3. the keyword associations, folding provenance
   (``db.folders.carry_merged_photo_keywords``);
4. the embedded-keyword offers
   (``db.photo_keywords.transfer_embedded_offered_keys``).

``FolderRepository.finish_merge`` then deletes the folded and phantom rows
and the source folder and cascades to missing children. Nothing here
commits: a refusal raised part-way through leaves rows moved, so a caller
that catches it runs the merge in a savepoint of its own
(``services.folder_relocation.relocate_folder``); one that lets it propagate
rolls back with it (the local-copy restores).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from repositories.folders import OriginalPathReturned
from services.identity_alternatives import prepare_photo_merge

if TYPE_CHECKING:
    from db import Database


def relocate(db: Database, folder_id: int, new_path: str) -> list[dict[str, Any]]:
    """Point ``folder_id`` at ``new_path`` and mark it 'ok' (no commit).

    If ``new_path`` is already tracked by another folder, a *missing* folder
    is merged into that one (``merge_into_existing``); if the missing
    folder's own path is back on disk, raises ``OriginalPathReturned``
    without writing; any other conflict is a ``ValueError``. A refusal can
    be raised part-way through the merge, so run this in a savepoint.

    Returns the child folder dicts that were also relocated.
    """
    conflict = db.folders.path_conflict(folder_id, new_path)
    if conflict:
        # Only merge if source folder is missing; for ok folders, reject
        source_row = db.folders.status_and_path(folder_id)
        if source_row and source_row["status"] == "missing":
            # Revalidate: if the original path came back, refuse without
            # writing; the caller refreshes the status (``mark_ok``).
            if os.path.isdir(source_row["path"]):
                raise OriginalPathReturned(
                    f"Path is already tracked as folder {conflict['id']}"
                )
            return merge_into_existing(db, folder_id, conflict["id"], new_path)
        raise ValueError(
            f"Path is already tracked as folder {conflict['id']}"
        )
    return db.folders.relocate_unclaimed(folder_id, new_path)


def merge_into_existing(
    db: Database, source_folder_id: int, target_folder_id: int, new_path: str,
) -> list[dict[str, Any]]:
    """Merge photos from a missing folder into an existing folder at the same path.

    - Photos with matching filenames in the target are folded into them and
      dropped from the source
    - Other photos are reassigned to the target folder
    - The source folder entry is deleted
    - Missing child folders are cascade-relocated using old_path -> new_path

    Returns list of child folder dicts that were also relocated (same as
    ``relocate``). Never commits.
    """
    source_row = db.folders.status_and_path(source_folder_id)
    old_path = source_row["path"] if source_row else ""

    # Reassign or drop each photo
    drop_ids = []
    # Where each dropped id's collection memberships go: the duplicate
    # that absorbs it, or nowhere for a phantom.
    collection_remap: dict[int, int | None] = {}
    for photo in db.folders.photos_in_folder(source_folder_id):
        existing = db.folders.photo_named(target_folder_id, photo["filename"])
        if existing:
            _fold_photo(db, photo["id"], existing["id"])
            drop_ids.append(photo["id"])
            collection_remap[photo["id"]] = existing["id"]
        elif os.path.exists(os.path.join(new_path, photo["filename"])):
            db.folders.move_photo_to_folder(photo["id"], target_folder_id)
        else:
            # File doesn't exist on disk at target — drop phantom record
            drop_ids.append(photo["id"])
            collection_remap[photo["id"]] = None

    return db.folders.finish_merge(
        source_folder_id, target_folder_id, new_path, old_path,
        drop_ids, collection_remap,
    )


def _fold_photo(db: Database, losing_id: int, survivor_id: int) -> None:
    """Carry one merged-away photo's identity, reviews, keywords and offers onto the survivor."""
    prepare_photo_merge(db, losing_id, survivor_id)
    db.moves_merge.transfer_gps_review(losing_id, survivor_id)
    db.folders.carry_merged_photo_keywords(losing_id, survivor_id)
    # Move the embedded-offered suppression onto the surviving row so a
    # user's keyword removal is not resurrected by a later rescan of the
    # same embedded value. The non-cascading FK on
    # ``photo_embedded_keyword_offered.photo_id`` would also abort the
    # ``DELETE FROM photos`` without this.
    db.photo_keywords.transfer_embedded_offered_keys(losing_id, survivor_id)
