"""Photo membership for folder-scoped Process requests."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Set

    from db import Database


def folder_photo_ids(
    db: Database, folder_ids: list[int], excluded_paths: Set[str] = frozenset(),
    excluded_photo_ids: Set[int] = frozenset(),
) -> list[int] | None:
    """Resolve visible subtree photos, or None if a selected root is unavailable."""
    workspace = db.active_workspace_id
    descendants: set[int] = set()
    for fid in folder_ids:
        if not db.workspace_folders.visible_ids(workspace, [fid]):
            return None
        descendants.update(db.folders.subtree_ids(fid))
        # Legacy descendants may have a NULL parent despite living under the root.
        descendants.update(db.workspace_folders.visible_ids(workspace, db.folders.subtree_ids_by_path(fid)))
    folders = list(descendants)
    paths = db.folders.paths_by_id(folders) if excluded_paths else {}
    photo_ids = []
    for row in db.photos.in_workspace_folders(folders, workspace):
        if row["id"] in excluded_photo_ids:
            continue
        if excluded_paths:
            path = os.path.join(paths.get(row["folder_id"]) or "", row["filename"] or "")
            if path in excluded_paths:
                continue
        photo_ids.append(row["id"])
    return photo_ids
