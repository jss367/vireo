"""Deleting a workspace.

``delete_workspace`` removes the workspace row (its scoped data cascades),
deletes the workspace's on-disk pipeline results, and drops its new-images
cache entry through the façade. ``Database.create_workspace`` stays on
``Database``: ``__init__`` creates the Default workspace through
``ensure_default_workspace``, and ``db.py`` never imports a service.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db import Database


def delete_workspace(db: Database, workspace_id: int) -> list[str]:
    """Delete a workspace and all its scoped data (cascade).

    Returns the result-file cleanup errors (empty when every file went).
    """
    with db.transaction():
        db.workspaces.delete(workspace_id)
    cleanup_errors: list[str] = []
    if db._db_path != ":memory:":
        from services.result_files import delete_workspace_results
        cleanup_errors = delete_workspace_results(os.path.dirname(db._db_path), workspace_id)
    # Drop any cached new-images payload for this workspace. Without this,
    # if the deleted id is later reused by SQLite for a new workspace,
    # ``get_new_images_for_workspace`` could serve the prior workspace's
    # data until TTL expiry.
    db.invalidate_new_images_cache_for_workspace(workspace_id)
    return cleanup_errors
