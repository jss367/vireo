"""Queue XMP sidecar work for keyword and location edits.

Routes that change a photo's keywords or assigned location record the
matching sidecar write as a pending change. These helpers keep the queue
minimal: an add cancels a still-pending remove of the same keyword (and
vice versa) instead of stacking both, and a location edit replaces any
earlier queued location write. Every function takes the request
``Database`` explicitly; ``_commit=False`` lets a caller batch several
edits into its own transaction.

Import jobs use their own thread-safe equivalents in ``web.imports``.
"""

from keyword_normalization import normalize_keyword_display


def queue_keyword_add(db, photo_id, keyword_name, workspace_id=None, _commit=True):
    """Queue a keyword add unless it cancels a pending removal."""
    # Normalize before the cancellation lookup: queue_change normalizes
    # on insert, so pending values are stored in clean form and an
    # exact-match cancel against a raw variant would miss its pair.
    keyword_name = normalize_keyword_display(keyword_name)
    if not keyword_name:
        return
    removed = db.remove_pending_changes(
        photo_id, "keyword_remove", keyword_name,
        workspace_id=workspace_id, _commit=_commit,
    )
    # A migration-generated flat removal is obsolete as soon as the user
    # explicitly re-adds that term. Clear it across every workspace that
    # owns the shared sidecar; otherwise "Use XMP" can filter the term
    # out and detach this fresh association before the add is written.
    db.clear_equivalent_flat_removals(
        [{
            "photo_id": photo_id,
            "change_type": "keyword_remove_flat",
            "value": keyword_name,
        }],
        _commit=_commit,
    )
    if removed == 0:
        db.queue_change(
            photo_id, "keyword_add", keyword_name,
            workspace_id=workspace_id, _commit=_commit,
        )


def queue_keyword_remove(db, photo_id, keyword_name, workspace_id=None, _commit=True):
    """Queue a keyword removal unless it cancels a pending add."""
    # See queue_keyword_add: keep the cancellation lookup in the same
    # normalized form queue_change stores.
    keyword_name = normalize_keyword_display(keyword_name)
    if not keyword_name:
        return
    removed = db.remove_pending_changes(
        photo_id, "keyword_add", keyword_name,
        workspace_id=workspace_id, _commit=_commit,
    )
    if removed == 0:
        db.queue_change(
            photo_id, "keyword_remove", keyword_name,
            workspace_id=workspace_id, _commit=_commit,
        )


def queue_location_sync_if_enabled(db, photo_id, workspace_id=None, _commit=True):
    """Queue GPS sidecar sync or cleanup work for location edits."""
    if workspace_id is None:
        if not db._photo_in_workspace(photo_id):
            return
    elif db.conn.execute(
        """SELECT 1 FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           WHERE p.id = ? AND wf.workspace_id = ?""",
        (photo_id, workspace_id),
    ).fetchone() is None:
        return
    # Queue even when assigned-location writes are disabled so sync can
    # remove stale Vireo-authored GPS previously written while enabled.
    db.remove_pending_changes(
        photo_id, "location", workspace_id=workspace_id, _commit=_commit,
    )
    db.queue_change(
        photo_id, "location", "effective",
        workspace_id=workspace_id, _commit=_commit,
    )
