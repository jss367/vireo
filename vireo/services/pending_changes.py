"""Queue XMP sidecar work for keyword and location edits, and retire it.

Routes that change a photo's keywords or assigned location record the
matching sidecar write as a pending change. These helpers keep the queue
minimal: an add cancels a still-pending remove of the same keyword (and
vice versa) instead of stacking both, and a location edit replaces any
earlier queued location write. Every function takes the request
``Database`` explicitly; ``_commit=False`` lets a caller batch several
edits into its own transaction.

The sidecar sync consumes the queue through the second half of this module:
``claim_pending_changes_for_sync`` marks the rows it is about to write, and
``clear_pending`` / ``clear_pending_by_token`` / ``remove_pending_change_token``
retire them once written (by immutable token where a reused rowid could name
a replacement). ``Database.queue_change`` and ``remove_pending_changes`` stay
on ``Database``: repositories bind them by name.

Import jobs use their own thread-safe equivalents in ``web.imports``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from keyword_normalization import normalize_keyword_display

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from db import Database


def queue_keyword_add(
    db: Database, photo_id: int, keyword_name: str, workspace_id: int | None = None,
    _commit: bool = True,
) -> None:
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
    db.pending_changes.clear_equivalent_flat_removals(
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


def queue_keyword_remove(
    db: Database, photo_id: int, keyword_name: str, workspace_id: int | None = None,
    _commit: bool = True,
) -> None:
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


def queue_location_sync_if_enabled(
    db: Database, photo_id: int, workspace_id: int | None = None, _commit: bool = True,
) -> None:
    """Queue GPS sidecar sync or cleanup work for location edits."""
    if workspace_id is None:
        if not db.workspace_folders.photo_in_workspace(photo_id, db.require_workspace_id()):
            return
    elif not db.workspace_folders.photo_in_workspace(photo_id, workspace_id):
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


def claim_pending_changes_for_sync(
    db: Database, changes: Sequence[Mapping[str, Any]],
) -> list[Any]:
    """Mark selected edits as possibly written and return surviving rows.

    A cancelled keyword must leave an opposing edit once a writer can
    have seen it. Persist that fact before any filesystem work, including
    across failed writes or a process restart. Match immutable tokens so
    a cancellation between selection and this claim cannot resurrect the
    old edit or claim a replacement that reused its rowid.
    """
    if not changes:
        return []
    return db.pending_changes.claim_for_sync(changes)


def mark_sync_started(
    db: Database, photo_id: int, workspace_id: int, change_type: str, value: str,
) -> None:
    """Retain captured-write repair status inside the caller's transaction."""
    db.pending_changes.mark_sync_started(photo_id, workspace_id, change_type, value)


def remove_pending_change_token(db: Database, change_token: str | None) -> int:
    """Delete a single pending change by immutable token. Returns rows removed."""
    if not change_token:
        return 0
    return db.pending_changes.remove_token(change_token)


def clear_pending(
    db: Database, change_ids: Sequence[int], *, clear_equivalent_flat_removals: bool = False,
    expected_tokens: Sequence[str | None] | None = None, _commit: bool = True,
) -> None:
    """Delete pending changes by id.

    ``_commit=False`` leaves the delete and any equivalent flat removals
    in the caller's transaction, allowing an atomic discard/history edit.

    When ``clear_equivalent_flat_removals`` is true, a successfully
    applied flat keyword removal also clears equivalent rows from sibling
    workspaces. The sidecar is global to the photo even though the review
    queue is workspace-scoped; leaving migration-generated duplicates in
    sibling queues would let a later sync replay a stale removal after the
    user had re-added the keyword.

    When ``expected_tokens`` is supplied (a list the same length as
    ``change_ids``), each delete is conditioned on the row's
    ``change_token`` matching too. That protects a concurrent sync from
    clobbering a replacement queued mid-run: the edit route deletes the
    selected pending row inside one transaction and immediately re-inserts,
    and SQLite reissues the freshly-freed rowid to the new row. A
    clear-by-id would drop that replacement without it ever being written
    to XMP -- for the pre-transfer sync, the stale sidecar would then
    travel to the NAS and the local original be removed. Rows predating
    the ``change_token`` column have a NULL token with no backfill, and
    for them the caller passes ``None`` in ``expected_tokens``; those fall
    back to id-only clearing scoped to null-token rows, so they keep
    exactly the exposure they always had.
    """
    if not change_ids:
        return
    repo = db.pending_changes
    synced_changes: list[Any] = repo.delete_by_ids(
        change_ids,
        clear_equivalent_flat_removals=clear_equivalent_flat_removals,
        expected_tokens=expected_tokens,
    )
    if synced_changes:
        repo.clear_equivalent_flat_removals(synced_changes, _commit=False)
    if _commit:
        repo.commit()


def clear_pending_by_token(
    db: Database, change_tokens: Sequence[str], *, clear_equivalent_flat_removals: bool = False,
) -> None:
    """Delete pending changes named by their immutable tokens.

    ``pending_changes.id`` is a bare ``INTEGER PRIMARY KEY``, so SQLite
    re-issues a deleted row's rowid to the next insert into the table. A
    caller that selected rows a while ago -- the sidecar sync, whose
    writes take as long as the storage does -- can find those ids now
    naming a *replacement* row: ``queue_flag_change_if_enabled`` deletes
    the old flag and inserts the new value, so clearing by id would throw
    away the edit the user just made, unwritten. A token is a fresh uuid
    per insert, so it names the row that was actually written.

    ``clear_equivalent_flat_removals`` behaves as in :func:`clear_pending`.
    """
    if not change_tokens:
        return
    repo = db.pending_changes
    synced_changes: list[Any] = repo.delete_by_tokens(
        change_tokens,
        clear_equivalent_flat_removals=clear_equivalent_flat_removals,
    )
    if synced_changes:
        repo.clear_equivalent_flat_removals(synced_changes, _commit=False)
    repo.commit()
