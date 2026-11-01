"""Capture reversible location assignments and exact prediction decisions.

Legacy audit rows lack the original locations and prediction IDs. New actions
use separate types so replay never guesses at those old entries' before-state.
"""

from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Any, TypedDict, cast

from services.pending_changes import queue_location_sync_if_enabled

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from db import Database
    from services.grouping_history import CacheRestore


class HistoryItem(TypedDict):
    """One per-photo before/after row of an ``edit_history`` entry."""

    photo_id: int
    old_value: str
    new_value: str


class _RefusedEdit(Exception):
    """Carries an error response out of the unit so the unit rolls back."""

    def __init__(self, result: object) -> None:
        super().__init__()
        self.result = result


def atomic_edit[**P, R](
    get_db: Callable[[], Database],
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Keep setters, sidecar work and history in one unit of work.

    An error response (status 400 or above) rolls the unit back, whatever it
    had written before deciding to refuse.
    """
    def decorate(fn: Callable[P, R]) -> Callable[P, R]:
        @wraps(fn)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            db = get_db()
            try:
                with db.transaction():
                    result = fn(*args, **kwargs)
                    status = result[1] if isinstance(result, tuple) else getattr(result, 'status_code', 200)
                    if status >= 400:
                        raise _RefusedEdit(result)
            except _RefusedEdit as refused:
                return cast('R', refused.result)
            return result
        return wrapped
    return decorate


def change_photo_location(
    db: Database, photo_id: int, keyword_id: int | None = None,
) -> HistoryItem | None:
    """Apply an assignment/clear and return its full before/after history item."""
    if not db.in_transaction:
        db.begin_immediate()
    old_value = db.edit_history.photo_location_state(photo_id)
    if keyword_id is None:
        db.locations.clear_photo(photo_id)
    else:
        db.set_photo_location(photo_id, keyword_id)
    new_value = db.edit_history.photo_location_state(photo_id)
    # The saved place may have gained coordinates before this assignment.
    # Its unchanged links still need a sidecar refresh, without a new undo step.
    queue_location_sync_if_enabled(db, photo_id, _commit=False)
    if old_value == new_value:
        return None
    return {'photo_id': photo_id, 'old_value': old_value, 'new_value': new_value}


def prediction_status_item(
    db: Database, photo_id: int, prediction_ids: Sequence[int], apply: Callable[[], object],
) -> HistoryItem | None:
    """Record precisely the rows a status-only decision changes."""
    db.edit_history.ensure_prediction_instances(prediction_ids)
    old_value = db.edit_history.prediction_status_state(prediction_ids)
    apply()
    new_value = db.edit_history.prediction_status_state(prediction_ids)
    if old_value == new_value:
        return None
    return {'photo_id': photo_id, 'old_value': old_value, 'new_value': new_value}

# -- Undo / redo ----------------------------------------------------------
#
# The dispatch lives here, not on ``Database``: the cache-linked actions
# (``pipeline_grouping``, ``species_confirm_cache``) and the
# ``identity_alternatives`` replay belong to services the data layer may not
# import. Plain per-photo actions still replay through
# ``Database._apply_undo`` / ``_apply_redo``.


def undo_last_edit(db: Database, *, cache_restore: CacheRestore | None = None) -> dict[str, Any] | None:
    """Undo the most recent undoable edit. Returns the undone entry dict, or None.

    Non-undoable entries (prediction_reject, discard) are skipped.
    The entry is marked as undone (not deleted) so it can be redone.

    Stale cache-linked actions are retired and reported without applying
    another entry. The caller must refresh the controls so the next click
    names the edit it will actually reverse. Retirement commits on its own;
    valid combined photo/group edits retain the writer lock until complete.

    ``cache_restore`` keeps the pipeline-cache restoration and regroup lock
    alive through the caller's outer unit-of-work commit.
    """
    found = db.edit_history.next_undo()
    if not found:
        return None
    entry, items = found
    history = db.edit_history

    if entry['action_type'] == 'pipeline_grouping':
        from services.grouping_history import (
            GroupingHistoryStale,
            apply_grouping_photo_edit,
            restore_grouping_edit,
        )

        try:
            with restore_grouping_edit(db, entry, undo=True, cache_restore=cache_restore):
                apply_grouping_photo_edit(db, entry, items, undo=True)
                history.mark_undone(entry['id'])
        except GroupingHistoryStale:
            history.retire_stale_grouping_entry(entry['id'])
            history.commit()
            raise GroupingHistoryStale(
                'That grouping or species action was superseded by newer analysis. '
                'History has been refreshed; review the next action before trying again.'
            ) from None
    elif entry['action_type'] == 'species_confirm_cache':
        from services.grouping_history import (
            GroupingHistoryStale,
            restore_species_confirm_cache_edit,
        )

        try:
            with restore_species_confirm_cache_edit(db, entry, undo=True, cache_restore=cache_restore):
                history.mark_undone(entry['id'])
        except GroupingHistoryStale:
            history.retire_stale_grouping_entry(entry['id'])
            history.commit()
            raise GroupingHistoryStale(
                'That grouping or species action was superseded by newer analysis. '
                'History has been refreshed; review the next action before trying again.'
            ) from None
    elif entry['action_type'] == 'identity_alternatives':
        _replay_identity_history(db, entry, items, undo=True)
    else:
        with db.transaction():
            db._apply_undo(entry, items)
            history.mark_undone(entry['id'])
    return entry


def redo_last_undo(db: Database, *, cache_restore: CacheRestore | None = None) -> dict[str, Any] | None:
    """Redo the most recently undone edit. Returns the entry dict, or None.

    Replays in chronological order (ASC) so sequential undos are redone
    correctly. Stale cache-linked actions are retired and reported without
    replaying another entry, just as in ``undo_last_edit``.

    ``cache_restore``: see ``undo_last_edit``.
    """
    found = db.edit_history.next_redo()
    if not found:
        return None
    entry, items = found
    history = db.edit_history

    if entry['action_type'] == 'pipeline_grouping':
        from services.grouping_history import (
            GroupingHistoryStale,
            apply_grouping_photo_edit,
            restore_grouping_edit,
        )

        try:
            with restore_grouping_edit(db, entry, undo=False, cache_restore=cache_restore):
                apply_grouping_photo_edit(db, entry, items, undo=False)
                history.mark_redone(entry['id'])
        except GroupingHistoryStale:
            history.retire_stale_grouping_entry(entry['id'])
            history.commit()
            raise GroupingHistoryStale(
                'That grouping or species action was superseded by newer analysis. '
                'History has been refreshed; review the next action before trying again.'
            ) from None
    elif entry['action_type'] == 'species_confirm_cache':
        from services.grouping_history import (
            GroupingHistoryStale,
            restore_species_confirm_cache_edit,
        )

        try:
            with restore_species_confirm_cache_edit(db, entry, undo=False, cache_restore=cache_restore):
                history.mark_redone(entry['id'])
        except GroupingHistoryStale:
            history.retire_stale_grouping_entry(entry['id'])
            history.commit()
            raise GroupingHistoryStale(
                'That grouping or species action was superseded by newer analysis. '
                'History has been refreshed; review the next action before trying again.'
            ) from None
    elif entry['action_type'] == 'identity_alternatives':
        _replay_identity_history(db, entry, items, undo=False)
    else:
        with db.transaction():
            db._apply_redo(entry, items)
            history.mark_redone(entry['id'])
    return entry


def _replay_identity_history(
    db: Database, entry: dict[str, Any], items: Sequence[Any], *, undo: bool,
) -> None:
    """Retire exactly this stale action while retaining the writer lock."""
    from services.identity_alternatives import IdentityHistoryStale

    history = db.edit_history
    stale = False
    try:
        with db.transaction():
            try:
                with history.identity_replay():
                    if undo:
                        _apply_identity_items(db, items, undo=True)
                        history.mark_undone(entry['id'])
                    else:
                        _apply_identity_items(db, items, undo=False)
                        history.mark_redone(entry['id'])
            except IdentityHistoryStale:
                history.retire_superseded_identity_entry(entry['id'])
                stale = True
    finally:
        # SQL savepoint rollback cannot undo Python-side snapshots. A
        # failed/retired replay must never contaminate a later edit.
        db.species_provenance.forget_snapshots()
    if stale:
        raise IdentityHistoryStale(
            'That identification was changed elsewhere. History has been refreshed; '
            'review the next action before trying again.'
        ) from None


def _apply_identity_items(db: Database, items: Sequence[Any], *, undo: bool) -> None:
    """Replay an ``identity_alternatives`` entry's photo identities and evidence."""
    from services.identity_alternatives import replay_identity_batch

    replay_identity_batch(db, items, undo=undo)
    # Identity snapshots already restore their retargeted authorship
    # lattice; the extra replay only owns species evidence here.
    db.species_provenance.replay_history(items, undo=undo, restore_source=False)
