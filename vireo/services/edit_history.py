"""Capture reversible location assignments and exact prediction decisions.

Legacy audit rows lack the original locations and prediction IDs. New actions
use separate types so replay never guesses at those old entries' before-state.
"""

from __future__ import annotations

import contextlib
from functools import wraps
from typing import TYPE_CHECKING, Any, TypedDict, cast

from services import subject_verdicts
from services.pending_changes import queue_location_sync_if_enabled

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from db import Database
    from repositories.edit_history import EditHistoryRepository
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
            return run_atomic_edit(get_db(), lambda: fn(*args, **kwargs))
        return wrapped
    return decorate


def run_atomic_edit[R](db: Database, work: Callable[[], R], *, immediate: bool = False) -> R:
    """Run ``work()`` as one unit of work and return its response.

    ``atomic_edit`` for a body that is not a whole route. An error response
    (status 400 or above) rolls the unit back, whatever it had written before
    deciding to refuse; ``immediate`` takes the writer lock before the first
    read, as ``Database.transaction`` does.
    """
    try:
        with db.transaction(immediate=immediate):
            result = work()
            status = result[1] if isinstance(result, tuple) else getattr(result, 'status_code', 200)
            if status >= 400:
                raise _RefusedEdit(result)
    except _RefusedEdit as refused:
        return cast('R', refused.result)
    return result


def change_photo_location(
    db: Database, photo_id: int, keyword_id: int | None = None,
) -> HistoryItem | None:
    """Apply an assignment/clear and return its full before/after history item.

    Runs in the caller's unit of work, holding the writer lock from the
    before-state read through the write.
    """
    db.hold_writer_lock()
    old_value = db.edit_history.photo_location_state(photo_id)
    if keyword_id is None:
        db.locations.clear_photo(photo_id)
    else:
        db.set_photo_location(photo_id, keyword_id)
    new_value = db.edit_history.photo_location_state(photo_id)
    # The saved place may have gained coordinates before this assignment.
    # Its unchanged links still need a sidecar refresh, without a new undo step.
    queue_location_sync_if_enabled(db, photo_id)
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
# (``pipeline_grouping``, ``species_confirm_cache``), the
# ``identity_alternatives`` replay and the ``subject_verdict`` replay (which
# re-projects the photo's primary subject) belong to services the data layer
# may not import. Plain per-photo actions still replay through
# ``Database._apply_undo`` / ``_apply_redo``.
#
# Each replay is one savepoint (``_replay_entry``): the entry's writes and
# its cursor flip apply together or not at all, whether the replay owns its
# unit of work or runs inside the caller's (the undo/redo routes' prediction
# decision). A stale cache-linked or identity entry is retired in the
# enclosing unit after that savepoint rolled back, so the retirement commits
# with the caller's unit and the replay's partial writes never do.

_GROUPING_STALE = (
    'That grouping or species action was superseded by newer analysis. '
    'History has been refreshed; review the next action before trying again.'
)
_IDENTITY_STALE = (
    'That identification was changed elsewhere. History has been refreshed; '
    'review the next action before trying again.'
)
_CACHE_LINKED = frozenset({'pipeline_grouping', 'species_confirm_cache'})
# Actions this module replays itself rather than through ``Database``'s
# setter and handler tables.
SERVICE_REPLAYED_ACTIONS = frozenset({
    *_CACHE_LINKED, 'identity_alternatives', 'photo_location', subject_verdicts.ACTION_TYPE,
})


def undo_last_edit(db: Database, *, cache_restore: CacheRestore | None = None) -> dict[str, Any] | None:
    """Undo the most recent undoable edit. Returns the undone entry dict, or None.

    Non-undoable entries (prediction_reject, discard) are skipped.
    The entry is marked as undone (not deleted) so it can be redone.

    Stale cache-linked and identity actions are retired and reported (the
    ``*HistoryStale`` exception) without applying another entry. The caller
    must refresh the controls so the next click names the edit it will
    actually reverse. The retirement commits with the caller's unit of work,
    or with this call's own unit when there is none.

    ``cache_restore`` keeps the pipeline-cache restoration and regroup lock
    alive through the caller's outer unit-of-work commit; without one, they
    last until this call's own commit.
    """
    return _replay_next(db, undo=True, cache_restore=cache_restore)


def redo_last_undo(db: Database, *, cache_restore: CacheRestore | None = None) -> dict[str, Any] | None:
    """Redo the most recently undone edit. Returns the entry dict, or None.

    Replays in chronological order (ASC) so sequential undos are redone
    correctly. Stale cache-linked and identity actions are retired and
    reported without replaying another entry, just as in ``undo_last_edit``.

    ``cache_restore``: see ``undo_last_edit``.
    """
    return _replay_next(db, undo=False, cache_restore=cache_restore)


def _replay_next(
    db: Database, *, undo: bool, cache_restore: CacheRestore | None,
) -> dict[str, Any] | None:
    from services.grouping_history import CacheRestore

    # Without a caller's ``CacheRestore`` this call scopes one around its own
    # unit, so a failed commit still puts a restored pipeline cache back.
    scope = contextlib.nullcontext(cache_restore) if cache_restore is not None else CacheRestore()
    with scope as restore:
        stale: Exception | None = None
        # The caller's unit when there is one (a savepoint inside it), else
        # this call's own: the retirement below commits with it.
        with db.transaction():
            history = db.edit_history
            found = history.next_undo() if undo else history.next_redo()
            if not found:
                return None
            entry, items = found
            stale = _replay_or_retire(db, entry, items, undo=undo, cache_restore=restore)
        if stale is not None:
            raise stale from None
        return entry


def _replay_or_retire(
    db: Database, entry: dict[str, Any], items: Sequence[Any], *, undo: bool, cache_restore: CacheRestore,
) -> Exception | None:
    """Replay ``entry`` as one savepoint, or retire it if it went stale.

    Returns the exception to report for a retired entry, else ``None``.
    """
    from services.grouping_history import GroupingHistoryStale
    from services.identity_alternatives import IdentityHistoryStale

    action = entry['action_type']
    history = db.edit_history
    try:
        _replay_entry(db, entry, items, undo=undo, cache_restore=cache_restore)
    except GroupingHistoryStale:
        if action not in _CACHE_LINKED:
            raise
        history.retire_stale_grouping_entry(entry['id'])
        return GroupingHistoryStale(_GROUPING_STALE)
    except IdentityHistoryStale:
        if action != 'identity_alternatives':
            raise
        history.retire_superseded_identity_entry(entry['id'])
        return IdentityHistoryStale(_IDENTITY_STALE)
    finally:
        if action == 'identity_alternatives':
            # A failed or retired identity replay must never leave Python-side
            # provenance snapshots for a later edit to fold in.
            db.species_provenance.forget_snapshots()
    return None


def _replay_entry(
    db: Database, entry: dict[str, Any], items: Sequence[Any], *, undo: bool, cache_restore: CacheRestore,
) -> None:
    """Apply one entry and flip its cursor, as one savepoint."""
    from services.grouping_history import (
        apply_grouping_photo_edit,
        restore_grouping_edit,
        restore_species_confirm_cache_edit,
    )

    history = db.edit_history
    action = entry['action_type']
    with db.transaction():
        if action == 'pipeline_grouping':
            with restore_grouping_edit(db, entry, undo=undo, cache_restore=cache_restore):
                apply_grouping_photo_edit(db, entry, items, undo=undo)
                _flip_cursor(history, entry, undo=undo)
        elif action == 'species_confirm_cache':
            with restore_species_confirm_cache_edit(db, entry, undo=undo, cache_restore=cache_restore):
                _flip_cursor(history, entry, undo=undo)
        elif action == 'identity_alternatives':
            _apply_identity_items(db, items, undo=undo)
            _flip_cursor(history, entry, undo=undo)
        elif action == subject_verdicts.ACTION_TYPE:
            subject_verdicts.replay_history(db, items, undo=undo)
            _flip_cursor(history, entry, undo=undo)
        elif action == 'photo_location':
            _replay_photo_locations(db, items, undo=undo)
            _flip_cursor(history, entry, undo=undo)
        else:
            if undo:
                db._apply_undo(entry, items)
            else:
                db._apply_redo(entry, items)
            _flip_cursor(history, entry, undo=undo)


def _replay_photo_locations(db: Database, items: Sequence[Any], *, undo: bool) -> None:
    """Restore each photo's recorded places, then queue its sidecar sync."""
    value_key = 'old_value' if undo else 'new_value'
    for item in items:
        db.restore_photo_location_state(item['photo_id'], item[value_key])
        queue_location_sync_if_enabled(db, item['photo_id'])
    db.species_provenance.replay_history(items, undo=undo)


def _flip_cursor(history: EditHistoryRepository, entry: dict[str, Any], *, undo: bool) -> None:
    if undo:
        history.mark_undone(entry['id'])
    else:
        history.mark_redone(entry['id'])


def _apply_identity_items(db: Database, items: Sequence[Any], *, undo: bool) -> None:
    """Replay an ``identity_alternatives`` entry's photo identities and evidence."""
    from services.identity_alternatives import replay_identity_batch

    replay_identity_batch(db, items, undo=undo)
    # Identity snapshots already restore their retargeted authorship
    # lattice; the extra replay only owns species evidence here.
    db.species_provenance.replay_history(items, undo=undo, restore_source=False)
