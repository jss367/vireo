"""Capture reversible location assignments and exact prediction decisions.

Legacy audit rows lack the original locations and prediction IDs. New actions
use separate types so replay never guesses at those old entries' before-state.
"""

from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, TypedDict

from services.pending_changes import queue_location_sync_if_enabled

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from db import Database


class HistoryItem(TypedDict):
    """One per-photo before/after row of an ``edit_history`` entry."""

    photo_id: int
    old_value: str
    new_value: str


def atomic_edit[**P, R](
    get_db: Callable[[], Database],
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Keep setters, sidecar work and history in one writer transaction."""
    def decorate(fn: Callable[P, R]) -> Callable[P, R]:
        @wraps(fn)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            db = get_db()
            with db._commits_held():
                result = fn(*args, **kwargs)
                status = result[1] if isinstance(result, tuple) else getattr(result, 'status_code', 200)
                if status >= 400:
                    db.rollback()
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
