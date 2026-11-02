"""The prediction-decision lock: one serialized writer for review decisions.

Every route that records a prediction decision (accept, reject, reviewed,
group apply, highlight confirm/relabel, undo/redo) is check-then-write and
must hold SQLite's writer lock across both halves. This module owns that lock
and the declared list of routes that take it, so the routes can live in
``vireo/app.py`` or in any blueprint under ``vireo/web/`` and still share one
implementation.

A route runs its checks and writes as one decision::

    from services import prediction_decisions

    def work():
        ...  # checks and writes; return an error response to refuse
        return jsonify(...)

    return prediction_decisions.under_prediction_decision_lock(
        db, work, json_error=json_error, prediction_ids=[pred_id],
    )

``decision`` is the context manager underneath: it takes the lock, owns the
transaction as a unit of work (``db.transaction()``), revalidates that the
predictions still belong to the active workspace, and commits when the block
finishes or rolls back when it raises. ``under_prediction_decision_lock``
also rolls back when ``work`` returns an error response (status 400 or
above), so no exit path needs its own rollback, and none may call
``db.commit()`` or ``db.rollback()``: the unit is the only commit point.

Call these through the module (or import them under their own names); do not
inject them into a blueprint factory under another name. The structural test
``test_route_contract.py::test_every_prediction_decision_route_locks`` finds
the lock by the name ``begin_prediction_decision``, and patching
``prediction_decisions.begin_prediction_decision`` in tests reaches every
caller only when they look it up here. No function here touches ``request``;
``json_error`` is the app's error-response builder, passed in explicitly.
"""

from __future__ import annotations

import contextlib
import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from db import Database
    from services.service_types import ErrorResponse, JsonError

# Every route that records a prediction decision, and therefore every route
# that must take the writer lock below. Entries are *view function names*
# (``def api_undo``), not endpoint names: an endpoint becomes
# blueprint-qualified (``predictions.api_undo``) when its route moves under
# ``vireo/web/``, but the function name does not change. So a route keeps its
# entry here when it moves, and a decision route's function name must be
# unique across ``vireo/app.py`` and ``vireo/web/*.py`` (the test enforces
# this).
#
# Serialization is only worth what the *least* careful writer does: a lock one
# side takes and the other does not is not a lock. That is exactly how the
# last gap arose — the batch endpoints held it, Review's single-row routes did
# not — and fixing only the routes a review names repeats the mistake one level
# up. So this list is checked against the set derived from the call graph of
# every route in ``vireo/app.py`` and ``vireo/web/*.py`` by
# ``test_route_contract.py::test_every_prediction_decision_route_locks``, which
# also asserts each name here reaches ``begin_prediction_decision``. A new
# route that touches ``prediction_review`` fails that test until it is listed
# and locked.
#
# The five below the single-row group were the remainder of the sweep: burst
# group apply writes accepted/rejected for whole photos, highlight confirm
# accepts through ``accept_prediction``, highlight relabel rejects each photo's
# top prediction, and undo/redo replay recorded statuses back out of edit
# history.
PREDICTION_DECISION_ROUTES = frozenset({
    "api_batch_accept_predictions",
    "api_batch_reject_predictions",
    "api_accept_prediction",
    "api_accept_subject_species",
    "api_reject_prediction",
    "api_mark_prediction_reviewed",
    "api_replace_species_keywords_with_prediction",
    "api_prediction_group_apply",
    "api_highlights_confirm",
    "api_highlights_relabel",
    "api_undo",
    "api_redo",
})


def begin_prediction_decision(db: Database, *, json_error: JsonError) -> ErrorResponse | None:
    """Hold SQLite's write lock across a decision's checks *and* its writes.

    Every route in ``PREDICTION_DECISION_ROUTES`` is check-then-write: it
    reads a row's status (and, for the batch pair, its ambiguity and label
    set), then writes the rows that pass. Read and write have to be one
    indivisible step, or the preconditions only *narrow* the race they
    claim to close — two overlapping requests (a double-clicked Accept, or
    Browse's batch accept and Review's single reject fired before either
    page reloads) can both finish their reads while the row is still
    pending and then both write. Waitress serves these routes on 16
    threads, and ``_get_db`` hands each request its own connection, so
    "overlapping" is a real interleaving and not a thought experiment.

    The completeness of that route list is the guarantee, not an
    implementation detail: a decision route that skips this lock puts
    every other route's atomicity back to "narrowed, not closed".

    ``BEGIN IMMEDIATE`` takes the database's single writer lock up front,
    before the first read. That is what makes the whole sequence atomic:
    SQLite's WAL mode allows one writer at a time, so a second decision
    request blocks here until the first commits and then re-reads the state
    the first one left. The alternative — a conditional
    ``UPDATE ... WHERE status = 'pending'`` — would only guard the status
    column, leaving ambiguity (a function of the photo's keywords) and the
    keyword/history writes outside the guarantee, and would need a second
    code path for the sibling and group writes that hang off the same
    decision. Python's ``sqlite3`` would otherwise open its implicit
    transaction at the *first write*, which is exactly too late.

    Returns ``None`` on success, or ``json_error(..., 503)`` when the lock
    cannot be taken within the connection's ``busy_timeout``. Reporting that
    plainly beats a silent non-atomic fallback: the caller can retry, and
    nothing has been written.
    """
    if db.in_transaction:
        # A previous statement in this request may have opened sqlite3's
        # implicit transaction; BEGIN cannot nest.
        db.commit()
    try:
        db.begin_immediate()
    except sqlite3.OperationalError:
        return json_error(
            "another prediction decision is in progress; nothing was "
            "changed — try again",
            503,
        )
    return None


class DecisionRefused(Exception):
    """Ends a decision without committing; carries the response to return."""

    def __init__(self, response: object) -> None:
        super().__init__()
        self.response = response


@contextlib.contextmanager
def decision(
    db: Database, *, json_error: JsonError, prediction_ids: Sequence[int] = (),
) -> Iterator[None]:
    """Run the block as one serialized prediction decision.

    Takes the writer lock (``begin_prediction_decision``), then owns the
    locked transaction as a unit of work: the block's checks and writes
    commit together when it finishes, and roll back when it raises. Raise
    ``DecisionRefused(response)`` to refuse after checking. With
    ``prediction_ids``, first refuses with a 404 if any of them left the
    active workspace between the caller's parse and the lock
    (``out_of_workspace_prediction_ids``). A lock that cannot be taken
    raises ``DecisionRefused`` with the 503 before anything runs.
    """
    lock_err = begin_prediction_decision(db, json_error=json_error)
    if lock_err is not None:
        raise DecisionRefused(lock_err)
    # The unit adopts the BEGIN IMMEDIATE the lock just took.
    with db.transaction():
        if prediction_ids and out_of_workspace_prediction_ids(db, prediction_ids):
            raise DecisionRefused(
                json_error("Prediction does not belong to the active workspace", 404),
            )
        yield


def _status(result: object) -> int:
    if isinstance(result, tuple):
        return int(result[1])
    return int(getattr(result, "status_code", 200))


def under_prediction_decision_lock[R](
    db: Database, work: Callable[[], R], *, json_error: JsonError,
    prediction_ids: Sequence[int] = (),
) -> R | ErrorResponse:
    """Run ``work()`` inside ``decision`` and return its response.

    An error response (status 400 or above) from ``work`` rolls the decision
    back, whatever it wrote before deciding to refuse, so a body with several
    exits needs no rollback at any of them. ``None`` (a body that reports
    success some other way) commits.
    """
    try:
        with decision(db, json_error=json_error, prediction_ids=prediction_ids):
            result = work()
            if result is not None and _status(result) >= 400:
                raise DecisionRefused(result)
    except DecisionRefused as refused:
        return refused.response  # pyright: ignore[reportReturnType] - an error response
    return result


def out_of_workspace_prediction_ids(db: Database, pred_ids: Sequence[int]) -> set[int]:
    """Which of ``pred_ids`` sit on a photo no longer in this workspace.

    A folder detach can commit between ``_parse_prediction_ids``'s ownership
    check and ``BEGIN IMMEDIATE``; skipping these rows keeps a batch from
    tagging a now-foreign photo or writing its workspace-scoped
    ``prediction_review`` state. Callers report them as
    ``skipped_out_of_workspace`` (the fix is a panel refresh, not Review).
    Call it after ``begin_prediction_decision``: only an in-lock read closes
    the window.
    """
    return db.predictions.out_of_workspace_ids(pred_ids)
