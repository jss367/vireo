"""The prediction-decision lock: one serialized writer for review decisions.

Every route that records a prediction decision (accept, reject, reviewed,
group apply, highlight confirm/relabel, undo/redo) is check-then-write and
must hold SQLite's writer lock across both halves. This module owns that lock
and the declared list of routes that take it, so the routes can live in
``vireo/app.py`` or in any blueprint under ``vireo/web/`` and still share one
implementation.

Taking the lock from a route::

    from services import prediction_decisions

    lock_err = prediction_decisions.begin_prediction_decision(
        db, json_error=json_error,
    )
    if lock_err is not None:
        return lock_err
    ...  # checks, writes, commit (roll back on every early exit)

or, for a body with several exits::

    return prediction_decisions.under_prediction_decision_lock(
        db, work, json_error=json_error,
    )

Call these through the module (or import them under their own names); do not
inject them into a blueprint factory under another name. The structural test
``test_route_contract.py::test_every_prediction_decision_route_locks`` finds
the lock by the name ``begin_prediction_decision``, and patching
``prediction_decisions.begin_prediction_decision`` in tests reaches every
caller only when they look it up here. No function here touches ``request``;
``json_error`` is the app's error-response builder, passed in explicitly.
"""

from __future__ import annotations

import sqlite3

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

# Same cap and rationale as ``vireo/app.py``'s ``_SQL_PARAM_CHUNK``: stay under
# SQLite's 999-variable limit on older builds, with headroom for the extra
# bound workspace id.
_SQL_PARAM_CHUNK = 900


def begin_prediction_decision(db, *, json_error):
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
    if db.conn.in_transaction:
        # A previous statement in this request may have opened sqlite3's
        # implicit transaction; BEGIN cannot nest.
        db.conn.commit()
    try:
        db.conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        return json_error(
            "another prediction decision is in progress; nothing was "
            "changed — try again",
            503,
        )
    return None


def under_prediction_decision_lock(db, work, *, json_error):
    """Run ``work()`` as one serialized prediction decision.

    The wrapper form of ``begin_prediction_decision`` for routes whose
    body has several exits. ``work`` commits when it writes; the
    ``finally`` covers the paths that do not — an early ``404``/``409``
    precondition failure, or an exception unwinding — because leaving
    ``BEGIN IMMEDIATE`` open would hold the database's single writer lock
    for the rest of this connection's life and stall every later decision
    served on it. The single-row endpoints take the same lock inline with
    explicit rollbacks at each exit; both shapes are equivalent, and the
    route-contract test checks the lock, not the shape.
    """
    lock_err = begin_prediction_decision(db, json_error=json_error)
    if lock_err is not None:
        return lock_err
    try:
        return work()
    finally:
        if db.conn.in_transaction:
            db.conn.rollback()


def out_of_workspace_prediction_ids(db, pred_ids):
    """Which of ``pred_ids`` sit on a photo no longer in this workspace.

    The fourth precondition alongside decided / superseded / ambiguous, and
    the same shape: ``_parse_prediction_ids`` verified workspace ownership
    before the lock, but a folder detach is itself a write. In WAL mode
    another connection can acquire the writer lock, delete the row from
    ``workspace_folders``, and commit — all in the window between
    ``_parse_prediction_ids`` finishing and ``BEGIN IMMEDIATE`` on this
    request succeeding. Skipping the row here catches that race so a batch
    cannot tag a now-foreign photo or write workspace-scoped
    ``prediction_review`` state for it. Reported as
    ``skipped_out_of_workspace`` so a caller can tell "detached mid-flight"
    from any of the other skip reasons — the user's next step is a panel
    refresh, and folding it into a different count would misname the
    problem the way ``CORE_PHILOSOPHY.md`` rules out.

    Call it after ``begin_prediction_decision``: only an in-lock read closes
    the window. Chunked for the same reason every other id query is (see
    ``_SQL_PARAM_CHUNK``).
    """
    if not pred_ids:
        return set()
    pred_ids = list(pred_ids)
    ws = db._ws_id()
    # Rows whose ``workspace_folders`` join misses are out of scope. The
    # LEFT JOIN keeps a row for every prediction id regardless of folder
    # membership; the WHERE clause selects the misses.
    found = set()
    for start in range(0, len(pred_ids), _SQL_PARAM_CHUNK):
        chunk = pred_ids[start:start + _SQL_PARAM_CHUNK]
        placeholders = ",".join("?" for _ in chunk)
        found.update(
            row["id"] for row in db.conn.execute(
                f"""SELECT pr.id FROM predictions pr
                    JOIN detections d ON d.id = pr.detection_id
                    JOIN photos ph ON ph.id = d.photo_id
                    LEFT JOIN workspace_folders wf
                      ON wf.folder_id = ph.folder_id
                     AND wf.workspace_id = ?
                    WHERE pr.id IN ({placeholders})
                      AND wf.workspace_id IS NULL""",
                (ws, *chunk),
            )
        )
    return found
