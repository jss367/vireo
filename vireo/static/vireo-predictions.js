/* Grouped prediction decisions — the client half of one server contract.
 *
 * ``/api/predictions/<id>/accept``, ``/replace-keywords`` and
 * ``/accept-subject`` all route through ``accept_prediction``, which fans the
 * decision out across every still-pending row in the same burst group. One
 * request therefore settles several rows, and each of the three routes names
 * every row its transaction decided in the response's ``prediction_ids``.
 *
 * Any caller that loops those routes over a snapshot of rows has to consume
 * that list. Otherwise its next iteration asks the server to decide a row the
 * loop itself already decided, meets the terminal-status 409, and reports it
 * as "not applied" or aborts the run — a false claim about the database, made
 * about work the loop had just performed.
 *
 * The list is also the only version of that answer a caller can trust, which
 * is why it rides on the write instead of being inferred from the 409's
 * "already accepted" wording: the server cannot say *who* settled a row. A row
 * settled from a second tab is not in this list, still 409s, and must still be
 * reported as a refusal. Preserving that distinction is the entire point —
 * "this batch did it" and "somebody else did it" are different facts and the
 * user is owed the difference.
 *
 * It lives in one shared, DOM-free file so every consumer (Review's Accept All
 * and single-card accept, ID Conflicts' batch accept) runs the same
 * implementation — two private copies is exactly how those two pages came to
 * disagree — and so tests can drive it under Node against a stub server.
 */
(function(global) {
  'use strict';

  var Vireo = global.Vireo = global.Vireo || /** @type {VireoNamespace} */ ({});

  /** @param {unknown} id @returns {id is number} */
  function isPredictionId(id) {
    return typeof id === 'number' && Number.isSafeInteger(id) && id > 0;
  }

  /* The rows a single decision response says its transaction decided.
   *
   * Absent or non-array ``prediction_ids`` yields an empty list; invalid
   * members are ignored. An older/errored body must not be read as "this decided nothing
   * beyond the row I asked about" *or* crash the loop — callers still credit
   * the row named in the URL themselves.
   */
  /** @param {unknown} response @returns {number[]} */
  function decidedPredictionIds(response) {
    if (!response || typeof response !== 'object' || !('prediction_ids' in response)) return [];
    var ids = response.prediction_ids;
    if (!Array.isArray(ids)) return [];
    /** @type {unknown[]} */
    var values = ids;
    return values.filter(isPredictionId);
  }

  /* Tracks which rows *this* run has already decided.
   *
   * Usage in a loop over a stale snapshot:
   *
   *   var decided = Vireo.predictions.groupedDecisionTracker();
   *   for (...) {
   *     if (decided.alreadyDecided(row.id)) continue;  // applied, not skipped
   *     decided.record(await post(row.id));
   *   }
   */
  /** @returns {GroupedDecisionTracker} */
  function groupedDecisionTracker() {
    /** @type {Set<number>} */
    var decided = new Set();
    return {
      alreadyDecided: function(/** @type {number} */ predId) {
        return decided.has(predId);
      },
      // Records what the response says it wrote and returns those ids, so a
      // caller can update its local rows in the same statement.
      record: function(/** @type {unknown} */ response) {
        var ids = decidedPredictionIds(response);
        for (var id of ids) decided.add(id);
        return ids;
      },
      size: function() {
        return decided.size;
      },
    };
  }

  Vireo.predictions = Object.assign(Vireo.predictions || {}, {
    decidedPredictionIds: decidedPredictionIds,
    groupedDecisionTracker: groupedDecisionTracker,
  });
})(typeof window !== 'undefined' ? window : globalThis);
