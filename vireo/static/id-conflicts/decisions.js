// Row selection and the batch bar, and accepting or rejecting predictions
// (one or in bulk), refreshing only the rows a decision touched.

import { state } from './state.js';
import { fetchRows, jsonFetch, loadComparison } from './loading.js';
import { renderCompare } from './render.js';
import { groupedDecisionTracker, showToast } from './shared.js';

/** @param {number} photoId @param {boolean} checked */
export function toggleRow(photoId, checked) {
  if (checked) state.selectedRows.add(photoId);
  else state.selectedRows.delete(photoId);
  updateBatchControls();
}

// Only this page's rows: a batch action sends one request per row, so
// "select everything the filter matched" would fire tens of thousands of
// writes from a single click.
/** @param {boolean} checked */
export function toggleAllRows(checked) {
  state.compareRows.forEach(function(photo) {
    if ((photo.subjects || []).length > 1) return;
    if (checked) state.selectedRows.add(photo.photo_id);
    else state.selectedRows.delete(photo.photo_id);
  });
  renderCompare();
}

export function updateBatchControls() {
  var row = /** @type {HTMLElement} */ (document.getElementById('batchRow'));
  if (!state.compareData) {
    row.style.display = 'none';
    return;
  }
  row.style.display = '';
  /** @type {HTMLElement} */ (document.getElementById('batchCount')).textContent = state.selectedRows.size + ' selected';
  ['batchAccept', 'batchReplace', 'batchReject', 'batchReviewed'].forEach(function(id) {
    /** @type {HTMLButtonElement} */ (document.getElementById(id)).disabled = state.selectedRows.size === 0;
  });
}

/** @param {number} photoId */
export function loadedPhoto(photoId) {
  return state.compareRows.find(function(photo) { return photo.photo_id === photoId; });
}

/** @param {(number | string)[]} predictionIds */
export function comparePhotoIdsForPredictions(predictionIds) {
  var wanted = new Set((predictionIds || []).map(function(id) { return Number(id); }));
  /** @type {number[]} */
  var photoIds = [];
  state.compareRows.forEach(function(photo) {
    var found = (photo.subjects || []).some(function(subject) {
      return Object.keys(subject.predictions || {}).some(function(model) {
        return ((subject.predictions || {})[model] || []).some(function(pred) {
          return wanted.has(Number(pred.id));
        });
      });
    });
    if (found) photoIds.push(photo.photo_id);
  });
  return photoIds;
}

/** @param {number | string} predId @param {string} status */
export function setComparePredictionStatus(predId, status) {
  var changed = false;
  state.compareRows.forEach(function(photo) {
    (photo.subjects || []).forEach(function(subject) {
      Object.keys(subject.predictions || {}).forEach(function(model) {
        ((subject.predictions || {})[model] || []).forEach(function(pred) {
          if (Number(pred.id) !== Number(predId)) return;
          pred.status = status;
          changed = true;
        });
      });
    });
  });
  return changed;
}

/** @returns {CompareScope} */
export function currentComparisonScope() {
  return {
    collectionId: /** @type {HTMLSelectElement} */ (document.getElementById('collectionPicker')).value,
    loadingSeq: state.loadingSeq,
  };
}

/** @param {CompareScope} scope */
export function comparisonScopeIsCurrent(scope) {
  return Boolean(state.compareData) && scope.loadingSeq === state.loadingSeq &&
    /** @type {HTMLSelectElement} */ (document.getElementById('collectionPicker')).value === scope.collectionId;
}

// A decision changed some photos. Rebuild exactly those inside the stored
// comparison and re-query this page, so the row, the chip counts and the
// filter membership all move together instead of drifting apart.
/** @param {(number | string)[]} photoIds @param {CompareScope=} scope */
export async function refreshComparisonPhotos(photoIds, scope) {
  if (!state.compareData) return;
  var uniqueIds = Array.from(new Set((photoIds || []).map(Number).filter(function(id) {
    return Number.isInteger(id) && id > 0;
  })));
  if (!uniqueIds.length) {
    await loadComparison();
    return;
  }
  var refreshScope = scope || currentComparisonScope();
  if (!comparisonScopeIsCurrent(refreshScope)) return;
  await fetchRows({refreshPhotoIds: uniqueIds, afterDecision: true});
}

/** @param {(number | string)[]} photoIds @param {CompareScope=} scope */
export function refreshDecisionPhotos(photoIds, scope) {
  // Decision buttons on different rows can be used before an earlier scoped
  // request finishes. Serialize the reads so an older server snapshot can
  // never merge after a newer one and overwrite its state.
  var queuedScope = scope || currentComparisonScope();
  var refresh = state.decisionRefreshQueue.then(async function() {
    // Capture scope when the decision is made, not when its queued refresh
    // eventually starts. A collection load while it waits supersedes it.
    if (!comparisonScopeIsCurrent(queuedScope)) return;
    try {
      await refreshComparisonPhotos(photoIds, queuedScope);
    } catch (e) {
      showToast('Could not refresh the changed photo — reloading the comparison.', 'error');
      await loadComparison();
    }
  });
  // A failed reload must not poison every later refresh in the queue.
  state.decisionRefreshQueue = refresh.catch(function() {});
  return refresh;
}

/** @param {number} predId @param {string} action */
export async function predictionAction(predId, action) {
  var decisionScope = currentComparisonScope();
  var url = '/api/predictions/' + predId + '/' + endpointForAction(action);
  var entryPhotoIds = comparePhotoIdsForPredictions([predId]);
  var data;
  try {
    data = await jsonFetch(url, { method: 'POST' });
  } catch (caught) {
    var e = /** @type {CompareRequestError | null} */ (caught);
    // The decision endpoints now answer 409 when the row was already settled
    // — from a second tab, or from a Browse batch decision landing while this
    // page sat open. The buttons here are disabled for non-pending rows, so
    // reaching this means the page is stale: say so and refresh that photo
    // rather than swallowing the click, which would look like the action
    // succeeded.
    //
    // Only a server that answered lets us say the decision was not applied.
    // For a dropped connection or a 5xx the request may well have committed,
    // so the honest report is that we cannot confirm it — the targeted
    // refresh below shows which it was either way.
    if (e && e.answered) {
      showToast(e.message || 'Could not apply that decision', 'error');
      // Another tab can settle an entire burst before this stale row reaches
      // the server. The 409 does not name those grouped photos, so a scoped
      // refresh cannot reconcile the comparison safely.
      if (e.status === 409 && (action === 'accept' || action === 'replace' ||
          action === 'accept_subject')) {
        await loadComparison();
        return;
      }
    } else {
      showToast('Could not confirm the decision (' +
        (e && e.message ? e.message : 'the request failed') +
        ') — reloading the comparison to show the current state.', 'error');
      // An accept/replace response can be lost after a grouped write commits.
      // Without its photo_ids response we cannot know every burst member that
      // changed, so only a full reload can reconcile the collection safely.
      await loadComparison();
      return;
    }
    await refreshDecisionPhotos(entryPhotoIds, decisionScope);
    return;
  }

  // A collection navigation that finished while the POST was in flight may
  // have loaded a snapshot from before this decision committed. Reconcile the
  // current collection without ever mutating it using the old row's state.
  if (!comparisonScopeIsCurrent(decisionScope)) {
    await loadComparison();
    return;
  }

  // Reject and Reviewed only change this prediction's review status. Show it
  // on the row immediately, then re-query so the counts and this row's
  // membership in the filter catch up.
  if (action === 'reject' || action === 'reviewed') {
    if (setComparePredictionStatus(
      predId, action === 'reject' ? 'rejected' : 'reviewed'
    )) {
      renderCompare();
    }
    await refreshDecisionPhotos(entryPhotoIds, decisionScope);
    return;
  }

  var affectedPhotoIds = entryPhotoIds.concat((data && data.photo_ids) || []);
  await refreshDecisionPhotos(affectedPhotoIds, decisionScope);
}

/** @param {string} action */
export function endpointForAction(action) {
  if (action === 'accept') return 'accept';
  if (action === 'accept_subject') return 'accept-subject';
  if (action === 'replace') return 'replace-keywords';
  if (action === 'reject') return 'reject';
  return 'reviewed';
}

/** @param {string} action */
export async function batchAction(action) {
  var decisionScope = currentComparisonScope();
  var ids = Array.from(state.selectedRows);
  var photos = /** @type {ComparePhoto[]} */ (ids.map(loadedPhoto).filter(Boolean));
  var attempted = 0;
  /** @type {string[]} */
  var refused = [];
  var unconfirmed = 0;
  var groupedConflict = false;
  var refreshPhotoIds = ids.slice();
  // Rows an earlier request in *this* loop already decided. Accept, replace
  // and accept-subject expand through the burst group, so one request can
  // settle several of the selected photos; the next iteration would then hit
  // the terminal-status 409 and be counted as "not applied" for work that
  // demonstrably landed. Shared with Review's Accept All, which loops the same
  // routes over the same contract — see vireo-predictions.js for why the ids
  // ride on the write rather than being read out of the 409's wording.
  var decidedByThisBatch = groupedDecisionTracker();
  for (var i = 0; i < photos.length; i++) {
    var predId = /** @type {ComparePhoto} */ (photos[i]).best_prediction_id;
    if (!predId) continue;
    attempted++;
    // Applied, not skipped: it counts in the denominator and against no
    // failure bucket, because the server told us it wrote this row.
    if (decidedByThisBatch.alreadyDecided(predId)) continue;
    try {
      var decision = await jsonFetch(
        '/api/predictions/' + predId + '/' + endpointForAction(action),
        { method: 'POST' });
      decidedByThisBatch.record(decision);
      refreshPhotoIds = refreshPhotoIds.concat(decision.photo_ids || []);
    } catch (caught) {
      var e = /** @type {CompareRequestError | null} */ (caught);
      // One already-settled row must not abandon the rest of the batch, but
      // the count has to be named — a batch that quietly did less than the
      // selection implies is the gap this whole flow exists to close. The
      // denominator is what was actually attempted, not the selection: rows
      // with no usable prediction were never sent.
      //
      // Split by outcome class rather than lumping every failure under "not
      // applied": that phrasing is a claim about the database, and it is only
      // true when the server answered. A lost response leaves the decision
      // possibly committed, and saying otherwise is the same kind of
      // convenient fiction the rest of this flow exists to remove.
      if (e && e.answered) {
        refused.push(e.message || 'refused');
        if (e.status === 409 && (action === 'accept' || action === 'replace' ||
            action === 'accept_subject')) groupedConflict = true;
      }
      else unconfirmed++;
    }
  }
  var reportParts = [];
  if (refused.length) {
    reportParts.push(refused.length + ' of ' + attempted + ' not applied — ' +
      refused[0]);
  }
  if (unconfirmed) {
    var reloadsAll = action === 'accept' || action === 'replace' ||
      action === 'accept_subject';
    reportParts.push(unconfirmed + ' of ' + attempted +
      ' could not be confirmed (the request failed with no answer from the ' +
      'server) — the ' + (reloadsAll ? 'reload' : 'refresh') +
      ' below shows whether they landed');
  }
  if (reportParts.length) {
    showToast(reportParts.join('; '), 'error');
  }
  state.selectedRows.clear();
  if (!comparisonScopeIsCurrent(decisionScope)) {
    await loadComparison();
    return;
  }
  // A grouped keyword decision may commit for unselected burst members before
  // its response is lost. Without the response photo_ids, a targeted refresh
  // cannot discover those members, so reconcile the whole collection.
  if (groupedConflict || (unconfirmed && (action === 'accept' ||
      action === 'replace' || action === 'accept_subject'))) {
    await loadComparison();
    return;
  }
  await refreshDecisionPhotos(refreshPhotoIds, decisionScope);
}
