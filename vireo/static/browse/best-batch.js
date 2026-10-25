/* Browse: best-of-batch picker. ES module (see
   browse/README.md). */
import { selection } from './selection-state.js';
import { state } from './state.js';
import { safeFetch, showToast } from './shared.js';
import {
  _clearRepresentativeStateIfIneligible, _refreshBatchInspectorIfActive,
} from './batch-actions.js';
import { showUndoToast } from './detail.js';
import { refreshGridCards } from './grid.js';
import { findBrowsePhoto } from './selection-panel.js';
import { getActiveSelection } from './selection.js';
import { scheduleCollectionCountsRefresh } from './sidebar-collections.js';
import { reconcileBrowseStackCovers, refreshExpandedBrowseStackMembers } from './stacks.js';

function bestBatchSeedId() {
  if (selection.focusedPhotoId != null) return selection.focusedPhotoId;
  var ids = getActiveSelection();
  return ids.length ? ids[0] : null;
}

export function hideBestBatch() {
  var modal = /** @type {HTMLElement} */ (document.getElementById('bestBatchModal'));
  if (modal) modal.classList.remove('open');
}

export function openBestBatchForIds(/** @type {number[]} */ ids, /** @type {number | null | undefined} */ seedId = undefined) {
  ids = Array.isArray(ids) ? ids.filter(function(id) { return id != null; }) : [];
  seedId = seedId || (ids.length ? ids[0] : bestBatchSeedId());
  if (!seedId) {
    showToast('Select a photo first.', 'error');
    return;
  }
  try {
    window.sessionStorage.setItem('vireo.bestBatchIds', JSON.stringify(ids));
    window.sessionStorage.setItem('vireo.bestBatchSeedId', String(seedId));
  } catch (e) {
    showToast('Could not open Best Batch for this selection.', 'error');
    return;
  }
  window.location.href = '/best-batch?photo_id=' + encodeURIComponent(seedId);
}

export function openBestBatchInReview() {
  if (!state.bestBatchData || !state.bestBatchData.photo_ids || state.bestBatchData.photo_ids.length < 2) return;
  try {
    window.sessionStorage.setItem('vireo.browseBurstReviewIds', JSON.stringify(state.bestBatchData.photo_ids));
  } catch (e) {
    showToast('Could not open burst review for this batch.', 'error');
    return;
  }
  window.location.href = '/pipeline/review?browse_burst=1';
}

export async function applyBestBatchPickOnly() {
  if (!state.bestBatchData || !state.bestBatchData.best_photo_id) return;
  var bestId = state.bestBatchData.best_photo_id;
  try {
    await safeFetch('/api/batch/flag', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: [bestId], flag: 'flagged'}),
    });
  } catch(e) { return; }
  var p = findBrowsePhoto(bestId);
  if (p) p.flag = 'flagged';
  await reconcileBrowseStackCovers([bestId]);
  refreshGridCards([bestId]);
  refreshExpandedBrowseStackMembers([bestId]);
  _refreshBatchInspectorIfActive();
  scheduleCollectionCountsRefresh();
  showUndoToast();
  showToast('Flagged best photo.', 'success');
}

export async function applyBestBatchPickAndReject() {
  if (!state.bestBatchData || !state.bestBatchData.best_photo_id) return;
  var bestId = state.bestBatchData.best_photo_id;
  var rejectIds = (state.bestBatchData.suggested_reject_ids || []).filter(function(id) { return id !== bestId; });
  try {
    await safeFetch('/api/batch/best-batch-flags', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({best_photo_id: bestId, reject_photo_ids: rejectIds}),
    });
  } catch(e) { return; }
  var touchedIds = [bestId].concat(rejectIds);
  touchedIds.forEach(function(id) {
    var p = findBrowsePhoto(id);
    /** @type {'flagged' | 'rejected'} */ var flag = id === bestId ? 'flagged' : 'rejected';
    if (p) p.flag = flag;
    _clearRepresentativeStateIfIneligible(id, flag);
  });
  await reconcileBrowseStackCovers(touchedIds);
  refreshGridCards(touchedIds);
  refreshExpandedBrowseStackMembers(touchedIds);
  _refreshBatchInspectorIfActive();
  scheduleCollectionCountsRefresh();
  showUndoToast();
  showToast('Applied best-batch flags.', 'success');
  hideBestBatch();
}
