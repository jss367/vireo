// Loading the batch and the toolbar actions: open the inspected frame in the
// lightbox, review the burst, flag the inspected frame, and apply validated suggestions.
// ES module.

import { state } from './state.js';
import { hasLightbox, openLightbox, showToast } from './shared.js';
import { fetchJson, seedIdFromRequest, selectedIdsFromStorage } from './helpers.js';
import { renderBatch, setStatus } from './render.js';

export async function loadBatch() {
  var ids = selectedIdsFromStorage();
  var seedId = seedIdFromRequest(ids);
  if (!seedId) {
    setStatus('Go back to Browse and select at least one photo first.');
    return;
  }
  if (ids.length >= 2 && ids.indexOf(seedId) === -1) {
    ids = [];
    try {
      window.sessionStorage.removeItem('vireo.bestBatchIds');
      window.sessionStorage.removeItem('vireo.bestBatchSeedId');
    } catch (e) {}
  }
  try {
    /** @type {BestBatchResponse} */
    var data;
    if (ids.length >= 2) {
      data = await fetchJson('/api/photos/best-batch', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({photo_ids: ids}),
      });
      data.scope_method = data.scope_method || 'selected_photos';
    } else {
      data = await fetchJson('/api/photos/' + seedId + '/best-batch');
    }
    renderBatch(data);
  } catch (e) {
    setStatus(e instanceof Error && e.message ? e.message : 'Could not rank this batch.');
  }
}

export function openInspectedLightbox() {
  var data = state.bestBatchData;
  if (!data || !hasLightbox() || !state.inspectedId) return;
  var cards = data.cards || [];
  var card = cards.find(function(c) { return c.id === state.inspectedId; }) || cards[0];
  if (!card) return;
  openLightbox(card.id, card.filename || '', cards.map(function(c) {
    return {id: c.id, filename: c.filename || ''};
  }));
}

export function openReviewBurst() {
  var data = state.bestBatchData;
  if (!data || !data.photo_ids || data.photo_ids.length < 2) return;
  try {
    window.sessionStorage.setItem('vireo.browseBurstReviewIds', JSON.stringify(data.photo_ids));
  } catch (e) {
    showToast('Could not open burst review for this batch.', 'error');
    return;
  }
  window.location.href = '/pipeline/review?browse_burst=1';
}

/** @param {unknown} error @param {string} fallback */
function errorMessage(error, fallback) {
  return error instanceof Error && error.message ? error.message : fallback;
}

export async function flagBest() {
  var data = state.bestBatchData;
  if (!data || !state.inspectedId) return;
  try {
    await fetchJson('/api/batch/flag', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: [state.inspectedId], flag: 'flagged'}),
    });
    showToast('Flagged inspected photo.', 'success');
  } catch (e) {
    showToast(errorMessage(e, 'Could not flag inspected photo.'), 'error');
  }
}

export async function flagBestRejectRest() {
  var data = state.bestBatchData;
  if (!data || !data.best_photo_id) return;
  try {
    await fetchJson('/api/batch/best-batch-flags', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        best_photo_id: data.best_photo_id,
        reject_photo_ids: data.suggested_reject_ids || [],
      }),
    });
    showToast('Applied best-batch flags.', 'success');
  } catch (e) {
    showToast(errorMessage(e, 'Could not apply best-batch flags.'), 'error');
  }
}
