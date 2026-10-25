// The merge dialog: debounced preview scheduling, refreshMergePreview,
// openMergeDialog, and the dialog's target, cancel, close and confirm handlers.

import { state } from './state.js';
import { loadKeywords } from './loading.js';
import { loadLocationMatches } from './location-review.js';
import { renderMergeFields, syncCoordinateInputs } from './merge-fields.js';
import { keywordErrorMessage } from './merge-helpers.js';
import { renderMergeMap } from './merge-map.js';
import {
  mergeCancel, mergeConfirm, mergeDialog, mergeError, mergeFields, mergeMapEl, mergeMapNote,
  mergePreview, mergeTarget,
} from './merge-state.js';
import { renderMergeSummary } from './merge-summary.js';
import { showToast } from './shared.js';

/** @param {boolean} immediate */ export function scheduleMergePreview(immediate) {
  if (state.mergeDebounce) clearTimeout(state.mergeDebounce);
  if (immediate) { refreshMergePreview(); return; }
  mergeConfirm.disabled = true;
  state.mergeDebounce = setTimeout(refreshMergePreview, 300);
}

export async function refreshMergePreview() {
  const requestId = ++state.mergeRequest;
  const targetId = Number(mergeTarget.value);
  state.mergeToken = null;
  mergeConfirm.disabled = true;
  mergeError.hidden = true;
  if (state.mergeFieldsFor === null) mergePreview.textContent = 'Loading preview…';
  try {
    const response = await fetch('/api/keywords/merge-preview', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({keyword_ids: state.mergeIds, target_id: targetId,
                            overrides: state.mergeOverrides})
    });
    /** @type {KeywordMergePreview} */ const data = await response.json();
    if (requestId !== state.mergeRequest || !mergeDialog.open) return;
    if (!response.ok) throw new Error(data.error || 'Could not preview merge.');
    if (state.mergeFieldsFor !== targetId) {
      renderMergeFields(data);
      state.mergeFieldsFor = targetId;
    }
    syncCoordinateInputs(data);
    renderMergeSummary(data);
    renderMergeMap(data);
    state.mergeToken = data.preview_token || null;
    // No token means the preview is still asking a question (two different
    // linked places, say); merging now would have to guess the answer.
    mergeConfirm.disabled = !state.mergeToken;
  } catch (error) {
    if (requestId !== state.mergeRequest || !mergeDialog.open) return;
    if (state.mergeFieldsFor === null) mergePreview.textContent = '';
    mergeError.textContent = keywordErrorMessage(error);
    mergeError.hidden = false;
  }
}

/** @param {number[]} ids */ export function openMergeDialog(ids) {
  if (ids.length < 2) return;
  state.mergeIds = [...ids];
  state.mergeOverrides = {};
  state.mergeFieldsFor = null;
  mergeFields.innerHTML = '';
  mergeMapEl.hidden = true;
  mergeMapNote.hidden = true;
  const records = state.mergeIds.map(id => state.allKeywords.find(k => k.id === id)).filter(/** @returns {record is KeywordRecord} */ record => record !== undefined);
  // Prefer the record carrying a place/taxon link, then coordinates.
  /** @param {KeywordRecord} k */ const score = k => (k.place_id ? 8 : 0) + (k.taxon_id ? 4 : 0) +
    (k.type === 'location' ? 2 : 0) + (k.latitude != null && k.longitude != null ? 1 : 0);
  records.sort((a, b) => score(b) - score(a));
  mergeTarget.replaceChildren(...records.map(k => {
    const option = document.createElement('option');
    option.value = String(k.id);
    option.textContent = (k.path || [k.name]).join(' → ') + ' · ' + k.type + (k.place_id ? ' · linked place' : '');
    return option;
  }));
  mergeDialog.showModal();
  refreshMergePreview();
}

export function bindMergeDialog() {
  /** @type {HTMLButtonElement} */ (document.getElementById('kwBulkMerge')).addEventListener('click', () => openMergeDialog([...state.selectedIds]));
  mergeTarget.addEventListener('change', function() {
    // A different surviving record has its own defaults; keeping the old
    // picks would silently apply one record's identity to another.
    state.mergeOverrides = {};
    state.mergeFieldsFor = null;
    refreshMergePreview();
  });
  mergeCancel.addEventListener('click', () => mergeDialog.close());
  mergeDialog.addEventListener('cancel', event => { if (state.mergeSaving) event.preventDefault(); });
  mergeDialog.addEventListener('close', () => {
    ++state.mergeRequest;
    if (state.mergeDebounce) clearTimeout(state.mergeDebounce);
    state.mergeToken = null;
    if (state.mergeMap) { state.mergeMap.remove(); state.mergeMap = null; state.mergeMapLayer = null; }
  });
  mergeConfirm.addEventListener('click', async function() {
    if (!state.mergeToken || state.mergeSaving) return;
    state.mergeSaving = true;
    mergeConfirm.disabled = true;
    mergeCancel.disabled = true;
    mergeTarget.disabled = true;
    mergeError.hidden = true;
    try {
      const response = await fetch('/api/keywords/merge', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({keyword_ids: state.mergeIds, target_id: Number(mergeTarget.value),
                              preview_token: state.mergeToken, overrides: state.mergeOverrides})
      });
      /** @type {KeywordMergePreview} */ const data = await response.json();
      if (!response.ok) throw new Error(data.error || 'Could not merge keywords.');
      mergeDialog.close();
      state.selectedIds.clear();
      await loadKeywords();
      if (!/** @type {HTMLElement} */ (document.getElementById('kwLocationReview')).hidden) await loadLocationMatches();
      showToast('Keywords merged into “' + data.resolved.name + '”.', 'success');
    } catch (error) {
      await refreshMergePreview();
      mergeError.textContent = keywordErrorMessage(error);
      mergeError.hidden = false;
    } finally {
      state.mergeSaving = false;
      mergeCancel.disabled = false;
      mergeTarget.disabled = false;
    }
  });
}
