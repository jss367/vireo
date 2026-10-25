// Bulk actions over the selection: apply type and delete.

import { state } from './state.js';
import { loadKeywords } from './loading.js';
import { keywordErrorMessage } from './merge-helpers.js';
import { showToast } from './shared.js';

// -- Bulk apply type --
export function bindKeywordBulkApply() {
  /** @type {HTMLButtonElement} */ (document.getElementById('kwBulkApply')).addEventListener('click', async function() {
    const newType = /** @type {HTMLSelectElement} */ (document.getElementById('kwBulkType')).value;
    if (!newType) return;
    const ids = Array.from(state.selectedIds);
    if (!ids.length) return;

    let failed = 0;
    let firstError = '';
    for (const id of ids) {
      try {
        const resp = await fetch('/api/keywords/' + id, {
          method: 'PUT',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({type: newType})
        });
        /** @type {KeywordMutation} */ const data = await resp.json().catch(() => ({}));
        if (!resp.ok) throw new Error(data.error || ('HTTP ' + resp.status));
        if (data && data.merged) {
          // Row was merged into a normalized-equal peer -- can't update in
          // place, will refetch after the loop rather than 100+ round trips.
          continue;
        }
        const kw = state.allKeywords.find(k => k.id === id);
        if (kw) kw.type = newType;
      } catch(e) {
        console.error('Failed to update', id, e);
        failed++;
        if (!firstError) firstError = keywordErrorMessage(e) || 'request failed';
      }
    }
    state.selectedIds.clear();
    /** @type {HTMLSelectElement} */ (document.getElementById('kwBulkType')).value = '';
    await loadKeywords();
    if (failed) {
      showToast('Could not change the type of ' + failed + ' of ' + ids.length +
        ' keyword(s): ' + firstError, 'error');
    }
  });
}

// -- Bulk delete --
export async function deleteSelectedKeywords() {
  const ids = Array.from(state.selectedIds);
  if (!ids.length) return;
  if (!confirm('Delete ' + ids.length + ' keyword(s)? This will also remove them from all photos.')) return;

  let failed = 0;
  let firstError = '';
  for (const id of ids) {
    try {
      const resp = await fetch('/api/keywords/' + id, { method: 'DELETE' });
      if (!resp.ok) {
        /** @type {KeywordMutation} */ const data = await resp.json().catch(() => ({}));
        throw new Error(data.error || ('HTTP ' + resp.status));
      }
      state.allKeywords = state.allKeywords.filter(k => k.id !== id);
      delete state.parentMap[id];
    } catch(e) {
      console.error('Failed to delete', id, e);
      failed++;
      if (!firstError) firstError = keywordErrorMessage(e) || 'request failed';
    }
  }
  state.selectedIds.clear();
  await loadKeywords();
  if (failed) {
    showToast('Could not delete ' + failed + ' of ' + ids.length +
      ' keyword(s): ' + firstError, 'error');
  }
}
