// Deleting the selected photos through the shared delete dialog.
// ES module.

import { state, CATEGORY_ORDER } from './state.js';
import { hasDeleteDialog, showDeleteDialog, showToast } from './shared.js';
import { loadMisses } from './loading.js';
import { render } from './render.js';
import { pruneSelectionToVisible } from './photo-open.js';
import { updateSelectionBar } from './selection.js';

/** @param {number[]} ids */ function removePhotosFromMissesData(ids) {
  var deleted = new Set(ids);
  CATEGORY_ORDER.forEach(function(cat) {
    state.missesData[cat] = (state.missesData[cat] || []).filter(function(p) {
      return !deleted.has(p.id);
    });
  });
  pruneSelectionToVisible();
}

function getSelectedPhotoRows() {
  /** @type {Record<number,MissPhoto>} */ var rowsById = {};
  CATEGORY_ORDER.forEach(function(cat) {
    (state.missesData[cat] || []).forEach(function(p) {
      if (state.selection.has(p.id) && !rowsById[p.id]) rowsById[p.id] = p;
    });
  });
  return Object.values(rowsById);
}

export async function deleteSelectedMisses() {
  if (state.selection.size === 0) return;
  var rows = getSelectedPhotoRows();
  var ids = rows.map(function(p) { return p.id; });
  var companionCount = rows.filter(function(p) { return !!p.companion_path; }).length;

  /** @param {{failed_photo_ids?:number[]}} data */ var afterDelete = function(data) {
    // Rows the server retained after Trash failures must remain visible —
    // dropping them from ``missesData`` would hide files still on disk until
    // reload. Callback receives the same ``data`` shape as other delete
    // callbacks: ``failed_photo_ids`` lists IDs kept intentionally.
    var retained = new Set((data && data.failed_photo_ids) || []);
    var actuallyDeleted = ids.filter(function(id) { return !retained.has(id); });
    // Match browse.html: retained IDs stay selected so the user can retry
    // immediately. clearSelection() would drop everything, including the
    // rows they still need to act on.
    actuallyDeleted.forEach(function(id) { state.selection.delete(id); });
    removePhotosFromMissesData(actuallyDeleted);
    render();
    updateSelectionBar();
    loadMisses();
  };

  if (hasDeleteDialog()) {
    showDeleteDialog(ids, companionCount, afterDelete);
    return;
  }

  if (!confirm('Delete ' + ids.length + ' selected photo' + (ids.length === 1 ? '' : 's') + '?')) return;
  try {
    var r = await fetch('/api/batch/delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ photo_ids: ids, mode: 'vireo' }),
    });
    if (!r.ok) throw new Error('http ' + r.status);
    var data = await r.json();
    afterDelete(data);
  } catch (err) {
    showToast('Delete failed', 'error');
  }
}
