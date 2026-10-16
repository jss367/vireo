// Deleting the selected photos through the shared delete dialog.
// Classic page script; load boot.js after all definitions.

function removePhotosFromMissesData(ids) {
  var deleted = new Set(ids);
  CATEGORY_ORDER.forEach(function(cat) {
    missesData[cat] = (missesData[cat] || []).filter(function(p) {
      return !deleted.has(p.id);
    });
  });
  pruneSelectionToVisible();
}

function getSelectedPhotoRows() {
  var rowsById = {};
  CATEGORY_ORDER.forEach(function(cat) {
    (missesData[cat] || []).forEach(function(p) {
      if (selection.has(p.id) && !rowsById[p.id]) rowsById[p.id] = p;
    });
  });
  return Object.keys(rowsById).map(function(id) { return rowsById[id]; });
}

async function deleteSelectedMisses() {
  if (selection.size === 0) return;
  var rows = getSelectedPhotoRows();
  var ids = rows.map(function(p) { return p.id; });
  var companionCount = rows.filter(function(p) { return !!p.companion_path; }).length;

  var afterDelete = function(data) {
    // Rows the server retained after Trash failures must remain visible —
    // dropping them from ``missesData`` would hide files still on disk until
    // reload. Callback receives the same ``data`` shape as other delete
    // callbacks: ``failed_photo_ids`` lists IDs kept intentionally.
    var retained = new Set((data && data.failed_photo_ids) || []);
    var actuallyDeleted = ids.filter(function(id) { return !retained.has(id); });
    // Match browse.html: retained IDs stay selected so the user can retry
    // immediately. clearSelection() would drop everything, including the
    // rows they still need to act on.
    actuallyDeleted.forEach(function(id) { selection.delete(id); });
    removePhotosFromMissesData(actuallyDeleted);
    render();
    updateSelectionBar();
    loadMisses();
  };

  if (typeof showDeleteDialog === 'function') {
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
    if (typeof showToast === 'function') showToast('Delete failed', 'error');
  }
}
