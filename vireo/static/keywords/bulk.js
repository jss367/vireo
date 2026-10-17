// Bulk actions over the selection: apply type and delete.
// Classic page script; load boot.js after all definitions.

// -- Bulk apply type --
function bindKeywordBulkApply() {
  document.getElementById('kwBulkApply').addEventListener('click', async function() {
    const newType = document.getElementById('kwBulkType').value;
    if (!newType) return;
    const ids = Array.from(selectedIds);
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
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) throw new Error(data.error || ('HTTP ' + resp.status));
        if (data && data.merged) {
          // Row was merged into a normalized-equal peer -- can't update in
          // place, will refetch after the loop rather than 100+ round trips.
          continue;
        }
        const kw = allKeywords.find(k => k.id === id);
        if (kw) kw.type = newType;
      } catch(e) {
        console.error('Failed to update', id, e);
        failed++;
        if (!firstError) firstError = e.message || 'request failed';
      }
    }
    selectedIds.clear();
    document.getElementById('kwBulkType').value = '';
    await loadKeywords();
    if (failed) {
      showToast('Could not change the type of ' + failed + ' of ' + ids.length +
        ' keyword(s): ' + firstError, 'error');
    }
  });
}

// -- Bulk delete --
async function deleteSelectedKeywords() {
  const ids = Array.from(selectedIds);
  if (!ids.length) return;
  if (!confirm('Delete ' + ids.length + ' keyword(s)? This will also remove them from all photos.')) return;

  let failed = 0;
  let firstError = '';
  for (const id of ids) {
    try {
      const resp = await fetch('/api/keywords/' + id, { method: 'DELETE' });
      if (!resp.ok) {
        const data = await resp.json().catch(() => ({}));
        throw new Error(data.error || ('HTTP ' + resp.status));
      }
      allKeywords = allKeywords.filter(k => k.id !== id);
      delete parentMap[id];
    } catch(e) {
      console.error('Failed to delete', id, e);
      failed++;
      if (!firstError) firstError = e.message || 'request failed';
    }
  }
  selectedIds.clear();
  await loadKeywords();
  if (failed) {
    showToast('Could not delete ' + failed + ' of ' + ids.length +
      ' keyword(s): ' + firstError, 'error');
  }
}
