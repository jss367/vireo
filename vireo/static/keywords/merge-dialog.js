// The merge dialog: debounced preview scheduling, refreshMergePreview,
// openMergeDialog, and the dialog's target, cancel, close and confirm handlers.
// Classic page script; load boot.js after all definitions.

/** @param {boolean} immediate */ function scheduleMergePreview(immediate) {
  if (mergeDebounce) clearTimeout(mergeDebounce);
  if (immediate) { refreshMergePreview(); return; }
  mergeConfirm.disabled = true;
  mergeDebounce = setTimeout(refreshMergePreview, 300);
}

async function refreshMergePreview() {
  const requestId = ++mergeRequest;
  const targetId = Number(mergeTarget.value);
  mergeToken = null;
  mergeConfirm.disabled = true;
  mergeError.hidden = true;
  if (mergeFieldsFor === null) mergePreview.textContent = 'Loading preview…';
  try {
    const response = await fetch('/api/keywords/merge-preview', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({keyword_ids: mergeIds, target_id: targetId,
                            overrides: mergeOverrides})
    });
    /** @type {KeywordMergePreview} */ const data = await response.json();
    if (requestId !== mergeRequest || !mergeDialog.open) return;
    if (!response.ok) throw new Error(data.error || 'Could not preview merge.');
    if (mergeFieldsFor !== targetId) {
      renderMergeFields(data);
      mergeFieldsFor = targetId;
    }
    syncCoordinateInputs(data);
    renderMergeSummary(data);
    renderMergeMap(data);
    mergeToken = data.preview_token || null;
    // No token means the preview is still asking a question (two different
    // linked places, say); merging now would have to guess the answer.
    mergeConfirm.disabled = !mergeToken;
  } catch (error) {
    if (requestId !== mergeRequest || !mergeDialog.open) return;
    if (mergeFieldsFor === null) mergePreview.textContent = '';
    mergeError.textContent = keywordErrorMessage(error);
    mergeError.hidden = false;
  }
}

/** @param {number[]} ids */ function openMergeDialog(ids) {
  if (ids.length < 2) return;
  mergeIds = [...ids];
  mergeOverrides = {};
  mergeFieldsFor = null;
  mergeFields.innerHTML = '';
  mergeMapEl.hidden = true;
  mergeMapNote.hidden = true;
  const records = mergeIds.map(id => allKeywords.find(k => k.id === id)).filter(/** @returns {record is KeywordRecord} */ record => record !== undefined);
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

function bindMergeDialog() {
  /** @type {HTMLButtonElement} */ (document.getElementById('kwBulkMerge')).addEventListener('click', () => openMergeDialog([...selectedIds]));
  mergeTarget.addEventListener('change', function() {
    // A different surviving record has its own defaults; keeping the old
    // picks would silently apply one record's identity to another.
    mergeOverrides = {};
    mergeFieldsFor = null;
    refreshMergePreview();
  });
  mergeCancel.addEventListener('click', () => mergeDialog.close());
  mergeDialog.addEventListener('cancel', event => { if (mergeSaving) event.preventDefault(); });
  mergeDialog.addEventListener('close', () => {
    ++mergeRequest;
    if (mergeDebounce) clearTimeout(mergeDebounce);
    mergeToken = null;
    if (mergeMap) { mergeMap.remove(); mergeMap = null; mergeMapLayer = null; }
  });
  mergeConfirm.addEventListener('click', async function() {
    if (!mergeToken || mergeSaving) return;
    mergeSaving = true;
    mergeConfirm.disabled = true;
    mergeCancel.disabled = true;
    mergeTarget.disabled = true;
    mergeError.hidden = true;
    try {
      const response = await fetch('/api/keywords/merge', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({keyword_ids: mergeIds, target_id: Number(mergeTarget.value),
                              preview_token: mergeToken, overrides: mergeOverrides})
      });
      /** @type {KeywordMergePreview} */ const data = await response.json();
      if (!response.ok) throw new Error(data.error || 'Could not merge keywords.');
      mergeDialog.close();
      selectedIds.clear();
      await loadKeywords();
      if (!/** @type {HTMLElement} */ (document.getElementById('kwLocationReview')).hidden) await loadLocationMatches();
      showToast('Keywords merged into “' + data.resolved.name + '”.', 'success');
    } catch (error) {
      await refreshMergePreview();
      mergeError.textContent = keywordErrorMessage(error);
      mergeError.hidden = false;
    } finally {
      mergeSaving = false;
      mergeCancel.disabled = false;
      mergeTarget.disabled = false;
    }
  });
}
