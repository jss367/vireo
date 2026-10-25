// The undo/redo page hooks: refuse while culling is busy or has unsaved
// pins, and refresh the photos' flags in place afterward.
// Classic page script.

/** Refuse an undo/redo while culling is updating or has unsaved pins. */
function cullBeforeHistoryChange() {
  if (cullOperationPending()) {
    showToast('Wait for culling to finish updating before undoing saved edits.', 'info');
    return false;
  }
  if (cullDirty) {
    showToast('Apply or clear your pinned culling decisions before undoing saved edits.', 'info');
    return false;
  }
  return true;
}

/**
 * Reload the photos' flags after an undo/redo, keeping the current grouping.
 *
 * @param {{undone?: string, redone?: string}} result
 */
async function cullAfterHistoryChange(result) {
  cullBusy = true;
  /** @type {HTMLButtonElement} */ (document.getElementById('applyBtn')).disabled = true;
  try {
    /** @type {CullSourceResults | null | undefined} */
    var results;
    if (pipelineResults) {
      // Cull analysis is ephemeral, including unfiltered runs. Keep this
      // grouping and refresh flags rather than restoring an older cache.
      var copy = /** @type {CullSourceResults} */ (JSON.parse(JSON.stringify(pipelineResults)));
      results = copy;
      var state = /** @type {{photos: Record<number, {flag?: string, flag_confirmation_status?: 'confirmed' | 'unverified'} | undefined>}} */ (await safeFetch('/api/pipeline/group/state', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({photo_ids: Array.from(new Set((cullSourceResults && cullSourceResults.photos || []).concat(copy.photos).map(function(photo) { return photo.id; })))}),
      }, {toast: false}));
      [results, cullSourceResults].forEach(function(source) {
        (source && source.photos || []).forEach(function(photo) {
          var saved = state.photos[photo.id];
          if (saved) {
            photo.flag = saved.flag;
            // Undo may have restored a legacy flag that no attribution matches.
            // Refresh confirmation from the server too, so an older Apply's
            // "confirmed" marker cannot survive on the restored flag value.
            photo.flag_confirmation_status = saved.flag_confirmation_status || 'unverified';
          }
        });
      });
      // Keep an applied Review pin only while live attribution still confirms
      // that clear. Undo can restore an unattributed none as well as a saved
      // keep/reject; neither may leave a stale pin overriding the live state.
      Object.keys(cullAppliedDecisions).forEach(function(pid) {
        if (cullAppliedDecisions[pid] !== 'review') return;
        var saved = state.photos[Number(pid)];
        if (!saved) return;
        if (saved.flag_confirmation_status === 'confirmed' && saved.flag === 'none') return;
        delete cullManualDecisions[pid];
        delete cullAppliedDecisions[pid];
      });
    } else {
      var data = /** @type {{results?: CullSourceResults} | null} */ (await safeFetch('/api/pipeline/page-init', {}, {toast: false}));
      results = data && data.results;
    }
    // page-init may return no results; both callees treat that as empty.
    syncCullSourceFlags(/** @type {CullSourceResults} */ (results));
    setPipelineResults(/** @type {CullSourceResults} */ (results),
      result.undone ? 'Undone: ' + result.undone : 'Redone: ' + result.redone, hasCullFilters());
  } catch (error) {
    cullData = null;
    renderCulling();
    throw error;
  } finally {
    cullBusy = false;
    updateCullApplyButton();
  }
}
