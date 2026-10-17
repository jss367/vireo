// Loading misses: applying a payload to page state, loadMisses() with its
// request sequence guard, and the navbar's undo/redo hook.
// Classic page script; load boot.js after all definitions.

/** @param {MissPayload} data */ function applyMissesPayload(data) {
  activeMissFloor = (data.config && data.config.detector_confidence != null)
    ? Number(data.config.detector_confidence)
    : null;
  missesData = {
    no_subject: sortPhotos(data.no_subject || []),
    clipped:    sortPhotos(data.clipped || []),
    oof:        sortPhotos(data.oof || []),
  };
  var present = new Set();
  CATEGORY_ORDER.forEach(function(cat) {
    (missesData[cat] || []).forEach(function(p) { present.add(p.id); });
  });
  selection.forEach(function(id) {
    if (!present.has(id)) selection.delete(id);
  });
  if (window.VireoFilter && VireoFilter.setResultTotal) {
    VireoFilter.setResultTotal(present.size);
  }
  if (window.VireoFilter && VireoFilter.setVisualInfo) {
    VireoFilter.setVisualInfo(data.visual || null);
  }
  render();
}

/* ---------- Fetch + render ---------- */
window.afterHistoryChange = async function() {
  await loadMisses();
};

async function loadMisses() {
  var requestId = ++missLoadRequestId;
  var since = getSinceParam();
  renderMissScopeBanner();
  if (legacyScopeError) {
    // Deep link couldn't be resolved: render an empty grid so bulk reject
    // and recompute have nothing to accidentally widen to. The applied
    // filter snapshot stays empty; a subsequent user filter edit calls
    // clearLegacyScopeError() and loadMisses() again with the new scope.
    missesData = { no_subject: [], clipped: [], oof: [] };
    appliedMissFilters = cloneMissFilterPayload(missFilterPayload());
    render();
    var recomputeBtn = /** @type {HTMLButtonElement} */ (document.getElementById('missRecomputeBtn'));
    if (recomputeBtn) recomputeBtn.disabled = true;
    return;
  }
  if (previewActive) {
    // Re-derive from the preview endpoint with the live slider values
    // instead of replaying a cached payload that a mutating action may
    // have made stale.
    await previewMisses();
    return;
  }
  var requestedFilters = missFilterPayload();
  var body = addFilterPayloadToBody({}, requestedFilters);
  if (since) body.since = since;
  var r = await fetch('/api/misses', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (requestId !== missLoadRequestId) return;
  if (!r.ok) {
    /** @type {HTMLElement} */ (document.getElementById('missesRoot')).innerHTML =
      '<div class="misses-empty-page">Failed to load misses.</div>';
    return;
  }
  var data = await r.json();
  if (requestId !== missLoadRequestId) return;
  appliedMissFilters = cloneMissFilterPayload(requestedFilters);
  applyMissesPayload(data);
  if (legacyScopeRecoveryPending) {
    legacyScopeRecoveryPending = false;
    if (missConfigLoaded) setMissTuningDisabled(false);
  }
}
