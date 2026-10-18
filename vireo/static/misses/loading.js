// Loading misses: applying a payload to page state, loadMisses() with its
// request sequence guard, and the navbar's undo/redo hook.
// ES module.

import { state, CATEGORY_ORDER } from './state.js';
import { VireoFilter } from './shared.js';
import { getSinceParam, sortPhotos } from './helpers.js';
import { addFilterPayloadToBody, cloneMissFilterPayload, missFilterPayload } from './filters.js';
import { renderMissScopeBanner } from './scope.js';
import { previewMisses, setMissTuningDisabled } from './tuning.js';
import { render } from './render.js';

/** @param {MissPayload} data */ export function applyMissesPayload(data) {
  state.activeMissFloor = (data.config && data.config.detector_confidence != null)
    ? Number(data.config.detector_confidence)
    : null;
  state.missesData = {
    no_subject: sortPhotos(data.no_subject || []),
    clipped:    sortPhotos(data.clipped || []),
    oof:        sortPhotos(data.oof || []),
  };
  var present = new Set();
  CATEGORY_ORDER.forEach(function(cat) {
    (state.missesData[cat] || []).forEach(function(p) { present.add(p.id); });
  });
  state.selection.forEach(function(id) {
    if (!present.has(id)) state.selection.delete(id);
  });
  if (VireoFilter && VireoFilter.setResultTotal) {
    VireoFilter.setResultTotal(present.size);
  }
  if (VireoFilter && VireoFilter.setVisualInfo) {
    VireoFilter.setVisualInfo(data.visual || null);
  }
  render();
}

/* ---------- Fetch + render ---------- */
// The navbar's undo/redo page hook, which boot.js registers.
export async function afterHistoryChange() {
  await loadMisses();
}

export async function loadMisses() {
  var requestId = ++state.missLoadRequestId;
  var since = getSinceParam();
  renderMissScopeBanner();
  if (state.legacyScopeError) {
    // Deep link couldn't be resolved: render an empty grid so bulk reject
    // and recompute have nothing to accidentally widen to. The applied
    // filter snapshot stays empty; a subsequent user filter edit calls
    // clearLegacyScopeError() and loadMisses() again with the new scope.
    state.missesData = { no_subject: [], clipped: [], oof: [] };
    state.appliedMissFilters = cloneMissFilterPayload(missFilterPayload());
    render();
    var recomputeBtn = /** @type {HTMLButtonElement} */ (document.getElementById('missRecomputeBtn'));
    if (recomputeBtn) recomputeBtn.disabled = true;
    return;
  }
  if (state.previewActive) {
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
  if (requestId !== state.missLoadRequestId) return;
  if (!r.ok) {
    /** @type {HTMLElement} */ (document.getElementById('missesRoot')).innerHTML =
      '<div class="misses-empty-page">Failed to load misses.</div>';
    return;
  }
  var data = await r.json();
  if (requestId !== state.missLoadRequestId) return;
  state.appliedMissFilters = cloneMissFilterPayload(requestedFilters);
  applyMissesPayload(data);
  if (state.legacyScopeRecoveryPending) {
    state.legacyScopeRecoveryPending = false;
    if (state.missConfigLoaded) setMissTuningDisabled(false);
  }
}
