// Running and recomputing the analysis: storing results, the request body
// for the current scope, the busy state behind Apply, and the debounced
// reflow and regroup.
// Classic page script.

/** @param {CullSourceResults} data @param {string=} statusText @param {boolean=} scoped */
function setPipelineResults(data, statusText, scoped) {
  pipelineResults = data;
  if (!scoped) cullSourceResults = data;
  renderCullSpeciesOptions();
  rebuildCullDataFromPipeline();
  cullDirty = unappliedPinCount() > 0;
  var kept = cullData ? (cullData.pinned_count || 0) : 0;
  var parts = [];
  if (statusText) parts.push(statusText);
  // Say what the recompute did NOT touch, so a preserved decision never
  // looks like the analysis quietly agreed with you.
  if (kept) parts.push(kept + ' pinned decision' + (kept === 1 ? '' : 's') + ' kept');
  var status = /** @type {HTMLElement} */ (document.getElementById('cullStatus'));
  if (parts.length && status) {
    status.textContent = parts.join(' \u00b7 ');
    status.style.color = 'var(--accent)';
  }
}

/** @param {Record<string, number>} config @param {boolean=} sourceOnly */
function pipelineBody(config, sourceOnly) {
  /** @type {{config: Record<string, number>, save_cache: boolean, photo_ids?: number[], collection_id?: number}} */
  var body = {config: config, save_cache: false};
  if (!sourceOnly && hasCullFilters()) body.photo_ids = cullScopePhotoIds();
  else if (selectedCollectionId != null) body.collection_id = selectedCollectionId;
  return body;
}

function cullOperationPending() {
  return cullBusy || cullScopeFailed || cullAnalysisPending > 0 || _reflowTimer != null || _regroupTimer != null
    || !!document.querySelector('#deleteModal.open')
    || (window.vireoHistoryBusy && window.vireoHistoryBusy());
}

function updateCullApplyButton() {
  /** @type {HTMLButtonElement} */ (document.getElementById('applyBtn')).disabled = !!cullOperationPending();
  var editing = cullBusy || (window.vireoHistoryBusy && window.vireoHistoryBusy()) ||
    !!document.querySelector('#deleteModal.open');
  document.querySelectorAll('.cull-filters input, .cull-filters button, #cullCollection').forEach(function(el) { /** @type {HTMLInputElement} */ (el).disabled = !!editing; });
}


function doReflow() {
  if (_reflowTimer != null) clearTimeout(_reflowTimer);
  _reflowTimer = null;
  if (!pipelineResults) return runCulling();
  // Reflow also groups photos, so send both sets of current slider values.
  return recomputeCulling('/api/pipeline/reflow');
}

function doRegroupLive() {
  if (_regroupTimer != null) clearTimeout(_regroupTimer);
  _regroupTimer = null;
  return recomputeCulling('/api/pipeline/regroup-live');
}

function toggleCullSettings() {
  var panel = /** @type {HTMLElement} */ (document.getElementById('cullSettings'));
  panel.style.display = panel.style.display === 'none' ? '' : 'none';
}

async function runCulling() {
  var status = /** @type {HTMLElement} */ (document.getElementById('cullStatus'));
  status.textContent = 'Analyzing pipeline...';
  status.style.color = 'var(--text-dim)';
  return recomputeCulling('/api/pipeline/regroup-live', 'Analysis complete!', !cullSourceResults);
}

async function loadCullingResults() {
  try {
    var data = await safeFetch('/api/pipeline/page-init', {}, { toast: false });
    if (data && data.results) {
      setPipelineResults(data.results);
    } else {
      /** @type {HTMLElement} */ (document.getElementById('cullContent')).innerHTML =
        '<div style="text-align:center;padding:40px;color:var(--text-dim);">No pipeline results. Run Analyze for Culling after extracting pipeline features.</div>';
    }
  } catch(e) {}
}

function rebuildCullDataFromPipeline() {
  cullData = pipelineToCullData(pipelineResults);
  renderCulling();
}
