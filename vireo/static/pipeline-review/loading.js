// Page initialization, empty states, readiness, and Browse handoff.
// Classic page script; shared globals are initialized before boot.js runs.

/** @param {ReviewReadiness | null} readiness */
function renderEmptyState(readiness) {
  var heading = /** @type {HTMLElement} */ (document.getElementById('emptyStateHeading'));
  var body = /** @type {HTMLElement} */ (document.getElementById('emptyStateBody'));
  var stages = /** @type {HTMLElement} */ (document.getElementById('readinessStages'));
  var actions = /** @type {HTMLElement} */ (document.getElementById('readinessActions'));
  /** @type {HTMLElement} */ (document.getElementById('emptyState')).style.display = '';

  // Per-stage rows
  /** @type {[string, number, number][]} */ var rows = [
    ['Photos', requireReviewValue(readiness).total_photos, requireReviewValue(readiness).total_photos],
    ['Masks', requireReviewValue(readiness).with_masks, requireReviewValue(readiness).mask_target_photos || requireReviewValue(readiness).total_photos],
    ['Embeddings', requireReviewValue(readiness).with_embeddings, requireReviewValue(readiness).embedding_target_photos || requireReviewValue(readiness).total_photos],
    ['Eye keypoints', requireReviewValue(readiness).with_eye_keypoint_attempts != null
      ? (requireReviewValue(readiness).with_eye_keypoint_attempts || 0)
      : requireReviewValue(readiness).with_eye_keypoints,
      requireReviewValue(readiness).eye_keypoint_target_photos != null
        ? (requireReviewValue(readiness).eye_keypoint_target_photos || 0)
        : requireReviewValue(readiness).total_photos],
    ['Species predictions', requireReviewValue(readiness).with_predictions, requireReviewValue(readiness).prediction_target_photos || requireReviewValue(readiness).total_photos],
  ];
  stages.innerHTML = rows.map(function(r) {
    var name = r[0], n = r[1], total = r[2];
    var cls = (n === total && requireReviewValue(total) > 0) ? 'stage-ok'
            : (requireReviewValue(n) > 0 ? 'stage-partial' : 'stage-missing');
    return '<span class="stage-name">' + name + '</span>'
         + '<span class="stage-count ' + cls + '">' + n + ' / ' + total + '</span>';
  }).join('');

  if (requireReviewValue(readiness).state === 'empty') {
    heading.textContent = 'No photos in this workspace';
    body.textContent = 'Add folders to this workspace from the Folders page.';
    actions.innerHTML = '';
    return;
  }

  if (requireReviewValue(readiness).state === 'insufficient') {
    heading.textContent = 'Not enough features to compute results yet';
    body.textContent = 'Run mask extraction on the Process page first '
      + '— the review page needs masks to score photo quality.';
    actions.innerHTML = '<a href="/pipeline" class="compute-btn" '
      + 'style="text-decoration:none;display:inline-block;">Open Process</a>';
    return;
  }

  // state === 'computable'
  heading.textContent = 'Ready to compute results';
  var enhancing = requireReviewValue(readiness).enhancing_missing || [];
  if (enhancing.length === 0) {
    body.textContent = 'All upstream stages are complete. '
      + 'Click below to group, score, and triage your photos.';
  } else {
    var labels = enhancing.map(function(k) {
      return ({masks_partial: 'full mask coverage',
               embeddings: 'embeddings',
               eye_keypoints: 'eye keypoints',
               species_predictions: 'species predictions'})[k] || k;
    });
    body.textContent = 'You can compute results now. Quality will be lower without: '
      + labels.join(', ') + '. Re-run those stages on the Process page later to improve.';
  }
  actions.innerHTML = '<button class="compute-btn" onclick="computeReviewNow()">'
    + 'Compute results now</button>';
}

function computeReviewNow() {
  var btn = /** @type {HTMLButtonElement | null} */ (document.querySelector('.readiness-actions .compute-btn'));
  if (btn) { btn.disabled = true; btn.textContent = 'Computing…'; }
  safeFetch('/api/pipeline/regroup-live', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({})
  }).then(function(data) {
    if (!data || !data.encounters) {
      if (btn) { btn.disabled = false; btn.textContent = 'Compute results now'; }
      return;
    }
    // Hide empty state, render results inline
    /** @type {HTMLElement} */ (document.getElementById('emptyState')).style.display = 'none';
    resultsCacheInfo = {
      workspace_photo_count: reviewReadiness ? reviewReadiness.total_photos : data.summary.total_photos,
      cached_photo_count: data.summary.total_photos,
      missing_photo_count: Math.max(
        (reviewReadiness ? reviewReadiness.total_photos : data.summary.total_photos)
          - data.summary.total_photos,
        0
      ),
      is_partial: false,
      group_fingerprint_status: 'current',
      review_mode: data.review_mode || null
    };
    cachedPipelineResults = cloneReviewData(data);
    cachedResultsCacheInfo = cloneReviewData(resultsCacheInfo);
    // Parity with initPipelineReviewPage's happy path:
    if (workspaceOverrides && workspaceOverrides.review_min_confidence != null) {
      minConfidence = workspaceOverrides.review_min_confidence;
      /** @type {HTMLInputElement} */ (document.getElementById('confSlider')).value = String(minConfidence);
      /** @type {HTMLElement} */ (document.getElementById('confSliderVal')).textContent = minConfidence + '%';
    }
    restorePipelineReviewViewState();
    applyReviewResults(data, resultsCacheInfo);
    openRequestedGroupReviewFromUrl();
    loadReviewScopeOptions();
  }).catch(function() {
    // safeFetch toasts the error; re-enable the button so the user can retry.
    if (btn) { btn.disabled = false; btn.textContent = 'Compute results now'; }
  });
}

function initPipelineReviewPage() {
  initPipelineSidebarState();
  if (isBrowseBurstReviewRequest()) {
    openBrowseBurstReviewFromStorage();
    return;
  }
  loadPipelinePageInit();
}

function loadPipelinePageInit() {
  safeFetch('/api/pipeline/page-init', {}, { toast: false })
  .then(function(data) {
    pipelineReviewWorkspaceId = data.workspace_id ?? null;
    // Capture overrides regardless of state so computeReviewNow() can apply
    // the same review_min_confidence logic when the user clicks Compute.
    workspaceOverrides = (data && data.workspace_overrides) || null;
    // Capture readiness so computeReviewNow() can re-render the degraded
    // banner with the current missing-features set after compute completes.
    reviewReadiness = (data && data.review_readiness) || null;
    if (!data || !data.results) {
      renderEmptyState(data && data.review_readiness ? data.review_readiness
                                                     : {state: 'empty', total_photos: 0, with_masks: 0, with_embeddings: 0, with_eye_keypoints: 0, with_predictions: 0});
      return;
    }
    // Hide empty state, show results UI
    var empty = /** @type {HTMLElement} */ (document.getElementById('emptyState'));
    if (empty) empty.style.display = 'none';

    resultsCacheInfo = data.results_cache_info || null;
    cachedPipelineResults = cloneReviewData(data.results);
    cachedResultsCacheInfo = cloneReviewData(resultsCacheInfo);

    if (data.workspace_overrides && data.workspace_overrides.review_min_confidence != null) {
      minConfidence = data.workspace_overrides.review_min_confidence;
      /** @type {HTMLInputElement} */ (document.getElementById('confSlider')).value = String(minConfidence);
      /** @type {HTMLElement} */ (document.getElementById('confSliderVal')).textContent = minConfidence + '%';
    }

    restorePipelineReviewViewState();
    applyReviewResults(data.results, resultsCacheInfo);
    openRequestedGroupReviewFromUrl();
    loadReviewScopeOptions();
  }).catch(function(error) {
    var empty = /** @type {HTMLElement} */ (document.getElementById('emptyState'));
    if (empty) {
      empty.style.display = '';
      empty.textContent = 'Could not load review: ' + (error.message || error) + '. Reload to try again.';
    }
    showToast('Could not load review: ' + (error.message || error), 'error');
  });
}

/* Show a "Review misses (N)" shortcut in the summary bar when the current
 * pipeline run actually recomputed miss flags. Gated on
 * currentReviewResults().miss_computed_at (set by pipeline_job's miss_stage);
 * without the marker we'd surface stale miss flags from a prior run as
 * current-run misses — e.g. when miss_enabled=false or the stage was
 * skipped. The link is scoped to that same timestamp so /misses only
 * shows misses written during this run. */
function refreshMissesReviewBtn() {
  var btn = /** @type {HTMLAnchorElement} */ (document.getElementById('missesReviewBtn'));
  if (!btn || !pipelineResults) return;
  var sinceTs = currentReviewResults().miss_computed_at;
  if (!sinceTs) return;  // miss stage didn't run this pipeline

  fetch('/api/misses?since=' + encodeURIComponent(sinceTs || '')).then(function(r) {
    if (!r.ok) return null;
    return r.json();
  }).then(function(data) {
    if (!data) return;
    var all = (data.no_subject || []).concat(data.clipped || []).concat(data.oof || []);
    var count = all.length;
    if (count === 0) return;  // stays hidden
    /** @type {HTMLElement} */ (document.getElementById('missesReviewCount')).textContent = count;
    btn.href = '/misses?since=' + encodeURIComponent(sinceTs || '');
    btn.style.display = '';
  }).catch(function() { /* no-op */ });
}

function isBrowseBurstReviewRequest() {
  var params = new URLSearchParams(window.location.search || '');
  return params.get('browse_burst') === '1';
}

function browseBurstStoredIds() {
  try {
    var raw = window.sessionStorage.getItem('vireo.browseBurstReviewIds');
    var parsed = raw ? JSON.parse(raw) : [];
    if (!Array.isArray(parsed)) return [];
    /** @type {Record<number,boolean>} */ var seen = {};
    return parsed.map(function(id) { return parseInt(String(id), 10); })
      .filter(function(id) {
        if (!Number.isInteger(id) || seen[id]) return false;
        seen[id] = true;
        return true;
      });
  } catch (e) {
    return [];
  }
}

function renderBrowseBurstReviewShell() {
  var empty = /** @type {HTMLElement} */ (document.getElementById('emptyState'));
  if (empty) empty.style.display = 'none';
  var sb = /** @type {HTMLElement} */ (document.getElementById('summaryBar'));
  if (sb) sb.style.display = '';
  var fb = /** @type {HTMLElement} */ (document.getElementById('filterBar'));
  if (fb) fb.style.display = '';
  var ss = /** @type {HTMLElement} */ (document.getElementById('sidebarScoring'));
  if (ss) ss.style.display = 'none';
  var sg = /** @type {HTMLElement} */ (document.getElementById('sidebarGrouping'));
  if (sg) sg.style.display = 'none';
}

// Drop the one-shot Browse→burst handoff (session storage + ?browse_burst=1).
// Only call this once the burst workflow has actually completed (flags
// applied) or we've decided to degrade to normal review — clearing it while
// the modal is still in play makes Esc/× and the /group/state retry path
// non-recoverable.
function clearBrowseBurstHandoff() {
  try {
    window.sessionStorage.removeItem('vireo.browseBurstReviewIds');
    window.history.replaceState(null, '', '/pipeline/review');
  } catch (e) {}
}

// Stale URL, cleared session storage, or a failed handoff should not strand
// the user on an empty page reachable only from Browse. Clear the one-shot
// handoff state and degrade to the standard page-init flow so the route
// stays self-recoverable on reload.
/** @param {string} message */
function fallbackToNormalPipelineReview(message) {
  clearBrowseBurstHandoff();
  if (message) showToast(message, 'error');
  loadPipelinePageInit();
}

function openBrowseBurstReviewFromStorage() {
  var ids = browseBurstStoredIds();
  if (ids.length < 2) {
    fallbackToNormalPipelineReview('Burst review selection was missing — showing the latest pipeline review instead.');
    return;
  }

  safeFetch('/api/pipeline/selection-results', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({photo_ids: ids}),
  }).then(function(data) {
    if (!data || !data.photos || data.photos.length < 2 || !data.encounters || data.encounters.length === 0) {
      fallbackToNormalPipelineReview('Could not load the selected photos for burst review — showing the latest pipeline review instead.');
      return;
    }

    /** @type {{encIdx: number; burstIdx: number; photoId: number} | null} */ var target = null;
    data.encounters.some(function(enc, encIdx) {
      var bursts = Array.isArray(enc.bursts) ? enc.bursts : [];
      return bursts.some(function(burst, burstIdx) {
        var firstIds = burst && (burst.photo_ids || burst);
        if (!firstIds || !firstIds.length) return false;
        target = { encIdx: encIdx, burstIdx: burstIdx, photoId: requireReviewValue(firstIds[0]) };
        return true;
      });
    });
    if (!target) {
      fallbackToNormalPipelineReview('Could not find a burst to review — showing the latest pipeline review instead.');
      return;
    }

    pipelineReviewWorkspaceId = data.workspace_id ?? null;
    pipelineResults = data;
    currentReviewResults().source = 'browse-selection';

    collapsedEncounters = new Set();
    renderBrowseBurstReviewShell();
    updateSummaryBar(currentReviewResults().summary);
    renderResults();
    // Keep the handoff state (session storage + ?browse_burst=1) in place so
    // closing the modal via Esc/× or hitting the /group/state retry path stays
    // recoverable across a plain reload. clearBrowseBurstHandoff() runs only
    // once the user applies (workflow complete) or we fall back to normal review.
    /** @type {{encIdx: number; burstIdx: number; photoId: number}} */ const selectedTarget = requireReviewValue(target); openGroupReview(selectedTarget.encIdx, selectedTarget.burstIdx, selectedTarget.photoId);
  }).catch(function() {
    fallbackToNormalPipelineReview('Could not open burst review — showing the latest pipeline review instead.');
  });
}

// Recover from a closed/errored burst modal. If the temporary group is still
// in memory (same-page Esc/× or /group/state failure), just reopen it.
// Otherwise (e.g. after a reload) re-run the handoff from session storage.
function reopenBrowseBurstReview() {
  if (pipelineResults && currentReviewResults().source === 'browse-selection') {
    var enc = currentReviewResults().encounters && currentReviewResults().encounters[0];
    var firstId = enc && enc.photo_ids && enc.photo_ids[0];
    if (firstId) {
      renderBrowseBurstReviewShell();
      openGroupReview(0, 0, firstId);
      return;
    }
  }
  openBrowseBurstReviewFromStorage();
}
