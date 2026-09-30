// Cached and scoped results, read-only guards, and scope controls.
// Classic page script; shared globals are initialized before boot.js runs.

// -- Init --

function renderDegradedBanner(enhancingMissing) {
  if (!enhancingMissing || enhancingMissing.length === 0) return;
  if (sessionStorage.getItem('degradedBannerDismissed')) return;
  var labelMap = {
    masks_partial: 'full mask coverage',
    embeddings: 'embeddings',
    eye_keypoints: 'eye keypoints',
    species_predictions: 'species predictions'
  };
  var labels = enhancingMissing.map(function(k) { return labelMap[k] || k; });
  document.getElementById('degradedBannerText').textContent =
    'These results were computed without ' + labels.join(', ')
    + '. Re-run those stages to improve quality.';
  document.getElementById('degradedBanner').style.display = '';
}

function dismissDegradedBanner() {
  sessionStorage.setItem('degradedBannerDismissed', '1');
  document.getElementById('degradedBanner').style.display = 'none';
}

function renderCacheScopeBanner(info) {
  var banner = document.getElementById('cacheScopeBanner');
  var text = document.getElementById('cacheScopeBannerText');
  if (!banner || !text) return;
  banner.style.display = 'none';
  if (!info || !info.is_partial) return;

  var shown = info.cached_photo_count || 0;
  var total = info.workspace_photo_count || 0;
  var missing = info.missing_photo_count || Math.max(total - shown, 0);
  text.innerHTML = '<strong>Showing ' + shown + ' of ' + total
    + ' active workspace photo' + (total === 1 ? '' : 's') + '.</strong> '
    + missing + ' photo' + (missing === 1 ? '' : 's')
    + ' are not in this review cache. This usually means the last Process run '
    + 'was scoped to selected folders, a collection, new images, or deselected photos.';
  banner.style.display = 'flex';
}

function cloneReviewData(data) {
  return data ? JSON.parse(JSON.stringify(data)) : null;
}

// Workspace/Collection scope shows a *view* of the pipeline for a different
// photo set — the result lives in `pipelineResults` alongside the persisted
// cache, but writing it back to /api/pipeline/save-cache (or triggering any
// server endpoint that rewrites the cache: detach-burst, detach-photo,
// /api/encounters/species) would clobber the workspace's saved review with
// this scoped subset. Callers use this to short-circuit those flows so
// editing is confined to the Latest review scope.
function isScopedReviewView() {
  return reviewScopeMode && reviewScopeMode !== 'cache';
}

function notifyReadOnlyScopedView() {
  var label = pipelineResults && pipelineResults.source === 'browse-selection' ? 'Browse selection' : (reviewScopeMode === 'collection' ? 'Collection' : 'Workspace');
  showToast(
    label + ' scope is view-only. Switch to Latest review to make changes.',
    'warning'
  );
}

function setReviewScopeStatus(text, isError) {
  var el = document.getElementById('reviewScopeStatus');
  if (!el) return;
  el.textContent = text || '';
  el.style.color = isError ? 'var(--danger)' : 'var(--text-dim)';
}

function updateReviewScopeControls() {
  var scopeSel = document.getElementById('reviewScopeSelect');
  var collectionSel = document.getElementById('reviewScopeCollectionSelect');
  if (scopeSel) scopeSel.value = reviewScopeMode;
  if (collectionSel) {
    collectionSel.style.display = reviewScopeMode === 'collection' ? '' : 'none';
    collectionSel.value = reviewScopeCollectionId || '';
  }
}

function updateReviewScopeStatusFromResults(data) {
  var n = data && data.summary ? data.summary.total_photos : null;
  if (n == null) {
    setReviewScopeStatus('');
    return;
  }
  setReviewScopeStatus(n + ' photo' + (n === 1 ? '' : 's'));
}

function populateReviewScopeCollections(collections) {
  reviewScopeCollections = Array.isArray(collections) ? collections : [];
  var sel = document.getElementById('reviewScopeCollectionSelect');
  if (!sel) return;
  sel.innerHTML = '<option value="">Select collection...</option>';
  reviewScopeCollections.forEach(function(c) {
    var opt = document.createElement('option');
    opt.value = String(c.id);
    // count_error collections have unresolvable rules; picking one would 400
    // the pipeline reflow request. Show it disabled with a hint instead of
    // offering a scope that can't run.
    if (c.count_error) {
      opt.disabled = true;
      opt.textContent = c.name + ' (unavailable — edit rules to fix)';
      opt.title = "This collection's rules could not be resolved. Edit it in Browse to make it usable.";
    } else if (c.has_visual) {
      // Reflow scopes to the collection via the rules-only reflow endpoint;
      // a visual collection would silently widen the scope to every
      // metadata match. Disable to match the server-side 400.
      opt.disabled = true;
      opt.textContent = c.name + ' (visual — open in Browse)';
      opt.title = 'Visual collections can only be used from Browse, where the filter bar resolves the visual-search clause.';
    } else {
      var available = c.available_photo_count != null
        ? c.available_photo_count
        : c.photo_count;
      var offline = Number(c.offline_photo_count || 0);
      var count = available != null
        ? ' (' + available + ' available' +
          (offline ? ', ' + offline + ' offline' : '') + ')'
        : '';
      opt.textContent = c.name + count;
    }
    sel.appendChild(opt);
  });
  updateReviewScopeControls();
}

function loadReviewScopeCollections() {
  safeFetch('/api/collections', {}, { toast: false })
    .then(populateReviewScopeCollections)
    .catch(function() { populateReviewScopeCollections([]); });
}

function reviewScopePayload(config) {
  // Latest review scope: send the pre-scope request shape (config only) so
  // the backend defaults save_cache=true with no photo_ids filter and
  // slider tuning persists to the saved cache. Adding photo_ids here (as
  // an earlier revision did) blocks the save branch in
  // api_pipeline_reflow/regroup_live and drops slider changes on reload.
  if (reviewScopeMode === 'cache') {
    return { config: config || {} };
  }
  var body = {
    config: config || {},
    save_cache: false,
  };
  if (reviewScopeMode === 'collection') {
    if (!reviewScopeCollectionId) return null;
    body.collection_id = parseInt(reviewScopeCollectionId, 10);
  }
  return body;
}

function applyReviewResults(data, info) {
  if (!data || !data.encounters) return;
  var empty = document.getElementById('emptyState');
  if (empty) empty.style.display = 'none';
  pipelineResults = data;
  resultsCacheInfo = info || null;
  collapsedEncounters = new Set();
  updatePipelineReviewSidebarVisibility();
  var sb = document.getElementById('summaryBar');
  if (sb) sb.style.display = '';
  updateSummaryBar(data.summary);
  var fb = document.getElementById('filterBar');
  if (fb) fb.style.display = '';
  applyPipelineReviewToolbarState();
  renderResults();
  checkPendingSync();
  refreshMissesReviewBtn();
  renderDegradedBanner(reviewReadiness && reviewReadiness.enhancing_missing);
  renderCacheScopeBanner(resultsCacheInfo);
  updateReviewScopeStatusFromResults(data);
  refreshLatestScopeSnapshotIfCurrent();
}

function scopedCacheInfoFor(data) {
  var workspaceTotal = reviewReadiness ? reviewReadiness.total_photos : (
    data && data.summary ? data.summary.total_photos : 0
  );
  var shown = data && data.summary ? data.summary.total_photos : 0;
  return {
    workspace_photo_count: workspaceTotal,
    cached_photo_count: shown,
    missing_photo_count: Math.max(workspaceTotal - shown, 0),
    is_partial: reviewScopeMode === 'cache' && shown < workspaceTotal,
    group_fingerprint_status: 'view',
    review_mode: data ? data.review_mode || null : null,
  };
}

// Sync cachedPipelineResults with the current view when we're in Latest
// review scope. cachedPipelineResults was originally only written at page
// init / computeReviewNow; without this, slider tunes, GRM apply, species
// confirmation, and detach — all of which mutate pipelineResults and save
// the cache while in Latest scope — would be lost the next time the user
// switched to Workspace/Collection and back, and a later save-cache from
// that stale view would overwrite the newer saved review on disk.
function refreshLatestScopeSnapshotIfCurrent() {
  if (reviewScopeMode !== 'cache') return;
  if (!pipelineResults) return;
  cachedPipelineResults = cloneReviewData(pipelineResults);
  cachedResultsCacheInfo = cloneReviewData(resultsCacheInfo);
}

function loadReviewScopeResults() {
  updateReviewScopeControls();
  // Bump the request sequence up front so that any scope fetch still in
  // flight from a prior scope choice is invalidated regardless of which
  // branch we take here. Otherwise a Workspace/Collection request that
  // resolves after the user switches back to Latest review could pass its
  // .then() guard and replace the cached view with stale scoped results.
  var seq = ++reviewScopeRequestSeq;
  if (reviewScopeMode === 'cache') {
    if (cachedPipelineResults) {
      applyReviewResults(
        cloneReviewData(cachedPipelineResults),
        cloneReviewData(cachedResultsCacheInfo)
      );
    }
    return;
  }

  var body = reviewScopePayload(Object.assign({}, getGroupingConfig(), getScoringConfig()));
  if (!body) {
    setReviewScopeStatus('Select one');
    return;
  }
  setReviewScopeStatus('Loading...');
  safeFetch('/api/pipeline/regroup-live', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body),
  }, { toast: false })
  .then(function(data) {
    if (seq !== reviewScopeRequestSeq || !data || data.error) return;
    applyReviewResults(data, scopedCacheInfoFor(data));
  })
  .catch(function(e) {
    if (seq !== reviewScopeRequestSeq) return;
    setReviewScopeStatus((e && e.message) || 'Could not load', true);
  });
}

function onReviewScopeModeChange(mode) {
  if (['cache', 'workspace', 'collection'].indexOf(mode) === -1) return;
  // Capture the current Latest-review state before switching away so a later
  // return to Latest restores the post-mutation view. Must run before
  // reviewScopeMode changes so refreshLatestScopeSnapshotIfCurrent still
  // matches on the outgoing (cache) mode.
  refreshLatestScopeSnapshotIfCurrent();
  reviewScopeMode = mode;
  updateReviewScopeControls();
  loadReviewScopeResults();
}

function onReviewScopeCollectionChange(value) {
  reviewScopeCollectionId = value || null;
  updateReviewScopeControls();
  loadReviewScopeResults();
}

function registerPipelineReviewScopeHook() {
    window.getPhotoExternalEditDisabledHint = function() {
      return isScopedReviewView()
        ? 'Switch to Latest review to make changes'
        : null;
    };
}
