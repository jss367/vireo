// Cached and scoped results, read-only guards, and scope controls.
// Classic page script; shared globals are initialized before boot.js runs.

// -- Init --

/** @param {string[] | null | undefined} enhancingMissing */
function renderDegradedBanner(enhancingMissing) {
  if (!enhancingMissing || enhancingMissing.length === 0) return;
  if (sessionStorage.getItem('degradedBannerDismissed')) return;
  /** @type {Record<string,string>} */ var labelMap = {
    masks_partial: 'full mask coverage',
    embeddings: 'embeddings',
    eye_keypoints: 'eye keypoints',
    species_predictions: 'species predictions'
  };
  var labels = enhancingMissing.map(function(k) { return labelMap[k] || k; });
  /** @type {HTMLElement} */ (document.getElementById('degradedBannerText')).textContent =
    'These results were computed without ' + labels.join(', ')
    + '. Re-run those stages to improve quality.';
  /** @type {HTMLElement} */ (document.getElementById('degradedBanner')).style.display = '';
}

function dismissDegradedBanner() {
  sessionStorage.setItem('degradedBannerDismissed', '1');
  /** @type {HTMLElement} */ (document.getElementById('degradedBanner')).style.display = 'none';
}

/** @param {ReviewCacheInfo | null} info */
function renderCacheScopeBanner(info) {
  var banner = /** @type {HTMLElement} */ (document.getElementById('cacheScopeBanner'));
  var text = /** @type {HTMLElement} */ (document.getElementById('cacheScopeBannerText'));
  if (!banner || !text) return;
  banner.style.display = 'none';

  var scopeIds = activeReviewScopeFilter();
  if (scopeIds) {
    // Folder/Collection filter: the question is whether every photo the user
    // picked is in the saved review, not whether the review covers the
    // whole workspace.
    var reviewed = countReviewedPhotosInScope(scopeIds);
    var inScope = reviewScopeTotal || 0;
    var unreviewed = Math.max(inScope - reviewed, 0);
    if (!unreviewed) return;
    var noun = reviewScopeMode === 'folder' ? 'this folder' : 'this collection';
    text.innerHTML = '<strong>' + unreviewed + ' of ' + inScope + ' photo'
      + (inScope === 1 ? '' : 's') + ' in ' + noun + ' '
      + (unreviewed === 1 ? 'is' : 'are') + ' not in the saved review.</strong> '
      + 'Run Process on ' + noun + ' to include ' + (unreviewed === 1 ? 'it' : 'them') + '.';
    banner.style.display = 'flex';
    return;
  }

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

/** @template T @param {T} data @returns {T} */
function cloneReviewData(data) {
  return data ? JSON.parse(JSON.stringify(data)) : data;
}

// Whole workspace scope, and a folder/collection the user chose to regroup,
// show a regroup-live *view* of a different photo set — the result lives in
// `pipelineResults` alongside the persisted cache, but writing it back to
// /api/pipeline/save-cache (or triggering any server endpoint that rewrites
// the cache: detach-burst, detach-photo, /api/encounters/species) would
// clobber the workspace's saved review with this regrouped subset. Callers
// use this to short-circuit those flows. Folder and Collection scopes that
// only filter the saved review are not view-only: pipelineResults is still
// the full saved review there.
function isScopedReviewView() {
  return reviewScopeMode === 'workspace' || reviewScopeRegrouped;
}

function isReviewScopeFilterMode() {
  return reviewScopeMode === 'folder' || reviewScopeMode === 'collection';
}

// The photo-ID set the saved review is filtered to, or null when every
// photo shows (All reviewed photos, or a regrouped view, whose results
// already contain only the scope's photos).
function activeReviewScopeFilter() {
  if (!isReviewScopeFilterMode() || reviewScopeRegrouped) return null;
  return reviewScopePhotoIds;
}

/** @param {number} photoId */
function photoInReviewScope(photoId) {
  var ids = activeReviewScopeFilter();
  return !ids || ids.has(photoId);
}

// -- Scope write guard --
//
// Folder and Collection scopes hide photos but leave the saved review
// editable, so every path that turns a photo ID into a write target must
// agree on which IDs are fair game. These two helpers are that single rule;
// callers use them at the shared choke points instead of re-checking scope:
// opening the inspector (openInspect) or lightbox (openPipelineLightbox), the
// native Photo menu selection (getActiveSelection), the context-menu builder,
// and the writers themselves (setPipelineReviewFlag, setPipelineReviewRating,
// setPipelineReviewGroupFlag, setWildlifeExcludedFor, addToCollection,
// batchAddKeyword, openPipelinePhotoEditor, detachPhoto). Out-of-scope IDs
// are dropped silently. Scope changes and membership refreshes reconcile
// remembered targets through reconcileReviewScopeTargets().
//
// Group Review is the deliberate exception: it shows and edits the whole
// burst, including members outside the scope, so the members of the open
// Group Review session stay actionable. Nothing else changes when no filter
// is active (All reviewed photos, regrouped views, browse selection).
/** @param {number} photoId */
function photoInOpenGroupReview(photoId) {
  if (typeof grmState === 'undefined' || !grmState || !Array.isArray(grmState.items)) {
    return false;
  }
  var overlay = /** @type {HTMLElement} */ (document.getElementById('grmOverlay'));
  if (!overlay || !overlay.classList.contains('open')) return false;
  return grmState.items.some(function(p) { return p && p.id === photoId; });
}

/** @param {number} photoId */
function isPhotoActionableInReviewScope(photoId) {
  var ids = activeReviewScopeFilter();
  if (!ids) return true;
  var pid = parseInt(String(photoId), 10);
  if (!pid) return false;
  return ids.has(pid) || photoInOpenGroupReview(pid);
}

/** @param {number[]} photoIds */
function photoIdsInReviewScope(photoIds) {
  return (photoIds || []).filter(isPhotoActionableInReviewScope);
}

// Drop remembered write targets that the current scope no longer allows: the
// right-click selection, the native menu's pinned selection, the legacy
// inspector, and the active lightbox. When the inspected photo stays in
// scope, rebuild the inspector so its encounter filmstrip drops thumbnails
// for photos that just left. The lightbox gets its read-only state refreshed
// against its current photo so a photo that just left scope stops accepting
// destructive controls (lightboxDelete and adjustments guard on _lbReadOnly
// only).
function reconcileReviewScopeTargets() {
  filterPipelineReviewContextSelectionToScope();
  // refreshPipelineReviewLightboxReadOnlyState lives in group-session.js and
  // is defined by the time any user action can reach us, but the regression
  // harness extracts scope.js functions one at a time, so guard the call.
  if (typeof refreshPipelineReviewLightboxReadOnlyState === 'function') {
    refreshPipelineReviewLightboxReadOnlyState();
  }
  if (typeof inspectPhotoId === 'undefined' || inspectPhotoId == null) return;
  if (!isPhotoActionableInReviewScope(inspectPhotoId)) {
    closeInspect();
  } else {
    openInspect(inspectPhotoId);
  }
}

/** @param {Set<number>} ids */
function countReviewedPhotosInScope(ids) {
  if (!pipelineResults || !ids) return 0;
  return (currentReviewResults().photos || []).filter(function(p) {
    return ids.has(p.id);
  }).length;
}

function scopedViewEditHint() {
  if (pipelineResults && currentReviewResults().source === 'browse-selection') {
    return 'Browse selection is view-only';
  }
  if (reviewScopeRegrouped) return 'Click Show saved grouping to make changes';
  return 'Switch Scope to All reviewed photos to make changes';
}

function scopedViewReadOnlyMessage() {
  if (pipelineResults && currentReviewResults().source === 'browse-selection') {
    return 'Browse selection is view-only.';
  }
  var label = reviewScopeRegrouped ? 'This regrouped view' : 'Whole workspace (regrouped)';
  return label + ' is view-only. ' + scopedViewEditHint() + '.';
}

function notifyReadOnlyScopedView() {
  showToast(scopedViewReadOnlyMessage(), 'warning');
}

/** @param {string} text @param {boolean} [isError] */
function setReviewScopeStatus(text, isError) {
  var el = /** @type {HTMLElement} */ (document.getElementById('reviewScopeStatus'));
  if (!el) return;
  el.textContent = text || '';
  el.style.color = isError ? 'var(--danger)' : 'var(--text-dim)';
}

function reviewScopeSelectionId() {
  if (reviewScopeMode === 'folder') return reviewScopeFolderId;
  if (reviewScopeMode === 'collection') return reviewScopeCollectionId;
  return null;
}

function updateReviewScopeControls() {
  var scopeSel = /** @type {HTMLSelectElement} */ (document.getElementById('reviewScopeSelect'));
  var folderSel = /** @type {HTMLSelectElement} */ (document.getElementById('reviewScopeFolderSelect'));
  var collectionSel = /** @type {HTMLSelectElement} */ (document.getElementById('reviewScopeCollectionSelect'));
  var regroupBtn = /** @type {HTMLButtonElement} */ (document.getElementById('reviewScopeRegroupBtn'));
  if (scopeSel) scopeSel.value = reviewScopeMode;
  if (folderSel) {
    folderSel.style.display = reviewScopeMode === 'folder' ? '' : 'none';
    folderSel.value = String(reviewScopeFolderId || '');
  }
  if (collectionSel) {
    collectionSel.style.display = reviewScopeMode === 'collection' ? '' : 'none';
    collectionSel.value = String(reviewScopeCollectionId || '');
  }
  if (regroupBtn) {
    regroupBtn.style.display = isReviewScopeFilterMode() && reviewScopeSelectionId() ? '' : 'none';
    regroupBtn.textContent = reviewScopeRegrouped ? 'Show saved grouping' : 'Regroup';
    regroupBtn.setAttribute('aria-pressed', reviewScopeRegrouped ? 'true' : 'false');
    regroupBtn.title = reviewScopeRegrouped
      ? 'Go back to the saved review, limited to these photos'
      : 'Rerun grouping and scoring on only these photos. View-only: the saved review is unchanged.';
  }
}

/** @param {ReviewResults} data */
function updateReviewScopeStatusFromResults(data) {
  var scopeIds = activeReviewScopeFilter();
  if (scopeIds) {
    var reviewed = countReviewedPhotosInScope(scopeIds);
    var total = reviewScopeTotal || 0;
    setReviewScopeStatus(reviewed === total
      ? reviewed + ' photo' + (reviewed === 1 ? '' : 's')
      : reviewed + ' of ' + total + ' reviewed');
    return;
  }
  var n = data && data.summary ? data.summary.total_photos : null;
  if (n == null) {
    setReviewScopeStatus('');
    return;
  }
  setReviewScopeStatus(n + ' photo' + (n === 1 ? '' : 's')
    + (reviewScopeRegrouped ? ', regrouped' : ''));
}

/** @param {ReviewCollection[]} collections */
function populateReviewScopeCollections(collections) {
  reviewScopeCollections = Array.isArray(collections) ? collections : [];
  var sel = /** @type {HTMLSelectElement} */ (document.getElementById('reviewScopeCollectionSelect'));
  if (!sel) return;
  sel.innerHTML = '<option value="">Select collection...</option>';
  reviewScopeCollections.forEach(function(c) {
    var opt = document.createElement('option');
    opt.value = String(c.id);
    // count_error collections have unresolvable rules; picking one would 400
    // the scope request. Show it disabled with a hint instead of offering a
    // scope that can't run.
    if (c.count_error) {
      opt.disabled = true;
      opt.textContent = c.name + ' (unavailable — edit rules to fix)';
      opt.title = "This collection's rules could not be resolved. Edit it in Browse to make it usable.";
    } else if (c.has_visual) {
      // The scope endpoint resolves collection rules only; a visual
      // collection would silently widen the scope to every metadata match.
      // Disable to match the server-side 400.
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

// /api/folders lists visible folders ordered by path with parents rewritten
// to the nearest visible ancestor. Indent by depth and count each folder's
// whole subtree, because the scope includes subfolders.
/** @param {ReviewFolder[]} folders */
function populateReviewScopeFolders(folders) {
  reviewScopeFolders = Array.isArray(folders) ? folders : [];
  var sel = /** @type {HTMLSelectElement} */ (document.getElementById('reviewScopeFolderSelect'));
  if (!sel) return;
  /** @type {Record<number,ReviewFolderNode>} */ var byId = {};
  reviewScopeFolders.forEach(function(f) {
    byId[f.id] = {folder: f, children: [], total: Number(f.photo_count || 0)};
  });
  /** @type {ReviewFolderNode[]} */ var roots = [];
  reviewScopeFolders.forEach(function(f) {
    var parent = f.parent_id != null ? byId[f.parent_id] : null;
    if (parent) parent.children.push(requireReviewValue(byId[f.id]));
    else roots.push(requireReviewValue(byId[f.id]));
  });
  function subtreeTotal(/** @type {ReviewFolderNode} */ node) {
    node.children.forEach(function(child) { node.total += subtreeTotal(child); });
    return node.total;
  }
  roots.forEach(subtreeTotal);
  sel.innerHTML = '<option value="">Select folder...</option>';
  function addOptions(/** @type {ReviewFolderNode} */ node, /** @type {number} */ depth) {
    var opt = document.createElement('option');
    opt.value = String(node.folder.id);
    opt.textContent = '  '.repeat(depth) + node.folder.name
      + ' (' + node.total + ')';
    opt.title = node.folder.path || node.folder.name;
    sel.appendChild(opt);
    node.children.forEach(function(child) { addOptions(child, depth + 1); });
  }
  roots.forEach(function(root) { addOptions(root, 0); });
  updateReviewScopeControls();
}

function loadReviewScopeOptions() {
  safeFetch('/api/collections', {}, { toast: false })
    .then(populateReviewScopeCollections)
    .catch(function() { populateReviewScopeCollections([]); });
  safeFetch('/api/folders', {}, { toast: false })
    .then(populateReviewScopeFolders)
    .catch(function() { populateReviewScopeFolders([]); });
}

/** @param {Record<string,unknown>} config */
function reviewScopePayload(config) {
  // Saved review (All reviewed photos, or a folder/collection filter over
  // it): send the pre-scope request shape (config only) so the backend
  // defaults save_cache=true with no photo_ids filter and slider tuning
  // persists to the saved cache. Adding photo_ids here (as an earlier
  // revision did) blocks the save branch in api_pipeline_reflow/regroup_live
  // and drops slider changes on reload. The folder/collection filter is
  // reapplied client-side to the returned results.
  if (!isScopedReviewView()) {
    return { config: config || {} };
  }
  /** @type {{config: Record<string,unknown>; save_cache: boolean; collection_id?: number; photo_ids?: number[]}} */ var body = {
    config: config || {},
    save_cache: false,
  };
  if (reviewScopeMode === 'collection') {
    if (!reviewScopeCollectionId) return null;
    body.collection_id = Number(reviewScopeCollectionId);
  } else if (reviewScopeMode === 'folder') {
    if (!reviewScopePhotoIds) return null;
    body.photo_ids = Array.from(reviewScopePhotoIds);
  }
  return body;
}

/** @param {ReviewResults} data @param {ReviewCacheInfo | null} info */
function applyReviewResults(data, info) {
  if (!data || !data.encounters) return;
  var empty = /** @type {HTMLElement} */ (document.getElementById('emptyState'));
  if (empty) empty.style.display = 'none';
  pipelineResults = data;
  resultsCacheInfo = info || null;
  collapsedEncounters = new Set();
  updatePipelineReviewSidebarVisibility();
  var sb = /** @type {HTMLElement} */ (document.getElementById('summaryBar'));
  if (sb) sb.style.display = '';
  updateSummaryBar(data.summary);
  var fb = /** @type {HTMLElement} */ (document.getElementById('filterBar'));
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

/** @param {ReviewResults} data */
function scopedCacheInfoFor(data) {
  var workspaceTotal = reviewReadiness ? reviewReadiness.total_photos : (
    data && data.summary ? data.summary.total_photos : 0
  );
  var shown = data && data.summary ? data.summary.total_photos : 0;
  return {
    workspace_photo_count: workspaceTotal,
    cached_photo_count: shown,
    missing_photo_count: Math.max(workspaceTotal - shown, 0),
    is_partial: !isScopedReviewView() && shown < workspaceTotal,
    group_fingerprint_status: 'view',
    review_mode: data ? data.review_mode || null : null,
  };
}

// Sync cachedPipelineResults with the current view whenever it is the saved
// review (All reviewed photos, or a folder/collection filter over it).
// cachedPipelineResults was originally only written at page init /
// computeReviewNow; without this, slider tunes, GRM apply, species
// confirmation, and detach — all of which mutate pipelineResults and save
// the cache — would be lost the next time the user switched to a regrouped
// view and back, and a later save-cache from that stale view would
// overwrite the newer saved review on disk.
function refreshLatestScopeSnapshotIfCurrent() {
  if (isScopedReviewView()) return;
  if (!pipelineResults) return;
  cachedPipelineResults = cloneReviewData(pipelineResults);
  cachedResultsCacheInfo = cloneReviewData(resultsCacheInfo);
  maybeRefreshCollectionScopeMembership();
}

// A smart collection whose rules key on mutable fields (rating, flag,
// keyword, species) drifts as the user edits in filter mode: the Set
// captured once by loadReviewScopeResults is only consulted on render, so
// a photo that no longer matches stays visible and still counts toward the
// "N of T not in saved review" banner. Re-fetch membership from the
// server after each mutation (debounced to a single trailing request per
// burst of edits) so the filter stays honest. Folders are skipped —
// folder membership doesn't change from this view.
/** @type {ReturnType<typeof setTimeout> | null} */ var _reviewScopeMembershipRefreshTimer = null;
var _reviewScopeMembershipRefreshSeq = 0;

function maybeRefreshCollectionScopeMembership() {
  if (reviewScopeMode !== 'collection' || reviewScopeRegrouped) return;
  if (!reviewScopeCollectionId) return;
  var selectionId = reviewScopeCollectionId;
  if (_reviewScopeMembershipRefreshTimer) {
    clearTimeout(_reviewScopeMembershipRefreshTimer || undefined);
  }
  _reviewScopeMembershipRefreshTimer = setTimeout(function() {
    _reviewScopeMembershipRefreshTimer = null;
    if (reviewScopeMode !== 'collection' || reviewScopeRegrouped) return;
    if (reviewScopeCollectionId !== selectionId) return;
    var seq = ++_reviewScopeMembershipRefreshSeq;
    // Also tag the scope-load generation. A switch to cache/workspace and
    // back to the same collection bumps reviewScopeMembershipSeq (through
    // loadReviewScopeResults) without touching _reviewScopeMembershipRefreshSeq,
    // so without this capture a stale refresh response could still pass the
    // local-seq, mode, and collection-id checks and overwrite the freshly
    // loaded membership from the scope's own fetch.
    var scopeSeq = reviewScopeMembershipSeq;
    /** @type {Promise<{photo_ids?: number[]}>} */ (safeFetch(
      `/api/pipeline/scope-photos?collection_id=${encodeURIComponent(selectionId)}`,
      {}, { toast: false }
    )).then(function(/** @type {{photo_ids?: number[]}} */ data) {
      if (seq !== _reviewScopeMembershipRefreshSeq) return;
      if (scopeSeq !== reviewScopeMembershipSeq) return;
      if (!data || !Array.isArray(data.photo_ids)) return;
      if (reviewScopeMode !== 'collection' || reviewScopeRegrouped) return;
      if (reviewScopeCollectionId !== selectionId) return;
      var newIds = new Set(data.photo_ids);
      var current = reviewScopePhotoIds;
      var unchanged = !!current && current.size === newIds.size;
      if (unchanged) {
        requireReviewValue(current).forEach(function(id) {
          if (!newIds.has(id)) unchanged = false;
        });
      }
      if (unchanged) return;
      reviewScopePhotoIds = newIds;
      reviewScopeTotal = newIds.size;
      // A dropped photo stays writable in filter mode, so the remembered
      // right-click selection, the native menu's pinned selection, and the
      // legacy inspector must be re-checked against the new membership.
      reconcileReviewScopeTargets();
      renderResults();
      // statTotalPhotos, encounter/burst totals and confirmation counts are
      // scope-filtered through scopedSummaryCounts, so a membership change
      // must redraw the summary bar too. Without this they continue showing
      // pre-edit totals even after the dropped photo correctly disappears.
      if (pipelineResults) updateSummaryBar(currentReviewResults().summary);
      updateReviewScopeStatusFromResults(currentReviewResults());
      renderCacheScopeBanner(resultsCacheInfo);
    }).catch(function() { /* leave the stale set; the next mutation retries */ });
  }, 250);
}

function showSavedReview() {
  if (cachedPipelineResults) {
    applyReviewResults(
      cloneReviewData(cachedPipelineResults),
      cloneReviewData(cachedResultsCacheInfo)
    );
  }
}

/** @param {number} seq */
function loadRegroupedScopeResults(seq) {
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

function loadReviewScopeResults() {
  updateReviewScopeControls();
  // Bump the request sequence up front so that any scope fetch still in
  // flight from a prior scope choice is invalidated regardless of which
  // branch we take here. Otherwise a regroup request that resolves after
  // the user switches back to the saved review could pass its .then() guard
  // and replace the cached view with stale scoped results.
  var seq = ++reviewScopeRequestSeq;
  // Also invalidate any inflight scope-photos membership load from the
  // previous selection, so a change to workspace/cache/regrouped (which
  // doesn't fire a new membership fetch) still drops the stale response.
  ++reviewScopeMembershipSeq;
  if (reviewScopeMode === 'cache') {
    reviewScopePhotoIds = null;
    reviewScopeTotal = null;
    showSavedReview();
    return;
  }
  if (reviewScopeMode === 'workspace') {
    loadRegroupedScopeResults(seq);
    return;
  }

  // Folder / Collection.
  var selectionId = reviewScopeSelectionId();
  if (!selectionId) {
    reviewScopePhotoIds = new Set();
    reviewScopeTotal = 0;
    showSavedReview();
    setReviewScopeStatus('Select one');
    return;
  }
  if (reviewScopeRegrouped) {
    loadRegroupedScopeResults(seq);
    return;
  }
  // Swap back to the saved review before the scope request returns: a
  // regrouped result left in pipelineResults while isScopedReviewView() is
  // already false would let an edit post its encounter indices against the
  // saved cache. Show nothing until the scope's photos are known.
  reviewScopePhotoIds = new Set();
  reviewScopeTotal = 0;
  showSavedReview();
  setReviewScopeStatus('Loading...');
  var param = reviewScopeMode === 'folder' ? 'folder_id' : 'collection_id';
  // Membership gets its own sequence so a slider move (which bumps
  // reviewScopeRequestSeq) does not drop a valid membership response. We
  // still check reviewScopeRequestSeq below so a scope change in flight
  // through onReviewScope*Change (which also bumps membership seq) is caught.
  var memSeq = ++reviewScopeMembershipSeq;
  /** @type {Promise<{photo_ids?: number[]}>} */ (safeFetch(`/api/pipeline/scope-photos?${param}=${encodeURIComponent(selectionId)}`, {}, { toast: false }))
  .then(function(/** @type {{photo_ids?: number[]}} */ data) {
    if (memSeq !== reviewScopeMembershipSeq || !data || !Array.isArray(data.photo_ids)) return;
    reviewScopePhotoIds = new Set(data.photo_ids);
    reviewScopeTotal = data.photo_ids.length;
    reconcileReviewScopeTargets();
    applyReviewResults(currentReviewResults(), resultsCacheInfo);
  })
  .catch(function(e) {
    if (memSeq !== reviewScopeMembershipSeq) return;
    setReviewScopeStatus((e && e.message) || 'Could not load', true);
  });
}

/** @param {string} mode */
function onReviewScopeModeChange(mode) {
  if (['cache', 'folder', 'collection', 'workspace'].indexOf(mode) === -1) return;
  // Capture the current saved-review state before switching away so a later
  // return restores the post-mutation view. Must run before reviewScopeMode
  // changes so refreshLatestScopeSnapshotIfCurrent still sees the outgoing
  // mode.
  refreshLatestScopeSnapshotIfCurrent();
  clearPipelineReviewContextSelection();
  reviewScopeMode = mode;
  reviewScopeRegrouped = false;
  updateReviewScopeControls();
  loadReviewScopeResults();
}

/** @param {string} value */
function onReviewScopeFolderChange(value) {
  refreshLatestScopeSnapshotIfCurrent();
  clearPipelineReviewContextSelection();
  reviewScopeFolderId = value || null;
  reviewScopeRegrouped = false;
  updateReviewScopeControls();
  loadReviewScopeResults();
}

/** @param {string} value */
function onReviewScopeCollectionChange(value) {
  refreshLatestScopeSnapshotIfCurrent();
  clearPipelineReviewContextSelection();
  reviewScopeCollectionId = value || null;
  reviewScopeRegrouped = false;
  updateReviewScopeControls();
  loadReviewScopeResults();
}

function toggleReviewScopeRegroup() {
  if (!isReviewScopeFilterMode() || !reviewScopeSelectionId()) return;
  clearPipelineReviewContextSelection();
  if (reviewScopeRegrouped) {
    reviewScopeRegrouped = false;
    updateReviewScopeControls();
    ++reviewScopeRequestSeq;
    showSavedReview();
    // maybeRefreshCollectionScopeMembership early-exits while regrouped, so a
    // mutation that queued the debounced membership refresh during filter
    // mode had its trailing timer bail out when we entered the regrouped
    // view. Retry here so returning to the saved review doesn't restore the
    // cached review with stale reviewScopePhotoIds (folder mode no-ops).
    maybeRefreshCollectionScopeMembership();
    return;
  }
  if (!reviewScopePhotoIds || !reviewScopeTotal) return;
  refreshLatestScopeSnapshotIfCurrent();
  reviewScopeRegrouped = true;
  updateReviewScopeControls();
  loadRegroupedScopeResults(++reviewScopeRequestSeq);
}

function registerPipelineReviewScopeHook() {
  Vireo.registerPageHooks({
    getPhotoExternalEditDisabledHint: function() {
      return isScopedReviewView() ? scopedViewEditHint() : null;
    },
  });
}
