// Loading /api/predictions, its sequence guard, and the reload flag that gates decisions.
// Classic page script; load boot.js after all definitions.

// Bumped by every loadPredictions() invocation; responses whose epoch is
// no longer current are dropped. The bootstrap kicks a pre-init unfiltered
// fetch, and VireoFilter.init().then() may kick a post-init filtered
// fetch \u2014 without this token, if the unfiltered fetch resolved LAST
// (large libraries), it would overwrite allPredictions / the grid / the
// filter-bar total with unfiltered rows while the filter chips remained
// active.
// Must be initialized BEFORE the bootstrap call to loadPredictions()
// below \u2014 `var` hoists the declaration but not the `= 0` initializer,
// so if declared later the first ++ increments `undefined` to `NaN` and
// every subsequent epoch check (NaN !== 0) drops the response silently.
var _loadPredictionsEpoch = 0;

// True while a loadPredictions() request is in flight. The old
// ``predictions`` array and action bar remain live until the new
// response returns, so without this flag a click on Accept All (or a
// keyboard accept/skip) between filter change and response would
// iterate the STALE list under the newer visible chips. renderButtons()
// / renderGrid() disable the mutation entry points while this is true,
// and each action captures the epoch on entry so a mid-flight reload
// aborts the loop rather than committing to now-hidden rows.
var _predictionsReloading = false;
/** @param {number} epoch */ function _predictionEpochStale(epoch) {
  return epoch !== _loadPredictionsEpoch;
}

// With no collection, filter or photo chosen, the request returns every
// prediction in the workspace, which takes from seconds to minutes on a large
// catalog; a collection returns only its own.
var REVIEW_WORKSPACE_LOAD_HINT = 'All collections loads every prediction in this workspace. '
  + 'A single collection loads faster.';

/** @type {VireoLoadingStatusHandle | null} */
var _reviewLoadStatus = null;

/** The line above the grid that says a load is running or has failed. */
function reviewLoadStatus() {
  if (!_reviewLoadStatus) {
    _reviewLoadStatus = VireoLoadingStatus.create(
      /** @type {HTMLElement} */ (document.getElementById('loading')),
      { content: document.getElementById('grid') }
    );
  }
  return _reviewLoadStatus;
}

/** @param {unknown} err */
function showReviewLoadFailure(err) {
  reviewLoadStatus().fail('Could not load predictions: ' + (err instanceof Error ? err.message : String(err)), {
    retry: function() { switchCollection(currentCollection); },
  });
}

/** @param {URLSearchParams} params */
function reviewLoadHint(params) {
  var narrowed = ['rules', 'visual', 'collection_id', 'photo_ids'].some(function(key) {
    return params.has(key);
  });
  return narrowed ? '' : REVIEW_WORKSPACE_LOAD_HINT;
}

/** @param {URLSearchParams} params @param {number} epoch */
async function fetchReviewScope(params, epoch) {
  var photoId = currentPhotoIdFilter;
  var paged = !!reviewLoadHint(params);
  if (paged) params.set('after_photo', '0');
  var qs = params.toString();
  /** @type {{predictions?:ReviewPrediction[];visual?:unknown;next_photo?:number|null;workspace_id?:number}} */
  var predData = await safeFetch('/api/predictions' + (qs ? '?' + qs : ''), {}, {toast: false});
  if (paged && reviewQueue.workspace != null && predData.workspace_id !== reviewQueue.workspace) {
    throw new Error('Workspace changed. Reload Review.');
  }
  if (paged && predData.next_photo != null) await readRemainingWorkspacePages(params, epoch, predData);
  var linkedPhoto = null;
  if (!predData.predictions?.length && photoId && currentCollection === 'all' && !params.has('rules') && !params.has('visual')) {
    // Browse can open a photo with no predictions to inspect.
    /** @type {{id:number;filename:string;flag?:string}} */
    var linked = await safeFetch('/api/photos/' + photoId, {}, {toast: false});
    linkedPhoto = {photo_id: linked.id, filename: linked.filename, flag: linked.flag};
  }
  return {predData, linkedPhoto};
}

/** @type {ReviewPrediction[]|null} */
var reviewPreviewRows = null;

function showReviewPreview() {
  if (!reviewPreviewRows) return;
  var photos = new Map();
  for (var row of filterReviewRows(reviewPreviewRows, currentTab)) {
    photos.set(row.photo_id, {id: row.photo_id, label: row.filename});
    if (photos.size === 8) break;
  }
  window.loadingPhotos.clear();
  window.loadingPhotos.show('Open a photo while the review queue loads', Array.from(photos.values()), '/review');
}

/** @param {URLSearchParams} params @param {number} epoch
 * @param {{predictions?:ReviewPrediction[];next_photo?:number|null;workspace_id?:number}} data */
async function readRemainingWorkspacePages(params, epoch, data) {
  var rows = data.predictions || [];
  var cursor = data.next_photo;
  params.set('workspace_id', String(data.workspace_id));
  params.set('page_size', '512');
  while (cursor != null && !_predictionEpochStale(epoch)) {
    if (!reviewPreviewRows && rows.length) {
      reviewPreviewRows = rows.slice(0, 512);
      showReviewPreview();
    }
    params.set('after_photo', String(cursor));
    /** @type {typeof data} */
    var page = await safeFetch('/api/predictions?' + params, {}, {toast: false});
    if (_predictionEpochStale(epoch)) return;
    if (page.workspace_id !== data.workspace_id) throw new Error('Workspace changed. Reload Review.');
    if (page.next_photo != null && page.next_photo <= cursor) throw new Error('Prediction page did not advance.');
    for (var prediction of page.predictions || []) rows.push(prediction);
    cursor = page.next_photo;
  }
  if (_predictionEpochStale(epoch)) return;
  rows.sort((a, b) => b.confidence - a.confidence || a.id - b.id);
  joinPagedReviewCards(rows);
  data.predictions = rows;
}

function clearReviewPreview() {
  reviewPreviewRows = null;
  window.loadingPhotos.clear();
}

async function loadPredictions() {
  var myEpoch = ++_loadPredictionsEpoch;
  clearReviewPreview();
  skippedPredictionOrder.clear();
  rqBeginReload();
  // Disable the header Accept All immediately so it cannot iterate the
  // stale ``predictions`` array while this reload is in flight. The
  // per-card actions check this flag in their own handlers.
  var _acceptAllReloadBtn = /** @type {HTMLButtonElement} */ (document.getElementById('acceptAllBtn'));
  if (_acceptAllReloadBtn) {
    _acceptAllReloadBtn.disabled = true;
    _acceptAllReloadBtn.textContent = 'Reloading…';
  }
  /** @type {HTMLElement} */ (document.getElementById('empty')).style.display = 'none';

  try {
    var params = new URLSearchParams({evidence: '1'});
    if (window.VireoFilter && VireoFilter.getRules) {
      var rules = VireoFilter.getRules();
      var ruleCount = Array.isArray(rules) ? rules.length : (rules.rules || []).length;
      if (ruleCount) params.set('rules', JSON.stringify(rules));
      var visual = VireoFilter.getVisual ? VireoFilter.getVisual() : null;
      if (visual) params.set('visual', JSON.stringify(visual));
    }
    // Forward the active Review collection so rule/visual resolution and the
    // returned visual_info describe the collection-scoped queue (not the
    // whole workspace). Without this, a visual clause with the collection
    // holding no embeddings would still return ``status: ok`` from
    // out-of-collection photos, the client would intersect those away, and
    // the Review grid would empty out while the visual chip stayed on-screen
    // with no fallback warning. Also keeps the filter-bar total honest —
    // it's the collection-scoped match count, not a workspace-wide proxy.
    if (currentCollection !== 'all') {
      var collId = parseInt(currentCollection, 10);
      if (!isNaN(collId)) params.set('collection_id', String(collId));
    }
    if (currentPhotoIdFilter != null) params.set('photo_ids', String(currentPhotoIdFilter));
    reviewLoadStatus().start('Loading photos…', { hint: reviewLoadHint(params) });
    const {predData, linkedPhoto} = await fetchReviewScope(params, myEpoch);
    if (myEpoch !== _loadPredictionsEpoch) return;
    clearReviewPreview();
    reviewQueue.linkedPhoto = linkedPhoto;
    allPredictions = predData.predictions || [];
    if (window.VireoFilter && VireoFilter.setResultTotal) {
      /** @type {Record<number,boolean>} */ var uniquePhotos = {};
      allPredictions.forEach(function(p) { uniquePhotos[p.photo_id] = true; });
      VireoFilter.setResultTotal(Object.keys(uniquePhotos).length);
    }
    // Surface the visual clause's status so the filter bar can warn on
    // fallback (no model / no embeddings / encoding failed). Without
    // this a visual chip stays on-screen while Accept All / bulk actions
    // operate on the broadened metadata-only result set.
    if (window.VireoFilter && VireoFilter.setVisualInfo) {
      VireoFilter.setVisualInfo(predData.visual || null);
    }

    // Re-apply collection filter if active
    if (currentCollection !== 'all' && collectionPhotoIds) {
      predictions = allPredictions.filter(function(p) { return collectionPhotoIds?.has(p.photo_id) || false; });
    } else {
      predictions = allPredictions.slice();
    }

    updateAvailableReviewModels();
    var pending = reviewCardCounts().pending || 0;
    mode = allPredictions.length > 0 ? 'review' : 'browse';

    /** @type {HTMLElement} */ (document.getElementById('title')).textContent =
      'Review Predictions (' + pending + ' pending cards)';
    document.title = 'Vireo - Review (' + pending + ' pending cards)';

    _predictionsReloading = false;
    reviewLoadStatus().done();
    renderAll();
  } catch(err) {
    if (myEpoch !== _loadPredictionsEpoch) return;  // superseded — drop
    _predictionsReloading = false;
    clearReviewPreview();
    showReviewLoadFailure(err);
    // Clear the stale ``predictions`` array before re-rendering so Accept
    // All and per-card handlers can't fire against the previous filter
    // or collection's rows under the new visible chips. Without this the
    // header button (and inline handlers) become enabled again with old
    // counts, letting users accept/reject predictions that are outside
    // the current filter scope.
    allPredictions = [];
    predictions = [];
    renderAll();
  }
}

function updateAvailableReviewModels() {
  // Extract unique models
  /** @type {Record<string,boolean>} */ var modelSet = {};
  allPredictions.forEach(function(p) {
    if (p.model && !modelSet[p.model]) modelSet[p.model] = true;
  });
  availableModels = Object.keys(modelSet);
  // Reset a stale ``currentModel`` when the new response no longer
  // contains it. Without this, a filter change (or collection switch)
  // that narrows the model universe leaves ``currentModel`` pinned to a
  // model that's not present: ``renderModelFilter()`` hides the selector
  // (0 or 1 models remain), while ``getVisibleItems()`` still filters by
  // the stale model — leaving an empty grid with no visible way to
  // clear the filter.
  if (currentModel !== 'all' && !modelSet[currentModel]) {
    currentModel = 'all';
  }
}
