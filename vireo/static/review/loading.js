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
function _predictionEpochStale(epoch) {
  return epoch !== _loadPredictionsEpoch;
}

async function loadPredictions() {
  var myEpoch = ++_loadPredictionsEpoch;
  _predictionsReloading = true;
  // Disable the header Accept All immediately so it cannot iterate the
  // stale ``predictions`` array while this reload is in flight. The
  // per-card actions are gated inline in their handlers because they
  // are rendered by ``renderGrid()`` and cheaper to guard on click.
  var _acceptAllReloadBtn = document.getElementById('acceptAllBtn');
  if (_acceptAllReloadBtn) {
    _acceptAllReloadBtn.disabled = true;
    _acceptAllReloadBtn.textContent = 'Reloading…';
  }
  document.getElementById('loading').style.display = 'block';
  document.getElementById('empty').style.display = 'none';

  try {
    var params = new URLSearchParams();
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
    var qs = params.toString();
    var predData = await safeFetch('/api/predictions' + (qs ? '?' + qs : ''), {}, { toast: false });
    if (myEpoch !== _loadPredictionsEpoch) return;  // superseded — drop
    allPredictions = predData.predictions || [];
    if (window.VireoFilter && VireoFilter.setResultTotal) {
      var uniquePhotos = {};
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
      predictions = allPredictions.filter(function(p) { return collectionPhotoIds.has(p.photo_id); });
    } else {
      predictions = allPredictions.slice();
    }

    // Extract unique models
    var modelSet = {};
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

    var pending = allPredictions.filter(function(p) { return p.status === 'pending'; });
    mode = allPredictions.length > 0 ? 'review' : 'browse';

    document.getElementById('title').textContent =
      'Review Predictions (' + pending.length + ' pending)';
    document.title = 'Vireo - Review (' + pending.length + ' pending)';

    document.getElementById('loading').style.display = 'none';
    _predictionsReloading = false;
    renderAll();
  } catch(err) {
    if (myEpoch !== _loadPredictionsEpoch) return;  // superseded — drop
    _predictionsReloading = false;
    document.getElementById('loading').textContent = 'Error: ' + err.message;
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
