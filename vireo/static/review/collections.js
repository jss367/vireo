// The collection picker and switching the Review scope to a collection.
// Classic page script; load boot.js after all definitions.

async function loadCollectionFilter() {
  try {
    var collections = await safeFetch('/api/collections', {}, { toast: false });
    var sel = document.getElementById('collectionFilter');
    collections.forEach(function(c) {
      var opt = document.createElement('option');
      opt.value = c.id;
      opt.textContent = c.name;
      // count_error means /api/collections couldn't resolve this collection's
      // rules — selecting it would 400 the downstream /photos endpoint and
      // (previously) silently widen the review scope back to every prediction.
      // Show it disabled with a hint so the state is visible.
      if (c.count_error) {
        opt.disabled = true;
        opt.textContent = c.name + ' (unavailable — edit rules to fix)';
        opt.title = "This collection's rules could not be resolved. Edit it in Browse to make it usable.";
      } else if (c.has_visual) {
        // Visual collections resolve their result set through the filter
        // bar's visual clause (see Browse). The Review picker funnels the
        // selected collection through /api/collections/<id>/photos, which
        // only evaluates ``rules`` — picking one here would silently
        // widen the review scope to every metadata match. Disable with a
        // tooltip so the option is visible but not usable, and the server
        // already 400s the endpoint as a defense-in-depth backstop.
        opt.disabled = true;
        opt.textContent = c.name + ' (visual — open in Browse)';
        opt.title = 'Visual collections can only be used from Browse, where the filter bar resolves the visual-search clause.';
      }
      sel.appendChild(opt);
    });
  } catch(e) {}
}

// Bumped by every switchCollection() invocation; late-returning
// /api/collections/<id>/photos responses whose epoch is no longer current
// are dropped before they can mutate collectionPhotoIds. Without this,
// two rapid switchCollection() calls (e.g. from back-to-back
// vireo:edit-history-changed events) whose fetches complete out of order
// can leave collectionPhotoIds holding the OLDER call's membership snapshot
// — loadPredictions() then intersects the newest server response against
// stale collection state and the Review grid stays wrong.
var _switchCollectionEpoch = 0;

async function switchCollection(val) {
  var mySwitchEpoch = ++_switchCollectionEpoch;
  currentCollection = val;
  // Gate mutation entry points immediately: currentCollection has already
  // changed, but the /api/collections/<id>/photos fetch below may take a
  // moment before loadPredictions() runs (and flips the flag itself).
  // Without setting this now, Accept All and per-card handlers stay
  // enabled under the newly selected collection and iterate the STALE
  // ``predictions`` array from the previous collection during that gap.
  _predictionsReloading = true;
  renderButtons();
  if (val === 'all') {
    collectionPhotoIds = null;
  } else {
    // Fetch photo IDs in this collection so ``loadPredictions``' existing
    // safety intersection (allPredictions ∩ collectionPhotoIds) still
    // narrows a stale ``allPredictions`` while the new fetch is in flight.
    try {
      var data = await safeFetch('/api/collections/' + val + '/photos?per_page=999999', {}, { toast: false });
      if (mySwitchEpoch !== _switchCollectionEpoch) return;  // superseded — drop
      collectionPhotoIds = new Set(data.photos.map(function(p) { return p.id; }));
    } catch(e) {
      if (mySwitchEpoch !== _switchCollectionEpoch) return;  // superseded — drop
      // The old catch fell back to allPredictions.slice(), which silently
      // widened the review scope to every prediction — the opposite of what
      // the user asked for. Keep the scope empty and surface the failure
      // instead so a broken rule can't quietly turn "just this collection"
      // into "everything".
      console.error('Failed to load photos for collection ' + val + ':', e);
      collectionPhotoIds = new Set();
      predictions = [];
      // Release the reload guard so ``renderButtons`` shows the correct
      // hidden/disabled state for the empty ``predictions`` array instead
      // of a stuck "Reloading…" button.
      _predictionsReloading = false;
      if (typeof showToast === 'function') {
        showToast('Could not load photos for this collection — check its rules', 'error');
      }
      renderAll();
      return;
    }
  }
  // Re-fetch server-side so ``/api/predictions`` resolves rules/visual
  // against the new collection (not the stale one). Without this the
  // visual_info chip would keep describing whichever collection was
  // active at the last loadPredictions call, and — for a collection
  // that hides some workspace embeddings — the filter-bar total would
  // stay workspace-scoped instead of collection-scoped.
  await loadPredictions();
}
