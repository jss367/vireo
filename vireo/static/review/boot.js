// Page startup.
// Classic page script; loads after every other Review page definition.

// Preserve the inline script's order: the boxes button, the shortcut config,
// the bootstrap requests and filter bar, then each listener.
document.addEventListener('DOMContentLoaded', _syncReviewDetectionBoxesBtn);

(async function() {
  try {
    var cfg = await safeFetch('/api/config', {}, { toast: false });
    _shortcuts = (cfg.keyboard_shortcuts || {}).review || {accept: 'a', skip: 's'};
    window._vireoShortcuts = cfg.keyboard_shortcuts || {};
    var el = document.getElementById('kbdHintReview');
    if (el && _shortcuts) el.textContent = 'Keyboard: ' + formatShortcut(_shortcuts.accept) + ' = accept, ' + formatShortcut(_shortcuts.skip) + ' = skip';
  } catch(e) {
    _shortcuts = {accept: 'a', skip: 's'};
  }
})();

/* ---------- Bootstrap ---------- */
VireoViewPreferences.restoreAll(document.getElementById('reviewBar'));
currentSort = document.getElementById('sortSelect').value;
updateThumbSize(document.getElementById('thumbSizeSlider').value);
applyReviewQueryParams();
loadPredictions();
loadCollectionFilter();
VireoFilter.init({
  page: 'review',
  root: document.getElementById('vireoFilterBar'),
  scopeLabel: 'Review \u00b7 Predictions',
  onChange: loadPredictions,
  // Review keeps its collection scope outside the filter tree and passes
  // it separately as ``collection_id`` on /api/predictions. The Misses
  // handoff payload only serializes ``state.root``/``state.visual``, so
  // without exposing this scope the handoff menu would offer Misses under
  // a Review-collection view and land on workspace-wide misses matching
  // the chip \u2014 with bulk reject/recompute enabled against photos
  // outside the source collection (Codex review r3627997828).
  getScope: function() {
    if (currentCollection === 'all') {
      return { folder_id: null, collection_id: null };
    }
    var collId = parseInt(currentCollection, 10);
    return {
      folder_id: null,
      collection_id: isNaN(collId) ? null : collId,
    };
  },
}).then(function() {
  // Restored/deep-linked filters weren't part of the first fetch above.
  if (VireoFilter.hasFilters()) loadPredictions();
}).catch(function() {});

// Undo/redo lives in the shared navbar. The database changes there, but this
// page otherwise keeps rendering the status snapshot fetched on initial load.
// Refetch after a history change so cards, tabs, and counts match the database.
// Route through ``switchCollection`` (not ``loadPredictions`` directly) so the
// active collection's ``collectionPhotoIds`` snapshot is refreshed too — an
// undo can move a photo into a rating- or keyword-based collection, and
// ``loadPredictions`` would otherwise intersect the fresh server response with
// the stale set and drop the newly matching card.
document.addEventListener('vireo:edit-history-changed', function() {
  switchCollection(currentCollection);
});

bindReviewDetectionBoxRenderSync();
bindReviewGridClicks();
bindReviewKeyboard();
bindReviewCardContextMenu();
bindBurstGroupContextMenu();
window.addEventListener('resize', grmUpdateSelectedEyeCrosshair);
bindBurstGroupKeyboard();
bindReviewLifeListEvents();
