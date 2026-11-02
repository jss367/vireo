// Startup for the sync panel (_sync_panel.html, on Browse, Review, Location
// Review and Pipeline Review): registers the native menu's Sync Metadata
// hook, binds the panel's controls, the review dialog's thumbnail clicks, and
// the lightbox listeners.
// Classic script; loads after every other sync panel script, and before the
// including page's own scripts, which call checkPendingSync and runSync.

// The native menu's Sync Metadata command (vireo-page-hooks.js).
Vireo.registerPageHooks({runSync: runSync});

/** @param {string} id @param {() => unknown} action */
function _syncOnClick(id, action) {
  /** @type {HTMLElement} */ (document.getElementById(id)).addEventListener('click', function() { action(); });
}

_syncOnClick('syncBannerReviewButton', openSyncPreview);
_syncOnClick('syncBannerSyncButton', function() { return runSync(); });
_syncOnClick('syncPreviewCloseButton', closeSyncPreview);
_syncOnClick('syncPreviewDiscardAllButton', discardAllChanges);
_syncOnClick('syncPreviewSyncButton', runVisibleSync);
/** @type {HTMLInputElement} */ (document.getElementById('syncThumbSize')).addEventListener('input', function(event) {
  updateSyncThumbSize(/** @type {HTMLInputElement} */ (event.currentTarget).value);
});

/* Delegate sync preview thumb clicks for lightbox (XSS-safe) */
/** @type {HTMLElement} */ (document.getElementById('syncPreviewContent')).addEventListener('click', function(e) {
  var thumb = e.target instanceof Element ? e.target.closest('img[data-photo-id]') : null;
  if (!(thumb instanceof HTMLElement)) return;
  // Pass the list of currently-visible photos so the lightbox arrows can navigate
  // between pending-change items. Without this, _lightboxPhotoList stays empty
  // and lightboxNav() bails out immediately.
  /** @type {{id: number, filename: string}[]} */
  var photoList = [];
  var filters = _syncActiveFilters;
  if (_syncPreviewData && _syncPreviewData.photos && filters) {
    _syncPreviewData.photos.forEach(function(p) {
      var hasVisible = p.changes.some(function(c) { return /** @type {Set<string>} */ (filters).has(c.type); });
      if (hasVisible) photoList.push({ id: p.photo_id, filename: p.filename });
    });
  }
  openLightbox(parseInt(thumb.dataset.photoId || '', 10), thumb.dataset.filename || '', photoList);
});

document.addEventListener('lightbox:photochanged', function(e) {
  if (!_syncModalOpen) return;
  var detail = /** @type {CustomEvent<{photoId?: number}>} */ (e).detail;
  _syncRenderLightboxPanel(detail && detail.photoId);
});

document.addEventListener('lightbox:closed', function() {
  _syncDestroyLightboxPanel();
});
