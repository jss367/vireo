// Opening a miss in the shared lightbox or in Browse, the focused and selected
// photo lookups, and pruning the selection to visible cards.
// Classic page script; load boot.js after all definitions.

/* ---------- Open lightbox ---------- */
/** @param {MissCategory | null} cat @param {number} idx @param {MouseEvent=} e */ function openMiss(cat, idx, e) {
  if (e && e.target instanceof Element && e.target.closest('button')) return;
  if (cat === null) return; const category = cat; var photos = missesData[category];
  if (!photos || !photos[idx]) return;
  var p = photos[idx];
  setFocused(cat, idx);
  missLightboxCategoryById = {};
  var photoList = photos.map(function(q) {
    missLightboxCategoryById[q.id] = category;
    return { id: q.id, filename: q.filename, missCategory: category };
  });
  missLightboxCurrent = { photoId: p.id, category: category };

  if (typeof openLightbox !== 'function') return;
  // The wrap onclick handler in _navbar.html guards on !window._lightboxZoomHandled,
  // and the mouseup zoom-out path sets it to true. Without resetting here, a stale
  // true from a prior session would swallow the first click-to-zoom.
  window._lightboxZoomHandled = false;
  openLightbox(p.id, p.filename || '', photoList);
}

function bindMissLightboxTracking() {
  document.addEventListener('lightbox:photochanged', function(e) {
    var photoId = e && e.detail ? e.detail.photoId : null;
    var cat = photoId != null ? missLightboxCategoryById[photoId] : null;
    missLightboxCurrent = cat && photoId != null ? { photoId: photoId, category: cat } : null;
  });

  document.addEventListener('lightbox:closed', function() {
    missLightboxCurrent = null;
  });
}

function getFocusedMiss() {
  if (focusedCategory == null || focusedIndex < 0) return null;
  return (missesData[focusedCategory] || [])[focusedIndex] || null;
}

function getActiveSelection() {
  if (selection.size > 0) return Array.from(selection);
  var focused = getFocusedMiss();
  return focused ? [focused.id] : [];
}

/** @param {number} photoId */ function openPhotoInBrowse(photoId) {
  photoId = parseInt(String(photoId), 10);
  if (isNaN(photoId)) return;
  window.location.href = '/browse?photo_id=' + encodeURIComponent(photoId);
}

function openSelectionInBrowse() {
  var ids = getActiveSelection();
  if (ids.length !== 1) return;
  if (ids[0] !== undefined) openPhotoInBrowse(ids[0]);
}

function openFocusedMissInBrowse() {
  var focused = getFocusedMiss();
  if (!focused) return;
  openPhotoInBrowse(focused.id);
}

/** @param {boolean} fullscreen */ function openBrowseShortcutPhoto(fullscreen) {
  var focused = getFocusedMiss();
  if (!focused) return false;
  openMiss(focusedCategory, focusedIndex);
  if (fullscreen && typeof requestLightboxFullscreen === 'function') {
    requestLightboxFullscreen();
  }
  return true;
}

/* Drop any selected ids that are no longer rendered in any category. Call
 * this after any synchronous mutation to `missesData` (per-card unflag,
 * bulk-reject section clear) so a follow-up bulk P/X/U via selection can't
 * silently act on photos the user no longer sees as selected. The async
 * loadMisses() prune doesn't cover this window. */
function pruneSelectionToVisible() {
  if (selection.size === 0) return;
  selection.forEach(function(id) {
    var stillVisible = CATEGORY_ORDER.some(function(c) {
      return (missesData[c] || []).some(function(p) { return p.id === id; });
    });
    if (!stillVisible) selection.delete(id);
  });
}
