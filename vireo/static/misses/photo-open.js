// Opening a miss in the shared lightbox or in Browse, the focused and selected
// photo lookups, and pruning the selection to visible cards.
// Classic page script; load boot.js after all definitions.

/* ---------- Open lightbox ---------- */
function openMiss(cat, idx, e) {
  if (e && e.target && e.target.closest('button')) return;
  var photos = missesData[cat];
  if (!photos || !photos[idx]) return;
  var p = photos[idx];
  setFocused(cat, idx);
  missLightboxCategoryById = {};
  var photoList = photos.map(function(q) {
    missLightboxCategoryById[q.id] = cat;
    return { id: q.id, filename: q.filename, missCategory: cat };
  });
  missLightboxCurrent = { photoId: p.id, category: cat };

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
    missLightboxCurrent = cat ? { photoId: photoId, category: cat } : null;
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

function openPhotoInBrowse(photoId) {
  photoId = parseInt(photoId, 10);
  if (isNaN(photoId)) return;
  window.location.href = '/browse?photo_id=' + encodeURIComponent(photoId);
}

function openSelectionInBrowse() {
  var ids = getActiveSelection();
  if (ids.length !== 1) return;
  openPhotoInBrowse(ids[0]);
}

function openFocusedMissInBrowse() {
  var focused = getFocusedMiss();
  if (!focused) return;
  openPhotoInBrowse(focused.id);
}

function openBrowseShortcutPhoto(fullscreen) {
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
