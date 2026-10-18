// Opening a miss in the shared lightbox or in Browse, the focused and selected
// photo lookups, and pruning the selection to visible cards.
// ES module.

import { state, CATEGORY_ORDER } from './state.js';
import { hasLightbox, openLightbox, requestLightboxFullscreen } from './shared.js';
import { setFocused } from './keyboard.js';

/* ---------- Open lightbox ---------- */
/** @param {MissCategory | null} cat @param {number} idx @param {MouseEvent=} e */ export function openMiss(cat, idx, e) {
  if (e && e.target instanceof Element && e.target.closest('button')) return;
  if (cat === null) return; const category = cat; var photos = state.missesData[category];
  if (!photos || !photos[idx]) return;
  var p = photos[idx];
  setFocused(cat, idx);
  state.missLightboxCategoryById = {};
  var photoList = photos.map(function(q) {
    state.missLightboxCategoryById[q.id] = category;
    return { id: q.id, filename: q.filename, missCategory: category };
  });
  state.missLightboxCurrent = { photoId: p.id, category: category };

  if (!hasLightbox()) return;
  // shared.js resets the lightbox's click-to-zoom latch before opening.
  openLightbox(p.id, p.filename || '', photoList);
}

export function bindMissLightboxTracking() {
  document.addEventListener('lightbox:photochanged', function(e) {
    var photoId = e && e.detail ? e.detail.photoId : null;
    var cat = photoId != null ? state.missLightboxCategoryById[photoId] : null;
    state.missLightboxCurrent = cat && photoId != null ? { photoId: photoId, category: cat } : null;
  });

  document.addEventListener('lightbox:closed', function() {
    state.missLightboxCurrent = null;
  });
}

function getFocusedMiss() {
  if (state.focusedCategory == null || state.focusedIndex < 0) return null;
  return (state.missesData[state.focusedCategory] || [])[state.focusedIndex] || null;
}

export function getActiveSelection() {
  if (state.selection.size > 0) return Array.from(state.selection);
  var focused = getFocusedMiss();
  return focused ? [focused.id] : [];
}

/** @param {number} photoId */ export function openPhotoInBrowse(photoId) {
  photoId = parseInt(String(photoId), 10);
  if (isNaN(photoId)) return;
  window.location.href = '/browse?photo_id=' + encodeURIComponent(photoId);
}

export function openSelectionInBrowse() {
  var ids = getActiveSelection();
  if (ids.length !== 1) return;
  if (ids[0] !== undefined) openPhotoInBrowse(ids[0]);
}

export function openFocusedMissInBrowse() {
  var focused = getFocusedMiss();
  if (!focused) return;
  openPhotoInBrowse(focused.id);
}

/** @param {boolean} fullscreen */ export function openBrowseShortcutPhoto(fullscreen) {
  var focused = getFocusedMiss();
  if (!focused) return false;
  openMiss(state.focusedCategory, state.focusedIndex);
  if (fullscreen) requestLightboxFullscreen();
  return true;
}

/* Drop any selected ids that are no longer rendered in any category. Call
 * this after any synchronous mutation to `missesData` (per-card unflag,
 * bulk-reject section clear) so a follow-up bulk P/X/U via selection can't
 * silently act on photos the user no longer sees as selected. The async
 * loadMisses() prune doesn't cover this window. */
export function pruneSelectionToVisible() {
  if (state.selection.size === 0) return;
  state.selection.forEach(function(id) {
    var stillVisible = CATEGORY_ORDER.some(function(c) {
      return (state.missesData[c] || []).some(function(p) { return p.id === id; });
    });
    if (!stillVisible) state.selection.delete(id);
  });
}
