// The multi-selection: clearing, toggling, the selection bar, shift-click
// ranges, and card click and double-click.
// ES module.

import { state } from './state.js';
import { openMiss } from './photo-open.js';
import { setFocused } from './keyboard.js';

/* ---------- Selection ----------
 * Plain click selects exactly one photo. Ctrl/Cmd-click toggles additional
 * photos, and Shift-click selects a range from the focused anchor. Double-click
 * opens the shared lightbox.
 *
 * Range selection (shift-click, shift+J/K) stays within a single category
 * because the misses page renders a separate grid per category and the user's
 * mental model of "this row" maps to "this category". Cross-category bulk
 * selection still works via repeated ctrl-click.
 */
export function clearSelection() {
  if (state.selection.size === 0) return;
  state.selection.clear();
  document.querySelectorAll('.miss-card.selected').forEach(function(el) {
    el.classList.remove('selected');
  });
  updateSelectionBar();
}

/** @param {number} photoId @param {boolean} on */ export function setSelected(photoId, on) {
  if (on) state.selection.add(photoId);
  else state.selection.delete(photoId);
  // querySelectorAll: a photo can appear in multiple miss categories at once
  // (e.g. both `clipped` and `oof`), and each section renders its own card.
  // querySelector would only update the first match, leaving the duplicate
  // visually out of sync with the selection set.
  var els = document.querySelectorAll('.miss-card[data-photo-id="' + photoId + '"]');
  els.forEach(function(el) { el.classList.toggle('selected', on); });
  updateSelectionBar();
}

export function updateSelectionBar() {
  var bar = /** @type {HTMLElement} */ (document.getElementById('missesSelectionBar'));
  var countEl = /** @type {HTMLElement} */ (document.getElementById('missesSelectionCount'));
  if (!bar || !countEl) return;
  var n = state.selection.size;
  bar.classList.toggle('active', n > 0);
  countEl.textContent = n + ' selected';
  var unflagBtn = /** @type {HTMLButtonElement} */ (document.getElementById('missesSelectionUnflagBtn'));
  if (unflagBtn) unflagBtn.disabled = state.previewActive;
  var browseBtn = /** @type {HTMLButtonElement} */ (document.getElementById('missesSelectionBrowseBtn'));
  if (browseBtn) browseBtn.disabled = n !== 1;
}

/** @param {MissCategory} cat @param {number} fromIdx @param {number} toIdx */ function selectRangeWithinCategory(cat, fromIdx, toIdx) {
  var photos = state.missesData[cat] || [];
  if (!photos.length) return;
  var lo = Math.min(fromIdx, toIdx);
  var hi = Math.max(fromIdx, toIdx);
  for (var i = lo; i <= hi && i < photos.length; i++) {
    if (i < 0) continue;
    var photo = photos[i]; if (photo) setSelected(photo.id, true);
  }
}

/** @param {MissCategory} cat @param {number} idx @param {number} photoId @param {MouseEvent=} e */ export function onCardClick(cat, idx, photoId, e) {
  // Let the inner unflag button keep its own click behavior.
  if (e && e.target instanceof Element && e.target.closest('button')) return;
  if (e && (e.ctrlKey || e.metaKey)) {
    e.preventDefault();
    e.stopPropagation();
    setSelected(photoId, !state.selection.has(photoId));
    setFocused(cat, idx);
    return;
  }
  if (e && e.shiftKey) {
    e.preventDefault();
    e.stopPropagation();
    var anchorIdx = (state.focusedCategory === cat && state.focusedIndex >= 0) ? state.focusedIndex : idx;
    selectRangeWithinCategory(cat, anchorIdx, idx);
    setFocused(cat, idx);
    return;
  }
  // Plain click selects exactly this card and moves the shift-click anchor.
  if (e) {
    e.preventDefault();
    e.stopPropagation();
  }
  clearSelection();
  setSelected(photoId, true);
  setFocused(cat, idx);
}

/** @param {MissCategory} cat @param {number} idx @param {MouseEvent=} e */ export function onCardDoubleClick(cat, idx, e) {
  if (e && e.target instanceof Element && e.target.closest('button')) return;
  if (e) {
    e.preventDefault();
    e.stopPropagation();
  }
  openMiss(cat, idx, e);
}
