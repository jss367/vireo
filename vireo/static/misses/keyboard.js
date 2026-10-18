// Focus tracking and the keyboard shortcuts: J/K navigation, Shift+J/K
// selection extension, the configured flag/reject/unflag keys, B, Enter and Esc.
// ES module.

import { state, CATEGORY_ORDER } from './state.js';
import { browseShortcuts, matchesShortcut, preferredScrollBehavior } from './shared.js';
import { openFocusedMissInBrowse, openMiss } from './photo-open.js';
import { bulkFlagSelection, bulkUnflagSelection, flagFocusedMiss, unflagMiss } from './flagging.js';
import { clearSelection, setSelected } from './selection.js';

/* ---------- Focus tracking + keyboard shortcuts ---------- */
/** @param {MissCategory | null} cat @param {number} idx */ export function setFocused(cat, idx) {
  state.focusedCategory = cat;
  state.focusedIndex = idx;
  document.querySelectorAll('.miss-card.focused').forEach(function(el) {
    el.classList.remove('focused');
  });
  if (cat == null || idx == null || idx < 0) return;
  var sel = '.miss-card[data-cat="' + cat + '"][data-idx="' + idx + '"]';
  var el = document.querySelector(sel);
  if (el) {
    el.classList.add('focused');
    el.scrollIntoView({ block: 'nearest', behavior: preferredScrollBehavior() });
  }
}

/** @param {number} delta */ function moveFocus(delta) {
  // Pick the first non-empty category if nothing focused yet.
  if (state.focusedCategory == null) {
    for (var i = 0; i < CATEGORY_ORDER.length; i++) {
      if (state.missesData[CATEGORY_ORDER[i] || 'no_subject'].length > 0) {
        setFocused(CATEGORY_ORDER[i] || null, 0);
        return;
      }
    }
    return;
  }
  var photos = state.missesData[state.focusedCategory] || [];
  var newIdx = state.focusedIndex + delta;
  if (newIdx < 0) newIdx = 0;
  if (newIdx >= photos.length) newIdx = photos.length - 1;
  setFocused(state.focusedCategory, newIdx);
}

export function bindMissKeyboard() {
  document.addEventListener('keydown', function(e) {
    if (!(e.target instanceof Element)) return;
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
    if (document.querySelector('.lightbox-overlay.active, .pipeline-overlay.active, .similar-overlay.active, .modal-overlay.open, .grm-overlay.open, .help-overlay.active')) return;

    // Flag / Reject / Unflag — share the configured browse.flag/reject/unflag
    // shortcuts so a single rebind (incl. modifier combos like ctrl+p) covers
    // Browse, Lightbox, and /misses consistently. Checked before the modifier
    // guard below so modifier-based rebindings still match. Acts on the
    // multi-selection if non-empty, otherwise falls back to the focused card.
    var sc = browseShortcuts();
    var flagKey = sc.flag || 'p';
    var rejectKey = sc.reject || 'x';
    var unflagKey = sc.unflag || 'u';
    var missFlag = null;
    if (matchesShortcut(e, flagKey)) missFlag = 'flagged';
    else if (matchesShortcut(e, rejectKey)) missFlag = 'rejected';
    else if (matchesShortcut(e, unflagKey)) missFlag = 'none';
    if (missFlag !== null) {
      if (state.selection.size > 0) {
        e.preventDefault();
        if (missFlag === 'none') bulkUnflagSelection();
        else bulkFlagSelection(missFlag);
      } else if (state.focusedCategory != null && state.focusedIndex >= 0) {
        var fp = (state.missesData[state.focusedCategory] || [])[state.focusedIndex];
        if (fp) {
          e.preventDefault();
          if (missFlag === 'none') unflagMiss(state.focusedCategory, fp.id);
          else flagFocusedMiss(fp.id, missFlag);
        }
      }
      return;
    }

    // Bare-key shortcuts (j/k/Esc/Enter) — modifier-free only. Shift is allowed
    // here so Shift+J/K can extend the selection.
    if (e.metaKey || e.ctrlKey || e.altKey) return;

    var k = e.key;
    if (k === 'j' || k === 'J') {
      e.preventDefault();
      if (e.shiftKey) extendSelectionByFocus(1);
      else moveFocus(1);
    } else if (k === 'k' || k === 'K') {
      e.preventDefault();
      if (e.shiftKey) extendSelectionByFocus(-1);
      else moveFocus(-1);
    } else if (k === 'Escape') {
      if (state.selection.size > 0) {
        e.preventDefault();
        clearSelection();
      }
    } else if (k === 'b' || k === 'B') {
      if (!e.shiftKey && state.focusedCategory != null && state.focusedIndex >= 0) {
        e.preventDefault();
        openFocusedMissInBrowse();
      }
    } else if (k === 'Enter') {
      if (state.focusedCategory != null && state.focusedIndex >= 0) {
        e.preventDefault();
        openMiss(state.focusedCategory, state.focusedIndex);
      }
    }
  });
}

/* extendSelectionByFocus — Shift+J/K. Move focus by `delta`, then add the new
 * focused card to the selection. The previously-focused card is also added so
 * that the first Shift+J after a plain j-walk seeds the range with two cards
 * instead of one (matches what users expect from text-editor selection). */
/** @param {number} delta */ function extendSelectionByFocus(delta) {
  if (state.focusedCategory == null) {
    moveFocus(delta);
    return;
  }
  var photos = state.missesData[state.focusedCategory] || [];
  var oldP = photos[state.focusedIndex];
  if (oldP) setSelected(oldP.id, true);
  moveFocus(delta);
  var newP = photos[state.focusedIndex];
  if (newP) setSelected(newP.id, true);
}
