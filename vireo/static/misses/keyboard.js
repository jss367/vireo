// Focus tracking and the keyboard shortcuts: J/K navigation, Shift+J/K
// selection extension, the configured flag/reject/unflag keys, B, Enter and Esc.
// ES module.

import { state, CATEGORY_ORDER } from './state.js';
import { browseShortcuts, matchesShortcut, preferredScrollBehavior } from './shared.js';
import { openFocusedMissInBrowse, openMiss } from './photo-open.js';
import { bulkFlagSelection, flagFocusedMiss } from './flagging.js';
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

/** @param {number} delta @param {boolean} [withinCategory] */
function moveFocus(delta, withinCategory = false) {
  const categories = CATEGORY_ORDER.filter(cat => state.missesData[cat].length &&
    !document.getElementById('missesSection-' + cat)?.classList.contains('collapsed'));
  const cat = state.focusedCategory;
  if (!cat || !categories.includes(cat)) {
    if (categories[0]) setFocused(categories[0], 0);
    return;
  }
  const length = state.missesData[cat].length;
  const next = state.focusedIndex + delta;
  if (!withinCategory && (next < 0 || next >= length)) {
    const adjacent = categories[categories.indexOf(cat) + Math.sign(delta)];
    if (adjacent) {
      setFocused(adjacent, delta > 0 ? 0 : state.missesData[adjacent].length - 1);
      return;
    }
  }
  setFocused(cat, Math.max(0, Math.min(next, length - 1)));
}

export function bindMissKeyboard() {
  document.addEventListener('keydown', function(e) {
    if (e.defaultPrevented || !(e.target instanceof HTMLElement) || e.target.isContentEditable) return;
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
    if (document.querySelector('.lightbox-overlay.active, .pipeline-overlay.active, .similar-overlay.active, .modal-overlay.open, .grm-overlay.open, .help-overlay.active')) return;

    // Flag / Reject / Unflag — share the configured browse.flag/reject/unflag
    // shortcuts so a single rebind (incl. modifier combos like ctrl+p) covers
    // Browse, Lightbox, and /misses consistently. Checked before the modifier
    // guard below so modifier-based rebindings still match. Acts on the
    // multi-selection if non-empty, otherwise falls back to the focused card.
    var sc = browseShortcuts();
    var flagKey = sc.flag ?? 'p';
    var rejectKey = sc.reject ?? 'x';
    var unflagKey = sc.unflag ?? 'u';
    var missFlag = null;
    if (matchesShortcut(e, flagKey)) missFlag = 'flagged';
    else if (matchesShortcut(e, rejectKey)) missFlag = 'rejected';
    else if (matchesShortcut(e, unflagKey)) missFlag = 'none';
    if (missFlag !== null) {
      if (state.selection.size > 0) {
        e.preventDefault();
        bulkFlagSelection(missFlag);
      } else if (state.focusedCategory != null && state.focusedIndex >= 0) {
        var fp = (state.missesData[state.focusedCategory] || [])[state.focusedIndex];
        if (fp) {
          e.preventDefault();
          flagFocusedMiss(fp.id, missFlag);
        }
      }
      return;
    }

    // Bare-key shortcuts (j/k/Esc/Enter) — modifier-free only. Shift is allowed
    // here so Shift+J/K can extend the selection.
    if (e.metaKey || e.ctrlKey || e.altKey) return;

    var k = e.key;
    const delta = missNavigationDelta(k);
    if (delta) {
      e.preventDefault();
      if (e.shiftKey) extendSelectionByFocus(delta);
      else {
        clearSelection();
        moveFocus(delta);
      }
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
    } else if (k === 'Enter' && !e.target.closest('button, a, [role=button]')) {
      if (state.focusedCategory != null && state.focusedIndex >= 0) {
        e.preventDefault();
        openMiss(state.focusedCategory, state.focusedIndex);
      }
    }
  });
}

/** @param {string} key */
function missNavigationDelta(key) {
  switch (key.toLowerCase()) {
    case 'arrowright': case 'j': return 1;
    case 'arrowleft': case 'k': return -1;
    case 'arrowup': case 'arrowdown': {
      const grid = document.querySelector('.misses-grid[data-cat="' + state.focusedCategory + '"]');
      const columns = grid ? getComputedStyle(grid).gridTemplateColumns.split(/\s+/).length : 1;
      return key === 'ArrowDown' ? columns : -columns;
    }
    default: return 0;
  }
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
  moveFocus(delta, true);
  var newP = photos[state.focusedIndex];
  if (newP) setSelected(newP.id, true);
}
