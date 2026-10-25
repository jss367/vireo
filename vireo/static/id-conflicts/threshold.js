// The conflict-threshold slider: its label, and saving a new threshold and
// reloading the comparison with it.

import { clampConfidence, CONFLICT_THRESHOLD_KEY, state } from './state.js';
import { reloadRows } from './loading.js';

export function updateConflictThresholdControl() {
  var input = /** @type {HTMLInputElement} */ (document.getElementById('conflictThreshold'));
  var value = /** @type {HTMLElement} */ (document.getElementById('conflictThresholdValue'));
  if (input) input.value = String(state.conflictMinConfidence);
  if (value) value.textContent = Math.round(state.conflictMinConfidence * 100) + '%';
}

// The slider's label follows the drag, but the comparison is only re-derived
// when the drag ends: the threshold changes what every row *means*, so it
// rebuilds the whole assessment rather than re-filtering a list.
/** @param {string} value */
export function previewConflictThreshold(value) {
  var label = /** @type {HTMLElement} */ (document.getElementById('conflictThresholdValue'));
  if (label) label.textContent = Math.round(clampConfidence(value) * 100) + '%';
}

/** @param {string} value */
export function setConflictThreshold(value) {
  var next = clampConfidence(value);
  if (next === state.conflictMinConfidence) return;
  state.conflictMinConfidence = next;
  try {
    if (window.localStorage) {
      window.localStorage.setItem(CONFLICT_THRESHOLD_KEY, String(state.conflictMinConfidence));
    }
  } catch (_e) {}
  updateConflictThresholdControl();
  reloadRows();
}
