// Life List page state, view preferences, escaping, and loading /api/life-list.

import { state } from './state.js';
import { render } from './cards.js';
import { populateLifeListTaxonomyFilters } from './filters.js';
import { safeFetch, VireoViewPreferences } from './shared.js';

export function restoreLifeListViewPreferences() {
  VireoViewPreferences.restoreAll(/** @type {HTMLElement} */ (document.getElementById('controlsBar')));
}

// All dynamic values rendered into card HTML pass through the shared
// escapeAttr (vireo-utils.js) — same escaping convention as highlights.html.

/** @param {string | undefined} iso */ export function formatDate(iso) {
  if (!iso) return null;
  var d = new Date(iso);
  if (isNaN(d.getTime())) return null;
  return d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
}

export async function loadLifeList() {
  var meta = /** @type {HTMLElement} */ (document.getElementById('meta'));
  var loadSeq = ++state.lifeListLoadSeq;
  meta.textContent = 'Loading life list…';
  /** @type {LifeData} */ var data;
  try {
    data = await safeFetch('/api/life-list');
  } catch (e) {
    if (loadSeq !== state.lifeListLoadSeq) return;
    meta.textContent = 'Failed to load life list.';
    return;
  }
  if (loadSeq !== state.lifeListLoadSeq) return;
  state.currentData = data;
  populateLifeListTaxonomyFilters();
  render();
}
