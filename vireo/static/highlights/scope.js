// Score formatting and the request parameters: scope (folder or workspace)
// and the filter controls.
// ES module.

import { VireoTextSearch } from './shared.js';
import { WORKSPACE_SCOPE } from './state.js';

/** @param {number | null | undefined} v */
export function formatScore(v) {
  return v == null ? '—' : Number(v).toFixed(2);
}

/** @param {number | null | undefined} v */
export function formatPercent(v) {
  return v == null ? '' : Math.round(Number(v) * 100) + '%';
}

// The folder the page is scoped to, null when it shows every folder.
export function selectedFolderId() {
  var value = /** @type {HTMLSelectElement} */ (document.getElementById('folderSelect')).value;
  if (!value || value === WORKSPACE_SCOPE) return null;
  var id = parseInt(value, 10);
  return isNaN(id) ? null : id;
}

// What a "make top highlight" click just claimed: how many other highlights
// of the species the photo was placed ahead of, and within what.
/** @param {number} overCount @param {boolean} inFolder */
export function topHighlightToast(overCount, inFolder) {
  var where = inFolder ? ' in this folder' : '';
  if (!overCount) return 'Top highlight set' + (inFolder ? ' for this folder' : '');
  return 'Top highlight set, ahead of ' + overCount + ' other highlight'
    + (overCount === 1 ? '' : 's') + where;
}

// What adding a highlight just claimed: it goes after the highlights the
// page was showing, and says nothing about the ones outside the folder.
/** @param {{added?: boolean, after?: number[]} | null | undefined} result @param {boolean} inFolder */
export function addedHighlightToast(result, inFolder) {
  if (result && result.added === false) return 'Already a highlight';
  var afterCount = result && result.after ? result.after.length : 0;
  if (!afterCount) return 'Added to Highlights';
  return 'Added to Highlights, after the other highlight' + (inFolder ? 's in this folder' : 's');
}

export function requestScopeParams() {
  var params = new URLSearchParams();
  var folderSelect = /** @type {HTMLSelectElement} */ (document.getElementById('folderSelect'));
  var firstLoad = folderSelect.options.length === 0;
  if (folderSelect.value === WORKSPACE_SCOPE || firstLoad) {
    params.set('scope', 'workspace');
  } else if (folderSelect.value) {
    params.set('folder_id', folderSelect.value);
  }
  params.set('min_quality', (parseInt(/** @type {HTMLInputElement} */ (document.getElementById('qualitySlider')).value) / 100).toFixed(2));
  params.set('confidence_threshold', (parseInt(/** @type {HTMLInputElement} */ (document.getElementById('confidenceSlider')).value) / 100).toFixed(2));
  params.set('confirmation', /** @type {HTMLSelectElement} */ (document.getElementById('confirmationFilter')).value || 'all');
  params.set('highlight_selection', /** @type {HTMLSelectElement} */ (document.getElementById('highlightSelectionFilter')).value || 'all');
  return params;
}

/** @param {URLSearchParams} params */
export function appendHighlightFilterParams(params) {
  var search = /** @type {HTMLInputElement} */ (document.getElementById('highlightSearch')).value.trim();
  var speciesSearch = /** @type {HTMLInputElement} */ (document.getElementById('speciesSearch')).value.trim();
  if (search) {
    params.set('q', search);
    VireoTextSearch.appendParams(
      params,
      VireoTextSearch.readOptions('highlightSearch'),
      'q_match_case',
      'q_whole_word'
    );
  }
  if (speciesSearch) {
    params.set('species', speciesSearch);
    VireoTextSearch.appendParams(
      params,
      VireoTextSearch.readOptions('highlightSpecies'),
      'species_match_case',
      'species_whole_word'
    );
  }
}

export function highlightsScopeParams() {
  var params = requestScopeParams();
  appendHighlightFilterParams(params);
  return params;
}
