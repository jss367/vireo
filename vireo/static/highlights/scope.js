// Score formatting and the request parameters: scope (workspace, folder or collection)
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

/** @returns {{folder_id?: number, collection_id?: number}} */
export function selectedScope() {
  var value = /** @type {HTMLSelectElement} */ (document.getElementById('folderSelect')).value;
  if (!value || value === WORKSPACE_SCOPE) return {};
  if (value.startsWith('collection:')) return { collection_id: Number(value.slice('collection:'.length)) };
  return { folder_id: Number(value) };
}

// "folder" or "collection" when the page shows one, '' for the whole workspace.
export function selectedScopeName() {
  var scope = selectedScope();
  return scope.collection_id ? 'collection' : scope.folder_id ? 'folder' : '';
}

// What a "make top highlight" click just claimed: how many other highlights
// of the species the photo was placed ahead of, and within what.
/** @param {number} overCount @param {string} scopeName */
export function topHighlightToast(overCount, scopeName) {
  if (!overCount) return 'Top highlight set' + (scopeName ? ' for this ' + scopeName : '');
  return 'Top highlight set, ahead of ' + overCount + ' other highlight'
    + (overCount === 1 ? '' : 's') + (scopeName ? ' in this ' + scopeName : '');
}

// What adding a highlight just claimed: it goes after the highlights the
// page was showing, and says nothing about the ones outside that scope.
/** @param {{added?: boolean, after?: number[]} | null | undefined} result @param {string} scopeName */
export function addedHighlightToast(result, scopeName) {
  if (result && result.added === false) return 'Already a highlight';
  var afterCount = result && result.after ? result.after.length : 0;
  if (!afterCount) return 'Added to Highlights';
  return 'Added to Highlights, after the other highlights' + (scopeName ? ' in this ' + scopeName : '');
}

export function requestScopeParams() {
  var params = new URLSearchParams();
  var scope = selectedScope();
  if (scope.collection_id != null) params.set('collection_id', String(scope.collection_id));
  else if (scope.folder_id != null) params.set('folder_id', String(scope.folder_id));
  else params.set('scope', 'workspace');
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
