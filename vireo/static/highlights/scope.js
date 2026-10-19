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
  params.set('species_representative', /** @type {HTMLSelectElement} */ (document.getElementById('representativeFilter')).value || 'all');
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
