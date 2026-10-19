// Loading highlights for the current scope and filters, sorting buckets,
// and the advanced-mode sort options.
// ES module.

import { isAdvancedMode, safeFetch } from './shared.js';
import { hideHighlightUndo, updateHighlightsLightboxControls } from './lightbox.js';
import { removeRejectedPhotoFromModel } from './rejects.js';
import { render } from './render.js';
import { appendHighlightFilterParams, requestScopeParams } from './scope.js';
import { WORKSPACE_SCOPE, state } from './state.js';

export async function loadHighlights() {
  var params = requestScopeParams();
  var folderSelect = /** @type {HTMLSelectElement} */ (document.getElementById('folderSelect'));
  params.set('limit_per_bucket', String(Math.max(20, parseInt(/** @type {HTMLInputElement} */ (document.getElementById('perRowSlider')).value))));
  appendHighlightFilterParams(params);

  // Capture the issue-time sequence so the prune below can tell rejections that
  // happened after this request was sent (response is stale for them) from
  // older ones the backend already excludes — or out-of-band restores it now
  // includes and we must not strip.
  var loadSeq = ++state.highlightsLoadSeq;
  /** @type {HighlightsData} */
  var data = await safeFetch('/api/highlights?' + params);
  if (loadSeq !== state.highlightsLoadSeq) return;
  state.currentData = data;

  // Guard against a reload issued against backend state from before a rejection
  // landed (e.g. un-reject starts a restore reload, then the photo is rejected
  // again before it returns). Only prune photos rejected at/after this request
  // was issued; a photo restored out-of-band has an older rejection seq, so the
  // authoritative response that now includes it is left intact.
  state.rejectedFromGrid.forEach(function(id) {
    if ((state.rejectedAtLoadSeq[id] || 0) >= loadSeq) removeRejectedPhotoFromModel(id);
  });

  // Populate folder dropdown on first load.
  if (!folderSelect.options.length) {
    var wsOpt = document.createElement('option');
    wsOpt.value = WORKSPACE_SCOPE;
    wsOpt.textContent = 'All folders in this workspace';
    folderSelect.appendChild(wsOpt);
    data.folders.forEach(function(f) {
      var opt = document.createElement('option');
      opt.value = String(f.id);
      opt.textContent = f.name + ' (' + f.photo_count + ')';
      folderSelect.appendChild(opt);
    });
  }
  var selectedOption = folderSelect.options[folderSelect.selectedIndex];
  state.currentFolderName = selectedOption
    ? (selectedOption.textContent || '').replace(/\s*\(\d+\)$/, '')
    : '';

  render();
}

export function sortedBuckets() {
  var sort = /** @type {HTMLSelectElement} */ (document.getElementById('sortSelect')).value;
  var buckets = (/** @type {HighlightsData} */ (state.currentData).buckets || []).slice();
  /** @param {HighlightsBucket} a @param {HighlightsBucket} b @param {boolean} newest */
  function compareTimestamp(a, b, newest) {
    var ta = a.best_timestamp || '';
    var tb = b.best_timestamp || '';
    if (!ta && !tb) return 0;
    if (!ta) return 1;
    if (!tb) return -1;
    return newest ? String(tb).localeCompare(String(ta)) : String(ta).localeCompare(String(tb));
  }
  buckets.sort(function(a, b) {
    if (sort === 'most') return b.photo_count - a.photo_count;
    if (sort === 'best') return (b.best_score || 0) - (a.best_score || 0);
    if (sort === 'worst') return (a.best_score || 0) - (b.best_score || 0);
    if (sort === 'species_asc') return String(a.species || '').localeCompare(String(b.species || ''));
    if (sort === 'species_desc') return String(b.species || '').localeCompare(String(a.species || ''));
    if (sort === 'newest') return compareTimestamp(a, b, true);
    if (sort === 'oldest') return compareTimestamp(a, b, false);
    if (sort === 'confidence') return (b.avg_confidence || 0) - (a.avg_confidence || 0);
    if (sort === 'confirmed') {
      var status = Number(!!b.is_accepted) - Number(!!a.is_accepted);
      if (status) return status;
      return (b.best_score || 0) - (a.best_score || 0);
    }
    return a.photo_count - b.photo_count; // 'fewest' default
  });
  return buckets;
}

export function isHighlightsAdvancedMode() {
  return isAdvancedMode();
}

export function updateAdvancedHighlightsOptions() {
  var advanced = isHighlightsAdvancedMode();
  var sortSelect = /** @type {HTMLSelectElement} */ (document.getElementById('sortSelect'));
  if (sortSelect) {
    var recommended = /** @type {HTMLOptionElement | null} */ (sortSelect.querySelector('option[value="best"]'));
    if (recommended) {
      recommended.textContent = advanced
        ? (recommended.dataset.advancedLabel || 'Best photo first')
        : (recommended.dataset.defaultLabel || 'Recommended first');
    }
    /** @type {NodeListOf<HTMLOptionElement>} */ (sortSelect.querySelectorAll('option[data-advanced-best-sort]')).forEach(function(opt) {
      opt.hidden = !advanced;
      opt.disabled = !advanced;
    });
    if (!advanced && sortSelect.value === 'worst') {
      sortSelect.value = 'best';
    }
  }
  if (state.currentData) render();
}

export function debounceLoad() {
  if (state.debounceTimer) clearTimeout(state.debounceTimer);
  state.debounceTimer = setTimeout(loadHighlights, 300);
}

/** The undo/redo page hook: drop this session's reject tracking and reload. */
export async function highlightsAfterHistoryChange() {
  state.rejectedFromGrid.clear();
  state.rejectedAtLoadSeq = {};
  hideHighlightUndo();
  await loadHighlights();
  updateHighlightsLightboxControls(state.activeLightboxPhotoId);
}
