// Loading highlights for the current scope and filters, sorting buckets,
// and the advanced-mode sort options.
// ES module.

import { VireoViewPreferences, isAdvancedMode, safeFetch } from './shared.js';
import { hideHighlightUndo, updateHighlightsLightboxControls } from './lightbox.js';
import { removeRejectedPhotoFromModel } from './rejects.js';
import { render } from './render.js';
import { highlightsScopeParams } from './scope.js';
import { WORKSPACE_SCOPE, state } from './state.js';

/** @param {HTMLSelectElement} folderSelect @param {HighlightsData['folders']} folderRows
 * @param {NonNullable<HighlightsData['collections']>} collectionRows */
function populateScopePicker(folderSelect, folderRows, collectionRows) {
  var firstLoad = !folderSelect.options.length;
  var saved = firstLoad ? VireoViewPreferences.read('vireo.highlights.folder') : folderSelect.value;
  folderSelect.replaceChildren(new Option('All folders in this workspace', WORKSPACE_SCOPE));
  var folders = document.createElement('optgroup');
  folders.label = 'Folders';
  folderRows.forEach(function(f) {
    var option = new Option(f.name + ' (' + f.photo_count + ')', String(f.id));
    option.dataset.name = f.name;
    folders.appendChild(option);
  });
  folderSelect.appendChild(folders);
  var collections = document.createElement('optgroup');
  collections.label = 'Collections';
  collectionRows.forEach(function(c) {
    var option = new Option(c.name + (c.degraded ? ' (unavailable — rule error)' : ''), 'collection:' + c.id);
    option.disabled = c.degraded;
    collections.appendChild(option);
  });
  folderSelect.appendChild(collections);
  if (saved && Array.from(folderSelect.options).some(o => o.value === saved && !o.disabled)) {
    folderSelect.value = saved;
  }
}

/** @param {boolean} [refreshCollections] */
export async function loadHighlights(refreshCollections = false) {
  var params = highlightsScopeParams();
  var scope = params.toString();
  var folderSelect = /** @type {HTMLSelectElement} */ (document.getElementById('folderSelect'));
  if (!folderSelect.options.length || refreshCollections) params.set('include_collections', '1');
  params.set('limit_per_bucket', String(Math.max(20, parseInt(/** @type {HTMLInputElement} */ (document.getElementById('perRowSlider')).value))));

  // Capture the issue-time sequence so the prune below can tell rejections that
  // happened after this request was sent (response is stale for them) from
  // older ones the backend already excludes — or out-of-band restores it now
  // includes and we must not strip.
  var refetchSeqAtLoad = { ...state.bucketRefetchSeq };
  var loadSeq = ++state.highlightsLoadSeq;
  /** @type {HighlightsData} */
  var data;
  try {
    data = await safeFetch('/api/highlights?' + params);
  } catch (error) {
    if (loadSeq === state.highlightsLoadSeq && params.has('collection_id')) {
      // safeFetch has shown the failure. Return the selector and grid to a usable scope.
      folderSelect.value = WORKSPACE_SCOPE;
      VireoViewPreferences.write('vireo.highlights.folder', WORKSPACE_SCOPE);
      return loadHighlights(true);
    }
    throw error;
  }
  if (loadSeq !== state.highlightsLoadSeq) return;
  // A Pick/Unpick after this request began can make its prefix stale against
  // the backend tail. Fetch again before replacing buckets or settling their
  // guards; that bucket refetch still targets the currently displayed data.
  if (Object.keys(state.bucketRefetchSeq).some(function(key) {
    return state.bucketRefetchSeq[key] !== refetchSeqAtLoad[key];
  })) return loadHighlights(refreshCollections);
  state.currentData = data;
  state.loadedHighlightsScope = scope;
  // No refetch began after this reload was issued, so its replacement buckets
  // can clear prior refetch failures and resume pagination. Mark every
  // outstanding refetch seq as settled too: an in-flight (or hung)
  // refetch would otherwise leave bucketRefetchSeq > bucketRefetchDone, and
  // loadMoreBucket's guard would then reject pagination against the new,
  // authoritative bucket until that obsolete request finally returns. Its own
  // finally only bumps `done` past our write, so this does not race it.
  Object.keys(state.bucketRefetchSeq).forEach(function(key) {
    var pendingSeq = state.bucketRefetchSeq[key] || 0;
    if (pendingSeq > (state.bucketRefetchDone[key] || 0)) {
      state.bucketRefetchDone[key] = pendingSeq;
    }
  });
  state.bucketRefetchFailed = {};

  // Guard against a reload issued against backend state from before a rejection
  // landed (e.g. un-reject starts a restore reload, then the photo is rejected
  // again before it returns). Only prune photos rejected at/after this request
  // was issued; a photo restored out-of-band has an older rejection seq, so the
  // authoritative response that now includes it is left intact.
  state.rejectedFromGrid.forEach(function(id) {
    if ((state.rejectedAtLoadSeq[id] || 0) >= loadSeq) removeRejectedPhotoFromModel(id);
  });

  if (data.collections) {
    var firstLoad = !folderSelect.options.length;
    populateScopePicker(folderSelect, data.folders, data.collections);
    if (firstLoad && folderSelect.value !== WORKSPACE_SCOPE) return loadHighlights();
  }
  var selectedOption = folderSelect.options[folderSelect.selectedIndex];
  state.currentScopeName = selectedOption
    ? (selectedOption.dataset?.name || selectedOption.textContent || '')
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
