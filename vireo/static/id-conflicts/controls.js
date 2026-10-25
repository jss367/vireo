// The search box, paging, page size, and the model and sort selects.

import { PER_PAGE_KEY, state } from './state.js';
import { fetchRows, reloadRows } from './loading.js';
import { escapeAttr, escapeHtml, VireoViewPreferences } from './shared.js';

export function scheduleSearch() {
  if (state.searchTimer) clearTimeout(state.searchTimer);
  state.searchTimer = setTimeout(function() {
    state.searchTimer = null;
    reloadRows();
  }, 250);
}

/** @param {number} page */
export function goToPage(page) {
  var pages = totalPages();
  if (page < 1 || page > pages || page === state.comparePage) return;
  state.comparePage = page;
  state.selectedRows.clear();
  fetchRows({page: page, control: true}).catch(function() {});
}

/** @param {string} value */
export function setPerPage(value) {
  var next = parseInt(value, 10);
  if (!(next > 0) || next === state.comparePerPage) return;
  state.comparePerPage = next;
  VireoViewPreferences.write(PER_PAGE_KEY, String(next));
  reloadRows();
}

export function totalPages() {
  return Math.max(1, Math.ceil(state.compareTotal / Math.max(1, state.comparePerPage)));
}

// Returns true when a previously-selected specific model was dropped because
// the new collection does not carry it: the caller needs to re-fetch so the
// response's counts, statuses and sort match the columns the user now sees.
/** @param {string[]} models */
export function populateModelSelects(models) {
  var a = /** @type {HTMLSelectElement} */ (document.getElementById('modelA'));
  var b = /** @type {HTMLSelectElement} */ (document.getElementById('modelB'));
  var oldA = a.value || 'all';
  var oldB = b.value || '';
  a.innerHTML = '<option value="all">All models</option>';
  b.innerHTML = '<option value="">No second model</option>';
  models.forEach(function(m) {
    a.innerHTML += '<option value="' + escapeAttr(m) + '">' + escapeHtml(m) + '</option>';
    b.innerHTML += '<option value="' + escapeAttr(m) + '">' + escapeHtml(m) + '</option>';
  });
  var newA = models.indexOf(oldA) >= 0 ? oldA : 'all';
  var newB = models.indexOf(oldB) >= 0 ? oldB : '';
  a.value = newA;
  b.value = newB;
  var droppedA = oldA !== 'all' && newA !== oldA;
  var droppedB = oldB !== '' && newB !== oldB;
  return droppedA || droppedB;
}

export function populateSortSelect() {
  var sel = /** @type {HTMLSelectElement} */ (document.getElementById('sortMode'));
  if (!sel || !state.compareSortDefs.length) return;
  sel.innerHTML = state.compareSortDefs.map(function(s) {
    return '<option value="' + escapeAttr(s.id) + '">' + escapeHtml(s.label) + '</option>';
  }).join('');
  sel.value = state.activeSort;
}
