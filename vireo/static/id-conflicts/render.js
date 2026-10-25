// Rendering the summary, the taxonomy note, sort, exclude and filter chips,
// and the pager; changing sort, filter and excludes.

import { state } from './state.js';
import { totalPages } from './controls.js';
import { updateBatchControls } from './decisions.js';
import { reloadRows } from './loading.js';
import { renderTable } from './table.js';
import { escapeAttr, escapeHtml, VireoViewPreferences } from './shared.js';

export function renderCompare() {
  if (!state.compareData) return;
  renderSummary();
  renderSorts();
  renderExcludes();
  renderFilters();
  renderPager();
  renderTaxonomyNote();
  renderTable(state.compareRows);
  updateBatchControls();
}

/** @returns {Record<string, number | undefined>} */
export function effectiveSummary() {
  return (state.compareData && state.compareData.summary) || {};
}

export function renderSummary() {
  var s = effectiveSummary();
  var items = [
    ['photos', 'Photos'],
    ['models', 'Models'],
    ['needs_review', 'Needs review'],
    ['model_disagreements', 'Models disagree'],
    ['keyword_model_conflicts', 'Keyword vs models'],
    ['additional', 'Additional species'],
    ['conflicts', 'Conflicts'],
    ['refinements', 'Refinements'],
    ['broader', 'Broader'],
    ['new', 'No species keyword'],
    ['missing_predictions', 'Missing predictions'],
  ];
  /** @type {HTMLElement} */ (document.getElementById('summaryGrid')).style.display = '';
  /** @type {HTMLElement} */ (document.getElementById('summaryGrid')).innerHTML = items.map(function(item) {
    return '<div class="summary-item"><span class="summary-value">' +
      escapeHtml(s[/** @type {string} */ (item[0])] || 0) + '</span><span class="summary-label">' +
      escapeHtml(item[1]) + '</span></div>';
  }).join('');
}

export function renderTaxonomyNote() {
  /** @type {HTMLElement} */ (document.getElementById('taxonomyNote')).classList.toggle(
    'visible',
    !!state.compareData && state.compareData.taxonomy_available === false
  );
}

export function renderSorts() {
  var row = /** @type {HTMLElement} */ (document.getElementById('sortRow'));
  row.style.display = '';
  row.innerHTML = '<span class="row-label">Sort by</span>' + state.compareSortDefs.map(function(s) {
    var cls = s.id === state.activeSort ? 'sort-btn active' : 'sort-btn';
    return '<button class="' + cls + '" data-compare-sort="' + escapeAttr(s.id) + '">' +
      escapeHtml(s.label) + '</button>';
  }).join('');
  var select = /** @type {HTMLSelectElement} */ (document.getElementById('sortMode'));
  if (select) select.value = state.activeSort;
}

export function renderExcludes() {
  var counts = (state.compareData && state.compareData.exclusion_counts) || {};
  var row = /** @type {HTMLElement} */ (document.getElementById('excludeRow'));
  row.style.display = '';
  row.innerHTML = '<span class="row-label">Exclude</span>' + state.compareExcludeDefs.map(function(f) {
    var cls = state.excludeState[f.id] ? 'exclude-btn active' : 'exclude-btn';
    return '<button class="' + cls + '" data-compare-exclude="' + escapeAttr(f.id) + '">' +
      escapeHtml(f.label) + ' <span>' + escapeHtml(counts[f.id] || 0) + '</span></button>';
  }).join('');
}

export function renderFilters() {
  var counts = (state.compareData && state.compareData.filter_counts) || {};
  var row = /** @type {HTMLElement} */ (document.getElementById('filterRow'));
  row.style.display = '';
  row.innerHTML = '<span class="row-label">Filter</span>' + state.compareFilterDefs.map(function(f) {
    var cls = f.id === state.activeFilter ? 'filter-btn active' : 'filter-btn';
    return '<button class="' + cls + '" data-compare-filter="' + escapeAttr(f.id) + '">' +
      escapeHtml(f.label) + ' <span>' + escapeHtml(counts[f.id] || 0) + '</span></button>';
  }).join('');
}

export function renderPager() {
  var row = /** @type {HTMLElement} */ (document.getElementById('pagerRow'));
  row.style.display = '';
  var pages = totalPages();
  var first = state.compareTotal ? (state.comparePage - 1) * state.comparePerPage + 1 : 0;
  var last = Math.min(state.compareTotal, state.comparePage * state.comparePerPage);
  /** @type {HTMLElement} */ (document.getElementById('pagerSummary')).textContent = state.compareTotal
    ? first + '–' + last + ' of ' + state.compareTotal + ' (page ' + state.comparePage + ' of ' + pages + ')'
    : 'No rows match';
  /** @type {HTMLButtonElement} */ (document.getElementById('pagerPrev')).disabled = state.comparePage <= 1;
  /** @type {HTMLButtonElement} */ (document.getElementById('pagerNext')).disabled = state.comparePage >= pages;
  var perPage = /** @type {HTMLSelectElement} */ (document.getElementById('pagerPerPage'));
  if (perPage) perPage.value = String(state.comparePerPage);
}

/** @param {string} id */
export function toggleExclude(id) {
  state.excludeState[id] = !state.excludeState[id];
  reloadRows();
}

/** @param {string} sort */
export function setSort(sort) {
  state.activeSort = sort;
  VireoViewPreferences.write('vireo.compare.sort', sort);
  reloadRows();
}

/** @param {string} filter */
export function setFilter(filter) {
  state.activeFilter = filter;
  reloadRows();
}
