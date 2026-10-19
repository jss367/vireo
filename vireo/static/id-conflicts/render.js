// Rendering the summary, the taxonomy note, sort, exclude and filter chips,
// and the pager; changing sort, filter and excludes.
// Classic page script.

function renderCompare() {
  if (!compareData) return;
  renderSummary();
  renderSorts();
  renderExcludes();
  renderFilters();
  renderPager();
  renderTaxonomyNote();
  renderTable(compareRows);
  updateBatchControls();
}

/** @returns {Record<string, number | undefined>} */
function effectiveSummary() {
  return (compareData && compareData.summary) || {};
}

function renderSummary() {
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

function renderTaxonomyNote() {
  /** @type {HTMLElement} */ (document.getElementById('taxonomyNote')).classList.toggle(
    'visible',
    !!compareData && compareData.taxonomy_available === false
  );
}

function renderSorts() {
  var row = /** @type {HTMLElement} */ (document.getElementById('sortRow'));
  row.style.display = '';
  row.innerHTML = '<span class="row-label">Sort by</span>' + compareSortDefs.map(function(s) {
    var cls = s.id === activeSort ? 'sort-btn active' : 'sort-btn';
    return '<button class="' + cls + '" onclick="setSort(\'' + s.id + '\')">' +
      escapeHtml(s.label) + '</button>';
  }).join('');
  var select = /** @type {HTMLSelectElement} */ (document.getElementById('sortMode'));
  if (select) select.value = activeSort;
}

function renderExcludes() {
  var counts = (compareData && compareData.exclusion_counts) || {};
  var row = /** @type {HTMLElement} */ (document.getElementById('excludeRow'));
  row.style.display = '';
  row.innerHTML = '<span class="row-label">Exclude</span>' + compareExcludeDefs.map(function(f) {
    var cls = excludeState[f.id] ? 'exclude-btn active' : 'exclude-btn';
    return '<button class="' + cls + '" onclick="toggleExclude(\'' + f.id + '\')">' +
      escapeHtml(f.label) + ' <span>' + escapeHtml(counts[f.id] || 0) + '</span></button>';
  }).join('');
}

function renderFilters() {
  var counts = (compareData && compareData.filter_counts) || {};
  var row = /** @type {HTMLElement} */ (document.getElementById('filterRow'));
  row.style.display = '';
  row.innerHTML = '<span class="row-label">Filter</span>' + compareFilterDefs.map(function(f) {
    var cls = f.id === activeFilter ? 'filter-btn active' : 'filter-btn';
    return '<button class="' + cls + '" onclick="setFilter(\'' + f.id + '\')">' +
      escapeHtml(f.label) + ' <span>' + escapeHtml(counts[f.id] || 0) + '</span></button>';
  }).join('');
}

function renderPager() {
  var row = /** @type {HTMLElement} */ (document.getElementById('pagerRow'));
  row.style.display = '';
  var pages = totalPages();
  var first = compareTotal ? (comparePage - 1) * comparePerPage + 1 : 0;
  var last = Math.min(compareTotal, comparePage * comparePerPage);
  /** @type {HTMLElement} */ (document.getElementById('pagerSummary')).textContent = compareTotal
    ? first + '–' + last + ' of ' + compareTotal + ' (page ' + comparePage + ' of ' + pages + ')'
    : 'No rows match';
  /** @type {HTMLButtonElement} */ (document.getElementById('pagerPrev')).disabled = comparePage <= 1;
  /** @type {HTMLButtonElement} */ (document.getElementById('pagerNext')).disabled = comparePage >= pages;
  var perPage = /** @type {HTMLSelectElement} */ (document.getElementById('pagerPerPage'));
  if (perPage) perPage.value = String(comparePerPage);
}

/** @param {string} id */
function toggleExclude(id) {
  excludeState[id] = !excludeState[id];
  reloadRows();
}

/** @param {string} sort */
function setSort(sort) {
  activeSort = sort;
  VireoViewPreferences.write('vireo.compare.sort', sort);
  reloadRows();
}

/** @param {string} filter */
function setFilter(filter) {
  activeFilter = filter;
  reloadRows();
}
