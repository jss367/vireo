// The search box, paging, page size, and the model and sort selects.
// Classic page script.

function scheduleSearch() {
  if (searchTimer) clearTimeout(searchTimer);
  searchTimer = setTimeout(function() {
    searchTimer = null;
    reloadRows();
  }, 250);
}

/** @param {number} page */
function goToPage(page) {
  var pages = totalPages();
  if (page < 1 || page > pages || page === comparePage) return;
  comparePage = page;
  selectedRows.clear();
  fetchRows({page: page, control: true}).catch(function() {});
}

/** @param {string} value */
function setPerPage(value) {
  var next = parseInt(value, 10);
  if (!(next > 0) || next === comparePerPage) return;
  comparePerPage = next;
  VireoViewPreferences.write(PER_PAGE_KEY, String(next));
  reloadRows();
}

function totalPages() {
  return Math.max(1, Math.ceil(compareTotal / Math.max(1, comparePerPage)));
}

// Returns true when a previously-selected specific model was dropped because
// the new collection does not carry it: the caller needs to re-fetch so the
// response's counts, statuses and sort match the columns the user now sees.
/** @param {string[]} models @param {string[]} visible */
function populateModelSelects(models, visible) {
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

function populateSortSelect() {
  var sel = /** @type {HTMLSelectElement} */ (document.getElementById('sortMode'));
  if (!sel || !compareSortDefs.length) return;
  sel.innerHTML = compareSortDefs.map(function(s) {
    return '<option value="' + escapeAttr(s.id) + '">' + escapeHtml(s.label) + '</option>';
  }).join('');
  sel.value = activeSort;
}
