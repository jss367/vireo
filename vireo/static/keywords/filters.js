// The records/groups toggle, search, type filter buttons, and column sorting.
// Classic page script; load boot.js after all definitions.

// -- Search & filter --
function bindKeywordRecordToggles() {
  /** @type {HTMLInputElement} */ (document.getElementById('kwShowRecords')).addEventListener('change', function() {
    showRecords = this.checked;
    selectedIds.clear();
    render();
  });
  /** @type {HTMLElement} */ (document.getElementById('kwBody')).addEventListener('click', function(event) {
    if (!(event.target instanceof Element) || !event.target.closest('[data-edit-records]')) return;
    /** @type {HTMLInputElement} */ (document.getElementById('kwShowRecords')).checked = true;
    showRecords = true;
    selectedIds.clear();
    render();
  });
}

function bindKeywordSearch() {
  /** @type {HTMLInputElement} */ (document.getElementById('kwSearch')).addEventListener('input', function() {
    searchTerm = this.value.trim();
    render();
  });
}

function bindKeywordFilterButtons() {
  document.querySelectorAll(/** @type {'button'} */ ('.kw-filter-btn')).forEach(btn => {
    btn.addEventListener('click', function() {
      document.querySelectorAll(/** @type {'button'} */ ('.kw-filter-btn')).forEach(b => b.classList.remove('active'));
      this.classList.add('active');
      filterType = this.dataset.type || 'all';
      render();
    });
  });
}

// -- Sorting --
function bindKeywordSorting() {
  document.querySelectorAll(/** @type {'th'} */ ('.kw-table th[data-col]')).forEach(th => {
    th.addEventListener('click', function() {
      const col = this.dataset.col || 'name';
      if (sortCol === col) { sortDir *= -1; } else { sortCol = col; sortDir = 1; }
      // Update header styles
      document.querySelectorAll('.kw-table th').forEach(h => {
        h.classList.remove('sorted');
        const arrow = h.querySelector('.sort-arrow');
        if (arrow) arrow.innerHTML = '&#9650;';
      });
      this.classList.add('sorted');
      const arrow = this.querySelector('.sort-arrow');
      if (arrow) arrow.innerHTML = sortDir === 1 ? '&#9650;' : '&#9660;';
      render();
    });
  });
}
