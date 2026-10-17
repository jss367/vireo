// Checkbox selection: per-row and select-all listeners, the select-all
// checkbox state, and the bulk bar.
// Classic page script; load boot.js after all definitions.

// -- Checkbox selection --
function bindKeywordSelection() {
  /** @type {HTMLElement} */ (document.getElementById('kwBody')).addEventListener('change', function(e) {
    if (e.target instanceof HTMLInputElement && e.target.classList.contains('kw-cb')) {
      const id = parseInt(e.target.dataset.id || '');
      if (e.target.checked) { selectedIds.add(id); } else { selectedIds.delete(id); }
      updateBulkBar();
      updateSelectAllCheckbox();
    }
  });

  /** @type {HTMLInputElement} */ (document.getElementById('kwSelectAll')).addEventListener('change', function() {
    const checked = this.checked;
    const filtered = getFiltered().filter(k => showRecords || !k.members || k.members.length === 1);
    filtered.forEach(k => {
      if (checked) { selectedIds.add(k.id); } else { selectedIds.delete(k.id); }
    });
    render();
  });
}

function updateSelectAllCheckbox() {
  const filtered = getFiltered().filter(k => showRecords || !k.members || k.members.length === 1);
  const cb = /** @type {HTMLInputElement} */ (document.getElementById('kwSelectAll'));
  if (!filtered.length) { cb.checked = false; cb.indeterminate = false; return; }
  const allChecked = filtered.every(k => selectedIds.has(k.id));
  const someChecked = filtered.some(k => selectedIds.has(k.id));
  cb.checked = allChecked;
  cb.indeterminate = someChecked && !allChecked;
}

function updateBulkBar() {
  const bar = /** @type {HTMLButtonElement} */ (document.getElementById('kwBulkBar'));
  const count = selectedIds.size;
  /** @type {HTMLButtonElement} */ (document.getElementById('kwBulkCount')).textContent = count + ' selected';
  /** @type {HTMLButtonElement} */ (document.getElementById('kwBulkMerge')).hidden = count < 2;
  if (count > 0) { bar.classList.add('visible'); } else { bar.classList.remove('visible'); }
}
