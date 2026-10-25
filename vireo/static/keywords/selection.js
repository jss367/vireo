// Checkbox selection: per-row and select-all listeners, the select-all
// checkbox state, and the bulk bar.

import { state } from './state.js';
import { getFiltered } from './filtering.js';
import { render } from './render.js';

// -- Checkbox selection --
export function bindKeywordSelection() {
  /** @type {HTMLElement} */ (document.getElementById('kwBody')).addEventListener('change', function(e) {
    if (e.target instanceof HTMLInputElement && e.target.classList.contains('kw-cb')) {
      const id = parseInt(e.target.dataset.id || '');
      if (e.target.checked) { state.selectedIds.add(id); } else { state.selectedIds.delete(id); }
      updateBulkBar();
      updateSelectAllCheckbox();
    }
  });

  /** @type {HTMLInputElement} */ (document.getElementById('kwSelectAll')).addEventListener('change', function() {
    const checked = this.checked;
    const filtered = getFiltered().filter(k => state.showRecords || !k.members || k.members.length === 1);
    filtered.forEach(k => {
      if (checked) { state.selectedIds.add(k.id); } else { state.selectedIds.delete(k.id); }
    });
    render();
  });
}

export function updateSelectAllCheckbox() {
  const filtered = getFiltered().filter(k => state.showRecords || !k.members || k.members.length === 1);
  const cb = /** @type {HTMLInputElement} */ (document.getElementById('kwSelectAll'));
  if (!filtered.length) { cb.checked = false; cb.indeterminate = false; return; }
  const allChecked = filtered.every(k => state.selectedIds.has(k.id));
  const someChecked = filtered.some(k => state.selectedIds.has(k.id));
  cb.checked = allChecked;
  cb.indeterminate = someChecked && !allChecked;
}

export function updateBulkBar() {
  const bar = /** @type {HTMLButtonElement} */ (document.getElementById('kwBulkBar'));
  const count = state.selectedIds.size;
  /** @type {HTMLButtonElement} */ (document.getElementById('kwBulkCount')).textContent = count + ' selected';
  /** @type {HTMLButtonElement} */ (document.getElementById('kwBulkMerge')).hidden = count < 2;
  if (count > 0) { bar.classList.add('visible'); } else { bar.classList.remove('visible'); }
}
