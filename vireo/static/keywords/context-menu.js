// The right-click menu for keyword rows and identity groups.

import { state, TYPES } from './state.js';
import { deleteSelectedKeywords } from './bulk.js';
import { openMergeDialog } from './merge-dialog.js';
import { renameKeyword } from './rename.js';
import { render } from './render.js';
import { updateType } from './type-dropdown.js';
import { coerceSelectionOnContext, openContextMenu } from './shared.js';

// -- Right-click context menu on keyword rows --

/** @param {number[]} kwIds */ export function buildKeywordContextMenu(kwIds) {
  const one = kwIds.length === 1;
  const hint = one ? undefined : 'Select a single keyword';
  const singleKw = one ? state.allKeywords.find(k => k.id === kwIds[0]) : null;

  /** @param {string} t */ const typeChip = function(t) {
    return {
      label: t,
      title: 'Set type to ' + t,
      onClick: function() {
        kwIds.forEach(function(id) { updateType(id, t); });
      },
    };
  };

  return [
    { label: 'Rename', disabled: !one, disabledHint: hint,
      onClick: function() {
        if (!singleKw) return;
        const newName = prompt('Rename keyword:', singleKw.name);
        if (newName && newName.trim() && newName.trim() !== singleKw.name) {
          renameKeyword(singleKw.id, newName.trim());
        }
      } },
    ...(kwIds.length > 1 ? [{label: 'Merge selected…', onClick: () => openMergeDialog(kwIds)}] : []),
    { chips: TYPES.map(typeChip) },
    { separator: true },
    { label: 'Show Photos with this Keyword',
      disabled: !one, disabledHint: hint,
      onClick: function() {
        if (!singleKw) return;
        window.location.href = '/browse?keyword=' + encodeURIComponent(singleKw.name);
      } },
    { separator: true },
    { label: 'Delete',
      onClick: function() { deleteSelectedKeywords(); } },
  ];
}

export function bindKeywordContextMenu() {
  document.addEventListener('contextmenu', function(e) {
    if (!(e.target instanceof Element)) return; const row = e.target.closest(/** @type {'tr'} */ ('tr[data-id]'));
    if (!row) return;
    // Ignore right-clicks that land inside the context menu itself.
    if (e.target.closest('.vireo-ctx-menu')) return;
    e.preventDefault();
    const kwId = parseInt(row.dataset.id || '', 10);
    if (!kwId) return;
    const group = !state.showRecords && state.identityGroups.find(k => k.id === kwId);
    if (group && group.members.length > 1) {
      openContextMenu(e, [
        {label: 'Show photos', onClick: function() {
          window.location.href = '/browse?keyword_identity=' + encodeURIComponent(group.identity) +
            '&keyword_label=' + encodeURIComponent(group.name);
        }},
        {label: 'Edit individual records', onClick: function() {
          /** @type {HTMLInputElement} */ (document.getElementById('kwShowRecords')).checked = true;
          state.showRecords = true;
          state.selectedIds.clear();
          render();
        }}
      ]);
      return;
    }
    // Finder-style coercion: right-click outside current selection replaces it.
    const ids = coerceSelectionOnContext(state.selectedIds, kwId);
    // Re-render so the checkbox visual state reflects the coerced selection.
    render();
    openContextMenu(e, buildKeywordContextMenu(ids));
  });
}
