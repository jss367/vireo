// The right-click menu for keyword rows and identity groups.
// Classic page script; load boot.js after all definitions.

// -- Right-click context menu on keyword rows --
// Expose selectedIds for tests to inspect Finder-style coercion.
window.__kwSelectedIds = selectedIds;

function buildKeywordContextMenu(kwIds) {
  const one = kwIds.length === 1;
  const hint = one ? undefined : 'Select a single keyword';
  const singleKw = one ? allKeywords.find(k => k.id === kwIds[0]) : null;

  const typeChip = function(t) {
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

function bindKeywordContextMenu() {
  document.addEventListener('contextmenu', function(e) {
    const row = e.target.closest('tr[data-id]');
    if (!row) return;
    // Ignore right-clicks that land inside the context menu itself.
    if (e.target.closest('.vireo-ctx-menu')) return;
    e.preventDefault();
    const kwId = parseInt(row.dataset.id, 10);
    if (!kwId) return;
    const group = !showRecords && identityGroups.find(k => k.id === kwId);
    if (group && group.members.length > 1) {
      openContextMenu(e, [
        {label: 'Show photos', onClick: function() {
          window.location.href = '/browse?keyword_identity=' + encodeURIComponent(group.identity) +
            '&keyword_label=' + encodeURIComponent(group.name);
        }},
        {label: 'Edit individual records', onClick: function() {
          document.getElementById('kwShowRecords').checked = true;
          showRecords = true;
          selectedIds.clear();
          render();
        }}
      ]);
      return;
    }
    // Finder-style coercion: right-click outside current selection replaces it.
    const ids = coerceSelectionOnContext(selectedIds, kwId);
    // Re-render so the checkbox visual state reflects the coerced selection.
    render();
    openContextMenu(e, buildKeywordContextMenu(ids));
  });
}
