// loadKeywords(): the identities fetch that every edit refetches through.
// Classic page script; load boot.js after all definitions.

// -- Load data --
async function loadKeywords() {
  try {
    const response = await fetch('/api/keywords/identities');
    if (!response.ok) throw new Error('Could not load keywords');
    identityGroups = await response.json();
    allKeywords = identityGroups.flatMap(group => group.members);
    const editable = new Set((showRecords ? allKeywords : identityGroups.filter(k => k.members.length === 1)).map(k => k.id));
    selectedIds.forEach(id => { if (!editable.has(id)) selectedIds.delete(id); });
    parentMap = {};
    allKeywords.forEach(k => { parentMap[k.id] = k.name; });
    render();
  } catch(e) {
    document.getElementById('kwEmpty').style.display = 'block';
    document.getElementById('kwEmpty').textContent = 'Failed to load keywords.';
  }
}
