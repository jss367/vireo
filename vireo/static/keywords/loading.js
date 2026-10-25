// loadKeywords(): the identities fetch that every edit refetches through.

import { state } from './state.js';
import { render } from './render.js';

// -- Load data --
export async function loadKeywords() {
  try {
    const response = await fetch('/api/keywords/identities');
    if (!response.ok) throw new Error('Could not load keywords');
    state.identityGroups = await response.json();
    state.allKeywords = state.identityGroups.flatMap(group => group.members);
    const editable = new Set((state.showRecords ? state.allKeywords : state.identityGroups.filter(k => k.members.length === 1)).map(k => k.id));
    state.selectedIds.forEach(id => { if (!editable.has(id)) state.selectedIds.delete(id); });
    state.parentMap = {};
    state.allKeywords.forEach(k => { state.parentMap[k.id] = k.name; });
    render();
  } catch(e) {
    /** @type {HTMLElement} */ (document.getElementById('kwEmpty')).style.display = 'block';
    /** @type {HTMLElement} */ (document.getElementById('kwEmpty')).textContent = 'Failed to load keywords.';
  }
}
