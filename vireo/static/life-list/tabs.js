// List/Explorer tab switching and the explorer's lazy first load.

import { state } from './state.js';
import { loadExplorer } from './explorer.js';

// ---------------- Explorer tab ----------------
   // lazy-load guard: fetch explorer once on first open

/** @param {string} name */ export function llSwitchTab(name) {
  document.querySelectorAll(/** @type {'div'} */ ('.ll-tab')).forEach(function(t) {
    t.classList.toggle('active', t.dataset.tab === name);
  });
  document.querySelectorAll('.ll-tabpanel').forEach(function(p) {
    p.classList.toggle('active', p.id === 'tab-' + name);
  });
  try {
    var url = new URL(window.location.href);
    url.searchParams.set('view', name);
    history.replaceState(null, '', url.toString());
  } catch (e) {}
  if (name === 'explorer' && !state.explorerLoaded) {
    state.explorerLoaded = true;
    loadExplorer();
  }
}
