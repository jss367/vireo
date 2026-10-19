// Page startup: the module the template loads. Binds the controls that used
// to carry inline handlers and shows the desktop-only Browse button.
// ES module.

import { byId } from './state.js';
import { isTauri } from './shared.js';
import { addCatalog, bindCatalogList, browseForCatalog, lrShowPhase } from './catalogs.js';
import { previewImport } from './preview.js';
import { executeLrImport, lrReset } from './import-job.js';

/** @param {string} id @param {() => unknown} action */
function onClick(id, action) {
  byId(id).addEventListener('click', function() { action(); });
}

/* ---------- Phase 1: Select ---------- */
byId('catalogInput').addEventListener('keydown', function(event) {
  if (event.key === 'Enter') addCatalog();
});
onClick('lrBrowseBtn', browseForCatalog);
onClick('lrAddBtn', addCatalog);
bindCatalogList();
onClick('lrPreviewBtn', previewImport);

/* ---------- Phase 2: Preview ---------- */
onClick('lrBackBtn', function() { lrShowPhase(1); });
onClick('lrImportBtn', executeLrImport);

/* ---------- Phase 3: Progress ---------- */
onClick('lrDoneBtn', lrReset);

/* ---------- Native file picker ---------- */
if (isTauri()) {
  document.querySelectorAll('.tauri-only').forEach(function(el) {
    /** @type {HTMLElement} */ (el).style.display = '';
  });
}
