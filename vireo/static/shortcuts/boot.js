// Page startup: the module the template loads. Binds the editor's buttons
// (rendered markup names its context and action in data attributes instead of
// inline handlers) and loads the bindings.
// ES module.

import { loadShortcuts, resetContext, resetShortcut } from './editor.js';
import { startCapture } from './capture.js';

/** One delegated click listener: the editor is re-rendered after every change. */
function bindEditorClicks() {
  var editor = /** @type {HTMLElement} */ (document.getElementById('shortcutsEditor'));
  editor.addEventListener('click', function(event) {
    var button = event.target instanceof Element ? event.target.closest('button') : null;
    if (!(button instanceof HTMLElement)) return;
    var ctx = button.dataset.ctx || '';
    var action = button.dataset.action || '';
    if (button.dataset.shortcutResetContext) resetContext(button.dataset.shortcutResetContext);
    else if (button.classList.contains('shortcut-key-btn')) startCapture(button, ctx, action);
    else if (button.classList.contains('shortcut-reset-btn')) resetShortcut(ctx, action);
  });
}

bindEditorClicks();
loadShortcuts();
