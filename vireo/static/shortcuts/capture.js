// Capturing a new key for one action: the button waits for the next key
// press (the global Keymap dispatcher is paused meanwhile), rejects a key
// another action in the context already uses, and saves the rest.
// ES module.

import { state } from './state.js';
import { SHORTCUT_LABELS } from './labels.js';
import { pauseKeymapDispatch, resumeKeymapDispatch } from './shared.js';
import { findConflict, isBarePageShortcut, renderShortcutsEditor, saveShortcuts } from './editor.js';

/** @param {HTMLElement} btn @param {string} ctx @param {string} action */
export function startCapture(btn, ctx, action) {
  if (state.capturingBtn) state.capturingBtn.classList.remove('capturing');
  state.capturingBtn = btn;
  btn.classList.add('capturing');
  btn.textContent = 'Press a key...';

  // Pause the global Keymap dispatcher so registered nav actions don't fire
  // (and navigate the user away) while we're trying to capture the next key.
  // Both this listener and the dispatcher run in the capture phase, but the
  // dispatcher was registered at module load and wins the race otherwise.
  pauseKeymapDispatch(onKey);

  function endCapture() {
    document.removeEventListener('keydown', onKey, true);
    resumeKeymapDispatch();
  }

  /** @param {KeyboardEvent} e */
  function onKey(e) {
    e.preventDefault();
    e.stopPropagation();
    if (['Control', 'Shift', 'Alt', 'Meta'].indexOf(e.key) !== -1) return;

    var parts = [];
    if (e.ctrlKey || e.metaKey) parts.push('ctrl');
    if (e.shiftKey) parts.push('shift');
    if (e.altKey) parts.push('alt');
    parts.push(e.key.toLowerCase());
    var keyStr = parts.join('+');
    if (ctx === 'navigation' && isBarePageShortcut(keyStr)) keyStr = '';

    var conflict = findConflict(ctx, action, keyStr);
    if (conflict) {
      btn.classList.remove('capturing');
      state.capturingBtn = null;
      endCapture();
      btn.textContent = 'Conflict: ' + (SHORTCUT_LABELS[ctx] || {})[conflict];
      btn.style.borderColor = 'var(--danger)';
      btn.style.color = 'var(--danger)';
      setTimeout(function() { renderShortcutsEditor(); }, 1500);
      return;
    }

    var shortcuts = /** @type {ShortcutConfig} */ (state.currentShortcuts);
    (shortcuts[ctx] = shortcuts[ctx] || {})[action] = keyStr;
    btn.classList.remove('capturing');
    state.capturingBtn = null;
    endCapture();
    renderShortcutsEditor();
    saveShortcuts();
  }

  document.addEventListener('keydown', onKey, true);

  function onBlur() {
    if (state.capturingBtn === btn) {
      btn.classList.remove('capturing');
      state.capturingBtn = null;
      endCapture();
      renderShortcutsEditor();
    }
    btn.removeEventListener('blur', onBlur);
  }
  btn.addEventListener('blur', onBlur);
}
