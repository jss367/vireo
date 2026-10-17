// Page startup.
// Classic page script; loads after every other Rapid Review definition.

// Preserve the inline script's order: the Keymap scope, the queue prompt
// listener, the history hooks, the keydown handler, the shortcut config and
// results fetches, then the stage's pointer listeners.

if (window.Keymap && window.Keymap.setScope) {
  window.Keymap.setScope(RAPID_SCOPE);
}

bindQueuePromptChoices();
registerRapidHistoryHooks();
bindRapidKeyboard();

loadRapidShortcuts();
initRapidReview();
/** @type {HTMLElement} */ (document.getElementById('photoStage')).addEventListener('pointerdown', rapidZoomPointerDown);
/** @type {HTMLElement} */ (document.getElementById('photoStage')).addEventListener('pointermove', rapidZoomPointerMove);
/** @type {HTMLElement} */ (document.getElementById('photoStage')).addEventListener('pointerup', rapidZoomPointerUp);
/** @type {HTMLElement} */ (document.getElementById('photoStage')).addEventListener('pointercancel', rapidZoomPointerUp);
