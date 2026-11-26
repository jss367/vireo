// Page startup.
// Classic page script; loads after every other Rapid Review definition.

if (window.Keymap && window.Keymap.setScope) {
  window.Keymap.setScope(RAPID_SCOPE);
}

bindQueuePromptChoices();
registerRapidHistoryHooks();

loadRapidShortcuts().catch(error => {
  console.warn('Could not load Rapid Review shortcuts.', error);
}).then(initRapidReview);
/** @type {HTMLElement} */ (document.getElementById('photoStage')).addEventListener('pointerdown', rapidZoomPointerDown);
/** @type {HTMLElement} */ (document.getElementById('photoStage')).addEventListener('pointermove', rapidZoomPointerMove);
/** @type {HTMLElement} */ (document.getElementById('photoStage')).addEventListener('pointerup', rapidZoomPointerUp);
/** @type {HTMLElement} */ (document.getElementById('photoStage')).addEventListener('pointercancel', rapidZoomPointerUp);
