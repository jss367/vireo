// Page startup.
// Classic page script; loads after every other Process page definition.

// The native menu's Run Pipeline command (vireo-page-hooks.js).
Vireo.registerPageHooks({startPipeline: startPipeline});

// Preserve the inline script's order: page-init, plan, and advanced-option
// handlers, saved processes, then the slot poller.
document.addEventListener('DOMContentLoaded', initPipelinePage);
document.addEventListener('DOMContentLoaded', function() {
  loadLegacyDetections();
  /** @type {HTMLElement} */ (document.getElementById('btnReclassifyLegacy')).addEventListener('click', reclassifyLegacyFolders);
});
// Render the source prompt immediately while page-init is in flight.
document.addEventListener('DOMContentLoaded', refreshPipelineUI);
document.addEventListener('DOMContentLoaded', updateAdvancedPipelineOptions);
window.addEventListener('advancedmodechange', updateAdvancedPipelineOptions);
window.addEventListener('devmodechange', updateAdvancedPipelineOptions);

document.addEventListener('DOMContentLoaded', function() { loadSavedProcesses(null); });

bindSlotPolling();
