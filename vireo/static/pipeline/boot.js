// Page startup.
// Classic page script; loads after every other Process page definition.

// Preserve the inline script's order: page-init, plan, and advanced-option
// handlers, saved processes, then the slot poller.
document.addEventListener('DOMContentLoaded', initPipelinePage);
// Render the source prompt immediately while page-init is in flight.
document.addEventListener('DOMContentLoaded', refreshPipelineUI);
document.addEventListener('DOMContentLoaded', updateAdvancedPipelineOptions);
window.addEventListener('advancedmodechange', updateAdvancedPipelineOptions);
window.addEventListener('devmodechange', updateAdvancedPipelineOptions);

document.addEventListener('DOMContentLoaded', function() { loadSavedProcesses(null); });

bindSlotPolling();
