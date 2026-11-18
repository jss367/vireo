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

let subjectCountsRequest = 0;
document.getElementById('subjectCounts')?.addEventListener('toggle', async function(event) {
  if (!(event.target instanceof HTMLDetailsElement)) return;
  const requestId = ++subjectCountsRequest;
  if (!event.target.open) return;
  const result = document.getElementById('subjectCountsResult');
  if (!result) return;
  result.textContent = 'Checking workspace…';
  try {
    const counts = /** @type {{unnamed: number, stale_mask: number, stale_score: number}} */ (
      await safeFetch('/api/audit/subjects/counts', {}, {toast: false}));
    if (requestId !== subjectCountsRequest) return;
    result.textContent = counts.unnamed + ' unclassified subjects with whole-frame predictions · ' +
      counts.stale_mask + ' active mask mismatches · ' + counts.stale_score + ' quality score mismatches';
  } catch (error) {
    if (requestId !== subjectCountsRequest) return;
    result.textContent = 'Could not load subject evidence counts: ' + String(error);
  }
});
