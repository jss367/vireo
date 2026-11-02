// Page startup: the module the template loads. Binds the list, filter and
// detail-pane listeners, then runs the first fetches and the pollers.

import { jobsElement, state } from './state.js';
import { bindDetailPaneActions } from './actions.js';
import { applyFilters, applyOriginFilter, bindJobListClicks } from './list.js';
import { fetchHistory, fetchJobs, retryJobsPolling } from './polling.js';
import { bindSourceCleanupConfirm } from './source-cleanup.js';
import { bindStepRetryButtons } from './steps.js';

// Preserve the old inline script's order: list clicks, the cleanup checkbox,
// detail pane clicks, then the initial fetches and pollers.
bindJobListClicks();
bindSourceCleanupConfirm();
bindStepRetryButtons();
bindDetailPaneActions();
jobsElement('retryJobsPolling', HTMLButtonElement).addEventListener('click', retryJobsPolling);
// The type and status filters apply on change.
jobsElement('filterType', HTMLSelectElement).addEventListener('change', applyFilters);
jobsElement('filterStatus', HTMLSelectElement).addEventListener('change', applyFilters);
jobsElement('filterOrigin', HTMLSelectElement).addEventListener('change', applyOriginFilter);

// Initial load
fetchJobs();
fetchHistory();
state.pollTimer = setInterval(function() { fetchJobs(true); }, 2000);
setInterval(function() { fetchHistory(false, true); }, 10000);
