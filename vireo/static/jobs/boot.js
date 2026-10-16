// Page startup.
// Classic page script; loads after every other Jobs page definition.

// Preserve the inline script's order: list clicks, the cleanup checkbox,
// detail pane clicks, then the initial fetches and pollers.
bindJobListClicks();
bindSourceCleanupConfirm();
bindDetailPaneActions();
document.getElementById('retryJobsPolling').addEventListener('click', retryJobsPolling);

// Initial load
fetchJobs();
fetchHistory();
pollTimer = setInterval(function() { fetchJobs(true); }, 2000);
setInterval(function() { fetchHistory(false, true); }, 10000);
