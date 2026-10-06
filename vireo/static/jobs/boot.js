// Page startup.
// Classic page script; loads after every other Jobs page definition.

// Preserve the inline script's order: list clicks, the cleanup checkbox,
// detail pane clicks, then the initial fetches and pollers.
bindJobListClicks();
bindSourceCleanupConfirm();
bindDetailPaneActions();

// Initial load
fetchJobs();
fetchHistory();
pollTimer = setInterval(fetchJobs, 2000);
setInterval(fetchHistory, 10000);
