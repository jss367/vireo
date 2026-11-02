// Page startup: the module the template loads. Binds the toolbar controls,
// loads the recent log lines, and connects the live log stream.
// ES module. Module scripts run in document order after the navbar's entry,
// so safeFetch and showToast are defined by the time this runs.

import { VireoTextSearch, safeFetch, showToast } from './shared.js';
import { addLogLine, applyFilter, clearLogs } from './lines.js';

/** @param {string} id */
function byId(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
}

/* ---------- Toolbar ---------- */
byId('levelFilter').addEventListener('change', applyFilter);
byId('searchFilter').addEventListener('input', applyFilter);
byId('logSearchMatchCaseBtn').addEventListener('click', function() {
  VireoTextSearch.toggle('logSearch', 'match_case', applyFilter);
});
byId('logSearchWholeWordBtn').addEventListener('click', function() {
  VireoTextSearch.toggle('logSearch', 'whole_word', applyFilter);
});
byId('logClearBtn').addEventListener('click', clearLogs);

/* ---------- Recent lines, then the live stream ---------- */
/** @type {Promise<LogRecord[]>} */ (safeFetch('/api/logs/recent?count=500', {}, { toast: false }))
  .then(function(logs) {
    logs.forEach(function(l) { addLogLine(l); });
    applyFilter();
  })
  .catch(function() {});

var source = new EventSource('/api/logs/stream');
source.addEventListener('log', function(e) {
  try {
    var record = /** @type {LogRecord} */ (JSON.parse(/** @type {MessageEvent<string>} */ (e).data));
    addLogLine(record);
    applyFilter();
  } catch(ex) { /* ignore malformed SSE data */ }
});
source.addEventListener('error', function() {
  source.close();
  showToast('Log stream disconnected', 'error');
});
/* Clean up SSE on page unload */
window.addEventListener('beforeunload', function() { source.close(); });
