// Page startup: the module the template loads. Registers the page hook,
// binds the template's controls and the delegated listeners for the markup
// the page renders, and starts the filter bar and the last-scan restore.

import { duplicatesEl, state } from './state.js';
import { bindDuplicateCardContextMenu } from './card.js';
import { bindDuplicateEvents } from './events.js';
import { refreshFilterMatches } from './filter.js';
import { applyRejections } from './patterns.js';
import { tryRestoreLastScan } from './restore.js';
import { clearResults } from './results.js';
import { startScan } from './scan.js';
import { registerPageHooks, VireoFilter } from './shared.js';

// The native menu's Find Duplicates command (vireo-page-hooks.js).
registerPageHooks({startScan: startScan});

/** @param {string} id @param {() => unknown} action */
function onClick(id, action) {
  duplicatesEl(id).addEventListener('click', function() { action(); });
}

// The template's controls.
onClick('scanBtn', startScan);
onClick('restoreRetryBtn', function() { return tryRestoreLastScan(); });
onClick('catalogRefreshBtn', function() { return tryRestoreLastScan(true); });
onClick('applyBtn', applyRejections);
onClick('clearResultsBtn', clearResults);
bindDuplicateEvents();
bindDuplicateCardContextMenu();
duplicatesEl('sameShotCompare').addEventListener('close', function(event) {
  if (!(/** @type {HTMLDialogElement} */ (event.target)).open) state.sameShotComparison = null;
});

// Module scripts run before DOMContentLoaded, where the classic page started
// the filter bar and the restore; keep that order.
document.addEventListener('DOMContentLoaded', function() {
  VireoFilter.init({
    page: 'duplicates',
    root: document.getElementById('vireoFilterBar'),
    scopeLabel: 'Duplicates · Grouped matches',
    onChange: refreshFilterMatches,
  }).then(refreshFilterMatches).catch(function() {
    // VireoFilter.init failing means no filter chips will ever load, so
    // there are no filters for the render to respect. Release
    // ``tryRestoreLastScan`` immediately — otherwise a cached last scan
    // would hang unrendered forever waiting for a filter query that
    // never runs.
    if (state.filterReadyResolve) { state.filterReadyResolve(); state.filterReadyResolve = null; }
  });
});
document.addEventListener('DOMContentLoaded', function(event) { tryRestoreLastScan(event); });
