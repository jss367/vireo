// Page startup: registers the page hook, binds the template's controls, and
// starts the filter bar and the last-scan restore.
// Classic page script; loads after every other Duplicates page script.

// The native menu's Find Duplicates command (vireo-page-hooks.js).
Vireo.registerPageHooks({startScan: startScan});

/** @param {string} selector @param {() => unknown} action */
function onDuplicatesClick(selector, action) {
  var el = /** @type {HTMLElement} */ (document.querySelector(selector));
  el.addEventListener('click', function() { action(); });
}

// The controls that used to carry inline onclick attributes.
onDuplicatesClick('#scanBtn', startScan);
onDuplicatesClick('#restoreRetryBtn', function() { return tryRestoreLastScan(); });
onDuplicatesClick('#catalogRefreshBtn', function() { return tryRestoreLastScan(true); });
onDuplicatesClick('#applyBtn', applyRejections);
onDuplicatesClick('#clearResultsBtn', clearResults);

document.addEventListener('DOMContentLoaded', () => VireoFilter.init({
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
  if (_filterReadyResolve) { _filterReadyResolve(); _filterReadyResolve = null; }
}));
document.addEventListener('DOMContentLoaded', function(event) { tryRestoreLastScan(event); });
