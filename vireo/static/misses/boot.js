// Page startup.
// Classic page script; loads after every other Misses page definition.

// Preserve the inline script's order: the listeners it attached at load, then
// the slider handlers, the filter bar, the config and the first load.
bindMissLightboxTracking();
bindMissContextMenu();
bindMissKeyboard();
bindMissLifeListEvents();
bindMissLightboxFlagEvents();

/* ---------- Boot ---------- */
installMissSliderHandlers();
var missSince = getSinceParam();
VireoFilter.init({
  page: 'misses',
  root: /** @type {HTMLElement} */ (document.getElementById('vireoFilterBar')),
  scopeLabel: missSince
    ? 'Misses \u00b7 Current pipeline run'
    : 'Misses \u00b7 Detected misses',
  excludedValues: { flag: ['rejected'] },
  onChange: applyMissFilters,
}).then(function() {
  return applyLegacyMissScopes();
}).then(function() {
  // Seed the applied-expression snapshot before threshold controls become
  // enabled. An immediate Recompute therefore scopes to the expression that
  // produced the initial grid, including legacy deep links and handoffs.
  appliedMissFilters = cloneMissFilterPayload(missFilterPayload());
  initializingMissFilters = false;
  return loadMissConfig();
}).then(loadMisses).catch(function() {
  initializingMissFilters = false;
  var current = new URL(window.location.href).searchParams;
  if (current.has('collection_id') || current.has('folder_id') ||
      current.has('filters')) {
    // If the shared registry/bootstrap fails, applyLegacyMissScopes() never
    // gets a chance to translate the legacy URL into universal rules. Keep
    // the page closed instead of loading the default empty expression, which
    // would expose every miss to bulk actions outside the requested scope.
    noteLegacyScopeError(
      'Could not initialize the requested filters. Showing no misses so ' +
      'bulk actions stay within the requested scope.'
    );
  }
  loadMissConfig().then(loadMisses);
});
