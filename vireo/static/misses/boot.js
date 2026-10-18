// Page startup: the module the template loads. Importing it loads the rest of
// the page's modules; this file then exposes the page hooks, binds every
// listener (including the controls that used to carry inline onclick
// attributes), and starts the filter bar, the config load and the first load.
// ES module.

import { state } from './state.js';
import { VireoFilter, exposePageHooks } from './shared.js';
import { getSinceParam } from './helpers.js';
import { applyMissFilters, cloneMissFilterPayload, missFilterPayload } from './filters.js';
import { applyLegacyMissScopes, noteLegacyScopeError } from './scope.js';
import {
  installMissSliderHandlers, loadMissConfig, recomputeMisses, resetMissThresholds,
} from './tuning.js';
import { afterHistoryChange, loadMisses } from './loading.js';
import {
  bindMissLightboxTracking, getActiveSelection, openBrowseShortcutPhoto, openSelectionInBrowse,
} from './photo-open.js';
import { bulkUnflagSelection, handleMissesLightboxFlagShortcut } from './flagging.js';
import { clearSelection } from './selection.js';
import { bindMissCardEvents } from './card-events.js';
import { bindMissContextMenu } from './context-menu.js';
import { deleteSelectedMisses } from './delete.js';
import { bindMissKeyboard } from './keyboard.js';
import { bindMissLifeListEvents, bindMissLightboxFlagEvents } from './badges.js';

// The shared navbar and lightbox call these by name. The classic page had
// them as globals from the moment its scripts parsed, so expose them first.
exposePageHooks({
  afterHistoryChange: afterHistoryChange,
  loadMisses: loadMisses,
  getActiveSelection: getActiveSelection,
  openBrowseShortcutPhoto: openBrowseShortcutPhoto,
  handleMissesLightboxFlagShortcut: handleMissesLightboxFlagShortcut,
});

/** @param {string} id @param {() => unknown} action */
function onButtonClick(id, action) {
  var button = document.getElementById(id);
  if (button) button.addEventListener('click', function() { action(); });
}

// The page controls that used to carry inline onclick attributes.
function bindMissPageButtons() {
  onButtonClick('missesSelectionBrowseBtn', openSelectionInBrowse);
  onButtonClick('missesSelectionUnflagBtn', bulkUnflagSelection);
  onButtonClick('missesSelectionDeleteBtn', deleteSelectedMisses);
  onButtonClick('missesSelectionClearBtn', clearSelection);
  onButtonClick('missRecomputeBtn', recomputeMisses);
  onButtonClick('missResetPreviewBtn', resetMissThresholds);
}

bindMissPageButtons();
bindMissCardEvents();

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
    ? 'Misses · Current pipeline run'
    : 'Misses · Detected misses',
  excludedValues: { flag: ['rejected'] },
  onChange: applyMissFilters,
}).then(function() {
  return applyLegacyMissScopes();
}).then(function() {
  // Seed the applied-expression snapshot before threshold controls become
  // enabled. An immediate Recompute therefore scopes to the expression that
  // produced the initial grid, including legacy deep links and handoffs.
  state.appliedMissFilters = cloneMissFilterPayload(missFilterPayload());
  state.initializingMissFilters = false;
  return loadMissConfig();
}).then(loadMisses).catch(function() {
  state.initializingMissFilters = false;
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
