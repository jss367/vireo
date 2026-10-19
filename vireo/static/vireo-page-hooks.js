/* Page hooks: how shared code calls into the page that is open.
 *
 * The navbar and lightbox are on every page, but some of their commands act on
 * page state: the native menu's Rate/Flag/Delete need the page's selection, the
 * undo/redo transport reloads the page after a change, the lightbox writes a
 * flag through the page's own model. Shared controllers a few pages load (the
 * export modal's presets and collision notice) likewise call back into the
 * page. A page hands those functions over by registering them here:
 *
 *   Vireo.registerPageHooks({getActiveSelection: getActiveSelection, ...});
 *
 * and shared code asks for one by name, getting null when the open page did not
 * register it:
 *
 *   var afterHistoryChange = Vireo.pageHook('afterHistoryChange');
 *   if (afterHistoryChange) await afterHistoryChange(data);
 *
 * Shared code never probes page globals by name (`typeof getActiveSelection`),
 * so a page's functions need not be globals and an unrelated global with a
 * matching name is never called. Classic pages call Vireo.registerPageHooks
 * from their boot script; module pages call it from their shared.js.
 *
 * This file is a classic script loaded right after vireo-utils.js, before any
 * page script, so classic pages can register while they parse. The hook names
 * are fixed: registering or asking for a name not in PAGE_HOOK_NAMES throws,
 * so a typo fails loudly instead of leaving a command silently unwired.
 * Registering a name again replaces the earlier function.
 */
(function(global) {
  'use strict';

  var Vireo = global.Vireo = global.Vireo || /** @type {VireoNamespace} */ ({});

  /**
   * Every hook a page may register, grouped by the shared code that calls it.
   * types/page-hooks.d.ts gives each one's signature and what it means.
   * @type {readonly VireoPageHookName[]}
   */
  var PAGE_HOOK_NAMES = Object.freeze(/** @type {VireoPageHookName[]} */ ([
    // What the native menu and lightbox act on.
    'getActiveSelection',
    'getGroupModalSelection',
    'getSelectedPhotoId',
    'getPhotos',
    'getDetailPhotoId',
    'getPredictions',
    // Native menu photo commands.
    'openBrowseShortcutPhoto',
    'openPipelineLightbox',
    'moveBrowseSelection',
    'batchSetRating',
    'batchSetFlag',
    'setRatingFor',
    'setReviewRating',
    'setFlagFor',
    'setReviewFlag',
    'setColorLabelFor',
    'setWildlifeExcludedFor',
    'openExportModal',
    'openPhotoExportModal',
    'revealPhoto',
    'revealReviewPhoto',
    'copyPhotoPaths',
    'openBrowseCompare',
    'batchAddKeyword',
    'addToCollection',
    'openCaptureTimeModal',
    'batchDelete',
    'acceptPrediction',
    'rejectPrediction',
    'acceptAllPending',
    // Native menu tool commands.
    'startPipeline',
    'startScan',
    'runSync',
    'verifyAllModels',
    // Lightbox integration.
    'getLightboxOpenOptions',
    'getLightboxBrowseDisabledHint',
    'getPhotoExternalEditDisabledHint',
    'removeDeletedLightboxPhoto',
    'handleMissesLightboxFlagShortcut',
    'loadHighlights',
    'updateHighlightsLightboxControls',
    // Undo/redo transport.
    'beforeHistoryChange',
    'afterHistoryChange',
    'changeLocalHistory',
    'localHistoryStatus',
    // Missing Originals deletes.
    'loadMisses',
    // Work Locally (vireo-work-locally.js) after a local-folder change.
    'loadWsFolders',
    // The export modal's shared controllers (vireo-export-presets.js,
    // vireo-export-collisions.js), on Browse and the Photo Editor.
    'applyExportPreset',
    'markExportCustom',
    'updateExportFormatControls',
    'updateExportPreview',
    'selectedExportMetadataFields',
    'buildExportPreflightRequest',
  ]));

  /** @type {Record<string, boolean>} */
  var known = Object.create(null);
  PAGE_HOOK_NAMES.forEach(function(name) { known[name] = true; });
  /** @type {Record<string, Function>} */
  var registered = Object.create(null);

  /** @param {string} name */
  function checkName(name) {
    if (!known[name]) throw new Error('Unknown page hook: ' + name);
  }

  /** @param {VireoPageHooks} hooks */
  function registerPageHooks(hooks) {
    Object.keys(hooks).forEach(function(name) {
      checkName(name);
      var hook = /** @type {Record<string, unknown>} */ (hooks)[name];
      if (typeof hook !== 'function') {
        throw new TypeError('Page hook ' + name + ' must be a function');
      }
      registered[name] = hook;
    });
  }

  /**
   * @template {VireoPageHookName} K
   * @param {K} name
   * @returns {NonNullable<VireoPageHooks[K]> | null}
   */
  function pageHook(name) {
    checkName(name);
    return /** @type {NonNullable<VireoPageHooks[K]> | undefined} */ (registered[name]) || null;
  }

  Vireo.registerPageHooks = registerPageHooks;
  Vireo.pageHook = pageHook;
  Vireo.pageHookNames = PAGE_HOOK_NAMES;
})(window);
