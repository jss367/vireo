// The navbar's classic bridge: the one module of the navbar and lightbox
// that writes to `window`. The navbar and lightbox are ES modules and share
// nothing through globals, but classic pages, inline handlers in page
// templates, and the remaining classic shared scripts (tauri-bridge.js,
// vireo-color-labels.js, vireo-page-find.js) still call some of their
// functions by global name. This module publishes exactly those names, and
// nothing else: a name a page stops using comes off this list.
//
// Module pages do not use these globals. They import the navbar's modules
// directly (see vireo/static/misses/shared.js).
//
// navbar-boot.js imports this module last, so every name below is defined by
// the time it is published. Classic page scripts are deferred behind
// navbar-boot.js (see _navbar.html), so they see these globals while they run,
// as they did when the navbar was classic. The page-hook registry
// (vireo-page-hooks.js) is a separate classic script because pages register
// with it while they parse.

import { navbarState } from './navbar-state.js';
import { closeCommandPalette, vireoSetNumberedNavigationMode } from './navbar-command-palette-open.js';
import { vireoEditNav } from './navbar-edit-nav.js';
import {
  closeContextMenu, closeReportModal, coerceSelectionOnContext, openContextMenu, openInBrowse,
} from './navbar-report-context-menu.js';
import { vireoRefreshNavigationHrefs } from './navbar-tabs.js';
import { vireoWorkspaceSwitcher } from './navbar-workspace-switcher-init.js';
import { lbState } from './lightbox/shared-state.js';
import { setLightboxReadOnlyMode, vireoLightboxSession } from './lightbox/state.js';
import { buildSpeciesHighlightMenuItems, buildSpeciesRepresentativeMenuItems } from './lightbox/curation.js';
import {
  _vireoPairKnownByPhoto, _vireoPairSource, vireoPhotoHasOrientationEdit, vireoPhotoIsRawJpegPair,
  vireoPreviewUrl, vireoRememberPhotoEditRecipe, vireoRememberPhotoPair, vireoThumbnailUrl,
  vireoTogglePairSource, vireoUpdatePairSourceControls,
} from './lightbox/edits.js';
import { clearLightboxProvisionalFlags, setLightboxProvisionalFlag } from './lightbox/flags.js';
import { _lbPhotoData, _lbRecipeHasGeometricEdit } from './lightbox/overlays.js';
import {
  exitLightboxFullscreen, lightboxNav, openLightbox, requestLightboxFullscreen,
  vireoRefreshEditRecipeCache,
} from './lightbox/navigation.js';
import { closeLightbox } from './lightbox/controls.js';
import { showDeleteDialog } from './lightbox/delete.js';
import { submitToInat, submitToInatBatch } from './lightbox/inat.js';
import { formatShortcut, matchesShortcut, parseShortcut } from './lightbox/keyboard.js';
import { openPipeline } from './navbar-pipeline-inspector.js';
import { findSimilar } from './navbar-similar-photos.js';
import {
  buildOpenInEditorMenuItems, developPhotos, doRedo, doUndo, formatDuration, formatJobType,
  getThemeList, handleNativeMenuCommand, invalidateEditorsCache, isAdvancedMode,
  nativeMenuActivePhotoIds, openExternalLink, preferredScrollBehavior, renderHistoryControls,
  safeEventSource, safeFetch, setTheme, showRevealFeedback, showToast, vireoHistoryBusy,
} from './navbar-shared-helpers.js';
import {
  _reconcileMissingFoldersInitSnapshot, markMissingFoldersReconciliationPending, missingFolders,
} from './navbar-folder-banners.js';

/* ---------- Helpers every page uses ---------- */
Object.assign(window, {
  safeFetch: safeFetch,
  safeEventSource: safeEventSource,
  showToast: showToast,
  preferredScrollBehavior: preferredScrollBehavior,
  formatJobType: formatJobType,
  formatDuration: formatDuration,
  isAdvancedMode: isAdvancedMode,
  getThemeList: getThemeList,
  setTheme: setTheme,
  openExternalLink: openExternalLink,
  showRevealFeedback: showRevealFeedback,
  developPhotos: developPhotos,
  invalidateEditorsCache: invalidateEditorsCache,
  buildOpenInEditorMenuItems: buildOpenInEditorMenuItems,
});

/* ---------- Navigation, menus and the native menu ---------- */
Object.assign(window, {
  handleNativeMenuCommand: handleNativeMenuCommand,
  nativeMenuActivePhotoIds: nativeMenuActivePhotoIds,
  closeCommandPalette: closeCommandPalette,
  vireoSetNumberedNavigationMode: vireoSetNumberedNavigationMode,
  vireoRefreshNavigationHrefs: vireoRefreshNavigationHrefs,
  vireoWorkspaceSwitcher: vireoWorkspaceSwitcher,
  vireoEditNav: vireoEditNav,
  openContextMenu: openContextMenu,
  closeContextMenu: closeContextMenu,
  coerceSelectionOnContext: coerceSelectionOnContext,
  closeReportModal: closeReportModal,
  openInBrowse: openInBrowse,
  findSimilar: findSimilar,
  openPipeline: openPipeline,
});

/* ---------- Undo/redo ---------- */
Object.assign(window, {
  doUndo: doUndo,
  doRedo: doRedo,
  vireoHistoryBusy: vireoHistoryBusy,
  renderHistoryControls: renderHistoryControls,
});

/* ---------- Keyboard shortcuts ---------- */
Object.assign(window, {
  matchesShortcut: matchesShortcut,
  parseShortcut: parseShortcut,
  formatShortcut: formatShortcut,
});

/* ---------- Lightbox ---------- */
Object.assign(window, {
  openLightbox: openLightbox,
  closeLightbox: closeLightbox,
  lightboxNav: lightboxNav,
  requestLightboxFullscreen: requestLightboxFullscreen,
  exitLightboxFullscreen: exitLightboxFullscreen,
  setLightboxReadOnlyMode: setLightboxReadOnlyMode,
  setLightboxProvisionalFlag: setLightboxProvisionalFlag,
  clearLightboxProvisionalFlags: clearLightboxProvisionalFlags,
  showDeleteDialog: showDeleteDialog,
  submitToInat: submitToInat,
  submitToInatBatch: submitToInatBatch,
  buildSpeciesHighlightMenuItems: buildSpeciesHighlightMenuItems,
  buildSpeciesRepresentativeMenuItems: buildSpeciesRepresentativeMenuItems,
  _lbPhotoData: _lbPhotoData,
  _lbRecipeHasGeometricEdit: _lbRecipeHasGeometricEdit,
});

/* ---------- Photo renders and RAW+JPEG pairs ---------- */
Object.assign(window, {
  vireoThumbnailUrl: vireoThumbnailUrl,
  vireoPreviewUrl: vireoPreviewUrl,
  vireoRefreshEditRecipeCache: vireoRefreshEditRecipeCache,
  vireoRememberPhotoEditRecipe: vireoRememberPhotoEditRecipe,
  vireoPhotoHasOrientationEdit: vireoPhotoHasOrientationEdit,
  vireoRememberPhotoPair: vireoRememberPhotoPair,
  vireoPhotoIsRawJpegPair: vireoPhotoIsRawJpegPair,
  vireoUpdatePairSourceControls: vireoUpdatePairSourceControls,
  vireoTogglePairSource: vireoTogglePairSource,
  _vireoPairSource: _vireoPairSource,
  _vireoPairKnownByPhoto: _vireoPairKnownByPhoto,
});

/* ---------- Missing-folder banner state Browse reconciles with ---------- */
Object.assign(window, {
  _reconcileMissingFoldersInitSnapshot: _reconcileMissingFoldersInitSnapshot,
  markMissingFoldersReconciliationPending: markMissingFoldersReconciliationPending,
});

/* ---------- Live state, as read-only accessors ---------- */
/** @param {string} name @param {() => unknown} get */
function publishReadOnly(name, get) {
  Object.defineProperty(window, name, {get: get, configurable: true, enumerable: true});
}
// The lightbox's session (pages ask it which photo is open), the list the open
// lightbox navigates (Browse, Highlights and the sync panel replace or compare
// it), whether the lightbox is open, and the banner's snapshot version Browse
// compares against its own init snapshot.
publishReadOnly('vireoLightboxSession', function() { return vireoLightboxSession; });
publishReadOnly('_lightboxPhotoList', function() { return lbState.photoList; });
publishReadOnly('_lbEscToken', function() { return lbState.escToken; });
publishReadOnly('_missingFoldersSnapshotVersion', function() { return missingFolders.snapshotVersion; });

/* ---------- Shared state pages also write ---------- */
/**
 * @template {keyof typeof navbarState} K
 * @param {string} name @param {K} key
 */
function publishReadWrite(name, key) {
  // A classic script that ran before this module may already have set it.
  var early = /** @type {Record<string, unknown>} */ (/** @type {unknown} */ (window))[name];
  if (early !== undefined) navbarState[key] = /** @type {typeof navbarState[K]} */ (early);
  Object.defineProperty(window, name, {
    get: function() { return navbarState[key]; },
    set: function(value) { navbarState[key] = value; },
    configurable: true,
    enumerable: true,
  });
}
publishReadWrite('_vireoShortcuts', 'shortcuts');
publishReadWrite('_vireoNativeMenuPhotoIdsOverride', 'nativeMenuPhotoIdsOverride');
