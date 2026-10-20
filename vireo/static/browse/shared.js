// Browse's boundary with shared code: the navbar and lightbox (ES modules
// whose bridge, navbar-globals.js, publishes what pages use as globals), the
// filter bar, the sync panel, and the classic helper scripts browse.html loads
// before the page (vireo-utils.js, vireo-dialog.js, vireo-folder-browser.js,
// vireo-export-*.js, vireo-batch-edits.js, vireo-browse-compare.js,
// vireo-panorama.js, vireo-release-originals.js, vireo-work-locally.js,
// tauri-bridge.js, help.js). Page modules import these names from here
// instead of reaching for globals, so this file is the one list of what the
// page uses from shared code. It reads the globals rather than importing the
// navbar's modules, so the page's type-check project stays its own.
//
// Functions are forwarders that look the global up at call time, as the
// classic page did, so one replaced at runtime (tests stub showToast and
// safeFetch) is still the one called. The controller objects and pure helpers
// are bound once: every script that defines them runs before the page's
// modules, and nothing replaces them.

/** @typedef {Window & typeof globalThis} SharedGlobals */

/**
 * A function that calls the global `name` when it is called.
 *
 * @template {keyof SharedGlobals} K
 * @param {K} name
 * @returns {NonNullable<SharedGlobals[K]>}
 */
function forward(name) {
  return /** @type {NonNullable<SharedGlobals[K]>} */ (/** @type {unknown} */ (function(/** @type {unknown[]} */ ...args) {
    return /** @type {Function} */ (/** @type {unknown} */ (window[name])).apply(window, args);
  }));
}

/**
 * Like forward(), for a global some page states do not define: a call while
 * it is missing does nothing.
 *
 * @template {keyof SharedGlobals} K
 * @param {K} name
 * @returns {NonNullable<SharedGlobals[K]>}
 */
function forwardIfPresent(name) {
  return /** @type {NonNullable<SharedGlobals[K]>} */ (/** @type {unknown} */ (function(/** @type {unknown[]} */ ...args) {
    var fn = /** @type {unknown} */ (window[name]);
    return typeof fn === 'function' ? fn.apply(window, args) : undefined;
  }));
}

// ---- vireo-utils.js, vireo-api.js, keymap.js, tauri-bridge.js ----
export const escapeHtml = window.escapeHtml;
export const escapeAttr = window.escapeAttr;
export const VireoViewPreferences = window.VireoViewPreferences;
export const VireoTime = window.VireoTime;
export const isTauri = forward('isTauri');
export const pickDirectory = forward('pickDirectory');

/** The `X-Vireo-Search-*` headers for a latest-wins read (services/search_lanes.py). @param {string} lane @param {number} sequence */
export function searchLaneHeaders(lane, sequence) {
  return window.Vireo?.api?.searchLaneHeaders(lane, sequence);
}

/**
 * Hand the page hooks the shared navbar and lightbox call to the page-hook
 * registry (vireo-page-hooks.js). `test_module_pages.py` holds the names
 * registered to the page's `pageHooks` in frontend-module-pages.json.
 *
 * @param {VireoPageHooks} hooks
 */
export function registerPageHooks(hooks) {
  window.Vireo.registerPageHooks(hooks);
}

// ---- the navbar and lightbox (navbar-globals.js) ----
export const safeFetch = forward('safeFetch');
export const safeEventSource = forward('safeEventSource');
export const preferredScrollBehavior = forward('preferredScrollBehavior');
export const matchesShortcut = forward('matchesShortcut');
export const openContextMenu = forward('openContextMenu');
export const coerceSelectionOnContext = forward('coerceSelectionOnContext');
export const buildOpenInEditorMenuItems = forward('buildOpenInEditorMenuItems');
export const buildSpeciesHighlightMenuItems = forward('buildSpeciesHighlightMenuItems');
export const buildSpeciesRepresentativeMenuItems = forward('buildSpeciesRepresentativeMenuItems');
export const showRevealFeedback = forward('showRevealFeedback');
export const developPhotos = forward('developPhotos');
export const findSimilar = forward('findSimilar');
export const openPipeline = forward('openPipeline');
export const doUndo = forward('doUndo');
export const doRedo = forward('doRedo');
export const closeCommandPalette = forward('closeCommandPalette');
export const closeReportModal = forward('closeReportModal');
export const openLightbox = forward('openLightbox');
export const closeLightbox = forward('closeLightbox');
export const lightboxNav = forward('lightboxNav');
export const requestLightboxFullscreen = forward('requestLightboxFullscreen');
export const exitLightboxFullscreen = forward('exitLightboxFullscreen');
export const showDeleteDialog = forward('showDeleteDialog');
export const submitToInatBatch = forward('submitToInatBatch');
export const _lbPhotoData = forward('_lbPhotoData');
export const vireoThumbnailUrl = forward('vireoThumbnailUrl');
export const vireoRememberPhotoEditRecipe = forward('vireoRememberPhotoEditRecipe');
export const vireoPhotoHasOrientationEdit = forward('vireoPhotoHasOrientationEdit');
export const vireoRememberPhotoPair = forward('vireoRememberPhotoPair');
export const vireoPhotoIsRawJpegPair = forward('vireoPhotoIsRawJpegPair');
export const vireoUpdatePairSourceControls = forward('vireoUpdatePairSourceControls');
export const vireoTogglePairSource = forward('vireoTogglePairSource');
export const _vireoPairSource = forward('_vireoPairSource');
export const _reconcileMissingFoldersInitSnapshot = forward('_reconcileMissingFoldersInitSnapshot');
export const markMissingFoldersReconciliationPending = forward('markMissingFoldersReconciliationPending');

/** @param {string} message @param {string=} kind */
export function showToast(message, kind) {
  if (typeof window.showToast === 'function') window.showToast(message, kind);
}

/** Which photos the native menu is pairing with RAW/JPEG companions. */
export function pairKnownByPhoto() {
  return window._vireoPairKnownByPhoto;
}

/** The Missing Originals snapshot version the navbar has applied. */
export function missingFoldersSnapshotVersion() {
  return window._missingFoldersSnapshotVersion;
}

/** Non-null while the lightbox is open (its Keymap Esc token). */
export function lightboxEscToken() {
  return window._lbEscToken;
}

/** The photo list the lightbox's arrows walk. */
export function lightboxPhotoList() {
  return window._lightboxPhotoList;
}

/** The lightbox's request/display session. */
export function lightboxSession() {
  return window.vireoLightboxSession;
}

/** The navbar's edit navigation (copied recipe, editor links). */
export function editNav() {
  return window.vireoEditNav;
}

/** The photo ids a native-menu command acts on instead of the selection. */
export function nativeMenuPhotoIdsOverride() {
  return window._vireoNativeMenuPhotoIdsOverride;
}

/** @param {number[] | null} ids */
export function setNativeMenuPhotoIdsOverride(ids) {
  window._vireoNativeMenuPhotoIdsOverride = ids;
}

/**
 * Publish the configured shortcuts to the navbar (its `?` sheet and
 * hotkeys read them).
 *
 * @param {Record<string, Record<string, string>>} shortcuts
 */
export function setKeyboardShortcuts(shortcuts) {
  window._vireoShortcuts = shortcuts;
}

// ---- help.js: defines closeHelpModal once its modal markup exists ----
export const closeHelpModal = forwardIfPresent('closeHelpModal');

// ---- the sync panel (_sync_panel.html) ----
export const checkPendingSync = forward('checkPendingSync');

// ---- the filter bar (_filterbar.html) and the other classic controllers ----
export const VireoFilter = window.VireoFilter;
export const VireoColorLabels = window.VireoColorLabels;
export const VireoDialog = window.VireoDialog;
export const VireoPanelLoader = window.VireoPanelLoader;
export const VireoFolderBrowser = window.VireoFolderBrowser;
export const VireoExportPresets = window.VireoExportPresets;
export const VireoExportCollisions = window.VireoExportCollisions;
export const VireoExportJob = window.VireoExportJob;
export const VireoBatchEdits = window.VireoBatchEdits;
export const VireoBrowseCompare = window.VireoBrowseCompare;
export const VireoReleaseOriginals = window.VireoReleaseOriginals;
export const openPanoramaModal = forward('openPanoramaModal');
export const closePanoramaModal = forward('closePanoramaModal');
export const startPanorama = forward('startPanorama');

/** The Reveal menu label for this platform ("Reveal in Finder", ...). */
export function revealLabel() {
  return window.VIREO_REVEAL_LABEL;
}

// ---- vireo-work-locally.js and vireo-pending-archives.js ----
/** Work Locally's per-folder state, once loaded. */
export function localFolderData() {
  return window.vireoLocalFolderData;
}

/** Work Locally's folder controller. */
export function localFolders() {
  return window.vireoLocalFolders;
}

/** The pending archive transfers, once loaded. */
export function pendingArchives() {
  return window.vireoPendingArchives;
}

// ---- Google Maps ----
/** The Maps API, once its script has loaded. */
export function googleMaps() {
  return window.google;
}

/**
 * The Maps script calls back a global by name (`&callback=_gmapsCallback`),
 * which keywords/link-place.js shares.
 *
 * @param {() => void} callback
 */
export function setGoogleMapsCallback(callback) {
  window._gmapsCallback = callback;
}
