// The Misses page's boundary with the shared navbar, lightbox, filter bar and
// vireo-utils.js, which are still classic scripts that publish globals. Page
// modules import these names from here instead of reaching for globals, so
// this file is the one list of what the page uses from shared code. When a
// shared script becomes a module, change its import here, not in every file.
//
// The forwarders look the global up at call time, as the classic page did, so
// a shared function that is absent on a page keeps its `typeof` guard here and
// one replaced at runtime (tests stub showToast and fetch) is still the one
// called. The escape helpers are pure and loaded first by _navbar.html, so
// they are bound once.

export const escapeHtml = window.escapeHtml;
export const escapeAttr = window.escapeAttr;

/** The shared filter bar from vireo-filter.js, which _filterbar.html loads. */
export const VireoFilter = /** @type {MissFilterController} */ (window.VireoFilter);

/** @param {string} message @param {string=} kind */
export function showToast(message, kind) {
  if (typeof window.showToast === 'function') window.showToast(message, kind);
}

/** @param {KeyboardEvent} event @param {string} shortcut */
export function matchesShortcut(event, shortcut) {
  return window.matchesShortcut(event, shortcut);
}

export function preferredScrollBehavior() {
  return window.preferredScrollBehavior();
}

/** The user's configured browse.flag/reject/unflag keys, if loaded. */
export function browseShortcuts() {
  return (window._vireoShortcuts && window._vireoShortcuts.browse) || {};
}

/** The navbar's thumbnail URL builder, or null where it is not loaded. */
export function sharedThumbnailUrl() {
  return window.vireoThumbnailUrl || null;
}

export function hasLightbox() {
  return typeof window.openLightbox === 'function';
}

/** @param {number} id @param {string} filename @param {{id:number;filename?:string;missCategory:MissCategory}[]} photos */
export function openLightbox(id, filename, photos) {
  // The wrap onclick handler in _navbar.html guards on !window._lightboxZoomHandled,
  // and the mouseup zoom-out path sets it to true. Without resetting here, a stale
  // true from a prior session would swallow the first click-to-zoom.
  window._lightboxZoomHandled = false;
  window.openLightbox(id, filename, photos);
}

export function requestLightboxFullscreen() {
  if (typeof window.requestLightboxFullscreen === 'function') window.requestLightboxFullscreen();
}

export function hasContextMenu() {
  return typeof window.openContextMenu === 'function';
}

/** @param {MouseEvent} event @param {MissMenuItem[]} items */
export function openContextMenu(event, items) {
  window.openContextMenu(event, items);
}

/** The Life List menu items from the shared lightbox, when it is loaded. @param {number} photoId */
export function speciesMenuItems(photoId) {
  return /** @type {MissMenuItem[]} */ ([]).concat(
    typeof window.buildSpeciesHighlightMenuItems === 'function'
      ? window.buildSpeciesHighlightMenuItems([photoId], { showFetchFallback: true })
      : []
  ).concat(
    typeof window.buildSpeciesRepresentativeMenuItems === 'function'
      ? window.buildSpeciesRepresentativeMenuItems([photoId], { showFetchFallback: true })
      : []
  );
}

export function hasDeleteDialog() {
  return typeof window.showDeleteDialog === 'function';
}

/** @param {number[]} ids @param {number} companions @param {(data:{failed_photo_ids?:number[]})=>void} callback */
export function showDeleteDialog(ids, companions, callback) {
  window.showDeleteDialog(ids, companions, callback);
}

/**
 * The hooks the shared navbar and lightbox call by name on whatever page is
 * open. They are the page's only globals; `test_module_pages.py` holds the
 * list to MODULE_PAGES["misses"].
 *
 * @param {{afterHistoryChange:()=>Promise<void>;loadMisses:()=>Promise<void>;getActiveSelection:()=>number[];openBrowseShortcutPhoto:(fullscreen:boolean)=>boolean;handleMissesLightboxFlagShortcut:(photoId:number,flag:string)=>boolean}} hooks
 */
export function exposePageHooks(hooks) {
  window.afterHistoryChange = hooks.afterHistoryChange;
  window.loadMisses = hooks.loadMisses;
  window.getActiveSelection = hooks.getActiveSelection;
  window.openBrowseShortcutPhoto = hooks.openBrowseShortcutPhoto;
  window.handleMissesLightboxFlagShortcut = hooks.handleMissesLightboxFlagShortcut;
}
