// The Life List page's boundary with the shared navbar, the lightbox and the
// shared classic scripts (vireo-utils.js, vireo-folder-browser.js,
// tauri-bridge.js). Page modules import these names from here instead of
// reaching for globals, so this file is the one list of what the page uses
// from shared code.
//
// The forwarders look the global up at call time, as the classic page did, so
// a shared function that is absent keeps its `typeof` guard here and one
// replaced at runtime (tests stub showToast and safeFetch) is still the one
// called. The escape, view-preference and text-search helpers are loaded first
// by _navbar.html and nobody replaces them, so they are bound once.

export const escapeHtml = window.escapeHtml;
export const escapeAttr = window.escapeAttr;
export const VireoViewPreferences = window.VireoViewPreferences;
export const VireoTextSearch = window.VireoTextSearch;

/**
 * @template T
 * @param {string} url @param {RequestInit=} init @param {{toast?: boolean}=} options
 * @returns {Promise<T>}
 */
export function safeFetch(url, init, options) {
  return window.safeFetch(url, init, options);
}

/** @param {string} message @param {string=} kind */
export function showToast(message, kind) {
  if (typeof window.showToast === 'function') window.showToast(message, kind);
}

export function hasEventSource() {
  return typeof window.safeEventSource === 'function';
}

/** @param {string} url @param {{onComplete: () => void}} callbacks */
export function safeEventSource(url, callbacks) {
  return /** @type {NonNullable<Window['safeEventSource']>} */ (window.safeEventSource)(url, callbacks);
}

export function hasLightbox() {
  return typeof window.openLightbox === 'function';
}

/** @param {number} id @param {string} filename @param {LifePhoto[]} photos */
export function openLightbox(id, filename, photos) {
  /** @type {NonNullable<Window['openLightbox']>} */ (window.openLightbox)(id, filename, photos);
}

/** The photo the shared lightbox is showing or loading. */
export function lightboxRequestedPhotoId() {
  return window.vireoLightboxSession.requestedPhotoId();
}

/**
 * The navbar's thumbnail URL for a photo, or the plain one where the navbar's
 * builder is not loaded.
 * @param {LifePhoto} photo
 */
export function thumbnailUrl(photo) {
  return window.vireoThumbnailUrl ? window.vireoThumbnailUrl(photo) : '/thumbnails/' + photo.id + '.jpg';
}

/** The desktop app's native folder picker (tauri-bridge.js), where there is one. */
export function hasPickDirectory() {
  return typeof window.pickDirectory === 'function';
}

/** @param {string} title @param {{defaultPath?: string}} options */
export function pickDirectory(title, options) {
  return /** @type {NonNullable<Window['pickDirectory']>} */ (window.pickDirectory)(title, options);
}

/** Whether the page runs in the desktop app (tauri-bridge.js). */
export function isTauriApp() {
  return typeof window.isTauri === 'function' && window.isTauri();
}

/**
 * The shared folder browser (vireo-folder-browser.js).
 * @param {ConstructorParameters<typeof VireoFolderBrowser>[0]} options
 */
export function createFolderBrowser(options) {
  return new window.VireoFolderBrowser(options);
}

/**
 * Hand the page hooks the shared navbar calls to the page-hook registry
 * (vireo-page-hooks.js). The page writes no globals; `test_module_pages.py`
 * holds the names registered to the page's `pageHooks` in
 * frontend-module-pages.json.
 *
 * @param {VireoPageHooks} hooks
 */
export function registerPageHooks(hooks) {
  window.Vireo.registerPageHooks(hooks);
}
