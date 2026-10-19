// The Highlights page's boundary with the shared navbar, lightbox and
// vireo-utils.js. Page modules import these names from here instead of
// reaching for globals, so this file is the one list of what the page uses
// from shared code.
//
// The forwarders look the global up at call time, as the classic page did, so
// a shared function that is absent keeps its guard here and one replaced at
// runtime (tests stub showToast and safeFetch) is still the one called. Live
// lightbox state (the open photo, its navigation list) is read through
// getters for the same reason. The escape helper, VireoTextSearch and
// VireoViewPreferences come from vireo-utils.js, which _navbar.html loads
// first and nobody replaces, so they are bound once.

export const escapeAttr = window.escapeAttr;
export const VireoTextSearch = window.VireoTextSearch;
export const VireoViewPreferences = window.VireoViewPreferences;

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

export function hasLightbox() {
  return typeof window.openLightbox === 'function';
}

/** @param {number} id @param {string} filename @param {HighlightsLightboxPhoto[]} photos */
export function openLightbox(id, filename, photos) {
  window.openLightbox(id, filename, photos);
}

export function closeLightbox() {
  window.closeLightbox();
}

/** The photo the open lightbox shows, or null when no lightbox is loaded. */
export function lightboxRequestedPhotoId() {
  return window.vireoLightboxSession ? window.vireoLightboxSession.requestedPhotoId() : null;
}

/** The list the open lightbox navigates. */
export function lightboxPhotoList() {
  return window._lightboxPhotoList;
}

/** The navbar's thumbnail URL builder, or null where it is not loaded. */
export function sharedThumbnailUrl() {
  return window.vireoThumbnailUrl || null;
}

export function isAdvancedMode() {
  return typeof window.isAdvancedMode === 'function' && window.isAdvancedMode();
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
