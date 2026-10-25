// The Duplicates page's boundary with the shared navbar, the filter bar and
// vireo-utils.js. The navbar is a set of modules whose bridge
// (navbar-globals.js) publishes the functions pages use as globals; the
// others are classic scripts. Page modules import these names from here
// instead of reaching for globals, so this file is the one list of what the
// page uses from shared code.
//
// The forwarders look the global up at call time, as the classic page did, so
// one replaced at runtime (tests stub showToast and safeFetch) is still the
// one called. The escape and byte helpers are pure and loaded first by
// _navbar.html, and VireoFilter is loaded by _filterbar.html before the page,
// so they are bound once.

export const escapeHtml = window.escapeHtml;
export const formatBytes = window.formatBytes;

/** The shared filter bar from vireo-filter.js, which _filterbar.html loads. */
export const VireoFilter = window.VireoFilter;

/**
 * @template T
 * @param {string} url @param {RequestInit=} init @param {{toast?: boolean}=} options
 * @returns {Promise<T>}
 */
export function safeFetch(url, init, options) {
  return window.safeFetch(url, init, options);
}

/**
 * @param {string} url
 * @param {{onProgress?(progress: DuplicateScanProgress): void; onComplete?(event: DuplicateScanComplete): void; onError?(): void}} callbacks
 */
export function safeEventSource(url, callbacks) {
  return window.safeEventSource(url, callbacks);
}

/** @param {string} message @param {string=} kind */
export function showToast(message, kind) {
  if (typeof window.showToast === 'function') window.showToast(message, kind);
}

/** @param {unknown} data */
export function showRevealFeedback(data) {
  window.showRevealFeedback(data);
}

/** @param {MouseEvent} event @param {Array<{label: string; onClick: () => void}>} items */
export function openContextMenu(event, items) {
  window.openContextMenu(event, items);
}

/**
 * The navbar's thumbnail URL for a photo, or the plain one where the navbar's
 * builder is not loaded.
 * @param {DuplicatePhoto} photo
 */
export function thumbnailUrl(photo) {
  return window.vireoThumbnailUrl ? window.vireoThumbnailUrl(photo) : '/thumbnails/' + photo.id + '.jpg';
}

/** "Reveal in Finder" (or the platform's wording), from /config-defaults.js. */
export function revealLabel() {
  return window.VIREO_REVEAL_LABEL;
}

/** The platform's file manager name, from /config-defaults.js. */
export function fileManagerName() {
  return window.VIREO_FILE_MANAGER_NAME || 'file manager';
}

/** The user's date-time format (vireo-time.js). @param {Date} date */
export function formatDateTime(date) {
  return window.VireoTime.dateTime(date);
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
