// The Storage page's boundary with the shared navbar and the shared classic
// scripts (vireo-release-originals.js, vireo-time.js). Page modules import
// these names from here instead of reaching for globals, so this file is the
// one list of what the page uses from shared code.
//
// The forwarders look the global up at call time, as the classic page did, so
// one replaced at runtime (tests stub showToast and safeFetch) is still the
// one called.

/**
 * @template [T=unknown]
 * @param {string} url @param {RequestInit=} init @param {{toast?: boolean}=} options
 * @returns {Promise<T>}
 */
export function safeFetch(url, init, options) {
  return window.safeFetch(url, init, options);
}

/** @param {string} url @param {{onComplete?(): void}} callbacks */
export function safeEventSource(url, callbacks) {
  return window.safeEventSource(url, callbacks);
}

/** @param {string} message @param {string=} kind */
export function showToast(message, kind) {
  if (typeof window.showToast === 'function') window.showToast(message, kind);
}

/** Releasing local originals (vireo-release-originals.js, loaded before the page). */
export const VireoReleaseOriginals = {
  storageButton: function() { return window.VireoReleaseOriginals.storageButton(); },
};

/** The user's date-time format (vireo-time.js). @param {Date} date */
export function formatDateTime(date) {
  return window.VireoTime.dateTime(date);
}

/** The user's time format (vireo-time.js). @param {Date} date @param {Intl.DateTimeFormatOptions=} options */
export function formatTime(date, options) {
  return window.VireoTime.time(date, options);
}

/**
 * Hand the page hooks shared scripts call to the page-hook registry
 * (vireo-page-hooks.js). The page writes no globals; `test_module_pages.py`
 * holds the names registered to the page's `pageHooks` in
 * frontend-module-pages.json.
 *
 * @param {VireoPageHooks} hooks
 */
export function registerPageHooks(hooks) {
  window.Vireo.registerPageHooks(hooks);
}
