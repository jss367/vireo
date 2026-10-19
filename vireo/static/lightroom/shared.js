// The Lightroom Import page's boundary with the shared navbar, vireo-utils.js
// and tauri-bridge.js. Page modules import these names from here instead of
// reaching for globals, so this file is the one list of what the page uses
// from shared code.
//
// The forwarders look the global up at call time, as the classic page did, so
// one replaced at runtime (tests stub safeFetch) is still the one called. The
// escape helper is pure and loaded first by _navbar.html, so it is bound once.

export const escapeHtml = window.escapeHtml;

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
 * @param {LightroomJobCallbacks} callbacks
 */
export function safeEventSource(url, callbacks) {
  return window.safeEventSource(url, callbacks);
}

/** Whether the page runs in the desktop app (tauri-bridge.js). */
export function isTauri() {
  return typeof window.isTauri === 'function' && window.isTauri();
}

/** @param {{title: string, filters: {name: string, extensions: string[]}[]}} options */
export function pickFile(options) {
  return window.pickFile(options);
}
