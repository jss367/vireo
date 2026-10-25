// The Keywords page's boundary with the shared navbar, vireo-utils.js,
// keywords-link-place-url.js and the third-party map scripts (Leaflet, Google
// Maps). Page modules import these names from here instead of reaching for
// globals, so this file is the one list of what the page uses from shared
// code.
//
// The forwarders look the global up at call time, as the classic page did, so
// one replaced at runtime (tests stub showToast) is still the one called. The
// escape and text-search helpers are loaded first by _navbar.html and nobody
// replaces them, so they are bound once.

export const escapeHtml = window.escapeHtml;
export const escapeAttr = window.escapeAttr;
export const VireoTextSearch = window.VireoTextSearch;

/** @param {string} message @param {string=} type */
export function showToast(message, type) {
  if (typeof window.showToast === 'function') window.showToast(message, type);
}

/** Open a link in the system browser (the navbar's bridge). @param {Event} event @param {string} url */
export function openExternalLink(event, url) {
  return window.openExternalLink(event, url);
}

/** @param {MouseEvent} event @param {KeywordMenuItem[]} items */
export function openContextMenu(event, items) {
  window.openContextMenu(event, items);
}

/** Finder-style selection on right-click. @param {Set<number>} ids @param {number} id */
export function coerceSelectionOnContext(ids, id) {
  return window.coerceSelectionOnContext(ids, id);
}

/**
 * The ?link_place= deep link (keywords-link-place-url.js).
 * @param {KeywordRecord[]} keywords @param {(id: number, name: string) => Promise<void>} open
 */
export function openLinkPlaceFromUrl(keywords, open) {
  window.openLinkPlaceFromUrl(keywords, open);
}

/** Leaflet, once its script has loaded (merge-map.js loads it on demand). */
export function leaflet() {
  return window.L;
}

/** The Google Maps API, once its script has loaded. */
export function googleMaps() {
  return window.google;
}

/**
 * The Maps script calls back a global by name (`&callback=_gmapsCallback`);
 * Browse's shared.js sets the same name for its own loader.
 *
 * @param {() => void} callback
 */
export function setGoogleMapsCallback(callback) {
  window._gmapsCallback = callback;
}
