// The ID Conflicts page's boundary with the shared navbar, the lightbox and
// the shared classic scripts (vireo-utils.js, vireo-predictions.js,
// vireo-time.js). Page modules import these names from here instead of
// reaching for globals, so this file is the one list of what the page uses
// from shared code.
//
// The forwarders look the global up at call time, as the classic page did, so
// one replaced at runtime (tests stub showToast) is still the one called. The
// escape helpers and the view-preference and text-search helpers are loaded
// first by _navbar.html and nobody replaces them, so they are bound once.

export const escapeHtml = window.escapeHtml;
export const escapeAttr = window.escapeAttr;
export const VireoViewPreferences = window.VireoViewPreferences;
export const VireoTextSearch = window.VireoTextSearch;

/** @param {string} message @param {string=} kind */
export function showToast(message, kind) {
  if (typeof window.showToast === 'function') window.showToast(message, kind);
}

/** @param {number} id @param {string} filename @param {{id: number; filename: string}[]} photos */
export function openLightbox(id, filename, photos) {
  window.openLightbox(id, filename, photos);
}

/** A tracker for one batch of grouped decisions (vireo-predictions.js). */
export function groupedDecisionTracker() {
  return window.Vireo.predictions.groupedDecisionTracker();
}

/**
 * The navbar's thumbnail URL for a row, or the plain one where the navbar's
 * builder is not loaded.
 * @param {ComparePhoto} photo
 */
export function thumbnailUrl(photo) {
  return window.vireoThumbnailUrl ? window.vireoThumbnailUrl(photo) : '/thumbnails/' + photo.photo_id + '.jpg';
}

/** A capture time in the user's format (vireo-time.js). @param {string} timestamp */
export function formatCaptureDateTime(timestamp) {
  return window.VireoTime.captureDateTime(timestamp);
}
