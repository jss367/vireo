// The Best Batch page's boundary with the shared navbar, lightbox and
// vireo-utils.js. Page modules import these names from here instead of
// reaching for globals, so this file is the one list of what the page uses
// from shared code.
//
// The forwarders look the global up at call time, as the classic page did, so
// a shared function that is absent keeps its guard here and one replaced at
// runtime (tests stub showToast) is still the one called. The escape helpers
// are pure and loaded first by _navbar.html, so they are bound once.

export const escapeHtml = window.escapeHtml;
export const escapeAttr = window.escapeAttr;

/** @param {string} message @param {string=} kind */
export function showToast(message, kind) {
  if (typeof window.showToast === 'function') window.showToast(message, kind);
}

/** The lightbox's preview URL for a photo at a long-edge size. @param {number} photoId @param {number} size */
export function vireoPreviewUrl(photoId, size) {
  return window.vireoPreviewUrl(photoId, size);
}

/** @param {BestBatchCard | number} photoOrId */
export function vireoThumbnailUrl(photoOrId) {
  var photoId = photoOrId && typeof photoOrId === 'object' ? photoOrId.id : photoOrId;
  return typeof window.vireoThumbnailUrl === 'function'
    ? window.vireoThumbnailUrl(photoOrId)
    : '/thumbnails/' + photoId + '.jpg';
}

export function hasLightbox() {
  return typeof window.openLightbox === 'function';
}

/** @param {number} id @param {string} filename @param {{id: number, filename: string}[]} photos */
export function openLightbox(id, filename, photos) {
  window.openLightbox(id, filename, photos);
}
