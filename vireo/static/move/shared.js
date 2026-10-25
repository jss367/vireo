// The Move page's boundary with the shared navbar and the shared classic
// scripts (vireo-utils.js, vireo-rsync-help.js). Page modules import these
// names from here instead of reaching for globals, so this file is the one
// list of what the page uses from shared code.
//
// The forwarders look the global up at call time, as the classic page did, so
// one replaced at runtime (tests stub showToast and safeFetch) is still the
// one called. The escape helpers are pure and loaded first by _navbar.html,
// so they are bound once.

export const escapeHtml = window.escapeHtml;
export const escapeAttr = window.escapeAttr;

/** The navbar's safeFetch, typed per endpoint this page reads (move.d.ts). */
export const safeFetch = /** @type {MoveSafeFetch} */ (
  /** @param {string} url @param {RequestInit=} init @param {{toast?: boolean}=} options */
  function(url, init, options) { return window.safeFetch(url, init, options); }
);

/** @param {string} message @param {string=} kind */
export function showToast(message, kind) {
  if (typeof window.showToast === 'function') window.showToast(message, kind);
}

/** How to install rsync on this platform (vireo-rsync-help.js). */
export function rsyncInstallHint() {
  return window.rsyncInstallHint();
}

/**
 * The navbar's thumbnail URL for a photo, or the plain one where the navbar's
 * builder is not loaded.
 * @param {MovePhoto} photo
 */
export function thumbnailUrl(photo) {
  return window.vireoThumbnailUrl ? window.vireoThumbnailUrl(photo) : '/thumbnails/' + photo.id + '.jpg';
}
