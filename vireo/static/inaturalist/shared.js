export const thumbnail = window.photoThumbnailUrl;
export const openExternalLink = window.openExternalLink;

/** @template T @param {string} url @param {RequestInit} [options] @returns {Promise<T>} */
export function safeFetch(url, options) {
  return window.safeFetch(url, options);
}

/** @type {Window['submitToInat']} */
export function submitToInat(id, comparison) {
  return window.submitToInat(id, comparison);
}

/** @type {Window['openLightbox']} */
export function openLightbox(id, filename, photos) {
  window.openLightbox(id, filename, photos);
}
