export const thumbnail = window.photoThumbnailUrl;
export const openExternalLink = window.openExternalLink;

/** @template T @param {string} url @returns {Promise<T>} */
export function safeFetch(url) {
  return window.safeFetch(url);
}

/** @type {Window['submitToInat']} */
export function submitToInat(id, comparison) {
  return window.submitToInat(id, comparison);
}

/** @type {Window['openLightbox']} */
export function openLightbox(id, filename, photos) {
  window.openLightbox(id, filename, photos);
}
