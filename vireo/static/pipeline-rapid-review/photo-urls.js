// Preview, original and thumbnail URLs, and a burst's photo ids.
// Classic page script; load boot.js after all definitions.

/** @param {number} id */
function photoPreviewUrl(id) {
  return '/photos/' + id + '/preview?size=2560';
}

/** @param {number} id */
function photoOriginalUrl(id) {
  return '/photos/' + id + '/original';
}

/** @param {RapidPhoto|number} photoOrId */
function thumbUrl(photoOrId) {
  var photoId = photoOrId && typeof photoOrId === 'object' ? photoOrId.id : photoOrId;
  return window.vireoThumbnailUrl
    ? window.vireoThumbnailUrl(photoOrId)
    : '/thumbnails/' + photoId + '.jpg';
}

/** @param {RapidBurst|number[]|null|undefined} burst @returns {number[]} */
function burstIds(burst) {
  return (burst && /** @type {RapidBurst} */ (burst).photo_ids) ? /** @type {RapidBurst} */ (burst).photo_ids : /** @type {number[]} */ (burst || []);
}
