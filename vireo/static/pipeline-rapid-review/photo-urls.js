// Preview, original and thumbnail URLs, and a burst's photo ids.
// Classic page script; load boot.js after all definitions.

function photoPreviewUrl(id) {
  return '/photos/' + id + '/preview?size=2560';
}

function photoOriginalUrl(id) {
  return '/photos/' + id + '/original';
}

function thumbUrl(photoOrId) {
  var photoId = photoOrId && typeof photoOrId === 'object' ? photoOrId.id : photoOrId;
  return window.vireoThumbnailUrl
    ? window.vireoThumbnailUrl(photoOrId)
    : '/thumbnails/' + photoId + '.jpg';
}

function burstIds(burst) {
  return (burst && burst.photo_ids) ? burst.photo_ids : (burst || []);
}
