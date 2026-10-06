// Shared-lightbox listeners: page prefetch, the navigation boundary, Picks, and close.
// Classic page script; load boot.js after all definitions.

function bindLifeListLightboxEvents() {
  // Prefetch the next page before the viewer reaches the end of the current
  // lightbox list. If navigation reaches the boundary first, the boundary
  // listener waits for that same request and advances as soon as it arrives.
  document.addEventListener('lightbox:photochanged', function(event) {
    var entry = lifeListEntry(lifeListLightboxSpecies);
    if (!entry || !entry.has_more) return;
    var photoId = event && event.detail && event.detail.photoId;
    var index = (entry.photos || []).findIndex(function(photo) { return photo.id === photoId; });
    if (index >= entry.photos.length - 3) loadMoreLifeListPhotos(entry);
  });

  document.addEventListener('lightbox:navigationboundary', function(event) {
    var detail = event && event.detail || {};
    if (detail.delta !== 1) return;
    var entry = lifeListEntry(lifeListLightboxSpecies);
    if (!entry || !entry.has_more) return;
    var currentId = detail.photoId;
    loadMoreLifeListPhotos(entry).then(function() {
      if (window.vireoLightboxSession.requestedPhotoId() !== currentId) return;
      var index = entry.photos.findIndex(function(photo) { return photo.id === currentId; });
      var next = entry.photos[index + 1];
      if (next && window.openLightbox) {
        openLightbox(next.id, next.filename, entry.photos);
      }
    });
  });

  // Picks change the server-authoritative Life List order. Refetch instead of
  // duplicating the Representative → Pick → ranked ordering in the browser;
  // the open lightbox keeps its stable navigation array, while the card is ready
  // in the new order (and with its Pick badge) when the viewer closes.
  document.addEventListener('lightbox:flagchanged', function(event) {
    var detail = event && event.detail || {};
    var previous = detail.previousFlag || 'none';
    var next = detail.flag || 'none';
    if (previous === next) return;
    if (previous !== 'flagged' && next !== 'flagged') return;
    loadLifeList();
  });

  document.addEventListener('lightbox:closed', function() {
    lifeListLightboxSpecies = null;
  });
}
