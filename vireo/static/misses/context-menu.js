// The right-click menu for miss cards.
// Classic page script; load boot.js after all definitions.

function buildMissContextMenu(photoId, cat, idx) {
  return [
    { label: 'Open in Browse',
      onClick: function() { openPhotoInBrowse(photoId); } },
    { label: 'Open in Lightbox',
      onClick: function() { openMiss(cat, idx); } },
  ].concat(
    typeof window.buildSpeciesHighlightMenuItems === 'function'
      ? window.buildSpeciesHighlightMenuItems([photoId], { showFetchFallback: true })
      : []
  ).concat(
    typeof window.buildSpeciesRepresentativeMenuItems === 'function'
      ? window.buildSpeciesRepresentativeMenuItems([photoId], { showFetchFallback: true })
      : []
  ).concat([
    { separator: true },
    { label: 'Flag as Pick',
      onClick: function() { flagFocusedMiss(photoId, 'flagged'); } },
    { label: 'Reject Photo',
      onClick: function() { flagFocusedMiss(photoId, 'rejected'); } },
    { label: 'Unmark Missed', disabled: previewActive,
      onClick: function() { unflagMiss(cat, photoId); } },
  ]);
}

function bindMissContextMenu() {
  document.addEventListener('contextmenu', function(e) {
    var card = e.target.closest('.miss-card');
    if (!card || !card.dataset.photoId) return;
    if (e.target.closest('.vireo-ctx-menu')) return;
    e.preventDefault();
    e.stopPropagation();
    var photoId = parseInt(card.dataset.photoId, 10);
    var cat = card.dataset.cat;
    var idx = parseInt(card.dataset.idx, 10);
    if (isNaN(photoId) || !cat || isNaN(idx)) return;
    setFocused(cat, idx);
    if (!selection.has(photoId)) {
      clearSelection();
      setSelected(photoId, true);
    }
    if (typeof openContextMenu === 'function') {
      openContextMenu(e, buildMissContextMenu(photoId, cat, idx));
    }
  });
}
