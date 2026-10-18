// The right-click menu for miss cards.
// ES module.

import { state, CATEGORY_ORDER } from './state.js';
import { hasContextMenu, openContextMenu, speciesMenuItems } from './shared.js';
import { openMiss, openPhotoInBrowse } from './photo-open.js';
import { flagFocusedMiss, unflagMiss } from './flagging.js';
import { clearSelection, setSelected } from './selection.js';
import { setFocused } from './keyboard.js';

/** @param {number} photoId @param {MissCategory} cat @param {number} idx */ function buildMissContextMenu(photoId, cat, idx) {
  return /** @type {MissMenuItem[]} */ ([
    { label: 'Open in Browse',
      onClick: function() { openPhotoInBrowse(photoId); } },
    { label: 'Open in Lightbox',
      onClick: function() { openMiss(cat, idx); } },
  ]).concat(
    speciesMenuItems(photoId)
  ).concat([
    { separator: true },
    { label: 'Flag as Pick',
      onClick: function() { flagFocusedMiss(photoId, 'flagged'); } },
    { label: 'Reject Photo',
      onClick: function() { flagFocusedMiss(photoId, 'rejected'); } },
    { label: 'Unmark Missed', disabled: state.previewActive,
      onClick: function() { unflagMiss(cat, photoId); } },
  ]);
}

export function bindMissContextMenu() {
  document.addEventListener('contextmenu', function(e) {
    if (!(e.target instanceof Element)) return;
    var card = e.target.closest(/** @type {'div'} */ ('.miss-card'));
    if (!card || !card.dataset.photoId) return;
    if (e.target.closest('.vireo-ctx-menu')) return;
    e.preventDefault();
    e.stopPropagation();
    var photoId = parseInt(card.dataset.photoId, 10);
    var cat = /** @type {MissCategory} */ (card.dataset.cat);
    var idx = parseInt(card.dataset.idx || '', 10);
    if (isNaN(photoId) || !CATEGORY_ORDER.includes(/** @type {MissCategory} */ (cat)) || isNaN(idx)) return;
    setFocused(cat, idx);
    if (!state.selection.has(photoId)) {
      clearSelection();
      setSelected(photoId, true);
    }
    if (hasContextMenu()) {
      openContextMenu(e, buildMissContextMenu(photoId, cat, idx));
    }
  });
}
