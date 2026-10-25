/* Browse: keyboard shortcuts and grid arrow-key navigation. ES module (see
   browse/README.md). */
import { pagination } from './pagination.js';
import { selection } from './selection-state.js';
import { state } from './state.js';
import {
  closeLightbox, closePanoramaModal, exitLightboxFullscreen, matchesShortcut,
  preferredScrollBehavior,
} from './shared.js';
import {
  batchSetColorLabel, batchSetFlag, batchSetRating, hideBatchCollectionModal,
  hideBatchKeywordModal,
} from './batch-actions.js';
import { hideCollectionModal } from './collection-editor.js';
import { _shortcuts } from './config.js';
import { closeDetail, redoLast, undoLast } from './detail.js';
import { setColorLabel, setFlag, setRating } from './edit-actions.js';
import { closeExportModal } from './export.js';
import {
  browseKeyMatchesConfiguredShortcut, browsePhotoIsAvailable, openBrowseShortcutPhoto,
} from './lightbox.js';
import { loadPhotos } from './loading.js';
import { browseCompare, openBrowseCompare } from './selection-panel.js';
import {
  clearSelection, getActiveSelection, selectAllMatchingPhotos, selectPhoto,
} from './selection.js';

/* ---------- Keyboard Shortcuts ---------- */
document.addEventListener('keydown', function(e) {
  // The shared folder browser installs its Escape handler when it opens.
  // Leave the underlying export modal alone so that handler can dismiss only
  // the topmost picker and preserve the user's export settings.
  if (/** @type {HTMLElement | null} */ (document.querySelector('.folder-browser-overlay.open'))) return;
  // Same split for the export-preset save/delete dialog: its own Escape
  // handler dismisses just that dialog and leaves the export modal open.
  if (/** @type {HTMLElement | null} */ (document.querySelector('.export-preset-dialog-overlay.open'))) return;

  if (browseCompare.isOpen()) {
    if (e.key === 'Escape') {
      e.preventDefault();
      browseCompare.close();
      return;
    }
    if (e.key === 'ArrowRight') {
      e.preventDefault();
      browseCompare.step(1);
      return;
    }
    if (e.key === 'ArrowLeft') {
      e.preventDefault();
      browseCompare.step(-1);
      return;
    }
    return;
  }

  // Suppress browse shortcuts while any modal is open
  var openModal = /** @type {HTMLElement | null} */ (document.querySelector('.modal-overlay.open'));
  if (openModal) {
    if (e.key === 'Escape') {
      if (openModal.id === 'batchKeywordModal') hideBatchKeywordModal();
      else if (openModal.id === 'batchCollectionModal') hideBatchCollectionModal();
      else if (openModal.id === 'collectionModal') hideCollectionModal();
      else if (openModal.id === 'exportOverlay') closeExportModal();
      else if (openModal.id === 'panoramaOverlay') closePanoramaModal();
      else openModal.classList.remove('open');
    }
    return;
  }

  if (/** @type {HTMLElement} */ (document.getElementById('lightboxOverlay')).classList.contains('active')) return;

  if (e.target instanceof Element && (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT')) return;

  if (e.key === 'Escape') {
    closeDetail();
    clearSelection();
    return;
  }

  if (!_shortcuts) return;

  // Undo
  if (matchesShortcut(e, _shortcuts.undo)) {
    e.preventDefault();
    undoLast();
    return;
  }

  // Redo
  if (matchesShortcut(e, _shortcuts.redo)) {
    e.preventDefault();
    redoLast();
    return;
  }

  // Select all
  if (matchesShortcut(e, _shortcuts.select_all)) {
    e.preventDefault();
    selectAllMatchingPhotos();
    return;
  }

  // Arrow keys: navigate grid (not configurable)
  if (e.key === 'ArrowRight') {
    e.preventDefault();
    moveBrowseSelection(1, e);
    return;
  } else if (e.key === 'ArrowLeft') {
    e.preventDefault();
    moveBrowseSelection(-1, e);
    return;
  } else if (e.key === 'ArrowDown') {
    e.preventDefault();
    moveBrowseSelection(getBrowseGridColumnCount(), e);
    return;
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    moveBrowseSelection(-getBrowseGridColumnCount(), e);
    return;
  }

  // Must stay aligned with updateBatchBar(): any non-empty selectedPhotos is
  // actionable (size >= 1, not > 1), otherwise shortcuts silently no-op while
  // the bar advertises "1 selected" after cmd-click reduces the set to one.
  var useBatch = selection.batchIds.size >= 1;
  var hasActive = useBatch || selection.focusedPhotoId;

  if (matchesShortcut(e, _shortcuts.compare || 'c')) {
    var compareIds = getActiveSelection();
    if (compareIds.length >= 2) {
      e.preventDefault();
      openBrowseCompare();
    }
    return;
  }

  // Ratings: 0-5
  if (hasActive) {
    for (var r = 0; r <= 5; r++) {
      if (matchesShortcut(e, _shortcuts['rate_' + r])) {
        if (useBatch) batchSetRating(r);
        else if (selection.focusedPhotoId != null) setRating(selection.focusedPhotoId, r);
        return;
      }
    }
  }

  // Flag, reject, unflag
  if (hasActive) {
    var applyFlag = useBatch ? batchSetFlag : setFlag;
    var applyColor = useBatch ? batchSetColorLabel : setColorLabel;
    if (matchesShortcut(e, _shortcuts.flag)) { e.preventDefault(); applyFlag('flagged'); return; }
    else if (matchesShortcut(e, _shortcuts.reject)) { e.preventDefault(); applyFlag('rejected'); return; }
    else if (matchesShortcut(e, _shortcuts.unflag)) { e.preventDefault(); applyFlag('none'); return; }

    // Color labels
    if (matchesShortcut(e, _shortcuts.color_red)) { e.preventDefault(); applyColor('red'); return; }
    else if (matchesShortcut(e, _shortcuts.color_yellow)) { e.preventDefault(); applyColor('yellow'); return; }
    else if (matchesShortcut(e, _shortcuts.color_green)) { e.preventDefault(); applyColor('green'); return; }
    else if (matchesShortcut(e, _shortcuts.color_blue)) { e.preventDefault(); applyColor('blue'); return; }
  }

  if (!e.ctrlKey && !e.metaKey && !e.altKey && !e.shiftKey && !browseKeyMatchesConfiguredShortcut(e)) {
    var browseViewKey = e.key.toLowerCase();
    if (browseViewKey === 'e') {
      if (openBrowseShortcutPhoto(false)) e.preventDefault();
      return;
    }
    if (browseViewKey === 'f') {
      if (openBrowseShortcutPhoto(true)) e.preventDefault();
      return;
    }
    if (browseViewKey === 'g') {
      exitLightboxFullscreen();
      if (/** @type {HTMLElement} */ (document.getElementById('lightboxOverlay')).classList.contains('active')) closeLightbox();
      e.preventDefault();
      return;
    }
  }
});

export function getBrowseGridColumnCount() {
  var grid = /** @type {HTMLElement} */ (document.getElementById('grid'));
  if (grid) {
    var template = window.getComputedStyle(grid).gridTemplateColumns || '';
    var tracks = template.trim().split(/\s+/).filter(Boolean);
    if (tracks.length && template !== 'none') return tracks.length;
  }

  var cards = grid ? /** @type {NodeListOf<HTMLElement>} */ (grid.querySelectorAll('.grid-card')) : /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('.grid-card'));
  if (!cards.length) return 1;

  var firstTop = cards[0]?.offsetTop || 0;
  var count = 0;
  for (var i = 0; i < cards.length; i++) {
    if (Math.abs((cards[i]?.offsetTop || 0) - firstTop) > 2) break;
    count++;
  }
  return Math.max(1, count);
}

export async function moveBrowseSelection(/** @type {number} */ delta, /** @type {KeyboardEvent | undefined} */ e = undefined) {
  var nextIndex = selection.anchorIndex < 0 && delta > 0 ? 0 : selection.anchorIndex + delta;
  while (true) {
    while (nextIndex >= 0 && nextIndex < state.photos.length && state.photos[nextIndex] &&
           !browsePhotoIsAvailable(/** @type {BrowsePhoto} */ (state.photos[nextIndex]))) {
      nextIndex += delta > 0 ? 1 : -1;
    }
    if (nextIndex >= 0 && nextIndex < state.photos.length && state.photos[nextIndex]) break;
    if (delta <= 0 || nextIndex < 0 || pagination.allLoaded) return;

    // Skipping trailing offline cards can move us beyond the loaded window.
    // Fetch the next page and resume the skip until an available row appears
    // or the dataset is exhausted.
    var beforeLength = state.photos.length;
    await loadPhotos();
    if (state.photos.length === beforeLength) return;
  }
  var selectionEvent = {
    shiftKey: !!(e && e.shiftKey),
    metaKey: !!(e && e.metaKey),
    ctrlKey: !!(e && e.ctrlKey),
  };
  var photo = state.photos[nextIndex]; if (!photo) return;
  selectPhoto(selectionEvent, photo.id, nextIndex);
  scrollToCard(selection.anchorIndex);
}

export function scrollToCard(/** @type {number} */ idx) {
  var cards = /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('.grid-card'));
  var card = cards[idx]; if (card) card.scrollIntoView({ block: 'nearest', behavior: preferredScrollBehavior() });
}
