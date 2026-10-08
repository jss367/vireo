/* Browse: keyboard shortcuts and grid arrow-key navigation.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Keyboard Shortcuts ---------- */
document.addEventListener('keydown', function(e) {
  // The shared folder browser installs its Escape handler when it opens.
  // Leave the underlying export modal alone so that handler can dismiss only
  // the topmost picker and preserve the user's export settings.
  if (document.querySelector('.folder-browser-overlay.open')) return;
  // Same split for the export-preset save/delete dialog: its own Escape
  // handler dismisses just that dialog and leaves the export modal open.
  if (document.querySelector('.export-preset-dialog-overlay.open')) return;

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
  var openModal = document.querySelector('.modal-overlay.open');
  if (openModal) {
    if (e.key === 'Escape') {
      if (openModal.id === 'batchKeywordModal') hideBatchKeywordModal();
      else if (openModal.id === 'batchCollectionModal') hideBatchCollectionModal();
      else if (openModal.id === 'exportOverlay') closeExportModal();
      else if (openModal.id === 'panoramaOverlay') closePanoramaModal();
      else openModal.classList.remove('open');
    }
    return;
  }

  if (document.getElementById('lightboxOverlay').classList.contains('active')) return;

  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;

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
  var useBatch = selectedPhotos.size >= 1;
  var hasActive = useBatch || selectedPhotoId;

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
        else setRating(selectedPhotoId, r);
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
      if (typeof exitLightboxFullscreen === 'function') exitLightboxFullscreen();
      if (document.getElementById('lightboxOverlay').classList.contains('active')) closeLightbox();
      e.preventDefault();
      return;
    }
  }
});

function getBrowseGridColumnCount() {
  var grid = document.getElementById('grid');
  if (grid) {
    var template = window.getComputedStyle(grid).gridTemplateColumns || '';
    var tracks = template.trim().split(/\s+/).filter(Boolean);
    if (tracks.length && template !== 'none') return tracks.length;
  }

  var cards = grid ? grid.querySelectorAll('.grid-card') : document.querySelectorAll('.grid-card');
  if (!cards.length) return 1;

  var firstTop = cards[0].offsetTop;
  var count = 0;
  for (var i = 0; i < cards.length; i++) {
    if (Math.abs(cards[i].offsetTop - firstTop) > 2) break;
    count++;
  }
  return Math.max(1, count);
}

async function moveBrowseSelection(delta, e) {
  var nextIndex = selectedIndex < 0 && delta > 0 ? 0 : selectedIndex + delta;
  while (true) {
    while (nextIndex >= 0 && nextIndex < photos.length && photos[nextIndex] &&
           !browsePhotoIsAvailable(photos[nextIndex])) {
      nextIndex += delta > 0 ? 1 : -1;
    }
    if (nextIndex >= 0 && nextIndex < photos.length && photos[nextIndex]) break;
    if (delta <= 0 || nextIndex < 0 || allLoaded) return;

    // Skipping trailing offline cards can move us beyond the loaded window.
    // Fetch the next page and resume the skip until an available row appears
    // or the dataset is exhausted.
    var beforeLength = photos.length;
    await loadPhotos();
    if (photos.length === beforeLength) return;
  }
  var selectionEvent = {
    shiftKey: !!(e && e.shiftKey),
    metaKey: !!(e && e.metaKey),
    ctrlKey: !!(e && e.ctrlKey),
  };
  selectPhoto(selectionEvent, photos[nextIndex].id, nextIndex);
  scrollToCard(selectedIndex);
}

function scrollToCard(idx) {
  var cards = document.querySelectorAll('.grid-card');
  if (cards[idx]) cards[idx].scrollIntoView({ block: 'nearest', behavior: preferredScrollBehavior() });
}
