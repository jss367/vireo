// Page startup.
// Classic page script; loads after every other Move page definition.

// Preserve the inline script's order: the initial fetches, then the
// destination-input and folder-browser listeners.

/* ---------- Init ---------- */
document.addEventListener('DOMContentLoaded', function() {
  loadFolders();
  loadRemoteTargets();
  loadRules();
  loadRecentDestinations();
  loadMoveVisibilityPreference();
});

/* ---------- Input listeners for button state ---------- */
document.addEventListener('DOMContentLoaded', function() {
  var quickDest = document.getElementById('quickDestInput');
  if (quickDest) quickDest.addEventListener('input', updateQuickMoveBtn);

  var photoDest = document.getElementById('photoDestInput');
  if (photoDest) photoDest.addEventListener('input', updatePhotoSelectionUI);

  // Delegated handler so folder paths (which can contain backslashes on
  // Windows, e.g. C:\) don't have to be JS-escaped into inline handlers.
  var moveBrowserList = document.getElementById('moveBrowserList');
  if (moveBrowserList) {
    moveBrowserList.addEventListener('dblclick', function(e) {
      var item = e.target.closest('.browser-folder-item[data-browse-path]');
      if (item) browseTo(item.getAttribute('data-browse-path'));
    });
  }
});
