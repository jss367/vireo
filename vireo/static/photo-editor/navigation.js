// Back to Browse, the unsaved-edits guard, and Prev/Next photo navigation.
// Classic page script; load boot.js after all definitions.

function goBackToBrowse() {
  if (isEditorDirty() && !window.confirm('Discard unsaved edits to this photo?')) return;
  // The explicit confirm above already covered this navigation; don't let the
  // beforeunload guard prompt a second time. Re-arm shortly after in case the
  // navigation is cancelled or the page comes back from the bfcache.
  editorState.suppressUnloadPrompt = true;
  setTimeout(function() { editorState.suppressUnloadPrompt = false; }, 1000);
  if (window.history.length > 1) window.history.back();
  else window.location.href = '/browse';
}

function initUnloadGuard() {
  // Prev/Next confirm discards via editorNav, and Back via goBackToBrowse —
  // this catches every other way off the page with unsaved edits: navbar
  // links, refresh, tab close.
  window.addEventListener('beforeunload', function(e) {
    if (editorState.suppressUnloadPrompt || !isEditorDirty()) return;
    e.preventDefault();
    e.returnValue = '';
  });
}

function updateNavControls() {
  var ids = editorState.navIds || [];
  var idx = ids.indexOf(Number(editorState.photoId));
  var prev = document.getElementById('prevBtn');
  var next = document.getElementById('nextBtn');
  var pos = document.getElementById('editorNavPos');
  var hasNav = ids.length > 1 && idx !== -1;
  if (prev) { prev.hidden = !hasNav; prev.disabled = !hasNav || idx <= 0; }
  if (next) { next.hidden = !hasNav; next.disabled = !hasNav || idx >= ids.length - 1; }
  if (pos) pos.textContent = hasNav ? (idx + 1) + ' / ' + ids.length : '';
}

function editorNav(delta) {
  var ids = editorState.navIds || [];
  var idx = ids.indexOf(Number(editorState.photoId));
  if (idx === -1) return;
  var newIdx = idx + delta;
  if (newIdx < 0 || newIdx >= ids.length) return;
  if (isEditorDirty() && !window.confirm('Discard unsaved edits to this photo?')) return;
  loadPhoto(ids[newIdx]);
}
