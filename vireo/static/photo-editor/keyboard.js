// Editor keyboard shortcuts.
// Classic page script; load boot.js after all definitions.

function initEditorKeyboard() {
  function isActivationControl(t, tag) {
    var role = t && t.getAttribute ? t.getAttribute('role') : null;
    return tag === 'button' || tag === 'a' || tag === 'summary' || tag === 'label' ||
      role === 'button' || role === 'link' || role === 'menuitem' || role === 'tab';
  }
  document.addEventListener('keydown', function(e) {
    // Let the folder browser's own Escape handler dismiss only the picker,
    // preserving the export modal and the options already entered there.
    if (document.querySelector('.folder-browser-overlay.open')) return;
    // Same split for the export-preset save/delete dialog: its own Escape
    // handler dismisses just that dialog and leaves the export modal open.
    if (document.querySelector('.export-preset-dialog-overlay.open')) return;
    var exportOverlay = document.getElementById('exportOverlay');
    if (exportOverlay && exportOverlay.classList.contains('open')) {
      if (e.key === 'Escape') closeExportModal();
      return;
    }
    var t = e.target;
    var tag = t && t.tagName ? t.tagName.toLowerCase() : '';
    var isTextEntry = tag === 'input' || tag === 'textarea' || tag === 'select' ||
      (t && t.isContentEditable);
    var cropFieldKey = tag === 'input' && t && {
      cropX: 'x', cropY: 'y', cropW: 'w', cropH: 'h'
    }[t.id];
    if ((e.key === ' ' || e.code === 'Space') && !e.metaKey && !e.ctrlKey && !e.altKey &&
        !isTextEntry && !isActivationControl(t, tag)) {
      // Skip when focus is on a button/link/summary/role="button" so Space
      // still triggers the control's native activation instead of pan mode.
      window.setEditorSpacePan(true);
      e.preventDefault();
      return;
    }
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    if (e.key === 'Enter' && cropFieldKey) {
      // These fields themselves reopen crop editing on focus. Apply the
      // pending typed value before accepting the crop; relying on change/blur
      // would commit the previous value because keydown fires first.
      e.preventDefault();
      setCropField(cropFieldKey, t.value);
      commitCropView();
      return;
    }
    if (isTextEntry) return;
    if (e.key === 'ArrowLeft') { e.preventDefault(); editorNav(-1); }
    else if (e.key === 'ArrowRight') { e.preventDefault(); editorNav(1); }
    else if (e.key === 'Enter') {
      // Enter locks in the current edit (crop, straighten, adjustments) by
      // committing the working recipe — same as clicking Save Changes. Skip
      // when focus is on a control with native Enter activation (button, link,
      // summary, role="button") so tabbing to Reset All / Back / a transform
      // button and pressing Enter still triggers that control. An unchanged
      // reopened crop can still be accepted without writing another history
      // entry; otherwise this is a no-op when there is nothing to save.
      if (isActivationControl(t, tag)) return;
      var saveBtn = document.getElementById('saveBtn');
      if ((saveBtn && !saveBtn.disabled) ||
          (editorState.cropEditing && recipeForSave(editorState.recipe).crop)) {
        e.preventDefault();
        commitCropView();
      }
    }
  });
  document.addEventListener('keyup', function(e) {
    if ((e.key === ' ' || e.code === 'Space') && editorState.spacePan) {
      window.setEditorSpacePan(false);
      e.preventDefault();
    }
  });
}
