// Copy settings, revert, export, iNaturalist, and the right-click menu.
// Classic page script; load boot.js after all definitions.

async function copyEditSettings() {
  if (!editorState.photoId || !window.vireoEditNav) return;
  var recipe = recipeForSave(editorState.recipe);
  if (!recipe || Object.keys(recipe).length === 0) {
    if (typeof showToast === 'function') showToast('No edits to copy', 'error');
    else setStatus('No edits to copy', true);
    return;
  }
  await window.vireoEditNav.setCopiedRecipe(recipe, {
    source: (editorState.photo && editorState.photo.filename) || null,
    at: Date.now(),
  });
  if (typeof showToast === 'function') showToast('Edit settings copied — paste onto a selection in Browse', 'success');
  else setStatus('Settings copied');
}

function revertUnsavedEdits() {
  if (editorState.loading || editorState.savingPhotoIds[String(editorState.photoId)] ||
      !isEditorDirty()) return;
  if (!window.confirm('Discard unsaved edits and return to the last saved version?')) return;
  // A mask update may still be awaiting its snapshot POST. Its result belongs
  // to the discarded working state and must not make the restored recipe dirty.
  editorState.localMaskUpdateSeq++;
  cancelPointColorPicker();
  editorState.recipe = cloneRecipe(editorState.savedRecipe);
  editorState.localStale = editorState.savedLocalStale;
  ensureCrop(editorState.recipe);
  editorState.showBefore = false;
  editorState.cropEditing = !recipeForSave(editorState.recipe).crop;
  editorState.cropAspect = null;
  editorState.zoomMode = 'fit';
  updateAspectButtons();
  recordEditorHistory();
  syncControls();
  updatePreview();
  if (typeof showToast === 'function') showToast('Unsaved edits discarded', 'success');
  else setStatus('Unsaved edits discarded');
}

function exportCurrentEditedPhoto() {
  if (!editorState.photoId || editorState.loading ||
      editorState.savingPhotoIds[String(editorState.photoId)]) return;
  return openExportModal();
}

function sendCurrentEditedPhotoToInat() {
  if (!editorState.photoId || editorState.loading ||
      editorState.savingPhotoIds[String(editorState.photoId)] || isEditorDirty()) return;
  if (typeof window.submitToInat === 'function') {
    window.submitToInat(editorState.photoId);
  }
}

function buildPhotoEditorContextMenu() {
  var unavailable = !editorState.photoId || editorState.loading;
  var dirty = !unavailable && isEditorDirty();
  var savePending = !unavailable &&
    !!editorState.savingPhotoIds[String(editorState.photoId)];
  var recipe = unavailable ? {} : recipeForSave(editorState.recipe);
  var hasEdits = Object.keys(recipe || {}).length > 0;
  var beforeLabel = editorState.showBefore ? 'Show Current Edits' : 'Show Saved Version';

  return [
    {
      label: 'Save Changes',
      disabled: !dirty || savePending,
      disabledHint: unavailable
        ? 'No photo is ready to edit'
        : (savePending ? 'Saving changes' : 'No unsaved changes'),
      onClick: function() { saveRecipe(); },
    },
    {
      label: 'Revert to Saved',
      disabled: !dirty || savePending,
      disabledHint: unavailable
        ? 'No photo is ready to edit'
        : (savePending ? 'Saving changes' : 'No unsaved changes'),
      onClick: revertUnsavedEdits,
    },
    { separator: true },
    {
      label: beforeLabel,
      disabled: !dirty && !editorState.showBefore,
      disabledHint: unavailable ? 'No photo is ready to edit' : 'Make an edit to compare it with the saved version',
      onClick: toggleBeforePreview,
    },
    {
      label: 'Fit to Window',
      disabled: unavailable || editorState.zoomMode === 'fit',
      disabledHint: unavailable ? 'No photo is ready to edit' : 'Already fit to the window',
      onClick: setEditorZoomToFit,
    },
    {
      label: 'View at 100%',
      disabled: unavailable || editorIsActualZoom(),
      disabledHint: unavailable ? 'No photo is ready to edit' : 'Already viewing at 100%',
      onClick: setEditorZoomToActual,
    },
    { separator: true },
    {
      label: 'Auto Tone',
      disabled: unavailable || savePending,
      disabledHint: unavailable
        ? 'No photo is ready to edit'
        : (savePending ? 'Saving changes' : undefined),
      onClick: autoTone,
    },
    {
      label: 'Edit Crop',
      disabled: unavailable || savePending || editorState.cropEditing || editorState.showBefore,
      disabledHint: unavailable
        ? 'No photo is ready to edit'
        : (savePending
          ? 'Saving changes'
          : (editorState.showBefore ? 'Return to the current edit first' : 'Crop editing is already active')),
      onClick: beginCropEdit,
    },
    {
      label: 'Full Frame',
      disabled: unavailable || savePending || !recipe.crop,
      disabledHint: unavailable
        ? 'No photo is ready to edit'
        : (savePending ? 'Saving changes' : 'The full frame is already selected'),
      onClick: resetCrop,
    },
    {
      label: 'Rotate Left',
      disabled: unavailable || savePending,
      disabledHint: unavailable
        ? 'No photo is ready to edit'
        : (savePending ? 'Saving changes' : undefined),
      onClick: function() { rotateRecipe(-90); },
    },
    {
      label: 'Rotate Right',
      disabled: unavailable || savePending,
      disabledHint: unavailable
        ? 'No photo is ready to edit'
        : (savePending ? 'Saving changes' : undefined),
      onClick: function() { rotateRecipe(90); },
    },
    {
      label: 'Flip Horizontal',
      disabled: unavailable || savePending,
      disabledHint: unavailable
        ? 'No photo is ready to edit'
        : (savePending ? 'Saving changes' : undefined),
      onClick: function() { flipRecipe('horizontal'); },
    },
    {
      label: 'Flip Vertical',
      disabled: unavailable || savePending,
      disabledHint: unavailable
        ? 'No photo is ready to edit'
        : (savePending ? 'Saving changes' : undefined),
      onClick: function() { flipRecipe('vertical'); },
    },
    { separator: true },
    {
      label: 'Copy Edit Settings',
      disabled: unavailable || !hasEdits,
      disabledHint: unavailable ? 'No photo is ready to edit' : 'There are no edit settings to copy',
      onClick: copyEditSettings,
    },
    {
      label: 'Reset All Edits',
      disabled: unavailable || savePending || !hasEdits,
      disabledHint: unavailable
        ? 'No photo is ready to edit'
        : (savePending ? 'Saving changes' : 'There are no edits to reset'),
      onClick: resetAllEdits,
    },
    { separator: true },
    {
      label: 'Export\u2026',
      disabled: unavailable || savePending,
      disabledHint: unavailable
        ? 'No photo is ready to export'
        : (savePending ? 'Saving changes' : undefined),
      onClick: exportCurrentEditedPhoto,
    },
    {
      label: 'Send to iNaturalist',
      disabled: unavailable || dirty || savePending,
      disabledHint: unavailable
        ? 'No photo is ready to send'
        : (savePending ? 'Saving changes' :
          (dirty ? 'Save changes before sending to iNaturalist' : undefined)),
      onClick: sendCurrentEditedPhotoToInat,
    },
  ];
}

function initEditorContextMenu() {
  var shell = document.querySelector('.editor-shell');
  if (!shell || typeof window.openContextMenu !== 'function') return;
  shell.addEventListener('contextmenu', function(e) {
    // Keep the browser's native spelling/copy/paste menu anywhere text can be
    // entered or selected. The Vireo menu owns the rest of the editor surface.
    var target = e.target;
    if (target && target.closest &&
        target.closest('input, textarea, select, [contenteditable]')) return;
    var selection = window.getSelection ? window.getSelection() : null;
    if (selection && !selection.isCollapsed && String(selection).trim()) return;
    if (target && target.closest && target.closest('.vireo-ctx-menu')) return;
    e.preventDefault();
    e.stopPropagation();
    openContextMenu(e, buildPhotoEditorContextMenu());
  });
}
