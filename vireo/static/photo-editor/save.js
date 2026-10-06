// Resetting all edits and saving the recipe.
// Classic page script; load boot.js after all definitions.

function resetAllEdits() {
  cancelPointColorPicker();
  editorState.recipe = {};
  ensureCrop(editorState.recipe);
  editorState.cropAspect = null;
  updateAspectButtons();
  syncControls();
  markChanged(true);
}

async function saveRecipe(description) {
  if (!editorState.photoId || editorState.loading) return false;
  if (editorState.savingPhotoIds[String(editorState.photoId)]) return false;
  // Capture the photo being saved so a Prev/Next during the PUT can't make us
  // write the previous photo's returned recipe back into the editor state for
  // a different photo (or refresh the cache under the wrong photoId).
  var savedPhotoId = editorState.photoId;
  editorState.savingPhotoIds[String(savedPhotoId)] = true;
  finishEditorHistoryGesture();
  if (window.renderHistoryControls) window.renderHistoryControls();
  var acceptingCropEdit = editorState.cropEditing &&
    !!recipeForSave(editorState.recipe).crop;
  var btn = document.getElementById('saveBtn');
  btn.disabled = true;
  setStatus('Saving...');
  // The sliders stay live during the PUT, so remember what we sent: an edit
  // made while the request is in flight must survive its response.
  var sentKey = recipeKey(editorState.recipe);
  try {
    var data = await safeFetch('/api/photos/' + savedPhotoId + '/edit-recipe', {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        recipe: recipeForSave(editorState.recipe),
        description: description || 'Updated photo edit recipe',
      }),
    }, {toast: false});
    // Sync the shared edit-recipe cache against the photo we actually saved,
    // regardless of where the editor is now.
    if (typeof window.vireoRefreshEditRecipeCache === 'function') {
      var updates = {};
      updates[String(savedPhotoId)] = data.recipe || null;
      window.vireoRefreshEditRecipeCache(updates);
    }
    if (typeof showToast === 'function') showToast('Edit saved', 'success');
    // If the user navigated to a different photo while the PUT was in flight,
    // the editor now owns that other photo's recipe / savedRecipe / Save
    // button — don't overwrite it with the previous photo's saved data.
    if (editorState.photoId !== savedPhotoId) return false;
    editorState.savedRecipe = cloneRecipe(data.recipe || {});
    editorState.savedRecipeSeq++;
    if (recipeKey(editorState.recipe) !== sentKey) {
      // The user kept editing while the PUT was in flight. What we sent is
      // now the saved baseline, but the newer edits are still in the editor:
      // keep them and their undo history, and show them as unsaved. Report
      // false so save-then-export doesn't render a recipe older than the
      // canvas.
      editorState.savedLocalStale = !!data.local_mask_stale;
      updateDirtyState();
      loadHistory();
      return false;
    }
    editorState.recipe = cloneRecipe(data.recipe || {});
    editorState.savedLocalStale = !!data.local_mask_stale;
    editorState.localStale = editorState.savedLocalStale;
    ensureCrop(editorState.recipe);
    editorState.cropEditing = !recipeForSave(editorState.recipe).crop;
    resetEditorHistory();
    if (acceptingCropEdit && !editorState.cropEditing) {
      editorState.zoomMode = 'fit';
    }
    syncControls();
    updatePreview();
    loadHistory();
    return true;
  } catch (e) {
    // Only re-enable the Save button / surface the error if the editor is
    // still on the photo we were saving; otherwise the new photo's controls
    // own these surfaces.
    if (editorState.photoId === savedPhotoId) {
      btn.disabled = false;
      setStatus(e.message || 'Save failed', true);
    }
    return false;
  } finally {
    delete editorState.savingPhotoIds[String(savedPhotoId)];
    if (window.renderHistoryControls) window.renderHistoryControls();
  }
}
