// Edit History checkpoints and the saved-edit undo/redo hooks.
// Classic page script; load boot.js after all definitions.

window.beforeHistoryChange = function() {
  if (editorState.loading || Object.keys(editorState.savingPhotoIds).length || isEditorDirty()) {
    showToast('Save or discard your editor changes before undoing saved edits.', 'info');
    return false;
  }
  return true;
};

// Undo/redo of saved edits (navbar history controls) announces when its request
// is in flight so the editor freezes until the reload it triggers takes over.
var editorHistoryLoadSeq = null;
function bindEditHistoryBusyEvents() {
  document.addEventListener('vireo:edit-history-busy', function(event) {
    if (event.detail.busy) {
      editorHistoryLoadSeq = editorState.loadSeq;
      setEditorLoading(true);
    } else {
      // A reload (including its failure state) owns the controls once it starts.
      // Only release our freeze if the history request failed before that reload.
      if (editorHistoryLoadSeq === editorState.loadSeq) setEditorLoading(false);
      editorHistoryLoadSeq = null;
    }
  });
}

window.afterHistoryChange = async function() {
  if (editorState.photoId) await loadPhoto(editorState.photoId, {skipUrl: true});
};

async function restoreRecipe(recipe) {
  // The history panel is also pointer-events: none while loading, but guard
  // here too so a programmatic restore (or any escape from the CSS gate) can't
  // copy a previous photo's checkpoint onto the photoId currently loading.
  if (editorState.loading) return;
  cancelPointColorPicker();
  editorState.recipe = cloneRecipe(recipe || {});
  ensureCrop(editorState.recipe);
  // The restored checkpoint's crop needn't match the locked ratio.
  editorState.cropAspect = null;
  updateAspectButtons();
  syncControls();
  updatePreview();
  await saveRecipe('Restored photo edit checkpoint');
}

function timeLabel(value) {
  if (!value) return '';
  var d = new Date(value + 'Z');
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString([], {
    month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit'
  });
}

function checkpointName(recipe) {
  var key = recipeKey(recipe || {});
  if (key === '{}') return 'Original';
  var r = recipeForSave(recipe || {});
  var names = [];
  if (r.crop) names.push('Crop');
  if (r.rotation || r.flip || r.straighten) names.push('Transform');
  if (r.adjustments) names.push('Adjustments');
  return names.length ? names.join(', ') : 'Edit recipe';
}

function historyRow(title, subtitle, recipe, current, disabled) {
  var row = document.createElement('div');
  row.className = 'history-row' + (current ? ' current' : '');
  var text = document.createElement('div');
  var strong = document.createElement('strong');
  var span = document.createElement('span');
  strong.textContent = title;
  span.textContent = subtitle || '';
  text.appendChild(strong);
  text.appendChild(span);
  var btn = document.createElement('button');
  btn.className = 'editor-btn';
  btn.type = 'button';
  btn.textContent = current ? 'Current' : 'Restore';
  btn.disabled = !!disabled || !!current;
  btn.onclick = function() { restoreRecipe(recipe); };
  row.appendChild(text);
  row.appendChild(btn);
  return row;
}

async function loadHistory(photoId) {
  // Rapid Next/Prev can leave multiple /edit-history requests in flight; bail
  // out if the editor has moved on so an earlier response can't paint Restore
  // rows pointing at another photo's recipes.
  if (photoId == null) photoId = editorState.photoId;
  var list = document.getElementById('historyList');
  list.innerHTML = '<div class="history-empty">Loading history...</div>';
  try {
    var data = await safeFetch('/api/photos/' + photoId + '/edit-history?limit=100', {}, {toast: false});
    if (editorState.photoId !== photoId) return;
    list.innerHTML = '';
    var currentKey = recipeKey(editorState.savedRecipe);
    var visibleHistoryRows = 0;
    list.appendChild(historyRow('Current saved edit', checkpointName(editorState.savedRecipe), editorState.savedRecipe, true, true));
    list.appendChild(historyRow('Original', 'No edit recipe', null, currentKey === '{}', false));
    (data.history || []).forEach(function(entry) {
      var recipe = entry.new_recipe || null;
      if (recipeKey(recipe) === currentKey) return;
      visibleHistoryRows++;
      var title = checkpointName(recipe);
      var subtitle = timeLabel(entry.created_at) + ' - ' + (entry.description || 'Saved edit');
      if (entry.undone) subtitle += ' (undone)';
      list.appendChild(historyRow(title, subtitle, recipe, false, false));
    });
    if ((data.history || []).length === 0 || visibleHistoryRows === 0) {
      var empty = document.createElement('div');
      empty.className = 'history-empty';
      empty.textContent = 'No saved edit checkpoints for this photo yet.';
      list.appendChild(empty);
    }
  } catch (e) {
    if (editorState.photoId !== photoId) return;
    list.innerHTML = '<div class="history-empty">Could not load history.</div>';
  }
}
