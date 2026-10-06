// Edit presets: loading, applying, saving, and deleting.
// Classic page script; load boot.js after all definitions.

// --- Presets ---------------------------------------------------------------
// Presets explicitly select the settings they own; application merges those
// settings into the working recipe and rebinds local masks to this photo.

async function loadPresets(selectId) {
  try {
    var data = await safeFetch('/api/edit-presets', {}, {toast: false});
    editorState.presets = data.presets || [];
  } catch (e) {
    editorState.presets = [];
  }
  renderPresetOptions(selectId);
}

function renderPresetOptions(selectId) {
  var sel = document.getElementById('presetSelect');
  if (!sel) return;
  var current = selectId != null ? String(selectId) : sel.value;
  sel.innerHTML = '';
  var placeholder = document.createElement('option');
  placeholder.value = '';
  placeholder.textContent = editorState.presets.length
    ? 'Select a preset...'
    : 'No presets saved yet';
  sel.appendChild(placeholder);
  editorState.presets.forEach(function(p) {
    var opt = document.createElement('option');
    opt.value = String(p.id);
    opt.textContent = p.name;
    sel.appendChild(opt);
  });
  sel.value = current;
  if (sel.value !== current) sel.value = '';
  onPresetSelected();
}

function selectedPreset() {
  var sel = document.getElementById('presetSelect');
  var id = sel ? Number(sel.value) : NaN;
  return editorState.presets.find(function(p) { return p.id === id; }) || null;
}

function onPresetSelected() {
  var preset = selectedPreset();
  document.getElementById('applyPresetBtn').disabled = !preset;
  document.getElementById('deletePresetBtn').disabled = !preset;
  if (preset) document.getElementById('presetNameInput').value = preset.name;
}

async function applySelectedPreset() {
  var preset = selectedPreset();
  if (!preset || editorState.loading || editorState.savingPhotoIds[String(editorState.photoId)]) return;
  var photoId = editorState.photoId;
  var savedRecipeSeq = editorState.savedRecipeSeq;
  var startingKey = recipeKey(editorState.recipe);
  var loadSeq = editorState.loadSeq;
  var button = document.getElementById('applyPresetBtn');
  button.disabled = true;
  try {
    var defs = await VireoBatchEdits.fields();
    var available = preset.fields || defs.filter(function(f) {
      return f.path.indexOf('adjustments.') === 0;
    }).map(function(f) { return f.path; });
    var fields = await VireoBatchEdits.selectFields({
      title: 'Apply preset “' + preset.name + '”', fields: available, available: available,
    });
    if (!fields || photoId !== editorState.photoId || loadSeq !== editorState.loadSeq) return;
    var data = await safeFetch('/api/photos/' + photoId + '/edit-recipe/compose', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({current: recipeForSave(editorState.recipe), recipe: preset.recipe || {}, fields: fields}),
    }, {toast: false});
    if (photoId !== editorState.photoId || loadSeq !== editorState.loadSeq ||
        savedRecipeSeq !== editorState.savedRecipeSeq || editorState.savingPhotoIds[String(photoId)] ||
        startingKey !== recipeKey(editorState.recipe)) {
      showToast('The photo changed while applying the preset. Apply it again to the current edits.', 'warning');
      return;
    }
    editorState.localMaskUpdateSeq++;
    editorState.recipe = cloneRecipe(data.recipe || {});
    if (fields.indexOf('local') !== -1) editorState.localStale = false;
    ensureCrop(editorState.recipe);
    editorState.cropEditing = !recipeForSave(editorState.recipe).crop;
    syncControls();
    markChanged(true);
    showToast('Applied preset "' + preset.name + '" (not saved yet)', 'success');
  } catch (error) {
    showToast(error.message || 'Could not apply preset', 'error');
  } finally { onPresetSelected(); }
}

async function saveCurrentAsPreset() {
  var current = recipeForSave(editorState.recipe);
  var name = document.getElementById('presetNameInput').value.trim();
  if (!name) {
    if (typeof showToast === 'function') {
      showToast('Enter a preset name first', 'error');
    }
    return;
  }
  // The server upserts by name, and selecting a preset auto-fills the name
  // field — without this, tweak-then-Save silently replaces that preset when
  // the user may have meant to create a new one.
  var existing = editorState.presets.find(function(p) { return p.name === name; });
  if (existing && !window.confirm('Overwrite preset "' + name + '" with the current adjustments?')) {
    return;
  }
  try {
    var fields = await VireoBatchEdits.selectFields({
      title: 'Save preset “' + name + '”', action: 'Save preset',
      hint: 'Choose the settings this preset will change. Unchecked settings stay untouched when applying it. Subject and background settings use each destination photo’s own mask.',
    });
    if (!fields) return;
    var data = await safeFetch('/api/edit-presets', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        name: name,
        recipe: current,
        fields: fields,
      }),
    }, {toast: false});
    await loadPresets(data.preset && data.preset.id);
    if (typeof showToast === 'function') {
      showToast('Saved preset "' + data.preset.name + '"', 'success');
    }
  } catch (e) {
    if (typeof showToast === 'function') {
      showToast(e.message || 'Could not save preset', 'error');
    }
  }
}

async function deleteSelectedPreset() {
  var preset = selectedPreset();
  if (!preset) return;
  if (!window.confirm('Delete preset "' + preset.name + '"?')) return;
  try {
    await safeFetch('/api/edit-presets/' + preset.id, {method: 'DELETE'}, {toast: false});
    document.getElementById('presetNameInput').value = '';
    await loadPresets('');
    if (typeof showToast === 'function') {
      showToast('Deleted preset "' + preset.name + '"', 'success');
    }
  } catch (e) {
    if (typeof showToast === 'function') {
      showToast(e.message || 'Could not delete preset', 'error');
    }
  }
}
