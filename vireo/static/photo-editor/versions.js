// Virtual copies keep their own saved recipe; snapshots restore into the current edit.
var editorVersionsRequestSeq = 0;

/** @param {number} photoId @param {string|null} versionId */
function editorRecipeUrl(photoId, versionId) {
  return '/api/photos/' + photoId + (versionId ? '/versions/' + versionId : '/edit-recipe');
}

/** @param {string|null} versionId */
async function switchEditorVersion(versionId) {
  if (editorState.loading || Object.keys(editorState.savingPhotoIds).length || versionId === editorState.versionId) return;
  if (isEditorDirty() && !window.confirm('Discard unsaved edits before switching versions?')) return;
  if (editorState.photoId) await loadPhoto(editorState.photoId, {versionId: versionId});
}

async function saveEditorSnapshot() {
  if (!editorState.photoId || editorState.loading || maskBrush.busy || maskBrush.stroke || Object.keys(editorState.savingPhotoIds).length) return;
  var name = window.prompt('Name this snapshot', '');
  if (name === null) return;
  var photoId = editorState.photoId, seq = editorState.loadSeq;
  finishEditorHistoryGesture();
  setEditorLoading(true);
  try {
    await safeFetch('/api/photos/' + photoId + '/versions', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({kind: 'snapshot', name: name.trim() || undefined, recipe: recipeForSave(editorState.recipe)}),
    }, {toast: false});
    if (editorState.loadSeq !== seq) return;
    await loadEditorVersions();
    showToast('Snapshot saved', 'success');
    if (window.renderHistoryControls) window.renderHistoryControls();
  } catch (error) { showToast(editorErrorMessage(error) || 'Could not save snapshot', 'error'); }
  finally { if (editorState.loadSeq === seq) setEditorLoading(false); }
}

/** @param {EditorVersion} version @param {boolean} remove */
async function changeEditorVersion(version, remove) {
  if (!editorState.photoId || editorState.loading || Object.keys(editorState.savingPhotoIds).length) return;
  if (remove && !window.confirm('Delete “' + version.name + '”? The original file is kept. You can undo this in Edit History.')) return;
  var name = remove ? null : window.prompt('Version name', version.name);
  if (!remove && name === null) return;
  var photoId = editorState.photoId, seq = editorState.loadSeq;
  setEditorLoading(true);
  try {
    /** @type {{version:EditorVersion}} */ var result = await safeFetch(editorRecipeUrl(photoId, version.id), {
      method: remove ? 'DELETE' : 'PUT', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({revision: version.revision, name: name}),
    }, {toast: false});
    if (seq !== editorState.loadSeq) return;
    if (version.id === editorState.versionId) {
      if (remove) { await loadPhoto(photoId); return; }
      editorState.version = result.version;
      var subhead = document.getElementById('editorSubhead');
      if (subhead) subhead.textContent = (subhead.textContent || '').split(' · ')[0] + ' · ' + result.version.name;
    }
    await loadEditorVersions();
    if (window.renderHistoryControls) window.renderHistoryControls();
  } catch (error) { showToast(editorErrorMessage(error) || 'Could not change version', 'error'); }
  finally { if (seq === editorState.loadSeq) setEditorLoading(false); }
}

/** @param {string} label @param {() => unknown} action */
function versionButton(label, action) {
  var button = document.createElement('button');
  button.type = 'button'; button.className = 'editor-btn'; button.textContent = label;
  button.addEventListener('click', action);
  return button;
}

/** @param {string} id @param {string} title @param {EditorVersion[]} versions @param {string} firstOption */
function editorVersionPicker(id, title, versions, firstOption) {
  var row = document.createElement('div'); row.className = 'editor-version-picker';
  var label = document.createElement('label'); label.htmlFor = id; label.textContent = title;
  var select = document.createElement('select'); select.id = id; select.className = 'advanced-select';
  select.add(new Option(firstOption, ''));
  for (var version of versions) select.add(new Option(version.name, version.id));
  row.append(label, select);
  return {row: row, select: select};
}

/** @param {HTMLElement} row @param {EditorVersion|null} version */
function appendEditorVersionActions(row, version) {
  if (!version) return;
  var actions = document.createElement('div'); actions.className = 'editor-version-actions';
  actions.appendChild(versionButton('Rename', function() { changeEditorVersion(version, false); }));
  actions.appendChild(versionButton('Delete', function() { changeEditorVersion(version, true); }));
  row.appendChild(actions);
}

/** @param {EditorVersion[]} versions */
function editorCopyPicker(versions) {
  var picker = editorVersionPicker('editorVersionSelect', 'Editing', versions, 'Original');
  picker.select.value = editorState.versionId || '';
  picker.select.addEventListener('change', function() {
    var versionId = picker.select.value || null;
    // Keep the label truthful when switching is blocked or the discard prompt is canceled.
    picker.select.value = editorState.versionId || '';
    switchEditorVersion(versionId);
  });
  appendEditorVersionActions(picker.row, versions.find(function(version) { return version.id === editorState.versionId; }) || null);
  return picker.row;
}

/** @param {EditorVersion[]} versions */
function editorSnapshotPicker(versions) {
  var picker = editorVersionPicker('editorSnapshotSelect', 'Snapshots', versions, 'Choose a snapshot…');
  var actions = document.createElement('div'); actions.className = 'editor-version-actions';
  picker.row.appendChild(actions);
  picker.select.addEventListener('change', function() {
    actions.replaceChildren();
    const version = versions.find(function(item) { return item.id === picker.select.value; });
    if (!version) return;
    actions.appendChild(versionButton('Restore', function() {
      if (editorState.loading || Object.keys(editorState.savingPhotoIds).length) return;
      if (isEditorDirty() && !window.confirm('Replace unsaved edits with this snapshot?')) return;
      restoreRecipe(version.recipe);
    }));
    appendEditorVersionActions(actions, version);
  });
  return picker.row;
}

async function loadEditorVersions() {
  var list = document.getElementById('editorVersions'); if (!list || !editorState.photoId) return;
  var photoId = editorState.photoId, seq = editorState.loadSeq, requestSeq = ++editorVersionsRequestSeq;
  list.textContent = 'Loading versions…';
  try {
    /** @type {{versions:EditorVersion[]}} */ var result = await safeFetch('/api/photos/' + photoId + '/versions', {}, {toast: false});
    if (seq !== editorState.loadSeq || requestSeq !== editorVersionsRequestSeq) return;
    list.replaceChildren(editorCopyPicker(result.versions.filter(function(version) { return version.kind === 'copy'; })));
    var snapshots = result.versions.filter(function(version) { return version.kind === 'snapshot'; });
    if (snapshots.length) list.appendChild(editorSnapshotPicker(snapshots));
  } catch (_) {
    if (seq === editorState.loadSeq && requestSeq === editorVersionsRequestSeq) list.textContent = 'Could not load versions. Reload this photo to try again.';
  }
}
