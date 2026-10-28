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

/** @param {'copy'|'snapshot'} kind */
async function createEditorVersion(kind) {
  if (!editorState.photoId || editorState.loading || maskBrush.busy || maskBrush.stroke) return;
  var name = window.prompt(kind === 'copy' ? 'Name this virtual copy' : 'Name this snapshot', '');
  if (name === null) return;
  var photoId = editorState.photoId, seq = editorState.loadSeq;
  finishEditorHistoryGesture();
  setEditorLoading(true);
  try {
    /** @type {{version:EditorVersion}} */ var result = await safeFetch('/api/photos/' + photoId + '/versions', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({kind: kind, name: name.trim() || undefined, recipe: recipeForSave(editorState.recipe)}),
    }, {toast: false});
    if (editorState.loadSeq !== seq) return;
    if (kind === 'copy') await loadPhoto(photoId, {versionId: result.version.id});
    else { await loadEditorVersions(); showToast('Snapshot saved', 'success'); }
    if (window.renderHistoryControls) window.renderHistoryControls();
  } catch (error) { showToast(editorErrorMessage(error) || 'Could not create version', 'error'); }
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

/** @param {EditorVersion|null} version */
function editorVersionCard(version) {
  var id = version ? version.id : null;
  var current = id === editorState.versionId;
  var card = document.createElement('div');
  card.className = 'editor-version-card' + (current ? ' current' : '');
  var image = document.createElement('img'); image.alt = ''; image.loading = 'lazy';
  image.src = '/photos/' + editorState.photoId + '/edit-preview?size=256&apply_crop=1' +
    (version ? '&version_id=' + version.id + '&v=' + version.revision : '&v=' + editorState.savedRecipeSeq);
  image.addEventListener('error', function() {
    var placeholder = document.createElement('small');
    placeholder.className = 'editor-version-placeholder'; placeholder.textContent = 'Preview unavailable';
    image.replaceWith(placeholder);
  }, {once: true});
  card.appendChild(image);
  var info = document.createElement('div');
  var name = document.createElement('strong'); name.textContent = version ? version.name : 'Original';
  info.appendChild(name);
  var kind = document.createElement('small');
  kind.textContent = current ? 'Editing' : (version && version.kind === 'snapshot' ? 'Snapshot' : 'Editable');
  info.appendChild(kind); card.appendChild(info);
  var actions = document.createElement('div'); actions.className = 'editor-version-actions';
  var select = versionButton(version && version.kind === 'snapshot' ? 'Restore' : 'Edit', function() {
    if (version && version.kind === 'snapshot') {
      if (isEditorDirty() && !window.confirm('Replace unsaved edits with this snapshot?')) return;
      restoreRecipe(version.recipe);
    } else switchEditorVersion(id);
  });
  select.disabled = current; actions.appendChild(select);
  if (version) {
    actions.appendChild(versionButton('Rename', function() { changeEditorVersion(version, false); }));
    actions.appendChild(versionButton('Delete', function() { changeEditorVersion(version, true); }));
  }
  card.appendChild(actions); return card;
}

async function loadEditorVersions() {
  var list = document.getElementById('editorVersions'); if (!list || !editorState.photoId) return;
  var photoId = editorState.photoId, seq = editorState.loadSeq, requestSeq = ++editorVersionsRequestSeq;
  list.textContent = 'Loading versions…';
  try {
    /** @type {{versions:EditorVersion[]}} */ var result = await safeFetch('/api/photos/' + photoId + '/versions', {}, {toast: false});
    if (seq !== editorState.loadSeq || requestSeq !== editorVersionsRequestSeq) return;
    list.replaceChildren(editorVersionCard(null));
    for (var version of result.versions) list.appendChild(editorVersionCard(version));
  } catch (_) {
    if (seq === editorState.loadSeq && requestSeq === editorVersionsRequestSeq) list.textContent = 'Could not load versions. Reload this photo to try again.';
  }
}
