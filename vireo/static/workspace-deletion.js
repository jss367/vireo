/* Workspace deletion uses an in-page dialog in browsers and the desktop shell. */
async function deleteWorkspaceConfirm(wsId, name) {
  if (document.getElementById('workspaceDeleteDialog')) return;
  var dialog = document.createElement('dialog');
  dialog.id = 'workspaceDeleteDialog';
  dialog.className = 'modal';
  dialog.setAttribute('aria-labelledby', 'workspaceDeleteTitle');
  var title = document.createElement('h3');
  title.id = 'workspaceDeleteTitle';
  title.textContent = 'Delete workspace “' + name + '”?';
  var explanation = document.createElement('p');
  explanation.textContent = 'This removes its collections, review decisions, pending changes, job history, and saved processing results. Shared photos, keywords, and photo analysis remain.';
  var actions = document.createElement('div');
  actions.className = 'modal-actions';
  var cancel = document.createElement('button');
  cancel.className = 'modal-btn';
  cancel.textContent = 'Cancel';
  var remove = document.createElement('button');
  remove.id = 'workspaceDeleteConfirm';
  remove.className = 'modal-btn modal-btn-primary';
  remove.textContent = 'Delete workspace';
  actions.append(cancel, remove);
  dialog.append(title, explanation, actions);
  document.body.appendChild(dialog);
  var previousFocus = document.activeElement;
  var accepted = await new Promise(function(resolve) {
    function finish(value) {
      dialog.close();
      dialog.remove();
      if (previousFocus) previousFocus.focus();
      resolve(value);
    }
    cancel.onclick = function() { finish(false); };
    remove.onclick = function() { finish(true); };
    dialog.oncancel = function(event) { event.preventDefault(); finish(false); };
    dialog.showModal();
    cancel.focus();
  });
  if (!accepted) return;
  try {
    var result = await safeFetch('/api/workspaces/' + wsId, {method: 'DELETE'});
    if ((result.cleanup_errors || []).length) {
      showToast('Workspace deleted. Some saved result files could not be removed; Storage maintenance will retry. See Logs for paths.', 'warning');
    }
    loadWorkspaces();
  } catch(e) {
    // safeFetch already reports the error and leaves the workspace visible.
    return;
  }
}
