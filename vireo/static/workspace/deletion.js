// Deleting a workspace: an in-page dialog (native confirm() does not work in
// the desktop shell), then the delete request and the list reload.
// ES module; was the classic workspace-deletion.js.

import { safeFetch, showToast } from './shared.js';
import { loadWorkspaces } from './workspaces.js';

/** Ask in a modal dialog; resolves true for Delete, false for Cancel or Esc. @param {string} name */
function confirmDeletion(name) {
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
  return new Promise(function(resolve) {
    /** @param {boolean} value */
    function finish(value) {
      dialog.close();
      dialog.remove();
      if (previousFocus instanceof HTMLElement) previousFocus.focus();
      resolve(value);
    }
    cancel.addEventListener('click', function() { finish(false); });
    remove.addEventListener('click', function() { finish(true); });
    dialog.addEventListener('cancel', function(event) { event.preventDefault(); finish(false); });
    dialog.showModal();
    cancel.focus();
  });
}

/** @param {number} wsId @param {string} name */
export async function deleteWorkspaceConfirm(wsId, name) {
  if (document.getElementById('workspaceDeleteDialog')) return;
  var accepted = await confirmDeletion(name);
  if (!accepted) return;
  try {
    var result = /** @type {{cleanup_errors?: string[]}} */ (await safeFetch('/api/workspaces/' + wsId, {method: 'DELETE'}));
    if ((result.cleanup_errors || []).length) {
      showToast('Workspace deleted. Some saved result files could not be removed; Storage maintenance will retry. See Logs for paths.', 'warning');
    }
    loadWorkspaces();
  } catch(e) {
    // safeFetch already reports the error and leaves the workspace visible.
    return;
  }
}
