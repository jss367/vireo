// Moving the checked folders to another (or a new) workspace: the Move
// Selected bar and the Move Folders modal.
// ES module.

import { byId, fetchActiveWorkspace, fetchWorkspaces } from './state.js';
import { escapeHtml, safeFetch, showToast } from './shared.js';
import { loadWsFolders } from './folders.js';
import { loadWorkspaces } from './workspaces.js';

export function updateMoveBtn() {
  var checked = document.querySelectorAll('.folder-move-cb:checked');
  var bar = document.getElementById('moveFoldersBar');
  if (bar) bar.style.display = checked.length > 0 ? 'block' : 'none';
}

export async function showMoveFoldersModal() {
  var workspaces = await fetchWorkspaces();
  var active = await fetchActiveWorkspace();
  var sel = byId('moveTargetWs');
  sel.innerHTML = '<option value="">-- Select --</option>';
  workspaces.forEach(function(ws) {
    if (ws.id !== active.id) {
      sel.innerHTML += '<option value="' + ws.id + '">' + escapeHtml(ws.name) + '</option>';
    }
  });
  /** @type {HTMLInputElement} */ (byId('moveNewWsName')).value = '';
  byId('moveFoldersError').style.display = 'none';
  byId('moveFoldersModal').classList.add('open');
}

export function hideMoveFoldersModal() {
  byId('moveFoldersModal').classList.remove('open');
}

/** @param {string} message */
function showMoveError(message) {
  var errEl = byId('moveFoldersError');
  errEl.textContent = message;
  errEl.style.display = 'block';
}

export async function doMoveFolders() {
  var checked = document.querySelectorAll('.folder-move-cb:checked');
  var folderIds = Array.from(checked).map(function(cb) { return parseInt(/** @type {HTMLInputElement} */ (cb).value); });
  if (folderIds.length === 0) return;

  var targetWsId = /** @type {HTMLSelectElement} */ (byId('moveTargetWs')).value;
  var newName = /** @type {HTMLInputElement} */ (byId('moveNewWsName')).value.trim();

  if (!targetWsId && !newName) {
    showMoveError('Select an existing workspace or enter a name for a new one');
    return;
  }

  var active = await fetchActiveWorkspace();
  /** @type {{folder_ids: number[], target_workspace_id?: number, new_workspace_name?: string}} */
  var payload = { folder_ids: folderIds };
  if (targetWsId) {
    payload.target_workspace_id = parseInt(targetWsId);
  } else {
    payload.new_workspace_name = newName;
  }

  try {
    var result = /** @type {MoveFoldersResult} */ (await safeFetch('/api/workspaces/' + active.id + '/move-folders', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    }));
    hideMoveFoldersModal();
    showToast('Moved ' + result.folders_moved + ' folder(s), ' + result.detections_moved + ' detection(s), ' + result.pending_changes_moved + ' pending change(s)', 'success');
    loadWsFolders();
    loadWorkspaces();
  } catch(e) {
    showMoveError((e instanceof Error && e.message) || 'Failed to move folders');
  }
}

/** The modal's controls: choosing an existing workspace clears the new name, and typing a name clears the choice. */
export function bindMoveFoldersModal() {
  var target = /** @type {HTMLSelectElement} */ (byId('moveTargetWs'));
  var newName = /** @type {HTMLInputElement} */ (byId('moveNewWsName'));
  target.addEventListener('change', function() { newName.value = ''; });
  newName.addEventListener('input', function() {
    if (newName.value.trim()) target.value = '';
  });
  byId('moveFoldersConfirmBtn').addEventListener('click', function() { doMoveFolders(); });
  byId('moveFoldersCancelBtn').addEventListener('click', hideMoveFoldersModal);
}
