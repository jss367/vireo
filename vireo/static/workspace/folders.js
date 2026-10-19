// The Folders section: the active workspace's folders with their Work Locally
// state and actions, removing a folder from the workspace, and the live
// progress of a running local-folder job.
// ES module.

import { byId, fetchActiveWorkspace, state } from './state.js';
import { escapeAttr, escapeHtml, localFolders, safeFetch } from './shared.js';
import { updateMoveBtn, showMoveFoldersModal } from './move-folders.js';

const SMALL_BUTTON = 'padding:4px 9px;font-size:11px;';

/**
 * A Work Locally action button for one folder; `vireo-work-locally.js` runs
 * the action (bindFolderList routes the click by `data-local-action`).
 *
 * @param {string} action @param {number} folderId @param {string} label
 * @param {string} blockedMessage Why the action is unavailable, or ''
 * @param {string=} extraStyle
 */
function localActionButton(action, folderId, label, blockedMessage, extraStyle) {
  return '<button type="button" data-local-action="' + action + '" data-folder-id="' + folderId + '" class="btn btn-secondary"' +
    (blockedMessage ? ' disabled title="' + escapeAttr(blockedMessage) + '"' : '') +
    ' style="' + SMALL_BUTTON + (extraStyle || '') + (blockedMessage ? 'opacity:.5;' : '') + '">' + label + '</button>';
}

/** @param {WorkLocalJob} localJob @param {WorkLocalFolder} local */
function localJobStatusHtml(localJob, local) {
  var jobProgress = localJob.progress || {};
  var jobPhase = jobProgress.phase || (
    localJob.type === 'work-locally-folder-sync' ? 'Preparing to sync to source…' :
    localJob.type === 'work-locally-folder-discard' ? 'Removing local copy…' :
    'Updating local folder…'
  );
  return '<div class="workspace-folder-job" role="status" aria-live="polite" data-local-folder-job-id="' +
      escapeAttr(localJob.id) + '" data-local-folder-root-id="' +
      escapeAttr(local.root_folder_id) + '">' +
    '<span class="btn-spinner" aria-hidden="true"></span>' +
    '<span class="workspace-folder-job-phase">' + escapeHtml(jobPhase) + '</span>' +
    '<span class="workspace-folder-job-progress">' + escapeHtml(fileCountText(jobProgress)) + '</span>' +
    '<a href="/jobs">View Jobs</a>' +
    '</div>';
}

/** "12 / 40 files" for a job's progress, or '' before it has a total. @param {WorkLocalProgress} progress */
function fileCountText(progress) {
  return progress.total
    ? Number(progress.current || 0).toLocaleString() + ' / ' +
      Number(progress.total).toLocaleString() + ' files'
    : '';
}

/**
 * The second line of a folder that has a local copy: its Local badge, then
 * the running job's progress or the Sync Back / Discard buttons.
 *
 * @param {WorkspaceFolder} f @param {WorkLocalFolder} local
 * @param {WorkLocalJob | null} localJob @param {string} blockedMessage
 */
function localActionsHtml(f, local, localJob, blockedMessage) {
  var sharedCount = (local.workspace_ids || []).length;
  var html = '<div class="workspace-folder-actions">';
  html += '<span style="font-size:10px;color:var(--accent);background:rgba(36,229,202,0.1);padding:2px 7px;border-radius:3px;" title="' +
    (sharedCount > 1 ? 'Shared by ' + sharedCount + ' workspaces' : 'Using managed local storage') + '">Local' +
    (sharedCount > 1 ? ' · ' + sharedCount + ' workspaces' : '') + '</span>';
  if (localJob) {
    html += localJobStatusHtml(localJob, local);
  } else {
    html += localActionButton('sync', f.id, 'Sync Back', blockedMessage);
    html += localActionButton('discard', f.id, 'Discard', blockedMessage, 'color:var(--danger);');
  }
  return html + '</div>';
}

/** The Work Locally control on a folder's first line, by local state. @param {WorkspaceFolder} f @param {WorkLocalFolder | null} local @param {string} blockedMessage */
function localStateHtml(f, local, blockedMessage) {
  if (!local) return '';
  if (local.state === 'remote') return localActionButton('stage', f.id, 'Work Locally', blockedMessage);
  if (local.state === 'staging') return '<span style="font-size:11px;color:var(--warning);">Copying...</span>';
  if (local.state === 'recovery' && local.recovery_kind !== 'sync') {
    return '<span style="font-size:11px;color:var(--warning);">Local copy needs cleanup</span>' +
      localActionButton('discard', f.id, 'Clean Up', blockedMessage, 'color:var(--danger);');
  }
  return '';
}

/** @param {ActiveWorkspace} ws @param {WorkspaceFolder} f */
function folderRowHtml(ws, f) {
  var controller = localFolders();
  var local = controller ? controller.statusFor(f.id) : null;
  var localBlocked = controller && controller.blockingJobFor(f.id);
  var blockedMessage = localBlocked && controller ? controller.blockingMessage(localBlocked) : '';
  var localJob = controller ? controller.jobFor(f.id) : null;
  var displayPath = local && local.display_path ? local.display_path : f.path;
  var hasLocalActions = !!local && local.state !== 'remote' && local.state !== 'staging' &&
    !(local.state === 'recovery' && local.recovery_kind !== 'sync');
  var n = f.workspace_photo_count || 0;
  var html = '<div class="setting-row workspace-folder-row' + (hasLocalActions ? ' workspace-folder-row-stacked' : '') + '">';
  html += '<div class="workspace-folder-main">';
  html += '<label style="display:flex;align-items:center;gap:8px;flex:1;min-width:0;">';
  html += '<input type="checkbox" class="folder-move-cb" value="' + f.id + '" style="accent-color:var(--accent);">';
  html += '<span class="workspace-folder-path" style="font-family:monospace;font-size:13px;" title="' + escapeAttr(displayPath) + '">' + escapeHtml(displayPath) + '</span>';
  html += '<span style="flex-shrink:0;color:var(--text-faint);font-size:12px;">' + n.toLocaleString() + ' photo' + (n === 1 ? '' : 's') + '</span>';
  html += '</label>';
  html += localStateHtml(f, local, blockedMessage);
  html += '<button type="button" data-remove-folder="' + f.id + '" data-workspace-id="' + ws.id + '" style="background:none;border:none;color:var(--danger);cursor:pointer;font-size:16px;" title="Remove from workspace">&times;</button>';
  html += '</div>';
  if (hasLocalActions && local) html += localActionsHtml(f, local, localJob, blockedMessage);
  return html + '</div>';
}

export async function loadWsFolders() {
  try {
    var ws = await fetchActiveWorkspace();
    var container = byId('wsFoldersContent');
    state.foldersById = {};
    if (!ws.folders || ws.folders.length === 0) {
      container.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">No folders in this workspace.</span>';
      return;
    }
    var html = '';
    ws.folders.forEach(function(f) {
      state.foldersById[f.id] = f;
      html += folderRowHtml(ws, f);
    });
    html += '<div id="moveFoldersBar" style="display:none;margin-top:8px;padding-top:8px;border-top:1px solid var(--border-primary);">';
    html += '<button type="button" id="moveSelectedFoldersBtn" style="background:var(--accent);color:var(--accent-text);border:none;border-radius:4px;padding:6px 14px;font-size:12px;cursor:pointer;font-weight:500;">Move Selected to...</button>';
    html += '</div>';
    container.innerHTML = html;
  } catch(e) {}
}

/** @param {number} wsId @param {number} folderId */
async function removeFolderFromWs(wsId, folderId) {
  var f = state.foldersById[folderId];
  var path = f ? f.path : '';
  var n = f ? (f.workspace_photo_count || 0) : 0;
  var msg = 'Remove "' + path + '" from this workspace?\n\n' +
            n + ' photo' + (n === 1 ? '' : 's') + ' in this folder will no longer appear in this workspace, ' +
            'and predictions, collections, and pending changes referencing them will be hidden here too.\n\n' +
            'Photos and the folder are not deleted. You can re-add the folder later to restore visibility.';
  if (!confirm(msg)) return;
  try { await safeFetch('/api/workspaces/' + wsId + '/folders/' + folderId, { method: 'DELETE' }); } catch(e) {}
  loadWsFolders();
}

/** @param {HTMLElement} button */
function runLocalAction(button) {
  var controller = localFolders();
  var folderId = Number(button.dataset.folderId);
  if (!controller) return;
  if (button.dataset.localAction === 'stage') controller.stage(folderId);
  else if (button.dataset.localAction === 'sync') controller.sync(folderId);
  else if (button.dataset.localAction === 'discard') controller.discard(folderId);
}

/**
 * One delegated listener per event on the folder list, which is re-rendered
 * on every load: the move checkboxes, the Work Locally and remove buttons,
 * and Move Selected.
 */
export function bindFolderList() {
  var container = byId('wsFoldersContent');
  container.addEventListener('change', function(event) {
    if (event.target instanceof HTMLInputElement && event.target.classList.contains('folder-move-cb')) updateMoveBtn();
  });
  container.addEventListener('click', function(event) {
    var button = event.target instanceof Element ? event.target.closest('button') : null;
    if (!(button instanceof HTMLButtonElement) || button.disabled) return;
    if (button.dataset.localAction) runLocalAction(button);
    else if (button.dataset.removeFolder) removeFolderFromWs(Number(button.dataset.workspaceId), Number(button.dataset.removeFolder));
    else if (button.id === 'moveSelectedFoldersBtn') showMoveFoldersModal();
  });
}

/** Update a running local-folder job's phase and count in place as it streams. */
export function bindLocalFolderJobProgress() {
  window.addEventListener('vireo:local-folder-job-progress', function(event) {
    var detail = /** @type {CustomEvent<{jobId?: string, progress?: WorkLocalProgress}>} */ (event).detail || {};
    var jobId = String(detail.jobId || '');
    var progress = detail.progress || {};
    var progressRootId = progress.root_folder_id == null
      ? null
      : String(Number(progress.root_folder_id));
    Array.from(document.querySelectorAll('[data-local-folder-job-id]')).forEach(function(node) {
      var status = /** @type {HTMLElement} */ (node);
      if (status.dataset.localFolderJobId !== jobId) return;
      if (progressRootId !== null && status.dataset.localFolderRootId !== progressRootId) return;
      var phase = status.querySelector('.workspace-folder-job-phase');
      var count = status.querySelector('.workspace-folder-job-progress');
      if (phase && progress.phase) phase.textContent = progress.phase;
      if (count) count.textContent = fileCountText(progress);
    });
  });
}
