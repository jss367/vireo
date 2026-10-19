// The All Workspaces list: renaming in place, switching, and deleting.
// ES module.

import { byId, fetchActiveWorkspace, fetchWorkspaces } from './state.js';
import { clearWorkspaceScopedCursors, escapeAttr, safeFetch } from './shared.js';
import { deleteWorkspaceConfirm } from './deletion.js';

const RENAME_INPUT_STYLE = 'background:transparent;border:1px solid transparent;color:var(--text-primary);'
  + 'font-size:14px;padding:2px 6px;border-radius:3px;width:200px;';
const ROW_BUTTON_STYLE = 'background:none;border:1px solid var(--border-primary);'
  + 'border-radius:4px;padding:4px 10px;font-size:11px;cursor:pointer;';

/** @param {WorkspaceListEntry} ws @param {boolean} isActive @param {boolean} canDelete */
function workspaceRowHtml(ws, isActive, canDelete) {
  var html = '<div class="setting-row" style="gap:8px;">';
  html += '  <div class="setting-label" style="display:flex;align-items:center;gap:8px;">';
  html += '    <input type="text" value="' + escapeAttr(ws.name) + '" data-rename-workspace="' + ws.id + '" '
        + 'style="' + RENAME_INPUT_STYLE + '">';
  if (isActive) {
    html += '    <span style="font-size:11px;color:var(--accent);background:rgba(36,229,202,0.1);'
          + 'padding:2px 8px;border-radius:3px;">Active</span>';
  }
  html += '  </div>';
  html += '  <div style="display:flex;align-items:center;gap:6px;">';
  if (!isActive) {
    html += '    <button type="button" data-activate-workspace="' + ws.id + '" '
          + 'style="' + ROW_BUTTON_STYLE + 'color:var(--text-secondary);" '
          + 'title="Switch to this workspace">Switch</button>';
  }
  if (canDelete) {
    html += '    <button type="button" data-delete-workspace="' + ws.id + '" data-workspace-name="' + escapeAttr(ws.name) + '" '
          + 'style="' + ROW_BUTTON_STYLE + 'color:var(--danger);" '
          + 'title="Delete this workspace">Delete</button>';
  }
  html += '  </div>';
  return html + '</div>';
}

export async function loadWorkspaces() {
  try {
    var workspaces = await fetchWorkspaces();
    var active = await fetchActiveWorkspace();
    var container = byId('workspacesContent');

    if (workspaces.length === 0) {
      container.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">No workspaces</span>';
      return;
    }

    container.innerHTML = workspaces.map(function(ws) {
      var isActive = ws.id === active.id;
      return workspaceRowHtml(ws, isActive, !isActive && workspaces.length > 1);
    }).join('');
  } catch(e) {
    byId('workspacesContent').innerHTML = '<span style="color:var(--danger);font-size:13px;">Error loading workspaces</span>';
  }
}

/** @param {number} wsId @param {string} newName */
async function renameWorkspace(wsId, newName) {
  newName = newName.trim();
  if (!newName) return;
  try {
    await safeFetch('/api/workspaces/' + wsId, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ name: newName })
    });
    // Update navbar if this is the active workspace
    var active = await fetchActiveWorkspace();
    if (wsId === active.id) {
      var btn = document.getElementById('wsCurrentBtn');
      if (btn && btn.firstChild) btn.firstChild.textContent = newName + ' ';
      var pageName = document.getElementById('wsPageName');
      if (pageName) pageName.textContent = newName;
    }
  } catch(e) {
    loadWorkspaces();
  }
}

/** @param {number} wsId */
async function activateWorkspace(wsId) {
  try {
    var data = /** @type {{restore_path?: string}} */ (await safeFetch('/api/workspaces/' + wsId + '/activate', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ current_path: window.location.pathname })
    }));
    clearWorkspaceScopedCursors();
    window.location.href = data.restore_path || window.location.pathname;
  } catch(e) {}
}

/** @param {EventTarget | null} target */
function renameInput(target) {
  return target instanceof HTMLInputElement && target.dataset.renameWorkspace ? target : null;
}

/**
 * Delegated listeners on the list, which is re-rendered on every load: rename
 * on change (the name input shows a border only while focused), Switch, and
 * Delete.
 */
export function bindWorkspaceList() {
  var container = byId('workspacesContent');
  container.addEventListener('change', function(event) {
    var input = renameInput(event.target);
    if (input) renameWorkspace(Number(input.dataset.renameWorkspace), input.value);
  });
  container.addEventListener('focusin', function(event) {
    var input = renameInput(event.target);
    if (input) input.style.borderColor = 'var(--border-primary)';
  });
  container.addEventListener('focusout', function(event) {
    var input = renameInput(event.target);
    if (input) input.style.borderColor = 'transparent';
  });
  container.addEventListener('click', function(event) {
    var button = event.target instanceof Element ? event.target.closest('button') : null;
    if (!(button instanceof HTMLButtonElement)) return;
    if (button.dataset.activateWorkspace) activateWorkspace(Number(button.dataset.activateWorkspace));
    else if (button.dataset.deleteWorkspace) {
      deleteWorkspaceConfirm(Number(button.dataset.deleteWorkspace), button.dataset.workspaceName || '');
    }
  });
}
