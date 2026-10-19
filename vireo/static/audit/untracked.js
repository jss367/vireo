// The Untracked tab: files on disk the catalog does not know, imported one at
// a time or all at once, or handed to Process.
// ES module.

import { state } from './state.js';
import { byId, paintFirst, postAuditAction, showCheckError, showCheckResult } from './helpers.js';
import { escapeHtml, safeFetch, showToast } from './shared.js';
import { loadSummary } from './summary.js';

export async function runUntrackedCheck() {
  var status = byId('untrackedStatus');
  var btn = /** @type {HTMLButtonElement} */ (byId('btnUntracked'));
  btn.disabled = true;
  status.textContent = 'Scanning folders for untracked files...';
  status.style.color = '';
  await paintFirst();
  try {
    // Scan roots are derived server-side from the active workspace so
    // the recorded audit run always covers the whole workspace.
    state.untrackedData = await safeFetch('/api/audit/untracked', {}, { toast: false });
    byId('untrackedBadge').textContent = String(state.untrackedData.length);
    renderUntracked();
    showCheckResult(status, state.untrackedData.length, 'untracked file(s) found', 'No untracked files');
    byId('sendToPipelineBtn').style.display =
      state.untrackedData.length > 0 ? '' : 'none';
  } catch(e) {
    showCheckError(status, e);
  }
  btn.disabled = false;
  loadSummary();
}

function renderUntracked() {
  var untrackedContent = byId('untrackedContent');
  if (state.untrackedData.length === 0) {
    untrackedContent.innerHTML = '<div class="empty-msg">No untracked files. All files on disk are in the database.</div>';
    return;
  }
  var html = '<table class="audit-table"><thead><tr><th>File</th><th>Folder</th><th>Actions</th></tr></thead><tbody>';
  state.untrackedData.forEach(function(u, i) {
    var fname = u.path.split('/').pop();
    html += '<tr><td>' + escapeHtml(fname) + '</td><td>' + escapeHtml(u.folder) + '</td>' +
      '<td><button class="btn btn-secondary" data-idx="' + i + '">Import</button></td></tr>';
  });
  html += '</tbody></table>';
  untrackedContent.innerHTML = html;
}

/** @param {string[]} paths */
async function importPaths(paths) {
  var resp = await postAuditAction('/api/audit/import-untracked', {paths: paths});
  if (resp && resp.warning) showToast(resp.warning, 'warning');
  runUntrackedCheck();
}

export async function importAllUntracked() {
  if (state.untrackedData.length === 0) return;
  await importPaths(state.untrackedData.map(function(u) { return u.path; }));
}

export function sendToPipeline() {
  /** @type {string[]} */
  var folders = [];
  state.untrackedData.forEach(function(u) {
    if (u.folder && folders.indexOf(u.folder) === -1) folders.push(u.folder);
  });
  var params = folders.map(function(f) { return 'folder=' + encodeURIComponent(f); }).join('&');
  window.location.href = '/pipeline?' + params;
}

/**
 * The rows' Import buttons, delegated because the table is re-rendered.
 * Paths are looked up from state.untrackedData by row index instead of
 * being written into the markup, so a quote or backslash in a filename
 * cannot break (or inject into) a handler.
 */
export function bindUntrackedTable() {
  byId('untrackedContent').addEventListener('click', function(event) {
    var button = event.target instanceof Element ? event.target.closest('button[data-idx]') : null;
    if (!(button instanceof HTMLElement)) return;
    var entry = state.untrackedData[parseInt(button.dataset.idx || '', 10)];
    if (entry) importPaths([entry.path]);
  });
}
