// The Sidecars tab: .xmp files whose photo is gone, moved to the Trash one at
// a time or all at once.
// ES module.

import { state } from './state.js';
import { byId, paintFirst, postAuditAction, showCheckError, showCheckResult } from './helpers.js';
import { escapeHtml, safeFetch } from './shared.js';
import { loadSummary } from './summary.js';

export async function runSidecarCheck() {
  var status = byId('sidecarStatus');
  var btn = /** @type {HTMLButtonElement} */ (byId('btnSidecars'));
  btn.disabled = true;
  status.textContent = 'Scanning folders for .xmp files with no photo...';
  status.style.color = '';
  await paintFirst();
  try {
    // Scan roots are derived server-side from the active workspace.
    state.sidecarData = await safeFetch('/api/audit/sidecars', {}, { toast: false });
    byId('sidecarBadge').textContent = String(state.sidecarData.length);
    renderSidecars();
    showCheckResult(status, state.sidecarData.length, 'stray sidecar(s) found', 'No stray sidecars');
  } catch(e) {
    showCheckError(status, e);
  }
  btn.disabled = false;
  loadSummary();
}

function renderSidecars() {
  var sidecarContent = byId('sidecarContent');
  if (state.sidecarData.length === 0) {
    sidecarContent.innerHTML = '<div class="empty-msg">No stray sidecars. Every .xmp file has a matching image next to it.</div>';
    return;
  }
  var html = '<table class="audit-table"><thead><tr><th>Sidecar</th><th>Folder</th><th>Actions</th></tr></thead><tbody>';
  state.sidecarData.forEach(function(s, i) {
    var fname = s.path.split('/').pop();
    html += '<tr><td>' + escapeHtml(fname) + '</td><td>' + escapeHtml(s.folder) + '</td>' +
      '<td><button class="btn btn-danger" data-idx="' + i + '">Move to Trash</button></td></tr>';
  });
  html += '</tbody></table>';
  sidecarContent.innerHTML = html;
}

/** @param {string[]} paths */
async function deleteSidecars(paths) {
  await postAuditAction('/api/audit/delete-sidecars', {paths: paths});
  runSidecarCheck();
}

export async function deleteAllSidecars() {
  if (state.sidecarData.length === 0) return;
  await deleteSidecars(state.sidecarData.map(function(s) { return s.path; }));
}

/** The rows' Move to Trash buttons; same row-index indirection as the Untracked tab. */
export function bindSidecarTable() {
  byId('sidecarContent').addEventListener('click', function(event) {
    var button = event.target instanceof Element ? event.target.closest('button[data-idx]') : null;
    if (!(button instanceof HTMLElement)) return;
    var entry = state.sidecarData[parseInt(button.dataset.idx || '', 10)];
    if (entry) deleteSidecars([entry.path]);
  });
}
