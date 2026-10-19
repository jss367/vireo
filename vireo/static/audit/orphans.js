// The Orphans tab: catalog entries whose file is gone, removed one at a time
// or all at once.
// ES module.

import { state } from './state.js';
import { byId, paintFirst, postAuditAction, showCheckError, showCheckResult } from './helpers.js';
import { escapeHtml, safeFetch } from './shared.js';
import { loadSummary } from './summary.js';

export async function runOrphanCheck() {
  var status = byId('orphanStatus');
  var btn = /** @type {HTMLButtonElement} */ (byId('btnOrphans'));
  btn.disabled = true;
  status.textContent = 'Checking for database entries with missing files...';
  status.style.color = '';
  await paintFirst();
  try {
    state.orphanData = await safeFetch('/api/audit/orphans', {}, { toast: false });
    byId('orphanBadge').textContent = String(state.orphanData.length);
    renderOrphans();
    showCheckResult(status, state.orphanData.length, 'orphan(s) found', 'No orphans — all entries have files');
  } catch(e) {
    showCheckError(status, e);
  }
  btn.disabled = false;
  loadSummary();
}

function renderOrphans() {
  if (state.orphanData.length === 0) {
    byId('orphanContent').innerHTML = '<div class="empty-msg">No orphan entries found. All database entries have files on disk.</div>';
    return;
  }
  var html = '<table class="audit-table"><thead><tr><th>File</th><th>Last Known Path</th><th>Actions</th></tr></thead><tbody>';
  state.orphanData.forEach(function(o) {
    html += '<tr><td>' + escapeHtml(o.filename) + '</td><td>' + escapeHtml(o.folder_path) + '</td>' +
      '<td><button class="btn btn-danger" data-photo-id="' + o.photo_id + '">Remove</button></td></tr>';
  });
  html += '</tbody></table>';
  byId('orphanContent').innerHTML = html;
}

/** @param {number[]} photoIds */
async function removeOrphans(photoIds) {
  await postAuditAction('/api/audit/remove-orphans', {photo_ids: photoIds});
  runOrphanCheck();
}

export async function removeAllOrphans() {
  if (state.orphanData.length === 0) return;
  await removeOrphans(state.orphanData.map(function(o) { return o.photo_id; }));
}

/** The rows' Remove buttons, delegated because the table is re-rendered. */
export function bindOrphanTable() {
  byId('orphanContent').addEventListener('click', function(event) {
    var button = event.target instanceof Element ? event.target.closest('button[data-photo-id]') : null;
    if (button instanceof HTMLElement) removeOrphans([Number(button.dataset.photoId)]);
  });
}
