// The Drift tab: differences between the database and XMP sidecars, resolved
// per row or all at once in either direction.
// ES module.

import { state } from './state.js';
import { byId, paintFirst, postAuditAction, showCheckError, showCheckResult } from './helpers.js';
import { escapeHtml, safeFetch } from './shared.js';
import { loadSummary } from './summary.js';

export async function runDriftCheck() {
  var status = byId('driftStatus');
  var btn = /** @type {HTMLButtonElement} */ (byId('btnDrift'));
  btn.disabled = true;
  status.textContent = 'Scanning all photos for drift between database and XMP files...';
  status.style.color = '';

  // Let the UI update before the blocking fetch
  await paintFirst();

  try {
    state.driftData = await safeFetch('/api/audit/drift', {}, { toast: false });
    byId('driftBadge').textContent = String(state.driftData.length);
    renderDrift();
    showCheckResult(status, state.driftData.length, 'drift(s) found', 'No drift — everything in sync');
  } catch(e) {
    showCheckError(status, e);
  }
  btn.disabled = false;
  loadSummary();
}

function renderDrift() {
  if (state.driftData.length === 0) {
    byId('driftContent').innerHTML = '<div class="empty-msg">No drift detected. Database and XMP files are in sync.</div>';
    return;
  }
  var html = '<table class="audit-table"><thead><tr><th>File</th><th>Field</th><th>Database</th><th>XMP</th><th>Actions</th></tr></thead><tbody>';
  state.driftData.forEach(function(d) {
    var dbVal = Array.isArray(d.db_value) ? d.db_value.join(', ') : d.db_value;
    var xmpVal = Array.isArray(d.xmp_value) ? d.xmp_value.join(', ') : d.xmp_value;
    html += '<tr><td>' + escapeHtml(d.filename) + '</td><td>' + d.field + '</td>' +
      '<td>' + escapeHtml(dbVal) + '</td><td>' + escapeHtml(xmpVal) + '</td>' +
      '<td><button class="btn btn-secondary" data-photo-id="' + d.photo_id + '" data-direction="use_db">Use DB</button> ' +
      '<button class="btn btn-secondary" data-photo-id="' + d.photo_id + '" data-direction="use_xmp">Use XMP</button></td></tr>';
  });
  html += '</tbody></table>';
  byId('driftContent').innerHTML = html;
}

/** @param {number} photoId @param {string} direction */
async function resolveDrift(photoId, direction) {
  await postAuditAction('/api/audit/resolve', {photo_id: photoId, direction: direction});
  runDriftCheck();
}

/** @param {string} direction */
export async function resolveAll(direction) {
  byId('driftStatus').textContent = 'Resolving...';
  await postAuditAction('/api/audit/resolve-all', {direction: direction});
  runDriftCheck();
}

/** The rows' Use DB / Use XMP buttons, delegated because the table is re-rendered. */
export function bindDriftTable() {
  byId('driftContent').addEventListener('click', function(event) {
    var button = event.target instanceof Element ? event.target.closest('button[data-direction]') : null;
    if (button instanceof HTMLElement) {
      resolveDrift(Number(button.dataset.photoId), button.dataset.direction || '');
    }
  });
}
