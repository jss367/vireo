// The Drift tab: differences between the database and XMP sidecars, resolved
// per row or all at once in either direction.
// ES module.

import { state } from './state.js';
import { byId, paintFirst, postAuditAction, showCheckError, showCheckResult } from './helpers.js';
import { escapeHtml, safeFetch, showToast } from './shared.js';
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
    var unreadable = d.direction === 'sidecar_unreadable';
    // Vireo never saw an unreadable sidecar's keywords; say why instead.
    var xmpCell = unreadable
      ? '<span class="drift-unreadable">Could not read this sidecar: ' + escapeHtml(d.xmp_error || 'unknown error') + '.</span>'
      : escapeHtml(Array.isArray(d.xmp_value) ? d.xmp_value.join(', ') : d.xmp_value);
    html += '<tr><td>' + escapeHtml(d.filename) + '</td><td>' + d.field + '</td>' +
      '<td>' + escapeHtml(dbVal) + '</td><td>' + xmpCell + '</td>' +
      '<td>' + driftActions(d, unreadable) + '</td></tr>';
  });
  html += '</tbody></table>';
  byId('driftContent').innerHTML = html;
}

/** Only the resolutions the server says can apply to this row. @param {AuditDrift} d @param {boolean} unreadable */
function driftActions(d, unreadable) {
  var offered = d.resolutions || ['use_db', 'use_xmp'];
  /** @type {[string, string][]} */ var choices = [['use_db', 'Use DB'], ['use_xmp', 'Use XMP']];
  var buttons = choices.filter(function(pair) {
    return offered.indexOf(pair[0]) >= 0;
  }).map(function(pair) {
    return '<button class="btn btn-secondary" data-photo-id="' + d.photo_id + '" data-direction="' + pair[0] + '">' + pair[1] + '</button>';
  }).join(' ');
  if (!unreadable) return buttons;
  return buttons + (buttons ? ' ' : '') + '<div class="drift-advice">' + escapeHtml(d.advice || '') + '</div>';
}

/** @param {number} photoId @param {string} direction */
async function resolveDrift(photoId, direction) {
  await postAuditAction('/api/audit/resolve', {photo_id: photoId, direction: direction});
  runDriftCheck();
}

/** @param {string} direction */
export async function resolveAll(direction) {
  byId('driftStatus').textContent = 'Resolving...';
  var result = await postAuditAction('/api/audit/resolve-all', {direction: direction});
  // Rows whose sidecar Vireo cannot read are left alone; say so.
  if (result && result.warning) showToast(result.warning, 'warning');
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
