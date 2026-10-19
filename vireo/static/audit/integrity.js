// The Integrity tab: re-hashing every file against its import-time hash
// (the verify-hashes job), the flagged files, and accepting a file's current
// content as its new baseline.
// ES module.

import { HASH_STATUS_LABELS, state } from './state.js';
import { byId, errorMessage, postAuditAction, timeAgo } from './helpers.js';
import { escapeAttr, escapeHtml, safeFetch } from './shared.js';
import { loadSummary } from './summary.js';

export async function loadIntegrity() {
  try {
    state.integrityData = await safeFetch('/api/audit/integrity', {}, { toast: false });
    renderIntegrity();
  } catch(e) {
    var el = byId('integrityContent');
    el.innerHTML = '<div class="empty-msg"></div>';
    /** @type {ChildNode} */ (el.firstChild).textContent = 'Error loading integrity state: ' + errorMessage(e);
  }
}

function renderIntegrity() {
  var data = /** @type {AuditIntegrity} */ (state.integrityData);
  var stats = data.stats;
  var flagged = data.flagged;
  byId('integrityBadge').textContent = String(flagged.length);

  var statParts = [];
  statParts.push(stats.checked + ' of ' + stats.total + ' photos verified against their import-time hash');
  if (stats.unchecked > 0) statParts.push(stats.unchecked + ' never verified');
  byId('integrityStats').textContent = statParts.join(' · ');

  if (flagged.length === 0) {
    var msg = stats.checked === 0
      ? 'No photos have been hash-verified yet. Click "Verify File Hashes" to re-hash every file and compare against the hash recorded at import.'
      : 'No integrity problems found in the last verification.';
    byId('integrityContent').innerHTML = '<div class="empty-msg">' + msg + '</div>';
    return;
  }
  var html = '<table class="audit-table"><thead><tr><th>File</th><th>Folder</th><th>Status</th><th>Checked</th><th>Actions</th></tr></thead><tbody>';
  flagged.forEach(function(f) {
    var st = HASH_STATUS_LABELS[f.hash_status] || {label: f.hash_status, cls: '', hint: ''};
    html += '<tr><td>' + escapeHtml(f.filename) + '</td><td>' + escapeHtml(f.folder_path) + '</td>' +
      '<td class="' + st.cls + '" title="' + escapeAttr(st.hint) + '">' + escapeHtml(st.label) + '</td>' +
      '<td>' + timeAgo(f.hash_checked_at) + '</td>' +
      '<td><button class="btn btn-secondary" title="Store the file\'s current content as the new baseline hash" ' +
      'data-photo-id="' + f.photo_id + '">Accept Current File</button></td></tr>';
  });
  html += '</tbody></table>';
  byId('integrityContent').innerHTML = html;
}

/** @param {number} photoId */
async function acceptHash(photoId) {
  await postAuditAction('/api/audit/accept-hash', {photo_ids: [photoId]});
  loadIntegrity();
  loadSummary();
}

/** Poll the verify-hashes job once a second until it ends, showing its progress. @param {string} jobId @param {HTMLElement} status */
async function waitForVerifyJob(jobId, status) {
  while (true) {
    await new Promise(function(r) { setTimeout(r, 1000); });
    var job = /** @type {AuditVerifyJob} */ (await safeFetch('/api/jobs/' + jobId, {}, { toast: false }));
    if (['completed', 'failed', 'cancelled', 'expired'].includes(job.status)) return job;
    var p = job.progress || {};
    if (p.total) {
      status.textContent = 'Hashing ' + p.current + ' / ' + p.total +
        (p.current_file ? ' — ' + p.current_file : '');
    }
  }
}

/** @param {AuditVerifyJob} job @param {HTMLElement} status */
function showVerifyResult(job, status) {
  if (job.status === 'completed' && job.result) {
    var r = job.result;
    var problems = (r.modified || 0) + (r.corrupt || 0) + (r.unreadable || 0);
    var parts = [r.checked + ' file(s) checked'];
    if (r.baselined) parts.push(r.baselined + ' baselined (no prior hash)');
    if (r.missing) parts.push(r.missing + ' missing (see Orphans)');
    parts.push(problems > 0 ? problems + ' problem(s) found' : 'no problems');
    status.textContent = parts.join(', ');
    status.style.color = problems > 0 ? 'var(--danger)' : 'var(--accent)';
  } else {
    status.textContent = 'Verification ' + job.status;
    status.style.color = 'var(--warning)';
  }
}

export async function runHashVerify() {
  var status = byId('integrityStatus');
  var btn = /** @type {HTMLButtonElement} */ (byId('btnVerify'));
  btn.disabled = true;
  status.textContent = 'Starting hash verification...';
  status.style.color = '';
  try {
    var resp = /** @type {{job_id: string}} */ (await safeFetch('/api/jobs/verify-hashes', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: '{}',
    }, { toast: false }));
    showVerifyResult(await waitForVerifyJob(resp.job_id, status), status);
  } catch(e) {
    status.textContent = 'Error: ' + errorMessage(e);
    status.style.color = 'var(--danger)';
  }
  btn.disabled = false;
  await loadIntegrity();
  loadSummary();
}

/** The flagged rows' Accept Current File buttons, delegated because the table is re-rendered. */
export function bindIntegrityTable() {
  byId('integrityContent').addEventListener('click', function(event) {
    var button = event.target instanceof Element ? event.target.closest('button[data-photo-id]') : null;
    if (button instanceof HTMLElement) acceptHash(Number(button.dataset.photoId));
  });
}
