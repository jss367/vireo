// The Camera Clocks tab: folders whose camera clock was wrong when the photos
// were taken (out of shot order, a time-zone setting changed mid-shoot, a
// clock off from GPS time, or an offset wrong for the GPS position), the
// evidence for each, and fixes applied through the capture-time job.
// ES module.

import { state } from './state.js';
import { byId, errorMessage, paintFirst, showCheckError, showCheckResult } from './helpers.js';
import { escapeHtml, safeFetch } from './shared.js';
import { loadSummary } from './summary.js';

export async function runClockCheck() {
  var status = byId('clockStatus');
  var btn = /** @type {HTMLButtonElement} */ (byId('btnClocks'));
  btn.disabled = true;
  status.textContent = 'Comparing capture times with shot order, GPS time and location...';
  status.style.color = '';
  await paintFirst();
  try {
    state.clockData = /** @type {AuditClockResult} */ (await safeFetch('/api/audit/camera-clocks', {}, { toast: false }));
    var findings = state.clockData.findings;
    byId('clockBadge').textContent = String(findings.length);
    renderClocks();
    showCheckResult(status, findings.length, 'folder(s) with camera-clock problems',
      'No camera-clock problems found');
  } catch(e) {
    showCheckError(status, e);
  }
  btn.disabled = false;
  loadSummary();
}

/** @param {AuditClockFix} fix */
function fixRange(fix) {
  return fix.count === 1 ? fix.first : fix.first + '–' + fix.last;
}

/** @param {AuditClockFix[]} fixes */
function groupFixes(fixes) {
  /** @type {Map<string, {fix: AuditClockFix, index: number}[]>} */
  var groups = new Map();
  fixes.forEach(function(fix, index) {
    var list = groups.get(fix.group) || [];
    list.push({fix: fix, index: index});
    groups.set(fix.group, list);
  });
  return groups;
}

/** @param {AuditClockFix} fix @param {number} findingIndex @param {number} fixIndex @param {boolean} pick */
function fixHtml(fix, findingIndex, fixIndex, pick) {
  return '<div class="clock-fix" data-finding="' + findingIndex + '" data-fix="' + fixIndex + '">' +
    '<div class="clock-fix-label">' + escapeHtml(fix.label) +
    (pick && fix.recommended ? ' <span class="clock-suggested">Suggested</span>' : '') + '</div>' +
    '<div class="clock-fix-why">' + escapeHtml(fix.explanation) + (fix.dependent_fixes || []).map(
      step => '<br>' + escapeHtml(fixRange(step) + ': ' + step.label)).join('') + '</div>' +
    '<div class="clock-fix-actions">' +
      '<button type="button" class="btn btn-secondary" data-action="preview">Preview</button>' +
      '<button type="button" class="btn btn-primary" data-action="apply">Apply…</button>' +
      '<span class="clock-fix-status"></span>' +
    '</div>' +
    '<div class="clock-fix-preview"></div>' +
  '</div>';
}

/** @param {AuditClockFinding} finding @param {number} findingIndex */
function findingHtml(finding, findingIndex) {
  var html = '<div class="clock-finding">' +
    '<div class="clock-folder">' + escapeHtml(finding.folder_path) + '</div>' +
    '<div class="clock-meta">' + escapeHtml(finding.camera || 'Unknown camera') + ' · ' +
      finding.photo_count + ' photo(s)</div>' +
    '<div class="clock-summary">' + escapeHtml(finding.summary) + '</div>' +
    '<ul class="clock-evidence">' + finding.evidence.map(function(line) {
      return '<li>' + escapeHtml(line) + '</li>';
    }).join('') + '</ul>';
  groupFixes(finding.fixes).forEach(function(entries) {
    var first = /** @type {{fix: AuditClockFix}} */ (entries[0]).fix;
    var pick = entries.length > 1;
    html += '<div class="clock-fix-group"><div class="clock-fix-group-title">' +
      first.count + ' photo(s), ' + escapeHtml(fixRange(first)) +
      (first.recorded_offset ? ', recorded at ' + escapeHtml(first.recorded_offset) : '') +
      (pick ? ' — Vireo cannot tell which reading is right; apply at most one:' : '') + '</div>';
    entries.forEach(function(entry) { html += fixHtml(entry.fix, findingIndex, entry.index, pick); });
    html += '</div>';
  });
  return html + '</div>';
}

function renderClocks() {
  var data = /** @type {AuditClockResult} */ (state.clockData);
  var content = byId('clockContent');
  var note = data.location_available ? '' :
    '<div class="clock-note">Vireo cannot look up time zones from GPS positions on this ' +
    'install, so it checked shot order and GPS time only.</div>';
  if (data.findings.length === 0) {
    content.innerHTML = note + '<div class="empty-msg">No camera-clock problems found. Capture ' +
      'times follow shot order and agree with GPS time and location wherever photos have them.</div>';
    return;
  }
  content.innerHTML = note + data.findings.map(findingHtml).join('');
}

/**
 * The fix a row's buttons act on. Rows are rendered from state.clockData and
 * replaced with it, so the indexes always resolve.
 * @param {HTMLElement} row
 */
function fixFor(row) {
  var data = /** @type {AuditClockResult} */ (state.clockData);
  var finding = /** @type {AuditClockFinding} */ (data.findings[Number(row.dataset.finding)]);
  return /** @type {AuditClockFix} */ (finding.fixes[Number(row.dataset.fix)]);
}

/** The capture-time request body for a fix, without the backup choice. @param {AuditClockFix} fix */
function captureTimeBody(fix) {
  return {
    photo_ids: fix.refs,
    expected_state: fix.expected_state,
    mode: fix.target_offset ? 'set_offset' : 'manual',
    target_offset: fix.target_offset,
    shift_minutes: fix.shift_minutes,
  };
}

/** @param {HTMLElement} row */
async function previewFix(row) {
  var out = /** @type {HTMLElement} */ (row.querySelector('.clock-fix-preview'));
  out.textContent = 'Loading preview…';
  try {
    const fix = fixFor(row);
    const tables = await Promise.all([fix, ...(fix.dependent_fixes || [])].map(previewStep));
    out.innerHTML = tables.join('');
  } catch(e) {
    out.textContent = 'Preview failed: ' + errorMessage(e);
  }
}

/** @param {AuditClockFix} step */
async function previewStep(step) {
  var preview = /** @type {AuditCaptureTimePreview} */ (await safeFetch('/api/capture-time/preview', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(captureTimeBody(step)),
  }, { toast: false }));
  var html = '<table class="audit-table"><thead><tr><th>File</th><th>Now</th><th>After</th></tr></thead><tbody>';
  preview.samples.forEach(function(sample) {
    html += '<tr><td>' + escapeHtml(sample.filename) + '</td>' +
      '<td>' + escapeHtml((sample.before_time || 'No capture time') + ' ' + (sample.before_offset || '')) + '</td>' +
      '<td>' + escapeHtml((sample.after_time || 'No capture time') + ' ' + (sample.after_offset || '')) + '</td></tr>';
  });
  html += '</tbody></table>';
  if (preview.count > preview.samples.length) {
    html += '<div class="clock-note">First ' + preview.samples.length + ' of ' + preview.count + ' photos.</div>';
  }
  return html;
}

/** Swap Apply… for an in-page confirmation (native dialogs do nothing in the desktop app). @param {HTMLElement} row */
function confirmFix(row) {
  var fix = fixFor(row);
  var actions = /** @type {HTMLElement} */ (row.querySelector('.clock-fix-actions'));
  var keep = /** @type {HTMLInputElement} */ (byId('clockKeepBackups')).checked;
  row.dataset.keepBackups = String(keep);
  var count = (fix.dependent_fixes || []).reduce((n, step) => n + step.count, fix.count);
  actions.innerHTML = '<span class="clock-confirm-text">Rewrite the capture time in ' + count +
    ' original file(s)' + (keep ? ', keeping an ExifTool backup of each' : ' without backups') + '?</span>' +
    '<button type="button" class="btn btn-primary" data-action="confirm">Rewrite</button>' +
    '<button type="button" class="btn btn-secondary" data-action="cancel">Cancel</button>' +
    '<span class="clock-fix-status"></span>';
}

/** Put the row's Preview / Apply… buttons back. @param {HTMLElement} row */
function resetActions(row) {
  /** @type {HTMLElement} */ (row.querySelector('.clock-fix-actions')).innerHTML =
    '<button type="button" class="btn btn-secondary" data-action="preview">Preview</button>' +
    '<button type="button" class="btn btn-primary" data-action="apply">Apply…</button>' +
    '<span class="clock-fix-status"></span>';
}

/** Poll a capture-time job until it ends, showing progress. @param {string} jobId @param {HTMLElement} status */
async function waitForCaptureTimeJob(jobId, status) {
  while (true) {
    await new Promise(function(r) { setTimeout(r, 1000); });
    var job = /** @type {AuditCaptureTimeJob} */ (await safeFetch('/api/jobs/' + jobId, {}, { toast: false }));
    if (['completed', 'failed', 'cancelled', 'expired'].includes(job.status)) return job;
    var p = job.progress || {};
    if (p.total) status.textContent = 'Rewriting ' + p.current + ' / ' + p.total + '…';
  }
}

/** @param {HTMLElement} row */
async function applyFix(row) {
  var fix = fixFor(row);
  var body = Object.assign(captureTimeBody(fix), {
    keep_backups: row.dataset.keepBackups === 'true',
    dependent_shifts: (fix.dependent_fixes || []).map(captureTimeBody),
  });
  var actions = /** @type {HTMLElement} */ (row.querySelector('.clock-fix-actions'));
  actions.innerHTML = '<span class="clock-fix-status">Starting…</span>';
  var status = /** @type {HTMLElement} */ (actions.querySelector('.clock-fix-status'));
  try {
    var started = /** @type {{job_id: string}} */ (await safeFetch('/api/jobs/capture-time', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    }, { toast: false }));
    var job = await waitForCaptureTimeJob(started.job_id, status);
    var r = job.result;
    if (job.status === 'completed' && r && !r.failed) {
      status.textContent = r.updated + ' rewritten, ' + r.skipped + ' already right. Checking again…';
      await runClockCheck();
      return;
    }
    status.textContent = r
      ? r.updated + ' rewritten, ' + r.failed + ' failed (see Jobs for which and why)'
      : 'Capture-time job ' + job.status + (job.error ? ': ' + job.error : '');
    status.style.color = 'var(--danger)';
  } catch(e) {
    status.textContent = 'Error: ' + errorMessage(e);
    status.style.color = 'var(--danger)';
  }
}

/** The fix buttons, delegated because the list is re-rendered. */
export function bindClockTable() {
  byId('clockContent').addEventListener('click', function(event) {
    var button = event.target instanceof Element ? event.target.closest('button[data-action]') : null;
    var row = button ? button.closest('.clock-fix') : null;
    if (!(button instanceof HTMLElement) || !(row instanceof HTMLElement)) return;
    var action = button.dataset.action;
    if (action === 'preview') previewFix(row);
    else if (action === 'apply') confirmFix(row);
    else if (action === 'cancel') resetActions(row);
    else if (action === 'confirm') applyFix(row);
  });
}
