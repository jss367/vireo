// The tabs and the archive-integrity summary banner: its traffic light,
// headline, one chip per check, and Run All Checks.
// ES module.

import { CHECK_LABELS } from './state.js';
import { byId, errorMessage, timeAgo } from './helpers.js';
import { safeFetch } from './shared.js';
import { runDriftCheck } from './drift.js';
import { runOrphanCheck } from './orphans.js';
import { runUntrackedCheck } from './untracked.js';
import { runSidecarCheck } from './sidecars.js';
import { runHashVerify } from './integrity.js';

/** @param {string} name */
export function switchTab(name) {
  document.querySelectorAll('.tab').forEach(function(node) {
    var t = /** @type {HTMLElement} */ (node);
    t.classList.toggle('active', t.dataset.tab === name);
  });
  document.querySelectorAll('.tab-panel').forEach(function(p) {
    p.classList.toggle('active', p.id === 'panel-' + name);
  });
}

export async function loadSummary() {
  try {
    var s = /** @type {AuditSummary} */ (await safeFetch('/api/audit/summary', {}, { toast: false }));
    renderSummary(s);
  } catch(e) {
    byId('summaryTitle').textContent = 'Could not load audit summary: ' + errorMessage(e);
  }
}

/** The banner headline for the summary's status. @param {AuditSummary} s */
function summaryHeadline(s) {
  if (s.status === 'intact') return 'Archive intact — all checks ran and found nothing';
  if (s.status === 'stale') {
    return 'Checks clean, but ' + s.integrity.unchecked + ' photo(s) have never been hash-verified';
  }
  if (s.status === 'problems') {
    var parts = [];
    if (s.missing_folders > 0) {
      parts.push(s.missing_folders + ' folder(s) missing — ' +
        s.missing_folder_photos + ' photo(s) unreachable and excluded from all checks');
    }
    if (s.problem_count > 0) {
      parts.push(s.problem_count + ' problem(s) found — see tabs below');
    }
    return parts.join(' · ');
  }
  var notRun = [];
  for (var k of Object.keys(s.checks)) {
    if (!s.checks[k]) notRun.push(CHECK_LABELS[k]);
  }
  return 'Not yet verified — never run: ' + notRun.join(', ');
}

/** @param {AuditSummary} s */
function renderSummary(s) {
  byId('summaryLight').className = 'summary-light ' + s.status;
  byId('summaryTitle').textContent = summaryHeadline(s);

  // All chip content is from fixed label maps, counts, and timeAgo()
  // output — no user-controlled strings. Each chip names its tab in
  // data-tab; bindSummaryChips switches to it.
  var chips = '';
  Object.keys(s.checks).forEach(function(k) {
    var c = s.checks[k];
    if (!c) {
      chips += '<span class="summary-chip chip-never" data-tab="' + k + '">' +
        CHECK_LABELS[k] + ': never run</span>';
    } else {
      var cls = c.problem_count > 0 ? 'chip-problem' : 'chip-clean';
      var count = c.problem_count > 0 ? c.problem_count : '✓';
      chips += '<span class="summary-chip ' + cls + '" data-tab="' + k + '">' +
        CHECK_LABELS[k] + ': <span class="chip-count">' + count + '</span>' +
        '<span class="chip-time">' + timeAgo(c.ran_at) + '</span></span>';
    }
  });
  byId('summaryChips').innerHTML = chips;

  var notes = [];
  if (s.integrity.total > 0) {
    notes.push(s.integrity.checked + ' of ' + s.integrity.total +
      ' photos hash-verified');
  }
  byId('summaryNote').textContent = notes.join(' · ');
}

/** The tab bar and the summary chips switch tabs; the chips are re-rendered, so that listener is delegated. */
export function bindTabs() {
  document.querySelectorAll('.tabs .tab').forEach(function(node) {
    var tab = /** @type {HTMLElement} */ (node);
    tab.addEventListener('click', function() { switchTab(tab.dataset.tab || ''); });
  });
  byId('summaryChips').addEventListener('click', function(event) {
    var chip = event.target instanceof Element ? event.target.closest('.summary-chip') : null;
    if (chip instanceof HTMLElement && chip.dataset.tab) switchTab(chip.dataset.tab);
  });
}

export async function runAllChecks() {
  var btn = /** @type {HTMLButtonElement} */ (byId('btnRunAll'));
  var note = byId('summaryNote');
  btn.disabled = true;
  try {
    note.textContent = 'Running drift check…';
    await runDriftCheck();
    note.textContent = 'Running orphan check…';
    await runOrphanCheck();
    note.textContent = 'Running untracked check…';
    await runUntrackedCheck();
    note.textContent = 'Running sidecar check…';
    await runSidecarCheck();
    note.textContent = 'Verifying file hashes…';
    await runHashVerify();
  } finally {
    btn.disabled = false;
  }
  await loadSummary();
}
