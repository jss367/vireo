// Scanning the card and following any job's stream (scan, verify, delete),
// and finishing a job when it completes.
// ES module.

import { cardCleanupRenderDeleteResult, cardCleanupResetPreview } from './delete.js';
import { cardCleanupLoadManifest } from './manifest.js';
import { cardCleanupErrorText, cardCleanupSetBusy, cardCleanupSetError, state } from './state.js';
import { cardCleanupFinishVerification } from './verification.js';

export async function cardCleanupStartScan() {
  if (state.jobId) return;  // a run is already streaming
  const source = (/** @type {HTMLInputElement} */ (document.getElementById('card-cleanup-source')).value || '').trim();
  const recursive = /** @type {HTMLInputElement} */ (document.getElementById('card-cleanup-recursive')).checked;
  if (!source) {
    cardCleanupSetError('Choose the card folder to check.');
    return;
  }
  cardCleanupSetError('');
  cardCleanupResetPreview();
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-result')).style.display = 'none';
  cardCleanupSetBusy(true);
  try {
    const resp = await fetch('/api/card-cleanup/scan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ source, recursive }),
    });
    const data = await resp.json();
    // The 400s here are written for the user (archive overlap, unreadable
    // path) — show the server's own words rather than a generic failure.
    if (!resp.ok) throw new Error(data.error || 'scan failed to start');
    state.scanJobId = data.job_id;
    cardCleanupWatchJob(data.job_id, 'scan', 'Starting scan…');
  } catch (e) {
    cardCleanupSetBusy(false);
    cardCleanupSetError(cardCleanupErrorText(e));
  }
}

/** @param {string} jobId @param {string} kind @param {string} startingPhase */
export function cardCleanupWatchJob(jobId, kind, startingPhase) {
  state.jobId = jobId;
  state.finishedJobId = null;
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-progress')).style.display = '';
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-phase')).textContent = startingPhase;
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-progress-fill')).style.width = '0%';
  const cancelBtn = /** @type {HTMLButtonElement} */ (document.getElementById('card-cleanup-cancel-btn'));
  cancelBtn.style.display = '';
  cancelBtn.disabled = false;
  cancelBtn.textContent = 'Cancel';
  if (state.es) { state.es.close(); state.es = null; }
  const es = new EventSource('/api/jobs/' + encodeURIComponent(jobId) + '/stream');
  state.es = es;
  es.addEventListener('progress', (evt) => {
    try { cardCleanupRenderProgress(JSON.parse(/** @type {MessageEvent<string>} */ (evt).data)); } catch (e) { /* ignore */ }
  });
  es.addEventListener('complete', () => {
    cardCleanupCloseStream();
    cardCleanupFinishJob(jobId, kind);
  });
  es.addEventListener('error', () => {
    // Stream dropped (server restart, network) — fall back to polling the
    // job rather than pretending progress continues.
    cardCleanupCloseStream();
    cardCleanupFinishJob(jobId, kind);
  });
}

export function cardCleanupCloseStream() {
  if (state.es) {
    state.es.close();
    state.es = null;
  }
}

/** @param {{phase?: string, current?: number, total?: number, current_file?: string}} data */
export function cardCleanupRenderProgress(data) {
  const parts = [];
  if (data.phase) parts.push(data.phase);
  if (data.total) {
    parts.push(Number(data.current || 0).toLocaleString() + ' / ' +
      Number(data.total).toLocaleString());
  }
  if (data.current_file) parts.push(data.current_file);
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-phase')).textContent = parts.join(' · ');
  const pct = data.total ? Math.round(100 * (data.current || 0) / data.total) : 0;
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-progress-fill')).style.width = pct + '%';
}

/** @param {string} jobId @param {string} kind */
export async function cardCleanupFinishJob(jobId, kind) {
  // 'complete' and 'error' can both fire for one job; only finish once.
  if (state.finishedJobId === jobId) return;
  state.finishedJobId = jobId;
  let job = null;
  // The stream is already gone by the time we poll, so say what the page
  // is actually doing instead of leaving the last SSE line under a bar
  // that has stopped moving.
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-phase')).textContent =
    'Checking job status…';
  // Poll until the job actually reaches a terminal state (CodeRabbit
  // review): a transient stream drop used to fall into a 30-second poll
  // that then gave up — re-enabling the buttons while the job kept
  // running on the server, exactly the orphaning the busy discipline
  // exists to prevent. Scans and audits here legitimately run for many
  // minutes, so poll indefinitely — 1s for the first 30 attempts, then
  // 10s — and stop only on a terminal status or when the server says it
  // no longer knows the job (a restart pruned it: several consecutive
  // 404s), which gets the honest lost-track copy below.
  let notFound = 0;
  for (let i = 0; ; i++) {
    // A poll that throws (server restarting, network dropped — the very
    // conditions that killed the stream) must not escape as an unhandled
    // rejection: that would skip the cleanup below and leave a frozen bar,
    // a live Cancel button pointing at a stale job, and a disabled Scan
    // button until reload. Treat it as one failed attempt and keep going.
    try {
      const resp = await fetch('/api/jobs/' + encodeURIComponent(jobId));
      if (resp.ok) {
        notFound = 0;
        const polled = await resp.json();
        if (polled.status && polled.status !== 'running'
            && polled.status !== 'queued'
            && polled.status !== 'pausing' && polled.status !== 'paused') {
          job = polled;
          break;
        }
        if (polled.progress) cardCleanupRenderProgress(polled.progress);
      } else if (resp.status === 404) {
        notFound += 1;
        if (notFound >= 3) break;
      }
    } catch (e) { /* retry below */ }
    await new Promise(r => setTimeout(r, i < 30 ? 1000 : 10000));
  }
  state.jobId = null;
  /** @type {HTMLButtonElement} */ (document.getElementById('card-cleanup-cancel-btn')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-progress')).style.display = 'none';
  const status = job && job.status;
  const jobError = job && (job.errors || [])[0];
  // A verification completion owns the page until its refreshed manifest
  // is rendered; otherwise Delete could briefly advertise the old totals.
  if (kind !== 'verify') cardCleanupSetBusy(false);
  if (kind === 'scan') {
    if (status === 'completed') {
      await cardCleanupLoadManifest();
    } else if (status === 'cancelled') {
      cardCleanupSetError(
        'Scan cancelled — no files were checked against the archive, and ' +
        'nothing was deleted. Scan again when you are ready.');
    } else if (status) {
      cardCleanupSetError('Scan ' + status + ' — no preview was produced' +
        (jobError ? ': ' + jobError : '.'));
    } else {
      cardCleanupSetError(
        'The scan is still running but this page lost track of it — ' +
        'check the Jobs page.');
    }
    return;
  }
  if (kind === 'verify') {
    await cardCleanupFinishVerification(job, status, jobError);
    return;
  }
  // Delete: the per-file outcome lives in the job result, so a job that
  // ended without one has to say so rather than imply a clean run.
  if (job && job.result) {
    cardCleanupRenderDeleteResult(job.result);
  } else if (status) {
    cardCleanupSetError('Deletion ' + status +
      ' and its summary could not be loaded' +
      (jobError ? ': ' + jobError : '') + ' — check the Jobs page for what ' +
      'was deleted, then re-scan the card.');
  } else {
    cardCleanupSetError(
      'The deletion is still running but this page lost track of it — ' +
      'check the Jobs page.');
  }
}
