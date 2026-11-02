// The card-scoped archive verification and the hash-failed callout.
// ES module.

import { cardCleanupWatchJob } from './jobs.js';
import { cardCleanupLoadManifest } from './manifest.js';
import { CARD_CLEANUP_HASH_FAILED_REASON, CARD_CLEANUP_VERIFY_REASON, cardCleanupErrorText, cardCleanupFileCount, cardCleanupSetBusy, cardCleanupSetError, state } from './state.js';

/* ---------- Card-scoped archive verification ---------- */

/** @param {CardCleanupManifest} manifest */
export function cardCleanupVerificationPendingCount(manifest) {
  // CodeRabbit: match verify_manifest_archives' filter — it groups
  // pending work by entry.hash and silently drops hashless entries, so
  // counting them here made the callout advertise a pending count
  // while the cost message below said zero archives would be read.
  return (manifest.entries || []).filter(entry =>
    entry.bucket === 'kept'
    && entry.reason === CARD_CLEANUP_VERIFY_REASON
    && entry.hash
  ).length;
}

/** @param {CardCleanupManifest} manifest */
export function cardCleanupRenderVerificationCallout(manifest) {
  const callout = /** @type {HTMLElement} */ (document.getElementById('card-cleanup-verify'));
  const pending = cardCleanupVerificationPendingCount(manifest);
  if (!pending) {
    callout.style.display = 'none';
    return;
  }
  const uniqueHashes = new Set((manifest.entries || [])
    .filter(entry => entry.bucket === 'kept'
      && entry.reason === CARD_CLEANUP_VERIFY_REASON && entry.hash)
    .map(entry => entry.hash)).size;
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-verify-text')).textContent =
    cardCleanupFileCount(pending) +
    (pending === 1 ? ' is' : ' are') +
    ' kept only because the matching archive copy has never been ' +
    'checksum-verified.';
  // The cost message must not promise "reads N matching copies" where
  // N = number of unique pending hashes:
  // verify_manifest_archives iterates every unchecked archive row for a
  // hash until one hashes cleanly, so a hash with several corrupt or
  // stale candidates can drive multiple full-file reads even though the
  // unique-hash count is one. Say "at least one per pending hash" and
  // name the retry condition explicitly, so the wording matches what
  // the verify job may actually do.
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-verify-cost')).textContent =
    'This reads matching archive files this card needs — at least one ' +
    'per ' +
    (uniqueHashes === 1
      ? 'pending hash'
      : uniqueHashes.toLocaleString() + ' pending hashes') +
    ', with additional reads if a candidate turns out to be corrupt or ' +
    'stale. Unrelated archive files are not touched, and duplicate ' +
    'files on the card share one check.';
  /** @type {HTMLButtonElement} */ (document.getElementById('card-cleanup-verify-btn')).disabled = false;
  callout.style.display = '';
}

/* ---------- Hash-failed affordance ----------
   Kept entries whose archive copies were checked and came back
   modified/corrupt/unreadable. They look like unverified rows to a
   casual read of the kept list but re-running verification would just
   reproduce the same bad verdict — the remedy is on the Audit page. */
/** @param {CardCleanupManifest} manifest */
export function cardCleanupHashFailedCount(manifest) {
  return (manifest.entries || []).filter(entry =>
    entry.bucket === 'kept'
    && String(entry.reason || '').indexOf(
        CARD_CLEANUP_HASH_FAILED_REASON) !== -1
  ).length;
}

/** @param {CardCleanupManifest} manifest */
export function cardCleanupRenderHashFailedCallout(manifest) {
  const callout = /** @type {HTMLElement} */ (document.getElementById('card-cleanup-hash-failed'));
  const failed = cardCleanupHashFailedCount(manifest);
  if (!failed) {
    callout.style.display = 'none';
    return;
  }
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-hash-failed-text')).textContent =
    cardCleanupFileCount(failed) +
    (failed === 1 ? ' is' : ' are') +
    ' kept because the archive copy has already failed an integrity ' +
    'check (modified, corrupt, or unreadable). Re-running verification ' +
    'would only reproduce that verdict — open the Audit page to accept ' +
    'the current bytes, restore from backup, or investigate.';
  // Scope honesty: the card scan matches photos across
  // ALL workspaces, but the Audit page only lists flagged rows for the
  // CURRENT workspace. If the failed archive copy belongs to a folder in
  // a different workspace, the suggested Audit remedy is unreachable
  // from here — say so up front, matching the audit callout's approach.
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-hash-failed-scope')).textContent =
    'The Audit page shows flagged rows for the current workspace only. ' +
    'If the failed archive copy lives in a folder from a different ' +
    'workspace, switch to that workspace to find it there.';
  callout.style.display = '';
}

export async function cardCleanupStartVerification() {
  if (state.jobId) return;  // a run is already streaming
  cardCleanupSetBusy(true, {
    deleteHint: 'Verification is running — deletion is available when it ' +
      'finishes.',
  });
  const resultEl = /** @type {HTMLElement} */ (document.getElementById('card-cleanup-verify-result'));
  resultEl.style.display = 'none';
  resultEl.textContent = '';
  cardCleanupSetError('');
  let resp;
  try {
    resp = await fetch('/api/card-cleanup/verify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({scan_job_id: state.scanJobId}),
    });
  } catch (e) {
    cardCleanupSetError(
      'The verify request never got a response, so we cannot tell whether ' +
      'the server queued it. Check the Jobs page for a card-cleanup-verify ' +
      'job, then reload this page. (' + cardCleanupErrorText(e) + ')');
    return;
  }
  if (!resp.ok) {
    // The server proved nothing was queued — hand the buttons back before
    // showing the error.
    cardCleanupSetBusy(false);
    let msg = 'verification failed to start';
    try {
      const data = await resp.json();
      if (data && data.error) msg = data.error;
    } catch (_) { /* keep default */ }
    cardCleanupSetError(msg);
    return;
  }
  let data;
  try {
    data = await resp.json();
  } catch (e) {
    cardCleanupSetError(
      'The verify request returned OK but its response could not be read, ' +
      'so verification may already be running. Check the Jobs page for a ' +
      'card-cleanup-verify job, then reload this page. (' +
      cardCleanupErrorText(e) + ')');
    return;
  }
  cardCleanupWatchJob(data.job_id, 'verify', 'Starting matching-copy verification…');
}

/** @param {CardCleanupJob | null} job @param {string} status @param {string | null | undefined} jobError */
export async function cardCleanupFinishVerification(job, status, jobError) {
  const resultEl = /** @type {HTMLElement} */ (document.getElementById('card-cleanup-verify-result'));
  // A completed or cancelled verification may
  // have promoted files, so the pre-verification Delete state is stale
  // until the refreshed manifest lands. Track both whether we tried to
  // reload and whether it succeeded; the "Preview updated" copy and
  // the Delete lock both depend on that success.
  const attemptedReload =
    (status === 'completed' || status === 'cancelled')
    && !!state.scanJobId;
  let refreshed = false;
  if (attemptedReload) {
    refreshed = await cardCleanupLoadManifest();
  }
  // Any completed/cancelled verification renders the
  // pre-verification Delete state stale. Do NOT let cardCleanupSetBusy
  // restore that state — a successful reload has already re-rendered
  // Delete via cardCleanupRenderManifest, and a failed reload needs it
  // locked. Restoring the busyRestore snapshot in between would flicker
  // Delete back to "enabled with old counts" for the moment between
  // this call and the manual disable below, and even that window could
  // authorize a stale deletion.
  cardCleanupSetBusy(false, {keepDeleteState: attemptedReload || refreshed});
  if (attemptedReload && !refreshed) {
    // Verification finished but the manifest reload failed or returned
    // an unreadable response, so the on-disk deletable set may now
    // exceed what the pre-verification preview advertises. Keep Delete
    // locked and require a successful reload or re-scan before it can
    // fire — the user would otherwise confirm the old totals while the
    // server deletes the freshly-promoted files too.
    const deleteBtn = /** @type {HTMLButtonElement} */ (document.getElementById('card-cleanup-delete-btn'));
    deleteBtn.disabled = true;
    /** @type {HTMLElement} */ (document.getElementById('card-cleanup-delete-hint')).textContent =
      'Verification finished but the updated preview could not be loaded ' +
      '— reload the page or scan again before deleting.';
  }
  resultEl.style.display = '';
  if (status === 'completed' && job && job.result) {
    const r = job.result;
    const problems = (r.modified || 0) + (r.corrupt || 0) + (r.unreadable || 0);
    const parts = [Number(r.archive_files_read || 0).toLocaleString() +
      ' matching archive ' + ((r.archive_files_read || 0) === 1 ? 'file' : 'files') +
      ' read'];
    parts.push(cardCleanupFileCount(r.unblocked_files || 0) + ' now ready to delete');
    if (problems) parts.push(problems.toLocaleString() + ' could not be verified');
    // Only claim the preview was updated once the refreshed manifest
    // has actually rendered — the counts above come from the job result,
    // not from the preview, so a failed reload would leave a stale
    // preview under a "Preview updated" line.
    resultEl.textContent = parts.join(' · ') +
      (refreshed
        ? '. Preview updated.'
        : '. The updated preview could not be loaded — reload the page ' +
          'or scan again before deleting.');
  } else if (status === 'cancelled') {
    resultEl.textContent = refreshed
      ? 'Verification cancelled — completed checks were kept and the preview was updated.'
      : 'Verification cancelled but the updated preview could not be loaded ' +
        '— reload the page or scan again before deleting.';
  } else if (status) {
    resultEl.textContent = 'Verification ' + status +
      (jobError ? ': ' + jobError : '.') +
      ' Check the Jobs page for what it managed to check.';
  } else {
    resultEl.textContent =
      'The verification is still running but this page lost track of it — ' +
      'check the Jobs page.';
  }
}
