// The bucket lists, the delete confirmation, deleting, the result, and
// cancelling a job.
// ES module.

import { formatBytes } from './shared.js';
import { cardCleanupWatchJob } from './jobs.js';
import { cardCleanupLoadManifest } from './manifest.js';
import { cardCleanupErrorText, cardCleanupFileCount, cardCleanupSetBusy, cardCleanupSetError, state } from './state.js';

// Bucket lists run to thousands of rows, so build them the first time the
// user opens that group rather than on every scan. Each <details> is bound
// once; the listener reads the manifest current when it is opened.
const boundBucketDetails = new Set();

/** @param {string} bucket @param {string} detailsId @param {string} listId */
export function cardCleanupBindBucket(bucket, detailsId, listId) {
  const details = /** @type {HTMLDetailsElement} */ (document.getElementById(detailsId));
  details.open = false;
  if (boundBucketDetails.has(detailsId)) return;
  boundBucketDetails.add(detailsId);
  details.addEventListener('toggle', () => {
    if (!details.open || state.listsRendered[bucket]) return;
    if (!state.manifest) return;
    state.listsRendered[bucket] = true;
    // Spec ("UX flow"): verified rows show path, size, and the matched
    // archive copy that justifies the deletion; kept rows show size and
    // the per-file reason. All fields come straight from the manifest.
    const lines = (/** @type {CardCleanupManifest} */ (state.manifest).entries || [])
      .filter(entry => entry.bucket === bucket)
      .map(entry => {
        const size = (typeof entry.size === 'number')
          ? ' (' + formatBytes(entry.size) + ')' : '';
        if (bucket === 'kept') {
          return entry.path + size + ' — ' + (entry.reason || 'not verified');
        }
        if (bucket === 'deletable') {
          return entry.path + size
            + ' → verified copy: ' + (entry.archive_path || 'unknown');
        }
        return entry.path;
      });
    cardCleanupFillList(listId, lines);
  });
}

/** @param {string} listId @param {string[]} lines */
export function cardCleanupFillList(listId, lines) {
  const list = /** @type {HTMLElement} */ (document.getElementById(listId));
  list.innerHTML = '';
  const frag = document.createDocumentFragment();
  if (!lines.length) {
    const li = document.createElement('li');
    li.textContent = 'No files in this group.';
    frag.appendChild(li);
  }
  lines.forEach((line) => {
    const li = document.createElement('li');
    li.textContent = line;
    frag.appendChild(li);
  });
  list.appendChild(frag);
}

export function cardCleanupResetPreview() {
  state.manifest = null;
  state.listsRendered = {};
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-preview')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-incomplete')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-verify')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-verify-result')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-hash-failed')).style.display = 'none';
  const deleteBtn = /** @type {HTMLButtonElement} */ (document.getElementById('card-cleanup-delete-btn'));
  deleteBtn.disabled = true;
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-delete-hint')).textContent = '';
  [
    'card-cleanup-deletable-details', 'card-cleanup-kept-details',
    'card-cleanup-ignored-details', 'card-cleanup-walk-errors-details',
  ].forEach((id) => { /** @type {HTMLDetailsElement} */ (document.getElementById(id)).open = false; });
}

export function cardCleanupOpenConfirm() {
  // Defence in depth: cardCleanupSetBusy already disables this button
  // while a scan, deletion, or verification is running, and starting a
  // deletion on top of one of those would strand the running job.
  if (state.jobId) return;
  const manifest = state.manifest;
  if (!manifest) return;
  const deletable = (manifest.totals || {}).deletable || { count: 0, bytes: 0 };
  if (!deletable.count) return;
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-confirm-count')).textContent =
    'Deleting ' + cardCleanupFileCount(deletable.count) + ' (' +
    formatBytes(deletable.bytes) + ') from ' +
    (manifest.source_root || 'the card') + '.';
  const overlay = /** @type {HTMLElement} */ (document.getElementById('card-cleanup-confirm'));
  overlay.classList.add('open');
  if (state.confirmEscHandler) {
    document.removeEventListener('keydown', state.confirmEscHandler);
  }
  state.confirmEscHandler = (e) => {
    if (e.key === 'Escape') cardCleanupCloseConfirm();
  };
  document.addEventListener('keydown', state.confirmEscHandler);
  document.body.style.overflow = 'hidden';
  // Focus the dialog's first control (the close button), not the delete
  // button — a stray Enter must not delete anything.
  const firstAction = overlay.querySelector('button');
  if (firstAction) firstAction.focus();
}

export function cardCleanupCloseConfirm() {
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-confirm')).classList.remove('open');
  if (state.confirmEscHandler) {
    document.removeEventListener('keydown', state.confirmEscHandler);
    state.confirmEscHandler = null;
  }
  document.body.style.overflow = '';
}

/**
 * The server refused the delete. A missing manifest takes the preview down;
 * a revision mismatch reloads it with the page still locked; anything else
 * reports the error.
 *
 * @param {Response} resp @param {{code?: string, error?: string} | null} data
 */
async function cardCleanupDeleteRefused(resp, data) {
  if (resp.status === 404) {
    // The manifest is gone, so the preview above no longer describes
    // anything that can be deleted — take it down with the error.
    cardCleanupSetBusy(false);
    state.scanJobId = null;
    cardCleanupResetPreview();
    cardCleanupSetError('Preview expired — re-scan the card.', { rescan: true });
  } else if (data && data.code === 'manifest_revision_mismatch') {
    // Reload the on-disk manifest so the preview and Delete button
    // reflect what would actually be deleted next; the totals the
    // user just confirmed no longer exist on the server.
    cardCleanupSetError(data.error ||
      'The preview changed since you confirmed it — review the ' +
      'updated totals before deleting.');
    // Await the reload before releasing the busy state. Releasing
    // first and reloading fire-and-forget would re-enable
    // Scan/Verify/Delete in the gap before the response lands: a
    // user who reopened the confirm dialog then would see the
    // pre-mismatch totals, and once the reload swapped
    // state.manifest for the fresh one, confirming would send the
    // new revision and the server would accept deletion of the
    // newly promoted set that never appeared in the dialog.
    // Awaiting the reload with the page still locked closes the
    // window. On success,
    // cardCleanupRenderManifest has already set Delete's disabled
    // state and hint from the fresh totals, so keepDeleteState
    // preserves them across the unlock. On failure the render
    // never ran, so explicitly disable Delete and clear the
    // stale "Deletion is starting…" hint — a stale preview must
    // not be able to authorize a deletion.
    const reloaded = await cardCleanupLoadManifest();
    cardCleanupSetBusy(false, { keepDeleteState: true });
    if (!reloaded) {
      const deleteBtn = /** @type {HTMLButtonElement} */ (document.getElementById('card-cleanup-delete-btn'));
      deleteBtn.disabled = true;
      /** @type {HTMLElement} */ (document.getElementById('card-cleanup-delete-hint')).textContent =
        'Reload the page or scan again before deleting — the ' +
        'preview could not be refreshed after the revision changed.';
    }
  } else {
    cardCleanupSetBusy(false);
    cardCleanupSetError((data && data.error) || 'deletion failed to start');
  }
}

export async function cardCleanupConfirmDelete() {
  // A job that started while this dialog was open (or one the page picked
  // up again) must not be replaced by the delete watch below — say so
  // instead of quietly doing nothing or stranding it.
  if (state.jobId) {
    cardCleanupCloseConfirm();
    cardCleanupSetError('Another job is still running — nothing was ' +
      'deleted. Wait for it to finish, then delete.');
    return;
  }
  const confirmBtn = /** @type {HTMLButtonElement} */ (document.getElementById('card-cleanup-confirm-btn'));
  confirmBtn.disabled = true;
  // Lock the page before the POST goes out. The confirm
  // dialog can be dismissed (backdrop click, Escape) while this request is
  // pending; if Scan and Verify stayed live in that gap, a second job could
  // start and its watch would overwrite this delete's jobId — including the
  // destructive path. Setting busy now disables Scan/Verify/Delete regardless
  // of whether the dialog is still up when the response arrives. Clear it on
  // any definitive failure so the user isn't left with dead buttons.
  cardCleanupSetBusy(true, {
    deleteHint: 'Deletion is starting…',
  });
  // Send the manifest revision we confirmed against. If a
  // verify has landed since (and rewritten the manifest with a fresh
  // revision), the server rejects this POST rather than deleting the
  // newly-promoted files the user never saw.
  //
  // A manifest saved before revisions existed has no `revision` key.
  // Sending undefined would make JSON.stringify
  // drop `manifest_revision` from the body, and the endpoint rejects a
  // missing key with 400 rather than treating it as the initial
  // revision — leaving every carried-over preview undeletable. Match
  // what the server already does when it loads a manifest without a
  // revision: default to INITIAL_MANIFEST_REVISION (1).
  const confirmedRevision =
    (state.manifest && state.manifest.revision) || 1;
  try {
    const resp = await fetch('/api/card-cleanup/delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        scan_job_id: state.scanJobId,
        manifest_revision: confirmedRevision,
      }),
    });
    const data = await resp.json();
    cardCleanupCloseConfirm();
    if (!resp.ok) {
      await cardCleanupDeleteRefused(resp, data);
      return;
    }
    cardCleanupSetError('');
    /** @type {HTMLElement} */ (document.getElementById('card-cleanup-result')).style.display = 'none';
    cardCleanupWatchJob(data.job_id, 'delete', 'Starting deletion…');
  } catch (e) {
    cardCleanupCloseConfirm();
    // The POST failed at the network layer, so the server may have
    // accepted the delete and queued the destructive job before the
    // connection dropped — we simply don't know.
    // Unlocking here would let a second job start and hijack the page's
    // jobId while the deletion continues unseen, so keep Scan / Verify /
    // Delete disabled and send the user to the Jobs page for the actual
    // outcome. A page reload rebuilds the busy state from a fresh
    // manifest once they've confirmed what happened.
    cardCleanupSetError(
      'The delete request never got a response, so we cannot tell whether ' +
      'the server queued the deletion. Check the Jobs page for a delete ' +
      'that started around now, then reload this page to work with the ' +
      'card again. (' + cardCleanupErrorText(e) + ')');
  } finally {
    confirmBtn.disabled = false;
  }
}

/** @param {CardCleanupDeleteResult | null | undefined} result */
export function cardCleanupRenderDeleteResult(result) {
  const res = result || {};
  const skipped = res.skipped || [];
  const failed = res.failed || [];
  // The job result bounds these lists to a sample; *_total carries the
  // exact count (falls back to list length for pre-trim results).
  const skippedTotal = (typeof res.skipped_total === 'number')
    ? res.skipped_total : skipped.length;
  const failedTotal = (typeof res.failed_total === 'number')
    ? res.failed_total : failed.length;
  const parts = ['Deleted ' + cardCleanupFileCount(res.deleted) +
    ' (' + formatBytes(res.deleted_bytes) + ')'];
  if (skippedTotal) {
    parts.push(skippedTotal.toLocaleString() + ' skipped');
  }
  if (failedTotal) {
    parts.push(failedTotal.toLocaleString() + ' failed');
  }
  if (res.cancelled) {
    parts.push('Cancelled — ' + Number(res.remaining || 0).toLocaleString() +
      ' files not yet processed.');
  }
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-result')).style.display = '';
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-result-summary')).textContent =
    parts.join(' · ');
  const lines = [];
  skipped.forEach((s) => {
    lines.push('Skipped: ' + s.path + ' — ' + (s.reason || 'no reason given'));
  });
  if (skippedTotal > skipped.length) {
    lines.push('…and ' + (skippedTotal - skipped.length).toLocaleString() +
      ' more skipped files not listed here (the counts above are exact).');
  }
  failed.forEach((f) => {
    lines.push('Failed: ' + f.path + ' — ' + (f.error || 'no error given'));
  });
  if (failedTotal > failed.length) {
    lines.push('…and ' + (failedTotal - failed.length).toLocaleString() +
      ' more failed files not listed here (the counts above are exact).');
  }
  const list = /** @type {HTMLElement} */ (document.getElementById('card-cleanup-result-list'));
  if (lines.length) {
    list.style.display = '';
    cardCleanupFillList('card-cleanup-result-list', lines);
  } else {
    list.style.display = 'none';
    list.innerHTML = '';
  }
  // The manifest describes files that are now gone, so the preview above
  // is history. Say so instead of leaving a live-looking Delete button.
  /** @type {HTMLButtonElement} */ (document.getElementById('card-cleanup-delete-btn')).disabled = true;
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-delete-hint')).textContent =
    'These numbers describe the scan that ran before this deletion — ' +
    'scan again to see what is still on the card.';
}

export async function cardCleanupCancelJob() {
  const jobId = state.jobId;
  if (!jobId) return;
  const btn = /** @type {HTMLButtonElement} */ (document.getElementById('card-cleanup-cancel-btn'));
  btn.disabled = true;
  btn.textContent = 'Cancelling…';
  try {
    const resp = await fetch(
      '/api/jobs/' + encodeURIComponent(jobId) + '/cancel', { method: 'POST' });
    if (!resp.ok) {
      const data = await resp.json().catch(() => ({}));
      btn.disabled = false;
      btn.textContent = 'Cancel';
      cardCleanupSetError(data.error || 'Unable to cancel the job.');
    }
  } catch (e) {
    btn.disabled = false;
    btn.textContent = 'Cancel';
    cardCleanupSetError(cardCleanupErrorText(e));
  }
}
