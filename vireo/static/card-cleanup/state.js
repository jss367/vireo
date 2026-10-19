// Card cleanup page state and helpers: the one job this page watches, the
// scan manifest, the manifest reasons it answers, busy state, file counts
// and the error line.
// ES module.

import { cardCleanupStartScan } from './jobs.js';

/* ---------- Free up card space ----------
   Deletes card files only where the archive copy is verified. Every count
   and byte total shown here is read straight out of the scan manifest —
   this page never estimates, and it marks the preview incomplete
   whenever the card walk hit a folder it could not read. */

/**
 * @typedef {object} CardCleanupState
 * @property {string | null} scanJobId
 * @property {string | null} jobId
 * @property {string | null} finishedJobId
 * @property {EventSource | null} es
 * @property {CardCleanupManifest | null} manifest
 * @property {Record<string, boolean>} listsRendered
 * @property {((event: KeyboardEvent) => void) | null} confirmEscHandler
 * @property {{deleteDisabled: boolean, deleteHint: string | null} | null} busyRestore
 */

/** @type {CardCleanupState} */
export const state = {
  scanJobId: null,      // manifest key of the last scan we started
  jobId: null,          // job currently streaming (scan, delete, or audit)
  finishedJobId: null,  // guards the double 'complete' + 'error' callback
  es: null,             // this page's job stream
  manifest: null,
  listsRendered: {},    // bucket -> true once its <li>s are in the DOM
  confirmEscHandler: null,
  busyRestore: null,    // delete button/hint state saved while a job runs
};

/* Exactly one job at a time. Starting a second one while this page is
   watching a job would close the first job's stream and overwrite
   `jobId`, so that job's completion handler would never run: the finished
   run would be orphaned from the page (blank status, stuck buttons) while
   it kept going on the server. Rather than let a click do that silently,
   the three start buttons are disabled for the duration of any run — and
   the Delete button, being the destructive one, says why. */
/** @param {boolean} running @param {{deleteHint?: string, keepDeleteState?: boolean}=} opts */
export function cardCleanupSetBusy(running, opts) {
  const scanBtn = /** @type {HTMLButtonElement} */ (document.getElementById('card-cleanup-scan-btn'));
  const deleteBtn = /** @type {HTMLButtonElement} */ (document.getElementById('card-cleanup-delete-btn'));
  const verifyBtn = /** @type {HTMLButtonElement} */ (document.getElementById('card-cleanup-verify-btn'));
  const deleteHint = /** @type {HTMLElement} */ (document.getElementById('card-cleanup-delete-hint'));
  if (running) {
    // Whether Delete may be pressed depends on the manifest (and on
    // whether a deletion already ran), so restore what was there rather
    // than recomputing it.
    if (state.busyRestore === null) {
      state.busyRestore = {
        deleteDisabled: deleteBtn.disabled,
        deleteHint: deleteHint.textContent,
      };
    }
    scanBtn.disabled = true;
    verifyBtn.disabled = true;
    deleteBtn.disabled = true;
    if (opts && opts.deleteHint) deleteHint.textContent = opts.deleteHint;
    return;
  }
  const restore = state.busyRestore;
  state.busyRestore = null;
  scanBtn.disabled = false;
  verifyBtn.disabled = false;
  if (opts && opts.keepDeleteState) return;
  if (restore) {
    deleteBtn.disabled = restore.deleteDisabled;
    deleteHint.textContent = restore.deleteHint;
  }
}

/** A caught error's message, or the value itself. @param {unknown} e */
export function cardCleanupErrorText(e) {
  return String((e instanceof Error && e.message) || e);
}

// Mirrors card_cleanup.KEEP_NOT_VERIFIED exactly.  Exact matching avoids
// offering verification for rows that already failed a previous check.
export const CARD_CLEANUP_VERIFY_REASON = 'archive copy has not been checksum-verified';
// The manifest reason that this page's hash-failed callout answers.
// Mirrors card_cleanup.KEEP_ARCHIVE_HASH_FAILED — kept distinct from
// CARD_CLEANUP_VERIFY_REASON so a check-failed row cannot pretend to be a
// never-checked one and get double-counted (or vice versa). Same tail
// match as above; test_hash_failed_callout_reason_stays_in_sync pins it.
export const CARD_CLEANUP_HASH_FAILED_REASON = 'see the Audit page';

/** @param {number | null | undefined} n */
export function cardCleanupFileCount(n) {
  const count = Number(n || 0);
  return count.toLocaleString() + (count === 1 ? ' file' : ' files');
}

/** @param {string | null | undefined} msg @param {{rescan?: boolean}=} opts */
export function cardCleanupSetError(msg, opts) {
  const el = /** @type {HTMLElement} */ (document.getElementById('card-cleanup-error'));
  el.textContent = '';
  el.classList.toggle('visible', !!msg);
  if (!msg) return;
  el.append(msg);
  if (opts && opts.rescan) {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'retry-link';
    btn.textContent = 'Re-scan';
    btn.addEventListener('click', () => cardCleanupStartScan());
    el.append(' ', btn);
  }
}
