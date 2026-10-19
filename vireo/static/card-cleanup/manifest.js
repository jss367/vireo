// Loading the scan manifest and rendering its buckets.
// ES module.

import { formatBytes } from './shared.js';
import { cardCleanupBindBucket, cardCleanupFillList } from './delete.js';
import { cardCleanupErrorText, cardCleanupFileCount, cardCleanupSetError, state } from './state.js';
import { cardCleanupRenderHashFailedCallout, cardCleanupRenderVerificationCallout } from './verification.js';

export async function cardCleanupLoadManifest() {
  // Codex P2 (commit 16a3f8e4): capture the scan we are loading FOR so a
  // response that arrives after a second scan has taken ownership of
  // scanJobId cannot render scan #1's manifest over scan #2's state.
  // Without this guard, cardCleanupFinishJob unlocks the buttons before
  // awaiting the manifest; a user who changes the source and starts
  // scan #2 during that await would see scan #1's totals/source rendered
  // by the late response and the confirmation dialog would advertise a
  // set the delete POST (which uses the current scanJobId) would not
  // actually operate on. It would also clobber the 404-clears-scanJobId
  // path and the error message.
  const requestedScanJobId = state.scanJobId;
  try {
    const resp = await fetch('/api/card-cleanup/' +
      encodeURIComponent(String(requestedScanJobId)) + '/manifest');
    const data = await resp.json();
    if (state.scanJobId !== requestedScanJobId) return false;
    if (resp.status === 404) {
      state.scanJobId = null;
      cardCleanupSetError('Preview expired — re-scan the card.', { rescan: true });
      return false;
    }
    if (!resp.ok) throw new Error(data.error || 'the preview could not be loaded');
    cardCleanupRenderManifest(data);
    return true;
  } catch (e) {
    // CodeRabbit: preserve the boolean return contract on the
    // stale-scan ownership check too. Returning undefined here would be
    // read as "not refreshed" by the caller, which happens to be
    // correct — but only by accident. State it.
    if (state.scanJobId !== requestedScanJobId) return false;
    cardCleanupSetError(cardCleanupErrorText(e));
    return false;
  }
}

/** @param {CardCleanupManifest} manifest */
export function cardCleanupRenderManifest(manifest) {
  state.manifest = manifest;
  state.listsRendered = {};
  const totals = manifest.totals || {};
  const deletable = totals.deletable || { count: 0, bytes: 0 };
  const kept = totals.kept || { count: 0, bytes: 0 };
  const ignored = totals.ignored || { count: 0 };
  const walkErrors = (manifest.walk_errors || []).map(String);

  // Unmissable and above the numbers: a walk that skipped folders means
  // the counts below are a floor, not the whole card.
  const incomplete = /** @type {HTMLElement} */ (document.getElementById('card-cleanup-incomplete'));
  if (walkErrors.length) {
    incomplete.style.display = '';
    /** @type {HTMLElement} */ (document.getElementById('card-cleanup-incomplete-text')).textContent =
      'Some folders could not be read; this preview is incomplete. Files ' +
      'that were not seen will never be deleted.';
    /** @type {HTMLElement} */ (document.getElementById('card-cleanup-walk-errors-summary')).textContent =
      walkErrors.length.toLocaleString() + ' folder ' +
      (walkErrors.length === 1 ? 'error' : 'errors');
    cardCleanupFillList('card-cleanup-walk-errors', walkErrors);
  } else {
    incomplete.style.display = 'none';
  }

  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-scanned-root')).textContent =
    'Checked ' + (manifest.source_root || '') +
    (manifest.recursive
      ? ' and its subfolders'
      : ' only (subfolders were not included)');

  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-deletable-summary')).textContent =
    cardCleanupFileCount(deletable.count) + ' / ' +
    formatBytes(deletable.bytes) +
    ' verified in the archive — safe to delete';
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-kept-summary')).textContent =
    cardCleanupFileCount(kept.count) + ' / ' + formatBytes(kept.bytes) +
    ' not verified — will be kept';
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-ignored-summary')).textContent =
    cardCleanupFileCount(ignored.count) +
    ' ignored (not photo files Vireo imports)';

  cardCleanupRenderVerificationCallout(manifest);
  cardCleanupRenderHashFailedCallout(manifest);

  cardCleanupBindBucket('deletable', 'card-cleanup-deletable-details',
    'card-cleanup-deletable-list');
  cardCleanupBindBucket('kept', 'card-cleanup-kept-details',
    'card-cleanup-kept-list');
  cardCleanupBindBucket('ignored', 'card-cleanup-ignored-details',
    'card-cleanup-ignored-list');

  const deleteBtn = /** @type {HTMLButtonElement} */ (document.getElementById('card-cleanup-delete-btn'));
  deleteBtn.disabled = deletable.count === 0;
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-delete-hint')).textContent =
    deletable.count === 0
      ? 'Nothing on this card is verified in the archive, so there is ' +
        'nothing safe to delete.'
      : 'Deletes the ' + cardCleanupFileCount(deletable.count) +
        ' listed as verified above, and nothing else.';
  /** @type {HTMLElement} */ (document.getElementById('card-cleanup-preview')).style.display = '';
}
