// Duplicates page: initial catalog/scan loading and restored-result banners.

import { state } from './state.js';
import { clearDuplicateTrashProgress, duplicateRequestError } from './cleanup.js';
import { renderResults } from './results.js';
import { hideRestoredBanner, setEmptyVisible } from './scan.js';
import { formatDateTime, safeFetch, showToast } from './shared.js';

export function cancelDuplicateLoad() {
  ++state.duplicateLoadEpoch;
  /** @type {HTMLElement} */ (document.getElementById('initialLoading')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('loadError')).style.display = 'none';
  /** @type {HTMLButtonElement} */ (document.getElementById('scanBtn')).disabled = false;
}

// Decisions apply to the displayed snapshot. A scan or restore must
// wait for it so it cannot install a catalog result captured before the switch.
export function duplicateResultsReloadBlocked() {
  if (state.sameShotReviewPending.size) {
    showToast('Wait for the review to save before loading duplicate results.', 'info');
    return true;
  }
  if (!state.duplicateKeepPending.size) return false;
  showToast('Wait for the switch to finish before loading duplicate results.', 'info');
  return true;
}

export function prepareDuplicateScan() {
  if (state.scanInProgress || duplicateResultsReloadBlocked()) return false;
  cancelDuplicateLoad();
  return true;
}

/** @param {boolean|Event} [forceCleanup] */
export async function tryRestoreLastScan(forceCleanup) {
  if (state.scanInProgress || duplicateResultsReloadBlocked()) return;
  clearDuplicateTrashProgress();
  var epoch = ++state.duplicateLoadEpoch;
  function current() { return epoch === state.duplicateLoadEpoch && !state.scanInProgress; }
  /** @type {HTMLElement} */ (document.getElementById('initialLoading')).style.display = '';
  /** @type {HTMLElement} */ (document.getElementById('loadError')).style.display = 'none';
  /** @type {HTMLButtonElement} */ (document.getElementById('scanBtn')).disabled = true;
  setEmptyVisible(false);
  hideRestoredBanner();
  // A refresh must not leave older cleanup buttons actionable beneath the
  // loading/error state. A subsequent filter edit must not revive them either.
  state.lastScanResult = null;
  state.sameShotVisibleLimit = 50;
  /** @type {HTMLElement} */ (document.getElementById('sameShotResults')).innerHTML = '';
  state.proposals = [];
  /** @type {HTMLElement} */ (document.getElementById('results')).innerHTML = '';
  /** @type {HTMLElement} */ (document.getElementById('summary')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('applyBar')).style.display = 'none';
  try {
    var cleanup = forceCleanup === true ||
      new URLSearchParams(window.location.search).get('show') === 'resolved';
    /** @type {DuplicateLastScan|null} */ var data = cleanup ? null :
      await safeFetch('/api/duplicates/last-scan', undefined, { toast: false });
    if (!current()) return;
    /** @type {DuplicateScan} */ var result;
    if (cleanup || !data || !data.found) {
      result = await safeFetch('/api/duplicates/cleanup', undefined, { toast: false });
      cleanup = true;
    } else {
      result = data.result || {proposals: []};
    }
    if (!current()) return;
    // Respect the first filter pass and any newer query still in flight.
    await state.filterReady;
    while (state.pendingFilterRefresh) {
      var pending = state.pendingFilterRefresh;
      await pending;
      if (state.pendingFilterRefresh === pending) break;
    }
    if (!current()) return;
    if (cleanup && !result.proposals.length &&
        new URLSearchParams(window.location.search).get('show') !== 'resolved' &&
        forceCleanup !== true) {
      setEmptyVisible(true);
      return;
    }
    renderResults(result);
    if (cleanup) {
      /** @type {HTMLElement} */ (document.getElementById('catalogBanner')).style.display = '';
      return;
    }
    /** @type {HTMLElement} */ (document.getElementById('restoredAgo')).textContent = formatTimeAgo(data?.finished_at);
    var stale = result.stale_group_count || 0;
    var remaining = (result.proposals || []).length;
    /** @type {HTMLElement} */ (document.getElementById('restoredStale')).textContent = (stale && remaining)
      ? ' ' + stale.toLocaleString() + (stale === 1
          ? ' group from that scan no longer matches your catalog and is hidden.'
          : ' groups from that scan no longer match your catalog and are hidden.')
      : '';
    /** @type {HTMLElement} */ (document.getElementById('restoredFreshness')).textContent = restoredFreshnessText(result);
    /** @type {HTMLElement} */ (document.getElementById('restoredBanner')).style.display = '';
  } catch (caught) { const e = duplicateRequestError(caught);
    if (!current()) return;
    /** @type {HTMLElement} */ (document.getElementById('loadErrorText')).textContent =
      'Could not load duplicate results. ' + (e.message || 'Try again.');
    /** @type {HTMLElement} */ (document.getElementById('loadError')).style.display = '';
  } finally {
    if (current()) {
      /** @type {HTMLElement} */ (document.getElementById('initialLoading')).style.display = 'none';
      /** @type {HTMLButtonElement} */ (document.getElementById('scanBtn')).disabled = false;
    }
  }
}

/** @param {string} [isoStr] */
export function formatTimeAgo(isoStr) {
  if (!isoStr) return '';
  // job_history timestamps are naive local-time ISO strings written by
  // datetime.now().isoformat() in vireo/jobs.py. Pass them through as-is so
  // JS parses them as local time; appending 'Z' would shift the displayed
  // age by the local UTC offset (e.g. a fresh scan appearing hours old).
  var d = new Date(isoStr);
  var sec = Math.floor((Date.now() - d.getTime()) / 1000);
  if (sec < 60) return 'just now';
  if (sec < 3600) return Math.floor(sec / 60) + 'm ago';
  if (sec < 86400) return Math.floor(sec / 3600) + 'h ago';
  return formatDateTime(d);
}

// Whether a new scan would show the same groups, from the counts
// last-scan compares against the catalog. Only asks for a rescan when one
// would actually show something different.
/** @param {DuplicateScan} result */
export function restoredFreshnessText(result) {
  var added = result.new_group_count || 0;
  var changed = result.changed_group_count || 0;
  if (!added && !changed) {
    return ' Exact-byte groups are unchanged in your catalog. Scan again to refresh possible same-shot copies.';
  }
  var parts = [];
  if (added) {
    parts.push(added.toLocaleString() + (added === 1
      ? ' new duplicate group has appeared'
      : ' new duplicate groups have appeared'));
  }
  if (changed) {
    parts.push(changed.toLocaleString() + (changed === 1
      ? ' group from that scan has changed'
      : ' groups from that scan have changed'));
  }
  return ' Since then, ' + parts.join(' and ') +
    '. Scan again to see them as they are now.';
}
