// Starting a duplicate scan and following its job: the progress line, the
// empty state, and the restored-scan banners.

import { state } from './state.js';
import { clearDuplicateTrashProgress } from './cleanup.js';
import { renderScanWhenFilterReady } from './filter.js';
import { prepareDuplicateScan } from './restore.js';
import { safeEventSource, safeFetch, showToast } from './shared.js';

/** @param {string=} msg */
export function showProgress(msg) {
  var pg = /** @type {HTMLElement} */ (document.getElementById('progress'));
  pg.style.display = '';
  /** @type {HTMLElement} */ (document.getElementById('progressStatus')).textContent = msg || 'Scanning...';
}

export function hideProgress() {
  /** @type {HTMLElement} */ (document.getElementById('progress')).style.display = 'none';
}

/** @param {boolean} visible */
export function setEmptyVisible(visible) {
  /** @type {HTMLElement} */ (document.getElementById('emptyState')).style.display = visible ? '' : 'none';
}

export function hideRestoredBanner() {
  /** @type {HTMLElement} */ (document.getElementById('restoredBanner')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('catalogBanner')).style.display = 'none';
}

export async function startScan() {
  if (!prepareDuplicateScan()) return;
  clearDuplicateTrashProgress();
  state.excludedDuplicateHashes.clear();
  var scanBtn = /** @type {HTMLButtonElement} */ (document.getElementById('scanBtn'));
  state.scanInProgress = true;
  scanBtn.disabled = true;
  hideRestoredBanner();
  setEmptyVisible(false);
  /** @type {HTMLElement} */ (document.getElementById('results')).innerHTML = '';
  /** @type {HTMLElement} */ (document.getElementById('summary')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('applyBar')).style.display = 'none';
  // Invalidate any prior scan snapshot (from a restored scan or a
  // previous run). If the new scan is cancelled, fails, or the
  // EventSource errors, ``state.scanInProgress`` flips back to false while
  // the DOM has already been cleared — leaving the stale snapshot in
  // ``state.lastScanResult`` would let the next filter edit / pause / resume
  // re-render those old duplicate cards and re-enable their actions.
  // Successful completion repopulates ``state.lastScanResult`` via
  // ``renderResults``.
  state.lastScanResult = null;
  state.sameShotVisibleLimit = 50;
  /** @type {HTMLElement} */ (document.getElementById('sameShotResults')).innerHTML = '';
  showProgress('Starting scan...');

  try {
    var data = await safeFetch('/api/duplicates/scan', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: '{}',
    }, { toast: false });
    state.currentJobId = data.job_id;

    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(p) {
        var msg = p.phase || p.current_file || 'Scanning...';
        if (p.total && p.current != null) {
          msg += ' (' + p.current + '/' + p.total + ')';
        }
        showProgress(msg);
      },
      onComplete: function(evt) {
        // Clear the in-progress flag at every terminal scan state.
        // Without this, a later bulk-resolve's renderResults guard
        // (``if (!state.scanInProgress)``) would block forever after the
        // first scan, and the just-resolved bucket card never
        // disappears from the page.
        state.scanInProgress = false;
        if (evt.status === 'completed') {
          // Wait for any in-flight filter refresh so the initial
          // render uses the CURRENT filter state, not the pre-edit
          // one. If the scan completes before a slower filter query
          // (typical with a visual clause) — or right after the user
          // edits chips mid-scan — a raw ``renderResults`` would use
          // the stale ``state.filterMatchedIds`` (often ``null``) and
          // briefly re-enable apply/bulk actions for groups the newly
          // edited chips will hide once the query settles.
          renderScanWhenFilterReady(evt.result);
        } else if (evt.status === 'cancelled') {
          hideProgress();
          scanBtn.disabled = false;
          setEmptyVisible(true);
          showToast('Scan cancelled', 'error');
        } else {
          hideProgress();
          scanBtn.disabled = false;
          setEmptyVisible(true);
          var err = (evt.errors || []).join(', ') || 'Scan failed';
          showToast(err, 'error');
        }
      },
      onError: function() {
        state.scanInProgress = false;
        hideProgress();
        scanBtn.disabled = false;
        setEmptyVisible(true);
      },
    });
  } catch (e) {
    state.scanInProgress = false;
    hideProgress();
    scanBtn.disabled = false;
    setEmptyVisible(true);
    showToast((e instanceof Error && e.message) || 'Failed to start scan', 'error');
  }
}
