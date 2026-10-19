// Starting a duplicate scan and following its job: the progress line, the
// empty state, and the restored-scan banners.
// Classic page script.

/** @param {string=} msg */
function showProgress(msg) {
  var pg = /** @type {HTMLElement} */ (document.getElementById('progress'));
  pg.style.display = '';
  /** @type {HTMLElement} */ (document.getElementById('progressStatus')).textContent = msg || 'Scanning...';
}

function hideProgress() {
  /** @type {HTMLElement} */ (document.getElementById('progress')).style.display = 'none';
}

/** @param {boolean} visible */
function setEmptyVisible(visible) {
  /** @type {HTMLElement} */ (document.getElementById('emptyState')).style.display = visible ? '' : 'none';
}

function hideRestoredBanner() {
  /** @type {HTMLElement} */ (document.getElementById('restoredBanner')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('catalogBanner')).style.display = 'none';
}

async function startScan() {
  if (!prepareDuplicateScan()) return;
  clearDuplicateTrashProgress();
  _excludedDuplicateHashes.clear();
  var scanBtn = /** @type {HTMLButtonElement} */ (document.getElementById('scanBtn'));
  _scanInProgress = true;
  scanBtn.disabled = true;
  hideRestoredBanner();
  setEmptyVisible(false);
  /** @type {HTMLElement} */ (document.getElementById('results')).innerHTML = '';
  /** @type {HTMLElement} */ (document.getElementById('summary')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('applyBar')).style.display = 'none';
  // Invalidate any prior scan snapshot (from a restored scan or a
  // previous run). If the new scan is cancelled, fails, or the
  // EventSource errors, ``_scanInProgress`` flips back to false while
  // the DOM has already been cleared — leaving the stale snapshot in
  // ``_lastScanResult`` would let the next filter edit / pause / resume
  // re-render those old duplicate cards and re-enable their actions.
  // Successful completion repopulates ``_lastScanResult`` via
  // ``renderResults``.
  _lastScanResult = null;
  showProgress('Starting scan...');

  try {
    var data = await safeFetch('/api/duplicates/scan', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: '{}',
    }, { toast: false });
    _currentJobId = data.job_id;

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
        // (``if (!_scanInProgress)``) would block forever after the
        // first scan, and the just-resolved bucket card never
        // disappears from the page.
        _scanInProgress = false;
        if (evt.status === 'completed') {
          // Wait for any in-flight filter refresh so the initial
          // render uses the CURRENT filter state, not the pre-edit
          // one. If the scan completes before a slower filter query
          // (typical with a visual clause) — or right after the user
          // edits chips mid-scan — a raw ``renderResults`` would use
          // the stale ``_filterMatchedIds`` (often ``null``) and
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
        _scanInProgress = false;
        hideProgress();
        scanBtn.disabled = false;
        setEmptyVisible(true);
      },
    });
  } catch (e) {
    _scanInProgress = false;
    hideProgress();
    scanBtn.disabled = false;
    setEmptyVisible(true);
    showToast((e instanceof Error && e.message) || 'Failed to start scan', 'error');
  }
}
