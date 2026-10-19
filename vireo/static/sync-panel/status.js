// The pending-changes banner: progress text, the running sync job, and
// polling /api/sync/status.
// Classic script.

/** @param {SyncProgress | null | undefined} p */
function _syncProgressText(p) {
  if (!p || !p.total) return (p && (p.current_file || p.phase)) || 'Preparing XMP sync...';
  var text = 'Processed ' + p.current.toLocaleString() + ' of ' + p.total.toLocaleString() + ' photos';
  if (typeof p.synced === 'number' && typeof p.failed === 'number') {
    text += ' · ' + p.synced.toLocaleString() + ' synced · ' + p.failed.toLocaleString() + ' with errors';
  }
  return text;
}

function _syncScheduleStatus() {
  if (_syncStatusTimer !== null) return;
  _syncStatusTimer = setTimeout(function() {
    _syncStatusTimer = null;
    void checkPendingSync();
  }, 2000);
}

/** @param {SyncJob} job */
function _syncShowActiveJob(job) {
  _syncActiveJob = job;
  _syncScheduleStatus();
  if (!_syncModalOpen) return;
  if (!_syncReviewWaiting) {
    _syncStopPreviewLoad();
    _syncPreviewData = null;
    _syncReviewWaiting = true;
    _syncSetPreviewActionsEnabled(false);
    /** @type {HTMLElement} */ (document.getElementById('syncPreviewContent')).textContent =
      'Sync is running. The remaining changes will be ready to review when it finishes.';
  }
  var p = job.progress || /** @type {SyncProgress} */ ({});
  var pct = p.total > 0 ? Math.min(100, Math.round(p.current * 100 / p.total)) : 0;
  /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgress')).style.display = 'block';
  var text = /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgressText'));
  text.textContent = _syncProgressText(p);
  text.style.color = 'var(--text-primary)';
  /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgressPercent')).textContent = p.total ? pct + '%' : '';
  var track = /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgressTrack'));
  track.setAttribute('aria-label', 'XMP sync progress');
  track.setAttribute('aria-valuemax', String(p.total || 100));
  track.setAttribute('aria-valuenow', String(p.current || 0));
  var bar = /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgressBar'));
  bar.classList.toggle('is-preparing', !p.total);
  bar.style.width = p.total ? pct + '%' : '32%';
  /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgressDetail')).textContent =
    'Progress is saved periodically. Changes with errors stay pending.';
}

/** @returns {Promise<SyncStatusSummary | undefined>} */
async function checkPendingSync() {
  if (_syncStatusRequest) return _syncStatusRequest;
  _syncStatusRequest = _syncRefreshStatus();
  try { return await _syncStatusRequest; }
  finally { _syncStatusRequest = null; }
}

/** @returns {Promise<SyncStatusSummary | undefined>} */
async function _syncRefreshStatus() {
  try {
    var data = /** @type {SyncStatusSummary} */ (await safeFetch('/api/sync/status', {}, { toast: false }));
    var banner = /** @type {HTMLElement} */ (document.getElementById('syncBanner'));
    _syncActiveJob = data.active_job || null;
    if (_syncActiveJob) {
      _syncShowActiveJob(_syncActiveJob);
      var activeStatus = /** @type {HTMLElement} */ (document.getElementById('syncStatus'));
      activeStatus.textContent = _syncProgressText(_syncActiveJob.progress);
      activeStatus.dataset.syncState = 'running';
      activeStatus.style.color = 'var(--text-dim)';
    } else if (_syncReviewWaiting && _syncModalOpen) {
      _syncReviewWaiting = false;
      void _syncStartPreviewLoad(0);
    }
    if (data.pending_count > 0 || _syncActiveJob) {
      var status = /** @type {HTMLElement} */ (document.getElementById('syncStatus'));
      // A stale success message ("Synced!", "Synced N photos!" from a
      // partial run that left the unchecked change types queued, or
      // "Nothing to sync." from a run that found nothing when there is
      // now something) alongside a nonzero pending count reads as a UI
      // bug. Match the state we set on the element, not the specific
      // words, so future copy changes cannot desync this check.
      if (status.dataset.syncState === 'success') {
        status.textContent = '';
        status.style.color = 'var(--text-dim)';
        status.dataset.syncState = '';
      }
      /** @type {HTMLElement} */ (document.getElementById('syncBannerText')).textContent =
        data.pending_count.toLocaleString() + ' change' + (data.pending_count !== 1 ? 's' : '') +
        ' across ' + data.pending_photo_count.toLocaleString() + ' photo' + (data.pending_photo_count !== 1 ? 's' : '') +
        (data.active_job
          ? ' — awaiting sync confirmation; counts update periodically during sync.'
          : ' — metadata changes have been saved in Vireo but not written to XMP sidecar files yet.');
      banner.style.display = 'flex';
    } else {
      banner.style.display = 'none';
    }
    if (!_syncActiveJob && /** @type {HTMLElement} */ (document.getElementById('syncStatus')).dataset.syncState === 'running') {
      /** @type {HTMLElement} */ (document.getElementById('syncStatus')).textContent = 'Sync finished. Review remaining changes for any errors.';
      /** @type {HTMLElement} */ (document.getElementById('syncStatus')).dataset.syncState = '';
    }
    return data;
  } catch(e) {
    if (_syncActiveJob || _syncReviewWaiting) _syncScheduleStatus();
  }
}
