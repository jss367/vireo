// The Review Changes dialog's progressive load: thumbnail size, the
// progress line, enabling actions, and paging /api/sync/preview.
// Classic script.

/** @param {string} val */
function updateSyncThumbSize(val) {
  _syncThumbSize = parseInt(val);
  /** @type {HTMLElement} */ (document.getElementById('syncThumbSizeVal')).textContent = val + 'px';
  /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('.sync-preview-photo')).forEach(function(photo) {
    photo.style.width = _syncThumbSize + 'px';
  });
  /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('.sync-preview-thumb')).forEach(function(img) {
    img.style.width = _syncThumbSize + 'px';
    img.style.height = Math.round(_syncThumbSize * 2 / 3) + 'px';
  });
}

function _syncElapsedText() {
  if (!_syncPreviewStartedAt) return '';
  var seconds = Math.max(0, Math.round((Date.now() - _syncPreviewStartedAt) / 1000));
  if (seconds < 60) return seconds + 's elapsed';
  return Math.floor(seconds / 60) + 'm ' + (seconds % 60) + 's elapsed';
}

/** @param {boolean} enabled */
function _syncSetPreviewActionsEnabled(enabled) {
  var bottomDiscard = /** @type {HTMLButtonElement} */ (document.getElementById('syncPreviewDiscardAllButton'));
  var bottomSync = /** @type {HTMLButtonElement} */ (document.getElementById('syncPreviewSyncButton'));
  if (bottomDiscard) bottomDiscard.disabled = !enabled;
  if (bottomSync) bottomSync.disabled = !enabled;
  /** @type {NodeListOf<HTMLButtonElement>} */ (document.querySelectorAll('#syncPreviewContent .sync-preview-action')).forEach(function(button) {
    button.disabled = !enabled;
  });
}

/** @param {'loading' | 'complete' | 'error'} state @param {string=} errorMessage */
function _syncUpdatePreviewProgress(state, errorMessage) {
  var box = /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgress'));
  var text = /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgressText'));
  var percent = /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgressPercent'));
  var track = /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgressTrack'));
  var bar = /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgressBar'));
  var detail = /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgressDetail'));
  if (!box || !text || !percent || !track || !bar || !detail) return;
  box.style.display = 'block';

  var loaded = _syncPreviewData && _syncPreviewData.photos
    ? _syncPreviewData.photos.length : 0;
  var total = _syncPreviewData ? (_syncPreviewData.total_photos || 0) : 0;
  var pct = total > 0 ? Math.min(100, Math.round(loaded * 100 / total)) : 0;
  bar.classList.remove('is-preparing');
  if (total > 0) {
    track.setAttribute('aria-valuemax', String(total));
    track.setAttribute('aria-valuenow', String(loaded));
  } else {
    track.removeAttribute('aria-valuemax');
    track.removeAttribute('aria-valuenow');
  }

  if (state === 'error') {
    text.textContent = 'Could not finish preparing the review';
    text.style.color = 'var(--danger)';
    percent.textContent = '';
    bar.style.width = pct + '%';
    detail.textContent = errorMessage || 'Try closing and reopening the review.';
    return;
  }
  text.style.color = 'var(--text-primary)';
  if (state === 'complete') {
    text.textContent = total === 0
      ? 'No pending changes'
      : 'Review ready — ' + total.toLocaleString() + ' photos checked';
    percent.textContent = total > 0 ? '100%' : '';
    bar.style.width = total > 0 ? '100%' : '0';
    detail.textContent = total > 0
      ? 'All results are loaded. Sync and discard actions are now available. · ' + _syncElapsedText()
      : '';
    return;
  }
  if (total > 0) {
    text.textContent = 'Reviewing XMP metadata — ' + loaded.toLocaleString() + ' of ' + total.toLocaleString() + ' photos';
    percent.textContent = pct + '%';
    bar.style.width = pct + '%';
    detail.textContent = 'Completed results are available below while Vireo reads the next batch. · ' + _syncElapsedText();
  } else {
    text.textContent = 'Preparing pending-change summary...';
    percent.textContent = '';
    bar.style.width = '32%';
    bar.classList.add('is-preparing');
    detail.textContent = 'The first results will appear as soon as they are ready. · ' + _syncElapsedText();
  }
}

function _syncStopPreviewLoad() {
  _syncPreviewLoadToken++;
  if (_syncPreviewAbortController) {
    _syncPreviewAbortController.abort();
    _syncPreviewAbortController = null;
  }
  if (_syncPreviewElapsedTimer !== null) {
    clearInterval(_syncPreviewElapsedTimer);
    _syncPreviewElapsedTimer = null;
  }
  _syncPreviewLoading = false;
  _syncPreviewComplete = false;
}

/** Reset the dialog for a fresh load and start its elapsed-time ticker. @param {number} loadToken */
function _syncBeginPreviewLoad(loadToken) {
  _syncReviewWaiting = false;
  /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgressTrack')).setAttribute('aria-label', 'Preparing pending-change review');
  _syncPreviewAbortController = new AbortController();
  _syncPreviewStartedAt = Date.now();
  _syncPreviewDisplayLimit = 300;
  _syncPreviewLoading = true;
  _syncPreviewData = null;
  _syncSetPreviewActionsEnabled(false);
  /** @type {HTMLElement} */ (document.getElementById('syncPreviewContent')).innerHTML =
    '<p style="color:var(--text-dim);padding:20px;">Waiting for the first results...</p>';
  _syncUpdatePreviewProgress('loading');
  _syncPreviewElapsedTimer = setInterval(function() {
    if (_syncPreviewLoading && loadToken === _syncPreviewLoadToken) {
      _syncUpdatePreviewProgress('loading');
    }
  }, 1000);
}

/** The review snapshot the first page of /api/sync/preview describes. @param {SyncPreviewPage} page @returns {SyncPreviewData} */
function _syncPreviewFromFirstPage(page) {
  return {
    photos: [],
    total_changes: page.total_changes,
    total_photos: page.total_photos,
    change_type_counts: page.change_type_counts || {},
    location_sync_enabled: page.location_sync_enabled,
    location_keyword_sync_enabled: page.location_keyword_sync_enabled,
    revision: page.revision,
  };
}

/** The last page arrived: enable the actions and stop the ticker. @param {SyncPreviewData} data */
function _syncFinishPreviewLoad(data) {
  _syncSetPreviewActionsEnabled(data.total_changes > 0);
  if (_syncPreviewElapsedTimer !== null) {
    clearInterval(_syncPreviewElapsedTimer);
    _syncPreviewElapsedTimer = null;
  }
}

/**
 * A preview request failed. A sync that started meanwhile, or a queue that
 * changed under the snapshot, restarts the load once; anything else shows
 * the error. Returns the restarted load, if any.
 *
 * @param {unknown} caught @param {number} loadToken @param {number} restartCount
 * @returns {Promise<void>}
 */
async function _syncPreviewLoadFailed(caught, loadToken, restartCount) {
  var e = /** @type {(Error & {code?: string}) | null} */ (caught);
  if (loadToken !== _syncPreviewLoadToken || (e && e.name === 'AbortError')) return;
  if (e && e.code === 'sync_in_progress') {
    var status = await checkPendingSync();
    if (loadToken !== _syncPreviewLoadToken || !_syncModalOpen) return;
    if (status && status.active_job) return;
    if (status && restartCount < 1) return _syncStartPreviewLoad(restartCount + 1);
  }
  if (e && e.code === 'sync_preview_changed' && restartCount < 1) {
    /** @type {HTMLElement} */ (document.getElementById('syncPreviewProgressDetail')).textContent =
      'Pending changes were updated; restarting with a fresh snapshot...';
    return _syncStartPreviewLoad(restartCount + 1);
  }
  _syncPreviewLoading = false;
  _syncSetPreviewActionsEnabled(false);
  _syncUpdatePreviewProgress('error', e ? e.message : undefined);
}

/** @param {number=} restartCount @returns {Promise<void>} */
async function _syncStartPreviewLoad(restartCount) {
  restartCount = restartCount || 0;
  _syncStopPreviewLoad();
  var loadToken = _syncPreviewLoadToken;
  _syncBeginPreviewLoad(loadToken);
  var signal = /** @type {AbortController} */ (_syncPreviewAbortController).signal;

  var offset = 0;
  /** @type {string | null} */
  var revision = null;
  var firstPage = true;
  try {
    var summary = /** @type {SyncStatusSummary} */ (await safeFetch('/api/sync/status', {
      signal: signal,
    }, { toast: false }));
    if (loadToken !== _syncPreviewLoadToken || !_syncModalOpen) return;
    if (summary.active_job) {
      _syncShowActiveJob(summary.active_job);
      return;
    }
    _syncPreviewData = {
      photos: [],
      total_changes: summary.pending_count || 0,
      total_photos: summary.pending_photo_count || 0,
      change_type_counts: summary.change_type_counts || {},
    };
    _syncActiveFilters = new Set(Object.keys(_syncPreviewData.change_type_counts));
    renderSyncPreview();
    _syncUpdatePreviewProgress('loading');

    while (_syncModalOpen) {
      var pageSize = firstPage ? 25 : 100;
      var url = '/api/sync/preview?limit=' + pageSize + '&offset=' + offset;
      if (revision) url += '&revision=' + encodeURIComponent(revision);
      var page = /** @type {SyncPreviewPage} */ (await safeFetch(url, {
        signal: signal,
      }, { toast: false }));
      if (loadToken !== _syncPreviewLoadToken || !_syncModalOpen) return;

      if (firstPage) {
        revision = page.revision;
        _syncPreviewData = _syncPreviewFromFirstPage(page);
        _syncActiveFilters = new Set(Object.keys(_syncPreviewData.change_type_counts));
      }
      var data = /** @type {SyncPreviewData} */ (_syncPreviewData);
      Array.prototype.push.apply(data.photos, page.photos || []);
      offset = page.next_offset;
      firstPage = false;
      if (!page.has_more) {
        _syncPreviewLoading = false;
        _syncPreviewComplete = true;
      }
      renderSyncPreview();
      _syncUpdatePreviewProgress(page.has_more ? 'loading' : 'complete');

      if (!page.has_more) {
        _syncFinishPreviewLoad(data);
        return;
      }
      // Let WebKit paint the completed batch before requesting the next one.
      await new Promise(function(resolve) { setTimeout(resolve, 0); });
    }
  } catch(e) {
    return _syncPreviewLoadFailed(e, loadToken, restartCount);
  }
}

async function openSyncPreview() {
  VireoViewPreferences.restore(/** @type {HTMLInputElement} */ (document.getElementById('syncThumbSize')));
  updateSyncThumbSize(/** @type {HTMLInputElement} */ (document.getElementById('syncThumbSize')).value);
  var alreadyOpen = _syncModalOpen;
  if (_syncEscToken !== null) Keymap.popEsc(_syncEscToken);
  _syncEscToken = Keymap.pushEsc(function() { closeSyncPreview(); });
  if (!alreadyOpen) Keymap.lockBodyScroll();
  _syncActiveFilters = null;
  _syncModalOpen = true;
  var overlay = /** @type {HTMLElement} */ (document.getElementById('syncPreviewOverlay'));
  overlay.style.display = 'block';
  await _syncStartPreviewLoad(0);
}
