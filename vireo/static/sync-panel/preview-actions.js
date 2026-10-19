// Closing the dialog, the visible change ids, Sync to XMP for those,
// discarding changes, and the change-type filters.
// Classic script.

function closeSyncPreview() {
  var wasOpen = _syncModalOpen;
  _syncReviewWaiting = false;
  _syncStopPreviewLoad();
  if (_syncEscToken !== null) {
    Keymap.popEsc(_syncEscToken);
    _syncEscToken = null;
  }
  /** @type {HTMLElement} */ (document.getElementById('syncPreviewOverlay')).style.display = 'none';
  _syncModalOpen = false;
  _syncDestroyLightboxPanel();
  if (wasOpen) Keymap.unlockBodyScroll();
  checkPendingSync();
}

function getVisibleSyncChangeIds() {
  /** @type {number[]} */
  var ids = [];
  var filters = _syncActiveFilters;
  if (!_syncPreviewData || !_syncPreviewData.photos || !filters) return ids;
  _syncPreviewData.photos.forEach(function(p) {
    p.changes.forEach(function(c) {
      if (/** @type {Set<string>} */ (filters).has(c.type)) ids.push(c.id);
    });
  });
  return ids;
}

function runVisibleSync() {
  if (!_syncPreviewComplete) return;
  var changeIds = getVisibleSyncChangeIds();
  if (changeIds.length === 0) {
    var status = /** @type {HTMLElement} */ (document.getElementById('syncStatus'));
    status.textContent = 'No checked changes to sync.';
    status.style.color = 'var(--text-dim)';
    return;
  }
  closeSyncPreview();
  runSync(changeIds);
}

// Reload the progressive preview after a discard so derived flags (creates_xmp_sidecar,
// paired_keyword_rename) and per-change presentations reflect the new state.
// Discarding one half of a same-key keyword_add/keyword_remove pair
// invalidates the surviving remove's rename-pairing flags, and rating
// previews rely on those flags to decide whether a sidecar will be
// created; without the refetch, a rating queued alongside such a pair
// can look like it will be written and then be silently dropped by
// sync_to_xmp.
async function _syncRefreshPreviewAfterDiscard() {
  if (!_syncModalOpen) return;
  await _syncStartPreviewLoad(0);
}

/** @param {number[]} changeIds */
async function discardSyncChanges(changeIds) {
  if (!_syncPreviewComplete || !changeIds || changeIds.length === 0) return;
  try {
    await safeFetch('/api/sync/discard', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({change_ids: changeIds}),
    });
    await _syncRefreshPreviewAfterDiscard();
  } catch(e) {}
}

/** @param {number} changeId */
function discardChange(changeId) {
  return discardSyncChanges([changeId]);
}

/** @param {number[]} changeIds */
function discardPhotoChanges(changeIds) {
  return discardSyncChanges(changeIds);
}

async function discardAllChanges() {
  if (!_syncPreviewComplete || !_syncPreviewData) return;
  if (!confirm('Discard ALL pending changes? Nothing will be written to XMP.')) return;
  try {
    await safeFetch('/api/sync/discard', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        discard_all: true,
        revision: _syncPreviewData.revision,
      }),
    });
    closeSyncPreview();
  } catch(caught) {
    var e = /** @type {SyncRequestError | null} */ (caught);
    if (e && e.code === 'sync_preview_changed' && _syncModalOpen) {
      await _syncStartPreviewLoad(0);
    }
  }
}

/** @param {string} type */
function toggleSyncFilter(type) {
  var filters = _syncActiveFilters || (_syncActiveFilters = new Set());
  if (filters.has(type)) {
    filters.delete(type);
  } else {
    filters.add(type);
  }
  _syncPreviewDisplayLimit = 300;
  renderSyncPreview();
}
