// The storage overview: totals, free space, locations, the working-copy
// status, the category cards, Refresh, Clear safe caches and Open folder.

import { state } from './state.js';
import {
  loadDetectionCacheStats, loadEmbeddingCache, loadEmbeddingMatrix, loadPreviewCacheStatus,
} from './caches.js';
import { readWorkingCopyQuotaMb, updateWorkingCopyLimitState } from './config.js';
import { storageEscapeAttr, storageEscapeHtml, storageFormatBytes } from './format.js';
import { hfCacheCardNote } from './hf-cache.js';
import { loadMasksCard } from './masks.js';
import { renderStorageCards, showStorageLoadError } from './overview.js';
import { loadRetainedStorage } from './retained.js';
import { formatTime, safeFetch, showToast } from './shared.js';

/** @param {string} id */
export function storageEl(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
}

/** Free space on the tightest volume, and a warning for every low one. @param {StorageReport} s */
export function renderStorageVolumes(s) {
  var volumes = s.volumes || [];
  var measuredVolumes = volumes.filter(function(v) { return v.free != null; });
  measuredVolumes.sort(function(a, b) { return /** @type {number} */ (a.free) - /** @type {number} */ (b.free); });
  /** @type {Partial<StorageVolume>} */
  var tightest = measuredVolumes[0] || {};
  storageEl('storageFreeSize').textContent = storageFormatBytes(tightest.free);
  storageEl('storageVolumeName').textContent = tightest.name
    ? (volumes.length > 1 ? volumes.length + ' volumes · tightest is ' : '') + tightest.name +
      (tightest.capacity != null ? ' · ' + storageFormatBytes(tightest.capacity) + ' capacity' : '')
    : 'Volume information unavailable';
  var lowVolumes = measuredVolumes.filter(function(v) {
    var free = /** @type {number} */ (v.free);
    return v.capacity != null &&
      (free < 10 * 1024 * 1024 * 1024 || free / v.capacity < 0.10);
  });
  var lowSpaceEl = storageEl('storageLowSpaceWarning');
  if (lowVolumes.length) {
    lowSpaceEl.textContent = 'Storage is running low on ' + lowVolumes.map(function(v) {
      return (v.name || 'a storage volume') + ' (' + storageFormatBytes(v.free) + ' free)';
    }).join(', ') + '.';
    lowSpaceEl.style.display = '';
  } else {
    lowSpaceEl.style.display = 'none';
  }
}

/** The storage locations, each with an Open folder button (data-storage-location). @param {StorageReport} s */
export function renderStorageLocations(s) {
  var locationsHtml = '';
  (s.locations || []).forEach(function(location) {
    var locationVolume = location.volume || {};
    locationsHtml += '<div class="storage-location">' +
      '<span style="font-size:11px;color:var(--text-dim);">' +
        storageEscapeHtml((location.labels || []).join(' · ')) + '</span>' +
      '<code>' + storageEscapeHtml(location.path || '') + '</code>' +
      '<span style="font-size:10px;color:var(--text-ghost);">' +
        storageEscapeHtml(locationVolume.name || '') + ' · ' + storageFormatBytes(locationVolume.free) + ' free</span>' +
      '<button class="btn-secondary" data-storage-location="' + storageEscapeAttr(location.id) + '" style="margin-left:auto;">Open folder</button>' +
      '</div>';
  });
  storageEl('storageLocations').innerHTML = locationsHtml;
}

/** @param {StorageWorkingCopies} working */
export function syncWorkingCopyStatus(working) {
  state.workingCopySizeBytes = working.size || 0;
  // Sync the cached "committed" quota from the server's authoritative
  // value so a concurrent change (another tab, All Settings) is
  // reflected in the Apply button state. Without this, an outside
  // reduction leaves the input showing the old higher value and Apply
  // disabled — the user can't restore that displayed limit without
  // perturbing the field or reloading. Only overwrite the input value
  // if it still matches the previously-known committed quota (the
  // user hasn't edited it) and no confirmation modal is open, so
  // in-progress edits and the pending-reduction dialog aren't
  // clobbered by a background refresh.
  if (working.quota_bytes != null) {
    var newCommittedMb = Math.round(working.quota_bytes / (1024 * 1024));
    if (newCommittedMb !== state.committedWorkingCopyQuotaMb) {
      var input = /** @type {HTMLInputElement} */ (storageEl('cfgWorkingCopyCacheMaxGb'));
      var currentInputMb = readWorkingCopyQuotaMb();
      var modal = /** @type {HTMLDialogElement | null} */ (document.getElementById('workingCopyLimitModal'));
      var modalOpen = modal && (modal.open || modal.classList.contains('open'));
      if (!modalOpen && currentInputMb === state.committedWorkingCopyQuotaMb) {
        input.value = String(newCommittedMb / 1024);
      }
      state.committedWorkingCopyQuotaMb = newCommittedMb;
    }
  }
  updateWorkingCopyLimitState();
  var workingFileWord = working.count === 1 ? 'file' : 'files';
  storageEl('workingCopyCacheStatus').textContent =
    'Current: ' + storageFormatBytes(working.size) + ' / ' +
    storageFormatBytes(working.quota_bytes) + ' (' +
    working.count.toLocaleString() + ' ' + workingFileWord + ')';
}

/** The category cards, the main caches first, then the auxiliary files. @param {StorageReport} s */
export function storageCategories(s) {
  /** @type {StorageOverviewCategory[]} */
  var categories = [
    {name: 'Database', size: s.database.size, type: null, kind: 'keep', kindLabel: 'Keep'},
    {name: 'Thumbnails', size: s.thumbnails.size, count: s.thumbnails.count, type: 'thumbnails', kind: 'safe', kindLabel: 'Safe to clear'},
    {name: 'Previews', size: s.previews.size, count: s.previews.count, type: 'previews', kind: 'safe', kindLabel: 'Safe to clear'},
    {name: 'Working Copies', size: s.working_copies.size, count: s.working_copies.count, type: null, kind: 'managed', kindLabel: 'Quota managed'},
    {name: 'Label Embeddings', size: s.embeddings.size, count: s.embeddings.count, type: 'embeddings', kind: 'safe', kindLabel: 'Safe to clear'},
    {name: 'Models', size: s.models.size, type: null, kind: 'download', kindLabel: 'Download again', settings: true},
    {name: 'Hugging Face Cache', size: s.hf_cache.size, type: null, kind: 'download', kindLabel: 'Download again', hfCache: true, note: hfCacheCardNote(s.hf_cache)},
    {name: 'Offline Originals', size: s.offline_originals.size, count: s.offline_originals.count, release: true, type: null, kind: 'keep', kindLabel: 'Release when done'},
  ];
  (s.other.categories || []).forEach(function(c) {
    categories.push({name: c.name, size: c.size, kind: 'keep', kindLabel: 'Review before removing'});
  });
  return categories;
}

/** The auxiliary files and folders, largest first in each category. @param {StorageAuxiliaryCategory[]} auxiliaryCategories */
export function renderStorageBreakdown(auxiliaryCategories) {
  var breakdownHtml = auxiliaryCategories.length
    ? '<p class="storage-legend">Files and folders below are sorted by size within each category. Sizes exclude data already counted in the main cache categories.</p>'
    : '';
  auxiliaryCategories.forEach(function(c) {
    breakdownHtml += '<details><summary>' + storageEscapeHtml(c.name) +
      ' · ' + storageFormatBytes(c.size) + ' · View files and folders</summary>';
    c.entries.forEach(function(entry) {
      breakdownHtml += '<div class="storage-breakdown-row"><strong>' +
        storageFormatBytes(entry.size) + '</strong><p>' +
        storageEscapeHtml(entry.description) + '</p><code>' +
        storageEscapeHtml(entry.path) + '</code></div>';
    });
    breakdownHtml += '</details>';
  });
  storageEl('storageBreakdown').innerHTML = breakdownHtml;
}

export async function loadStoragePanel() {
  /** @type {StorageReport | undefined} */
  var s;
  try {
    s = /** @type {StorageReport} */ (await safeFetch('/api/storage', {}, { toast: false }));
    storageEl('storageTotalSize').textContent = storageFormatBytes(s.total);
    storageEl('storageReclaimableSize').textContent = storageFormatBytes(s.reclaimable);
    renderStorageVolumes(s);
    renderStorageLocations(s);
    var clearSafeBtn = /** @type {HTMLButtonElement} */ (storageEl('clearSafeCachesBtn'));
    clearSafeBtn.disabled = !s.reclaimable;
    clearSafeBtn.textContent = s.reclaimable
      ? 'Clear safe caches · ' + storageFormatBytes(s.reclaimable)
      : 'Safe caches are empty';
    syncWorkingCopyStatus(s.working_copies || {count: 0, size: 0, quota_bytes: 0});
    var categories = storageCategories(s);
    renderStorageBreakdown(s.other.categories || []);
    renderStorageCards(categories);
  } catch(e) {
    // Usage is unknown until the next successful scan; keep Apply disabled
    // so a stale zero can't skip the eviction confirmation.
    state.workingCopySizeBytes = null;
    updateWorkingCopyLimitState();
    showStorageLoadError();
  }
  await loadMasksCard(s && s.masks);
}

export async function refreshStoragePage() {
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('refreshStorageBtn'));
  if (btn) btn.disabled = true;
  await Promise.all([
    loadStoragePanel(), loadPreviewCacheStatus(), loadDetectionCacheStats(),
    loadEmbeddingMatrix(), loadEmbeddingCache(), loadRetainedStorage(),
  ]);
  /** @type {HTMLElement} */ (document.getElementById('storageUpdatedAt')).textContent =
    'Updated ' + formatTime(new Date(), {hour: 'numeric', minute: '2-digit', second: '2-digit'});
  if (btn) btn.disabled = false;
}

export async function clearSafeCaches() {
  var size = /** @type {HTMLElement} */ (document.getElementById('storageReclaimableSize')).textContent;
  if (!confirm('Clear ' + size + ' of thumbnails, previews, and label embeddings? They will regenerate as needed.')) return;
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('clearSafeCachesBtn'));
  btn.disabled = true;
  btn.textContent = 'Clearing…';
  try {
    await safeFetch('/api/storage/clear-safe', {method: 'POST'});
    showToast('Safe caches cleared', 'success');
  } catch(e) {
    showToast('Could not clear all safe caches', 'error');
  }
  await refreshStoragePage();
}

/** @param {string} locationId */
export async function openStorageFolder(locationId) {
  try {
    var result = /** @type {{ok?: boolean, reason?: string}} */ (await safeFetch('/api/storage/open-folder', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({location: locationId}),
    }));
    if (!result.ok) throw new Error(result.reason || 'Could not open storage folder');
  } catch(e) {
    showToast((e instanceof Error && e.message) || 'Could not open storage folder', 'error');
  }
}
