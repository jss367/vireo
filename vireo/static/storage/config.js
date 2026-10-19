// The cache settings: loading /api/config into the controls and the debounced
// autosave of everything except the working-copy quota, plus the shared
// working-copy quota state.
// Classic page script.

/** @type {ReturnType<typeof setTimeout> | null} */
var _storageSaveTimer = null;
var _committedWorkingCopyQuotaMb = 20480;
var MAX_WORKING_COPY_QUOTA_MB = 1048576;
// null = current working-copy usage has not yet been measured.
/** @type {number | null} */
var _workingCopySizeBytes = null;
/** @type {number | null} */
var _pendingWorkingCopyQuotaMb = null;

async function loadStorageConfig() {
  try {
    var cfg = /** @type {StorageConfigValues} */ (await safeFetch('/api/config', {}, { toast: false }));
    var ts = String(cfg.thumbnail_size != null ? cfg.thumbnail_size : 400);
    /** @type {HTMLInputElement} */ (document.getElementById('cfgThumbSize')).value = ts;
    /** @type {HTMLElement} */ (document.getElementById('cfgThumbSizeVal')).textContent = ts + 'px';
    var tq = String(cfg.thumbnail_quality != null ? cfg.thumbnail_quality : 85);
    /** @type {HTMLInputElement} */ (document.getElementById('cfgThumbQuality')).value = tq;
    /** @type {HTMLElement} */ (document.getElementById('cfgThumbQualityVal')).textContent = tq;
    var pq = String(cfg.preview_quality != null ? cfg.preview_quality : 90);
    /** @type {HTMLInputElement} */ (document.getElementById('cfgPreviewQuality')).value = pq;
    /** @type {HTMLElement} */ (document.getElementById('cfgPreviewQualityVal')).textContent = pq;
    /** @type {HTMLInputElement} */ (document.getElementById('cfgPreviewCacheMaxMb')).value =
      String(cfg.preview_cache_max_mb != null ? cfg.preview_cache_max_mb : 20480);
    _committedWorkingCopyQuotaMb =
      cfg.working_copy_cache_max_mb != null ? cfg.working_copy_cache_max_mb : 20480;
    /** @type {HTMLInputElement} */ (document.getElementById('cfgWorkingCopyCacheMaxGb')).value =
      String(_committedWorkingCopyQuotaMb / 1024);
    updateWorkingCopyLimitState();
  } catch(e) {}
}

function readWorkingCopyQuotaMb() {
  var input = /** @type {HTMLInputElement} */ (document.getElementById('cfgWorkingCopyCacheMaxGb'));
  var gb = Number(input.value);
  if (!Number.isFinite(gb) || gb < 0 || input.value.trim() === '') return null;
  var quotaMb = Math.round(gb * 1024);
  if (!Number.isFinite(quotaMb) || quotaMb > MAX_WORKING_COPY_QUOTA_MB) return null;
  return quotaMb;
}

function updateWorkingCopyLimitState() {
  var quotaMb = readWorkingCopyQuotaMb();
  var input = /** @type {HTMLInputElement} */ (document.getElementById('cfgWorkingCopyCacheMaxGb'));
  var applyBtn = /** @type {HTMLButtonElement} */ (document.getElementById('applyWorkingCopyLimitBtn'));
  var valid = quotaMb != null;
  input.setAttribute('aria-invalid', valid ? 'false' : 'true');
  applyBtn.disabled = !valid || quotaMb === _committedWorkingCopyQuotaMb;
}

// Autosaved storage controls — everything EXCEPT the working-copy quota.
// The quota has its own explicit Apply flow (postWorkingCopyLimit); mixing
// it into the autosave payload would let a debounced autosave that fires
// while commitWorkingCopyLimit is in flight silently POST the old
// _committedWorkingCopyQuotaMb and undo the quota change on the server.
function autosaveStorageConfigPayload() {
  return {
    thumbnail_size: parseInt(/** @type {HTMLInputElement} */ (document.getElementById('cfgThumbSize')).value, 10) || 400,
    thumbnail_quality: parseInt(/** @type {HTMLInputElement} */ (document.getElementById('cfgThumbQuality')).value, 10) || 85,
    preview_quality: parseInt(/** @type {HTMLInputElement} */ (document.getElementById('cfgPreviewQuality')).value, 10) || 90,
    preview_cache_max_mb: Math.max(0, parseInt(/** @type {HTMLInputElement} */ (document.getElementById('cfgPreviewCacheMaxMb')).value, 10) || 0),
  };
}

async function postAutosaveStorageConfig() {
  await safeFetch('/api/config', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(autosaveStorageConfigPayload()),
  });
}

/** @param {number} workingCopyQuotaMb @param {{confirmed?: boolean}=} opts */
async function postWorkingCopyLimit(workingCopyQuotaMb, opts) {
  opts = opts || {};
  /** @type {{working_copy_cache_max_mb: number, _confirm_working_copy_eviction?: boolean}} */
  var payload = {working_copy_cache_max_mb: workingCopyQuotaMb};
  if (opts.confirmed) payload._confirm_working_copy_eviction = true;
  // Suppress the default toast: commitWorkingCopyLimit routes the
  // 409 confirmation-required response back into the modal flow, and
  // shows its own message for other failures.
  await safeFetch('/api/config', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload),
  }, { toast: false });
}

function saveStorageConfig() {
  if (_storageSaveTimer) clearTimeout(_storageSaveTimer);
  _storageSaveTimer = setTimeout(async function() {
    _storageSaveTimer = null;
    try {
      await postAutosaveStorageConfig();
      loadPreviewCacheStatus();
      loadStoragePanel();
    } catch(e) {}
  }, 250);
}

// If a debounced autosave for other storage controls is queued but has not
// yet fired, POST its payload synchronously before returning. The
// working-copy Apply flow used to `clearTimeout(_storageSaveTimer)` on
// entry, which silently dropped any preview-cache or thumbnail edit from
// the last 250 ms; flushing keeps those edits reaching the server.
async function flushPendingStorageAutosave() {
  if (_storageSaveTimer == null) return;
  clearTimeout(_storageSaveTimer);
  _storageSaveTimer = null;
  try {
    await postAutosaveStorageConfig();
  } catch(e) {
    // Keep the unrelated edit queued for retry and tell the Apply flow to
    // stop. Proceeding would both lose that edit and send a quota snapshot
    // whose surrounding storage settings are stale.
    saveStorageConfig();
    throw e;
  }
}
