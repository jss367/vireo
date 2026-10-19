// The working-copy quota's explicit Apply flow: measuring usage, the
// reduction confirmation modal, and committing the new limit (rerouting a
// server-side 409 back into the modal).
// Classic page script.

// Fetch a fresh working-copy usage snapshot. Returns the size in bytes on
// success, or null if the /api/storage scan failed. Keeps
// _workingCopySizeBytes and the Apply button state in sync with the result.
async function refreshWorkingCopyUsage() {
  try {
    var s = /** @type {Partial<StorageReport> | null} */ (await safeFetch('/api/storage', {}, { toast: false }));
    var working = s && s.working_copies ? s.working_copies : null;
    if (!working) {
      _workingCopySizeBytes = null;
      return null;
    }
    _workingCopySizeBytes = working.size || 0;
    return _workingCopySizeBytes;
  } catch(e) {
    _workingCopySizeBytes = null;
    return null;
  } finally {
    updateWorkingCopyLimitState();
  }
}

async function applyWorkingCopyLimit() {
  var quotaMb = readWorkingCopyQuotaMb();
  if (quotaMb == null) {
    showToast('Enter a valid working-copy storage limit', 'error');
    return;
  }
  if (quotaMb === _committedWorkingCopyQuotaMb) return;

  // Any reduction the client can already see needs the confirmation modal.
  // For a raise, POST straight through: if _committedWorkingCopyQuotaMb is
  // stale and the server sees a reduction, /api/config rejects with 409
  // and commitWorkingCopyLimit reroutes to the modal below.
  if (quotaMb > _committedWorkingCopyQuotaMb) {
    await commitWorkingCopyLimit(quotaMb, { confirmed: false });
    return;
  }
  await promptWorkingCopyLimit(quotaMb);
}

// Open the eviction confirmation modal for a proposed reduction. When the
// server has already measured usage under the publication guard (the 409
// response body), reuse that value instead of racing another /api/storage
// scan against a concurrent publication.
/** @param {number} quotaMb @param {{usageBytes?: number | null}=} opts */
async function promptWorkingCopyLimit(quotaMb, opts) {
  opts = opts || {};
  var applyBtn = /** @type {HTMLButtonElement} */ (document.getElementById('applyWorkingCopyLimitBtn'));
  var input = /** @type {HTMLInputElement} */ (document.getElementById('cfgWorkingCopyCacheMaxGb'));
  applyBtn.disabled = true;
  input.disabled = true;
  var usage;
  if (opts.usageBytes != null) {
    _workingCopySizeBytes = opts.usageBytes;
    usage = opts.usageBytes;
  } else {
    applyBtn.textContent = 'Checking…';
    usage = await refreshWorkingCopyUsage();
  }
  var quotaBytes = quotaMb * 1024 * 1024;
  var removalBytes = usage == null ? null : Math.max(0, usage - quotaBytes);
  var newLimit = storageFormatBytes(quotaBytes);
  input.disabled = true;
  applyBtn.textContent = 'Apply';
  updateWorkingCopyLimitState();
  _pendingWorkingCopyQuotaMb = quotaMb;
  var desc = /** @type {HTMLElement} */ (document.getElementById('workingCopyLimitModalDesc'));
  var confirmBtn = /** @type {HTMLButtonElement} */ (document.getElementById('confirmWorkingCopyLimitBtn'));
  if (removalBytes != null && removalBytes > 0) {
    desc.textContent =
      'Lowering the limit to ' + newLimit + ' will remove approximately ' +
      storageFormatBytes(removalBytes) + ' of the oldest generated working copies. ' +
      'Your original photos will not be deleted. Vireo will use the originals when a working copy is unavailable.';
    confirmBtn.textContent = 'Lower limit and remove copies';
  } else {
    var usageNote = usage == null
      ? 'Current working-copy usage could not be measured. '
      : 'No working copies currently exceed the new limit. ';
    desc.textContent = usageNote + 'If usage is above ' + newLimit +
      ' when the change is applied, Vireo will remove the oldest generated working copies. ' +
      'Your original photos will not be deleted.';
    confirmBtn.textContent = 'Lower storage limit';
  }
  var modal = /** @type {HTMLDialogElement} */ (document.getElementById('workingCopyLimitModal'));
  modal.classList.add('open');
  modal.showModal();
  confirmBtn.focus();
}

function closeWorkingCopyLimitModal() {
  var modal = /** @type {HTMLDialogElement} */ (document.getElementById('workingCopyLimitModal'));
  if (modal.open) modal.close();
  modal.classList.remove('open');
  _pendingWorkingCopyQuotaMb = null;
  /** @type {HTMLInputElement} */ (document.getElementById('cfgWorkingCopyCacheMaxGb')).disabled = false;
  updateWorkingCopyLimitState();
  /** @type {HTMLButtonElement} */ (document.getElementById('applyWorkingCopyLimitBtn')).focus();
}

function confirmWorkingCopyLimit() {
  var quotaMb = _pendingWorkingCopyQuotaMb;
  var modal = /** @type {HTMLDialogElement} */ (document.getElementById('workingCopyLimitModal'));
  if (modal.open) modal.close();
  modal.classList.remove('open');
  _pendingWorkingCopyQuotaMb = null;
  if (quotaMb != null) commitWorkingCopyLimit(quotaMb, { confirmed: true });
}

/** @param {number} quotaMb @param {{confirmed?: boolean}=} opts */
async function commitWorkingCopyLimit(quotaMb, opts) {
  opts = opts || {};
  var input = /** @type {HTMLInputElement} */ (document.getElementById('cfgWorkingCopyCacheMaxGb'));
  var applyBtn = /** @type {HTMLButtonElement} */ (document.getElementById('applyWorkingCopyLimitBtn'));
  try {
    await flushPendingStorageAutosave();
  } catch(e) {
    input.disabled = false;
    applyBtn.textContent = 'Apply';
    updateWorkingCopyLimitState();
    showToast(
      'Could not save pending storage changes; the working-copy limit was not updated',
      'error'
    );
    return;
  }
  input.disabled = true;
  applyBtn.disabled = true;
  applyBtn.textContent = 'Applying…';
  var reroutedToModal = false;
  try {
    await postWorkingCopyLimit(quotaMb, opts);
    _committedWorkingCopyQuotaMb = quotaMb;
    input.value = String(quotaMb / 1024);
    showToast('Working-copy storage limit updated', 'success');
    await refreshStoragePage();
  } catch(caught) {
    var e = /** @type {StorageRequestError} */ (caught);
    if (
      !opts.confirmed && e && e.status === 409 &&
      e.code === 'working_copy_eviction_confirmation_required'
    ) {
      // The server sees this as a reduction — either because the client's
      // cached committed quota was stale (another tab raised it), or
      // because the storage page was loaded before the current stored
      // value. Sync the cached committed value to the server's truth so
      // the Apply button state reflects reality, then re-prompt with the
      // server's usage measurement.
      var body = e.body || {};
      if (typeof body.previous_working_copy_quota_mb === 'number') {
        _committedWorkingCopyQuotaMb = body.previous_working_copy_quota_mb;
      }
      reroutedToModal = true;
      input.disabled = false;
      applyBtn.textContent = 'Apply';
      updateWorkingCopyLimitState();
      await promptWorkingCopyLimit(quotaMb, {
        usageBytes:
          typeof body.current_working_copy_usage_bytes === 'number'
            ? body.current_working_copy_usage_bytes
            : null,
      });
    } else {
      showToast('Could not update working-copy storage limit', 'error');
    }
  } finally {
    if (!reroutedToModal) {
      input.disabled = false;
      applyBtn.textContent = 'Apply';
      updateWorkingCopyLimitState();
    }
  }
}
