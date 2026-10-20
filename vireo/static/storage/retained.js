/* Storage for retained generated payloads and grouping undo snapshots. */
/** @type {RetainedStorageResponse | null} */ var retainedStorageData = null;

async function loadRetainedStorage() {
  var panel = /** @type {HTMLElement} */ (document.getElementById('retainedStorageItems'));
  try {
    /** @type {RetainedStorageResponse} */ var data = await safeFetch('/api/storage/retained', {}, {toast: false});
    retainedStorageData = data;
    if (data.maintenance) {
      /** @type {HTMLElement} */ (document.getElementById('retainedStorageStatus')).textContent = 'Latest storage maintenance: ' +
        data.maintenance.status + ' · ' + window.VireoTime.dateTime(new Date(data.maintenance.finished_at || data.maintenance.started_at)) +
        (data.maintenance.summary ? ' · ' + data.maintenance.summary : '');
    }
    panel.replaceChildren();
    data.caches.forEach(function(cache) {
      var item = document.createElement('details');
      item.className = 'storage-retained-item';
      var title = document.createElement('summary');
      title.textContent = cache.name + ' · ' + storageFormatBytes(cache.size) + ' · ' + cache.count.toLocaleString() + ' files';
      if (cache.errors.length) title.textContent += ' · usage incomplete';
      item.appendChild(title);
      var path = document.createElement('code');
      path.textContent = cache.path;
      item.appendChild(path);
      var clear = document.createElement('button');
      clear.className = 'btn-secondary';
      clear.textContent = 'Clear ' + cache.name.toLowerCase();
      clear.disabled = !cache.count;
      clear.onclick = function() { clearRetainedStorage(cache.type, cache.name, clear); };
      if (cache.clearable) item.appendChild(clear);
      else {
        var preserved = document.createElement('p');
        preserved.textContent = 'Preserved: external editors can save unique changes here. Save a separate copy before removing files manually.';
        item.appendChild(preserved);
      }
      var open = document.createElement('button');
      open.className = 'btn-secondary';
      open.textContent = 'Open folder';
      open.onclick = function() { openStorageFolder('retained-' + cache.type); };
      item.appendChild(open);
      var files = document.createElement('div');
      files.className = 'storage-retained-files';
      cache.files.forEach(function(file) {
        var line = document.createElement('p');
        line.textContent = file.name + ' · ' + storageFormatBytes(file.size);
        files.appendChild(line);
      });
      item.appendChild(files);
      cache.errors.forEach(function(error) {
        var warning = document.createElement('p');
        warning.textContent = error;
        item.appendChild(warning);
      });
      if (cache.truncated) {
        var notice = document.createElement('p');
        notice.textContent = 'Showing the first ' + cache.files.length + ' files. Totals include all matching generated payloads.';
        item.appendChild(notice);
      }
      panel.appendChild(item);
    });
    var history = data.grouping_history;
    var historyDetails = document.createElement('details');
    historyDetails.className = 'storage-retained-item';
    var historyTitle = document.createElement('summary');
    historyTitle.textContent = 'Grouping undo snapshots · ' + storageFormatBytes(history.bytes) +
      ' · ' + history.count.toLocaleString() + ' snapshots';
    historyDetails.appendChild(historyTitle);
    var historyNote = document.createElement('p');
    historyNote.textContent = 'These snapshot bytes are part of the catalog database. ' + storageFormatBytes(history.redo_bytes) +
      ' awaits redo and is protected from automatic pruning. Clearing removes grouping undo and redo; current grouping and ordinary photo edits remain.';
    historyDetails.appendChild(historyNote);
    var historyClear = document.createElement('button');
    historyClear.className = 'btn-secondary';
    historyClear.textContent = 'Clear grouping undo and redo snapshots';
    historyClear.disabled = !history.count;
    historyClear.onclick = function() { clearRetainedStorage('grouping-history', 'Grouping undo snapshots', historyClear); };
    historyDetails.appendChild(historyClear);
    history.entries.forEach(function(entry) {
      var line = document.createElement('p');
      line.textContent = entry.workspace + ' · ' + entry.description + ' · ' + storageFormatBytes(entry.bytes) +
        (entry.undone ? ' · awaiting redo' : '');
      historyDetails.appendChild(line);
    });
    if (history.truncated) {
      var more = document.createElement('p');
      more.textContent = 'Showing the newest ' + history.entries.length + ' snapshots; totals include all workspaces.';
      historyDetails.appendChild(more);
    }
    panel.appendChild(historyDetails);
    // Preserve unsaved policy inputs during a refresh.
    if (!/** @type {HTMLInputElement} */ (document.getElementById('retainedCacheLimit')).dataset.loaded) {
      /** @type {HTMLInputElement} */ (document.getElementById('retainedCacheLimit')).value = String(data.policy.retained_cache_max_mb);
      /** @type {HTMLInputElement} */ (document.getElementById('retainedCacheAge')).value = String(data.policy.retained_cache_max_age_days);
      /** @type {HTMLInputElement} */ (document.getElementById('groupingHistoryLimit')).value = String(data.policy.grouping_history_max_mb);
      /** @type {HTMLInputElement} */ (document.getElementById('retainedCacheLimit')).dataset.loaded = '1';
    }
  } catch(e) {
    panel.textContent = 'Could not measure retained storage. Refresh to try again.';
  }
}

/** @param {string} type @param {string} name @param {HTMLButtonElement} button */
async function clearRetainedStorage(type, name, button) {
  var message = type === 'grouping-history'
    ? 'Remove grouping undo and redo snapshots from every workspace? Current grouping and ordinary photo edits remain. Removed grouping history cannot be restored.'
    : 'Clear ' + name.toLowerCase() + ' shared by every workspace? Cached renditions may be your only available offline copy. Confirm that sources or backups are available before clearing. Imported computation objects must be reimported or recomputed. Original photos, external editor files, and exports remain.';
  if (!await confirmRetainedStorage(message)) return;
  button.disabled = true;
  try {
    /** @type {{job_id:string}} */ var response = await safeFetch('/api/storage/retained/clear', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({type: type, confirm_history_loss: type === 'grouping-history'}),
    });
    watchRetainedStorageJob(response.job_id);
  } catch(e) {
    button.disabled = false;
    showToast('Could not start storage cleanup', 'error');
  }
}

/** @param {HTMLButtonElement} button */
async function saveRetainedStoragePolicy(button) {
  var fields = ['retainedCacheLimit', 'retainedCacheAge', 'groupingHistoryLimit'];
  if (!fields.every(function(id) { return /** @type {HTMLInputElement} */ (document.getElementById(id)).reportValidity(); })) return;
  var values = {
    retained_cache_max_mb: Number(/** @type {HTMLInputElement} */ (document.getElementById('retainedCacheLimit')).value),
    retained_cache_max_age_days: Number(/** @type {HTMLInputElement} */ (document.getElementById('retainedCacheAge')).value),
    grouping_history_max_mb: Number(/** @type {HTMLInputElement} */ (document.getElementById('groupingHistoryLimit')).value),
  };
  if (!await confirmRetainedStorage('Apply these limits to every workspace? Maintenance removes older generated files and completed grouping undo above their limits. Snapshots awaiting redo and ordinary photo edits are retained. Newly written files have a one-hour grace period.')) return;
  button.disabled = true;
  try {
    /** @type {{job_id:string}} */ var response = await safeFetch('/api/storage/retention', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(values),
    });
    watchRetainedStorageJob(response.job_id);
  } catch(e) {
    showToast('Could not update retention limits', 'error');
  } finally {
    button.disabled = false;
  }
}

/** @param {string} jobId */
function watchRetainedStorageJob(jobId) {
  var status = /** @type {HTMLElement} */ (document.getElementById('retainedStorageStatus'));
  status.textContent = 'Storage cleanup is running. Follow progress or cancel in Jobs.';
  async function poll() {
    try {
      /** @type {{status:string;_persisted?:boolean}} */ var job = await safeFetch('/api/jobs/' + encodeURIComponent(jobId), {}, {toast: false});
      if (['completed', 'failed', 'cancelled'].indexOf(job.status) < 0 || !job._persisted) {
        setTimeout(poll, 1000);
        return;
      }
      status.textContent = job.status === 'completed'
        ? 'Storage cleanup completed.' : 'Storage cleanup ' + job.status + '. See Jobs for details; files that could not be removed remain included in usage.';
      await refreshStoragePage();
    } catch(e) {
      status.textContent = 'Could not read cleanup progress. Open Jobs or refresh Storage.';
    }
  }
  poll();
}

/** @param {string} message @returns {Promise<boolean>} */
function confirmRetainedStorage(message) {
  var modal = /** @type {HTMLDialogElement} */ (document.getElementById('retainedActionModal'));
  if (modal.open) return Promise.resolve(false);
  /** @type {HTMLElement} */ (document.getElementById('retainedActionDescription')).textContent = message;
  var previousFocus = document.activeElement;
  return new Promise(function(resolve) {
    /** @param {boolean} accepted */ function finish(accepted) {
      modal.close();
      modal.classList.remove('open');
      modal.oncancel = null;
      /** @type {HTMLElement} */ (document.getElementById('retainedActionConfirm')).onclick = null;
      /** @type {HTMLElement} */ (document.getElementById('retainedActionCancel')).onclick = null;
      if (previousFocus instanceof HTMLElement) previousFocus.focus();
      resolve(accepted);
    }
    /** @type {HTMLElement} */ (document.getElementById('retainedActionConfirm')).onclick = function() { finish(true); };
    /** @type {HTMLElement} */ (document.getElementById('retainedActionCancel')).onclick = function() { finish(false); };
    modal.oncancel = function(event) { event.preventDefault(); finish(false); };
    modal.classList.add('open');
    modal.showModal();
    /** @type {HTMLElement} */ (document.getElementById('retainedActionCancel')).focus();
  });
}
