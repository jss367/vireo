// Sync to XMP: starting the sync job, following its stream, and reporting
// what actually reached the sidecars.
// Classic script.

/**
 * Report a finished sync in the banner: what reached the sidecars, not merely
 * that the job ran. A NAS that rejects every write finishes the job normally;
 * saying "Synced!" over it is why a stuck banner looks like a UI bug.
 *
 * @param {HTMLElement} status @param {SyncJobEvent} event
 */
function _syncShowSyncResult(status, event) {
  var result = (event && event.result) || null;
  // A sync that raised before returning — a failing get_pending_changes
  // or clear_pending — completes with a null result. Defaulting the
  // counters to zero there would claim "failed on 0" over a fatal
  // error. Gate on the counters themselves rather than on the status,
  // so a resultless completed or cancelled event cannot slip through
  // either, and so a result object without them never reads as zero.
  var counted = !!result && typeof result.synced === 'number'
    && typeof result.failed === 'number';
  var synced = counted && result ? /** @type {number} */ (result.synced) : 0;
  var failed = counted && result ? /** @type {number} */ (result.failed) : 0;
  var reasons = (event && event.errors && event.errors.length)
    ? event.errors
    : ((result && result.errors) || []);
  // Assign unconditionally: a stale tooltip from an earlier partial
  // failure must not survive a later clean run.
  status.title = reasons.join('\n');
  if (!counted) {
    // The run never established any counts. Name the fatal cause and
    // say only what is actually known: when clear_pending is what
    // failed, sidecars really were written, so "Wrote 0" would be a
    // lie and "Changes stay pending" alone would hide the writes.
    var why = (event && event.failure && event.failure.message)
      || reasons[0] || '';
    if (event.status === 'cancelled') {
      status.textContent = 'Sync cancelled before it reported results.';
      status.style.color = 'var(--text-dim)';
      status.dataset.syncState = 'cancelled';
    } else {
      status.textContent = 'Sync failed'
        + (why ? ': ' + why : ' (no detail reported)')
        + '. Sidecars already written stay written; everything else '
        + 'stays pending.';
      status.style.color = 'var(--danger)';
      status.dataset.syncState = 'failure';
    }
  } else if (event.status === 'cancelled') {
    // sync_to_xmp() does not poll cancellation inside its per-photo
    // loop, so a cancel requested mid-run still returns a structured
    // result and any writes that failed before the cancel took hold
    // must be surfaced. Reporting only ``N written`` would present a
    // NAS-wide permission failure as an ordinary cancellation.
    var cancelledMsg = 'Sync cancelled — ' + synced.toLocaleString() + ' written';
    if (failed > 0) {
      cancelledMsg += ', failed on ' + failed.toLocaleString();
      if (reasons.length) cancelledMsg += ' — ' + reasons[0];
    }
    status.textContent = cancelledMsg + '.';
    status.style.color = failed > 0 ? 'var(--danger)' : 'var(--text-dim)';
    status.dataset.syncState = failed > 0 ? 'failure' : 'cancelled';
  } else if (failed === 0 && event.status === 'completed') {
    status.textContent = synced > 0
      ? 'Synced ' + synced.toLocaleString() + ' photo' + (synced === 1 ? '' : 's') + '!'
      : 'Nothing to sync.';
    status.style.color = 'var(--accent)';
    status.dataset.syncState = 'success';
  } else {
    status.textContent = 'Wrote ' + synced.toLocaleString() + ', failed on '
      + failed.toLocaleString() + (reasons.length ? ' — ' + reasons[0] : '')
      + '. Changes stay pending.';
    status.style.color = 'var(--danger)';
    status.dataset.syncState = 'failure';
  }
}

/** @param {number[]=} changeIds Sync only these changes (the dialog's checked types); omit for all. */
async function runSync(changeIds) {
  var status = /** @type {HTMLElement} */ (document.getElementById('syncStatus'));
  status.textContent = 'Syncing...';
  status.style.color = 'var(--text-dim)';
  // Any state from the previous run is now stale.
  status.dataset.syncState = '';
  try {
    /** @type {RequestInit} */
    var requestOptions = {method: 'POST'};
    if (Array.isArray(changeIds)) {
      if (changeIds.length === 0) {
        status.textContent = 'No checked changes to sync.';
        status.style.color = 'var(--text-dim)';
        return;
      }
      requestOptions.headers = {'Content-Type': 'application/json'};
      requestOptions.body = JSON.stringify({change_ids: changeIds});
    }
    var data = /** @type {{job_id: string}} */ (await safeFetch('/api/jobs/sync', requestOptions, { toast: false }));
    void checkPendingSync();
    var lastCheckpoint = 0;

    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(p) {
        status.textContent = _syncProgressText(p);
        status.dataset.syncState = 'running';
        if (_syncActiveJob && _syncActiveJob.id === data.job_id) {
          _syncActiveJob.progress = p;
          _syncShowActiveJob(_syncActiveJob);
        }
        if ((p.checkpoint || 0) > lastCheckpoint) {
          lastCheckpoint = p.checkpoint || 0;
          void checkPendingSync();
        }
      },
      onComplete: function(event) {
        _syncShowSyncResult(status, event);
        setTimeout(checkPendingSync, 1000);
      },
      onError: function() {
        status.textContent = 'Connection lost';
        status.style.color = 'var(--danger)';
      }
    });
  } catch(e) {
    status.textContent = 'Error: ' + (e instanceof Error ? e.message : String(e));
  }
}
