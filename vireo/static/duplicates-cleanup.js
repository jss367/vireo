// Duplicates page: resolved-copy batch cleanup. The progress container lives
// outside the group list so category/filter renders cannot detach it.

var _duplicateTrashActive = false;
var _duplicateBucketOperations = new Map();

function pinDuplicateBucketOperation(card, bi) {
  _duplicateBucketOperations.set(card, {snapshot: _lastScanResult, index: bi});
  document.getElementById('duplicateBucketOperationList').appendChild(card);
  document.getElementById('duplicateBucketOperations').hidden = false;
}

function unpinDuplicateBucketOperation(card) {
  _duplicateBucketOperations.delete(card);
  card.remove();
  document.getElementById('duplicateBucketOperations').hidden = !_duplicateBucketOperations.size;
}

function duplicateBucketIsPinned(snapshot, bi) {
  return Array.from(_duplicateBucketOperations.values()).some(function(operation) {
    return operation.snapshot === snapshot && operation.index === bi;
  });
}

function clearDuplicateTrashProgress() {
  var progress = document.getElementById('trashAllProgress');
  if (progress && !_duplicateTrashActive) progress.hidden = true;
}

function refreshDuplicateTrashControls() {
  var button = document.getElementById('trashAllBtn');
  if (button && _duplicateTrashActive) button.disabled = true;
}

async function trashAllLoserFiles() {
  if (_duplicateTrashActive) return;
  // ``groupMatchesFilter`` restricts to groups the filter actually
  // surfaced. Without it, an active filter shrinks the rendered
  // resolved section and the button label (both driven by the
  // filtered ``resolved`` list in ``renderResults``) while this
  // action still walked every resolved proposal — a click on "Move
  // N extra copies to Trash" would then trash records for groups the
  // filter had hidden from view.
  var resolved = _proposals.filter(function(p) {
    return p.status === 'resolved' && groupMatchesFilter(p);
  });
  var ids = [];
  resolved.forEach(function(p) {
    if (isBulkTrashProtected(p)) return;
    (p.losers || []).forEach(function(l) {
      // Skip ones already trashed in this session.
      var card = document.querySelector('.dup-card[data-photo-id="' + l.id + '"]');
      if (!card || card.classList.contains('trashed')) return;
      if (l.id != null) ids.push(l.id);
    });
  });
  if (ids.length === 0) {
    showToast('Nothing to trash', 'success');
    return;
  }
  if (!confirm('Move ' + ids.length + ' file' + (ids.length === 1 ? '' : 's') +
               ' to Trash and remove their entries from Vireo?\n\n' +
               'Files go to your OS Trash (recoverable from there). The ' +
               'corresponding extra-copy records (and their thumbnails/previews) ' +
               'are deleted from Vireo. Kept copies retain all merged keywords ' +
               'and ratings.')) {
    return;
  }

  var btn = document.getElementById('trashAllBtn');
  var progressEl = document.getElementById('trashAllProgress');
  _duplicateTrashActive = true;
  if (btn) { btn.disabled = true; btn.textContent = 'Moving to Trash...'; }

  try {
    var data = await trashLoserFilesInBatches(
      ids,
      function(progress) { updateTrashProgress(progressEl, progress); },
      applyTrashBatchToCards
    );
    var trashed = data.trashed || 0;
    var failed = (data.failed || []).length;

    var actionableSkips = (data.skipped || []).filter(isActionableSkip).length;
    var msg = 'Moved ' + trashed + ' file' + (trashed === 1 ? '' : 's') + ' to Trash';
    if (actionableSkips) msg += ', ' + actionableSkips + ' skipped';
    if (failed) msg += ', ' + failed + ' failed';
    showToast(msg, failed ? 'error' : 'success');

  } catch (e) {
    var completed = e.trashSummary ? e.trashSummary.completed : 0;
    showToast('Trash cleanup paused after ' + completed + ' of ' + ids.length +
              ' files: ' + (e.message || 'request failed') +
              '. Use the button to retry the remaining files.', 'error');
  } finally {
    _duplicateTrashActive = false;
    refreshTrashAllButton(document.getElementById('trashAllBtn'));
  }
}

function showBucketTrashRetry(card, remainingIds, resolvedHashes) {
  if (!card) return;
  card.setAttribute('data-pending-trash', JSON.stringify(remainingIds));
  if (resolvedHashes) {
    var hashList = Array.isArray(resolvedHashes)
      ? resolvedHashes : Array.from(resolvedHashes);
    card.setAttribute('data-pending-resolved-hashes', JSON.stringify(hashList));
  }
  var bi = card.getAttribute('data-bi');
  var actions = card.querySelector('.bucket-actions');
  if (!actions) return;
  var n = remainingIds.length;
  actions.innerHTML =
    '<button class="keep-btn" data-retry-trash onclick="retryBucketTrash(this)">' +
      'Retry ' + n + ' file' + (n === 1 ? '' : 's') + ' to Trash' +
    '</button>' +
    bucketRevealButton(Number(bi));
}

async function retryBucketTrash(btn) {
  var card = btn.closest('.bucket-card');
  if (!card) return;
  var raw = card.getAttribute('data-pending-trash');
  if (!raw) return;
  var ids;
  try { ids = JSON.parse(raw); } catch (e) { return; }
  if (!Array.isArray(ids) || ids.length === 0) return;

  // Recover the resolved-hash subset stashed by the initial
  // bulkResolveByFolder failure path. Falling back to
  // ``bucket.file_hashes`` would over-prune whenever bulk-resolve
  // returned any hashes in ``skipped`` — those groups were never
  // resolved and must stay visible on the page.
  var resolvedHashes = null;
  var rawHashes = card.getAttribute('data-pending-resolved-hashes');
  if (rawHashes) {
    try {
      var parsed = JSON.parse(rawHashes);
      if (Array.isArray(parsed)) resolvedHashes = parsed;
    } catch (e) { /* fall through */ }
  }

  var status = card.querySelector('[data-bucket-status]');
  var actionBtns = card.querySelectorAll('.bucket-actions button');
  Array.prototype.forEach.call(actionBtns, function(b) { b.disabled = true; });
  card.classList.add('applying');
  if (status) {
    status.classList.add('trash-progress', 'compact');
    status.hidden = false;
  }

  var pending = null;
  var terminalError = null;
  try {
    var trashData = await trashLoserFilesInBatches(
      ids,
      function(progress) { updateTrashProgress(status, progress); }
    );
    var trashed = trashData.trashed || 0;
    var failedNow = (trashData.failed || [])
      .map(function(f) { return f.id; })
      .filter(function(id) { return id != null; });
    if (failedNow.length > 0) {
      pending = failedNow;
      showToast('Moved ' + trashed + ', ' + failedNow.length +
                ' still failed to Trash — retry when ready', 'error');
    } else {
      showToast('Moved ' + trashed + ' file' + (trashed === 1 ? '' : 's') +
                ' to Trash', 'success');
    }
  } catch (e) {
    terminalError = e;
    var s = e.trashSummary || {trashed: 0, failed: [], completed: 0};
    var completed = s.completed || 0;
    var unattempted = ids.slice(completed);
    var failedInAttempted = (s.failed || [])
      .map(function(f) { return f.id; })
      .filter(function(id) { return id != null; });
    pending = failedInAttempted.concat(unattempted);
    showToast('Retry paused after ' + (s.trashed || 0) + ' of ' + ids.length +
              ' more to Trash: ' + (e.message || 'error'), 'error');
  }

  card.classList.remove('applying');
  if (pending && pending.length > 0) {
    // Re-arm with the same resolved-hash subset so a later successful
    // retry still knows which hashes to prune.
    showBucketTrashRetry(card, pending, resolvedHashes);
    return;
  }
  if (terminalError) return;
  // Everything remaining trashed successfully — the whole bucket is
  // now resolved on-disk. Prune it and re-render so the card goes away.
  card.removeAttribute('data-pending-trash');
  card.removeAttribute('data-pending-resolved-hashes');
  var hashesToPrune = resolvedHashes;
  if (!hashesToPrune) {
    // Legacy fallback for cards that were retry-armed before this data
    // attribute existed. In practice these only appear when bulk-resolve
    // resolved every hash in the bucket, so bucket.file_hashes matches.
    var bi = parseInt(card.getAttribute('data-bi'), 10);
    var bucket = (_lastScanResult && _lastScanResult.buckets || [])[bi];
    hashesToPrune = (bucket && bucket.file_hashes) || [];
  }
  if (hashesToPrune.length > 0) {
    pruneResolvedHashesFromScan(new Set(hashesToPrune));
  }
  unpinDuplicateBucketOperation(card);
  if (!_scanInProgress && _lastScanResult) {
    renderResults(_lastScanResult);
  }
}

async function bulkResolveByFolder(bi, keepFolder) {
  var card = document.querySelector('#results .bucket-card[data-bi="' + bi + '"]');
  if (!card) return;
  // Same filter-restricted view used at render + click time so the
  // confirmation count and the POSTed file_hashes describe only the
  // visible groups. Skipping this step would let a filter-driven "N
  // shown" bucket resolve every group behind the filter as well.
  var bucket = visibleBucket((_lastScanResult && _lastScanResult.buckets || [])[bi]);
  if (!bucket) return;

  var deleteToggle = card.querySelector('input[data-delete-toggle]');
  var deleteFiles = !!(deleteToggle && deleteToggle.checked);
  var status = card.querySelector('[data-bucket-status]');
  var btns = card.querySelectorAll('.keep-btn');

  var n = bucket.group_count;
  var label = keepFolder.split('/').filter(Boolean).pop() || keepFolder;
  var confirmMsg = 'Keep all ' + n + ' photo' + (n === 1 ? '' : 's') +
    ' in ' + label + ' and reject the duplicates' +
    (deleteFiles ? ', then move the duplicate files to Trash' : '') + '?';
  if (!confirm(confirmMsg)) return;

  pinDuplicateBucketOperation(card, bi);
  card.classList.add('applying');
  btns.forEach(function(b) { b.disabled = true; });
  if (status) status.textContent = 'Resolving ' + n + ' group' + (n === 1 ? '' : 's') + '...';

  try {
    var data = await safeFetch('/api/duplicates/bulk-resolve', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        file_hashes: bucket.file_hashes,
        keep_folder: keepFolder,
      }),
    });
    var resolved = data.resolved || [];
    var skippedCount = (data.skipped || []).length;
    var loserIds = [];
    resolved.forEach(function(r) {
      (r.loser_ids || []).forEach(function(id) { loserIds.push(id); });
    });
    // These groups are already rejected on the server. Their pinned operation
    // owns pending file cleanup; keep them out of the unresolved review cache.
    var resolvedHashList = resolved.map(function(r) { return r.file_hash; });
    pruneResolvedHashesFromScan(new Set(resolvedHashList));
    if (!_scanInProgress && _lastScanResult) renderResults(_lastScanResult);

    var pendingTrashIds = null;
    if (deleteFiles && loserIds.length > 0) {
      if (status) {
        status.classList.add('trash-progress', 'compact');
        status.hidden = false;
      }
      try {
        var trashData = await trashLoserFilesInBatches(
          loserIds,
          function(progress) { updateTrashProgress(status, progress); }
        );
        var trashed = trashData.trashed || 0;
        var trashFailed = (trashData.failed || []).length;
        var msg = 'Resolved ' + resolved.length + ', moved ' + trashed + ' to Trash';
        if (trashFailed) msg += ', ' + trashFailed + ' trash failed — retry when ready';
        if (skippedCount) msg += ', ' + skippedCount + ' skipped';
        showToast(msg, trashFailed ? 'error' : 'success');
        if (trashFailed) {
          // Partial trash failure inside completed batches. The DB rows
          // are already rejected (bulk-resolve ran first), so a retry
          // must skip bulk-resolve and go straight to trash on the
          // failed IDs.
          pendingTrashIds = (trashData.failed || [])
            .map(function(f) { return f.id; })
            .filter(function(id) { return id != null; });
        }
      } catch (e) {
        // Whole-batch failure mid-loop. Completed batches are already
        // trashed + DB-deleted; remaining batches never fired. Any
        // per-file failures inside completed batches also need retry.
        // ``bulk-resolve`` already rejected every loser in the bucket,
        // so re-clicking the Keep-folder button would find "fewer than 2
        // non-rejected candidates" and skip — the retry has to trash
        // the remaining loser IDs directly.
        var s = e.trashSummary || {trashed: 0, failed: [], completed: 0};
        var completed = s.completed || 0;
        var trashedSoFar = s.trashed || 0;
        var unattempted = loserIds.slice(completed);
        var failedInAttempted = (s.failed || [])
          .map(function(f) { return f.id; })
          .filter(function(id) { return id != null; });
        pendingTrashIds = failedInAttempted.concat(unattempted);
        showToast('Resolved ' + resolved.length + ', moved ' + trashedSoFar +
                  ' to Trash before pausing: ' + (e.message || 'error') +
                  ' — retry when ready', 'error');
      }
  } else {
    var resolveMsg = 'Resolved ' + resolved.length + ' group' +
      (resolved.length === 1 ? '' : 's');
    if (skippedCount) resolveMsg += ', ' + skippedCount + ' skipped';
    showToast(resolveMsg, 'success');
    }

    if (pendingTrashIds && pendingTrashIds.length > 0) {
      // The pinned card retains the confirmed selection and retry IDs across
      // category/filter renders. Skipped groups remain in the review cache.
      card.classList.remove('applying');
      showBucketTrashRetry(card, pendingTrashIds, resolvedHashList);
      return;
    }

    unpinDuplicateBucketOperation(card);
    // Don't clobber an in-flight scan's UI: if the user clicked Scan
    // while bulk-resolve was awaiting, ``renderResults`` would hide the
    // progress spinner, re-enable the scan button, and paint stale
    // pre-scan results — risking overlapping scan jobs. Same guard
    // tryRestoreLastScan uses.
    if (!_scanInProgress && _lastScanResult) {
      renderResults(_lastScanResult);
    }
  } catch (e) {
    card.classList.remove('applying');
    btns.forEach(function(b) { b.disabled = false; });
    if (status) status.textContent = '';
    showToast('Bulk resolve failed: ' + (e.message || 'error'), 'error');
    unpinDuplicateBucketOperation(card);
    if (!_scanInProgress && _lastScanResult) renderResults(_lastScanResult);
  }
}
