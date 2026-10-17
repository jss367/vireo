// Duplicates page: resolved-copy batch cleanup. The progress container lives
// outside the group list so category/filter renders cannot detach it.

var _duplicateTrashActive = false;
/** @type {Map<Element,{snapshot:DuplicateScan|null;index:number}>} */ var _duplicateBucketOperations = new Map();
/** @type {Map<number,DuplicateTrashState>} */ var _duplicateFileTrashState = new Map();

/** @param {number} photoId */
function duplicateFileTrashIsPending(photoId) {
  var state = _duplicateFileTrashState.get(photoId);
  return !!(state && state.pending);
}

/** @param {number} photoId @param {Element|null} [currentCard] */
function updateDuplicateFileTrashCard(photoId, currentCard) {
  var state = _duplicateFileTrashState.get(photoId);
  var card = currentCard || document.querySelector('.dup-card[data-photo-id="' + photoId + '"]');
  if (!state || !card) return;
  var keep = card.querySelector(/** @type {'button'} */ ('.keep-btn'));
  if (keep) keep.disabled = state.pending || state.terminal;
  var button = card.querySelector(/** @type {'button'} */ ('.trash-btn'));
  if (!button) return;
  button.disabled = state.pending || state.terminal;
  var status = card.querySelector(/** @type {'div'} */ ('[data-trash-status]'));
  if (status) status.textContent = state.message;
}

/** @param {number[]} photoIds @param {object} owner */
function prepareDuplicateTrashBatch(photoIds, owner) {
  if (photoIds.some(duplicateFileTrashIsPending)) {
    throw new Error('Some files are already being cleaned up. Wait for that cleanup to finish.');
  }
  photoIds.forEach(function(id) {
    _duplicateFileTrashState.set(id, {owner: owner, pending: true, terminal: false, message: 'Waiting to move to Trash...'});
  });
  document.querySelectorAll(/** @type {'div'} */ ('.dup-card[data-photo-id]')).forEach(function(card) {
    updateDuplicateFileTrashCard(Number(card.getAttribute('data-photo-id')), card);
  });
  refreshTrashAllButton(/** @type {HTMLButtonElement} */ (document.getElementById('trashAllBtn')));
}

/** @param {DuplicateTrashResponse} data @param {number[]} photoIds @param {object} owner */
function recordDuplicateTrashBatch(data, photoIds, owner) {
  var failed = new Map((data.failed || []).map(function(item) { return [item.id, item]; }));
  var skipped = new Map((data.skipped || []).map(function(item) { return [item.id, item]; }));
  photoIds.forEach(function(id) {
    var failure = failed.get(id);
    var skip = skipped.get(id);
    var terminal = !failure && (!skip || skip.reason === 'file already missing');
    var message = failure ? 'Failed: ' + (failure.error || 'failed')
      : skip ? (terminal ? 'Cleaned up (file was already gone)' : 'Skipped: ' + (skip.reason || 'skipped'))
      : 'Moved to Trash';
    _duplicateFileTrashState.set(id, {owner: owner, pending: false, terminal: terminal, message: message});
    updateDuplicateFileTrashCard(id);
    if (terminal) _duplicateFileTrashState.delete(id);
  });
}

/** @param {number[]} photoIds @param {object} owner */
function releasePausedDuplicateTrashBatch(photoIds, owner) {
  photoIds.forEach(function(id) {
    var state = _duplicateFileTrashState.get(id);
    if (!state || state.owner !== owner || !state.pending) return;
    state.pending = false;
    state.message = 'Cleanup paused. Retry this file.';
    updateDuplicateFileTrashCard(id);
  });
  refreshTrashAllButton(/** @type {HTMLButtonElement} */ (document.getElementById('trashAllBtn')));
}

/** @param {number} photoId */
async function trashOneLoserFile(photoId) {
  if (duplicateFileTrashIsPending(photoId)) return;
  var card = document.querySelector('.dup-card[data-photo-id="' + photoId + '"]');
  if (!card || !card.querySelector(/** @type {'button'} */ ('.trash-btn'))) return;
  var state = {pending: true, terminal: false, message: 'Moving to Trash...'};
  _duplicateFileTrashState.set(photoId, state);
  updateDuplicateFileTrashCard(photoId);
  refreshTrashAllButton(/** @type {HTMLButtonElement} */ (document.getElementById('trashAllBtn')));
  try {
    /** @type {DuplicateTrashResponse} */ var data = await safeFetch('/api/duplicates/delete-loser-files', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: [photoId]}),
    });
    if (data.trashed === 1) {
      state.terminal = true;
      state.message = 'Moved to Trash';
      forgetCleanedDuplicateCopies(new Set([photoId]));
      showToast('Moved 1 file to Trash', 'success');
    } else if ((data.skipped || []).length) {
      var reason = data.skipped?.[0]?.reason || 'skipped';
      if (reason === 'file already missing') {
        state.terminal = true;
        state.message = 'Cleaned up (file was already gone)';
        forgetCleanedDuplicateCopies(new Set([photoId]));
      } else {
        state.message = 'Skipped: ' + reason;
      }
    } else if ((data.failed || []).length) {
      var failureText = data.failed?.[0]?.error || 'failed';
      state.message = 'Failed: ' + failureText;
      showToast('Trash failed: ' + failureText, 'error');
    } else {
      state.message = 'No file was moved. Try again.';
    }
  } catch (caught) { const error = duplicateRequestError(caught);
    state.message = 'Failed: ' + (error.message || 'request failed');
  } finally {
    state.pending = false;
    // Rebuild retry controls after non-terminal outcomes. Each card reads its
    // state by photo ID rather than retaining a node detached by a filter.
    if (!state.terminal && !_scanInProgress && _lastScanResult) renderResults(_lastScanResult);
    updateDuplicateFileTrashCard(photoId);
    refreshTrashAllButton(/** @type {HTMLButtonElement} */ (document.getElementById('trashAllBtn')));
    if (state.terminal) _duplicateFileTrashState.delete(photoId);
  }
}

// Same words as ``KEPT_ANOTHER_COPY_REASON`` in duplicate_scan.py.
var KEPT_ANOTHER_COPY_REASON = 'kept another copy instead';
/** @type {Set<number>} */ var _duplicateKeepPending = new Set();

// Re-point a resolved group at the copy the server now keeps: every other
// copy shown is rejected, the ones it just rejected saying why.
/** @param {DuplicateProposal} proposal @param {DuplicateKeepResponse} data */
function applyKeptDuplicateCopy(proposal, data) {
  var members = [proposal.winner].concat(proposal.other_kept || [], proposal.losers || []);
  var chosen = members.find(function(m) { return m.id === data.winner_id; });
  if (!chosen) return false;
  var replaced = new Set(data.loser_ids);
  /** @type {DuplicatePhoto} */ var winner = Object.assign({}, chosen, {
    rating: data.rating, exists: true, volume_offline: false,
  });
  delete winner.rejected;
  delete winner.reason;
  proposal.winner = winner;
  proposal.other_kept = [];
  proposal.losers = members.filter(function(m) { return m !== chosen; }).map(function(m) {
    return replaced.has(m.id) ? Object.assign({}, m, {rejected: true, reason: KEPT_ANOTHER_COPY_REASON}) : m;
  });
  return true;
}

/** @param {number} photoId */
async function keepDuplicateCopy(photoId) {
  if (_duplicateKeepPending.has(photoId) || duplicateFileTrashIsPending(photoId)) return;
  if (_duplicateTrashActive) {
    showToast('Wait for the cleanup to finish before choosing another copy to keep.', 'info');
    return;
  }
  var proposal = (_proposals || []).find(function(p) {
    return p.status === 'resolved' && (p.losers || []).some(function(l) { return l.id === photoId; });
  });
  if (!proposal) return;
  _duplicateKeepPending.add(photoId);
  var card = document.querySelector('.dup-card[data-photo-id="' + photoId + '"]');
  var button = card && card.querySelector(/** @type {'button'} */ ('.keep-btn'));
  if (button) { button.disabled = true; button.textContent = 'Switching...'; }
  try {
    /** @type {DuplicateKeepResponse} */ var data = await safeFetch('/api/duplicates/keep', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_id: photoId}),
    });
    if (!applyKeptDuplicateCopy(proposal, data)) return;
    var kept = proposal.winner.filename || 'this copy';
    var rejected = data.loser_ids.length;
    showToast('Now keeping ' + kept + (rejected
      ? '; rejected ' + rejected + ' other ' + (rejected === 1 ? 'copy' : 'copies') : '') + '.', 'success');
  } catch (_caught) {
    // safeFetch already showed the server's reason.
  } finally {
    _duplicateKeepPending.delete(photoId);
    // A rescan or restore that replaced the results meanwhile owns the page.
    if (_lastScanResult && !_scanInProgress && _proposals.indexOf(proposal) !== -1) {
      var group = document.querySelector('.dup-group[data-hash="' + proposal.file_hash + '"]');
      if (group && groupMatchesFilter(proposal)) {
        group.outerHTML = renderResolvedGroup(proposal);
        refreshDuplicateSummaries();
      } else {
        renderResults(_lastScanResult);
      }
    }
  }
}

/** @param {Element} card @param {number} bi */
function pinDuplicateBucketOperation(card, bi) {
  var bucket = (_lastScanResult && _lastScanResult.buckets || [])[bi];
  if (!card.hasAttribute('data-reveal-paths')) {
    var visible = visibleBucket(bucket);
    card.setAttribute('data-reveal-paths', JSON.stringify((visible && visible.folders || []).slice()));
  }
  _duplicateBucketOperations.set(card, {snapshot: _lastScanResult, index: bi});
  /** @type {HTMLElement} */ (document.getElementById('duplicateBucketOperationList')).appendChild(card);
  /** @type {HTMLElement} */ (document.getElementById('duplicateBucketOperations')).hidden = false;
}

/** @param {Element} card */
function unpinDuplicateBucketOperation(card) {
  _duplicateBucketOperations.delete(card);
  card.remove();
  /** @type {HTMLElement} */ (document.getElementById('duplicateBucketOperations')).hidden = !_duplicateBucketOperations.size;
}

/** @param {DuplicateScan|null} snapshot @param {number} bi */
function duplicateBucketIsPinned(snapshot, bi) {
  return Array.from(_duplicateBucketOperations.values()).some(function(operation) {
    return operation.snapshot === snapshot && operation.index === bi;
  });
}

function clearDuplicateTrashProgress() {
  var progress = /** @type {HTMLElement} */ (document.getElementById('trashAllProgress'));
  if (progress && !_duplicateTrashActive) progress.hidden = true;
}

function refreshDuplicateTrashControls() {
  var button = /** @type {HTMLButtonElement} */ (document.getElementById('trashAllBtn'));
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
  /** @type {number[]} */ var ids = [];
  resolved.forEach(function(p) {
    if (isBulkTrashProtected(p)) return;
    (p.losers || []).forEach(function(l) {
      // Skip ones already trashed in this session.
      var card = document.querySelector('.dup-card[data-photo-id="' + l.id + '"]');
      if (!card || card.classList.contains('trashed') || duplicateFileTrashIsPending(l.id)) return;
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

  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('trashAllBtn'));
  var progressEl = /** @type {HTMLElement} */ (document.getElementById('trashAllProgress'));
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

  } catch (caught) { const e = duplicateRequestError(caught);
    var completed = e.trashSummary ? e.trashSummary.completed : 0;
    showToast('Trash cleanup paused after ' + completed + ' of ' + ids.length +
              ' files: ' + (e.message || 'request failed') +
              '. Use the button to retry the remaining files.', 'error');
  } finally {
    _duplicateTrashActive = false;
    refreshTrashAllButton(/** @type {HTMLButtonElement} */ (document.getElementById('trashAllBtn')));
  }
}

/** @param {Element|null} card @param {number[]} remainingIds @param {string[]|Set<string>|null} [resolvedHashes] */
function showBucketTrashRetry(card, remainingIds, resolvedHashes) {
  if (!card) return;
  card.setAttribute('data-pending-trash', JSON.stringify(remainingIds));
  if (resolvedHashes) {
    var hashList = Array.isArray(resolvedHashes)
      ? resolvedHashes : Array.from(resolvedHashes);
    card.setAttribute('data-pending-resolved-hashes', JSON.stringify(hashList));
  }
  var bi = card.getAttribute('data-bi');
  var actions = card.querySelector(/** @type {'div'} */ ('.bucket-actions'));
  if (!actions) return;
  var n = remainingIds.length;
  actions.innerHTML =
    '<button class="keep-btn" data-retry-trash onclick="retryBucketTrash(this)">' +
      'Retry ' + n + ' file' + (n === 1 ? '' : 's') + ' to Trash' +
    '</button>' +
    bucketRevealButton(Number(bi), card);
}

/** @param {HTMLButtonElement} btn */
async function retryBucketTrash(btn) {
  var card = btn.closest('.bucket-card');
  if (!card) return;
  var raw = card.getAttribute('data-pending-trash');
  if (!raw) return;
  /** @type {unknown} */ var ids;
  try { ids = JSON.parse(raw); } catch (e) { return; }
  if (!duplicateNumberList(ids) || ids.length === 0) return;

  // Recover the resolved-hash subset stashed by the initial
  // bulkResolveByFolder failure path. Falling back to
  // ``bucket.file_hashes`` would over-prune whenever bulk-resolve
  // returned any hashes in ``skipped`` — those groups were never
  // resolved and must stay visible on the page.
  /** @type {string[]|null} */ var resolvedHashes = null;
  var rawHashes = card.getAttribute('data-pending-resolved-hashes');
  if (rawHashes) {
    try {
      /** @type {unknown} */ var parsed = JSON.parse(rawHashes);
      if (duplicateStringList(parsed)) resolvedHashes = parsed;
    } catch (e) { /* fall through */ }
  }

  var status = card.querySelector(/** @type {'div'} */ ('[data-bucket-status]'));
  var actionBtns = card.querySelectorAll(/** @type {'button'} */ ('.bucket-actions button'));
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
  } catch (caught) { const e = duplicateRequestError(caught);
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
    var bi = parseInt(card.getAttribute('data-bi') || '', 10);
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

/** @param {number} bi @param {string} keepFolder */
async function bulkResolveByFolder(bi, keepFolder) {
  var card = document.querySelector('#results .bucket-card[data-bi="' + bi + '"]');
  if (!card) return;
  // Same filter-restricted view used at render + click time so the
  // confirmation count and the POSTed file_hashes describe only the
  // visible groups. Skipping this step would let a filter-driven "N
  // shown" bucket resolve every group behind the filter as well.
  var bucket = visibleBucket((_lastScanResult && _lastScanResult.buckets || [])[bi]);
  if (!bucket) return;

  var deleteToggle = card.querySelector(/** @type {'input'} */ ('input[data-delete-toggle]'));
  var deleteFiles = !!(deleteToggle && deleteToggle.checked);
  var status = card.querySelector(/** @type {'div'} */ ('[data-bucket-status]'));
  var btns = card.querySelectorAll(/** @type {'button'} */ ('.keep-btn'));

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
    /** @type {{resolved?:Array<{file_hash:string;loser_ids?:number[]}>;skipped?:unknown[]}} */ var data = await safeFetch('/api/duplicates/bulk-resolve', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        file_hashes: bucket.file_hashes,
        keep_folder: keepFolder,
      }),
    });
    var resolved = data.resolved || [];
    var skippedCount = (data.skipped || []).length;
    /** @type {number[]} */ var loserIds = [];
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
      } catch (caught) { const e = duplicateRequestError(caught);
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
  } catch (caught) { const e = duplicateRequestError(caught);
    card.classList.remove('applying');
    btns.forEach(function(b) { b.disabled = false; });
    if (status) status.textContent = '';
    showToast('Bulk resolve failed: ' + (e.message || 'error'), 'error');
    unpinDuplicateBucketOperation(card);
    if (!_scanInProgress && _lastScanResult) renderResults(_lastScanResult);
  }
}

/** @param {number[]} photoIds @param {(progress:DuplicateTrashProgress)=>void} [onProgress] @param {(data:DuplicateTrashResponse,ids:number[])=>void} [onBatch] */
async function trashLoserFilesInBatches(photoIds, onProgress, onBatch) {
  /** @type {DuplicateTrashSummary} */ var summary = {trashed: 0, skipped: [], failed: [], completed: 0,
                 total: photoIds.length};
  var owner = {};
  prepareDuplicateTrashBatch(photoIds, owner);
  try {
    for (var offset = 0; offset < photoIds.length; offset += TRASH_BATCH_SIZE) {
      var batch = photoIds.slice(offset, offset + TRASH_BATCH_SIZE);
      batch.forEach(function(id) {
        var state = _duplicateFileTrashState.get(id); if (state) state.message = 'Moving to Trash...';
        updateDuplicateFileTrashCard(id);
      });
      var actionableSkipSoFar = summary.skipped.filter(isActionableSkip).length;
      /** @type {DuplicateTrashProgress} */ var progress = {
        phase: 'moving', total: photoIds.length, completed: offset,
        batchStart: offset + 1, batchEnd: offset + batch.length,
        trashed: summary.trashed, skipped: actionableSkipSoFar,
        failed: summary.failed.length,
      };
      if (onProgress) onProgress(progress);

      /** @type {DuplicateTrashResponse} */ var data;
      try {
        data = await safeFetch('/api/duplicates/delete-loser-files', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ photo_ids: batch }),
        }, {toast: false});
      } catch (caught) { const e = duplicateRequestError(caught);
        recordDuplicateTrashBatch({failed: batch.map(function(id) {
          return {id: id, error: e.message || 'request failed'};
        })}, batch, owner);
        progress.phase = 'error';
        progress.completed = summary.completed;
        if (onProgress) onProgress(progress);
        e.trashSummary = summary;
        throw e;
      }

      recordDuplicateTrashBatch(data, batch, owner);
      summary.trashed += data.trashed || 0;
      summary.skipped = summary.skipped.concat(data.skipped || []);
      summary.failed = summary.failed.concat(data.failed || []);
      summary.completed += batch.length;
      if (onBatch) onBatch(data, batch);

      var actionableSkipCount = summary.skipped.filter(isActionableSkip).length;
      if (onProgress) onProgress({
        phase: summary.completed === photoIds.length
          ? (summary.failed.length || actionableSkipCount
              ? 'complete_with_errors' : 'complete')
          : 'moving',
        total: photoIds.length, completed: summary.completed,
        batchStart: summary.completed === photoIds.length ? summary.completed : summary.completed + 1,
        batchEnd: Math.min(summary.completed + TRASH_BATCH_SIZE, photoIds.length),
        trashed: summary.trashed, skipped: actionableSkipCount,
        failed: summary.failed.length,
      });
    }
    return summary;
  } finally {
    releasePausedDuplicateTrashBatch(photoIds, owner);
  }
}

/** @param {DuplicateTrashResponse} data @param {number[]} requestedIds */
function applyTrashBatchToCards(data, requestedIds) {
  // "file already missing" still deletes the orphan DB row, so it is a
  // terminal completion even though no file needed to be moved.
  var terminalSkips = new Set(['file already missing']);
  var completedSet = new Set(requestedIds);
  (data.skipped || []).forEach(function(s) {
    if (!terminalSkips.has(s.reason)) completedSet.delete(s.id);
  });
  (data.failed || []).forEach(function(f) { completedSet.delete(f.id); });
  completedSet.forEach(function(pid) {
    var card = document.querySelector('.dup-card[data-photo-id="' + pid + '"]');
    if (card) {
      card.classList.add('trashed');
      var status = card.querySelector(/** @type {'div'} */ ('[data-trash-status]'));
      if (status) status.textContent = 'Cleaned up';
    }
  });
  forgetCleanedDuplicateCopies(completedSet);
  (data.skipped || []).forEach(function(s) {
    if (terminalSkips.has(s.reason)) return;
    var card = document.querySelector('.dup-card[data-photo-id="' + s.id + '"]');
    if (card) {
      var status = card.querySelector(/** @type {'div'} */ ('[data-trash-status]'));
      if (status) status.textContent = 'Skipped: ' + (s.reason || '');
    }
  });
  (data.failed || []).forEach(function(f) {
    var card = document.querySelector('.dup-card[data-photo-id="' + f.id + '"]');
    if (card) {
      var status = card.querySelector(/** @type {'div'} */ ('[data-trash-status]'));
      if (status) status.textContent = 'Failed: ' + (f.error || '');
    }
  });
}

/** @param {HTMLButtonElement|null} btn */
function refreshTrashAllButton(btn) {
  var remaining = 0;
  _proposals.forEach(function(p) {
    if (p.status !== 'resolved' || isBulkTrashProtected(p)) return;
    // Only count losers from groups that survived the active filter.
    // The bulk button label mirrors the render loop's filtered count
    // (see ``bulkTrashableLoserCount``); walking the unfiltered list
    // here would let the label drift to reflect hidden groups and
    // trick the user into thinking the action still has work to do.
    if (!groupMatchesFilter(p)) return;
    (p.losers || []).forEach(function(l) {
      var card = document.querySelector('.dup-card[data-photo-id="' + l.id + '"]');
      if (card && !card.classList.contains('trashed') && !duplicateFileTrashIsPending(l.id)) remaining++;
    });
  });
  if (remaining > 0) {
    if (!btn) {
      var toggle = /** @type {HTMLElement} */ (document.getElementById('resolvedToggle'));
      var actions = toggle && toggle.closest('.section-header')?.querySelector(/** @type {'div'} */ ('.section-actions'));
      if (!actions) return;
      btn = document.createElement('button');
      btn.id = 'trashAllBtn';
      btn.className = 'btn-secondary';
      btn.onclick = trashAllLoserFiles;
      actions.appendChild(btn);
    }
    btn.disabled = _duplicateTrashActive;
    btn.textContent = 'Move ' + remaining + ' extra ' +
                      (remaining === 1 ? 'copy' : 'copies') + ' to Trash';
  } else if (btn) {
    btn.remove();
  }
}

/** @param {unknown} value @returns {value is number[]} */
function duplicateNumberList(value) {
  return Array.isArray(value) && value.every(function(id) { return typeof id === 'number' && Number.isInteger(id) && id > 0; });
}

/** @param {unknown} value @returns {value is string[]} */
function duplicateStringList(value) {
  return Array.isArray(value) && value.every(function(item) { return typeof item === 'string'; });
}

/** @param {unknown} value @returns {value is DuplicateTrashSummary} */
function duplicateTrashSummary(value) {
  if (!value || typeof value !== 'object') return false;
  return 'trashed' in value && typeof value.trashed === 'number' &&
    'completed' in value && typeof value.completed === 'number' &&
    'total' in value && typeof value.total === 'number' &&
    'failed' in value && Array.isArray(value.failed) && value.failed.every(function(item) {
      return item && typeof item === 'object' && typeof item.id === 'number' &&
        (item.error === undefined || typeof item.error === 'string');
    }) && 'skipped' in value && Array.isArray(value.skipped) && value.skipped.every(function(item) {
      return item && typeof item === 'object' && typeof item.id === 'number' && typeof item.reason === 'string';
    });
}

/** @param {unknown} value @returns {DuplicateTrashError} */
function duplicateRequestError(value) {
  /** @type {DuplicateTrashError} */ var error = value instanceof Error ? value : new Error(
    value && typeof value === 'object' && 'message' in value && typeof value.message === 'string'
      ? value.message : '');
  if (value && typeof value === 'object' && 'trashSummary' in value && duplicateTrashSummary(value.trashSummary)) {
    error.trashSummary = value.trashSummary;
  }
  return error;
}
