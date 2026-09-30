// Photo and group flags, including in-flight write coordination.
// Classic page script; shared globals are initialized before boot.js runs.

var pipelineReviewGroupFlagInFlight = {};
// Photo IDs that any in-flight bulk reject/clear is currently mutating. We
// check for overlap so an encounter action and one of its burst actions
// cannot make competing decisions from the same live-state read. The server
// records each completed write in history. Disjoint groups can still proceed
// in parallel.
var pipelineReviewGroupFlagInFlightPhotoIds = new Set();
// Direct per-photo flag writes can already be in flight when Group Review
// opens. Track them by photo so Apply can wait before persisting a newer
// staged decision for the same ID; otherwise network reordering could let the
// older request land last and overwrite the Group Review choice.
var pipelineReviewDirectFlagWritesByPhoto = {};

function trackPipelineReviewDirectFlagWrite(photoId, promise) {
  var key = String(photoId);
  if (!pipelineReviewDirectFlagWritesByPhoto[key]) {
    pipelineReviewDirectFlagWritesByPhoto[key] = new Set();
  }
  var writes = pipelineReviewDirectFlagWritesByPhoto[key];
  writes.add(promise);
  function release() {
    writes.delete(promise);
    if (!writes.size) delete pipelineReviewDirectFlagWritesByPhoto[key];
  }
  Promise.resolve(promise).then(release, release);
  return promise;
}

function waitForPipelineReviewDirectFlagWrites(photoIds) {
  var pending = [];
  (photoIds || []).forEach(function(photoId) {
    var writes = pipelineReviewDirectFlagWritesByPhoto[String(photoId)];
    if (writes) pending = pending.concat(Array.from(writes));
  });
  return Promise.all(pending.map(function(promise) {
    return Promise.resolve(promise).catch(function() { return false; });
  }));
}
// Photo IDs each rendered bulk-reject button actually represents, keyed by the
// same 'encounter:ei' / 'burst:ei:bi' strings that group flag targets use.
// renderResults() rebuilds this every render so hide-confirmed AND the label /
// species-conflict filter both narrow the target to what the user can see.
// groupFlagTarget() reads from here so the click applies to the visible set,
// not the raw underlying photo list.
var pipelineReviewVisibleTargets = {};

function normalizedPipelineReviewFlag(flag) {
  return flag === 'flagged' || flag === 'rejected' ? flag : 'none';
}

function updatePipelineReviewPhotoFlag(photo, flag) {
  if (!photo) return;
  photo.flag = flag;
  // Rejected photos lose representative eligibility server-side
  // (get_species_representatives(eligible_only=True) filters them out), so
  // mirror that in the review cache immediately.
  if (flag === 'rejected') {
    if (photo.is_species_representative) photo.is_species_representative = false;
    if (Array.isArray(photo.life_list)) {
      photo.life_list.forEach(function(entry) {
        if (!entry) return;
        if (entry.is_current_photo) entry.is_current_photo = false;
        if (entry.is_species_representative) entry.is_species_representative = false;
      });
    }
  }
}

function renderGroupRejectButton(kind, encIdx, burstIdx, photoIds, photoMap) {
  photoIds = Array.isArray(photoIds) ? photoIds : [];
  var allRejected = photoIds.length > 0 && photoIds.every(function(pid) {
    return photoMap[pid] && photoMap[pid].flag === 'rejected';
  });
  var noun = kind === 'burst' ? 'burst' : 'encounter';
  var label = allRejected ? 'Clear rejects' : 'Reject ' + noun;
  var title = allRejected
    ? 'Clear the rejected flag from every photo in this ' + noun
    : 'Mark every photo in this ' + noun + ' as rejected';
  var handler = kind === 'burst'
    ? 'toggleBurstRejected(event,' + encIdx + ',' + burstIdx + ')'
    : 'toggleEncounterRejected(event,' + encIdx + ')';
  var testId = kind === 'burst' ? 'reject-burst' : 'reject-encounter';
  return '<button type="button" class="group-reject-btn' + (allRejected ? ' all-rejected' : '') +
    '" data-testid="' + testId + '" onclick="' + handler + '" title="' + escapeAttr(title) +
    '" aria-label="' + escapeAttr(label) + '"><span aria-hidden="true">&times;</span>' +
    escapeHtml(label) + '</button>';
}

function groupFlagTarget(encIdx, burstIdx) {
  if (!pipelineResults || !pipelineResults.encounters) return null;
  var enc = pipelineResults.encounters[encIdx];
  if (!enc) return null;
  if (burstIdx == null) {
    // Read the exact set of photos the encounter button represents on
    // screen: renderResults() cached it under this key, and it already
    // excludes photos hidden by Hide confirmed or by the active label /
    // species-conflict filter so a click can't flip flags on photos the
    // user can't see. Fall back to the hide-confirmed-only set only when
    // the cache is missing (e.g. no render has happened yet).
    var encKey = 'encounter:' + encIdx;
    var encIds = pipelineReviewVisibleTargets[encKey];
    if (!encIds) encIds = visibleEncounterPhotoIds(enc);
    return {key: encKey, noun: 'encounter', photoIds: encIds};
  }
  var burst = enc.bursts && enc.bursts[burstIdx];
  if (!burst) return null;
  var burstKey = 'burst:' + encIdx + ':' + burstIdx;
  var burstIds = pipelineReviewVisibleTargets[burstKey];
  if (!burstIds) burstIds = burst.photo_ids || burst || [];
  return {
    key: burstKey,
    noun: 'burst',
    photoIds: burstIds,
  };
}

function setPipelineReviewGroupFlag(target, flag) {
  if (!target || pipelineReviewGroupFlagInFlight[target.key]) return Promise.resolve();
  if (isScopedReviewView()) {
    notifyReadOnlyScopedView();
    return Promise.resolve();
  }
  var ids = Array.from(new Set((target.photoIds || []).map(function(pid) {
    return parseInt(pid, 10);
  }).filter(function(pid) { return !!pid && !!findPhotoInResults(pid); })));
  if (!ids.length) return Promise.resolve();

  // Block bulk actions whose photos overlap an in-flight bulk action — e.g.,
  // clicking Reject burst while the parent encounter's Reject is still
  // running. Both requests would otherwise make their decisions from the
  // state before the first write completed.
  var overlaps = ids.some(function(pid) {
    return pipelineReviewGroupFlagInFlightPhotoIds.has(pid);
  });
  if (overlaps) {
    showToast('Another bulk reject is still finishing — try again in a moment', 'error');
    return Promise.resolve();
  }

  pipelineReviewGroupFlagInFlight[target.key] = true;
  ids.forEach(function(pid) { pipelineReviewGroupFlagInFlightPhotoIds.add(pid); });
  // pipelineResults.photos[].flag is a cache written when the pipeline ran;
  // GET /api/pipeline/results serves it as-is without refreshing from the DB
  // (unlike the GRM, which fetches /api/pipeline/group/state on open). If the
  // user picked a photo in Browse after the cache was written, the cache can
  // still read 'rejected' for it. Deriving changedIds and previousFlags from
  // the stale cache would post flag: 'none' for that live pick on "Clear
  // rejects", and Undo would restore the cached 'rejected' rather than the
  // pick. Read live DB flags first and drive the decision off those.
  return safeFetch('/api/pipeline/group/state', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({photo_ids: ids, species: ''}),
  }).then(function(data) {
    var live = (data && data.photos) || {};
    var liveFlagFor = function(pid) {
      return normalizedPipelineReviewFlag(live[pid] && live[pid].flag);
    };
    // Sync the review cache to the live snapshot so subsequent button
    // labels and card badges reflect truth, not the cache's opening value.
    ids.forEach(function(pid) {
      var liveFlag = liveFlagFor(pid);
      var photo = findPhotoInResults(pid);
      if (photo && normalizedPipelineReviewFlag(photo.flag) !== liveFlag) {
        updatePipelineReviewPhotoFlag(photo, liveFlag);
      }
    });
    // "Reject" applies the reject flag to anything not already rejected —
    // including live picks — because the user explicitly asked to reject the
    // group. "Clear rejects" only touches photos that are actually rejected;
    // it must never overwrite a live pick or an unflagged photo to 'none'.
    var changedIds = ids.filter(function(pid) {
      var liveFlag = liveFlagFor(pid);
      if (flag === 'none') return liveFlag === 'rejected';
      return liveFlag !== flag;
    });
    if (!changedIds.length) {
      renderResults();
      return;
    }
    return safeFetch('/api/batch/flag', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: changedIds, flag: flag}),
    }).then(function() {
      changedIds.forEach(function(pid) {
        updatePipelineReviewPhotoFlag(findPhotoInResults(pid), flag);
      });
      renderResults();
      refreshLatestScopeSnapshotIfCurrent();
      var count = changedIds.length;
      var message = flag === 'rejected'
        ? 'Rejected ' + count + ' photo' + (count === 1 ? '' : 's') + ' in ' + target.noun
        : 'Cleared rejects from ' + count + ' photo' + (count === 1 ? '' : 's') + ' in ' + target.noun;
      showToast(message, 'success');
    });
  }).finally(function() {
    delete pipelineReviewGroupFlagInFlight[target.key];
    ids.forEach(function(pid) { pipelineReviewGroupFlagInFlightPhotoIds.delete(pid); });
  });
}

function toggleEncounterRejected(event, encIdx) {
  if (event && event.stopPropagation) event.stopPropagation();
  var target = groupFlagTarget(encIdx, null);
  if (!target) return;
  var allRejected = target.photoIds.length > 0 && target.photoIds.every(function(pid) {
    var photo = findPhotoInResults(pid);
    return photo && photo.flag === 'rejected';
  });
  setPipelineReviewGroupFlag(target, allRejected ? 'none' : 'rejected');
}

function toggleBurstRejected(event, encIdx, burstIdx) {
  if (event && event.stopPropagation) event.stopPropagation();
  var target = groupFlagTarget(encIdx, burstIdx);
  if (!target) return;
  var allRejected = target.photoIds.length > 0 && target.photoIds.every(function(pid) {
    var photo = findPhotoInResults(pid);
    return photo && photo.flag === 'rejected';
  });
  setPipelineReviewGroupFlag(target, allRejected ? 'none' : 'rejected');
}

// Native Photo menu commands prefer a page-level batchSetFlag helper. In the
// burst modal, route those commands through the modal's pending zones (the
// same behavior as its P/X/Space shortcuts) rather than writing directly to
// /api/batch/flag behind the modal's Apply workflow.
function batchSetFlag(flag) {
  if (flag !== 'flagged' && flag !== 'rejected' && flag !== 'none') {
    return Promise.resolve(false);
  }
  var overlay = document.getElementById('grmOverlay');
  if (overlay && overlay.classList.contains('open') && grmState) {
    if (grmState.applying) {
      showToast('Group Review is applying changes — wait for it to finish', 'warning');
      return Promise.resolve(false);
    }
    var ids = _grmActionTargetIds();
    var overlapsBulkFlagWrite = ids.some(function(pid) {
      return pipelineReviewGroupFlagInFlightPhotoIds.has(pid);
    });
    if (overlapsBulkFlagWrite) {
      showToast('A bulk reject for the selected photos is still finishing — try again in a moment', 'error');
      return Promise.resolve(false);
    }
    if (flag === 'flagged') grmMovePick();
    else if (flag === 'rejected') grmMoveReject();
    else grmMoveCandidate();
    return Promise.resolve(true);
  }

  var activeIds = typeof nativeMenuActivePhotoIds === 'function'
    ? nativeMenuActivePhotoIds()
    : [];
  if (!activeIds.length) return Promise.resolve(false);
  return Promise.all(activeIds.map(function(pid) {
    return setPipelineReviewFlag(pid, flag);
  })).then(function(results) {
    return results.every(function(result) { return result !== false; });
  });
}

function setPipelineReviewFlag(photoId, flag) {
  if (window.vireoHistoryBusy && window.vireoHistoryBusy()) {
    showToast('Undo or redo is still finishing — try again in a moment', 'warning');
    return Promise.resolve(false);
  }
  if (flag !== 'flagged' && flag !== 'rejected' && flag !== 'none') return Promise.resolve();
  photoId = parseInt(photoId, 10);
  if (!photoId) return Promise.resolve();
  if (isScopedReviewView()) {
    notifyReadOnlyScopedView();
    // The shared lightbox treats undefined as a successful write. Return an
    // explicit failure so it restores the confirmed chip and does not mutate
    // the shared photo cache for this read-only view.
    return Promise.resolve(false);
  }
  // When the shared lightbox (or any other caller) writes a flag while Group
  // Review is open and this photo is a group member, route the change through
  // the burst's pending zones instead of writing the DB directly — the group
  // context menu already does this for its own flag chips. Otherwise the
  // burst's Apply reads a stale zone value and can clobber this newer flag,
  // and closing the lightbox shows the pre-lightbox pick/reject state.
  var grmOverlay = document.getElementById('grmOverlay');
  if (grmOverlay && grmOverlay.classList.contains('open') && grmState && grmState.items) {
    if (grmState.applying) {
      showToast('Group Review is applying changes — wait for it to finish', 'warning');
      return Promise.resolve(false);
    }
    var inGroup = grmState.items.some(function(p) { return p.id === photoId; });
    if (inGroup) {
      _grmMarkTouched(photoId);
      if (flag === 'flagged') {
        grmState.rejects.delete(photoId);
        grmState.picks.add(photoId);
      } else if (flag === 'rejected') {
        grmState.picks.delete(photoId);
        grmState.rejects.add(photoId);
      } else {
        grmState.picks.delete(photoId);
        grmState.rejects.delete(photoId);
      }
      grmSyncZoneCards();
      // The shared lightbox needs to display this pending choice without
      // caching it as a confirmed database write. A structured result keeps
      // that distinction explicit while other callers retain the historical
      // true/false write contract.
      return Promise.resolve({provisional: true});
    }
  }
  // A group action snapshots live flags before writing its batch. Allowing a
  // per-photo pick/reject for one of those same IDs before the batch finishes
  // would let the batch overwrite the newer choice, while its Undo would
  // restore the older snapshot. Honor the group lock here as well as in the
  // bulk path so every overlapping flag write is serialized.
  if (pipelineReviewGroupFlagInFlightPhotoIds.has(photoId)) {
    showToast('A bulk reject for this photo is still finishing — try again in a moment', 'error');
    // The shared lightbox treats every resolved value except false as a
    // successful write and updates its own cache/event listeners. Return an
    // explicit failure so its UI stays on the last confirmed database flag.
    return Promise.resolve(false);
  }

  var directWrite = safeFetch('/api/photos/' + photoId + '/flag', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ flag: flag }),
  }).then(function() {
    var photo = findPhotoInResults(photoId);
    if (photo) {
      updatePipelineReviewPhotoFlag(photo, flag);
    }
    renderResults();
    if (inspectPhotoId === photoId && document.getElementById('inspectOverlay').classList.contains('open')) {
      openInspect(photoId);
    }
    var msg = flag === 'flagged' ? 'Marked as pick' : (flag === 'rejected' ? 'Marked as reject' : 'Flag cleared');
    showToast(msg, 'success');
    refreshLatestScopeSnapshotIfCurrent();
  });
  return trackPipelineReviewDirectFlagWrite(photoId, directWrite);
}
