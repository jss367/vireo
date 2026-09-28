
/* ---------- Delete Confirmation ---------- */
var _deletePhotoIds = [];
var _deleteCallback = null;
var _deleteJobSource = null;
var _deleteProgressStageOrder = [];

function showDeleteDialog(photoIds, companionCount, callback) {
  _deletePhotoIds = photoIds;
  _deleteCallback = callback;
  var count = photoIds.length;
  document.getElementById('deleteModalTitle').textContent =
    count === 1 ? 'Delete photo?' : 'Delete ' + count + ' photos?';
  var compRow = document.getElementById('deleteCompanionRow');
  if (companionCount > 0) {
    compRow.style.display = '';
    document.getElementById('deleteCompanionLabel').textContent =
      'Also delete ' + companionCount + ' companion file' + (companionCount === 1 ? '' : 's');
    document.getElementById('deleteCompanionCheck').checked = true;
  } else {
    compRow.style.display = 'none';
  }
  document.querySelector('input[name="deleteMode"][value="vireo"]').checked = true;
  resetDeleteProgress();
  document.getElementById('deleteModal').classList.add('open');
}

function hideDeleteModal() {
  if (_deleteJobSource) {
    _deleteJobSource.close();
    _deleteJobSource = null;
  }
  setDeleteModalBusy(false);
  resetDeleteProgress();
  document.getElementById('deleteModal').classList.remove('open');
  _deletePhotoIds = [];
  _deleteCallback = null;
}

function resetDeleteProgress() {
  var box = document.getElementById('deleteProgress');
  var detail = document.getElementById('deleteProgressDetail');
  if (box) box.style.display = 'none';
  if (detail) detail.textContent = '';
  _deleteProgressStageOrder = [];
  ['Files', 'Catalog', 'Cache'].forEach(function(name) {
    var row = document.getElementById('deleteProgress' + name + 'Stage');
    var status = document.getElementById('deleteProgress' + name + 'Status');
    if (!row) return;
    row.hidden = false;
    row.classList.add('waiting');
    row.classList.remove('active', 'complete', 'partial');
    delete row.dataset.failed;
    if (status) status.textContent = 'Waiting';
    var bar = row.querySelector('[role="progressbar"]');
    var fill = row.querySelector('.inat-progress-fill');
    if (bar) {
      bar.setAttribute('aria-valuemax', '1');
      bar.setAttribute('aria-valuenow', '0');
      bar.setAttribute('aria-valuetext', 'Waiting');
    }
    if (fill) fill.style.width = '0%';
  });
}

function setDeleteProgressStage(stage, state, current, total, statusText) {
  var name = stage.charAt(0).toUpperCase() + stage.slice(1);
  var row = document.getElementById('deleteProgress' + name + 'Stage');
  if (!row) return;
  // A stage that has already recorded failures must not be silently
  // "completed" by a later phase transition \u2014 that would let the UI report
  // e.g. "Move files to Trash \u2713 Complete" while files were actually
  // retained. Keep the failure state visible until the modal is reset.
  if (state === 'complete' && row.dataset.failed) {
    return;
  }
  var status = document.getElementById('deleteProgress' + name + 'Status');
  var bar = row.querySelector('[role="progressbar"]');
  var fill = row.querySelector('.inat-progress-fill');
  current = Math.max(0, Number(current || 0));
  total = Math.max(0, Number(total || 0));
  var pct = (state === 'complete' || state === 'partial')
    ? 100
    : (total > 0 ? Math.max(0, Math.min(100, Math.round(100 * current / total))) : 0);

  row.classList.toggle('waiting', state === 'waiting');
  row.classList.toggle('active', state === 'active');
  row.classList.toggle('complete', state === 'complete');
  row.classList.toggle('partial', state === 'partial');
  if (fill) fill.style.width = pct + '%';
  if (bar) {
    bar.setAttribute('aria-valuemax', String(total || 1));
    bar.setAttribute(
      'aria-valuenow',
      String((state === 'complete' || state === 'partial') ? (total || 1) : current),
    );
  }

  var displayStatus = statusText;
  if (!displayStatus) {
    if (state === 'waiting') displayStatus = 'Waiting';
    else if (state === 'complete') displayStatus = '\u2713 Complete';
    else if (state === 'partial') displayStatus = 'Processed with errors';
    else if (total > 0) displayStatus = current + '/' + total;
    else displayStatus = 'Working\u2026';
  }
  if (status) status.textContent = displayStatus;
  if (bar) bar.setAttribute('aria-valuetext', displayStatus);
}

function startDeleteProgress(mode, count) {
  var filesRow = document.getElementById('deleteProgressFilesStage');
  var filesLabel = document.getElementById('deleteProgressFilesLabel');
  var diskMode = mode !== 'vireo';
  _deleteProgressStageOrder = diskMode
    ? ['files', 'catalog', 'cache']
    : ['catalog', 'cache'];
  if (filesRow) filesRow.hidden = !diskMode;
  if (filesLabel) {
    filesLabel.textContent = mode === 'disk_permanent'
      ? 'Delete files permanently'
      : 'Move files to Trash';
  }
  _deleteProgressStageOrder.forEach(function(stage) {
    setDeleteProgressStage(stage, 'waiting', 0, count);
  });
  var box = document.getElementById('deleteProgress');
  if (box) box.style.display = '';
}

function deleteProgressStageForPhase(phase) {
  if (phase === 'Moving files to Trash' || phase === 'Deleting files permanently') {
    return 'files';
  }
  if (phase === 'Removing from Vireo' || phase === 'Removed from Vireo') {
    return 'catalog';
  }
  if (phase === 'Pruning pipeline cache' || phase === 'Cleaning cached files') {
    return 'cache';
  }
  if (phase === 'Starting delete') return _deleteProgressStageOrder[0];
  return null;
}

function markDeleteStageFailed(stage, failed, current, total) {
  var name = stage.charAt(0).toUpperCase() + stage.slice(1);
  var row = document.getElementById('deleteProgress' + name + 'Stage');
  if (!row) return;
  row.dataset.failed = String(failed);
  // ``failed`` is a per-photo count (len(failed_ids)) rather than a
  // per-path count. A single retained photo may leave both a companion and
  // its unattempted primary on disk, so labelling the number as "files"
  // would understate what remains -- label as photos to match the count's
  // real semantics.
  var statusText = failed === 1
    ? '1 photo retained'
    : failed + ' photos retained';
  setDeleteProgressStage(stage, 'partial', current, total, statusText);
}

function updateDeleteProgress(data) {
  data = data || {};
  var box = document.getElementById('deleteProgress');
  var detail = document.getElementById('deleteProgressDetail');
  if (box) box.style.display = '';
  var phase = data.phase || 'Working';
  var total = Number(data.total || 0);
  var current = Number(data.current || 0);
  var failed = Number(data.failed || 0);
  var stageFailures = data.stage_failures || {};

  if (phase === 'Finishing') {
    _deleteProgressStageOrder.forEach(function(stage) {
      // Prefer the per-stage failure map so a single Finishing event is
      // enough to render the correct state even if an intermediate
      // per-stage emit never arrived. ``setDeleteProgressStage`` still
      // refuses to overwrite a stage flagged failed, so a stage already
      // marked partial by an earlier emit stays partial.
      var stageFailed = Number(stageFailures[stage] || 0);
      if (stageFailed > 0) {
        markDeleteStageFailed(stage, stageFailed, 0, 0);
      } else {
        setDeleteProgressStage(stage, 'complete');
      }
    });
    if (detail) detail.textContent = '';
    return;
  }

  var stage = deleteProgressStageForPhase(phase);
  var stageIndex = _deleteProgressStageOrder.indexOf(stage);
  if (stageIndex >= 0) {
    _deleteProgressStageOrder.slice(0, stageIndex).forEach(function(previous) {
      setDeleteProgressStage(previous, 'complete');
    });
    var isDiskPhase = (
      phase === 'Moving files to Trash' ||
      phase === 'Deleting files permanently'
    );
    if (isDiskPhase && failed > 0 && total > 0 && current >= total) {
      // Filesystem step finished with retained files -- surface the failure
      // now rather than letting a later phase mark this stage green.
      markDeleteStageFailed(stage, failed, current, total);
    } else if (phase === 'Pruning pipeline cache') {
      // This usually completes too quickly to deserve its own bar. Keep the
      // cleanup bar monotonic at zero until per-photo cache cleanup begins.
      setDeleteProgressStage(stage, 'active', 0, 0, 'Updating review cache\u2026');
    } else if (phase === 'Removed from Vireo') {
      if (failed > 0) {
        // Catalog revalidation preserved rows whose identity changed mid-
        // delete -- keep the stage from turning green and let the user see
        // that the catalog work was only partially applied.
        markDeleteStageFailed(stage, failed, current, total);
      } else {
        setDeleteProgressStage(stage, 'complete', current, total);
      }
    } else {
      setDeleteProgressStage(
        stage, 'active', current, total,
        phase === 'Starting delete' ? 'Starting\u2026' : ''
      );
    }
  }

  var parts = [];
  if (data.current_file) parts.push(data.current_file);
  if (data.detail) parts.push(data.detail);
  if (detail) detail.textContent = parts.join(' · ');
}

async function handleDeleteJobComplete(evt, savedCallback, mode, includeCompanions) {
  var data = evt && evt.result;
  // A delete that retained some photos after file errors ends "failed" but
  // still carries its result: handle it like a completed one so the
  // retained photos stay visible and the permanent-delete fallback is offered.
  var partial = !!(evt && evt.status === 'failed' && data &&
    data.failed_photo_ids && data.failed_photo_ids.length);
  if (!evt || !data || (evt.status !== 'completed' && !partial)) {
    hideDeleteModal();
    var errors = (evt && evt.errors) || [];
    showToast('Delete failed' + (errors.length ? ': ' + errors[0] : ''), 'error');
    return;
  }

  hideDeleteModal();

  var failedIds = (data.failed_photo_ids || []).filter(function(id, idx, all) {
    return all.indexOf(id) === idx;
  });
  if (data.trash_failed && data.trash_failed.length > 0 && failedIds.length) {
    var paths = data.trash_failed.map(function(f) { return f.path; });
    if (confirm('Trash not available for ' + paths.length + ' file(s):\n' +
        paths.slice(0, 5).join('\n') +
        (paths.length > 5 ? '\n... and ' + (paths.length - 5) + ' more' : '') +
        '\n\nPermanently delete instead?')) {
      try {
        // The server retains catalog rows whose Trash operation failed, so
        // retry by photo id and let it remove each row only after its
        // permanent file deletion succeeds. There is no retry by raw path:
        // the catalog row is what vouches for a file.
        var retry = await safeFetch('/api/batch/delete', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            photo_ids: failedIds,
            mode: 'disk_permanent',
            include_companions: !!includeCompanions,
          }),
        });
        data.deleted += Number(retry.deleted || 0);
        data.trashed += Number(retry.trashed || 0);
        data.trash_failed = retry.trash_failed || [];
        data.failed_photo_ids = retry.failed_photo_ids || [];
      } catch (e) {
        // safeFetch already surfaced the actionable error.
      }
    }
  }

  var msg = mode === 'vireo'
    ? data.deleted + ' photo' + (data.deleted === 1 ? '' : 's') + ' removed'
    : data.deleted + ' photo' + (data.deleted === 1 ? '' : 's') + ' moved to Trash';
  if (data.failed_photo_ids && data.failed_photo_ids.length) {
    msg += '; ' + data.failed_photo_ids.length + ' retained after file errors';
  }
  showToast(msg, data.failed_photo_ids && data.failed_photo_ids.length ? 'error' : 'success');

  if (savedCallback) savedCallback(data);
}

// Toggle the delete modal's in-flight state. While busy we keep the modal
// open (rather than closing it instantly) so the user sees that their click
// registered and work is underway — deleting a large batch can take many
// seconds of synchronous backend work with no other feedback.
var _deleteBusy = false;
function setDeleteModalBusy(busy, count, mode) {
  _deleteBusy = busy;
  var confirmBtn = document.getElementById('deleteConfirmBtn');
  var cancelBtn = document.querySelector('#deleteModal .modal-btn-cancel');
  // The mode radios and companion checkbox were already read into the request
  // when the delete started, so changing them mid-flight does nothing — lock
  // them too so the UI stays honest about what's in progress.
  var inputs = document.querySelectorAll(
    '#deleteModal input[name="deleteMode"], #deleteCompanionCheck');
  if (busy) {
    var verb = mode === 'vireo' ? 'Removing' : 'Deleting';
    var noun = count === 1 ? 'photo' : count + ' photos';
    if (confirmBtn) {
      confirmBtn.disabled = true;
      confirmBtn.style.opacity = '0.85';
      confirmBtn.style.cursor = 'default';
      confirmBtn.innerHTML = '<span class="btn-spinner"></span>' + verb + ' ' + noun + '…';
    }
    if (cancelBtn) cancelBtn.disabled = true;
    inputs.forEach(function(el) { el.disabled = true; });
  } else {
    if (confirmBtn) {
      confirmBtn.disabled = false;
      confirmBtn.style.opacity = '';
      confirmBtn.style.cursor = 'pointer';
      confirmBtn.textContent = 'Delete';
    }
    if (cancelBtn) cancelBtn.disabled = false;
    inputs.forEach(function(el) { el.disabled = false; });
  }
}

async function confirmDelete() {
  if (_deleteBusy) return;  // guard against double-submit
  var mode = document.querySelector('input[name="deleteMode"]:checked').value;
  var includeCompanions = document.getElementById('deleteCompanionCheck').checked &&
    document.getElementById('deleteCompanionRow').style.display !== 'none';
  var savedIds = _deletePhotoIds.slice();
  var savedCallback = _deleteCallback;
  setDeleteModalBusy(true, savedIds.length, mode);
  startDeleteProgress(mode, savedIds.length);
  updateDeleteProgress({
    phase: 'Starting delete',
    current: 0,
    total: savedIds.length,
  });
  try {
    var start = await safeFetch('/api/jobs/batch-delete', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        photo_ids: savedIds,
        mode: mode,
        include_companions: includeCompanions,
      }),
    });
  } catch(e) { hideDeleteModal(); return; }
  _deleteJobSource = safeEventSource('/api/jobs/' + start.job_id + '/stream', {
    onProgress: updateDeleteProgress,
    onComplete: function(evt) {
      _deleteJobSource = null;
      handleDeleteJobComplete(evt, savedCallback, mode, includeCompanions);
    },
    onError: function() {
      _deleteJobSource = null;
      hideDeleteModal();
    },
  });
}

async function lightboxToggleWildlifeExcluded() {
  if (!_lightboxCurrentId) return;
  if (_lbGuardReadOnly()) return false;
  var currentId = _lightboxCurrentId;
  var excluded = !_lbCurrentWildlifeExcluded;
  try {
    if (typeof window.setWildlifeExcludedFor === 'function') {
      var updated = await window.setWildlifeExcludedFor(currentId, excluded);
      if (updated === false) return false;
    } else {
      await safeFetch('/api/photos/' + currentId + '/wildlife_excluded', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ excluded: excluded }),
      }, { toast: false });
    }
    _lbCurrentWildlifeExcluded = excluded;
    if (window.toast) {
      window.toast(
        excluded ? 'Excluded from wildlife classification' : 'Included in wildlife classification',
        'success'
      );
    }
  } catch (e) {
    if (window.toast) window.toast('Update failed: ' + (e && e.message ? e.message : 'unknown'), 'error');
  }
}

function lightboxDelete() {
  if (_lbGuardReadOnly()) return false;
  var overlay = document.getElementById('lightboxOverlay');
  if (
    _lbVisualTransitionPending ||
    !_lightboxCurrentId ||
    !overlay ||
    !overlay.classList.contains('active')
  ) return;
  var currentId = _lightboxCurrentId;
  var p = _lightboxPhotoList.find(function(x) { return x.id === currentId; });
  var companionCount = (p && p.companion_path) ? 1 : 0;

  showDeleteDialog([currentId], companionCount, function(data) {
    // If the server retained this photo because its Trash step failed (and
    // the user declined the permanent-delete fallback), leave it in the
    // lightbox and grid. Dropping it here would hide a file still on disk
    // until the next reload — the toast already told the user it was kept.
    var retained = (data && data.failed_photo_ids) || [];
    if (retained.indexOf(currentId) !== -1) {
      return;
    }

    // Capture the lightbox position before Browse updates its grid. Browse's
    // lazy-loaded lightbox deliberately shares the same array object, so a
    // grid removal may also remove the lightbox entry in one splice.
    var idx = _lightboxPhotoList.findIndex(function(x) { return x.id === currentId; });
    var removedFromSharedLightboxList = false;

    // Update the browse grid before touching lightbox state. Otherwise
    // Browse's `lightbox:closed` handler still finds the deleted row in
    // `photos` and re-selects it (loading stale detail state that this
    // block would then null out — leaving the sidebar blank).
    if (typeof photos !== 'undefined' && typeof renderGrid === 'function') {
      var browseIdx = photos.findIndex(function(x) { return x.id === currentId; });
      if (browseIdx >= 0) {
        removedFromSharedLightboxList = photos === _lightboxPhotoList;
        photos.splice(browseIdx, 1);
      }
      selectedPhotos.delete(currentId);
      if (selectedPhotoId === currentId) selectedPhotoId = null;
      renderGrid();
      if (typeof refreshBrowseSidebarCounts === 'function') {
        refreshBrowseSidebarCounts();
      }
    }

    // Remove from a page-specific lightbox list. When Browse shared its live
    // array, the grid splice above already performed this removal.
    if (!removedFromSharedLightboxList && idx >= 0) {
      _lightboxPhotoList.splice(idx, 1);
    }

    if (_lightboxPhotoList.length === 0) {
      closeLightbox(null);
    } else {
      var nextIdx = Math.min(idx, _lightboxPhotoList.length - 1);
      var next = _lightboxPhotoList[nextIdx];
      openLightbox(next.id, next.filename, _lightboxPhotoList);
    }

    try {
      document.dispatchEvent(new CustomEvent('lightbox:photodeleted', {
        detail: { photoId: currentId, result: data || null }
      }));
    } catch (_) {}
  });
}
