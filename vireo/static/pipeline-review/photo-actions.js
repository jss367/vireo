// Selection, ratings, reveal/copy, wildlife exclusion, and photo editing.
// Classic page script; shared globals are initialized before boot.js runs.

// Expose the last explicit Process Review selection to the shared native
// Photo menu. The burst modal owns a real multi-selection; the main results
// grid is single-select and remembers the card most recently right-clicked.
function getActiveSelection() {
  if (window._vireoNativeMenuPhotoIdsOverride && window._vireoNativeMenuPhotoIdsOverride.length) {
    return pipelineReviewUniquePhotoIds(window._vireoNativeMenuPhotoIdsOverride);
  }
  var overlay = document.getElementById('grmOverlay');
  if (overlay && overlay.classList.contains('open') && grmState) {
    return pipelineReviewUniquePhotoIds(_grmActionTargetIds());
  }
  if (inspectPhotoId) return [inspectPhotoId];
  return pipelineReviewContextPhotoIds.slice();
}

async function setPipelineReviewRating(photoIds, rating) {
  if (isScopedReviewView()) {
    notifyReadOnlyScopedView();
    return false;
  }
  if (grmState && grmState.applying) {
    showToast('Group Review is applying changes — wait for it to finish', 'warning');
    return false;
  }
  var ids = pipelineReviewUniquePhotoIds(photoIds);
  if (!ids.length) return false;
  try {
    await safeFetch('/api/batch/rating', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids, rating: rating}),
    });
  } catch(e) { return false; }
  ids.forEach(function(id) {
    var photo = findPhotoInResults(id);
    if (photo) photo.rating = rating;
  });
  if (document.getElementById('grmOverlay').classList.contains('open')) {
    renderGroupModal();
  }
  showToast('Rated ' + ids.length + ' photo' + (ids.length === 1 ? '' : 's') + ' ' + rating, 'success');
  refreshLatestScopeSnapshotIfCurrent();
  return true;
}

function setRatingFor(photoId, rating) {
  return setPipelineReviewRating([photoId], rating);
}

function batchSetRating(rating) {
  return setPipelineReviewRating(getActiveSelection(), rating);
}

function revealPhoto(photoId) {
  return safeFetch('/api/files/reveal', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({photo_id: photoId}),
  }, {toast: false}).then(function(data) {
    if (typeof showRevealFeedback === 'function') showRevealFeedback(data);
  }).catch(function(err) {
    showToast('Reveal failed: ' + (err.message || 'request failed'), 'error');
  });
}

async function copyPhotoPaths(photoIds) {
  var ids = pipelineReviewUniquePhotoIds(photoIds);
  var settled = await Promise.allSettled(ids.map(function(id) {
    return safeFetch('/api/photos/' + id, {}, {toast: false});
  }));
  var paths = settled.filter(function(result) {
    return result.status === 'fulfilled' && result.value && result.value.path;
  }).map(function(result) { return result.value.path; });
  if (!paths.length) {
    showToast('No paths could be copied', 'error');
    return false;
  }
  try {
    await navigator.clipboard.writeText(paths.join('\n'));
    showToast(paths.length === 1 ? 'Path copied' : paths.length + ' paths copied', 'success');
    return true;
  } catch(e) {
    showToast('Could not copy paths', 'error');
    return false;
  }
}

async function setWildlifeExcludedFor(photoId, excluded) {
  if (isScopedReviewView()) {
    notifyReadOnlyScopedView();
    return false;
  }
  if (grmState && grmState.applying) {
    showToast('Group Review is applying changes — wait for it to finish', 'warning');
    return false;
  }
  try {
    await safeFetch('/api/photos/' + photoId + '/wildlife_excluded', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({excluded: !!excluded}),
    }, {toast: false});
  } catch (e) { return false; }
  var photo = findPhotoInResults(photoId);
  if (photo) photo.wildlife_excluded = excluded ? 1 : 0;
  return true;
}

function openPipelinePhotoEditor(photoId) {
  if (isScopedReviewView()) {
    notifyReadOnlyScopedView();
    return false;
  }
  if (window.vireoEditNav) {
    var list = pipelineReviewVisiblePhotoList(photoId);
    window.vireoEditNav.setList(list, photoId);
    window.vireoEditNav.setLastPhoto(photoId);
  }
  var url = '/edit/' + photoId;
  var groupOpen = document.getElementById('grmOverlay').classList.contains('open');
  var tauri = typeof isTauri === 'function' && isTauri();
  if (groupOpen && tauri && grmHasPendingUserEdits()) {
    showToast('Apply or close Group Review before leaving this page', 'warning');
    return false;
  }
  if (groupOpen && !tauri) {
    window.open(url, '_blank', 'noopener');
  } else {
    window.location.href = url;
  }
  return true;
}
