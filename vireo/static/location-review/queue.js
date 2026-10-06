// The group queue: the current group, its facts, progress, and moving between groups.
// Classic page script; load boot.js after all definitions.
'use strict';

function currentGroup() { return state.groups[state.currentIndex] || null; }

function showEmpty(title, message, error) {
  document.getElementById('locationReviewMain').classList.add('location-review-hidden');
  var empty = document.getElementById('locationReviewEmpty');
  empty.classList.remove('location-review-hidden');
  document.getElementById('locationReviewEmptyTitle').textContent = title;
  var messageEl = document.getElementById('locationReviewEmptyMessage');
  messageEl.textContent = message;
  messageEl.classList.toggle('location-review-error', !!error);
}

function updateProgress() {
  var completed = state.assignedGroups;
  var total = state.initialGroupCount;
  var pct = total ? Math.round((completed / total) * 100) : 100;
  document.getElementById('locationReviewProgressText').textContent = completed + ' of ' + total + (state.mode === 'discrepancies' ? ' reviewed' : ' assigned');
  document.getElementById('locationReviewRemaining').textContent = state.groups.length ? state.groups.length + ' in queue' : '';
  document.getElementById('locationReviewProgressFill').style.width = pct + '%';
}

function refreshGroupMetadata(group) {
  if (!group || !group.photos.length) return;
  if (state.mode === 'time' || state.mode === 'discrepancies') {
    group.captured_from = group.photos[0].timestamp;
    group.captured_to = group.photos[group.photos.length - 1].timestamp;
    return;
  }
  var referenceLng = Number(group.photos[0].longitude);
  var centerLat = group.photos.reduce(function(total, photo) {
    return total + Number(photo.latitude);
  }, 0) / group.photos.length;
  var lngDeltas = group.photos.map(function(photo) {
    return ((Number(photo.longitude) - referenceLng + 180) % 360 + 360) % 360 - 180;
  });
  var centerLng = ((referenceLng + lngDeltas.reduce(function(total, value) {
    return total + value;
  }, 0) / group.photos.length + 180) % 360 + 360) % 360 - 180;
  var timestamps = group.photos.map(function(photo) { return photo.timestamp; }).filter(Boolean).sort();
  group.center = {lat: centerLat, lng: centerLng};
  group.bounds = {
    south: Math.min.apply(null, group.photos.map(function(photo) { return Number(photo.latitude); })),
    west: Math.min.apply(null, group.photos.map(function(photo) { return Number(photo.longitude); })),
    north: Math.max.apply(null, group.photos.map(function(photo) { return Number(photo.latitude); })),
    east: Math.max.apply(null, group.photos.map(function(photo) { return Number(photo.longitude); })),
  };
  group.spread_m = Math.round(Math.max.apply(null, group.photos.map(function(photo) {
    return distanceBetween(centerLat, centerLng, Number(photo.latitude), Number(photo.longitude));
  })) * 10) / 10;
  group.captured_from = timestamps.length ? timestamps[0] : null;
  group.captured_to = timestamps.length ? timestamps[timestamps.length - 1] : null;
}

function renderGroupFacts(group) {
  document.getElementById('locationReviewGroupTitle').textContent = formatNumber(group.count) + (group.count === 1 ? ' photo' : ' photos');
  document.getElementById('locationReviewGroupPosition').textContent = (state.mode === 'time' ? 'Suggested outing ' : 'Coordinate group ') + (state.currentIndex + 1) + ' of ' + state.groups.length;
  document.getElementById('locationReviewCaptured').textContent = formatCaptureRange(group);
  document.getElementById('locationReviewSpread').textContent = state.mode === 'time' ? (group.captured_from ? 'Capture time · review before assigning' : 'Date unavailable · review individually') : group.spread_m < 10 ? 'Same point' : formatDistance(group.spread_m) + ' from center';
  document.getElementById('locationReviewCoordinates').textContent =
    state.mode === 'time' ? 'No usable GPS. Assigning a saved or searched place uses that place’s coordinates; a custom name adds a location label only.' :
    'Center: ' + group.center.lat.toFixed(6) + ', ' + group.center.lng.toFixed(6) + '. These are the original photo coordinates; assigning a name will not change them.';
  document.getElementById('locationReviewFilenames').innerHTML = group.photos.map(function(photo) {
    return '<div>' + escapeHtml(photo.filename) + '</div>';
  }).join('');
  document.getElementById('locationReviewPrevious').disabled = state.groups.length < 2;
  document.getElementById('locationReviewNext').disabled = state.groups.length < 2;
  renderThumbnails(group);
}

function renderCurrentGroup() {
  state.assignment = null;
  state.isAssigning = false;
  resetAssignmentProgress();
  setAssignmentNavigationDisabled(false);
  var group = currentGroup();
  if (!group) {
    if (state.skippedGroups.length) {
      showEmpty(
        'Location review paused',
        formatNumber(state.skippedGroups.length) +
          (state.skippedGroups.length === 1 ? ' group was' : ' groups were') +
          ' skipped without changes. ' +
          (state.skippedGroups.length === 1 ? 'It will' : 'They will') +
          ' appear again the next time you open this review set.'
      );
    } else {
      showEmpty('All locations reviewed', 'There are no remaining groups in this set.');
    }
    if (state.mode === 'discrepancies' && state.queuedCorrections) {
      document.getElementById('locationReviewEmptyMessage').textContent += ' ' + state.queuedCorrections +
        ' corrections were queued. Review queued metadata changes and sync to write them to the sidecars.';
    }
    updateProgress();
    return;
  }
  state.photoPage = 0;
  state.renderToken += 1;
  if (state.mode === 'discrepancies') { renderDiscrepancyGroup(group); updateProgress(); return; }
  var token = state.renderToken;
  state.selectedChoice = null;
  state.savedCandidates = [];
  state.googleCandidates = [];
  state.googleCandidatesLoaded = false;
  state.regionCandidates = [];
  state.searchedCandidate = null;
  document.getElementById('locationReviewSearch').value = '';
  var assign = document.getElementById('locationReviewAssign');
  assign.disabled = true;
  assign.textContent = 'Select a location';
  document.getElementById('locationReviewSuggestionStatus').textContent = 'Looking nearby…';
  document.getElementById('locationReviewCandidates').innerHTML = '<div class="location-review-loading">Finding saved and nearby locations…</div>';
  renderGroupFacts(group);
  renderPhotoMarkers(group);
  updateProgress();
  loadSuggestions(group, token);
}

function moveGroup(direction) {
  if (state.isAssigning || hasPartialAssignmentProgress() || state.groups.length < 2) return;
  state.currentIndex = (state.currentIndex + direction + state.groups.length) % state.groups.length;
  renderCurrentGroup();
}
