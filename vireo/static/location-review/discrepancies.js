// GPS discrepancy review: selecting photos and keeping or correcting their GPS.
// Classic page script; load boot.js after all definitions.
'use strict';

function updateDiscrepancySelection() {
  var count = state.selectedGpsPhotos.size;
  document.getElementById('locationReviewAssign').disabled = state.isAssigning || !count;
  document.getElementById('locationReviewAssign').textContent = 'Use assigned place coordinates' + (count ? ' (' + count + ')' : '');
  document.getElementById('locationReviewKeep').disabled = state.isAssigning || !count;
  document.querySelectorAll('[data-gps-select]').forEach(function(input) {
    input.checked = state.selectedGpsPhotos.has(Number(input.dataset.gpsSelect));
    input.disabled = state.isAssigning;
  });
  ['locationReviewSelectAll', 'locationReviewSelectNone'].forEach(function(id) {
    document.getElementById(id).disabled = state.isAssigning;
  });
}

function renderDiscrepancyGroup(group) {
  state.selectedGpsPhotos = new Set();
  document.getElementById('locationReviewGroupTitle').textContent = group.assigned_location.name;
  document.getElementById('locationReviewGroupPosition').textContent = group.count + (group.count === 1 ? ' photo' : ' photos') + ' · Group ' + (state.currentIndex + 1) + ' of ' + state.groups.length;
  document.getElementById('locationReviewCaptured').textContent = formatCaptureRange(group);
  document.getElementById('locationReviewBasisLabel').textContent = 'Distance from assigned place';
  var distances = group.photos.map(function(p) { return p.distance_m; });
  var nearest = Math.round(Math.min.apply(null, distances));
  var farthest = Math.round(Math.max.apply(null, distances));
  document.getElementById('locationReviewSpread').textContent = (nearest === farthest ? formatNumber(nearest) : formatNumber(nearest) + ' – ' + formatNumber(farthest)) + ' m';
  var assigned = group.assigned_location;
  document.getElementById('locationReviewDiscrepancySummary').textContent =
    'Gold marker: ' + assigned.name + ' (' + assigned.latitude.toFixed(6) + ', ' + assigned.longitude.toFixed(6) + '). Teal markers: original photo GPS.';
  document.getElementById('locationReviewCoordinates').textContent = 'Select photos below the map to review individually or together.';
  document.getElementById('locationReviewFilenames').textContent = '';
  var container = document.getElementById('locationReviewThumbnails');
  container.innerHTML = group.photos.map(function(photo) {
    var override = photo.sidecar_location;
    var detail = override && override.latitude != null && override.longitude != null
      ? '<div>Current sidecar GPS: ' + Number(override.latitude).toFixed(6) + ', ' + Number(override.longitude).toFixed(6) + '</div>' : '';
    return '<div class="location-review-discrepancy-photo"><button class="location-review-thumb" type="button" data-gps-preview="' + photo.id + '">' +
      '<img src="/thumbnails/' + photo.id + '.jpg" alt="" loading="lazy"><span>' + escapeHtml(photo.filename) + '</span></button>' +
      '<label><input type="checkbox" data-gps-select="' + photo.id + '" aria-label="Select ' + escapeAttr(photo.filename) + '"> ' + formatNumber(Math.round(photo.distance_m)) + ' m away</label>' +
      '<div>' + photo.latitude.toFixed(6) + ', ' + photo.longitude.toFixed(6) + '</div>' + detail + '</div>';
  }).join('');
  container.querySelectorAll('[data-gps-preview]').forEach(function(button) {
    button.addEventListener('click', function() {
      openPhotoPreview(group.photos.find(function(p) { return p.id === Number(button.dataset.gpsPreview); }));
    });
  });
  container.querySelectorAll('[data-gps-select]').forEach(function(input) {
    input.addEventListener('change', function() {
      if (input.checked) state.selectedGpsPhotos.add(Number(input.dataset.gpsSelect));
      else state.selectedGpsPhotos.delete(Number(input.dataset.gpsSelect));
      updateDiscrepancySelection();
    });
  });
  renderPhotoMarkers(group);
  updateDiscrepancySelection();
}

async function resolveDiscrepancies(action) {
  var group = currentGroup();
  if (!group || state.isAssigning || !state.selectedGpsPhotos.size) return;
  var selected = group.photos.filter(function(p) { return state.selectedGpsPhotos.has(p.id); });
  var fingerprints = {};
  selected.forEach(function(p) { fingerprints[p.id] = p.fingerprint; });
  if (lightboxIsOpen() && typeof closeLightbox === 'function') closeLightbox();
  state.isAssigning = true;
  setAssignmentNavigationDisabled(true);
  updateDiscrepancySelection();
  try {
    var result = await safeFetch('/api/location-review/resolve-discrepancies', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({action: action, photo_ids: selected.map(function(p) { return p.id; }), fingerprints: fingerprints})
    });
    state.queuedCorrections += result.queued;
    var message = result.queued ? result.queued + (result.queued === 1 ? ' correction queued.' : ' corrections queued.') + ' Review metadata changes and sync to write the sidecars.'
      : selected.length + (selected.length === 1 ? ' photo kept unchanged.' : ' photos kept unchanged.') + ' This decision is remembered until their location data changes.';
    document.getElementById('locationReviewCorrectionStatus').textContent = message;
    showToast(message);
    group.photos = group.photos.filter(function(p) { return !state.selectedGpsPhotos.has(p.id); });
    group.photo_ids = group.photos.map(function(p) { return p.id; });
    group.count = group.photos.length;
    if (group.count) refreshGroupMetadata(group);
    if (!group.count) {
      state.groups.splice(state.currentIndex, 1);
      state.assignedGroups += 1;
      state.currentIndex = Math.min(state.currentIndex, Math.max(0, state.groups.length - 1));
    }
    checkPendingSync();
    renderCurrentGroup();
  } catch (e) {
    document.getElementById('locationReviewCorrectionStatus').textContent = e.message || 'Could not save the review. Reload and try again.';
  } finally {
    state.isAssigning = false;
    setAssignmentNavigationDisabled(false);
    updateDiscrepancySelection();
  }
}
