// Opening the review set: options, collections, config, the map, and the preview.
// Classic page script; load boot.js after all definitions.
'use strict';

async function initialize() {
  state.source = parseSource();
  var sourceParams = new URLSearchParams(window.location.search);
  if (state.mode !== 'coordinates' && !state.source && !sourceParams.has('source') && !sourceParams.has('collection_id')) {
    state.source = {scope: 'all'};
  }
  if ([15, 30, 60, 120].indexOf(state.gapMinutes) === -1) state.gapMinutes = 60;
  document.getElementById('locationReviewMode').value = state.mode;
  document.getElementById('locationReviewGap').value = String(state.gapMinutes);
  document.body.classList.toggle('location-review-time', state.mode === 'time');
  document.body.classList.toggle('location-review-discrepancies', state.mode === 'discrepancies');
  document.querySelectorAll('.discrepancy-only').forEach(function(el) { el.hidden = state.mode !== 'discrepancies'; });
  document.getElementById('locationReviewDistance').value = state.minimumDistance;
  document.getElementById('locationReviewIncludeKept').checked = state.includeKept;
  document.querySelectorAll('.time-only').forEach(function(el) { el.hidden = state.mode !== 'time'; });
  if (state.mode === 'time') {
    document.getElementById('locationReviewScope').textContent = 'Grouping photos by capture time…';
    document.getElementById('locationReviewLocationsHeading').textContent = 'Choose a location';
    document.getElementById('locationReviewBasisLabel').textContent = 'Grouping evidence';
    document.getElementById('locationReviewSearch').placeholder = 'Search saved locations, find a place, or enter a name';
    document.getElementById('locationReviewSuggestionMode').hidden = true;
    try {
      var keywords = await safeFetch('/api/keywords/all', {}, {toast: false});
      state.savedLocations = keywords.filter(function(keyword) { return keyword.type === 'location'; });
    } catch (e) { showToast('Saved locations could not be loaded. You can still enter a custom name.', 'error'); }
  }
  try {
    await loadCollectionChoices();
  } catch (e) {
    if (!state.source) {
      document.getElementById('locationReviewScope').textContent = 'Collections could not be loaded';
      document.getElementById('locationReviewProgressText').textContent = 'Unavailable';
      showEmpty('Could not load collections', e.message || 'The collection list is temporarily unavailable.', true);
      return;
    }
  }
  if (!state.source) {
    document.getElementById('locationReviewScope').textContent = 'Choose a collection to begin';
    document.getElementById('locationReviewProgressText').textContent = 'Ready';
    document.getElementById('locationReviewRemaining').textContent = '';
    document.getElementById('locationReviewProgressFill').style.width = '0';
    showEmpty(
      'Choose a collection',
      'Select a collection or All available photos above. To add locations to photos without GPS, choose Missing GPS · group by time.'
    );
    return;
  }
  try {
    state.config = await safeFetch('/api/config', {}, {toast: false});
  } catch (e) { state.config = {}; }
  await initMap();
  if (state.mode === 'discrepancies') {
    if (state.map) document.getElementById('locationReviewMapMessage').classList.add('location-review-hidden');
    else document.getElementById('locationReviewMapMessage').textContent = 'Map unavailable. Coordinates, distances and photo review are still available.';
    checkPendingSync();
    if (!state.config.write_assigned_location_to_xmp) {
      document.getElementById('locationReviewCorrectionStatus').textContent = 'To queue corrections, first enable Write assigned locations to XMP in Settings.';
    }
  }
  try {
    var preview = await safeFetch('/api/location-review/preview', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(Object.assign({}, state.source, {mode: state.mode, gap_minutes: state.gapMinutes, minimum_distance_m: state.minimumDistance, include_reviewed: state.includeKept}))
    });
    state.groups = preview.groups || [];
    state.initialGroupCount = state.groups.length;
    var scopeParts = [formatNumber(preview.reviewable) + ' photos ready for review'];
    var assigned = (preview.skipped || []).filter(function(photo) { return photo.reason === 'already_has_location'; }).length;
    var withCoordinates = (preview.skipped || []).length - assigned;
    if (assigned) scopeParts.push(formatNumber(assigned) + ' already assigned');
    if (withCoordinates) scopeParts.push(formatNumber(withCoordinates) + ' already have GPS');
    if ((preview.unresolved || []).length) scopeParts.push(formatNumber(preview.unresolved.length) + ' without usable coordinates');
    document.getElementById('locationReviewScope').textContent = scopeParts.join(' · ');
    if (!state.groups.length) {
      showEmpty('No locations to review', state.mode === 'discrepancies' ? 'No unreviewed GPS differences exceed this distance. Photos need both GPS and an assigned place with coordinates. Existing sidecar corrections are excluded.' : preview.total ? (state.mode === 'time' ? 'Every photo in this set already has a location or usable GPS.' : 'Every photo in this set is already assigned or lacks usable coordinates.') : 'This set contains no photos.');
      updateProgress();
      return;
    }
    renderCurrentGroup();
  } catch (e) {
    showEmpty('Could not load locations', e.message || 'The location review set could not be opened.', true);
  }
}
