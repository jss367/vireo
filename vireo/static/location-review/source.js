// The review source: Back to Browse, the collection picker, and changing review options.
// Classic page script; load boot.js after all definitions.
'use strict';

function returnToBrowse() { window.location.href = state.returnUrl || '/browse'; }

function parseSource() {
  var params = new URLSearchParams(window.location.search);
  var collectionId = parseInt(params.get('collection_id') || '', 10);
  if (!isNaN(collectionId)) return { collection_id: collectionId };
  if (params.get('scope') === 'all') return {scope: 'all'};
  if (params.get('source') !== 'selection') return null;
  try {
    var stored = JSON.parse(sessionStorage.getItem('vireoLocationReviewSource') || 'null');
    if (stored && Array.isArray(stored.photo_ids) && stored.photo_ids.length) {
      return { photo_ids: stored.photo_ids };
    }
  } catch (e) {}
  return null;
}

function collectionOptionLabel(collection) {
  var count = collection.available_photo_count != null
    ? collection.available_photo_count
    : collection.photo_count;
  if (count == null) return collection.name;
  var offline = Number(collection.offline_photo_count || 0);
  return collection.name + ' (' + formatNumber(count) + ' available' +
    (offline ? ', ' + formatNumber(offline) + ' offline' : '') + ')';
}

async function loadCollectionChoices() {
  var select = document.getElementById('locationReviewCollection');
  try {
    state.collections = await safeFetch('/api/collections', {}, {toast: false});
  } catch (e) {
    select.options[0].textContent = 'Collections unavailable';
    throw e;
  }

  state.collections.forEach(function(collection) {
    var option = document.createElement('option');
    option.value = String(collection.id);
    option.textContent = collectionOptionLabel(collection);
    if (collection.count_error) {
      option.disabled = true;
      option.textContent = collection.name + ' (unavailable — edit rules to fix)';
    } else if (collection.has_visual) {
      // Location review consumes the collection via
      // get_collection_photo_ids, which is rules-only. Match the
      // server-side 400 by disabling with an explanatory label.
      option.disabled = true;
      option.textContent = collection.name + ' (visual — open in Browse)';
      option.title = 'Visual collections can only be used from Browse, where the filter bar resolves the visual-search clause.';
    }
    select.appendChild(option);
  });

  if (state.source && state.source.photo_ids) {
    var selectionOption = document.createElement('option');
    selectionOption.value = '__selection__';
    selectionOption.textContent = 'Selected photos (' + formatNumber(state.source.photo_ids.length) + ')';
    select.insertBefore(selectionOption, select.options[1] || null);
    select.value = '__selection__';
  } else if (state.source && state.source.scope === 'all') {
    select.value = 'all';
  } else if (state.source && state.source.collection_id != null) {
    select.value = String(state.source.collection_id);
  }
}

function changeReviewSource(source) {
  if (state.isAssigning || hasPartialAssignmentProgress()) return;
  if (!source) {
    document.getElementById('locationReviewMode').value = state.mode;
    document.getElementById('locationReviewGap').value = String(state.gapMinutes);
    showToast('Choose a collection or All available photos before changing review options.', 'error');
    return;
  }
  var params = new URLSearchParams();
  if (source && source.collection_id != null) params.set('collection_id', source.collection_id);
  else if (source && source.photo_ids) params.set('source', 'selection');
  else params.set('scope', 'all');
  if (document.getElementById('locationReviewMode').value === 'time') {
    params.set('mode', 'time');
    params.set('gap_minutes', document.getElementById('locationReviewGap').value);
  }
  if (document.getElementById('locationReviewMode').value === 'discrepancies') {
    var distance = document.getElementById('locationReviewDistance');
    if (!distance.reportValidity()) return;
    params.set('mode', 'discrepancies');
    params.set('minimum_distance_m', distance.value);
    params.set('include_reviewed', document.getElementById('locationReviewIncludeKept').checked);
  }
  window.location.href = '/locations/review?' + params.toString();
}

function dropPhotoFromSelectionSource(photoId) {
  if (!state.source || !Array.isArray(state.source.photo_ids)) return;
  if (state.source.photo_ids.indexOf(photoId) === -1) return;
  state.source.photo_ids = state.source.photo_ids.filter(function(id) {
    return id !== photoId;
  });
  try {
    sessionStorage.setItem(
      'vireoLocationReviewSource',
      JSON.stringify({photo_ids: state.source.photo_ids.slice()})
    );
  } catch (e) {}
  var selectionOption = document.querySelector(
    '#locationReviewCollection option[value="__selection__"]'
  );
  if (selectionOption) {
    selectionOption.textContent = 'Selected photos ('
      + formatNumber(state.source.photo_ids.length) + ')';
  }
}
