// Loading saved, nearby Google, and broader-area suggestions for a group.
// Classic page script; load boot.js after all definitions.
'use strict';

function renderSavedTimeLocations() {
  if (state.isAssigning || hasPartialAssignmentProgress()) return;
  var query = /** @type {HTMLInputElement} */ (document.getElementById('locationReviewSearch')).value.trim().toLocaleLowerCase();
  state.savedCandidates = state.savedLocations.filter(function(keyword) {
    return !query || keyword.name.toLocaleLowerCase().includes(query);
  }).slice(0, 20).map(function(keyword) {
    return normalizeCandidate(Object.assign({}, keyword, {kind: 'keyword', keyword_id: keyword.id}));
  });
  /** @type {HTMLElement} */ (document.getElementById('locationReviewSuggestionStatus')).textContent = 'Choose a saved place or search';
  renderCandidates();
}

/** @param {LocationGroup} group @param {number} token */ async function loadSavedSuggestions(group, token) {
  var radius = Math.min(100000, Math.max(25000, Number(group.spread_m || 0) + 10000));
  var params = new URLSearchParams({
    lat: String(locationGroupCenter(group).lat),
    lng: String(locationGroupCenter(group).lng),
    radius_m: String(radius),
  });
  try {
    /** @type {{suggestions?:LocationChoice[]}} */ var data = await safeFetch('/api/location-review/saved-suggestions?' + params.toString(), {}, {toast: false});
    if (token !== state.renderToken) return;
    state.savedCandidates = (data.suggestions || []).map(function(item) {
      return normalizeCandidate(Object.assign({kind: 'keyword'}, item));
    });
  } catch (e) {}
}

/** @param {LocationPlace|null} result @param {string=} typeLabel */ function googleResultToChoice(result, typeLabel) {
  if (!result) return null;
  var placeId = result.place_id || result.id;
  var name = result.name || (result.displayName && (typeof result.displayName === 'string' ? result.displayName : result.displayName.text)) || '';
  var location = result.geometry && result.geometry.location ? result.geometry.location : result.location;
  var lat = location && (typeof location.lat === 'function' ? location.lat() : location.lat);
  var lng = location && (typeof location.lng === 'function' ? location.lng() : location.lng);
  var types = Array.isArray(result.types) ? result.types : [];
  if (!placeId || !name || types.indexOf('plus_code') !== -1 || /^[A-Z0-9]{4,}\+[A-Z0-9]+\b/i.test(name)) return null;
  return normalizeCandidate({
    kind: 'google', place_id: placeId, name: name, types: types,
    type_label: typeLabel || '', latitude: Number(lat), longitude: Number(lng),
    context: result.vicinity || result.formatted_address || result.formattedAddress || '',
    _googleResult: result,
  });
}

/** @param {google.maps.places.SearchRequest} request @returns {Promise<LocationPlace[]>} */ function nearbySearch(request) {
  return new Promise(function(resolve) {
    if (!state.placesService) { resolve([]); return; }
    state.placesService.nearbySearch(request, function(results, status) {
      var ok = status === google.maps.places.PlacesServiceStatus.OK;
      resolve(ok ? (results || []) : []);
    });
  });
}

/** @param {google.maps.LatLng} location @returns {Promise<LocationPlace[]>} */ function geocodeLocation(location) {
  return new Promise(function(resolve) {
    var geocoder = new google.maps.Geocoder();
    geocoder.geocode({location: location}, function(results, status) {
      resolve(status === 'OK' ? (results || []) : []);
    });
  });
}

/** @param {LocationGroup} group */ function representativeGroupLocations(group) {
  var center = {lat: Number(locationGroupCenter(group).lat), lng: Number(locationGroupCenter(group).lng)};
  var positions = (group.photos || []).map(function(photo) {
    return {lat: Number(photo.latitude), lng: Number(photo.longitude)};
  }).filter(function(position) {
    return Number.isFinite(position.lat) && Number.isFinite(position.lng);
  });
  /** @type {LocationPoint[]} */ var unique = [];
  /** @type {Record<string,boolean>} */ var seen = {};
  positions.forEach(function(position) {
    var key = position.lat.toFixed(5) + ',' + position.lng.toFixed(5);
    if (seen[key]) return;
    seen[key] = true;
    unique.push(position);
  });
  if (!unique.length) return [center];
  var farthestFromCenter = unique.slice().sort(function(a, b) {
    return distanceBetween(center.lat, center.lng, b.lat, b.lng)
      - distanceBetween(center.lat, center.lng, a.lat, a.lng);
  })[0] || center;
  var farthestFromEdge = unique.slice().sort(function(a, b) {
    return distanceBetween(farthestFromCenter.lat, farthestFromCenter.lng, b.lat, b.lng)
      - distanceBetween(farthestFromCenter.lat, farthestFromCenter.lng, a.lat, a.lng);
  })[0] || center;
  var samples = [center, farthestFromCenter, farthestFromEdge];
  /** @type {Record<string,boolean>} */ var sampleSeen = {};
  return samples.filter(function(position) {
    var key = position.lat.toFixed(5) + ',' + position.lng.toFixed(5);
    if (sampleSeen[key]) return false;
    sampleSeen[key] = true;
    return true;
  });
}

/** @param {LocationPlace} result @param {LocationPoint} center */ function regionChoiceFromResult(result, center) {
  const matchedType = Object.keys(AREA_TYPES).find(function(type) {
    return (result.types || []).indexOf(type) !== -1;
  });
  if (!matchedType) return null;
  var component = (result.address_components || []).find(function(item) {
    return (item.types || []).indexOf(matchedType) !== -1;
  });
  if (!component) return null;
  var details = AREA_TYPES[matchedType]; if (!details) return null;
  var namedResult = Object.assign({}, result, {name: component.long_name});
  var choice = googleResultToChoice(namedResult, details.label);
  if (!choice) return null;
  choice.category = 'areas';
  choice.place_type = matchedType;
  choice.area_scope = details.scope;
  choice.relationship = 'at_photos';
  choice.address_components = result.address_components || [];
  choice.distance_m = distanceBetween(center.lat, center.lng, Number(choice.latitude), Number(choice.longitude));
  return choice;
}

/** @param {LocationGroup} group @param {number} token */ async function loadGoogleSuggestions(group, token) {
  if (state.mapMode !== 'google' || !state.placesService) return;
  if (state.googleCandidatesLoaded) return;
  var location = new google.maps.LatLng(locationGroupCenter(group).lat, locationGroupCenter(group).lng);
  var radius = Math.min(50000, Math.max(10000, Number(group.spread_m || 0) + 12000));
  var requests = [
    {location: location, radius: radius, type: 'park'},
    {location: location, radius: radius, type: 'campground'},
    {location: location, radius: radius, keyword: 'state park wildlife refuge nature preserve hiking area'},
    {location: location, radius: radius},
  ];
  var geocodeLocations = representativeGroupLocations(group).map(function(position) {
    return new google.maps.LatLng(position.lat, position.lng);
  });
  var payloads = await Promise.all([
    Promise.all(requests.map(nearbySearch)),
    Promise.all(geocodeLocations.map(geocodeLocation)),
  ]);
  if (token !== state.renderToken) return;
  var center = locationGroupCenter(group);
  /** @type {LocationChoice[]} */ var choices = [];
  payloads[0].forEach(function(results) {
    results.forEach(function(result) {
      var choice = googleResultToChoice(result);
      if (!choice) return;
      choice.distance_m = distanceBetween(center.lat, center.lng, Number(choice.latitude), Number(choice.longitude));
      choices.push(choice);
    });
  });
  choices.sort(function(a, b) { return Number(a.distance_m) - Number(b.distance_m); });
  /** @type {Record<string,boolean>} */ var seenPlaceIds = {};
  choices = choices.filter(function(choice) {
    if (seenPlaceIds[choice.place_id || '']) return false;
    seenPlaceIds[choice.place_id || ''] = true;
    return true;
  });
  /** @type {Record<string,number>} */ var categoryCounts = {nature: 0, places: 0, areas: 0};
  state.googleCandidates = choices.filter(function(choice) {
    var category = choice.category || 'places';
    if (categoryCounts[category] == null || (categoryCounts[category] || 0) >= 8) return false;
    categoryCounts[category] = (categoryCounts[category] || 0) + 1;
    return true;
  });

  /** @type {Record<string,boolean>} */ var regionSeen = {};
  /** @type {LocationChoice[]} */ var regionChoices = [];
  payloads[1].forEach(function(results) {
    results.forEach(function(result) {
      var choice = regionChoiceFromResult(result, center);
      if (!choice) return;
      var key = candidateKey(choice);
      if (regionSeen[key]) return;
      regionSeen[key] = true;
      regionChoices.push(choice);
    });
  });
  regionChoices.sort(function(a, b) {
    if (a.area_scope !== b.area_scope) return a.area_scope === 'local' ? -1 : 1;
    return Number(a.distance_m) - Number(b.distance_m);
  });
  state.regionCandidates = regionChoices.slice(0, 8);
  state.googleCandidatesLoaded = true;
}

/** @param {number} token */ function finishSuggestionRender(token) {
  if (token !== state.renderToken) return;
  renderCandidates();
  renderCandidateMarkers();
  var count = visibleCandidates().length;
  var status = count ? count + (count === 1 ? ' option' : ' options') : 'No matches';
  if (state.googleMapsUnavailableReason === 'missing_key') {
    status = count ? count + (count === 1 ? ' saved option' : ' saved options') : 'Setup needed';
  } else if (state.googleMapsUnavailableReason === 'load_failed') {
    status = count ? count + (count === 1 ? ' saved option' : ' saved options') : 'Google Maps unavailable';
  }
  /** @type {HTMLElement} */ (document.getElementById('locationReviewSuggestionStatus')).textContent = status;
}

/** @param {LocationGroup} group @param {number} token */ async function loadSuggestions(group, token) {
  if (state.mode === 'time') { renderSavedTimeLocations(); return; }
  await Promise.all([
    loadSavedSuggestions(group, token),
    loadGoogleSuggestions(group, token),
  ]);
  finishSuggestionRender(token);
}
