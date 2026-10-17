// The Google Maps or Leaflet map, photo and candidate markers, and place search.
// Classic page script; load boot.js after all definitions.
'use strict';

function clearMapObjects() {
  if (state.mapMode === 'google') {
    state.photoMarkers.forEach(function(marker) { removeLocationMarker(marker); });
    state.candidateMarkers.forEach(function(marker) { removeLocationMarker(marker); });
  } else if (state.mapMode === 'leaflet' && state.map) {
    state.photoMarkers.forEach(function(marker) { removeLocationMarker(marker); });
    state.candidateMarkers.forEach(function(marker) { removeLocationMarker(marker); });
  }
  state.photoMarkers = [];
  state.candidateMarkers = [];
}

/** @param {LocationGroup} group */ function renderPhotoMarkers(group) {
  clearMapObjects();
  if (!state.map || !group || !group.center) return;
  /** @type {(LocationPhoto | {latitude:number;longitude:number;filename:string;assigned:true})[]} */ var mapPhotos = group.photos.slice();
  if (state.mode === 'discrepancies') {
    mapPhotos.push({latitude: locationAssignedPlace(group).latitude, longitude: locationAssignedPlace(group).longitude,
      filename: 'Assigned place: ' + locationAssignedPlace(group).name, assigned: true});
  }
  if (state.mapMode === 'google') {
    var bounds = new google.maps.LatLngBounds();
    mapPhotos.forEach(function(photo) {
      var position = {lat: photo.latitude, lng: photo.longitude};
      var marker = new google.maps.Marker({
        position: position,
        map: locationGoogleMap(),
        title: photo.filename,
        icon: {
          path: google.maps.SymbolPath.CIRCLE,
          scale: photo.assigned ? 9 : 4,
          fillColor: photo.assigned ? '#f5b942' : '#24E5CA',
          fillOpacity: .92,
          strokeColor: '#ffffff',
          strokeWeight: 1.5,
        },
        zIndex: 20,
      });
      if (!photo.assigned) marker.addListener('click', function() { openPhotoPreview(photo); });
      state.photoMarkers.push(marker);
      bounds.extend(position);
    });
    locationGoogleMap().fitBounds(bounds, 48);
    google.maps.event.addListenerOnce(locationGoogleMap(), 'idle', function() {
      if (locationGoogleMap().getZoom() > 17) locationGoogleMap().setZoom(17);
    });
  } else {
    /** @type {[number,number][]} */ var latLngs = [];
    mapPhotos.forEach(function(photo) {
      var marker = L.marker([photo.latitude, photo.longitude], {
        icon: L.divIcon({className: '', html: '<div class="location-review-photo-dot"' + (photo.assigned ? ' style="background:#f5b942;width:19px;height:19px"' : '') + '></div>', iconSize: [19,19], iconAnchor: [9,9]}),
        title: photo.filename,
      }).addTo(locationLeafletMap());
      if (!photo.assigned) marker.on('click', function() { openPhotoPreview(photo); });
      state.photoMarkers.push(marker);
      latLngs.push([photo.latitude, photo.longitude]);
    });
    if (latLngs.length === 1) locationLeafletMap().setView(latLngs[0] || [0,0], 16);
    else locationLeafletMap().fitBounds(latLngs, {padding: [40, 40], maxZoom: 17});
    setTimeout(function() { locationLeafletMap().invalidateSize(); }, 0);
  }
}

function renderCandidateMarkers() {
  if (!state.map) return;
  if (state.mapMode === 'google') {
    state.candidateMarkers.forEach(function(marker) { removeLocationMarker(marker); });
  } else {
    state.candidateMarkers.forEach(function(marker) { removeLocationMarker(marker); });
  }
  state.candidateMarkers = [];
  visibleCandidates().forEach(function(choice) {
    if (choice.latitude == null || choice.longitude == null) return;
    var selected = candidateKey(choice) === candidateKey(state.selectedChoice);
    if (state.mapMode === 'google') {
      var marker = new google.maps.Marker({
        position: {lat: Number(choice.latitude), lng: Number(choice.longitude)},
        map: locationGoogleMap(),
        title: choice.name,
        label: selected ? {text: '✓', color: '#05211d', fontWeight: '700'} : null,
        opacity: selected ? 1 : .72,
        zIndex: selected ? 40 : 10,
      });
      marker.addListener('click', function() { selectChoice(choice); });
      state.candidateMarkers.push(marker);
    } else if (choice.kind === 'keyword') {
      var leafletMarker = L.marker([choice.latitude, choice.longitude], {title: choice.name})
        .addTo(locationLeafletMap()).bindTooltip(choice.name);
      leafletMarker.on('click', function() { selectChoice(choice); });
      state.candidateMarkers.push(leafletMarker);
    }
  });
}

/** @param {string} apiKey @param {boolean} preferEnglish */ function loadGoogleMaps(apiKey, preferEnglish) {
  return new Promise(function(resolve, reject) {
    if (window.google && window.google.maps) { resolve(window.google); return; }
    window._vireoLocationReviewMapsReady = function() { resolve(window.google); };
    var script = document.createElement('script');
    script.src = 'https://maps.googleapis.com/maps/api/js?key=' +
      encodeURIComponent(apiKey) + '&libraries=places' +
      (preferEnglish ? '&language=en' : '') +
      '&loading=async&callback=_vireoLocationReviewMapsReady';
    script.async = true;
    script.onerror = function() { reject(new Error('maps_load_failed')); };
    document.head.appendChild(script);
  });
}

function loadLeaflet() {
  if (window._vireoLeafletPromise) return window._vireoLeafletPromise;
  window._vireoLeafletPromise = new Promise(function(resolve, reject) {
    if (window.L) { resolve(window.L); return; }
    var link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css';
    document.head.appendChild(link);
    var script = document.createElement('script');
    script.src = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js';
    script.async = true;
    script.onload = function() { resolve(window.L); };
    script.onerror = function() { reject(new Error('leaflet_load_failed')); };
    document.head.appendChild(script);
  });
  return window._vireoLeafletPromise;
}

async function initMap() {
  var message = /** @type {HTMLElement} */ (document.getElementById('locationReviewMapMessage'));
  if (state.config && state.config.google_maps_api_key) {
    try {
      await loadGoogleMaps(
        state.config.google_maps_api_key,
        state.config.google_maps_prefer_english !== false
      );
      state.mapMode = 'google';
      state.map = new google.maps.Map(/** @type {HTMLElement} */ (document.getElementById('locationReviewMap')), {
        center: {lat: 20, lng: 0}, zoom: 2, mapTypeId: 'roadmap',
        mapTypeControl: true, mapTypeControlOptions: {
          mapTypeIds: ['roadmap', 'satellite', 'terrain'],
          style: google.maps.MapTypeControlStyle.HORIZONTAL_BAR,
        },
        streetViewControl: false, fullscreenControl: true, clickableIcons: true,
      });
      state.placesService = new google.maps.places.PlacesService(state.map);
      bindGoogleSearch();
      state.map.addListener('click', function(event) {
        if (state.mode === 'discrepancies') return;
        if (!event.placeId) return;
        if (event.stop) event.stop();
        state.placesService?.getDetails({placeId: event.placeId, fields: ['place_id','name','types','formatted_address','geometry','address_components']}, function(place, status) {
          if (status !== google.maps.places.PlacesServiceStatus.OK || !place) return;
          var choice = googleResultToChoice(place);
          if (!choice) return;
          choice.address_components = place?.address_components || [];
          state.searchedCandidate = choice;
          selectChoice(choice);
        });
      });
      message.classList.add('location-review-hidden');
      return;
    } catch (e) {
      state.googleMapsUnavailableReason = 'load_failed';
      console.warn('Google Maps unavailable; using basic map', e);
    }
  } else {
    state.googleMapsUnavailableReason = 'missing_key';
  }

  if (state.mode === 'time') { state.mapMode = 'none'; return; }
  state.mapMode = 'leaflet';
  try {
    await loadLeaflet();
  } catch (e) {
    console.warn('Leaflet unavailable; disabling map', e);
  }
  if (!window.L) {
    state.mapMode = 'none';
    state.map = null;
    message.textContent = 'The map could not be loaded. You can still review photo details and assign a custom location name.';
    return;
  }
  var street = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {attribution: '&copy; OpenStreetMap contributors', maxZoom: 19});
  var satellite = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {attribution: '&copy; Esri', maxZoom: 19});
  var terrain = L.tileLayer('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png', {attribution: '&copy; OpenTopoMap', maxZoom: 17});
  state.map = L.map('locationReviewMap', {layers: [street]}).setView([20,0], 2);
  L.control.layers({'Street': street, 'Satellite': satellite, 'Terrain': terrain}).addTo(locationLeafletMap());
  if (state.googleMapsUnavailableReason === 'load_failed') {
    message.innerHTML = 'Google Maps could not be loaded. <a href="/settings#google-maps">Check Settings</a> to restore nearby suggestions.';
  } else {
    message.innerHTML = 'Add a Google Maps key for nearby park and place suggestions. <a href="/settings#google-maps">Open Settings</a>.';
  }
}

function bindGoogleSearch() {
  var input = /** @type {HTMLInputElement} */ (document.getElementById('locationReviewSearch'));
  state.autocomplete = new google.maps.places.Autocomplete(input, {
    fields: ['place_id','name','types','formatted_address','geometry','address_components'],
    types: ['geocode', 'establishment'],
  });
  state.autocomplete.bindTo('bounds', locationGoogleMap());
  state.autocomplete.addListener('place_changed', function() {
    var place = state.autocomplete?.getPlace() || null;
    var choice = googleResultToChoice(place);
    if (!choice) return;
    choice.address_components = place?.address_components || [];
    state.searchedCandidate = choice;
    selectChoice(choice);
    if (place?.geometry?.viewport) locationGoogleMap().fitBounds(place.geometry.viewport);
    else if (choice.latitude != null) locationGoogleMap().panTo({lat: choice.latitude, lng: choice.longitude});
  });
}

/** @returns {google.maps.Map} */
function locationGoogleMap() {
  if (!state.map || !('getZoom' in state.map)) throw new Error('Google map is unavailable');
  return state.map;
}

/** @returns {L.Map} */
function locationLeafletMap() {
  if (!state.map || !('removeLayer' in state.map)) throw new Error('Basic map is unavailable');
  return state.map;
}

/** @param {google.maps.Marker | L.Marker} marker */
function removeLocationMarker(marker) {
  if ('setMap' in marker) marker.setMap(null);
  else if (state.map && 'removeLayer' in state.map) state.map.removeLayer(marker);
}
