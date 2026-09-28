/* Browse: detail-panel location editing (Google Places, keywords, EXIF suggestion).
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Location section ---------- */
let googleMapsLoadPromise = null;
function loadGoogleMapsJs() {
  if (googleMapsLoadPromise) return googleMapsLoadPromise;
  const apiKey = window.GOOGLE_MAPS_API_KEY || '';
  if (!apiKey) {
    googleMapsLoadPromise = Promise.reject(new Error('no_api_key'));
    return googleMapsLoadPromise;
  }
  googleMapsLoadPromise = new Promise(function(resolve, reject) {
    window._gmapsCallback = function() { resolve(window.google); };
    var s = document.createElement('script');
    s.src = 'https://maps.googleapis.com/maps/api/js?key=' +
      encodeURIComponent(apiKey) +
      '&libraries=places' +
      (window.GOOGLE_MAPS_PREFER_ENGLISH ? '&language=en' : '') +
      '&loading=async&callback=_gmapsCallback';
    s.async = true;
    s.onerror = function() { reject(new Error('gmaps_load_failed')); };
    document.head.appendChild(s);
  });
  return googleMapsLoadPromise;
}

// Per-input autocomplete state. Keyed by `input` element so we can detect
// whether a `place_changed` event recently fired (and thus the Enter
// handler should defer to it).
var _locationAutocomplete = null;
var _locationLastPickedAt = 0;
var _locationAutocompleteBound = false;
// Pending free-text submit triggered by Enter. Held for ~300ms so a
// keyboard-selected autocomplete suggestion (where place_changed fires
// AFTER the keydown event) gets a chance to cancel the text path.
var _locationPendingTextSubmit = null;
var _locationKeywordState = {
  matches: [],
  activeIndex: -1,
};

function _showLocationError(msg) {
  var el = document.getElementById('locationError');
  if (!el) return;
  el.textContent = msg;
  el.hidden = false;
  setTimeout(function() {
    if (el.textContent === msg) el.hidden = true;
  }, 4000);
}

function _hideLocationError() {
  var el = document.getElementById('locationError');
  if (el) el.hidden = true;
}

function hideLocationKeywordSuggestions() {
  var dropdown = document.getElementById('locationKeywordSuggestions');
  if (dropdown) {
    dropdown.classList.remove('open');
    dropdown.innerHTML = '';
  }
  _locationKeywordState.matches = [];
  _locationKeywordState.activeIndex = -1;
}

function renderLocationKeywordSuggestions() {
  var input = document.getElementById('locationInput');
  var dropdown = document.getElementById('locationKeywordSuggestions');
  if (!input || !dropdown) return;
  var query = input.value.trim().toLowerCase();
  if (!query || !keywordAutocompleteCache || !keywordAutocompleteCache.length) {
    hideLocationKeywordSuggestions();
    return;
  }

  _locationKeywordState.matches = keywordAutocompleteCache
    .filter(function(k) { return k && k.type === 'location'; })
    .map(function(k) {
      return { keyword: k, score: keywordMatchScore(k.search, query) };
    })
    .filter(function(item) { return item.score < 99; })
    .sort(function(a, b) {
      return a.score - b.score || a.keyword.search.localeCompare(b.keyword.search) || a.keyword.id - b.keyword.id;
    })
    .slice(0, 6)
    .map(function(item) { return item.keyword; });

  if (!_locationKeywordState.matches.length) {
    hideLocationKeywordSuggestions();
    return;
  }
  if (_locationKeywordState.activeIndex < 0 || _locationKeywordState.activeIndex >= _locationKeywordState.matches.length) {
    _locationKeywordState.activeIndex = 0;
  }

  dropdown.innerHTML = _locationKeywordState.matches.map(function(k, idx) {
    var active = idx === _locationKeywordState.activeIndex ? ' active' : '';
    var count = k.photo_count === 1 ? '1 photo' : k.photo_count + ' photos';
    var source = k.place_id ? 'Saved Google place' : 'Saved location';
    return '<div class="keyword-suggestion-option' + active + '" role="option" data-index="' + idx + '">' +
      '<span class="keyword-suggestion-name">' + escapeHtml(k.name) + '</span>' +
      '<span class="keyword-suggestion-meta">' + escapeHtml(source + ' · ' + count) + '</span>' +
      '</div>';
  }).join('');
  dropdown.classList.add('open');
}

function updateLocationKeywordSuggestions() {
  var input = document.getElementById('locationInput');
  if (!input || !input.value.trim()) {
    hideLocationKeywordSuggestions();
    return;
  }
  loadKeywordAutocompleteOptions().then(renderLocationKeywordSuggestions);
}

function chooseLocationKeywordSuggestion(index) {
  var keyword = _locationKeywordState.matches[index];
  if (!keyword) return;
  var input = document.getElementById('locationInput');
  if (input) input.value = keyword.name;
  hideLocationKeywordSuggestions();
  if (_locationPendingTextSubmit) {
    clearTimeout(_locationPendingTextSubmit);
    _locationPendingTextSubmit = null;
  }
  _locationLastPickedAt = Date.now();
  _submitLocationKeyword(keyword);
}

function _locationApplyPhotoIds() {
  // Batch flow (Cmd/Ctrl-click without ever opening a single-photo detail)
  // leaves window._detailPhotoId unset even though getActiveSelection() has
  // targets, so derive from the selection directly rather than gating on
  // the focused-detail photo.
  return getActiveSelection();
}

// The batch location endpoints reject more than 1000 photo_ids per request
// (vireo/app.py), and reject the whole request — so posting a bigger selection
// in one shot applied the location to *nothing*. Split it. location_review.html
// chunks against the same cap.
var LOCATION_BATCH_LIMIT = 1000;

// A location save is async (a big batch posts 1000 ids per request, so it can
// take seconds) and the user can open another photo meanwhile. The response
// describes the photos the save targeted (a batch returns photo_ids[0]'s
// location), so painting it after the target changed would show the saved
// place on an unrelated photo, whose × button would then clear *that* photo.
// Capture the target before the await and paint only if it is still open.
function _locationTargetKey() {
  return selectionIdsKey(_locationApplyPhotoIds()) + '|' + (window._detailPhotoId || '');
}

async function _postLocationBatched(endpoint, baseBody, ids) {
  var last = null;
  for (var offset = 0; offset < ids.length; offset += LOCATION_BATCH_LIMIT) {
    var body = Object.assign({}, baseBody, {
      photo_ids: ids.slice(offset, offset + LOCATION_BATCH_LIMIT),
    });
    last = await safeFetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  }
  return last;
}

async function _afterLocationMutation(photoIds, detailPhotoId) {
  // Bump the location-mutation epoch so any in-flight reverse-geocode from an
  // earlier maybeShowExifSuggestion(A) bails out of its post-await paint. Every
  // save/clear/accept path funnels through here, so this is the single choke
  // point where the batch-save-during-pending-geocode race can be closed.
  // Codex P2 on PR #1097 (17:39Z): open A → Cmd-click B → save batch location
  // for {A,B} → geocode resolves. renderBatchInspector rehides #locationFilled,
  // so the DOM-only guard in maybeShowExifSuggestion can't tell the batch was
  // just given a location and repaints A's stale Accept line.
  window._locationMutationEpoch = (window._locationMutationEpoch || 0) + 1;
  invalidateKeywordAutocompleteCache();
  loadKeywords();
  scheduleCollectionCountsRefresh();
  refreshPendingSyncBanner();
  if (activeCollectionId) {
    await filterByCollection(activeCollectionId);
    return;
  }
  // A saved location is a location keyword, so the same rule as the keyword
  // paths applies: re-run the query when the active expression can notice,
  // and keep the user where they were when it does.
  if (window.VireoFilter && VireoFilter.hasFilters() &&
      (!VireoFilter.dependsOnMutation ||
       VireoFilter.dependsOnMutation([MUTATION_KEYWORD]))) {
    resetAndLoad({ preserveScroll: true });
    return;
  }
  if (photoIds && photoIds.length) {
    var idsToRefresh = photoIds.filter(function(id) {
      return !!findBrowsePhoto(id);
    });
    try {
      for (var offset = 0; offset < idsToRefresh.length; offset += 500) {
        var statusData = await safeFetch('/api/photos/by-ids', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({photo_ids: idsToRefresh.slice(offset, offset + 500)})
        }, {toast: false});
        (statusData.photos || []).forEach(function(updated) {
          var local = findBrowsePhoto(updated.id);
          if (local) local.location_status = updated.location_status || 'none';
        });
      }
    } catch (e) {}
    refreshGridCards(idsToRefresh);
    refreshExpandedBrowseStackMembers(idsToRefresh);
  }
  // If a multi-selection is still active, keep the batch inspector rendered.
  // Falling through to loadDetail(anchor) would strip .batch-mode and rewire
  // the rating stars to setRating(anchorId, i) — collapsing the panel to the
  // anchor's single-photo view while the user still has N photos selected.
  var activeSel = getActiveSelection();
  if (activeSel.length > 1) {
    renderBatchInspector(activeSel);
  } else if (detailPhotoId && selectedPhotoId === detailPhotoId) {
    loadDetail(detailPhotoId);
  } else {
    loadSummary();
  }
}

async function bindLocationAutocomplete() {
  if (_locationAutocompleteBound) return;
  // Wait for the /api/config fetch to resolve so window.GOOGLE_MAPS_API_KEY
  // is populated. The user can focus the input before _cfgPromise has
  // resolved on a slow page load — without this await we'd take the
  // empty-key branch permanently.
  try { await _cfgPromise; } catch(e) {}
  if (!window.GOOGLE_MAPS_API_KEY) return;  // no key -> free-text only
  _locationAutocompleteBound = true;
  var input = document.getElementById('locationInput');
  if (!input) return;
  loadGoogleMapsJs().then(function(google) {
    if (!google || !google.maps || !google.maps.places) return;
    _locationAutocomplete = new google.maps.places.Autocomplete(input, {
      types: ['geocode', 'establishment'],
      fields: [
        'place_id',
        'name',
        'types',
        'formatted_address',
        'geometry',
        'address_components'
      ],
    });
    _locationAutocomplete.addListener('place_changed', function() {
      var place = _locationAutocomplete.getPlace();
      if (!place || !place.place_id) return;
      _locationLastPickedAt = Date.now();
      // Cancel any pending free-text submit from the Enter that triggered
      // this place_changed event — keydown fires synchronously BEFORE
      // place_changed, so the text path's setTimeout is still queued.
      if (_locationPendingTextSubmit) {
        clearTimeout(_locationPendingTextSubmit);
        _locationPendingTextSubmit = null;
      }
      _submitLocationPlace(place.place_id, normalizeGooglePlaceForSubmit(place));
    });
  }).catch(function(err) {
    console.warn('Google Maps load failed:', err);
    // Don't disable the input — free-text Enter still works.
  });
}

function normalizeGooglePlaceForSubmit(place) {
  if (!place || !place.place_id) return null;
  var loc = place.geometry && place.geometry.location;
  var lat = null;
  var lng = null;
  if (loc) {
    lat = (typeof loc.lat === 'function') ? loc.lat() : loc.lat;
    lng = (typeof loc.lng === 'function') ? loc.lng() : loc.lng;
  }
  if (lat == null || lng == null) return null;
  return {
    place_id: place.place_id,
    name: place.name || place.formatted_address || '',
    types: Array.isArray(place.types) ? place.types : [],
    lat: lat,
    lng: lng,
    address_components: (place.address_components || []).map(function(c) {
      return {
        name: c.long_name || c.name || '',
        short_name: c.short_name || '',
        types: c.types || [],
      };
    }),
  };
}

async function _submitLocationPlace(placeId, placeDetails) {
  _hideLocationError();
  // Derive the target photos from the batch-aware selection so a fresh
  // multi-select (Cmd/Ctrl-click without ever opening a single-photo detail)
  // still applies to every selected photo. window._detailPhotoId is unset in
  // that flow, so gating on it up front would silently apply to none.
  var ids = _locationApplyPhotoIds();
  if (!ids.length) return;
  var useBatch = ids.length > 1;
  var detailPhotoId = window._detailPhotoId || null;
  var targetKey = _locationTargetKey();
  var body = { place_id: placeId };
  if (placeDetails) body.place = placeDetails;
  try {
    var resp = useBatch
      ? await _postLocationBatched('/api/batch/location', body, ids)
      : await safeFetch('/api/photos/' + ids[0] + '/location', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
    if (resp && resp.location && _locationTargetKey() === targetKey) {
      renderLocationFilled(resp.location);
    }
    await _afterLocationMutation(ids, detailPhotoId);
  } catch(e) {
    _showLocationError(e && e.message ? e.message : 'Could not save location.');
  }
}

async function _submitLocationKeyword(keyword) {
  if (!keyword || !keyword.id) return;
  _hideLocationError();
  var ids = _locationApplyPhotoIds();
  if (!ids.length) return;
  var useBatch = ids.length > 1;
  var detailPhotoId = window._detailPhotoId || null;
  var targetKey = _locationTargetKey();
  var body = { keyword_id: keyword.id };
  try {
    var resp = useBatch
      ? await _postLocationBatched('/api/batch/location', body, ids)
      : await safeFetch('/api/photos/' + ids[0] + '/location', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
    if (resp && resp.location && _locationTargetKey() === targetKey) {
      renderLocationFilled(resp.location);
    }
    await _afterLocationMutation(ids, detailPhotoId);
  } catch(e) {
    _showLocationError(e && e.message ? e.message : 'Could not save location.');
  }
}

async function _submitLocationText(name) {
  _hideLocationError();
  var ids = _locationApplyPhotoIds();
  if (!ids.length) return;
  var useBatch = ids.length > 1;
  var detailPhotoId = window._detailPhotoId || null;
  var targetKey = _locationTargetKey();
  try {
    var resp = useBatch
      ? await _postLocationBatched('/api/batch/location/text', { name: name }, ids)
      : await safeFetch('/api/photos/' + ids[0] + '/location/text', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name: name }),
        });
    if (resp && resp.location) {
      if (_locationTargetKey() === targetKey) renderLocationFilled(resp.location);
      await _afterLocationMutation(ids, detailPhotoId);
    }
  } catch(e) {
    _showLocationError(e && e.message ? e.message : 'Could not save location.');
  }
}

async function clearPhotoLocation() {
  var photoId = window._detailPhotoId;
  if (!photoId) return;
  _hideLocationError();
  try {
    await safeFetch('/api/photos/' + photoId + '/location', { method: 'DELETE' });
    renderLocationEmpty();
    await _afterLocationMutation([photoId], photoId);
  } catch(e) {
    _showLocationError(e && e.message ? e.message : 'Could not clear location.');
  }
}

// Scrub the inline EXIF suggestion line (hide, empty innerHTML, drop the
// data-photo-id owner tag). Every scrub site must go through this helper so
// they can't silently diverge — e.g. if a future scrub also needs to clear a
// stored placeId. Codex nitpick on PR #1097.
function clearExifSuggestion() {
  var sugg = document.getElementById('locationExifSuggestion');
  if (!sugg) return;
  sugg.hidden = true;
  sugg.innerHTML = '';
  if (sugg.dataset) delete sugg.dataset.photoId;
}

function formatCoordinatePair(latitude, longitude) {
  if (latitude == null || longitude == null) return '';
  return Number(latitude).toFixed(5) + ', ' + Number(longitude).toFixed(5);
}

function renderCoordinateStatus(photo) {
  var el = document.getElementById('locationCoordinateStatus');
  if (!el) return;
  var status = photo && photo.location_status ? photo.location_status : 'none';
  var text;
  if (status === 'exif') {
    text = '📍 EXIF GPS — ' + formatCoordinatePair(photo.latitude, photo.longitude) +
      '. Embedded in the original photo and used on the map.';
  } else if (status === 'assigned') {
    var loc = photo.location || {};
    var coords = formatCoordinatePair(loc.latitude, loc.longitude);
    text = '● Assigned map location' + (coords ? ' — ' + coords : '') +
      '. The original photo does not contain a complete EXIF GPS pair.';
  } else {
    text = '⊘ No coordinates — this photo has no complete EXIF GPS pair or assigned map location.';
  }
  el.className = 'coordinate-status ' + status;
  el.textContent = text;
}

function renderLocationFilled(loc) {
  var filled = document.getElementById('locationFilled');
  var empty = document.getElementById('locationEmpty');
  if (!filled || !empty) return;
  if (!loc) { renderLocationEmpty(); return; }
  var parents = (loc.parent_chain || [])
    .map(function(p) { return p && p.name ? p.name : ''; })
    .filter(Boolean);
  var parentsHtml = parents.length
    ? '<span class="filled-parents">' + parents.map(escapeHtml).join(' · ') + '</span>'
    : '';
  filled.innerHTML =
    '<div class="filled-row">' +
      '<div class="filled-text">' +
        '<span class="filled-place">' + escapeHtml(loc.name || '') + '</span>' +
        parentsHtml +
      '</div>' +
      '<button class="clear-location" type="button" title="Clear location" onclick="clearPhotoLocation()">×</button>' +
    '</div>';
  filled.hidden = false;
  empty.hidden = true;
  // Reset the input value for next time.
  var input = document.getElementById('locationInput');
  if (input) input.value = '';
  // Once a saved location is on screen any pending EXIF suggestion is moot,
  // and leaving it (hidden) in #locationEmpty makes it stale state: open A
  // (suggestion fetched) → open B with a saved location → Cmd-click A would
  // otherwise resurrect A's Accept line for the {B, A} batch and overwrite
  // B's location (Codex P2 on PR #1097). Clear it at the source instead.
  clearExifSuggestion();
}

function renderLocationEmpty(opts) {
  opts = opts || {};
  var filled = document.getElementById('locationFilled');
  var empty = document.getElementById('locationEmpty');
  if (!filled || !empty) return;
  filled.innerHTML = '';
  filled.hidden = true;
  empty.hidden = false;
  var input = document.getElementById('locationInput');
  if (input) input.value = '';
  hideLocationKeywordSuggestions();
  // Reset any prior EXIF suggestion. The caller (renderDetail) re-runs
  // maybeShowExifSuggestion(photo) after this, which decides whether to
  // populate the line for the new photo. Entering batch mode preserves it
  // instead (preserveExifSuggestion) so the anchor's suggestion stays
  // acceptable for the whole selection — but only while the photo that
  // produced it is still in the selection. Otherwise Accept would apply
  // that anchor's GPS-derived location to unrelated photos (Codex P1 on
  // PR #1097: open A → Cmd-click B → Cmd-click A to drop → Cmd-click C).
  var sugg = document.getElementById('locationExifSuggestion');
  if (!sugg) return;
  var keepSugg = false;
  if (opts.preserveExifSuggestion) {
    var ownerAttr = sugg.dataset ? sugg.dataset.photoId : '';
    var owner = ownerAttr ? parseInt(ownerAttr, 10) : NaN;
    keepSugg = !isNaN(owner)
      && Array.isArray(opts.selectionIds)
      && opts.selectionIds.indexOf(owner) !== -1;
  }
  if (!keepSugg) clearExifSuggestion();
}

/* When a photo with EXIF GPS but no location keyword is opened, fetch a
   reverse-geocode suggestion and offer it as an inline Accept line. Bails
   silently when there's no key or no coords; the proxy returns null when
   Google has no match for the cell. */
async function maybeShowExifSuggestion(photo) {
  clearExifSuggestion();
  var sugg = document.getElementById('locationExifSuggestion');
  if (!sugg) return;

  if (!photo) return;
  if (photo.location) return;
  if (photo.latitude == null || photo.longitude == null) return;

  // Capture the photo id at request time so a slow reverse-geocode for an
  // older photo doesn't paint into the suggestion line for a newer one.
  var requestPhotoId = photo.id;
  // Capture the location-mutation epoch BEFORE the first await, so any
  // save/clear/accept during _cfgPromise or the reverse-geocode fetch bumps
  // the counter past what we captured. The post-await guard bails on any
  // mismatch. Closes the batch-save-during-pending-geocode race that the
  // DOM `!filled.hidden` check alone can't catch (see below).
  var requestEpoch = window._locationMutationEpoch || 0;

  await _cfgPromise;
  var apiKey = (window.GOOGLE_MAPS_API_KEY || '').trim();
  if (!apiKey) return;
  try {
    var params = new URLSearchParams({lat: photo.latitude, lng: photo.longitude});
    var r = await fetch('/api/places/reverse-geocode?' + params.toString());
    if (!r.ok) return;
    var data = await r.json();
    if (!data || !data.place_id || !data.summary) return;
    // Three races the post-await guards must handle.
    //   1. Stale response for a photo the detail already navigated away
    //      from (open A → open B): caught by _detailPhotoId, which
    //      renderDetail overwrites on every open.
    //   2. Slow response for an anchor the user has since dropped from a
    //      batch (open A → Cmd-click B → Cmd-click A out → Cmd-click C):
    //      renderBatchInspector never touches _detailPhotoId, so the anchor
    //      pointer could still match while getActiveSelection() no longer
    //      includes A. Accept posts to getActiveSelection(), so the guard
    //      must recheck selection membership too. Codex P1 on PR #1097.
    //   3. Slow response for a photo the user has left behind by closing
    //      the detail panel, clearing the batch, or Cmd-click-dropping the
    //      anchor, then Select All that brings the departed photo back
    //      into the batch. The DOM scrub in each of those sites isn't
    //      enough on its own: without also nulling _detailPhotoId the
    //      pointer keeps matching after Select All re-includes the photo,
    //      and Accept would apply the departed anchor's EXIF place to the
    //      whole batch. Those sites now null _detailPhotoId, so the first
    //      guard catches this race too. Codex P2 on PR #1097 (17:04Z).
    if (window._detailPhotoId !== requestPhotoId) return;
    if (_locationApplyPhotoIds().indexOf(requestPhotoId) === -1) return;
    // Fourth race: while we were awaiting reverse-geocode, the user saved
    // a location for this photo (via the input, a keyword pick, or a batch
    // op). Every mutation path funnels through _afterLocationMutation, which
    // bumps _locationMutationEpoch. If it moved, bail. (Codex P2 on PR #1097
    // at 17:39Z: save-batch-location-during-pending-geocode overwriting the
    // just-saved batch location. The DOM `!filled.hidden` check below alone
    // can't catch that — after a batch save, _afterLocationMutation runs
    // renderBatchInspector → renderLocationEmpty, which rehides
    // #locationFilled, so the paint sneaks through.)
    if ((window._locationMutationEpoch || 0) !== requestEpoch) return;
    // Defense-in-depth: if a filled row is on screen right now, whoever
    // painted it wanted the suggestion gone. The epoch check above already
    // covers the batch race; this catches any future save site that renders
    // filled without funneling through _afterLocationMutation.
    var filled = document.getElementById('locationFilled');
    if (filled && !filled.hidden) return;

    sugg.innerHTML = '';
    var text = document.createElement('span');
    text.textContent = '💡 EXIF says: ' + data.summary + '  ';
    var btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'accept-btn';
    btn.textContent = 'Accept';
    btn.dataset.placeId = data.place_id;
    btn.addEventListener('click', function() {
      acceptExifSuggestion(data.place_id);
    });
    sugg.appendChild(text);
    sugg.appendChild(btn);
    sugg.dataset.photoId = String(requestPhotoId);
    sugg.hidden = false;
  } catch (e) {
    console.warn('reverse-geocode suggestion failed:', e);
  }
}

async function acceptExifSuggestion(placeId) {
  var photoId = window._detailPhotoId;
  if (!photoId || !placeId) return;
  _hideLocationError();
  var ids = _locationApplyPhotoIds();
  var useBatch = ids.length > 1;
  var targetKey = _locationTargetKey();
  try {
    var resp = useBatch
      ? await _postLocationBatched('/api/batch/location', { place_id: placeId }, ids)
      : await safeFetch('/api/photos/' + photoId + '/location', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ place_id: placeId }),
        });
    // The suggestion line now belongs to whatever photo is open; only paint
    // the saved place or clear the line if that is still the accepted target.
    if (_locationTargetKey() === targetKey) {
      if (resp && resp.location) renderLocationFilled(resp.location);
      clearExifSuggestion();
    }
    await _afterLocationMutation(useBatch ? ids : [photoId], photoId);
  } catch(e) {
    _showLocationError(e && e.message ? e.message : 'Could not save location.');
  }
}

function _onLocationInputFocus() {
  bindLocationAutocomplete();
  updateLocationKeywordSuggestions();
}

function _onLocationInputInput() {
  _locationKeywordState.activeIndex = 0;
  updateLocationKeywordSuggestions();
}

function _onLocationInputKeydown(e) {
  var dropdown = document.getElementById('locationKeywordSuggestions');
  var localOpen = dropdown && dropdown.classList.contains('open') && _locationKeywordState.matches.length > 0;
  var googlePlacesMayHandleKeyboard = !!(window.GOOGLE_MAPS_API_KEY || '').trim();
  if (localOpen && !googlePlacesMayHandleKeyboard && e.key === 'ArrowDown') {
    e.preventDefault();
    _locationKeywordState.activeIndex = (_locationKeywordState.activeIndex + 1) % _locationKeywordState.matches.length;
    renderLocationKeywordSuggestions();
    return;
  }
  if (localOpen && !googlePlacesMayHandleKeyboard && e.key === 'ArrowUp') {
    e.preventDefault();
    _locationKeywordState.activeIndex = (_locationKeywordState.activeIndex - 1 + _locationKeywordState.matches.length) % _locationKeywordState.matches.length;
    renderLocationKeywordSuggestions();
    return;
  }
  if (e.key === 'Escape') {
    hideLocationKeywordSuggestions();
    return;
  }
  if (e.key !== 'Enter') return;
  if (localOpen && !googlePlacesMayHandleKeyboard && _locationKeywordState.activeIndex >= 0) {
    e.preventDefault();
    chooseLocationKeywordSuggestion(_locationKeywordState.activeIndex);
    return;
  }
  // DO NOT preventDefault — Google's autocomplete listens for Enter to
  // pick the highlighted suggestion. Suppressing the event would block
  // keyboard place selection. The input has no surrounding form, so
  // letting Enter through has no native side effect.
  // If a place was JUST picked (e.g. mouse click within ~500ms ago),
  // skip outright.
  if (Date.now() - _locationLastPickedAt < 500) return;
  var input = e.target;
  var val = (input.value || '').trim();
  if (!val) return;
  // Defer the free-text submit. If a Google suggestion was highlighted
  // and the user pressed Enter to pick it, place_changed will fire after
  // this keydown (synchronously, but still after the event-loop tick) and
  // cancel our pending timeout. If place_changed never fires (no
  // suggestion highlighted), the text submit goes through after 300ms.
  if (_locationPendingTextSubmit) clearTimeout(_locationPendingTextSubmit);
  _locationPendingTextSubmit = setTimeout(function() {
    _locationPendingTextSubmit = null;
    // Final guard: place_changed may have raced in just before this fired.
    if (Date.now() - _locationLastPickedAt < 1000) return;
    _submitLocationText(val);
  }, 300);
}

// Wire up listeners once on initial script load. The input element exists
// in the DOM unconditionally (server renders the section), so we don't need
// to re-bind on every photo load.
document.addEventListener('DOMContentLoaded', function() {
  var input = document.getElementById('locationInput');
  if (input) {
    input.addEventListener('focus', _onLocationInputFocus);
    input.addEventListener('input', _onLocationInputInput);
    input.addEventListener('keydown', _onLocationInputKeydown);
    input.addEventListener('blur', function() {
      setTimeout(hideLocationKeywordSuggestions, 120);
    });
  }
  var locationDropdown = document.getElementById('locationKeywordSuggestions');
  if (locationDropdown) {
    locationDropdown.addEventListener('mousedown', function(e) {
      var option = e.target.closest('.keyword-suggestion-option');
      if (!option) return;
      e.preventDefault();
      chooseLocationKeywordSuggestion(parseInt(option.dataset.index, 10));
    });
  }
  bindKeywordAutocomplete('addKeywordInput', 'addKeywordSuggestions', addKeyword);
  bindKeywordAutocomplete('batchKeywordInput', 'batchKeywordSuggestions', function() {
    confirmBatchKeyword();
  });
});
