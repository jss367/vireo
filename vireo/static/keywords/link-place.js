// The link-to-Google-place flow: the Maps loader and config fetch, the modal,
// place normalization, submitLinkPlace, the per-row Link button, and the
// modal's close controls.
// Classic page script; load boot.js after all definitions.

// -- Link-to-Google-place flow ---------------------------------------------

window.GOOGLE_MAPS_PREFER_ENGLISH = true;

// Cached promise for the Google Maps JS loader (memoized).
let _gmapsLoadPromise = null;
function loadGoogleMapsJs() {
  if (_gmapsLoadPromise) return _gmapsLoadPromise;
  const apiKey = window.GOOGLE_MAPS_API_KEY || '';
  if (!apiKey) {
    _gmapsLoadPromise = Promise.reject(new Error('no_api_key'));
    return _gmapsLoadPromise;
  }
  _gmapsLoadPromise = new Promise(function(resolve, reject) {
    window._gmapsCallback = function() { resolve(window.google); };
    const s = document.createElement('script');
    s.src = 'https://maps.googleapis.com/maps/api/js?key=' +
      encodeURIComponent(apiKey) +
      '&libraries=places' +
      (window.GOOGLE_MAPS_PREFER_ENGLISH ? '&language=en' : '') +
      '&loading=async&callback=_gmapsCallback';
    s.async = true;
    s.onerror = function() { reject(new Error('gmaps_load_failed')); };
    document.head.appendChild(s);
  });
  return _gmapsLoadPromise;
}

// Fetch /api/config once on page load to populate window.GOOGLE_MAPS_API_KEY.
// Mirrors browse.html's _cfgPromise pattern. The modal awaits this promise
// before deciding whether to bind autocomplete vs. show the empty-key error.
const _cfgPromise = (async function() {
  try {
    const r = await fetch('/api/config');
    const cfg = await r.json();
    window.GOOGLE_MAPS_API_KEY = cfg.google_maps_api_key || '';
    window.GOOGLE_MAPS_PREFER_ENGLISH = cfg.google_maps_prefer_english !== false;
    return cfg;
  } catch(e) {
    window.GOOGLE_MAPS_API_KEY = '';
    window.GOOGLE_MAPS_PREFER_ENGLISH = true;
    return null;
  }
})();

let _activeLinkAutocomplete = null;
let _activeLinkKeywordId = null;

function _showLinkError(msg) {
  const el = document.getElementById('linkPlaceError');
  if (!el) return;
  el.textContent = msg;
  el.style.display = 'block';
}

function _hideLinkError() {
  const el = document.getElementById('linkPlaceError');
  if (el) el.style.display = 'none';
}

async function openLinkPlaceModal(keywordId, currentName) {
  const modal = document.getElementById('linkPlaceModal');
  const input = document.getElementById('linkPlaceInput');
  if (!modal || !input) return;

  _hideLinkError();
  _activeLinkKeywordId = keywordId;
  input.value = currentName || '';
  modal.classList.add('open');
  // Defer focus so the modal is laid out first.
  setTimeout(function() { input.focus(); input.select(); }, 0);

  // Wait for /api/config so window.GOOGLE_MAPS_API_KEY is populated even
  // on a slow first paint.
  try { await _cfgPromise; } catch(e) {}

  if (!window.GOOGLE_MAPS_API_KEY) {
    _showLinkError(
      'Google Maps API key not configured. Add one in Settings to enable autocomplete.'
    );
    return;
  }

  try {
    const google = await loadGoogleMapsJs();
    if (!google || !google.maps || !google.maps.places) {
      _showLinkError('Google Maps unavailable.');
      return;
    }
    // Detach any previous listener by replacing the autocomplete instance
    // (Google's API doesn't expose a clean detach for the binding to
    // `input`; binding twice triggers two `place_changed` events).
    _activeLinkAutocomplete = new google.maps.places.Autocomplete(input, {
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
    _activeLinkAutocomplete.addListener('place_changed', function() {
      const place = _activeLinkAutocomplete.getPlace();
      if (place && place.place_id && _activeLinkKeywordId) {
        submitLinkPlace(
          _activeLinkKeywordId,
          place.place_id,
          normalizeGooglePlaceForSubmit(place)
        );
      }
    });
  } catch(err) {
    _showLinkError('Failed to load Google Maps: ' + (err && err.message || err));
  }
}

function closeLinkPlaceModal() {
  const modal = document.getElementById('linkPlaceModal');
  if (modal) modal.classList.remove('open');
  _activeLinkKeywordId = null;
  _activeLinkAutocomplete = null;
  _hideLinkError();
}

function normalizeGooglePlaceForSubmit(place) {
  if (!place || !place.place_id) return null;
  const loc = place.geometry && place.geometry.location;
  let lat = null;
  let lng = null;
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

async function submitLinkPlace(keywordId, placeId, placeDetails) {
  _hideLinkError();
  const body = {place_id: placeId};
  if (placeDetails) body.place = placeDetails;
  let resp;
  try {
    resp = await fetch('/api/keywords/' + keywordId + '/link-place', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
  } catch(err) {
    _showLinkError('Network error: ' + (err && err.message || err));
    return;
  }
  if (!resp.ok) {
    let body = {};
    try { body = await resp.json(); } catch(e) {}
    _showLinkError(body.message || body.error || ('Failed (HTTP ' + resp.status + ')'));
    return;
  }
  let data = {};
  try { data = await resp.json(); } catch(e) {}

  closeLinkPlaceModal();

  if (data.merged && typeof showToast === 'function') {
    const mergedName = (data.keyword && data.keyword.name) || 'existing keyword';
    showToast('Merged into existing "' + mergedName + '"', 'success');
  } else if (typeof showToast === 'function') {
    const linkedName = (data.keyword && data.keyword.name) || 'place';
    showToast('Linked to ' + linkedName, 'success');
  }

  // Re-fetch to pick up new place_id / coords / parent_id, and so the
  // unlinked-locations chip count updates.
  await loadKeywords();
}

// Per-row "Link..." button click handler. Bound on the body via delegation
// because rows are re-rendered by render().
function bindLinkPlaceControls() {
  document.getElementById('kwBody').addEventListener('click', function(e) {
    const btn = e.target.closest('.kw-link-btn');
    if (!btn) return;
    e.stopPropagation();
    const kwId = parseInt(btn.dataset.linkId, 10);
    if (!kwId) return;
    const kw = allKeywords.find(k => k.id === kwId);
    openLinkPlaceModal(kwId, kw ? kw.name : '');
  });

  // Modal: cancel button + click-on-overlay + Escape key.
  document.getElementById('linkPlaceCancel').addEventListener(
    'click', closeLinkPlaceModal
  );
  document.getElementById('linkPlaceModal').addEventListener('click', function(e) {
    // Click on the overlay (outside .modal) closes it.
    if (e.target === this) closeLinkPlaceModal();
  });
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      const modal = document.getElementById('linkPlaceModal');
      if (modal && modal.classList.contains('open')) closeLinkPlaceModal();
    }
  });
}

// Test hook: lets e2e tests trigger the API path without driving the
// Google Places UI (which Playwright can't reasonably exercise without a
// live network round-trip).
window.__linkPlaceForTest = submitLinkPlace;
