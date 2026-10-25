// The link-to-Google-place flow: the Maps loader and config fetch, the modal,
// place normalization, submitLinkPlace, the per-row Link button, and the
// modal's close controls.

import { state } from './state.js';
import { loadKeywords } from './loading.js';
import { keywordErrorMessage } from './merge-helpers.js';
import { googleMaps, setGoogleMapsCallback, showToast } from './shared.js';

// -- Link-to-Google-place flow ---------------------------------------------

// Cached promise for the Google Maps JS loader (memoized).
/** @returns {Promise<KeywordGoogle|undefined>} */ export function loadGoogleMapsJs() {
  if (state.gmapsLoadPromise) return state.gmapsLoadPromise;
  const apiKey = state.googleMapsApiKey;
  if (!apiKey) {
    state.gmapsLoadPromise = Promise.reject(new Error('no_api_key'));
    return state.gmapsLoadPromise;
  }
  state.gmapsLoadPromise = new Promise(function(resolve, reject) {
    setGoogleMapsCallback(function() { resolve(googleMaps()); });
    const s = document.createElement('script');
    s.src = 'https://maps.googleapis.com/maps/api/js?key=' +
      encodeURIComponent(apiKey) +
      '&libraries=places' +
      (state.googleMapsPreferEnglish ? '&language=en' : '') +
      '&loading=async&callback=_gmapsCallback';
    s.async = true;
    s.addEventListener('error', function() { reject(new Error('gmaps_load_failed')); });
    document.head.appendChild(s);
  });
  return state.gmapsLoadPromise;
}

// Fetch /api/config once on page load to populate state.googleMapsApiKey.
// Mirrors browse.html's _cfgPromise pattern. The modal awaits this promise
// before deciding whether to bind autocomplete vs. show the empty-key error.
export const _cfgPromise = (async function() {
  try {
    const r = await fetch('/api/config');
    /** @type {{google_maps_api_key?:string;google_maps_prefer_english?:boolean}} */ const cfg = await r.json();
    state.googleMapsApiKey = cfg.google_maps_api_key || '';
    state.googleMapsPreferEnglish = cfg.google_maps_prefer_english !== false;
    return cfg;
  } catch(e) {
    state.googleMapsApiKey = '';
    state.googleMapsPreferEnglish = true;
    return null;
  }
})();

/** @param {string} msg */ export function _showLinkError(msg) {
  const el = /** @type {HTMLElement} */ (document.getElementById('linkPlaceError'));
  if (!el) return;
  el.textContent = msg;
  el.style.display = 'block';
}

export function _hideLinkError() {
  const el = /** @type {HTMLElement} */ (document.getElementById('linkPlaceError'));
  if (el) el.style.display = 'none';
}

/** @param {number} keywordId @param {string} currentName */ export async function openLinkPlaceModal(keywordId, currentName) {
  const modal = /** @type {HTMLElement} */ (document.getElementById('linkPlaceModal'));
  const input = /** @type {HTMLInputElement} */ (document.getElementById('linkPlaceInput'));
  if (!modal || !input) return;

  _hideLinkError();
  state.activeLinkKeywordId = keywordId;
  input.value = currentName || '';
  modal.classList.add('open');
  // Defer focus so the modal is laid out first.
  setTimeout(function() { input.focus(); input.select(); }, 0);

  // Wait for /api/config so state.googleMapsApiKey is populated even
  // on a slow first paint.
  try { await _cfgPromise; } catch(e) {}

  if (!state.googleMapsApiKey) {
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
    state.activeLinkAutocomplete = new google.maps.places.Autocomplete(input, {
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
    state.activeLinkAutocomplete.addListener('place_changed', function() {
      if (!state.activeLinkAutocomplete) return; const place = state.activeLinkAutocomplete.getPlace();
      if (place && place.place_id && state.activeLinkKeywordId) {
        submitLinkPlace(
          state.activeLinkKeywordId,
          place.place_id,
          normalizeGooglePlaceForSubmit(place)
        );
      }
    });
  } catch(err) {
    _showLinkError('Failed to load Google Maps: ' + keywordErrorMessage(err));
  }
}

export function closeLinkPlaceModal() {
  const modal = /** @type {HTMLElement} */ (document.getElementById('linkPlaceModal'));
  if (modal) modal.classList.remove('open');
  state.activeLinkKeywordId = null;
  state.activeLinkAutocomplete = null;
  _hideLinkError();
}

/** @param {KeywordGooglePlace|null|undefined} place @returns {KeywordPlaceSubmission|null} */ export function normalizeGooglePlaceForSubmit(place) {
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

/** @param {number} keywordId @param {string} placeId @param {KeywordPlaceSubmission|null} [placeDetails] */ export async function submitLinkPlace(keywordId, placeId, placeDetails) {
  _hideLinkError();
  /** @type {{place_id:string;place?:KeywordPlaceSubmission}} */ const body = {place_id: placeId};
  if (placeDetails) body.place = placeDetails;
  let resp;
  try {
    resp = await fetch('/api/keywords/' + keywordId + '/link-place', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
  } catch(err) {
    _showLinkError('Network error: ' + keywordErrorMessage(err));
    return;
  }
  if (!resp.ok) {
    /** @type {KeywordMutation} */ let body = {};
    try { body = await resp.json(); } catch(e) {}
    _showLinkError(body.message || body.error || ('Failed (HTTP ' + resp.status + ')'));
    return;
  }
  /** @type {KeywordMutation} */ let data = {};
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
export function bindLinkPlaceControls() {
  /** @type {HTMLElement} */ (document.getElementById('kwBody')).addEventListener('click', function(e) {
    if (!(e.target instanceof Element)) return; const btn = e.target.closest(/** @type {'button'} */ ('.kw-link-btn'));
    if (!btn) return;
    e.stopPropagation();
    const kwId = parseInt(btn.dataset.linkId || '', 10);
    if (!kwId) return;
    const kw = state.allKeywords.find(k => k.id === kwId);
    openLinkPlaceModal(kwId, kw ? kw.name : '');
  });

  // Modal: cancel button + click-on-overlay + Escape key.
  /** @type {HTMLButtonElement} */ (document.getElementById('linkPlaceCancel')).addEventListener(
    'click', closeLinkPlaceModal
  );
  /** @type {HTMLElement} */ (document.getElementById('linkPlaceModal')).addEventListener('click', function(e) {
    // Click on the overlay (outside .modal) closes it.
    if (e.target === this) closeLinkPlaceModal();
  });
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      const modal = /** @type {HTMLElement} */ (document.getElementById('linkPlaceModal'));
      if (modal && modal.classList.contains('open')) closeLinkPlaceModal();
    }
  });
}
