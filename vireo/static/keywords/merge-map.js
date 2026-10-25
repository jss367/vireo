// The merge dialog's map: lazy Leaflet loading and plotting every candidate
// point plus the kept one.

import { state } from './state.js';
import { mergeDialog, mergeMapEl, mergeMapNote } from './merge-state.js';
import { escapeHtml, leaflet } from './shared.js';

/** @type {Promise<KeywordLeaflet> | null} */ let leafletPromise = null;

/** @returns {Promise<KeywordLeaflet>} */ export function loadLeaflet() {
  if (leafletPromise) return leafletPromise;
  leafletPromise = new Promise(function(resolve, reject) {
    const loaded = leaflet();
    if (loaded) { resolve(loaded); return; }
    const link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css';
    document.head.appendChild(link);
    const script = document.createElement('script');
    script.src = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js';
    script.async = true;
    script.addEventListener('load', function() {
      const loadedNow = leaflet();
      if (loadedNow) resolve(loadedNow); else reject(new Error('Could not load the map.'));
    });
    script.addEventListener('error', function() { reject(new Error('Could not load the map.')); });
    document.head.appendChild(script);
  });
  return leafletPromise;
}

/* Plot every selected keyword that has a point, plus the point the merge
   will keep, so "which of these is the right place" is answerable without
   leaving the dialog. */
/** @param {KeywordMergePreview} data */ export async function renderMergeMap(data) {
  /** @type {Array<{latitude:number;longitude:number;label:string;kept:boolean}>} */ const points = [];
  [data.target, ...data.sources].forEach(record => {
    if (record.latitude != null && record.longitude != null) {
      points.push({
        latitude: record.latitude, longitude: record.longitude,
        label: record.path.join(' → '), kept: false
      });
    }
  });
  const resolved = data.resolved;
  if (resolved.latitude != null && resolved.longitude != null) {
    points.push({
      latitude: resolved.latitude, longitude: resolved.longitude,
      label: 'Combined keyword: ' + resolved.path.join(' → '), kept: true
    });
  }
  if (!points.length) {
    mergeMapEl.hidden = true;
    mergeMapNote.hidden = false;
    mergeMapNote.textContent =
      'None of the selected keywords has coordinates, so there is nothing to map.';
    return;
  }
  mergeMapEl.hidden = false;
  let L;
  try {
    L = await loadLeaflet();
  } catch (error) {
    mergeMapEl.hidden = true;
    mergeMapNote.hidden = false;
    mergeMapNote.textContent =
      'Map unavailable offline — the coordinate links above open OpenStreetMap.';
    return;
  }
  if (!mergeDialog.open) return;
  mergeMapNote.hidden = true;
  if (!state.mergeMap) {
    state.mergeMap = L.map(mergeMapEl, {attributionControl: true});
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors', maxZoom: 19
    }).addTo(state.mergeMap);
  }
  if (state.mergeMapLayer) state.mergeMapLayer.remove();
  state.mergeMapLayer = L.layerGroup(points.map(point => {
    const marker = point.kept
      ? L.marker([point.latitude, point.longitude], {title: point.label, zIndexOffset: 1000})
      : L.circleMarker([point.latitude, point.longitude], {
          radius: 7, color: '#888', weight: 2, fillOpacity: 0.35
        });
    // Leaflet inserts a string tooltip via innerHTML, so a keyword path
    // carrying markup from imported XMP would run in the app's origin.
    return marker.bindTooltip(escapeHtml(point.label));
  })).addTo(state.mergeMap);
  state.mergeMap.invalidateSize();
  state.mergeMap.fitBounds(L.latLngBounds(points.map(p => [p.latitude, p.longitude])),
                     {padding: [28, 28], maxZoom: 14});
}
