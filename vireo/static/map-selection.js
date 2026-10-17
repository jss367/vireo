// Selection mode for the Map page. Browse's View on Map on several photos
// stores their ids in sessionStorage and opens /map?source=selection; the map
// then plots only those photos (still narrowed by the filter bar) and says why
// each selected photo it leaves off is missing.
// Classic script: load before map.html's inline script, which calls these.
'use strict';

var mapSelectionIds = (function() {
  if (new URLSearchParams(window.location.search).get('source') !== 'selection') {
    return null;
  }
  try {
    /** @type {unknown} */
    var stored = JSON.parse(sessionStorage.getItem('vireoMapSelection') || 'null');
    if (stored && typeof stored === 'object' && 'photo_ids' in stored && Array.isArray(stored.photo_ids)) {
      var ids = stored.photo_ids.filter(function(id) {
        return typeof id === 'number' && Number.isSafeInteger(id) && id > 0;
      });
      if (ids.length) return /** @type {number[]} */ (ids);
    }
  } catch (e) {}
  // Opened in a new tab, or the tab's session storage was cleared: there is
  // no selection to show, so say so rather than silently mapping everything.
  if (typeof showToast === 'function') {
    showToast('The selection to show on the map is no longer available, so the map shows all photos.', 'error');
  }
  return null;
})();

function mapSelectionScopeLabel() {
  if (!mapSelectionIds) return null;
  return 'Map \u00b7 ' + mapSelectionIds.length.toLocaleString() + ' selected photos';
}

// The selection travels in a POST body: a large one would not fit in a URL.
/** @param {unknown} rules @param {unknown} visual @returns {Promise<MapGeoResponse>} */
function fetchMapSelection(rules, visual) {
  return /** @type {Promise<MapGeoResponse>} */ (safeFetch('/api/photos/geo', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({photo_ids: mapSelectionIds, rules: rules, visual: visual}),
  }));
}
