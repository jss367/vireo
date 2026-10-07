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
    var stored = JSON.parse(sessionStorage.getItem('vireoMapSelection') || 'null');
    if (stored && Array.isArray(stored.photo_ids) && stored.photo_ids.length) {
      return stored.photo_ids;
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
function fetchMapSelection(rules, visual) {
  return safeFetch('/api/photos/geo', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({photo_ids: mapSelectionIds, rules: rules, visual: visual}),
  });
}
