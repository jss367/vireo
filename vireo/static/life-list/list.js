// Life List page state, view preferences, escaping, and loading /api/life-list.
// Classic page script; load boot.js after all definitions.

var currentData = null;
var lifeListLoadSeq = 0;

function restoreLifeListViewPreferences() {
  VireoViewPreferences.restoreAll(document.getElementById('controlsBar'));
}

// All dynamic values rendered into card HTML pass through the shared
// escapeAttr (vireo-utils.js) — same escaping convention as highlights.html.

function formatDate(iso) {
  if (!iso) return null;
  var d = new Date(iso);
  if (isNaN(d.getTime())) return null;
  return d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
}

async function loadLifeList() {
  var meta = document.getElementById('meta');
  var loadSeq = ++lifeListLoadSeq;
  meta.textContent = 'Loading life list…';
  var data;
  try {
    data = await safeFetch('/api/life-list');
  } catch (e) {
    if (loadSeq !== lifeListLoadSeq) return;
    meta.textContent = 'Failed to load life list.';
    return;
  }
  if (loadSeq !== lifeListLoadSeq) return;
  currentData = data;
  populateLifeListTaxonomyFilters();
  render();
}
