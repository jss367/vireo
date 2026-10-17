// Per-frame species keyword state: the seeded /group/state snapshot and the
// debounced refetch as the species field changes.
// Classic page script; load boot.js after all definitions.

var _rapidSpeciesRefetchTimer = null;
var _rapidSpeciesRefetchToken = 0;

function rapidOnSpeciesInput() {
  renderApplyState();
  if (_rapidSpeciesRefetchTimer) clearTimeout(_rapidSpeciesRefetchTimer);
  _rapidSpeciesRefetchTimer = setTimeout(rapidRefreshSpeciesKeywordState, 250);
}

// Per-frame keyword presence for each species in `speciesList`, keyed by
// speciesKey. Prefers the per-name map from /group/state; falls back to the
// legacy single has_species_keyword flag for every name.
function rapidKeywordStateFor(st, speciesList) {
  var map = {};
  var perName = (st && st.has_species_keywords) || null;
  speciesList.forEach(function(name) {
    if (perName) {
      // A name the server did not report on is simply not attached.
      map[speciesKey(name)] = !!perName[name];
    } else {
      map[speciesKey(name)] = !!(st && st.has_species_keyword);
    }
  });
  return map;
}

// True when the seeded keyword snapshot covers `targetList` and says frame
// `pid` already carries `name`. A snapshot fetched for a different list is
// treated as "missing" so the Apply label stays a conservative overestimate.
function rapidFrameHasKeyword(pid, name, targetList) {
  if (rapid.keywordStateSpecies !== joinSpecies(targetList)) return false;
  var st = rapid.initialSpeciesKeywords[pid];
  return !!(st && st[speciesKey(name)]);
}

function rapidRefreshSpeciesKeywordState() {
  if (!rapid.items || !rapid.items.length || rapid.sessionIndex < 0) return;
  var speciesEl = document.getElementById('speciesInput');
  var speciesList = parseSpeciesList(speciesEl ? speciesEl.value : '');
  var token = ++_rapidSpeciesRefetchToken;
  var openToken = rapid.openToken;
  var sessionIndex = rapid.sessionIndex;
  var photoIds = rapid.items.map(function(p) { return p.id; });

  safeFetch('/api/pipeline/group/state', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      photo_ids: photoIds,
      species: speciesList[0] || '',
      species_list: speciesList,
    }),
  }, { toast: false }).then(function(data) {
    if (token !== _rapidSpeciesRefetchToken) return;
    if (rapid.openToken !== openToken || rapid.sessionIndex !== sessionIndex) return;
    var fresh = (data && data.photos) || {};
    photoIds.forEach(function(pid) {
      var row = fresh[pid] || fresh[String(pid)] || {};
      rapid.initialSpeciesKeywords[pid] = rapidKeywordStateFor(row, speciesList);
    });
    rapid.keywordStateSpecies = joinSpecies(speciesList);
    renderApplyState();
  }).catch(function() {
    // Keep the previous keyword snapshot. If the typed species differs from
    // that snapshot, the Apply label remains a conservative tag overestimate.
  });
}
