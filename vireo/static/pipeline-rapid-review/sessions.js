// Burst sessions built from the pipeline results, and the per-photo flag
// and confirmed-species readers the queue filters use.
// Classic page script; load boot.js after all definitions.

function buildSessions(results) {
  var sessions = [];
  (results.encounters || []).forEach(function(enc, encIdx) {
    var bursts = enc.bursts && enc.bursts.length ? enc.bursts : [{ photo_ids: enc.photo_ids || [] }];
    bursts.forEach(function(burst, burstIdx) {
      var ids = burstIds(burst).filter(function(id) { return rapid.photoMap[id]; });
      if (!ids.length) return;
      sessions.push({
        rawIndex: sessions.length,
        encIdx: encIdx,
        burstIdx: burstIdx,
        ids: ids,
        species: burstSpecies(enc, burst),
        speciesList: burstSpeciesList(enc, burst),
        hasBurstOverride: hasBurstSpeciesOverride(burst),
        title: 'Encounter ' + (encIdx + 1) + ', Burst ' + (burstIdx + 1),
        subtitle: ids.length + ' photos' + (burstSpecies(enc, burst) ? ' - ' + burstSpecies(enc, burst) : ''),
      });
    });
  });
  return sessions;
}

function sessionKey(session) {
  return session ? (session.encIdx + ':' + session.burstIdx) : '';
}

function photoFlag(p) {
  return (p && p.flag) || 'none';
}

function photoSpeciesList(p) {
  if (!p) return [];
  if (Array.isArray(p.confirmed_species_list)) return p.confirmed_species_list.filter(Boolean);
  return p.confirmed_species ? [p.confirmed_species] : [];
}

function photoSpecies(p) {
  return joinSpecies(photoSpeciesList(p));
}

function photoHasSpecies(p) {
  return photoSpeciesList(p).length > 0;
}

function sessionPhotos(session) {
  return (session ? session.ids : []).map(function(id) { return rapid.photoMap[id]; }).filter(Boolean);
}

function queueIncludedPhotos(session) {
  var photos = sessionPhotos(session);
  if (!rapid.queue.includeRejected) {
    photos = photos.filter(function(p) { return photoFlag(p) !== 'rejected'; });
  }
  return photos;
}
