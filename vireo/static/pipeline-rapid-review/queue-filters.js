// The queue filters (needs species, needs culling, unreviewed, review
// picks), the sidebar reason chips, and rebuilding the filtered sessions.
// Classic page script; load boot.js after all definitions.

/** @param {RapidSession} session */
function hasMixedSpecies(session) {
  // Frames disagree when their confirmed species *sets* differ; a frame
  // holding both a teal and a wigeon is not "mixed" with an identical frame.
  var seen = /** @type {Record<string,boolean>} */ ({});
  queueIncludedPhotos(session).forEach(function(p) {
    var confirmed = /** @type {Record<string,string>} */ ({});
    (p.confirmed_species_identities || []).forEach(function(entry) { confirmed[entry.name] = entry.key; });
    var key = photoSpeciesList(p).map(function(name) {
      if (confirmed[name]) return confirmed[name];
      var identity = rapid.results && rapid.results.species_identities && rapid.results.species_identities[name];
      return identity ? identity.key : speciesKey(name);
    }).sort().join('|');
    if (key) seen[key] = true;
  });
  return Object.keys(seen).length > 1;
}

/** @param {RapidSession} session */
function needsSpecies(session) {
  var photos = queueIncludedPhotos(session);
  if (!photos.length) return false;
  if (rapid.queue.multiOnly && photos.length < 2) return false;
  return photos.some(function(p) {
    if (!rapid.queue.includeRejected && photoFlag(p) === 'rejected') return false;
    return !photoHasSpecies(p);
  });
}

/** @param {RapidSession} session */
function cullingState(session) {
  var photos = queueIncludedPhotos(session);
  var nonRejected = photos.filter(function(p) { return photoFlag(p) !== 'rejected'; });
  if (!photos.length) return { needs: false, reason: '' };
  if (rapid.queue.multiOnly && nonRejected.length < 2) return { needs: false, reason: '' };
  var flagged = nonRejected.filter(function(p) { return photoFlag(p) === 'flagged'; }).length;
  var neutral = nonRejected.filter(function(p) { return photoFlag(p) === 'none'; }).length;
  var rejected = photos.filter(function(p) { return photoFlag(p) === 'rejected'; }).length;
  if (nonRejected.length > 1 && flagged === 0) return { needs: true, reason: 'No keeper' };
  if (flagged > 1) return { needs: true, reason: 'Multiple keepers' };
  if (neutral > 0 && rejected > 0 && flagged === 0) return { needs: true, reason: 'No keeper' };
  return { needs: false, reason: '' };
}

/** @param {RapidSession} session */
function needsCulling(session) {
  return cullingState(session).needs;
}

/** @param {RapidSession} session */
function unreviewed(session) {
  var photos = queueIncludedPhotos(session);
  if (!photos.length) return false;
  if (rapid.queue.multiOnly && photos.length < 2) return false;
  return photos.some(function(p) {
    return photoFlag(p) === 'none' && !photoHasSpecies(p);
  });
}

/** @param {RapidSession} session */
function reviewPicks(session) {
  var photos = queueIncludedPhotos(session);
  if (!photos.length) return false;
  if (rapid.queue.multiOnly && photos.length < 2) return false;
  return photos.some(function(p) {
    return photoFlag(p) === 'flagged' || photoHasSpecies(p);
  });
}

/** @param {RapidSession} session */
function sessionMatchesQueue(session) {
  if (rapid.queue.multiOnly && queueIncludedPhotos(session).length < 2) return false;
  if (rapid.queue.filter === 'all') return true;
  if (rapid.queue.filter === 'needs-species') return needsSpecies(session);
  if (rapid.queue.filter === 'needs-culling') return needsCulling(session);
  if (rapid.queue.filter === 'unreviewed') return unreviewed(session);
  if (rapid.queue.filter === 'review-picks') return reviewPicks(session);
  return true;
}

/** @param {RapidSession} session */
function sessionReasonChips(session) {
  var chips = [];
  if (needsSpecies(session)) chips.push('No species');
  if (hasMixedSpecies(session)) chips.push('Mixed species');
  var cull = cullingState(session);
  if (cull.needs && cull.reason) chips.push(cull.reason);
  if (reviewPicks(session)) chips.push('Already tagged');
  if (sessionPhotos(session).some(function(p) { return photoFlag(p) === 'rejected'; })) chips.push('Rejected');
  if (!chips.length) chips.push('Complete');
  return chips;
}

function rebuildFilteredSessions() {
  rapid.sessions = rapid.allSessions.filter(sessionMatchesQueue);
}
