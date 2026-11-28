// Per-photo decisions: species names (every species a photo counts under),
// how a flag or label maps to an action, pinned counts, resolving a photo's
// decision (your pin, then the applied flag), and encounter labels.
// Classic page script.

/** @param {string} name @param {CullIdentityIndex} index @param {unknown} [sourceKey] @returns {CullSpecies} */
function cullSpeciesIdentity(name, index, sourceKey) {
  var known = Object.hasOwn(index, name) ? index[name] : undefined;
  var key = typeof sourceKey === 'string' && sourceKey || known?.key;
  // Legacy labels use the same whitespace and ASCII folding as keyword_match_key.
  return {
    key: key || 'name:' + name.trim().replace(/\s+/g, ' ').replace(/[A-Z]/g, function(ch) { return ch.toLowerCase(); }),
    name: known && known.key === key ? known.display_name : name,
  };
}

/** @param {Partial<CullSourcePhoto> | Partial<CullEncounter>} item @param {CullIdentityIndex} index */
function cullConfirmedSpecies(item, index) {
  var names = item.confirmed_species_list?.length ? item.confirmed_species_list : item.confirmed_species;
  var identities = 'confirmed_species_identities' in item ? item.confirmed_species_identities || [] : [];
  return (Array.isArray(names) ? names : names ? [names] : []).filter(Boolean).flatMap(function(name) {
    var matches = identities.filter(function(entry) { return entry.name === name; });
    return matches.length ? matches.map(function(entry) { return cullSpeciesIdentity(name, index, entry.key); })
      : [cullSpeciesIdentity(name, index)];
  });
}

/** @param {CullPrediction} prediction @param {CullIdentityIndex} index */
function cullPredictionSpecies(prediction, index) {
  return cullSpeciesIdentity(prediction[0], index, prediction[3]);
}

/** @param {CullSubject} subject */
function subjectTopPrediction(subject) {
  /** @type {CullPrediction | undefined} */
  var best;
  (subject.predictions || []).forEach(function(prediction) {
    if (prediction && prediction[0] && (!best || prediction[1] > best[1])) best = prediction;
  });
  return best;
}

/** @param {CullSourcePhoto} photo @param {CullIdentityIndex} index @returns {CullSpecies[]} */
function speciesListForPhoto(photo, index = {}) {
  var confirmed = cullConfirmedSpecies(photo, index);
  if (confirmed.length) return confirmed;
  var subjects = (photo.subjects || []).flatMap(function(subject) {
    var best = subjectTopPrediction(subject);
    return best ? [cullPredictionSpecies(best, index)] : [];
  });
  var prediction = photo.species_top5?.[0];
  if (!prediction?.[0]) return subjects;
  var top = cullPredictionSpecies(prediction, index);
  return !subjects.length || subjects.some(function(entry) { return entry.key === top.key; })
    ? [top, ...subjects] : subjects;
}

/** @param {Partial<CullEncounter>} enc @param {Record<number, CullSourcePhoto>} photoMap @param {CullIdentityIndex} index */
function speciesListForEncounter(enc, photoMap, index) {
  var confirmed = cullConfirmedSpecies(enc, index);
  if (confirmed.length) return confirmed;
  var vote = enc.species_predictions?.[0];
  var label = enc.species || vote?.species;
  var name = Array.isArray(label) ? label[0] : label;
  if (name) {
    var match = enc.species_predictions?.find(function(v) { return v.species === name; });
    return [cullSpeciesIdentity(name, index, match?.species_key)];
  }
  var first = enc.photo_ids?.find(function(id) { return !!photoMap[id]; });
  var prediction = first === undefined ? undefined : photoMap[first]?.species_top5?.[0];
  return [prediction ? cullPredictionSpecies(prediction, index) : cullSpeciesIdentity('Unclassified', index)];
}

/** @param {string | undefined} label @returns {CullAction} */
function actionForLabel(label) {
  if (label === 'KEEP') return 'keep';
  if (label === 'REJECT') return 'reject';
  return 'review';
}

/** @param {string | undefined} flag @returns {CullAction | null} */
function actionForFlag(flag) {
  if (flag === 'flagged' || flag === 'picked') return 'keep';
  if (flag === 'rejected') return 'reject';
  return null;
}

function manualDecisionCount() {
  return Object.keys(cullManualDecisions).length;
}

function unappliedPinCount() {
  return Object.keys(cullManualDecisions).filter(function(pid) {
    return cullAppliedDecisions[pid] !== cullManualDecisions[pid];
  }).length;
}

// Resolve one photo's decision: your hand edit wins, then the flag you
// already applied. Other photos stay undecided until chosen as a favorite
// or explicitly rejected. Returns the action plus where
// it came from so the card can show it.
/** @param {CullSourcePhoto} photo @returns {CullDecision} */
function resolveCullDecision(photo) {
  // A cached KEEP can outlive the manual flag that produced it. Only the
  // server's current eligibility assessment can admit an automatic favorite.
  var suggested = photo.label === 'KEEP' && photo.automatic_keep_eligible !== true
    ? 'review' : actionForLabel(photo.label);
  var manual = cullManualDecisions[photo.id];
  if (manual) return {action: manual, source: 'manual', suggested: suggested};
  // A confirmed clear is an explicit Review; a bare none is still undecided.
  var confirmed = photo.flag_confirmation_status === 'confirmed'
    ? (photo.flag === 'none' ? 'review' : actionForFlag(photo.flag)) : null;
  if (confirmed) {
    return {action: confirmed, source: 'confirmed', suggested: suggested};
  }
  return {action: 'review', source: 'auto', suggested: suggested};
}

/** @param {number} count @param {string} noun */
function cullCount(count, noun) {
  return count + ' ' + noun + (count === 1 ? '' : 's');
}

/** @param {CullEncounter} enc @param {number} idx @param {number} count */
function encounterLabel(enc, idx, count) {
  var label = 'Encounter ' + (idx + 1) + ' (' + cullCount(count, 'photo') + ')';
  if (enc.time_range && enc.time_range[0]) {
    var day = enc.time_range[0].substring(0, 10);
    var start = window.VireoTime.captureTime(enc.time_range[0]);
    var end = enc.time_range[1] ? window.VireoTime.captureTime(enc.time_range[1]) : '';
    label = day + ' · Encounter ' + (idx + 1) + ' - ' + start + (end ? ' to ' + end : '') + ' (' + cullCount(count, 'photo') + ')';
  }
  if (enc.burst_count) label += ' / ' + cullCount(enc.burst_count, 'burst');
  return label;
}
