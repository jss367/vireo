// Per-photo decisions: species names (every species a photo counts under),
// how a flag or label maps to an action, pinned counts, resolving a photo's
// decision (your pin, then the applied flag), and encounter labels.
// Classic page script.

/** @param {CullSourcePhoto | null | undefined} photo */
function topSpeciesForPhoto(photo) {
  var top5 = photo && photo.species_top5;
  if (top5 && top5.length && top5[0] && top5[0][0]) return top5[0][0];
  return 'Unclassified';
}

/** @param {string | string[] | null | undefined} value */
function speciesName(value) {
  if (Array.isArray(value)) return value[0] || 'Unclassified';
  return value || 'Unclassified';
}

/**
 * The distinct non-empty names in a confirmed-species value, which the
 * cache stores as a list (``confirmed_species_list``) or, in older caches,
 * as one name or an array in ``confirmed_species``.
 *
 * @param {string | string[] | null | undefined} value @returns {string[]}
 */
function speciesNameList(value) {
  /** @type {string[]} */
  var names = [];
  (Array.isArray(value) ? value : [value]).forEach(function(name) {
    if (typeof name === 'string' && name && names.indexOf(name) < 0) names.push(name);
  });
  return names;
}

/** @param {CullSubject} subject @returns {string} */
function subjectTopSpecies(subject) {
  /** @type {[string, number, ...unknown[]] | null} */
  var best = null;
  (subject && subject.predictions || []).forEach(function(prediction) {
    if (prediction && prediction[0] && (!best || Number(prediction[1] || 0) > Number(best[1] || 0))) best = prediction;
  });
  return best ? best[0] : '';
}

/**
 * Every species a photo shows, from what the results carry: its confirmed
 * species (all of them), else each detected subject's top prediction, else
 * the photo's single top prediction. Empty when the photo has none of
 * these (the encounter then decides). A photo with two animals therefore
 * counts under both species.
 *
 * @param {CullSourcePhoto} photo @returns {string[]}
 */
function speciesListForPhoto(photo) {
  var confirmed = speciesNameList(photo.confirmed_species_list);
  if (!confirmed.length) confirmed = speciesNameList(photo.confirmed_species);
  if (confirmed.length) return confirmed;
  var top = photo.species_top5 && photo.species_top5.length ? topSpeciesForPhoto(photo) : '';
  var subjects = speciesNameList((photo.subjects || []).map(subjectTopSpecies));
  if (subjects.length) {
    // The photo's overall best guess leads, as it did before subjects.
    if (top && subjects.indexOf(top) > 0) subjects = [top].concat(subjects.filter(function(n) { return n !== top; }));
    return subjects;
  }
  return top ? [top] : [];
}

/**
 * The species an encounter stands for when one of its photos carries no
 * species of its own: every confirmed species, else its predicted label.
 *
 * @param {Partial<CullEncounter>} enc @param {Record<number, CullSourcePhoto>} photoMap @returns {string[]}
 */
function speciesListForEncounter(enc, photoMap) {
  var confirmed = speciesNameList(enc.confirmed_species_list);
  if (!confirmed.length) confirmed = speciesNameList(enc.confirmed_species);
  if (confirmed.length) return confirmed;
  if (enc.species) return [speciesName(enc.species)];
  var votes = enc.species_predictions || [];
  if (votes.length && votes[0] && votes[0].species) return [speciesName(votes[0].species)];
  var ids = enc.photo_ids || [];
  for (var i = 0; i < ids.length; i++) {
    var photo = photoMap[/** @type {number} */ (ids[i])];
    if (photo) return [topSpeciesForPhoto(photo)];
  }
  return ['Unclassified'];
}

/** @param {string | undefined} label @returns {CullAction} */
function actionForLabel(label) {
  if (label === 'KEEP') return 'keep';
  if (label === 'REJECT') return 'reject';
  return 'review';
}

/** @param {string | undefined} flag @returns {CullAction | null} */
function actionForFlag(flag) {
  if (flag === 'flagged') return 'keep';
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
  var suggested = actionForLabel(photo.label);
  var manual = cullManualDecisions[photo.id];
  if (manual) return {action: manual, source: 'manual', suggested: suggested};
  var confirmed = photo.flag_confirmation_status === 'confirmed' ? actionForFlag(photo.flag) : null;
  if (confirmed) {
    return {action: confirmed, source: 'confirmed', suggested: suggested};
  }
  return {action: 'review', source: 'auto', suggested: suggested};
}

/** @param {CullEncounter} enc @param {number} idx @param {number} count */
function encounterLabel(enc, idx, count) {
  var label = 'Encounter ' + (idx + 1) + ' (' + count + ' photos)';
  if (enc.time_range && enc.time_range[0]) {
    var day = enc.time_range[0].substring(0, 10);
    var start = window.VireoTime.captureTime(enc.time_range[0]);
    var end = enc.time_range[1] ? window.VireoTime.captureTime(enc.time_range[1]) : '';
    label = day + ' · Encounter ' + (idx + 1) + ' - ' + start + (end ? ' to ' + end : '') + ' (' + count + ' photos)';
  }
  if (enc.burst_count) label += ' / ' + enc.burst_count + ' bursts';
  return label;
}
