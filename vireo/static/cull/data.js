// Turning the pipeline's photos and encounters into the cull view: one
// species group per species (a photo showing two species is listed under
// each), one scene group per encounter, each photo's resolved decision, and
// the totals the summary shows (counted once per photo).
// Classic page script.

/**
 * An encounter's photos in burst order, then any outside a burst in
 * encounter order.
 *
 * @param {CullEncounter} enc @param {number[]} encIds
 */
function cullBurstOrder(enc, encIds) {
  /** @type {number[]} */
  var burstOrder = [];
  (enc.bursts || []).forEach(function(burst) {
    (/** @type {{photo_ids?: number[]}} */ (burst).photo_ids || /** @type {number[]} */ (burst) || []).forEach(function(pid) {
      if (encIds.indexOf(pid) >= 0 && burstOrder.indexOf(pid) < 0) burstOrder.push(pid);
    });
  });
  encIds.forEach(function(pid) {
    if (burstOrder.indexOf(pid) < 0) burstOrder.push(pid);
  });
  return burstOrder;
}

/**
 * One photo card of a scene group.
 *
 * @param {CullSourcePhoto} p @param {CullEncounter} enc @param {number} ei
 * @param {{action: CullAction, source: string, suggested: CullAction}} decision
 * @returns {CullDisplayPhoto}
 */
function cullGroupPhoto(p, enc, ei, decision) {
  return /** @type {CullDisplayPhoto} */ ({
    photo_id: p.id,
    quality: p.quality_composite || 0,
    quality_score: p.quality_composite != null ? p.quality_composite : null,
    sharpness: p.subject_sharpness != null ? p.subject_sharpness : null,
    filename: p.filename || '',
    timestamp: p.timestamp || null,
    day: cullPhotoDay(p),
    burst_id: cullBurstId(enc, p.id, ei),
    action: decision.action,
    decision_source: decision.source,
    suggested_action: decision.suggested,
    pipeline_label: p.label || 'REVIEW',
  });
}

/**
 * Add one encounter's photos of one species as a scene group, updating the
 * species' counts.
 *
 * @param {CullSpeciesGroup} sg @param {CullEncounter} enc @param {number} ei
 * @param {CullDisplayPhoto[]} groupPhotos
 */
function addCullSceneGroup(sg, enc, ei, groupPhotos) {
  sg.photo_count += groupPhotos.length;
  sg.keepers += groupPhotos.filter(function(p) { return p.action === 'keep'; }).length;
  sg.reviews += groupPhotos.filter(function(p) { return p.action === 'review'; }).length;
  sg.rejects += groupPhotos.filter(function(p) { return p.action === 'reject'; }).length;
  sg.scene_groups.push({
    scene_id: ei,
    label: encounterLabel(enc, ei, groupPhotos.length),
    photos: groupPhotos,
  });
}

/**
 * Page-wide tallies, one per photo: a photo listed under two species counts
 * once here, though each species' own counts include it.
 *
 * @param {CullSpeciesGroup[]} speciesGroups
 */
function cullPhotoTallies(speciesGroups) {
  /** @type {Record<number, CullAction>} */
  var actions = Object.create(null);
  /** @type {Record<number, number>} */
  var listings = Object.create(null);
  speciesGroups.forEach(function(sg) {
    sg.scene_groups.forEach(function(group) {
      group.photos.forEach(function(p) {
        listings[p.photo_id] = (listings[p.photo_id] || 0) + 1;
        if (actions[p.photo_id] !== 'keep') actions[p.photo_id] = p.action;
      });
    });
  });
  var tally = {keep: 0, review: 0, reject: 0, multi: 0};
  Object.keys(actions).forEach(function(id) {
    tally[/** @type {CullAction} */ (actions[Number(id)])]++;
    if ((listings[Number(id)] || 0) > 1) tally.multi++;
  });
  return tally;
}

/**
 * Group one encounter's photos by species. A photo showing two species is
 * listed under each (that the species filter lets through).
 *
 * @param {CullEncounter} enc @param {number[]} encIds @param {Record<number, CullSourcePhoto>} photoMap
 * @param {Record<number, string[]>} speciesById
 * @returns {Record<string, number[]>}
 */
function cullEncounterBySpecies(enc, encIds, photoMap, speciesById) {
  /** @type {Record<string, number[]>} */
  var bySpecies = Object.create(null);
  encIds.forEach(function(pid) {
    var p = /** @type {CullSourcePhoto} */ (photoMap[pid]);
    var species = speciesById[pid] = cullPhotoSpecies(p, enc, photoMap);
    species.forEach(function(name) {
      if (!cullPhotoMatches(p, [name])) return;
      (bySpecies[name] || (bySpecies[name] = [])).push(pid);
    });
  });
  return bySpecies;
}

/** @param {CullSourceResults | null} results @returns {CullData | null} */
function pipelineToCullData(results) {
  if (!results || !results.photos || !results.encounters) return null;
  /** @type {Record<number, CullSourcePhoto>} */
  var photoMap = {};
  results.photos.forEach(function(p) { photoMap[p.id] = p; });

  /** @type {Record<string, CullSpeciesGroup>} */
  var speciesMap = Object.create(null);
  var counts = {total: 0, pinned: 0, confirmed: 0, saved: 0};
  var counted = new Set();
  /** @type {Record<number, string[]>} */
  var speciesById = {};

  results.encounters.forEach(function(enc, ei) {
    var encIds = (enc.photo_ids || []).filter(function(pid) {
      return !!photoMap[pid];
    });
    if (!encIds.length) return;

    var bySpecies = cullEncounterBySpecies(enc, encIds, photoMap, speciesById);
    Object.keys(bySpecies).forEach(function(species) {
      var sg = speciesMap[species] || (speciesMap[species] = /** @type {CullSpeciesGroup} */ ({
        species: species,
        photo_count: 0,
        keepers: 0,
        reviews: 0,
        rejects: 0,
        scene_groups: [],
      }));
      var groupPhotos = cullBurstOrder(enc, bySpecies[species] || []).map(function(pid) {
        var p = /** @type {CullSourcePhoto} */ (photoMap[pid]);
        var decision = resolveCullDecision(p);
        if (!counted.has(pid)) {
          counted.add(pid);
          if (decision.source === 'manual') counts.pinned++;
          else if (decision.source === 'confirmed') counts.confirmed++;
          if (actionForFlag(p.flag)) counts.saved++;
          counts.total++;
        }
        var shown = cullGroupPhoto(p, enc, ei, decision);
        var others = (speciesById[pid] || []).filter(function(name) { return name !== species; });
        if (others.length) shown.also_species = others;
        return shown;
      });
      addCullSceneGroup(sg, enc, ei, groupPhotos);
    });
  });

  var speciesGroups = Object.keys(speciesMap).map(function(k) { return /** @type {CullSpeciesGroup} */ (speciesMap[k]); });
  speciesGroups.forEach(assignCullFavorites);
  shareCullSuggestedFavorites(speciesGroups);
  var tally = cullPhotoTallies(speciesGroups);
  speciesGroups.sort(function(a, b) { return b.photo_count - a.photo_count || a.species.localeCompare(b.species); });
  return /** @type {CullData} */ ({
    species_groups: speciesGroups,
    total_photos: counts.total,
    suggested_keepers: tally.keep,
    suggested_reviews: tally.review,
    suggested_rejects: tally.reject,
    multi_species_photos: tally.multi,
    pinned_count: counts.pinned,
    confirmed_count: counts.confirmed,
    saved_flag_count: counts.saved,
    // Pins on photos outside the current scope stay in cullManualDecisions
    // but aren't on screen; count them separately so "Clear N pinned" can
    // say what it will actually drop.
    pinned_offscreen: Math.max(0, manualDecisionCount() - counts.pinned),
    pipeline_summary: results.summary || {},
  });
}
