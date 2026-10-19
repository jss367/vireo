// Turning the pipeline's photos and encounters into the cull view: one
// species group per species, one scene group per encounter, each photo's
// resolved decision, and the totals the summary shows.
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

/** @param {CullSourceResults | null} results @returns {CullData | null} */
function pipelineToCullData(results) {
  if (!results || !results.photos || !results.encounters) return null;
  /** @type {Record<number, CullSourcePhoto>} */
  var photoMap = {};
  results.photos.forEach(function(p) { photoMap[p.id] = p; });

  /** @type {Record<string, CullSpeciesGroup>} */
  var speciesMap = Object.create(null);
  var counts = {total: 0, reject: 0, pinned: 0, confirmed: 0, saved: 0};

  results.encounters.forEach(function(enc, ei) {
    var encIds = (enc.photo_ids || []).filter(function(pid) {
      return !!photoMap[pid];
    });
    if (!encIds.length) return;

    /** @type {Record<string, number[]>} */
    var bySpecies = Object.create(null);
    encIds.forEach(function(pid) {
      var p = /** @type {CullSourcePhoto} */ (photoMap[pid]);
      var species = cullPhotoSpecies(p, enc, photoMap);
      if (!cullPhotoMatches(p, species)) return;
      (bySpecies[species] || (bySpecies[species] = [])).push(pid);
    });
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
        if (decision.source === 'manual') counts.pinned++;
        else if (decision.source === 'confirmed') counts.confirmed++;
        if (actionForFlag(p.flag)) counts.saved++;
        counts.total++;
        if (decision.action === 'reject') counts.reject++;
        return cullGroupPhoto(p, enc, ei, decision);
      });
      addCullSceneGroup(sg, enc, ei, groupPhotos);
    });
  });

  var speciesGroups = Object.keys(speciesMap).map(function(k) { return /** @type {CullSpeciesGroup} */ (speciesMap[k]); });
  speciesGroups.forEach(assignCullFavorites);
  var keep = speciesGroups.reduce(function(n, sg) { return n + sg.keepers; }, 0);
  var review = speciesGroups.reduce(function(n, sg) { return n + sg.reviews; }, 0);
  speciesGroups.sort(function(a, b) { return b.photo_count - a.photo_count || a.species.localeCompare(b.species); });
  return /** @type {CullData} */ ({
    species_groups: speciesGroups,
    total_photos: counts.total,
    suggested_keepers: keep,
    suggested_reviews: review,
    suggested_rejects: counts.reject,
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
