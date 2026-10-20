// Species predictions, consensus, and photo metadata presentation.
// Classic page script; shared globals are initialized before boot.js runs.

// Render a single species-prediction block (title + rows). Each row in `rows`
// is {species, models: [{model, confidence}]}. Returns '' when there are no
// rows so we never emit an empty header.
/** @param {string} title @param {ReviewSpeciesRow[]} rows */
function renderSpeciesBlock(title, rows) {
  if (!rows || rows.length === 0) return '';
  var html = '<div class="inspect-species-predictions">';
  html += '<div style="font-size:13px;font-weight:600;margin-bottom:6px;">' + escapeHtml(title) + '</div>';
  rows.forEach(function(sp) {
    var modelParts = (sp.models || []).map(function(m) {
      var pct = (m.confidence * 100).toFixed(0) + '%';
      // Some aggregate rows are confidence-only (no model name); show just the
      // percentage rather than an empty label with a leading space.
      var part = m.model ? escapeHtml(m.model) + ' ' + pct : pct;
      // Consensus rows carry photo_count — the number of frames where this
      // model predicted this species. Surface it so a minority species
      // predicted on a single frame isn't read as a broad consensus
      // (CORE_PHILOSOPHY "No black boxes"). Per-photo rows have no photo_count
      // (each is one frame) and stay count-free.
      if (typeof m.photo_count === 'number') {
        part += ' (' + m.photo_count + ' frame' + (m.photo_count === 1 ? '' : 's') + ')';
      }
      return part;
    });
    html += '<div class="inspect-species-row">';
    html += '<span class="inspect-species-name">' + escapeHtml(sp.species) + '</span>';
    html += '<span class="inspect-species-conf">' + modelParts.join(', ') + '</span>';
    html += '</div>';
  });
  html += '</div>';
  return html;
}

// Group a single photo's raw species_top5 ([species, confidence, model] rows)
// into the same {species, models} shape the aggregate uses, dropping models
// below `threshold` and sorting by confidence descending.
/** @param {ReviewPrediction[]} top5 @param {number} [threshold] */
function buildSpeciesRows(top5, threshold) {
  top5 = top5 || [];
  if (top5.length === 0) return [];
  /** @type {Record<string,ReviewSpeciesModel[]>} */ var bySpecies = {};
  /** @type {string[]} */ var order = [];
  top5.forEach(function(entry) {
    var sp = entry[0];
    var conf = entry[1];
    var model = entry[2] || 'unknown';
    if (conf < requireReviewValue(threshold)) return;
    if (!bySpecies[sp]) { bySpecies[sp] = []; order.push(sp); }
    requireReviewValue(bySpecies[sp]).push({ model: model, confidence: conf });
  });
  var rows = order.map(function(sp) {
    var models = requireReviewValue(bySpecies[sp]).slice().sort(function(a, b) { return b.confidence - a.confidence; });
    return { species: sp, models: models };
  });
  rows.sort(function(a, b) { return requireReviewValue(requireReviewValue(b.models)[0]).confidence - requireReviewValue(requireReviewValue(a.models)[0]).confidence; });
  return rows;
}

/** @param {ReviewPhoto} photo @param {number} [threshold] */
function buildPerPhotoSpeciesRows(photo, threshold) {
  return buildSpeciesRows((photo && photo.species_top5) || [], threshold);
}

/** @param {ReviewPhoto} photo @param {number} [threshold] */
function buildMultiSubjectReviewHtml(photo, threshold) {
  var subjects = grmPhotoSubjects(photo);
  if (subjects.length < 2) return '';
  var selected = grmSelectedSubject(photo);
  if (!selected) return '';

  var html = '<div class="grm-subject-review" data-testid="multi-subject-review">';
  html += '<div class="grm-subject-review-heading">' + subjects.length + ' subjects detected</div>';
  html += '<div class="grm-subject-review-hint">Choose a subject to see the prediction tied to that part of the photo.</div>';
  html += '<div class="grm-subject-switcher">';
  subjects.forEach(function(subject, index) {
    var isSelected = String(subject.detection_id) === String(requireReviewValue(requireReviewValue(selected).detection_id));
    html += '<button type="button" class="grm-subject-chip' + (isSelected ? ' selected' : '') +
      '" data-testid="subject-chip" onclick="grmSelectSubject(' + photo.id + ', ' +
      Number(subject.detection_id) + ')">' + escapeHtml(grmSubjectLabel(subject, index)) + '</button>';
  });
  html += '</div>';

  var box = selected.box || {};
  var conf = selected.detection_confidence;
  var boxText = [box.x, box.y, box.w, box.h].every(function(value) { return value != null; })
    ? ' · box x ' + Math.round(Number(box.x) * 100) + '–' + Math.round((Number(box.x) + Number(box.w)) * 100) +
      '%, y ' + Math.round(Number(box.y) * 100) + '–' + Math.round((Number(box.y) + Number(box.h)) * 100) + '%'
    : '';
  html += '<div class="grm-subject-review-meta">Detector confidence ' +
    (conf == null ? 'unknown' : Math.round(conf * 100) + '%') + boxText + '</div>';
  html += '</div>';
  html += renderSpeciesBlock(
    'Selected subject predictions',
    buildSpeciesRows(selected.predictions || [], threshold)
  );
  if (!selected.predictions || selected.predictions.length === 0) {
    html += '<div class="grm-subject-review-hint" data-testid="subject-unclassified">No species prediction is available for this subject.</div>';
  }
  return html;
}

// Drop aggregate rows (and their models) below `threshold`.
/** @param {ReviewSpeciesRow[]} rows @param {number} [threshold] */
function filterAggSpeciesRows(rows, threshold) {
  if (!threshold) return rows || [];
  return (rows || []).map(function(sp) {
    return { species: sp.species, models: (sp.models || []).filter(function(m) { return m.confidence >= threshold; }) };
  }).filter(function(sp) { return sp.models.length > 0; });
}

// Client-side mirror of _build_species_predictions (vireo/pipeline.py): aggregate
// a list of photo objects' species_top5 into consensus rows
// ({species, count, avg_confidence, models:[{model, confidence, photo_count}]},
// sorted by total count desc, models sorted by name). Used to rebuild a burst's
// predictions after a local detach so the cached/displayed "Burst consensus"
// reflects only the photos still in that burst — not the ones split off.
/** @param {ReviewPhoto[]} photos */
function buildBurstConsensus(photos) {
  /** @type {Record<string,Record<string,{confs: number[]; count: number}>>} */ var modelData = {};   // species -> model -> {confs:[], count:0}
  /** @type {string[]} */ var order = [];       // species insertion order (stable tiebreak, matches server)
  (photos || []).forEach(function(p) {
    ((p && p.species_top5) || []).forEach(function(entry) {
      var sp = entry[0];
      var conf = entry[1];
      var model = entry[2] || 'unknown';
      if (!modelData[sp]) { modelData[sp] = {}; order.push(sp); }
      if (!requireReviewValue(modelData[sp])[model]) requireReviewValue(modelData[sp])[model] = { confs: [], count: 0 };
      requireReviewValue(requireReviewValue(modelData[sp])[model]).confs.push(conf);
      requireReviewValue(requireReviewValue(modelData[sp])[model]).count += 1;
    });
  });
  var round4 = function(/** @type {number} */ x) { return Math.round(x * 1e4) / 1e4; };
  var rows = order.map(function(sp) {
    /** @type {ReviewSpeciesModel[]} */ var models = [];
    var totalCount = 0, totalConfSum = 0, totalConfCount = 0;
    Object.keys(modelData[sp] || {}).sort().forEach(function(model) {
      var data = requireReviewValue(modelData[sp])[model];
      var sum = requireReviewValue(data).confs.reduce(function(a, b) { return a + b; }, 0);
      models.push({ model: model, confidence: round4(sum / requireReviewValue(data).confs.length), photo_count: requireReviewValue(data).count });
      totalCount += requireReviewValue(data).count;
      totalConfSum += sum;
      totalConfCount += requireReviewValue(data).confs.length;
    });
    return {
      species: sp,
      count: totalCount,
      avg_confidence: totalConfCount ? round4(totalConfSum / totalConfCount) : 0,
      models: models,
    };
  });
  // Stable sort by total count desc (insertion order breaks ties, like the server).
  rows.sort(function(a, b) { return b.count - a.count; });
  return rows;
}

// Client mirror of encounter_species_label() (vireo/encounters.py): pick the
// species with the highest confidence-weighted sum across all photos' top-5
// predictions, breaking ties by first-appearance order. Used to persist an
// unconfirmed species override for a detached single-photo burst so the
// local save-cache path agrees with the server detach endpoint, which
// derives the same label via _rebuild_encounter_species_label. Sorting by
// prediction count (as buildBurstConsensus does for display) can pick a
// different species when a lower-confidence label repeats across
// detections/models while a higher-confidence one appears once — e.g.
// [A .90, B .44, B .44] where the server picks A but a count sort picks B.
/** @param {ReviewPhoto[]} photos */
function candidateSpeciesOverrideFromPhotos(photos) {
  /** @type {Record<string,number>} */ var weights = {};
  /** @type {string[]} */ var order = [];
  (photos || []).forEach(function(p) {
    ((p && p.species_top5) || []).forEach(function(entry) {
      var sp = entry[0];
      var conf = entry[1] || 0;
      if (!(sp in weights)) { weights[sp] = 0; order.push(sp); }
      weights[sp] = (weights[sp] || 0) + conf;
    });
  });
  if (order.length === 0) return null;
  var best = order[0] || '';
  for (var i = 1; i < order.length; i++) {
    if ((weights[order[i] || ''] || 0) > (weights[best] || 0)) best = order[i] || '';
  }
  return { species: best, confirmed: false };
}

// Build the per-photo + consensus species prediction blocks. The per-photo
// block changes as you select different photos; the consensus block is the
// per-species mean over the scope's frames. The title names only the scope
// ("Burst consensus" / "Encounter consensus") — it deliberately does NOT claim
// a global frame denominator, because each row is averaged only over the frames
// where that species appears (surfaced as per-model frame counts in
// renderSpeciesBlock). Callers pass the scope's own predictions
// (`consensusRows`) and title so the block matches what's on screen — burst for
// the GRM, encounter for the flat-cache fallback. `threshold` (0..1) filters by
// confidence; pass 0 for none.
/** @param {ReviewPhoto} photo @param {ReviewSpeciesRow[]} consensusRows @param {string} consensusTitleText @param {number} [threshold] */
function buildSpeciesPredictionsHtml(photo, consensusRows, consensusTitleText, threshold) {
  threshold = threshold || 0;
  var html = '';
  var subjects = grmPhotoSubjects(photo);
  if (subjects.length > 1) {
    html += buildMultiSubjectReviewHtml(photo, threshold);
  } else {
    html += renderSpeciesBlock('Species Predictions (this photo)', buildPerPhotoSpeciesRows(photo, threshold));
  }
  if (consensusRows && consensusRows.length > 0) {
    html += renderSpeciesBlock(consensusTitleText, filterAggSpeciesRows(consensusRows, threshold));
  }
  return html;
}

/** @param {ReviewPhoto} photo */
function buildPipelineMetadataHtml(photo) {
  var enc = currentReviewResults().encounters[grmState.encIdx ?? -1];
  var label = (photo.label || '').toLowerCase();

  var html = '';

  // Triage badge
  html += '<span class="triage-badge ' + label + '">' + (photo.label || 'UNSCORED') + '</span>';
  if (photo.rarity_protected) {
    html += ' <span class="triage-badge review">Rarity Protected</span>';
  }

  // Reject reasons
  if (photo.reject_reasons && photo.reject_reasons.length > 0) {
    html += '<div class="reject-reasons">';
    photo.reject_reasons.forEach(function(r) {
      html += '<div class="reject-reason">&#9888; ' + escapeHtml(describeReviewRejectReason(r)) + '</div>';
    });
    html += '</div>';
  }

  var conflictEnc = pipelineResults && currentReviewResults().encounters &&
    currentReviewResults().encounters[grmState.encIdx ?? -1];
  var conflictBurst = conflictEnc && conflictEnc.bursts &&
    conflictEnc.bursts[grmState.burstIdx ?? -1];
  var conflictExpected = speciesCandidateForReviewUnit(conflictEnc, conflictBurst);
  // Same expected identity the card behind this modal used: without the
  // review unit's own candidate key, an encounter holding two identified taxa
  // that share a display name falls back to the ambiguous "name:" key, and
  // the modal would read that as agreement while the card reads a conflict.
  var speciesConflict = analyzePhotoSpeciesConflict(
    photo, conflictExpected, null,
    speciesCandidateKeyForReviewUnit(conflictEnc, conflictBurst, conflictExpected));
  if (speciesConflict && speciesConflict.severity) {
    var conflictHeading = speciesConflict.severity === 'strong'
      ? 'Strong classification conflict'
      : 'Possible classification conflict';
    html += '<div class="species-conflict-badge inspect-species-conflict ' +
      speciesConflict.severity + '"><span aria-hidden="true">&#9888;</span><span><strong>' +
      conflictHeading + '</strong><span class="species-conflict-explanation">This photo averages ' +
      escapeHtml(speciesConflict.alternativeSpecies) + ' at ' +
      formatSpeciesConfidence(speciesConflict.alternativeSupport) + ', while the current suggestion ' +
      escapeHtml(speciesConflict.expectedSpecies) + ' averages ' +
      formatSpeciesConfidence(speciesConflict.expectedSupport) +
      '. Review the burst before changing its grouping or species.</span></span></div>';
  }

  // Species predictions: this photo, then the consensus for the burst being
  // reviewed (the GRM is scoped to a single burst — see openGroupReview). Using
  // burst-level predictions keeps sibling bursts in the same encounter out of
  // the consensus, and grmState.items (this burst's photos) gives the honest
  // "frames with predictions" denominator. Legacy caches store bursts as raw
  // photo-id arrays with no species_predictions field; detect that with
  // Array.isArray and fall back to encounter scope so the consensus block isn't
  // dropped. An empty burst array stays burst-scoped (the block is simply
  // omitted) rather than re-leaking sibling-burst predictions.
  // Compute the consensus fresh from this burst's current photos rather than
  // trusting the cached burst.species_predictions, which can go stale after a
  // local detach (single-photo bursts inherit the parent's predictions) or a
  // reclassification (per-photo species_top5 updates without rebuilding burst
  // aggregates). Fresh aggregation keeps "Burst consensus" consistent with the
  // per-photo blocks and the photos actually in this burst. Use the same
  // non-removed item set as the visible cards (_grmVisibleItems) so a photo the
  // reviewer has marked for removal (but not yet applied) doesn't keep
  // influencing the consensus.
  var consRows = buildBurstConsensus(_grmVisibleItems());
  html += buildSpeciesPredictionsHtml(photo, consRows, 'Burst consensus', 0);

  html += buildQualityEvidenceHtml(photo);

  // Raw feature table
  html += '<table class="feature-table">';
  var features = [
    ['Subject Tenengrad', photo.subject_tenengrad],
    ['Background Tenengrad', photo.bg_tenengrad],
    ['Crop Completeness', photo.crop_complete],
    ['Background Separation', photo.bg_separation],
    ['Highlight Clip', photo.subject_clip_high],
    ['Shadow Clip', photo.subject_clip_low],
    ['Median Luminance', photo.subject_y_median],
    ['Subject Size', photo.subject_size],
    ['Crop pHash', photo.phash_crop],
    ['Detection Confidence', photo.detection_conf],
  ];
  features.forEach(function(f) {
    var val = f[1];
    if (val === null || val === undefined) val = '—';
    else if (typeof val === 'number') val = val.toFixed(4);
    html += '<tr><td>' + f[0] + '</td><td>' + val + '</td></tr>';
  });
  html += '</table>';

  return html;
}

/** @param {ReviewPhoto} photo */
function buildQualityEvidenceHtml(photo) {
  var html = '';
  if (hasQualityScore(photo)) {
    /** @type {{name: string; key: 'focus_score' | 'exposure_score' | 'composition_score' | 'area_score' | 'noise_score'; weight: number; cls: string}[]} */ var scores = [
      {name: 'Relative focus', key: 'focus_score', weight: 0.45, cls: 'focus'},
      {name: 'Exposure', key: 'exposure_score', weight: 0.20, cls: 'exposure'},
      {name: 'Composition', key: 'composition_score', weight: 0.15, cls: 'composition'},
      {name: 'Area', key: 'area_score', weight: 0.10, cls: 'area'},
      {name: 'Noise', key: 'noise_score', weight: 0.10, cls: 'noise'},
    ];
    html += '<div class="score-waterfall"><div>Quality ranking score: ' + Number(photo.quality_composite).toFixed(3) + '</div>';
    scores.forEach(function(s) {
      var val = Number(photo[s.key]) || 0;
      html += '<div class="score-waterfall-row"><span class="score-wf-label">' + s.name + ' (' + (s.weight * 100) + '%)</span>';
      html += '<div class="score-wf-bar-track"><div class="score-wf-bar-fill ' + s.cls + '" style="width:' + Math.round(val * 100) + '%"></div></div>';
      html += '<span class="score-wf-val">' + val.toFixed(3) + ' (' + (val * s.weight).toFixed(3) + ')</span></div>';
    });
    html += '</div>';
  }
  return html + buildRelativeFocusHtml(photo);
}

/** @param {ReviewPhoto} photo */
function buildRelativeFocusHtml(photo) {
  var evidence = photo.focus_evidence;
  var html = '<section class="relative-focus-evidence" aria-label="Relative focus evidence"><strong>Relative focus evidence</strong>';
  if (!evidence) {
    html += '<p>Measurement basis and comparison group were not recorded for this saved score. Rerun scoring to inspect them.</p>';
    if (photo.eye_tenengrad != null) html += '<p>Cached eye Tenengrad: ' + Number(photo.eye_tenengrad).toFixed(2) + '</p>';
  } else {
    var basis = evidence.basis === 'eye' ? 'Eye window (multiple scales)' : 'Subject mask (multiple scales)';
    html += '<p>Measurement basis: ' + basis + '. Raw Tenengrad: ' + evidence.measurement.toFixed(2) + ' (mean squared image gradients).</p>';
    html += '<p>Within-group percentile: ' + (evidence.percentile * 100).toFixed(2) + '% among ' + evidence.cohort_size +
      ' photos with current subject analysis and the same measurement basis in the encounter, including this photo.</p>';
    html += evidence.method === 'eye_percentile'
      ? '<p>Relative focus score equals the eye percentile. Ties receive their midpoint rank; a single photo receives 50%.</p>'
      : '<p>Relative focus score = 70% subject percentile + 30% subject / (subject + background) Tenengrad. Background Tenengrad: ' +
        Number(evidence.background_measurement).toFixed(2) + '. Measured zero subject gradients receive a score of 0. Ties receive their midpoint rank; a single photo receives 50%.</p>';
    html += buildFocusCohortHtml(photo, evidence);
  }
  html += '<p>Absolute sharpness: unvalidated. Relative scores and raw gradients do not establish that a photo is out of focus. ' +
    'Regrouping can change rank; exposure, texture, noise, processing and the selected region can change Tenengrad. Inspect the image before deciding.</p></section>';
  return html;
}

/** @param {ReviewPhoto} photo @param {ReviewFocusEvidence} evidence */
function buildFocusCohortHtml(photo, evidence) {
  var encounter = findEncounterForPhoto(photo.id);
  var cohort = encounter && encounter.focus_cohorts && encounter.focus_cohorts[evidence.cohort_key];
  if (!cohort || cohort.size !== evidence.cohort_size || cohort.photo_ids.indexOf(photo.id) < 0) {
    return '<p>Exact comparison membership is unavailable in this saved review. Rerun scoring to refresh it.</p>';
  }
  var names = cohort.photo_ids.map(function(id) {
    var peer = findPhotoInResults(id);
    return escapeHtml(peer ? peer.filename : 'Photo') + ' (photo ' + id + ')';
  });
  var html = '<details><summary>Comparison group: ' + cohort.size + ' encounter photos</summary><p>' + names.join(', ') + '</p>';
  if (cohort.unidentified_count) html += '<p>' + cohort.unidentified_count + ' additional measurements have no photo identifier.</p>';
  return html + '</details>';
}

/** @param {string} reason */
function describeReviewRejectReason(reason) {
  if (/^(out_of_focus|eye_soft)\b/.test(reason)) {
    return 'Saved relative-focus warning: absolute defocus was not validated. Rerun scoring to refresh this suggestion.';
  }
  if (reason.startsWith('low_quality_without_focus')) return reason.replace('low_quality_without_focus', 'Quality floor with relative focus replaced by a neutral midpoint');
  return reason;
}
