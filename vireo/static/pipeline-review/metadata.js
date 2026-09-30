// Species predictions, consensus, and photo metadata presentation.
// Classic page script; shared globals are initialized before boot.js runs.

// Render a single species-prediction block (title + rows). Each row in `rows`
// is {species, models: [{model, confidence}]}. Returns '' when there are no
// rows so we never emit an empty header.
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
function buildSpeciesRows(top5, threshold) {
  top5 = top5 || [];
  if (top5.length === 0) return [];
  var bySpecies = {};
  var order = [];
  top5.forEach(function(entry) {
    var sp = entry[0];
    var conf = entry[1];
    var model = entry[2] || 'unknown';
    if (conf < threshold) return;
    if (!bySpecies[sp]) { bySpecies[sp] = []; order.push(sp); }
    bySpecies[sp].push({ model: model, confidence: conf });
  });
  var rows = order.map(function(sp) {
    var models = bySpecies[sp].slice().sort(function(a, b) { return b.confidence - a.confidence; });
    return { species: sp, models: models };
  });
  rows.sort(function(a, b) { return b.models[0].confidence - a.models[0].confidence; });
  return rows;
}

function buildPerPhotoSpeciesRows(photo, threshold) {
  return buildSpeciesRows((photo && photo.species_top5) || [], threshold);
}

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
    var isSelected = String(subject.detection_id) === String(selected.detection_id);
    html += '<button type="button" class="grm-subject-chip' + (isSelected ? ' selected' : '') +
      '" data-testid="subject-chip" onclick="grmSelectSubject(' + photo.id + ', ' +
      Number(subject.detection_id) + ')">' + escapeHtml(grmSubjectLabel(subject, index)) + '</button>';
  });
  html += '</div>';

  var box = selected.box || {};
  var conf = selected.detection_confidence;
  var boxText = [box.x, box.y, box.w, box.h].every(function(value) { return value != null; })
    ? ' · box x ' + Math.round(box.x * 100) + '–' + Math.round((box.x + box.w) * 100) +
      '%, y ' + Math.round(box.y * 100) + '–' + Math.round((box.y + box.h) * 100) + '%'
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
function buildBurstConsensus(photos) {
  var modelData = {};   // species -> model -> {confs:[], count:0}
  var order = [];       // species insertion order (stable tiebreak, matches server)
  (photos || []).forEach(function(p) {
    ((p && p.species_top5) || []).forEach(function(entry) {
      var sp = entry[0];
      var conf = entry[1];
      var model = entry[2] || 'unknown';
      if (!modelData[sp]) { modelData[sp] = {}; order.push(sp); }
      if (!modelData[sp][model]) modelData[sp][model] = { confs: [], count: 0 };
      modelData[sp][model].confs.push(conf);
      modelData[sp][model].count += 1;
    });
  });
  var round4 = function(x) { return Math.round(x * 1e4) / 1e4; };
  var rows = order.map(function(sp) {
    var models = [];
    var totalCount = 0, totalConfSum = 0, totalConfCount = 0;
    Object.keys(modelData[sp]).sort().forEach(function(model) {
      var data = modelData[sp][model];
      var sum = data.confs.reduce(function(a, b) { return a + b; }, 0);
      models.push({ model: model, confidence: round4(sum / data.confs.length), photo_count: data.count });
      totalCount += data.count;
      totalConfSum += sum;
      totalConfCount += data.confs.length;
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
function candidateSpeciesOverrideFromPhotos(photos) {
  var weights = {};
  var order = [];
  (photos || []).forEach(function(p) {
    ((p && p.species_top5) || []).forEach(function(entry) {
      var sp = entry[0];
      var conf = entry[1] || 0;
      if (!(sp in weights)) { weights[sp] = 0; order.push(sp); }
      weights[sp] += conf;
    });
  });
  if (order.length === 0) return null;
  var best = order[0];
  for (var i = 1; i < order.length; i++) {
    if (weights[order[i]] > weights[best]) best = order[i];
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

function buildPipelineMetadataHtml(photo) {
  var enc = pipelineResults.encounters[grmState.encIdx];
  var label = (photo.label || '').toLowerCase();
  var q = hasQualityScore(photo) ? Number(photo.quality_composite) : null;

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
      html += '<div class="reject-reason">&#9888; ' + r + '</div>';
    });
    html += '</div>';
  }

  var conflictEnc = pipelineResults && pipelineResults.encounters &&
    pipelineResults.encounters[grmState.encIdx];
  var conflictBurst = conflictEnc && conflictEnc.bursts &&
    conflictEnc.bursts[grmState.burstIdx];
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

  // Score waterfall
  var scores = [
    {name: 'Focus', key: 'focus_score', weight: 0.45, cls: 'focus'},
    {name: 'Exposure', key: 'exposure_score', weight: 0.20, cls: 'exposure'},
    {name: 'Composition', key: 'composition_score', weight: 0.15, cls: 'composition'},
    {name: 'Area', key: 'area_score', weight: 0.10, cls: 'area'},
    {name: 'Noise', key: 'noise_score', weight: 0.10, cls: 'noise'},
  ];
  if (q !== null) {
    html += '<div class="score-waterfall">';
    html += '<div style="font-size:13px;font-weight:600;margin-bottom:8px;">Quality Score: ' + q.toFixed(3) + '</div>';
    scores.forEach(function(s) {
      var val = photo[s.key] || 0;
      var weighted = val * s.weight;
      var pct = Math.round(val * 100);
      html += '<div class="score-waterfall-row">';
      html += '<span class="score-wf-label">' + s.name + ' (' + (s.weight*100) + '%)</span>';
      html += '<div class="score-wf-bar-track"><div class="score-wf-bar-fill ' + s.cls + '" style="width:' + pct + '%"></div></div>';
      html += '<span class="score-wf-val">' + val.toFixed(3) + ' (' + weighted.toFixed(3) + ')</span>';
      html += '</div>';
    });
    html += '</div>';
  }

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
